use std::{env, fs, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use inference_streams::{
    SCHEMA_VERSION, StreamsClient, StreamsConfig,
    wire::{
        self, AttemptStarted, GenerationCompleted, GenerationFailed, GenerationResult, TokenChunk,
        generation_result::Event,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::oneshot;
use tracing::{error, info, warn};

#[derive(Clone)]
struct EngineConfig {
    backend: Backend,
    model: String,
    endpoint: String,
    synthetic_token_delay: Duration,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct ServiceConfig {
    broker: BrokerConfig,
    model: ModelConfig,
    backend: BackendConfig,
    #[serde(default)]
    streams: StreamSettings,
}

#[derive(Debug, Deserialize)]
struct BrokerConfig {
    url: String,
}

#[derive(Debug, Deserialize)]
struct ModelConfig {
    id: String,
    #[serde(default)]
    endpoint: String,
}

#[derive(Debug, Deserialize)]
struct BackendConfig {
    kind: String,
    mlx_base_url: Option<String>,
    openai_base_url: Option<String>,
    synthetic_token_delay_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct StreamSettings {
    active_ttl_seconds: u64,
    result_retention_seconds: u64,
    lease_seconds: u64,
    reclaim_idle_seconds: u64,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            active_ttl_seconds: 3_600,
            result_retention_seconds: 600,
            lease_seconds: 60,
            reclaim_idle_seconds: 90,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backend {
    Synthetic,
    OpenAiCompatible,
}

#[derive(Debug, Deserialize)]
struct RuntimeRequest {
    model: String,
    messages: Option<Vec<Value>>,
    prompt: Option<String>,
    max_tokens: usize,
    temperature: f32,
    #[serde(default)]
    stop: Vec<String>,
}

#[derive(Debug, Default)]
struct GenerationUsage {
    prompt_tokens: u64,
    generated_tokens: u64,
    finish_reason: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let service = load_config()?;
    let broker_url = env::var("VALKEY_URL").unwrap_or(service.broker.url);
    let stream_config = StreamsConfig {
        active_ttl: Duration::from_secs(service.streams.active_ttl_seconds),
        result_retention: Duration::from_secs(service.streams.result_retention_seconds),
        lease_duration: Duration::from_secs(service.streams.lease_seconds),
    };
    let streams = StreamsClient::connect(&broker_url, stream_config).await?;
    let worker_id =
        env::var("ENGINE_ID").unwrap_or_else(|_| format!("engine-{}", uuid::Uuid::new_v4()));
    let mut consumer = streams.consumer(worker_id.clone()).await?;
    let backend_kind = env::var("BACKEND").unwrap_or(service.backend.kind);
    let backend = if backend_kind == "synthetic" {
        Backend::Synthetic
    } else {
        Backend::OpenAiCompatible
    };
    let endpoint = env::var("MODEL_ENDPOINT")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| (!service.model.endpoint.is_empty()).then_some(service.model.endpoint))
        .or(service.backend.openai_base_url)
        .or(service.backend.mlx_base_url)
        .unwrap_or_else(|| "http://127.0.0.1:18000".to_string());
    let config = EngineConfig {
        backend,
        model: env::var("MODEL_ID").unwrap_or(service.model.id),
        endpoint,
        synthetic_token_delay: Duration::from_millis(
            service.backend.synthetic_token_delay_ms.unwrap_or(0),
        ),
        http: reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()?,
    };

    info!(engine_id = %worker_id, model = %config.model, endpoint = %config.endpoint, backend = ?config.backend, "inference engine started");
    loop {
        let consumed = match consumer.next(Duration::from_secs(5)).await {
            Ok(Some(job)) => Some(job),
            Ok(None) => {
                consumer
                    .reclaim_stale(Duration::from_secs(service.streams.reclaim_idle_seconds))
                    .await?
            }
            Err(error) => {
                warn!(%error, "request subscription interrupted; reconnecting consumer");
                tokio::time::sleep(Duration::from_millis(250)).await;
                consumer = streams.consumer(worker_id.clone()).await?;
                None
            }
        };
        let Some(consumed) = consumed else {
            continue;
        };
        let entry_id = consumed.entry_id.clone();
        match run_job(&streams, &config, &worker_id, consumed.job).await {
            Ok(JobDisposition::Acknowledge) => {
                let acknowledged = consumer.acknowledge(&entry_id).await?;
                info!(%entry_id, acknowledged, "completed inference job");
            }
            Ok(JobDisposition::Pending) => {
                info!(%entry_id, "job is owned by another live engine");
            }
            Err(error) => {
                error!(%entry_id, %error, "job failed before terminal event; leaving it pending");
            }
        }
    }
}

enum JobDisposition {
    Acknowledge,
    Pending,
}

async fn run_job(
    streams: &StreamsClient,
    config: &EngineConfig,
    engine_id: &str,
    job: wire::GenerationJob,
) -> Result<JobDisposition> {
    if job.deadline_unix_ms > 0 && unix_ms() > job.deadline_unix_ms {
        streams.cancel_quota(&job.reservation_id).await?;
        publish_failure(
            streams,
            &job,
            engine_id,
            "deadline_exceeded",
            "generation deadline exceeded",
            false,
        )
        .await?;
        return Ok(JobDisposition::Acknowledge);
    }
    if streams
        .status(&job.request_id)
        .await?
        .is_some_and(|status| status.state == "completed")
    {
        return Ok(JobDisposition::Acknowledge);
    }
    let attempt_id = uuid::Uuid::new_v4().to_string();
    let lease_owner = format!("{engine_id}:{attempt_id}");
    if !streams.acquire_lease(&job.request_id, &lease_owner).await? {
        return Ok(JobDisposition::Pending);
    }
    let (stop_heartbeat, heartbeat_done) =
        spawn_lease_heartbeat(streams.clone(), job.request_id.clone(), lease_owner.clone());
    let result = execute_attempt(streams, config, &job, &attempt_id).await;
    let _ = stop_heartbeat.send(());
    let _ = heartbeat_done.await;
    streams.release_lease(&job.request_id, &lease_owner).await?;

    match result {
        Ok(usage) => {
            streams
                .reconcile_quota(&job.reservation_id, usage.generated_tokens)
                .await?;
            publish_completion(streams, &job, &attempt_id, &usage).await?;
            info!(request_id = %job.request_id, %attempt_id, prompt_tokens = usage.prompt_tokens, generated_tokens = usage.generated_tokens, finish_reason = %usage.finish_reason, "generation attempt completed");
            Ok(JobDisposition::Acknowledge)
        }
        Err(error) => {
            let error_chain = format!("{error:#}");
            error!(
                request_id = %job.request_id,
                %attempt_id,
                error = %error_chain,
                "generation backend failed"
            );
            streams.reconcile_quota(&job.reservation_id, 0).await?;
            publish_failure(
                streams,
                &job,
                &attempt_id,
                "backend_error",
                &error_chain,
                false,
            )
            .await?;
            Ok(JobDisposition::Acknowledge)
        }
    }
}

fn spawn_lease_heartbeat(
    streams: StreamsClient,
    request_id: String,
    owner: String,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (sender, mut receiver) = oneshot::channel();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut receiver => return,
                _ = tokio::time::sleep(Duration::from_secs(20)) => {
                    match streams.renew_lease(&request_id, &owner).await {
                        Ok(true) => {}
                        Ok(false) => {
                            warn!(%request_id, "generation lease was lost");
                            return;
                        }
                        Err(error) => warn!(%request_id, %error, "failed to renew generation lease"),
                    }
                }
            }
        }
    });
    (sender, handle)
}

async fn execute_attempt(
    streams: &StreamsClient,
    config: &EngineConfig,
    job: &wire::GenerationJob,
    attempt_id: &str,
) -> Result<GenerationUsage> {
    if job.model_id != config.model {
        bail!("model {} is not loaded by this engine", job.model_id);
    }
    let request: RuntimeRequest = serde_json::from_slice(&job.request_json)?;
    streams
        .publish_result(&GenerationResult {
            schema_version: SCHEMA_VERSION,
            request_id: job.request_id.clone(),
            attempt_id: attempt_id.to_string(),
            event: Some(Event::AttemptStarted(AttemptStarted {})),
        })
        .await?;

    let mut usage = match config.backend {
        Backend::Synthetic => stream_synthetic(streams, config, job, attempt_id, &request).await?,
        Backend::OpenAiCompatible => {
            stream_openai_compatible(streams, config, job, attempt_id, request).await?
        }
    };
    if usage.finish_reason.is_empty() {
        usage.finish_reason = "stop".to_string();
    }
    Ok(usage)
}

async fn publish_completion(
    streams: &StreamsClient,
    job: &wire::GenerationJob,
    attempt_id: &str,
    usage: &GenerationUsage,
) -> Result<()> {
    streams
        .publish_result(&GenerationResult {
            schema_version: SCHEMA_VERSION,
            request_id: job.request_id.clone(),
            attempt_id: attempt_id.to_string(),
            event: Some(Event::Completed(GenerationCompleted {
                prompt_tokens: usage.prompt_tokens,
                generated_tokens: usage.generated_tokens,
                finish_reason: usage.finish_reason.clone(),
            })),
        })
        .await?;
    Ok(())
}

async fn stream_synthetic(
    streams: &StreamsClient,
    config: &EngineConfig,
    job: &wire::GenerationJob,
    attempt_id: &str,
    request: &RuntimeRequest,
) -> Result<GenerationUsage> {
    let prompt = extract_prompt(request);
    let words = format!("Generated response for: {prompt}")
        .split_whitespace()
        .map(str::to_string)
        .take(request.max_tokens)
        .collect::<Vec<_>>();
    for (index, word) in words.iter().enumerate() {
        publish_token(
            streams,
            job,
            attempt_id,
            format!("{word} "),
            index as u64 + 1,
        )
        .await?;
        if !config.synthetic_token_delay.is_zero() {
            tokio::time::sleep(config.synthetic_token_delay).await;
        }
    }
    Ok(GenerationUsage {
        prompt_tokens: estimate_tokens(&prompt),
        generated_tokens: words.len() as u64,
        finish_reason: "stop".to_string(),
    })
}

async fn stream_openai_compatible(
    streams: &StreamsClient,
    config: &EngineConfig,
    job: &wire::GenerationJob,
    attempt_id: &str,
    request: RuntimeRequest,
) -> Result<GenerationUsage> {
    let endpoint = if job.model_endpoint.is_empty() {
        &config.endpoint
    } else {
        &job.model_endpoint
    };
    let mut payload = json!({
        "model": request.model,
        "messages": request.messages.clone().unwrap_or_else(|| vec![json!({"role": "user", "content": request.prompt.clone().unwrap_or_default()})]),
        "max_tokens": request.max_tokens,
        "temperature": request.temperature,
        "stream": true,
        "stream_options": {"include_usage": true}
    });
    if payload["messages"].as_array().is_some_and(Vec::is_empty) {
        payload["messages"] =
            json!([{"role": "user", "content": request.prompt.clone().unwrap_or_default()}]);
    }
    if !request.stop.is_empty() {
        payload["stop"] = json!(request.stop);
    }
    let response = config
        .http
        .post(format!(
            "{}/v1/chat/completions",
            endpoint.trim_end_matches('/')
        ))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    let mut bytes = response.bytes_stream();
    let mut buffer = String::new();
    let mut usage = GenerationUsage::default();
    while let Some(chunk) = bytes.next().await {
        buffer.push_str(&String::from_utf8_lossy(&chunk?));
        while let Some(newline) = buffer.find('\n') {
            let line = buffer[..newline].trim_end_matches('\r').to_string();
            buffer.drain(..=newline);
            if let Some(data) = line.strip_prefix("data:").map(str::trim) {
                if data == "[DONE]" || data.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(data)
                    .with_context(|| format!("decode model SSE event: {data}"))?;
                if let Some(runtime_usage) = value.get("usage") {
                    usage.prompt_tokens = runtime_usage
                        .get("prompt_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(usage.prompt_tokens);
                    usage.generated_tokens = runtime_usage
                        .get("completion_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(usage.generated_tokens);
                }
                let choice = value
                    .get("choices")
                    .and_then(Value::as_array)
                    .and_then(|items| items.first());
                if let Some(reason) = choice
                    .and_then(|choice| choice.get("finish_reason"))
                    .and_then(Value::as_str)
                {
                    usage.finish_reason = reason.to_string();
                }
                let text = choice
                    .and_then(|choice| choice.get("delta"))
                    .and_then(|delta| delta.get("content"))
                    .or_else(|| choice.and_then(|choice| choice.get("text")))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if !text.is_empty() {
                    usage.generated_tokens += estimate_tokens(text).max(1);
                    publish_token(
                        streams,
                        job,
                        attempt_id,
                        text.to_string(),
                        usage.generated_tokens,
                    )
                    .await?;
                }
            }
        }
    }
    if usage.prompt_tokens == 0 {
        usage.prompt_tokens = estimate_tokens(&extract_prompt(&request));
    }
    Ok(usage)
}

async fn publish_token(
    streams: &StreamsClient,
    job: &wire::GenerationJob,
    attempt_id: &str,
    text: String,
    generated_tokens: u64,
) -> Result<()> {
    streams
        .publish_result(&GenerationResult {
            schema_version: SCHEMA_VERSION,
            request_id: job.request_id.clone(),
            attempt_id: attempt_id.to_string(),
            event: Some(Event::Token(TokenChunk {
                text,
                cumulative_generated_tokens: generated_tokens,
            })),
        })
        .await?;
    Ok(())
}

async fn publish_failure(
    streams: &StreamsClient,
    job: &wire::GenerationJob,
    attempt_id: &str,
    code: &str,
    message: &str,
    retryable: bool,
) -> Result<()> {
    streams
        .publish_result(&GenerationResult {
            schema_version: SCHEMA_VERSION,
            request_id: job.request_id.clone(),
            attempt_id: attempt_id.to_string(),
            event: Some(Event::Failed(GenerationFailed {
                code: code.to_string(),
                message: message.to_string(),
                retryable,
            })),
        })
        .await?;
    Ok(())
}

fn extract_prompt(request: &RuntimeRequest) -> String {
    if let Some(messages) = &request.messages {
        return messages
            .iter()
            .filter_map(|message| message.get("content").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
    }
    request.prompt.clone().unwrap_or_default()
}

fn estimate_tokens(text: &str) -> u64 {
    text.split_whitespace().count() as u64
}

fn load_config() -> Result<ServiceConfig> {
    let path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config/local.yaml".to_string());
    let contents = fs::read_to_string(&path).with_context(|| format!("read config file {path}"))?;
    serde_yaml::from_str(&contents).with_context(|| format!("parse config file {path}"))
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_is_assembled_from_messages() {
        let request = RuntimeRequest {
            model: "model".into(),
            messages: Some(vec![
                json!({"role": "user", "content": "hello"}),
                json!({"role": "assistant", "content": "world"}),
            ]),
            prompt: None,
            max_tokens: 10,
            temperature: 0.0,
            stop: Vec::new(),
        };
        assert_eq!(extract_prompt(&request), "hello\nworld");
    }
}
