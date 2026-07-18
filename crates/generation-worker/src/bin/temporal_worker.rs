use std::{env, time::Duration};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use redis::AsyncCommands;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use temporalio_client::{
    Client, ClientOptions, ClientTlsOptions, Connection, ConnectionOptions, TlsOptions,
};
use temporalio_common::{telemetry::TelemetryOptions, worker::WorkerTaskTypes};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ApplicationFailure, Worker, WorkerOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions};
use tracing::{error, info};

const ACTIVE_GENERATIONS_KEY: &str = "inference:active_generations";
const LAST_GENERATION_FINISHED_MS_KEY: &str = "inference:last_generation_finished_ms";

#[derive(Debug, Deserialize, Serialize, Clone)]
struct GenerationJob {
    request_id: String,
    upstream_path: String,
    response_stream: String,
    body: JsonValue,
    cache: CacheMetadata,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct CacheMetadata {
    cache_key: String,
    model: String,
    prefix_tokens_estimate: u64,
    cache_policy: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct GenerateWorkflowResult {
    request_id: String,
    generated_tokens: u64,
    finish_reason: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseEvent {
    Chunk { data: String, token_estimate: u64 },
    Done { generated_tokens: u64 },
    Error { message: String },
}

#[derive(Clone)]
struct WorkerConfig {
    redis_url: String,
    backend: Backend,
    model: String,
    openai_base_url: String,
    synthetic_token_delay: Duration,
    http: HttpClient,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backend {
    Synthetic,
    OpenAi,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let task_queue =
        env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| "inference-gpu".to_string());
    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()
            .map_err(anyhow::Error::msg)?,
    )?;
    let connection = Connection::connect(temporal_connection_options()?).await?;
    let client = Client::new(
        connection,
        ClientOptions::new(
            env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string()),
        )
        .build(),
    )?;

    let worker_options = WorkerOptions::new(task_queue.clone())
        .task_types(WorkerTaskTypes::all())
        .register_workflow::<GpuGenerateWorkflow>()
        .register_workflow::<GpuStreamGenerateWorkflow>()
        .register_activities(GenerationActivities)
        .build();

    info!(%task_queue, "starting Rust Temporal generation worker");
    let mut worker = Worker::new(&runtime, client, worker_options)
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    worker.run().await?;
    Ok(())
}

#[workflow(name = "GpuGenerateWorkflow")]
#[derive(Default)]
struct GpuGenerateWorkflow;

#[workflow_methods]
impl GpuGenerateWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: GenerationJob,
    ) -> WorkflowResult<GenerateWorkflowResult> {
        run_generation_workflow(ctx, input).await
    }
}

#[workflow(name = "GpuStreamGenerateWorkflow")]
#[derive(Default)]
struct GpuStreamGenerateWorkflow;

#[workflow_methods]
impl GpuStreamGenerateWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: GenerationJob,
    ) -> WorkflowResult<GenerateWorkflowResult> {
        run_generation_workflow(ctx, input).await
    }
}

async fn run_generation_workflow<W>(
    ctx: &mut WorkflowContext<W>,
    input: GenerationJob,
) -> WorkflowResult<GenerateWorkflowResult> {
    Ok(ctx
        .start_activity(
            GenerationActivities::run_generation,
            input,
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(600))
                .heartbeat_timeout(Duration::from_secs(30))
                .build(),
        )
        .await?)
}

struct GenerationActivities;

#[activities]
impl GenerationActivities {
    #[activity(name = "RunGeneration")]
    async fn run_generation(
        ctx: ActivityContext,
        job: GenerationJob,
    ) -> Result<GenerateWorkflowResult, ActivityError> {
        run_generation_activity(ctx, job)
            .await
            .map_err(|error| ActivityError::application(ApplicationFailure::non_retryable(error)))
    }
}

async fn run_generation_activity(
    ctx: ActivityContext,
    job: GenerationJob,
) -> Result<GenerateWorkflowResult> {
    let config = worker_config()?;
    let broker = redis::Client::open(config.redis_url.clone())?;
    let mut conn = broker.get_multiplexed_async_connection().await?;
    info!(
        request_id = %job.request_id,
        response_stream = %job.response_stream,
        backend = ?config.backend,
        cache_key = %job.cache.cache_key,
        prefix_tokens_estimate = job.cache.prefix_tokens_estimate,
        "starting Temporal generation activity"
    );

    let _: i64 = conn.incr(ACTIVE_GENERATIONS_KEY, 1).await?;
    let result = if job
        .body
        .get("stream")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        stream_completion(ctx, &mut conn, &config, &job).await
    } else {
        write_completion(ctx, &mut conn, &config, &job).await
    };
    let _: i64 = conn.decr(ACTIVE_GENERATIONS_KEY, 1).await?;
    let _: () = conn
        .set(
            LAST_GENERATION_FINISHED_MS_KEY,
            chrono::Utc::now().timestamp_millis(),
        )
        .await?;

    match result {
        Ok(generated_tokens) => Ok(GenerateWorkflowResult {
            request_id: job.request_id,
            generated_tokens,
            finish_reason: "stop".to_string(),
        }),
        Err(error) => {
            error!(request_id = %job.request_id, %error, "generation activity failed");
            let _ = write_event(
                &mut conn,
                &job.response_stream,
                ResponseEvent::Error {
                    message: error.to_string(),
                },
            )
            .await;
            Err(error)
        }
    }
}

async fn write_completion(
    ctx: ActivityContext,
    conn: &mut redis::aio::MultiplexedConnection,
    config: &WorkerConfig,
    job: &GenerationJob,
) -> Result<u64> {
    if config.backend == Backend::OpenAi {
        return stream_with_openai(ctx, conn, config, job).await;
    }

    let text = generate_text(config, &job.body).await?;
    let generated_tokens = estimate_tokens(&text);
    let body = json!({
        "id": format!("chatcmpl-{}", job.request_id),
        "object": "chat.completion",
        "model": config.model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": "stop"
        }],
        "usage": {
            "completion_tokens": generated_tokens,
            "total_tokens": generated_tokens
        }
    });
    write_event(
        conn,
        &job.response_stream,
        ResponseEvent::Chunk {
            data: serde_json::to_string(&body)?,
            token_estimate: generated_tokens,
        },
    )
    .await?;
    ctx.record_heartbeat(Vec::new());
    write_event(
        conn,
        &job.response_stream,
        ResponseEvent::Done { generated_tokens },
    )
    .await?;
    Ok(generated_tokens)
}

async fn stream_with_openai(
    ctx: ActivityContext,
    conn: &mut redis::aio::MultiplexedConnection,
    config: &WorkerConfig,
    job: &GenerationJob,
) -> Result<u64> {
    let payload = json!({
        "model": config.model,
        "messages": job.body.get("messages").cloned().unwrap_or_else(|| {
            json!([{"role": "user", "content": extract_prompt(&job.body)}])
        }),
        "max_tokens": max_tokens(&job.body),
        "temperature": job.body.get("temperature").and_then(JsonValue::as_f64).unwrap_or(0.0),
        "stream": true
    });
    let response = config
        .http
        .post(format!("{}/v1/chat/completions", config.openai_base_url))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;

    let mut generated_tokens = 0;
    let mut pending = String::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        if ctx.is_cancelled() {
            anyhow::bail!("generation cancelled");
        }
        pending.push_str(&String::from_utf8_lossy(&chunk?));
        let events = drain_sse_events(&mut pending);
        for data in events {
            if data == "[DONE]" {
                write_event(
                    conn,
                    &job.response_stream,
                    ResponseEvent::Chunk {
                        data: "[DONE]".to_string(),
                        token_estimate: 0,
                    },
                )
                .await?;
                write_event(
                    conn,
                    &job.response_stream,
                    ResponseEvent::Done { generated_tokens },
                )
                .await?;
                return Ok(generated_tokens);
            }

            let delta = streamed_delta_text(&data)?;
            let token_estimate = estimate_stream_delta_tokens(&delta);
            generated_tokens += token_estimate;
            write_event(
                conn,
                &job.response_stream,
                ResponseEvent::Chunk {
                    data,
                    token_estimate,
                },
            )
            .await?;
            ctx.record_heartbeat(Vec::new());
        }
    }

    write_event(
        conn,
        &job.response_stream,
        ResponseEvent::Done { generated_tokens },
    )
    .await?;
    Ok(generated_tokens)
}

async fn stream_completion(
    ctx: ActivityContext,
    conn: &mut redis::aio::MultiplexedConnection,
    config: &WorkerConfig,
    job: &GenerationJob,
) -> Result<u64> {
    let text = generate_text(config, &job.body).await?;
    let mut generated_tokens = 0;
    for token in text.split_whitespace() {
        if ctx.is_cancelled() {
            anyhow::bail!("generation cancelled");
        }
        let chunk = json!({
            "id": format!("chatcmpl-{}", job.request_id),
            "object": "chat.completion.chunk",
            "model": config.model,
            "choices": [{
                "index": 0,
                "delta": {"content": format!("{token} ")},
                "finish_reason": null
            }]
        });
        generated_tokens += 1;
        write_event(
            conn,
            &job.response_stream,
            ResponseEvent::Chunk {
                data: serde_json::to_string(&chunk)?,
                token_estimate: 1,
            },
        )
        .await?;
        ctx.record_heartbeat(Vec::new());
        if !config.synthetic_token_delay.is_zero() {
            tokio::time::sleep(config.synthetic_token_delay).await;
        }
    }
    write_event(
        conn,
        &job.response_stream,
        ResponseEvent::Chunk {
            data: "[DONE]".to_string(),
            token_estimate: 0,
        },
    )
    .await?;
    write_event(
        conn,
        &job.response_stream,
        ResponseEvent::Done { generated_tokens },
    )
    .await?;
    Ok(generated_tokens)
}

async fn generate_text(config: &WorkerConfig, body: &JsonValue) -> Result<String> {
    match config.backend {
        Backend::Synthetic => Ok(synthetic_generate(body)),
        Backend::OpenAi => generate_with_openai(config, body).await,
    }
}

async fn generate_with_openai(config: &WorkerConfig, body: &JsonValue) -> Result<String> {
    let payload = json!({
        "model": config.model,
        "messages": body.get("messages").cloned().unwrap_or_else(|| {
            json!([{"role": "user", "content": extract_prompt(body)}])
        }),
        "max_tokens": max_tokens(body),
        "temperature": body.get("temperature").and_then(JsonValue::as_f64).unwrap_or(0.0),
        "stream": false
    });
    let response: JsonValue = config
        .http
        .post(format!("{}/v1/chat/completions", config.openai_base_url))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(response
        .get("choices")
        .and_then(JsonValue::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string())
}

async fn write_event(
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    event: ResponseEvent,
) -> Result<()> {
    let payload = serde_json::to_string(&event)?;
    let _: String = conn.xadd(stream, "*", &[("event", payload)]).await?;
    Ok(())
}

fn worker_config() -> Result<WorkerConfig> {
    let backend = match env::var("BACKEND")
        .unwrap_or_else(|_| "synthetic".to_string())
        .as_str()
    {
        "openai" | "vllm" => Backend::OpenAi,
        _ => Backend::Synthetic,
    };
    Ok(WorkerConfig {
        redis_url: env::var("REDIS_URL").context("REDIS_URL is required")?,
        backend,
        model: env::var("MODEL_ID")
            .unwrap_or_else(|_| "HuggingFaceTB/SmolLM2-135M-Instruct".to_string()),
        openai_base_url: env::var("OPENAI_BASE_URL")
            .or_else(|_| env::var("GPU_BACKEND_URL"))
            .unwrap_or_else(|_| "http://127.0.0.1:8000".to_string())
            .trim_end_matches('/')
            .to_string(),
        synthetic_token_delay: Duration::from_millis(
            env::var("SYNTHETIC_TOKEN_DELAY_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
        ),
        http: HttpClient::new(),
    })
}

fn temporal_connection_options() -> Result<ConnectionOptions> {
    let address = env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| "127.0.0.1:7233".to_string());
    let api_key = env::var("TEMPORAL_API_KEY")
        .ok()
        .filter(|value| !value.is_empty());
    let tls_options = load_temporal_tls_options();
    let target = temporal_target_url(&address, api_key.is_some() || tls_options.is_some())?;
    let mut options = ConnectionOptions::new(target)
        .identity("inference-temporal-worker-rust".to_string())
        .build();
    if let Some(api_key) = api_key {
        options.api_key = Some(api_key);
    }
    options.tls_options =
        tls_options.or_else(|| options.api_key.as_ref().map(|_| TlsOptions::default()));
    Ok(options)
}

fn load_temporal_tls_options() -> Option<TlsOptions> {
    let server_root_ca_cert = env_bytes("TEMPORAL_TLS_CA_CERT");
    let client_cert = env_bytes("TEMPORAL_TLS_CERT");
    let client_private_key = env_bytes("TEMPORAL_TLS_KEY");
    let domain = env::var("TEMPORAL_TLS_DOMAIN")
        .ok()
        .filter(|value| !value.is_empty());

    if server_root_ca_cert.is_none()
        && client_cert.is_none()
        && client_private_key.is_none()
        && domain.is_none()
    {
        return None;
    }

    let client_tls_options = match (client_cert, client_private_key) {
        (Some(client_cert), Some(client_private_key)) => Some(ClientTlsOptions {
            client_cert,
            client_private_key,
        }),
        (None, None) => None,
        _ => {
            error!(
                "partial Temporal mTLS config ignored; set TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY together"
            );
            None
        }
    };

    Some(TlsOptions {
        server_root_ca_cert,
        domain,
        client_tls_options,
    })
}

fn env_bytes(name: &str) -> Option<Vec<u8>> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .map(|value| value.into_bytes())
}

fn temporal_target_url(address: &str, tls_default: bool) -> Result<url::Url> {
    if address.starts_with("http://") || address.starts_with("https://") {
        return Ok(url::Url::parse(address)?);
    }
    let scheme = if tls_default { "https" } else { "http" };
    Ok(url::Url::parse(&format!("{scheme}://{address}"))?)
}

fn synthetic_generate(body: &JsonValue) -> String {
    let prompt = extract_prompt(body);
    let response = if prompt.is_empty() {
        "demo model response: admitted request from Rust Temporal worker".to_string()
    } else {
        format!(
            "demo model response: admitted request from Rust Temporal worker: {}",
            summarize(&prompt, 24)
        )
    };
    response
        .split_whitespace()
        .take(max_tokens(body).max(1))
        .collect::<Vec<_>>()
        .join(" ")
}

fn extract_prompt(body: &JsonValue) -> String {
    if let Some(messages) = body.get("messages").and_then(JsonValue::as_array) {
        return messages
            .iter()
            .filter_map(|message| {
                let role = message.get("role")?.as_str()?;
                let content = message.get("content")?.as_str()?;
                Some(format!("{role}: {content}"))
            })
            .collect::<Vec<_>>()
            .join("\n");
    }
    body.get("prompt")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string()
}

fn summarize(text: &str, limit: usize) -> String {
    text.split_whitespace()
        .take(limit)
        .collect::<Vec<_>>()
        .join(" ")
}

fn max_tokens(body: &JsonValue) -> usize {
    body.get("max_tokens")
        .and_then(JsonValue::as_u64)
        .unwrap_or(64) as usize
}

fn estimate_tokens(text: &str) -> u64 {
    text.split_whitespace().count().max(1) as u64
}

fn estimate_stream_delta_tokens(text: &str) -> u64 {
    if text.is_empty() {
        0
    } else {
        estimate_tokens(text)
    }
}

fn streamed_delta_text(data: &str) -> Result<String> {
    let value: JsonValue = serde_json::from_str(data)?;
    Ok(value
        .get("choices")
        .and_then(JsonValue::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("delta"))
        .and_then(|delta| delta.get("content"))
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string())
}

fn drain_sse_events(buffer: &mut String) -> Vec<String> {
    let mut events = Vec::new();
    while let Some(event_end) = buffer.find("\n\n") {
        let raw_event = buffer[..event_end].to_string();
        buffer.drain(..event_end + 2);
        let data = raw_event
            .lines()
            .filter_map(|line| line.trim_end_matches('\r').strip_prefix("data:"))
            .map(str::trim)
            .collect::<Vec<_>>()
            .join("\n");
        if !data.is_empty() {
            events.push(data);
        }
    }
    events
}
