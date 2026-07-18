use std::{
    env, fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use redis::{AsyncCommands, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tracing::{error, info};

const REQUEST_STREAM: &str = "inference:requests";
const REQUEST_GROUP: &str = "generation-workers";
const ACTIVE_GENERATIONS_KEY: &str = "inference:active_generations";
const LAST_GENERATION_FINISHED_MS_KEY: &str = "inference:last_generation_finished_ms";

#[derive(Clone)]
struct WorkerConfig {
    backend: Backend,
    model: String,
    mlx_base_url: String,
    openai_base_url: String,
    synthetic_token_delay: Duration,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct ServiceConfig {
    broker: BrokerConfig,
    model: ModelConfig,
    backend: BackendConfig,
}

#[derive(Debug, Deserialize)]
struct BrokerConfig {
    url: String,
}

#[derive(Debug, Deserialize)]
struct ModelConfig {
    id: String,
}

#[derive(Debug, Deserialize)]
struct BackendConfig {
    kind: String,
    mlx_base_url: Option<String>,
    openai_base_url: Option<String>,
    synthetic_token_delay_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Backend {
    Synthetic,
    Mlx,
    OpenAi,
}

#[derive(Debug, Deserialize)]
struct GenerationJob {
    request_id: String,
    upstream_path: String,
    response_stream: String,
    body: JsonValue,
    #[serde(default)]
    cache: Option<CacheMetadata>,
}

#[derive(Debug, Deserialize)]
struct CacheMetadata {
    cache_key: String,
    model: String,
    prefix_tokens_estimate: u64,
    cache_policy: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseEvent {
    Chunk { data: String, token_estimate: u64 },
    Done { generated_tokens: u64 },
    Error { message: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let service_config = load_config()?;
    let broker_url = env::var("REDIS_URL").unwrap_or(service_config.broker.url);
    let broker = redis::Client::open(broker_url)?;
    let mut conn = broker.get_multiplexed_async_connection().await?;
    ensure_consumer_group(&mut conn).await?;
    let consumer =
        env::var("WORKER_ID").unwrap_or_else(|_| format!("worker-{}", uuid::Uuid::new_v4()));
    let config = WorkerConfig {
        backend: match env::var("BACKEND")
            .unwrap_or(service_config.backend.kind)
            .as_str()
        {
            "mlx" => Backend::Mlx,
            "openai" | "vllm" => Backend::OpenAi,
            _ => Backend::Synthetic,
        },
        model: env::var("MODEL_ID").unwrap_or(service_config.model.id),
        mlx_base_url: env::var("MLX_BASE_URL").unwrap_or_else(|_| {
            service_config
                .backend
                .mlx_base_url
                .unwrap_or_else(|| "http://127.0.0.1:18000".to_string())
        }),
        openai_base_url: env::var("OPENAI_BASE_URL").unwrap_or_else(|_| {
            service_config
                .backend
                .openai_base_url
                .unwrap_or_else(|| "http://127.0.0.1:8000".to_string())
        }),
        synthetic_token_delay: Duration::from_millis(
            service_config.backend.synthetic_token_delay_ms.unwrap_or(0),
        ),
        http: reqwest::Client::new(),
    };

    info!(
        model = %config.model,
        backend = ?config.backend,
        consumer = %consumer,
        "generation worker started"
    );
    loop {
        let reply: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(REQUEST_GROUP)
            .arg(&consumer)
            .arg("BLOCK")
            .arg(0)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(REQUEST_STREAM)
            .arg(">")
            .query_async(&mut conn)
            .await?;

        let Some((entry_id, job)) = parse_job(reply)? else {
            continue;
        };

        if let Err(error) = run_job(&mut conn, &config, job).await {
            error!(entry_id = %entry_id, %error, "generation job failed; leaving stream entry pending");
            continue;
        }
        let acked: i64 = redis::cmd("XACK")
            .arg(REQUEST_STREAM)
            .arg(REQUEST_GROUP)
            .arg(&entry_id)
            .query_async(&mut conn)
            .await?;
        info!(entry_id = %entry_id, acked, "acked generation job stream entry");
    }
}

async fn ensure_consumer_group(conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
    let result: redis::RedisResult<Value> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(REQUEST_STREAM)
        .arg(REQUEST_GROUP)
        .arg("0-0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) if error.to_string().contains("BUSYGROUP") => Ok(()),
        Err(error) => Err(error.into()),
    }
}

async fn run_job(
    conn: &mut redis::aio::MultiplexedConnection,
    config: &WorkerConfig,
    job: GenerationJob,
) -> Result<()> {
    info!(
        request_id = %job.request_id,
        upstream_path = %job.upstream_path,
        model = %config.model,
        backend = ?config.backend,
        cache_key = job.cache.as_ref().map(|cache| cache.cache_key.as_str()).unwrap_or("none"),
        prefix_tokens_estimate = job.cache.as_ref().map(|cache| cache.prefix_tokens_estimate).unwrap_or(0),
        cache_policy = job.cache.as_ref().and_then(|cache| cache.cache_policy.as_deref()).unwrap_or("none"),
        cache_model = job.cache.as_ref().map(|cache| cache.model.as_str()).unwrap_or("none"),
        "starting generation job"
    );

    if job
        .body
        .get("model")
        .and_then(JsonValue::as_str)
        .is_some_and(|model| model != config.model)
    {
        write_event(
            conn,
            &job.response_stream,
            ResponseEvent::Error {
                message: format!("unknown model; this worker only serves {}", config.model),
            },
        )
        .await?;
        return Ok(());
    }

    let _: i64 = conn.incr(ACTIVE_GENERATIONS_KEY, 1).await?;
    let result = if job
        .body
        .get("stream")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        stream_completion(conn, config, &job).await
    } else {
        write_completion(conn, config, &job).await
    };
    mark_generation_finished(conn).await?;
    result?;

    info!(request_id = %job.request_id, "finished demo generation job");
    Ok(())
}

async fn mark_generation_finished(conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
    let active: i64 = conn.decr(ACTIVE_GENERATIONS_KEY, 1).await?;
    let _: () = conn
        .set(LAST_GENERATION_FINISHED_MS_KEY, unix_timestamp_ms())
        .await?;
    info!(
        active_generations = active.max(0),
        "generation activity recorded"
    );
    Ok(())
}

async fn write_completion(
    conn: &mut redis::aio::MultiplexedConnection,
    config: &WorkerConfig,
    job: &GenerationJob,
) -> Result<()> {
    let prompt = extract_prompt(&job.body);
    let text = generate_text(config, &job.body, &prompt).await?;
    let generated_tokens = estimate_tokens(&text);
    let body = if job.upstream_path.contains("chat") {
        json!({
            "id": format!("chatcmpl-{}", job.request_id),
            "object": "chat.completion",
            "model": config.model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": estimate_tokens(&prompt),
                "completion_tokens": generated_tokens,
                "total_tokens": estimate_tokens(&prompt) + generated_tokens
            }
        })
    } else {
        json!({
            "id": format!("cmpl-{}", job.request_id),
            "object": "text_completion",
            "model": config.model,
            "choices": [{
                "index": 0,
                "text": text,
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": estimate_tokens(&prompt),
                "completion_tokens": generated_tokens,
                "total_tokens": estimate_tokens(&prompt) + generated_tokens
            }
        })
    };

    write_event(
        conn,
        &job.response_stream,
        ResponseEvent::Chunk {
            data: serde_json::to_string(&body)?,
            token_estimate: generated_tokens,
        },
    )
    .await?;
    write_event(
        conn,
        &job.response_stream,
        ResponseEvent::Done { generated_tokens },
    )
    .await?;
    Ok(())
}

async fn stream_completion(
    conn: &mut redis::aio::MultiplexedConnection,
    config: &WorkerConfig,
    job: &GenerationJob,
) -> Result<()> {
    let prompt = extract_prompt(&job.body);
    let text = generate_text(config, &job.body, &prompt).await?;
    let mut generated_tokens = 0;

    for token in text.split_whitespace() {
        let content = format!("{token} ");
        generated_tokens += 1;
        let chunk = if job.upstream_path.contains("chat") {
            json!({
                "id": format!("chatcmpl-{}", job.request_id),
                "object": "chat.completion.chunk",
                "model": config.model,
                "choices": [{
                    "index": 0,
                    "delta": {"content": content},
                    "finish_reason": null
                }]
            })
        } else {
            json!({
                "id": format!("cmpl-{}", job.request_id),
                "object": "text_completion.chunk",
                "model": config.model,
                "choices": [{
                    "index": 0,
                    "text": content,
                    "finish_reason": null
                }]
            })
        };
        write_event(
            conn,
            &job.response_stream,
            ResponseEvent::Chunk {
                data: serde_json::to_string(&chunk)?,
                token_estimate: 1,
            },
        )
        .await?;
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
    Ok(())
}

async fn generate_text(config: &WorkerConfig, body: &JsonValue, prompt: &str) -> Result<String> {
    match config.backend {
        Backend::Synthetic => Ok(synthetic_generate(prompt, max_tokens(body))),
        Backend::Mlx => generate_with_mlx(config, body).await,
        Backend::OpenAi => generate_with_openai(config, body).await,
    }
}

async fn generate_with_mlx(config: &WorkerConfig, body: &JsonValue) -> Result<String> {
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
        .post(format!("{}/v1/chat/completions", config.mlx_base_url))
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

    match body.get("prompt") {
        Some(JsonValue::String(prompt)) => prompt.clone(),
        Some(JsonValue::Array(items)) => items
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn synthetic_generate(prompt: &str, max_tokens: usize) -> String {
    let normalized = prompt
        .split_whitespace()
        .take(24)
        .collect::<Vec<_>>()
        .join(" ");
    let response = if normalized.is_empty() {
        "demo model response: no prompt provided".to_string()
    } else {
        format!("demo model response: admitted request, prompt summary: {normalized}")
    };
    response
        .split_whitespace()
        .take(max_tokens.max(1))
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

fn unix_timestamp_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn parse_job(reply: Value) -> Result<Option<(String, GenerationJob)>> {
    let Value::Array(streams) = reply else {
        return Ok(None);
    };
    let Some(Value::Array(stream)) = streams.first() else {
        return Ok(None);
    };
    let Some(Value::Array(entries)) = stream.get(1) else {
        return Ok(None);
    };
    let Some(Value::Array(entry)) = entries.first() else {
        return Ok(None);
    };
    let Some(Value::BulkString(id)) = entry.first() else {
        return Ok(None);
    };
    let Some(Value::Array(fields)) = entry.get(1) else {
        return Ok(None);
    };

    let mut payload = None;
    for pair in fields.chunks(2) {
        if let [Value::BulkString(key), Value::BulkString(value)] = pair {
            if key == b"job" {
                payload = Some(String::from_utf8(value.clone())?);
            }
        }
    }

    let Some(payload) = payload else {
        return Ok(None);
    };
    let entry_id = String::from_utf8(id.clone())?;
    Ok(Some((entry_id, serde_json::from_str(&payload)?)))
}

fn load_config() -> Result<ServiceConfig> {
    let path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config/local.yaml".to_string());
    let contents = fs::read_to_string(&path)?;
    Ok(serde_yaml::from_str(&contents)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_legacy_job_without_cache_metadata() {
        let raw = r#"{
          "request_id": "req-1",
          "upstream_path": "/inference.v1.InferenceService/Generate",
          "response_stream": "inference:responses:req-1",
          "body": {"model": "model-a", "prompt": "hello"}
        }"#;
        let job: GenerationJob = serde_json::from_str(raw).unwrap();
        assert!(job.cache.is_none());
    }

    #[test]
    fn deserializes_job_with_cache_metadata() {
        let raw = r#"{
          "request_id": "req-1",
          "upstream_path": "/inference.v1.InferenceService/Generate",
          "response_stream": "inference:responses:req-1",
          "body": {"model": "model-a", "prompt": "hello"},
          "cache": {
            "cache_key": "sha256:abc",
            "model": "model-a",
            "prefix_tokens_estimate": 4,
            "cache_policy": "prefix_affinity"
          }
        }"#;
        let job: GenerationJob = serde_json::from_str(raw).unwrap();
        let cache = job.cache.unwrap();
        assert_eq!(cache.cache_key, "sha256:abc");
        assert_eq!(cache.prefix_tokens_estimate, 4);
        assert_eq!(cache.cache_policy.as_deref(), Some("prefix_affinity"));
    }
}
