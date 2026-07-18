use std::{collections::HashSet, env, fs, net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use async_stream::stream;
use axum::{
    Json, Router,
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::{BufMut, Bytes, BytesMut};
use inference_streams::{
    QuotaLimits, SCHEMA_VERSION, StreamsClient, StreamsConfig,
    api::{GenerateRequest, GenerateResponse, GenerationEvent, SubscribeGenerationRequest},
    wire::{self, CacheMetadata, GenerationJob},
};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use token_budget_engine::{
    ApproxTokenCounter, BudgetPolicy, BudgetRequest, ChatMessage, ModelBudget, Prompt,
    decide_budget,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    streams: StreamsClient,
    model: ModelConfig,
    api_keys: HashSet<String>,
    quota: QuotaLimits,
    generation_timeout: Duration,
}

#[derive(Debug, Deserialize)]
struct ServiceConfig {
    frontend: FrontendConfig,
    broker: BrokerConfig,
    model: ModelConfig,
    auth: AuthConfig,
    quota: QuotaConfig,
    #[serde(default)]
    streams: StreamSettings,
}

#[derive(Debug, Deserialize)]
struct FrontendConfig {
    bind_addr: String,
}

#[derive(Debug, Deserialize)]
struct BrokerConfig {
    url: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ModelConfig {
    id: String,
    #[serde(default = "default_revision")]
    revision: String,
    #[serde(default)]
    endpoint: String,
    #[serde(default)]
    tokenizer_id: String,
    #[serde(default = "default_revision")]
    tokenizer_revision: String,
    context_limit: usize,
    default_max_tokens: usize,
    max_output_tokens: usize,
}

#[derive(Debug, Deserialize)]
struct AuthConfig {
    api_keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct QuotaConfig {
    requests_per_minute: u64,
    prompt_tokens_per_minute: u64,
    generated_tokens_per_minute: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
struct StreamSettings {
    active_ttl_seconds: u64,
    result_retention_seconds: u64,
    lease_seconds: u64,
    generation_timeout_seconds: u64,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            active_ttl_seconds: 3_600,
            result_retention_seconds: 600,
            lease_seconds: 60,
            generation_timeout_seconds: 300,
        }
    }
}

#[derive(Debug, Serialize)]
struct RuntimeRequest {
    model: String,
    messages: Option<Vec<ChatMessage>>,
    prompt: Option<String>,
    max_tokens: usize,
    temperature: f32,
    stop: Vec<String>,
    stream: bool,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: ErrorMessage,
}

#[derive(Debug, Serialize)]
struct ErrorMessage {
    message: String,
    #[serde(rename = "type")]
    kind: String,
}

struct Admission {
    request_id: String,
    request_entry_id: String,
    reservation_id: String,
    prompt_tokens: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let config = load_config()?;
    let broker_url = env::var("VALKEY_URL").unwrap_or(config.broker.url);
    let streams = StreamsClient::connect(
        &broker_url,
        StreamsConfig {
            active_ttl: Duration::from_secs(config.streams.active_ttl_seconds),
            result_retention: Duration::from_secs(config.streams.result_retention_seconds),
            lease_duration: Duration::from_secs(config.streams.lease_seconds),
        },
    )
    .await?;
    let addr: SocketAddr = env::var("FRONTEND_BIND_ADDR")
        .unwrap_or(config.frontend.bind_addr)
        .parse()
        .context("invalid frontend bind_addr")?;
    let state = AppState {
        streams,
        model: config.model,
        api_keys: api_keys_from_env().unwrap_or_else(|| {
            config
                .auth
                .api_keys
                .into_iter()
                .map(|key| key.trim().to_string())
                .filter(|key| !key.is_empty())
                .collect()
        }),
        quota: QuotaLimits {
            requests_per_minute: config.quota.requests_per_minute,
            prompt_tokens_per_minute: config.quota.prompt_tokens_per_minute,
            generated_tokens_per_minute: config.quota.generated_tokens_per_minute,
        },
        generation_timeout: Duration::from_secs(config.streams.generation_timeout_seconds),
    };

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/inference.v1.InferenceService/Generate",
            post(rpc_generate),
        )
        .route(
            "/inference.v1.InferenceService/StreamGenerate",
            post(rpc_stream_generate),
        )
        .route(
            "/inference.v1.InferenceService/SubscribeGeneration",
            post(rpc_subscribe_generation),
        )
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    info!(%addr, "starting inference frontend");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn rpc_generate(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let request = match GenerateRequest::decode(body) {
        Ok(request) => request,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, error.to_string()),
    };
    let admission = match admit_and_dispatch(&state, &headers, request).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    collect_generation(state, admission).await
}

async fn rpc_stream_generate(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request = match GenerateRequest::decode(body) {
        Ok(request) => request,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, error.to_string()),
    };
    let admission = match admit_and_dispatch(&state, &headers, request).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    stream_generation(state, admission.request_id, None)
}

async fn rpc_subscribe_generation(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Err(response) = authenticate(&state, &headers) {
        return *response;
    }
    let request = match SubscribeGenerationRequest::decode(body) {
        Ok(request) => request,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, error.to_string()),
    };
    if request.request_id.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "request_id is required");
    }
    let cursor = request.after_event_id.filter(|value| !value.is_empty());
    if cursor
        .as_deref()
        .is_some_and(|value| !valid_stream_id(value))
    {
        return error_response(StatusCode::BAD_REQUEST, "invalid after_event_id");
    }
    match state.streams.status(&request.request_id).await {
        Ok(Some(_)) => stream_generation(state, request.request_id, cursor),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "generation is unknown or expired"),
        Err(error) => {
            error!(%error, "failed to inspect generation");
            error_response(StatusCode::BAD_GATEWAY, "failed to inspect generation")
        }
    }
}

async fn admit_and_dispatch(
    state: &AppState,
    headers: &HeaderMap,
    request: GenerateRequest,
) -> std::result::Result<Admission, Response> {
    let api_key = authenticate(state, headers).map_err(|response| *response)?;
    let prompt = request_prompt(&request)
        .map_err(|message| error_response(StatusCode::BAD_REQUEST, message))?;
    let model = if request.model.is_empty() {
        state.model.id.clone()
    } else {
        request.model.clone()
    };
    if model != state.model.id {
        return Err(error_response(
            StatusCode::NOT_FOUND,
            "model is not registered",
        ));
    }
    let budget = decide_budget(
        BudgetRequest {
            prompt,
            requested_max_tokens: (request.max_tokens > 0).then_some(request.max_tokens as usize),
            policy: BudgetPolicy::StrictReject,
            model: ModelBudget {
                model: model.clone(),
                context_limit: state.model.context_limit,
                default_max_tokens: state.model.default_max_tokens,
                max_output_tokens: state.model.max_output_tokens,
            },
        },
        &ApproxTokenCounter,
    )
    .map_err(|error| error_response(StatusCode::BAD_REQUEST, error.to_string()))?;
    if !budget.admitted {
        return Err(error_response(StatusCode::BAD_REQUEST, budget.reason));
    }

    let reservation = state
        .streams
        .reserve_quota(
            &api_key,
            budget.prompt_tokens as u64,
            budget.admitted_max_tokens as u64,
            state.quota,
        )
        .await
        .map_err(|error| error_response(StatusCode::TOO_MANY_REQUESTS, error.to_string()))?;
    let request_id = Uuid::new_v4().to_string();
    let runtime_request = RuntimeRequest {
        model: model.clone(),
        messages: (!request.messages.is_empty()).then(|| {
            request
                .messages
                .iter()
                .map(|message| ChatMessage {
                    role: message.role.clone(),
                    content: message.content.clone(),
                })
                .collect()
        }),
        prompt: (!request.prompt.is_empty()).then_some(request.prompt.clone()),
        max_tokens: budget.admitted_max_tokens,
        temperature: request.temperature,
        stop: request.stop.clone(),
        stream: true,
    };
    let prefix = prompt_prefix(&request);
    let job = GenerationJob {
        schema_version: SCHEMA_VERSION,
        request_id: request_id.clone(),
        model_id: model.clone(),
        model_revision: state.model.revision.clone(),
        model_endpoint: state.model.endpoint.clone(),
        request_json: serde_json::to_vec(&runtime_request).expect("runtime request serializes"),
        deadline_unix_ms: unix_ms() + state.generation_timeout.as_millis() as u64,
        reservation_id: reservation.id.clone(),
        cache: Some(CacheMetadata {
            cache_key: cache_key(&model, &prefix),
            prefix_tokens_estimate: budget.prompt_tokens as u64,
        }),
    };
    let cache_key = job
        .cache
        .as_ref()
        .map(|cache| cache.cache_key.clone())
        .unwrap_or_default();
    let entry_id = match state.streams.publish_generation(&job).await {
        Ok(entry_id) => entry_id,
        Err(error) => {
            let _ = state.streams.cancel_quota(&reservation.id).await;
            error!(%request_id, %error, "failed to publish generation");
            return Err(error_response(
                StatusCode::BAD_GATEWAY,
                "failed to dispatch generation",
            ));
        }
    };
    info!(
        %request_id,
        request_entry_id = %entry_id,
        %cache_key,
        prompt_tokens = budget.prompt_tokens,
        requested_output_tokens = budget.admitted_max_tokens,
        tokenizer_id = %state.model.tokenizer_id,
        tokenizer_revision = %state.model.tokenizer_revision,
        token_counting = "approximate_preflight_runtime_authoritative",
        "admitted generation"
    );
    Ok(Admission {
        request_id,
        request_entry_id: entry_id,
        reservation_id: reservation.id,
        prompt_tokens: budget.prompt_tokens as u64,
    })
}

async fn collect_generation(state: AppState, admission: Admission) -> Response {
    let mut subscription = match state.streams.subscribe(&admission.request_id, None).await {
        Ok(Some(subscription)) => subscription,
        _ => return error_response(StatusCode::BAD_GATEWAY, "result stream unavailable"),
    };
    let mut text = String::new();
    let mut current_attempt = String::new();
    let (generated_tokens, finish_reason) = loop {
        match subscription.next(Duration::from_secs(300)).await {
            Ok(Some(entry)) => match entry.result.event {
                Some(wire::generation_result::Event::AttemptStarted(_)) => {
                    if !current_attempt.is_empty() && current_attempt != entry.result.attempt_id {
                        text.clear();
                    }
                    current_attempt = entry.result.attempt_id;
                }
                Some(wire::generation_result::Event::Token(token)) => {
                    text.push_str(&token.text);
                }
                Some(wire::generation_result::Event::Completed(completed)) => {
                    break (completed.generated_tokens, completed.finish_reason);
                }
                Some(wire::generation_result::Event::Failed(failed)) => {
                    return error_response(StatusCode::BAD_GATEWAY, failed.message);
                }
                None => {}
            },
            Ok(None) => return error_response(StatusCode::GATEWAY_TIMEOUT, "generation timed out"),
            Err(error) => {
                error!(request_id = %admission.request_id, %error, "result subscription failed");
                return error_response(StatusCode::BAD_GATEWAY, "result stream failed");
            }
        }
    };
    let body = GenerateResponse {
        request_id: admission.request_id,
        model: state.model.id,
        text,
        prompt_tokens: admission.prompt_tokens,
        generated_tokens,
        finish_reason,
    };
    let mut response = Response::new(Body::from(encode_proto(&body)));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/proto"),
    );
    response.headers_mut().insert(
        "x-request-entry-id",
        HeaderValue::from_str(&admission.request_entry_id).unwrap(),
    );
    response.headers_mut().insert(
        "x-reservation-id",
        HeaderValue::from_str(&admission.reservation_id).unwrap(),
    );
    response
}

fn stream_generation(state: AppState, request_id: String, cursor: Option<String>) -> Response {
    let body_stream = stream! {
        let mut subscription = match state.streams.subscribe(&request_id, cursor.as_deref()).await {
            Ok(Some(subscription)) => subscription,
            Ok(None) => {
                yield Ok::<Bytes, std::convert::Infallible>(connect_error_envelope(&request_id, "generation is unknown or expired"));
                return;
            }
            Err(error) => {
                error!(%request_id, %error, "failed to subscribe to result stream");
                yield Ok(connect_error_envelope(&request_id, "failed to subscribe to generation"));
                return;
            }
        };
        loop {
            match subscription.next(Duration::from_secs(300)).await {
                Ok(Some(entry)) => {
                    let attempt_id = entry.result.attempt_id.clone();
                    match entry.result.event {
                        Some(wire::generation_result::Event::AttemptStarted(_)) => continue,
                        Some(wire::generation_result::Event::Token(token)) => {
                            let event = GenerationEvent {
                                request_id: request_id.clone(),
                                model: state.model.id.clone(),
                                delta: token.text,
                                generated_tokens: token.cumulative_generated_tokens,
                                finish_reason: String::new(),
                                event_id: entry.event_id,
                                attempt_id,
                                prompt_tokens: 0,
                                terminal: false,
                            };
                            yield Ok(connect_envelope(&event, false));
                        }
                        Some(wire::generation_result::Event::Completed(done)) => {
                            let event = GenerationEvent {
                                request_id: request_id.clone(),
                                model: state.model.id.clone(),
                                delta: String::new(),
                                generated_tokens: done.generated_tokens,
                                finish_reason: done.finish_reason,
                                event_id: entry.event_id,
                                attempt_id,
                                prompt_tokens: done.prompt_tokens,
                                terminal: true,
                            };
                            yield Ok(connect_envelope(&event, false));
                            return;
                        }
                        Some(wire::generation_result::Event::Failed(failed)) => {
                            let event = GenerationEvent {
                                request_id: request_id.clone(),
                                model: state.model.id.clone(),
                                delta: failed.message,
                                generated_tokens: 0,
                                finish_reason: failed.code,
                                event_id: entry.event_id,
                                attempt_id,
                                prompt_tokens: 0,
                                terminal: true,
                            };
                            yield Ok(connect_envelope(&event, false));
                            return;
                        }
                        None => {}
                    }
                }
                Ok(None) => {
                    yield Ok(connect_error_envelope(&request_id, "generation timed out"));
                    return;
                }
                Err(error) => {
                    error!(%request_id, %error, "failed reading result stream");
                    yield Ok(connect_error_envelope(&request_id, "failed reading generation"));
                    return;
                }
            }
        }
    };
    let mut response = Response::new(Body::from_stream(body_stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/connect+proto"),
    );
    response
}

fn authenticate(
    state: &AppState,
    headers: &HeaderMap,
) -> std::result::Result<String, Box<Response>> {
    let token = bearer_token(headers).ok_or_else(|| {
        Box::new(error_response(
            StatusCode::UNAUTHORIZED,
            "missing bearer token",
        ))
    })?;
    if !state.api_keys.contains(&token) {
        return Err(Box::new(error_response(
            StatusCode::FORBIDDEN,
            "invalid API key",
        )));
    }
    Ok(token)
}

fn request_prompt(request: &GenerateRequest) -> std::result::Result<Prompt, String> {
    if !request.messages.is_empty() {
        return Ok(Prompt::Chat(
            request
                .messages
                .iter()
                .map(|message| ChatMessage {
                    role: message.role.clone(),
                    content: message.content.clone(),
                })
                .collect(),
        ));
    }
    if request.prompt.is_empty() {
        return Err("either messages or prompt is required".to_string());
    }
    Ok(Prompt::Text(request.prompt.clone()))
}

fn prompt_prefix(request: &GenerateRequest) -> String {
    const MAX_PREFIX_CHARS: usize = 4096;
    let value = if request.messages.is_empty() {
        request.prompt.clone()
    } else {
        request
            .messages
            .iter()
            .map(|message| format!("{}\n{}", message.role, message.content))
            .collect::<Vec<_>>()
            .join("\n")
    };
    value.chars().take(MAX_PREFIX_CHARS).collect()
}

fn cache_key(model: &str, prefix: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(model.as_bytes());
    hasher.update(b"\0");
    hasher.update(prefix.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn encode_proto<T: Message>(message: &T) -> Bytes {
    let mut body = BytesMut::with_capacity(message.encoded_len());
    message.encode(&mut body).expect("encode protobuf");
    body.freeze()
}

fn connect_envelope<T: Message>(message: &T, end_stream: bool) -> Bytes {
    let payload = encode_proto(message);
    let mut bytes = BytesMut::with_capacity(5 + payload.len());
    bytes.put_u8(if end_stream { 0b0000_0010 } else { 0 });
    bytes.put_u32(payload.len() as u32);
    bytes.extend_from_slice(&payload);
    bytes.freeze()
}

fn connect_error_envelope(request_id: &str, message: &str) -> Bytes {
    connect_envelope(
        &GenerationEvent {
            request_id: request_id.to_string(),
            delta: message.to_string(),
            finish_reason: "error".to_string(),
            terminal: true,
            ..Default::default()
        },
        true,
    )
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(ToString::to_string)
}

fn valid_stream_id(value: &str) -> bool {
    let Some((milliseconds, sequence)) = value.split_once('-') else {
        return false;
    };
    milliseconds.parse::<u64>().is_ok() && sequence.parse::<u64>().is_ok()
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorBody {
            error: ErrorMessage {
                message: message.into(),
                kind: "inference_error".to_string(),
            },
        }),
    )
        .into_response()
}

fn load_config() -> Result<ServiceConfig> {
    let path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config/local.yaml".to_string());
    let contents = fs::read_to_string(&path).with_context(|| format!("read config file {path}"))?;
    serde_yaml::from_str(&contents).with_context(|| format!("parse config file {path}"))
}

fn api_keys_from_env() -> Option<HashSet<String>> {
    let raw = env::var("API_KEYS").ok()?;
    Some(
        raw.split(',')
            .map(|key| key.trim().to_string())
            .filter(|key| !key.is_empty())
            .collect(),
    )
}

fn default_revision() -> String {
    "main".to_string()
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use inference_streams::api::ChatMessage as ProtoChatMessage;

    #[test]
    fn cursor_must_be_a_stream_id() {
        assert!(valid_stream_id("123-0"));
        assert!(!valid_stream_id("123"));
        assert!(!valid_stream_id("newest"));
    }

    #[test]
    fn same_model_and_prompt_have_stable_cache_key() {
        let request = GenerateRequest {
            model: "model-a".into(),
            messages: vec![ProtoChatMessage {
                role: "user".into(),
                content: "hello".into(),
            }],
            ..Default::default()
        };
        let prefix = prompt_prefix(&request);
        assert_eq!(cache_key("model-a", &prefix), cache_key("model-a", &prefix));
        assert_ne!(cache_key("model-a", &prefix), cache_key("model-b", &prefix));
    }
}
