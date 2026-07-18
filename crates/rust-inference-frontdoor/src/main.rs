use std::{collections::HashMap, collections::HashSet, env, fs, net::SocketAddr, time::Instant};

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
use generation_quota_limiter::{InMemoryQuotaLimiter, QuotaCheck, QuotaLimits};
use prost::Message;
use redis::{AsyncCommands, Value as RedisValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use temporalio_client::{
    Client, ClientOptions, ClientTlsOptions, Connection, ConnectionOptions, TlsOptions,
    grpc::WorkflowService, tonic::IntoRequest,
};
use temporalio_common::protos::temporal::api::{
    common::v1::{Payload, Payloads, WorkflowType},
    taskqueue::v1::TaskQueue,
    workflowservice::v1::StartWorkflowExecutionRequest,
};
use token_budget_engine::{
    ApproxTokenCounter, BudgetPolicy, BudgetRequest, ChatMessage, ModelBudget, Prompt,
    decide_budget,
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info, warn};
use uuid::Uuid;

const REQUEST_STREAM: &str = "inference:requests";

#[derive(Clone)]
struct AppState {
    redis: redis::Client,
    model: ModelBudget,
    api_keys: HashSet<String>,
    quota: InMemoryQuotaLimiter,
    temporal: Option<TemporalConfig>,
}

#[derive(Debug, Deserialize)]
struct ServiceConfig {
    frontend: FrontendConfig,
    broker: BrokerConfig,
    model: ModelConfig,
    auth: AuthConfig,
    quota: QuotaConfig,
}

#[derive(Debug, Deserialize)]
struct FrontendConfig {
    bind_addr: String,
}

#[derive(Debug, Deserialize)]
struct BrokerConfig {
    url: String,
}

#[derive(Debug, Deserialize)]
struct ModelConfig {
    id: String,
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

#[derive(Debug, Deserialize, Serialize, Clone)]
struct OpenAiRequest {
    model: Option<String>,
    messages: Option<Vec<ChatMessage>>,
    prompt: Option<Value>,
    max_tokens: Option<usize>,
    temperature: Option<f32>,
    stream: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RpcGenerateRequest {
    model: Option<String>,
    messages: Option<Vec<ChatMessage>>,
    prompt: Option<String>,
    max_tokens: Option<usize>,
    temperature: Option<f32>,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoChatMessage {
    #[prost(string, tag = "1")]
    role: String,
    #[prost(string, tag = "2")]
    content: String,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoGenerateRequest {
    #[prost(string, tag = "1")]
    model: String,
    #[prost(message, repeated, tag = "2")]
    messages: Vec<ProtoChatMessage>,
    #[prost(string, tag = "3")]
    prompt: String,
    #[prost(uint64, tag = "4")]
    max_tokens: u64,
    #[prost(float, tag = "5")]
    temperature: f32,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoGenerateResponse {
    #[prost(string, tag = "1")]
    request_id: String,
    #[prost(string, tag = "2")]
    model: String,
    #[prost(string, tag = "3")]
    text: String,
    #[prost(uint64, tag = "4")]
    prompt_tokens: u64,
    #[prost(uint64, tag = "5")]
    generated_tokens: u64,
    #[prost(string, tag = "6")]
    finish_reason: String,
}

#[derive(Clone, PartialEq, Message)]
struct ProtoGenerateChunk {
    #[prost(string, tag = "1")]
    request_id: String,
    #[prost(string, tag = "2")]
    model: String,
    #[prost(string, tag = "3")]
    delta: String,
    #[prost(uint64, tag = "4")]
    generated_tokens: u64,
    #[prost(string, tag = "5")]
    finish_reason: String,
}

#[derive(Debug, Serialize)]
struct GenerationJob {
    request_id: String,
    upstream_path: String,
    response_stream: String,
    body: Value,
    cache: CacheMetadata,
}

#[derive(Clone, Debug)]
struct TemporalConfig {
    address: String,
    namespace: String,
    task_queue: String,
    api_key: Option<String>,
    tls: Option<TlsOptions>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
struct CacheMetadata {
    cache_key: String,
    model: String,
    prefix_tokens_estimate: u64,
    cache_policy: CachePolicy,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CachePolicy {
    PrefixAffinity,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseEvent {
    Chunk { data: String, token_estimate: u64 },
    Done { generated_tokens: u64 },
    Error { message: String },
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let config = load_config()?;
    let state = AppState {
        redis: redis::Client::open(env::var("REDIS_URL").unwrap_or(config.broker.url.clone()))?,
        model: ModelBudget {
            model: env::var("MODEL_ID").unwrap_or(config.model.id.clone()),
            context_limit: env_usize("MODEL_CONTEXT_LIMIT", config.model.context_limit),
            default_max_tokens: env_usize(
                "MODEL_DEFAULT_MAX_TOKENS",
                config.model.default_max_tokens,
            ),
            max_output_tokens: env_usize("MODEL_MAX_OUTPUT_TOKENS", config.model.max_output_tokens),
        },
        api_keys: api_keys_from_env().unwrap_or_else(|| {
            config
                .auth
                .api_keys
                .into_iter()
                .map(|key| key.trim().to_string())
                .filter(|key| !key.is_empty())
                .collect()
        }),
        quota: InMemoryQuotaLimiter::new(QuotaLimits {
            requests_per_minute: env_u64(
                "QUOTA_REQUESTS_PER_MINUTE",
                config.quota.requests_per_minute,
            ),
            prompt_tokens_per_minute: env_u64(
                "QUOTA_PROMPT_TOKENS_PER_MINUTE",
                config.quota.prompt_tokens_per_minute,
            ),
            generated_tokens_per_minute: env_u64(
                "QUOTA_GENERATED_TOKENS_PER_MINUTE",
                config.quota.generated_tokens_per_minute,
            ),
        }),
        temporal: load_temporal_config(),
    };

    let addr: SocketAddr = env::var("FRONTEND_BIND_ADDR")
        .unwrap_or(config.frontend.bind_addr)
        .parse()
        .context("invalid frontend bind_addr")?;
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
    let request = match decode_proto_request(body) {
        Ok(request) => request,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let request = rpc_to_openai_request(request, false, &state.model.model);
    handle_rpc_generation(state, headers, request, false).await
}

async fn rpc_stream_generate(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let request = match decode_proto_request(body) {
        Ok(request) => request,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let request = rpc_to_openai_request(request, true, &state.model.model);
    handle_rpc_generation(state, headers, request, true).await
}

async fn handle_rpc_generation(
    state: AppState,
    headers: HeaderMap,
    mut request: OpenAiRequest,
    response_streaming: bool,
) -> Response {
    let request_id = Uuid::new_v4();
    let started = Instant::now();
    let Some(api_key) = bearer_token(&headers) else {
        return error_response(StatusCode::UNAUTHORIZED, "missing bearer token");
    };
    if !state.api_keys.contains(&api_key) {
        return error_response(StatusCode::FORBIDDEN, "invalid API key");
    }

    let prompt = match request_prompt(&request) {
        Ok(prompt) => prompt,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, message),
    };
    let budget = match decide_budget(
        BudgetRequest {
            prompt,
            requested_max_tokens: request.max_tokens,
            policy: BudgetPolicy::StrictReject,
            model: state.model.clone(),
        },
        &ApproxTokenCounter,
    ) {
        Ok(decision) => decision,
        Err(error) => return error_response(StatusCode::BAD_REQUEST, error.to_string()),
    };
    if !budget.admitted {
        warn!(%request_id, reason = %budget.reason, "rejected oversized prompt");
        return error_response(StatusCode::BAD_REQUEST, budget.reason);
    }

    request.max_tokens = Some(budget.admitted_max_tokens);
    let cache = cache_metadata(&request, &state.model.model, budget.prompt_tokens as u64);
    let reservation = match state
        .quota
        .reserve(QuotaCheck {
            api_key,
            prompt_tokens: budget.prompt_tokens as u64,
            reserved_generated_tokens: budget.admitted_max_tokens as u64,
        })
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => return error_response(StatusCode::TOO_MANY_REQUESTS, error.to_string()),
    };

    let response_stream = format!("inference:responses:{request_id}");
    let upstream_path = if response_streaming {
        "/inference.v1.InferenceService/StreamGenerate"
    } else {
        "/inference.v1.InferenceService/Generate"
    };
    let dispatch_result = match dispatch_generation(
        &state,
        request_id,
        upstream_path,
        &response_stream,
        &request,
        cache.clone(),
        response_streaming,
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            let _ = state.quota.cancel(reservation.id).await;
            error!(%request_id, %error, "failed to dispatch generation job");
            return error_response(StatusCode::BAD_GATEWAY, "failed to dispatch generation job");
        }
    };
    info!(
        %request_id,
        dispatch = %dispatch_result.kind,
        dispatch_id = %dispatch_result.id,
        response_stream = %response_stream,
        cache_key = %cache.cache_key,
        prefix_tokens_estimate = cache.prefix_tokens_estimate,
        cache_policy = ?cache.cache_policy,
        "dispatched generation job"
    );

    if response_streaming {
        connect_stream_from_redis(state, response_stream, reservation.id, request_id, cache)
    } else {
        connect_unary_from_redis(
            state,
            response_stream,
            reservation.id,
            request_id,
            started,
            budget.prompt_tokens as u64,
            cache,
        )
        .await
    }
}

async fn connect_unary_from_redis(
    state: AppState,
    response_stream: String,
    reservation_id: Uuid,
    request_id: Uuid,
    started: Instant,
    prompt_tokens: u64,
    cache: CacheMetadata,
) -> Response {
    let mut conn = match state.redis.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(error) => {
            let _ = state.quota.cancel(reservation_id).await;
            error!(%request_id, %error, "failed to connect to Valkey");
            return error_response(StatusCode::BAD_GATEWAY, "failed to connect to Valkey");
        }
    };

    let mut last_id = "0-0".to_string();
    let mut text = String::new();
    let generated_tokens = loop {
        match read_event(&mut conn, &response_stream, &last_id, 30_000).await {
            Ok(Some((
                id,
                ResponseEvent::Chunk {
                    data,
                    token_estimate: _,
                },
            ))) => {
                last_id = id;
                if let Ok(value) = serde_json::from_str::<Value>(&data) {
                    text.push_str(&extract_text(&value));
                }
            }
            Ok(Some((_, ResponseEvent::Done { generated_tokens }))) => break generated_tokens,
            Ok(Some((_, ResponseEvent::Error { message }))) => {
                let _ = state.quota.cancel(reservation_id).await;
                return error_response(StatusCode::BAD_GATEWAY, message);
            }
            Ok(None) => {
                let _ = state.quota.cancel(reservation_id).await;
                return error_response(StatusCode::GATEWAY_TIMEOUT, "generation timed out");
            }
            Err(error) => {
                let _ = state.quota.cancel(reservation_id).await;
                error!(%request_id, %error, "failed reading Valkey response stream");
                return error_response(StatusCode::BAD_GATEWAY, "failed reading response stream");
            }
        }
    };

    let refund = state
        .quota
        .reconcile(reservation_id, generated_tokens)
        .await
        .unwrap_or(0);
    info!(
        %request_id,
        generated_tokens,
        refunded_tokens = refund,
        latency_ms = started.elapsed().as_millis(),
        cache_key = %cache.cache_key,
        prefix_tokens_estimate = cache.prefix_tokens_estimate,
        "completed Connect unary generation request"
    );

    let body = ProtoGenerateResponse {
        request_id: request_id.to_string(),
        model: state.model.model,
        text,
        prompt_tokens,
        generated_tokens,
        finish_reason: "stop".to_string(),
    };
    let mut response = Response::new(Body::from(encode_proto(&body)));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/proto"),
    );
    attach_quota_headers(
        response.headers_mut(),
        reservation_id,
        generated_tokens,
        refund,
    );
    response
}

fn connect_stream_from_redis(
    state: AppState,
    response_stream: String,
    reservation_id: Uuid,
    request_id: Uuid,
    cache: CacheMetadata,
) -> Response {
    let body_stream = stream! {
        let started = Instant::now();
        let mut conn = match state.redis.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(error) => {
                let _ = state.quota.cancel(reservation_id).await;
                error!(%request_id, %error, "failed to connect to Valkey");
                yield Ok::<Bytes, std::convert::Infallible>(connect_error_envelope("failed to connect to Valkey"));
                return;
            }
        };

        let mut last_id = "0-0".to_string();
        let mut generated_so_far = 0;
        loop {
            match read_event(&mut conn, &response_stream, &last_id, 300_000).await {
                Ok(Some((
                    id,
                    ResponseEvent::Chunk {
                        data,
                        token_estimate,
                    },
                ))) => {
                    last_id = id;
                    if data == "[DONE]" {
                        continue;
                    }
                    let delta = serde_json::from_str::<Value>(&data)
                        .map(|value| extract_text(&value))
                        .unwrap_or_default();
                    generated_so_far += token_estimate;
                    let chunk = ProtoGenerateChunk {
                        request_id: request_id.to_string(),
                        model: state.model.model.clone(),
                        delta,
                        generated_tokens: generated_so_far,
                        finish_reason: String::new(),
                    };
                    yield Ok(connect_envelope(&chunk, false));
                }
                Ok(Some((_, ResponseEvent::Done { generated_tokens }))) => {
                    let refund = state
                        .quota
                        .reconcile(reservation_id, generated_tokens)
                        .await
                        .unwrap_or(0);
                    info!(
                        %request_id,
                        generated_tokens,
                        refunded_tokens = refund,
                        latency_ms = started.elapsed().as_millis(),
                        cache_key = %cache.cache_key,
                        prefix_tokens_estimate = cache.prefix_tokens_estimate,
                        "completed Connect streaming generation request"
                    );
                    let done = ProtoGenerateChunk {
                        request_id: request_id.to_string(),
                        model: state.model.model.clone(),
                        delta: String::new(),
                        generated_tokens,
                        finish_reason: "stop".to_string(),
                    };
                    yield Ok(connect_envelope(&done, false));
                    return;
                }
                Ok(Some((_, ResponseEvent::Error { message }))) => {
                    let _ = state.quota.cancel(reservation_id).await;
                    yield Ok(connect_error_envelope(&message));
                    return;
                }
                Ok(None) => {
                    let _ = state.quota.cancel(reservation_id).await;
                    yield Ok(connect_error_envelope("generation timed out"));
                    return;
                }
                Err(error) => {
                    let _ = state.quota.cancel(reservation_id).await;
                    error!(%request_id, %error, "failed reading Valkey response stream");
                    yield Ok(connect_error_envelope("failed reading response stream"));
                    return;
                }
            }
        }
    };

    let mut response = Response::new(Body::from_stream(body_stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/connect+proto"),
    );
    response
}

fn rpc_to_openai_request(
    request: RpcGenerateRequest,
    response_streaming: bool,
    default_model: &str,
) -> OpenAiRequest {
    OpenAiRequest {
        model: request.model.or_else(|| Some(default_model.to_string())),
        messages: request.messages,
        prompt: request.prompt.map(Value::String),
        max_tokens: request.max_tokens,
        temperature: request.temperature,
        stream: Some(response_streaming),
    }
}

fn decode_proto_request(body: Bytes) -> Result<RpcGenerateRequest, String> {
    let request = ProtoGenerateRequest::decode(body).map_err(|error| error.to_string())?;
    Ok(RpcGenerateRequest {
        model: (!request.model.is_empty()).then_some(request.model),
        messages: (!request.messages.is_empty()).then_some(
            request
                .messages
                .into_iter()
                .map(|message| ChatMessage {
                    role: message.role,
                    content: message.content,
                })
                .collect(),
        ),
        prompt: (!request.prompt.is_empty()).then_some(request.prompt),
        max_tokens: (request.max_tokens > 0).then_some(request.max_tokens as usize),
        temperature: (request.temperature > 0.0).then_some(request.temperature),
    })
}

fn encode_proto<T: Message>(message: &T) -> Bytes {
    let mut body = BytesMut::with_capacity(message.encoded_len());
    message
        .encode(&mut body)
        .expect("encoding to BytesMut cannot fail");
    body.freeze()
}

fn extract_text(value: &Value) -> String {
    value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| {
            choice
                .get("message")
                .and_then(|message| message.get("content"))
                .or_else(|| choice.get("delta").and_then(|delta| delta.get("content")))
                .or_else(|| choice.get("text"))
        })
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn connect_envelope<T: Message>(message: &T, end_stream: bool) -> Bytes {
    let payload = encode_proto(message);
    let mut bytes = BytesMut::with_capacity(5 + payload.len());
    bytes.put_u8(if end_stream { 0b0000_0010 } else { 0 });
    bytes.put_u32(payload.len() as u32);
    bytes.extend_from_slice(&payload);
    bytes.freeze()
}

fn connect_error_envelope(message: &str) -> Bytes {
    let chunk = ProtoGenerateChunk {
        request_id: String::new(),
        model: String::new(),
        delta: message.to_string(),
        generated_tokens: 0,
        finish_reason: "error".to_string(),
    };
    connect_envelope(&chunk, true)
}

async fn enqueue_job(
    state: &AppState,
    request_id: Uuid,
    upstream_path: &str,
    response_stream: &str,
    request: &OpenAiRequest,
    cache: CacheMetadata,
) -> anyhow::Result<String> {
    let mut conn = state.redis.get_multiplexed_async_connection().await?;
    let job = GenerationJob {
        request_id: request_id.to_string(),
        upstream_path: upstream_path.to_string(),
        response_stream: response_stream.to_string(),
        body: serde_json::to_value(request)?,
        cache,
    };
    let payload = serde_json::to_string(&job)?;
    let entry_id: String = conn.xadd(REQUEST_STREAM, "*", &[("job", payload)]).await?;
    Ok(entry_id)
}

#[derive(Debug)]
struct DispatchResult {
    kind: &'static str,
    id: String,
}

async fn dispatch_generation(
    state: &AppState,
    request_id: Uuid,
    upstream_path: &str,
    response_stream: &str,
    request: &OpenAiRequest,
    cache: CacheMetadata,
    response_streaming: bool,
) -> anyhow::Result<DispatchResult> {
    if let Some(temporal) = &state.temporal {
        let workflow_id = start_temporal_workflow(
            temporal,
            request_id,
            upstream_path,
            response_stream,
            request,
            cache,
            response_streaming,
        )
        .await?;
        return Ok(DispatchResult {
            kind: "temporal",
            id: workflow_id,
        });
    }

    let entry_id = enqueue_job(
        state,
        request_id,
        upstream_path,
        response_stream,
        request,
        cache,
    )
    .await?;
    Ok(DispatchResult {
        kind: "valkey_stream",
        id: entry_id,
    })
}

async fn start_temporal_workflow(
    config: &TemporalConfig,
    request_id: Uuid,
    upstream_path: &str,
    response_stream: &str,
    request: &OpenAiRequest,
    cache: CacheMetadata,
    response_streaming: bool,
) -> anyhow::Result<String> {
    let job = GenerationJob {
        request_id: request_id.to_string(),
        upstream_path: upstream_path.to_string(),
        response_stream: response_stream.to_string(),
        body: serde_json::to_value(request)?,
        cache,
    };
    let workflow_name = if response_streaming {
        "GpuStreamGenerateWorkflow"
    } else {
        "GpuGenerateWorkflow"
    };
    let workflow_id = format!("inference-{request_id}");
    let use_tls = config.api_key.is_some() || config.tls.is_some();
    let target = temporal_target_url(&config.address, use_tls)?;
    let mut connection = ConnectionOptions::new(target)
        .identity("inference-frontend".to_string())
        .build();
    if let Some(api_key) = &config.api_key {
        connection.api_key = Some(api_key.clone());
    }
    connection.tls_options = config
        .tls
        .clone()
        .or_else(|| config.api_key.as_ref().map(|_| TlsOptions::default()));
    let connection = Connection::connect(connection).await?;
    let client = Client::new(
        connection,
        ClientOptions::new(config.namespace.clone()).build(),
    )?;
    let input = Payloads {
        payloads: vec![json_payload(&job)?],
    };
    client
        .clone()
        .start_workflow_execution(
            StartWorkflowExecutionRequest {
                namespace: config.namespace.clone(),
                workflow_id: workflow_id.clone(),
                workflow_type: Some(WorkflowType {
                    name: workflow_name.to_string(),
                }),
                task_queue: Some(TaskQueue {
                    name: config.task_queue.clone(),
                    ..Default::default()
                }),
                input: Some(input),
                request_id: Uuid::new_v4().to_string(),
                ..Default::default()
            }
            .into_request(),
        )
        .await?;
    Ok(workflow_id)
}

fn json_payload<T: Serialize>(value: &T) -> anyhow::Result<Payload> {
    Ok(Payload {
        metadata: HashMap::from([("encoding".to_string(), b"json/plain".to_vec())]),
        data: serde_json::to_vec(value)?,
        ..Default::default()
    })
}

fn temporal_target_url(address: &str, tls_default: bool) -> anyhow::Result<url::Url> {
    if address.starts_with("http://") || address.starts_with("https://") {
        return Ok(url::Url::parse(address)?);
    }
    let scheme = if tls_default { "https" } else { "http" };
    Ok(url::Url::parse(&format!("{scheme}://{address}"))?)
}

fn load_temporal_config() -> Option<TemporalConfig> {
    let enabled = env::var("TEMPORAL_ENABLED")
        .map(|value| value == "true" || value == "1")
        .unwrap_or(false);
    if !enabled {
        return None;
    }
    Some(TemporalConfig {
        address: env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| "127.0.0.1:7233".to_string()),
        namespace: env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string()),
        task_queue: env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| "inference-gpu".to_string()),
        api_key: env::var("TEMPORAL_API_KEY")
            .ok()
            .filter(|value| !value.is_empty()),
        tls: load_temporal_tls_options(),
    })
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
            warn!(
                "ignoring partial Temporal mTLS config; set TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY together"
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

fn cache_metadata(request: &OpenAiRequest, model: &str, prompt_tokens: u64) -> CacheMetadata {
    let prefix = prompt_prefix(request);
    CacheMetadata {
        cache_key: cache_key(model, &prefix),
        model: model.to_string(),
        prefix_tokens_estimate: prompt_tokens,
        cache_policy: CachePolicy::PrefixAffinity,
    }
}

fn cache_key(model: &str, prefix: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(model.as_bytes());
    hasher.update(b"\0");
    hasher.update(prefix.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn prompt_prefix(request: &OpenAiRequest) -> String {
    const MAX_PREFIX_CHARS: usize = 4096;
    let mut prefix = String::new();
    if let Some(messages) = &request.messages {
        for message in messages {
            prefix.push_str(&message.role);
            prefix.push('\n');
            prefix.push_str(&message.content);
            prefix.push('\n');
            if prefix.len() >= MAX_PREFIX_CHARS {
                break;
            }
        }
    } else if let Some(prompt) = &request.prompt {
        match prompt {
            Value::String(text) => prefix.push_str(text),
            Value::Array(items) => {
                for item in items {
                    if let Some(text) = item.as_str() {
                        prefix.push_str(text);
                        prefix.push('\n');
                    }
                    if prefix.len() >= MAX_PREFIX_CHARS {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    prefix.chars().take(MAX_PREFIX_CHARS).collect()
}

async fn read_event(
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    last_id: &str,
    block_ms: u64,
) -> anyhow::Result<Option<(String, ResponseEvent)>> {
    let reply: RedisValue = redis::cmd("XREAD")
        .arg("BLOCK")
        .arg(block_ms)
        .arg("COUNT")
        .arg(1)
        .arg("STREAMS")
        .arg(stream)
        .arg(last_id)
        .query_async(conn)
        .await?;
    parse_response_event(reply)
}

fn parse_response_event(reply: RedisValue) -> anyhow::Result<Option<(String, ResponseEvent)>> {
    let RedisValue::Array(streams) = reply else {
        return Ok(None);
    };
    let Some(RedisValue::Array(stream)) = streams.first() else {
        return Ok(None);
    };
    let Some(RedisValue::Array(entries)) = stream.get(1) else {
        return Ok(None);
    };
    let Some(RedisValue::Array(entry)) = entries.first() else {
        return Ok(None);
    };
    let Some(RedisValue::BulkString(id)) = entry.first() else {
        return Ok(None);
    };
    let Some(RedisValue::Array(fields)) = entry.get(1) else {
        return Ok(None);
    };

    let mut payload = None;
    for pair in fields.chunks(2) {
        if let [RedisValue::BulkString(key), RedisValue::BulkString(value)] = pair {
            if key == b"event" {
                payload = Some(String::from_utf8(value.clone())?);
            }
        }
    }

    let Some(payload) = payload else {
        return Ok(None);
    };
    Ok(Some((
        String::from_utf8(id.clone())?,
        serde_json::from_str(&payload)?,
    )))
}

fn request_prompt(request: &OpenAiRequest) -> Result<Prompt, String> {
    if let Some(messages) = &request.messages {
        return Ok(Prompt::Chat(messages.clone()));
    }
    let Some(prompt) = &request.prompt else {
        return Err("either messages or prompt is required".to_string());
    };
    match prompt {
        Value::String(text) => Ok(Prompt::Text(text.clone())),
        Value::Array(items) => Ok(Prompt::Text(
            items
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("\n"),
        )),
        _ => Err("prompt must be a string or array of strings".to_string()),
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(ToString::to_string)
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response {
    (
        status,
        Json(ErrorBody {
            error: ErrorMessage {
                message: message.into(),
                kind: "invalid_request_error".to_string(),
            },
        }),
    )
        .into_response()
}

fn attach_quota_headers(
    headers: &mut HeaderMap,
    reservation_id: Uuid,
    generated: u64,
    refunded: u64,
) {
    headers.insert(
        "x-reservation-id",
        HeaderValue::from_str(&reservation_id.to_string()).unwrap(),
    );
    headers.insert(
        "x-generated-tokens",
        HeaderValue::from_str(&generated.to_string()).unwrap(),
    );
    headers.insert(
        "x-refunded-tokens",
        HeaderValue::from_str(&refunded.to_string()).unwrap(),
    );
}

fn load_config() -> Result<ServiceConfig> {
    let path = env::var("CONFIG_FILE").unwrap_or_else(|_| "config/local.yaml".to_string());
    let contents = fs::read_to_string(&path).with_context(|| format!("read config file {path}"))?;
    serde_yaml::from_str(&contents).with_context(|| format!("parse config file {path}"))
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chat_request(content: &str) -> OpenAiRequest {
        OpenAiRequest {
            model: Some("test-model".to_string()),
            messages: Some(vec![ChatMessage {
                role: "user".to_string(),
                content: content.to_string(),
            }]),
            prompt: None,
            max_tokens: Some(32),
            temperature: Some(0.0),
            stream: Some(false),
        }
    }

    #[test]
    fn cache_key_is_stable_for_same_prompt_and_model() {
        let first = cache_metadata(&chat_request("same prefix"), "model-a", 3);
        let second = cache_metadata(&chat_request("same prefix"), "model-a", 3);
        assert_eq!(first.cache_key, second.cache_key);
        assert_eq!(first.prefix_tokens_estimate, 3);
        assert_eq!(first.cache_policy, CachePolicy::PrefixAffinity);
    }

    #[test]
    fn cache_key_changes_with_model() {
        let first = cache_metadata(&chat_request("same prefix"), "model-a", 3);
        let second = cache_metadata(&chat_request("same prefix"), "model-b", 3);
        assert_ne!(first.cache_key, second.cache_key);
    }

    #[test]
    fn cache_key_changes_with_prompt_prefix() {
        let first = cache_metadata(&chat_request("first prefix"), "model-a", 3);
        let second = cache_metadata(&chat_request("second prefix"), "model-a", 3);
        assert_ne!(first.cache_key, second.cache_key);
    }

    #[test]
    fn cache_key_ignores_streaming_mode_for_same_prompt() {
        let mut unary = chat_request("same prefix");
        unary.stream = Some(false);
        let mut streaming = chat_request("same prefix");
        streaming.stream = Some(true);
        assert_eq!(
            cache_metadata(&unary, "model-a", 3).cache_key,
            cache_metadata(&streaming, "model-a", 3).cache_key
        );
    }
}
