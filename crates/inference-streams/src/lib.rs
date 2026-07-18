use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use fred::prelude::{
    Builder as FredBuilder, Client as FredClient, ClientLike, Config as FredConfig,
    StreamsInterface,
};
use prost::Message;
use redis::{Value, aio::ConnectionManager};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

pub mod api {
    include!(concat!(env!("OUT_DIR"), "/inference.v1.rs"));
}

pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/inference.internal.v1.rs"));
}

pub const SCHEMA_VERSION: u32 = 1;
pub const REQUEST_STREAM: &str = "inference:requests";
pub const REQUEST_GROUP: &str = "inference-engines";

#[derive(Clone, Debug)]
pub struct StreamsConfig {
    pub active_ttl: Duration,
    pub result_retention: Duration,
    pub lease_duration: Duration,
}

impl Default for StreamsConfig {
    fn default() -> Self {
        Self {
            active_ttl: Duration::from_secs(3_600),
            result_retention: Duration::from_secs(600),
            lease_duration: Duration::from_secs(60),
        }
    }
}

#[derive(Clone)]
pub struct StreamsClient {
    client: redis::Client,
    fast: FredClient,
    commands: ConnectionManager,
    config: StreamsConfig,
}

#[derive(Debug)]
pub struct ConsumedJob {
    pub entry_id: String,
    pub job: wire::GenerationJob,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResultEntry {
    pub event_id: String,
    pub result: wire::GenerationResult,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenerationStatus {
    pub state: String,
    pub model_id: String,
    pub active_attempt_id: Option<String>,
    pub terminal_event_id: Option<String>,
}

pub struct JobConsumer {
    streams: StreamsClient,
    connection: redis::aio::MultiplexedConnection,
    consumer: String,
}

pub struct ResultSubscription {
    client: redis::Client,
    connection: redis::aio::MultiplexedConnection,
    stream: String,
    last_event_id: String,
}

#[derive(Clone, Copy, Debug)]
pub struct QuotaLimits {
    pub requests_per_minute: u64,
    pub prompt_tokens_per_minute: u64,
    pub generated_tokens_per_minute: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuotaReservation {
    pub id: String,
    pub remaining_requests: u64,
    pub remaining_prompt_tokens: u64,
    pub remaining_generated_tokens: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum QuotaError {
    #[error("request quota exceeded")]
    Requests,
    #[error("prompt-token quota exceeded")]
    PromptTokens,
    #[error("generated-token quota exceeded")]
    GeneratedTokens,
    #[error(transparent)]
    Broker(#[from] redis::RedisError),
}

impl StreamsClient {
    pub async fn connect(url: &str, config: StreamsConfig) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let commands = client.get_connection_manager().await?;
        let fast = FredBuilder::from_config(FredConfig::from_url(url)?).build()?;
        fast.init().await?;
        Ok(Self {
            client,
            fast,
            commands,
            config,
        })
    }

    pub fn result_stream(request_id: &str) -> String {
        format!("inference:results:{request_id}")
    }

    pub fn status_key(request_id: &str) -> String {
        format!("inference:status:{request_id}")
    }

    fn lease_key(request_id: &str) -> String {
        format!("inference:lease:{request_id}")
    }

    pub async fn publish_generation(&self, job: &wire::GenerationJob) -> Result<String> {
        validate_schema(job.schema_version)?;
        let payload = job.encode_to_vec();
        let status = Self::status_key(&job.request_id);
        let entry_id: String = self
            .fast
            .xadd(
                REQUEST_STREAM,
                false,
                None::<()>,
                "*",
                vec![("payload", payload)],
            )
            .await?;
        let mut connection = self.commands.clone();
        let _: () = redis::pipe()
            .atomic()
            .cmd("HSET")
            .arg(&status)
            .arg("state")
            .arg("queued")
            .arg("model_id")
            .arg(&job.model_id)
            .arg("request_entry_id")
            .arg(&entry_id)
            .ignore()
            .cmd("EXPIRE")
            .arg(&status)
            .arg(self.config.active_ttl.as_secs())
            .ignore()
            .query_async(&mut connection)
            .await?;
        Ok(entry_id)
    }

    pub async fn consumer(&self, consumer: impl Into<String>) -> Result<JobConsumer> {
        let mut commands = self.commands.clone();
        let result: redis::RedisResult<Value> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(REQUEST_STREAM)
            .arg(REQUEST_GROUP)
            .arg("0-0")
            .arg("MKSTREAM")
            .query_async(&mut commands)
            .await;
        if let Err(error) = result
            && !error.to_string().contains("BUSYGROUP")
        {
            return Err(error.into());
        }
        Ok(JobConsumer {
            streams: self.clone(),
            connection: self.client.get_multiplexed_async_connection().await?,
            consumer: consumer.into(),
        })
    }

    pub async fn publish_result(&self, result: &wire::GenerationResult) -> Result<String> {
        validate_schema(result.schema_version)?;
        let stream = Self::result_stream(&result.request_id);
        let status = Self::status_key(&result.request_id);
        let event_id: String = self
            .fast
            .xadd(
                stream.clone(),
                false,
                None::<()>,
                "*",
                vec![("payload", result.encode_to_vec())],
            )
            .await?;
        let mut connection = self.commands.clone();

        let terminal = matches!(
            result.event,
            Some(wire::generation_result::Event::Completed(_))
                | Some(wire::generation_result::Event::Failed(_))
        );
        let state = if terminal { "completed" } else { "running" };
        let ttl = if terminal {
            self.config.result_retention
        } else {
            self.config.active_ttl
        };
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("HSET")
            .arg(&status)
            .arg("state")
            .arg(state)
            .arg("active_attempt_id")
            .arg(&result.attempt_id)
            .ignore();
        if terminal {
            pipe.cmd("HSET")
                .arg(&status)
                .arg("terminal_event_id")
                .arg(&event_id)
                .ignore();
        }
        let _: () = pipe
            .cmd("EXPIRE")
            .arg(&stream)
            .arg(ttl.as_secs())
            .ignore()
            .cmd("EXPIRE")
            .arg(&status)
            .arg(ttl.as_secs())
            .ignore()
            .query_async(&mut connection)
            .await?;
        Ok(event_id)
    }

    pub async fn subscribe(
        &self,
        request_id: &str,
        after_event_id: Option<&str>,
    ) -> Result<Option<ResultSubscription>> {
        let stream = Self::result_stream(request_id);
        let status = Self::status_key(request_id);
        let mut connection = self.commands.clone();
        let exists: i64 = redis::cmd("EXISTS")
            .arg(&stream)
            .arg(&status)
            .query_async(&mut connection)
            .await?;
        if exists == 0 {
            return Ok(None);
        }
        Ok(Some(ResultSubscription {
            client: self.client.clone(),
            connection: self.client.get_multiplexed_async_connection().await?,
            stream,
            last_event_id: after_event_id.unwrap_or("0-0").to_string(),
        }))
    }

    pub async fn status(&self, request_id: &str) -> Result<Option<GenerationStatus>> {
        let mut connection = self.commands.clone();
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(Self::status_key(request_id))
            .arg("state")
            .arg("model_id")
            .arg("active_attempt_id")
            .arg("terminal_event_id")
            .query_async(&mut connection)
            .await?;
        if values
            .first()
            .and_then(Option::as_deref)
            .is_none_or(str::is_empty)
        {
            return Ok(None);
        }
        Ok(Some(GenerationStatus {
            state: values.first().cloned().flatten().unwrap_or_default(),
            model_id: values.get(1).cloned().flatten().unwrap_or_default(),
            active_attempt_id: values
                .get(2)
                .cloned()
                .flatten()
                .filter(|value| !value.is_empty()),
            terminal_event_id: values
                .get(3)
                .cloned()
                .flatten()
                .filter(|value| !value.is_empty()),
        }))
    }

    pub async fn acquire_lease(&self, request_id: &str, owner: &str) -> Result<bool> {
        let mut connection = self.commands.clone();
        let value: Option<String> = redis::cmd("SET")
            .arg(Self::lease_key(request_id))
            .arg(owner)
            .arg("NX")
            .arg("PX")
            .arg(self.config.lease_duration.as_millis() as u64)
            .query_async(&mut connection)
            .await?;
        Ok(value.is_some())
    }

    pub async fn release_lease(&self, request_id: &str, owner: &str) -> Result<()> {
        let script = redis::Script::new(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end",
        );
        let mut connection = self.commands.clone();
        let _: i64 = script
            .key(Self::lease_key(request_id))
            .arg(owner)
            .invoke_async(&mut connection)
            .await?;
        Ok(())
    }

    pub async fn renew_lease(&self, request_id: &str, owner: &str) -> Result<bool> {
        let script = redis::Script::new(
            "if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('PEXPIRE', KEYS[1], ARGV[2]); return 1 else return 0 end",
        );
        let mut connection = self.commands.clone();
        let renewed: i64 = script
            .key(Self::lease_key(request_id))
            .arg(owner)
            .arg(self.config.lease_duration.as_millis() as u64)
            .invoke_async(&mut connection)
            .await?;
        Ok(renewed == 1)
    }

    pub async fn reserve_quota(
        &self,
        api_key: &str,
        prompt_tokens: u64,
        generated_tokens: u64,
        limits: QuotaLimits,
    ) -> std::result::Result<QuotaReservation, QuotaError> {
        let id = uuid::Uuid::new_v4().to_string();
        let bucket = unix_ms() / 60_000;
        let key_hash = format!("{:x}", Sha256::digest(api_key.as_bytes()));
        let prefix = format!("inference:quota:{key_hash}:{bucket}");
        let reservation_key = format!("inference:reservation:{id}");
        let script = redis::Script::new(RESERVE_QUOTA_LUA);
        let mut connection = self.commands.clone();
        let values: Vec<i64> = script
            .key(format!("{prefix}:requests"))
            .key(format!("{prefix}:prompt"))
            .key(format!("{prefix}:generated"))
            .key(&reservation_key)
            .arg(prompt_tokens)
            .arg(generated_tokens)
            .arg(limits.requests_per_minute)
            .arg(limits.prompt_tokens_per_minute)
            .arg(limits.generated_tokens_per_minute)
            .arg(120)
            .invoke_async(&mut connection)
            .await?;
        match values.as_slice() {
            [1, requests, prompt, generated] => Ok(QuotaReservation {
                id,
                remaining_requests: (*requests).max(0) as u64,
                remaining_prompt_tokens: (*prompt).max(0) as u64,
                remaining_generated_tokens: (*generated).max(0) as u64,
            }),
            [0, 1, ..] => Err(QuotaError::Requests),
            [0, 2, ..] => Err(QuotaError::PromptTokens),
            [0, 3, ..] => Err(QuotaError::GeneratedTokens),
            _ => Err(QuotaError::Broker(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "invalid quota script response",
            )))),
        }
    }

    pub async fn reconcile_quota(&self, reservation_id: &str, actual: u64) -> Result<u64> {
        let mut connection = self.commands.clone();
        let refunded: i64 = redis::Script::new(RECONCILE_QUOTA_LUA)
            .key(format!("inference:reservation:{reservation_id}"))
            .arg(actual)
            .invoke_async(&mut connection)
            .await?;
        Ok(refunded.max(0) as u64)
    }

    pub async fn cancel_quota(&self, reservation_id: &str) -> Result<()> {
        let mut connection = self.commands.clone();
        let _: i64 = redis::Script::new(CANCEL_QUOTA_LUA)
            .key(format!("inference:reservation:{reservation_id}"))
            .invoke_async(&mut connection)
            .await?;
        Ok(())
    }
}

impl JobConsumer {
    pub async fn next(&mut self, block: Duration) -> Result<Option<ConsumedJob>> {
        let reply: Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(REQUEST_GROUP)
            .arg(&self.consumer)
            .arg("BLOCK")
            .arg(block.as_millis() as u64)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(REQUEST_STREAM)
            .arg(">")
            .query_async(&mut self.connection)
            .await?;
        parse_job_reply(reply)
    }

    pub async fn reclaim_stale(&mut self, idle: Duration) -> Result<Option<ConsumedJob>> {
        let reply: Value = redis::cmd("XAUTOCLAIM")
            .arg(REQUEST_STREAM)
            .arg(REQUEST_GROUP)
            .arg(&self.consumer)
            .arg(idle.as_millis() as u64)
            .arg("0-0")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut self.connection)
            .await?;
        parse_autoclaim_reply(reply)
    }

    pub async fn acknowledge(&self, entry_id: &str) -> Result<bool> {
        let mut connection = self.streams.commands.clone();
        let count: i64 = redis::cmd("XACK")
            .arg(REQUEST_STREAM)
            .arg(REQUEST_GROUP)
            .arg(entry_id)
            .query_async(&mut connection)
            .await?;
        Ok(count == 1)
    }
}

impl ResultSubscription {
    pub fn last_event_id(&self) -> &str {
        &self.last_event_id
    }

    pub async fn next(&mut self, block: Duration) -> Result<Option<ResultEntry>> {
        let mut backoff = Duration::from_millis(50);
        for attempt in 0..4 {
            let reply: redis::RedisResult<Value> = redis::cmd("XREAD")
                .arg("BLOCK")
                .arg(block.as_millis() as u64)
                .arg("COUNT")
                .arg(1)
                .arg("STREAMS")
                .arg(&self.stream)
                .arg(&self.last_event_id)
                .query_async(&mut self.connection)
                .await;
            match reply {
                Ok(reply) => {
                    let entry = parse_result_reply(reply)?;
                    if let Some(entry) = &entry {
                        self.last_event_id.clone_from(&entry.event_id);
                    }
                    return Ok(entry);
                }
                Err(error) if attempt < 3 => {
                    sleep(backoff).await;
                    backoff *= 2;
                    self.connection = self.client.get_multiplexed_async_connection().await?;
                    if error.is_timeout() {
                        continue;
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        unreachable!()
    }
}

fn validate_schema(version: u32) -> Result<()> {
    if version != SCHEMA_VERSION {
        bail!("unsupported inference Streams schema version {version}");
    }
    Ok(())
}

fn parse_job_reply(reply: Value) -> Result<Option<ConsumedJob>> {
    let Some((id, payload)) = parse_xread_payload(reply)? else {
        return Ok(None);
    };
    let job = wire::GenerationJob::decode(payload.as_slice()).context("decode generation job")?;
    validate_schema(job.schema_version)?;
    Ok(Some(ConsumedJob { entry_id: id, job }))
}

fn parse_autoclaim_reply(reply: Value) -> Result<Option<ConsumedJob>> {
    let Value::Array(parts) = reply else {
        return Ok(None);
    };
    let Some(Value::Array(entries)) = parts.get(1) else {
        return Ok(None);
    };
    parse_entry(entries.first()).and_then(|entry| match entry {
        Some((entry_id, payload)) => {
            let job = wire::GenerationJob::decode(payload.as_slice())?;
            validate_schema(job.schema_version)?;
            Ok(Some(ConsumedJob { entry_id, job }))
        }
        None => Ok(None),
    })
}

fn parse_result_reply(reply: Value) -> Result<Option<ResultEntry>> {
    let Some((event_id, payload)) = parse_xread_payload(reply)? else {
        return Ok(None);
    };
    let result = wire::GenerationResult::decode(payload.as_slice())?;
    validate_schema(result.schema_version)?;
    Ok(Some(ResultEntry { event_id, result }))
}

fn parse_xread_payload(reply: Value) -> Result<Option<(String, Vec<u8>)>> {
    let Value::Array(streams) = reply else {
        return Ok(None);
    };
    let Some(Value::Array(stream)) = streams.first() else {
        return Ok(None);
    };
    let Some(Value::Array(entries)) = stream.get(1) else {
        return Ok(None);
    };
    parse_entry(entries.first())
}

fn parse_entry(value: Option<&Value>) -> Result<Option<(String, Vec<u8>)>> {
    let Some(Value::Array(entry)) = value else {
        return Ok(None);
    };
    let Some(Value::BulkString(id)) = entry.first() else {
        return Ok(None);
    };
    let Some(Value::Array(fields)) = entry.get(1) else {
        return Ok(None);
    };
    for pair in fields.chunks(2) {
        if let [Value::BulkString(key), Value::BulkString(payload)] = pair
            && key == b"payload"
        {
            return Ok(Some((String::from_utf8(id.clone())?, payload.clone())));
        }
    }
    Err(anyhow!("Stream entry has no protobuf payload field"))
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

const RESERVE_QUOTA_LUA: &str = r#"
local requests = tonumber(redis.call('GET', KEYS[1]) or '0')
local prompt = tonumber(redis.call('GET', KEYS[2]) or '0')
local generated = tonumber(redis.call('GET', KEYS[3]) or '0')
local prompt_add = tonumber(ARGV[1])
local generated_add = tonumber(ARGV[2])
if requests + 1 > tonumber(ARGV[3]) then return {0, 1} end
if prompt + prompt_add > tonumber(ARGV[4]) then return {0, 2} end
if generated + generated_add > tonumber(ARGV[5]) then return {0, 3} end
redis.call('INCR', KEYS[1])
redis.call('INCRBY', KEYS[2], prompt_add)
redis.call('INCRBY', KEYS[3], generated_add)
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('EXPIRE', KEYS[2], ARGV[6])
redis.call('EXPIRE', KEYS[3], ARGV[6])
redis.call('HSET', KEYS[4], 'status', 'reserved', 'requests_key', KEYS[1], 'prompt_key', KEYS[2], 'generated_key', KEYS[3], 'prompt', prompt_add, 'generated', generated_add)
redis.call('EXPIRE', KEYS[4], ARGV[6])
return {1, tonumber(ARGV[3]) - requests - 1, tonumber(ARGV[4]) - prompt - prompt_add, tonumber(ARGV[5]) - generated - generated_add}
"#;

const RECONCILE_QUOTA_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'status') ~= 'reserved' then return 0 end
local reserved = tonumber(redis.call('HGET', KEYS[1], 'generated') or '0')
local actual = tonumber(ARGV[1])
local refund = math.max(reserved - actual, 0)
local generated_key = redis.call('HGET', KEYS[1], 'generated_key')
if refund > 0 and generated_key then redis.call('DECRBY', generated_key, refund) end
redis.call('HSET', KEYS[1], 'status', 'reconciled', 'actual_generated', actual, 'refunded', refund)
return refund
"#;

const CANCEL_QUOTA_LUA: &str = r#"
if redis.call('HGET', KEYS[1], 'status') ~= 'reserved' then return 0 end
local requests_key = redis.call('HGET', KEYS[1], 'requests_key')
local prompt_key = redis.call('HGET', KEYS[1], 'prompt_key')
local generated_key = redis.call('HGET', KEYS[1], 'generated_key')
local prompt = tonumber(redis.call('HGET', KEYS[1], 'prompt') or '0')
local generated = tonumber(redis.call('HGET', KEYS[1], 'generated') or '0')
if requests_key then redis.call('DECR', requests_key) end
if prompt_key then redis.call('DECRBY', prompt_key, prompt) end
if generated_key then redis.call('DECRBY', generated_key, generated) end
redis.call('HSET', KEYS[1], 'status', 'cancelled')
return 1
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_and_status_keys_are_stable() {
        assert_eq!(StreamsClient::result_stream("abc"), "inference:results:abc");
        assert_eq!(StreamsClient::status_key("abc"), "inference:status:abc");
    }

    #[test]
    fn protobuf_job_round_trips() {
        let job = wire::GenerationJob {
            schema_version: SCHEMA_VERSION,
            request_id: "request-1".into(),
            model_id: "model".into(),
            request_json: br#"{"prompt":"hi"}"#.to_vec(),
            ..Default::default()
        };
        let decoded = wire::GenerationJob::decode(job.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, job);
    }
}
