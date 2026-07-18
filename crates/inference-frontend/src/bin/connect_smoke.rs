use std::{
    io::Write,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use bytes::{Buf, BytesMut};
use futures_util::StreamExt;
use inference_streams::api::{
    ChatMessage, GenerateRequest, GenerateResponse, GenerationEvent, SubscribeGenerationRequest,
};
use prost::Message;

#[tokio::main]
async fn main() -> Result<()> {
    let base_url = std::env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into());
    let api_key = std::env::var("API_KEY").unwrap_or_else(|_| "dev-key".into());
    let model =
        std::env::var("MODEL").unwrap_or_else(|_| "mlx-community/SmolLM2-135M-Instruct".into());
    let max_tokens = std::env::var("MAX_TOKENS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(96);
    let mut builder = reqwest::Client::builder();
    if base_url.starts_with("http://") {
        builder = builder.http2_prior_knowledge();
    }
    let client = builder.build()?;

    let unary = GenerateRequest {
        model: model.clone(),
        messages: vec![ChatMessage {
            role: "user".to_string(),
            content: "Explain token budgeting in one sentence.".to_string(),
        }],
        max_tokens,
        ..Default::default()
    };
    let unary_started = Instant::now();
    let response = client
        .post(format!("{base_url}/inference.v1.InferenceService/Generate"))
        .bearer_auth(&api_key)
        .header("content-type", "application/proto")
        .body(encode(&unary))
        .send()
        .await?
        .error_for_status()?;
    let decoded = GenerateResponse::decode(response.bytes().await?)
        .context("decode unary protobuf response")?;
    println!(
        "unary: request_id={} model={} generated_tokens={} latency_ms={} text={}",
        decoded.request_id,
        decoded.model,
        decoded.generated_tokens,
        unary_started.elapsed().as_millis(),
        decoded.text
    );

    let streaming = GenerateRequest {
        model,
        messages: vec![ChatMessage {
            role: "user".to_string(),
            content: "Count from one to five.".to_string(),
        }],
        max_tokens,
        ..Default::default()
    };
    let live = read_stream(
        &client,
        &base_url,
        &api_key,
        "StreamGenerate",
        encode(&streaming),
        true,
    )
    .await?;
    let request_id = live
        .events
        .first()
        .map(|event| event.request_id.clone())
        .context("live stream returned no events")?;
    let live_text: String = live
        .events
        .iter()
        .map(|event| event.delta.as_str())
        .collect();
    println!(
        "stream metrics: ttft_ms={} duration_ms={} generated_tokens={}",
        live.first_token_latency.unwrap_or_default().as_millis(),
        live.elapsed.as_millis(),
        live.events
            .iter()
            .map(|event| event.generated_tokens)
            .max()
            .unwrap_or_default()
    );

    let replay = read_stream(
        &client,
        &base_url,
        &api_key,
        "SubscribeGeneration",
        encode(&SubscribeGenerationRequest {
            request_id: request_id.clone(),
            after_event_id: None,
        }),
        false,
    )
    .await?;
    let replay_text: String = replay
        .events
        .iter()
        .map(|event| event.delta.as_str())
        .collect();
    if replay_text != live_text {
        bail!("replayed token sequence differs from live stream");
    }
    println!(
        "replay: request_id={} events={} latency_ms={} text={}",
        request_id,
        replay.events.len(),
        replay.elapsed.as_millis(),
        replay_text.trim()
    );
    Ok(())
}

async fn read_stream(
    client: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    method: &str,
    body: Vec<u8>,
    print_live: bool,
) -> Result<StreamRead> {
    let started = Instant::now();
    let response = client
        .post(format!("{base_url}/inference.v1.InferenceService/{method}"))
        .bearer_auth(api_key)
        .header("content-type", "application/proto")
        .body(body)
        .send()
        .await?
        .error_for_status()?;
    let mut buffered = BytesMut::new();
    let mut chunks = response.bytes_stream();
    let mut events = Vec::new();
    let mut first_token_latency = None;
    if print_live {
        print!("streaming: ");
    }
    while let Some(chunk) = chunks.next().await {
        buffered.extend_from_slice(&chunk?);
        while buffered.len() >= 5 {
            let flags = buffered[0];
            let len =
                u32::from_be_bytes([buffered[1], buffered[2], buffered[3], buffered[4]]) as usize;
            if buffered.len() < 5 + len {
                break;
            }
            buffered.advance(5);
            let message = buffered.split_to(len);
            let event = GenerationEvent::decode(message).context("decode GenerationEvent")?;
            if flags & 0b0000_0010 != 0 {
                bail!("stream error: {}", event.delta);
            }
            if print_live && !event.delta.is_empty() {
                print!("{}", event.delta);
                std::io::stdout().flush()?;
            }
            if first_token_latency.is_none() && !event.delta.is_empty() {
                first_token_latency = Some(started.elapsed());
            }
            let terminal = event.terminal;
            events.push(event);
            if terminal && print_live {
                println!();
            }
        }
    }
    Ok(StreamRead {
        events,
        first_token_latency,
        elapsed: started.elapsed(),
    })
}

struct StreamRead {
    events: Vec<GenerationEvent>,
    first_token_latency: Option<Duration>,
    elapsed: Duration,
}

fn encode<T: Message>(message: &T) -> Vec<u8> {
    message.encode_to_vec()
}
