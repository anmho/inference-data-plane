use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use futures_util::StreamExt;
use prost::Message;
use std::io::Write;

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
    let mut client_builder = reqwest::Client::builder();
    if base_url.starts_with("http://") {
        client_builder = client_builder.http2_prior_knowledge();
    }
    let client = client_builder.build()?;

    let unary = ProtoGenerateRequest {
        model: model.clone(),
        messages: vec![ProtoChatMessage {
            role: "user".to_string(),
            content: "Explain token budgeting in one sentence.".to_string(),
        }],
        prompt: String::new(),
        max_tokens,
        temperature: 0.0,
    };

    let response = client
        .post(format!("{base_url}/inference.v1.InferenceService/Generate"))
        .bearer_auth(&api_key)
        .header("content-type", "application/proto")
        .body(encode(&unary))
        .send()
        .await?
        .error_for_status()?;
    let body = response.bytes().await?;
    let decoded = ProtoGenerateResponse::decode(body).context("decode unary protobuf response")?;
    println!(
        "unary: model={} generated_tokens={} text={}",
        decoded.model, decoded.generated_tokens, decoded.text
    );

    let streaming = ProtoGenerateRequest {
        model,
        messages: vec![ProtoChatMessage {
            role: "user".to_string(),
            content: "Count from one to five.".to_string(),
        }],
        prompt: String::new(),
        max_tokens,
        temperature: 0.0,
    };

    let response = client
        .post(format!(
            "{base_url}/inference.v1.InferenceService/StreamGenerate"
        ))
        .bearer_auth(&api_key)
        .header("content-type", "application/proto")
        .body(encode(&streaming))
        .send()
        .await?
        .error_for_status()?;

    print!("streaming:");
    let mut buffered = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        buffered.extend_from_slice(&chunk?);
        while buffered.len() >= 5 {
            let compressed = buffered[0];
            let len =
                u32::from_be_bytes([buffered[1], buffered[2], buffered[3], buffered[4]]) as usize;
            if buffered.len() < 5 + len {
                break;
            }
            buffered.advance(5);
            let message = buffered.split_to(len);
            let decoded =
                ProtoGenerateChunk::decode(message).context("decode streaming protobuf chunk")?;
            if compressed & 0b0000_0010 != 0 {
                println!(" error={}", decoded.delta);
                return Ok(());
            }
            if !decoded.delta.is_empty() {
                print!(" {}", decoded.delta.trim());
                std::io::stdout().flush()?;
            }
            if !decoded.finish_reason.is_empty() {
                println!(" finish={}", decoded.finish_reason);
            }
        }
    }

    Ok(())
}

fn encode<T: Message>(message: &T) -> Vec<u8> {
    let mut body = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut body)
        .expect("encoding to Vec cannot fail");
    body
}
