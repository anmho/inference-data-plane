# inference-frontend

The Rust frontend is the admission and client-streaming boundary. It authenticates a request, estimates prompt tokens, applies the configured context/output policy, reserves quota, and publishes one versioned Valkey job.

## Length contract

`max_tokens` is interpreted as a maximum number of generated tokens for one request. The frontend clamps the default and configured per-model maximum through `token-budget-engine`, then rejects when the estimated prompt plus admitted output exceeds the model context limit. This check is deliberately conservative: the runtime's usage is authoritative after generation.

The frontend does not implement model decoding, batching, EOS detection, or stop-sequence matching. It forwards the request's `stop` strings, including values such as `[END]`, to the engine. The runtime decides whether EOS or a stop string ends decoding and reports the finish reason. See vLLM's [sampling parameters](https://docs.vllm.ai/en/latest/api/vllm/) and [engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/).

## Terminal events

`StreamGenerate` and `SubscribeGeneration` expose the same `GenerationEvent` shape. The frontend emits token events followed by exactly one terminal event carrying `finish_reason`, cumulative generated tokens, and the Valkey event ID. A stop marker is not a separate queue message and is not acknowledged by clients.

The frontend does not batch requests. It provides admission and streaming; batching belongs to the model runtime. The current engine process is intentionally serialized, so a future concurrency change must be benchmarked separately from admission changes.

## References

- [vLLM SamplingParams](https://docs.vllm.ai/en/latest/api/vllm/): per-request `max_tokens`, `stop`, `stop_token_ids`, and EOS behavior.
- [vLLM engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/): model length and scheduler-level batch controls.
- [ConnectRPC protocol](https://connectrpc.com/docs/protocol/): protobuf streaming transport used by the public methods.
