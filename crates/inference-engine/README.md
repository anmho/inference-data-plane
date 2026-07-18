# inference-engine

The Rust engine is the Valkey job consumer and model-runtime adapter. It claims one job from the `inference-engines` consumer group, calls the configured KServe/OpenAI-compatible endpoint, publishes token events, publishes a terminal result, reconciles quota, and only then acknowledges the job.

## Runtime ownership

The engine forwards `max_tokens`, `temperature`, and non-empty `stop` strings. EOS and custom stop matching are owned by the model runtime, not reimplemented in the engine. vLLM's [SamplingParams API](https://docs.vllm.ai/en/latest/api/vllm/) defines `max_tokens`, `stop`, `stop_token_ids`, and the EOS-related behavior. The engine records the runtime `finish_reason` and uses `stop` only as a conservative fallback when a backend omits one.

The GKE vLLM manifest sets `--max-model-len 4096`. That is the runtime's maximum sequence length. It is distinct from the frontend's per-request `max_tokens` and from vLLM scheduler settings such as `--max-num-seqs` and `--max-num-batched-tokens`, documented in [vLLM engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/).

## Batching status

vLLM itself uses continuous batching when it receives concurrent requests. This engine currently awaits each generation before reading the next job, so one engine process does not currently create concurrent runtime requests. That is intentional for the first correctness benchmark: adding concurrency changes queue fairness, lease ownership, cancellation, and quota reconciliation. It is tracked as a separate performance milestone rather than being hidden behind a misleading batch flag.

## Terminal result

The engine writes `GenerationCompleted` or `GenerationFailed` after the attempt. The frontend converts that result into the terminal `GenerationEvent`. Partial output from a failed attempt includes an attempt ID, so a retry cannot be silently combined with the abandoned attempt.

## References

- [vLLM engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/)
- [vLLM SamplingParams](https://docs.vllm.ai/en/latest/api/vllm/)
- [Valkey Streams](https://valkey.io/topics/streams-intro/)
