# ADR 0006: Batching, Length, and Termination Ownership

## Status

Accepted.

## Context

The request output limit, model context window, runtime scheduler batch, EOS handling, and custom stop strings are related during generation but have different owners. Treating them as one limit would make admission, billing, and runtime behavior ambiguous.

## Decision

- The Rust frontend enforces `prompt_tokens + max_tokens <= context_limit` using a conservative preflight estimate.
- The Rust engine forwards per-request `max_tokens` and optional `stop` strings. The model runtime owns EOS and stop matching and returns the finish reason. vLLM documents these controls in its [SamplingParams API](https://docs.vllm.ai/en/latest/api/vllm/).
- vLLM owns continuous batching. Its `max-num-seqs`, `max-num-batched-tokens`, and `max-model-len` settings are scheduler/sequence controls, not substitutes for frontend admission. See [vLLM engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/).
- The current engine remains one-job-at-a-time per process until concurrency is added with explicit fairness, lease, cancellation, and quota tests. The local benchmark therefore reports request and token throughput, not a claim of optimized vLLM batching.
- A runtime terminal condition becomes one terminal result event and then one terminal `GenerationEvent`. Clients do not send acknowledgements for EOS, `[END]`, or individual token chunks.

## Consequences

The same `max_tokens` policy works with MLX and vLLM, while each runtime can implement its own tokenizer, EOS, stop-string, and batch scheduler behavior. A future concurrent engine milestone can tune vLLM batch settings without changing the public stream cursor contract.
