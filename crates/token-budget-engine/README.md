# token-budget-engine

This pure Rust crate makes the frontend's admission decision before a job reaches Valkey or a model runtime.

## Decision rules

The engine estimates prompt tokens, selects the requested value or model default for `max_tokens`, clamps to the configured per-model output maximum, and applies strict rejection when:

```text
estimated_prompt_tokens + admitted_max_tokens > model_context_limit
```

This is a policy guard, not a replacement tokenizer. The runtime may report more accurate usage after tokenization. A model server's sequence limit is a separate runtime setting; vLLM documents `--max-model-len` and scheduler batch limits in its [engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/).

`max_tokens` limits one response. It does not limit the number of requests in a batch, the total batch token budget, or the lifetime of a Valkey result stream. Those are owned by the runtime scheduler and stream-retention policy respectively.
