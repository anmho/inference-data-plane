# Configuration

Configuration files are declarative policy for the local and optional GKE paths. They define model context/output limits, broker retention, quota, and runtime endpoints; they do not turn the Rust engine into a model scheduler.

`context_limit` and `max_output_tokens` are admission policy. `max_tokens` is supplied per request. Runtime batch capacity and `max-model-len` are vLLM engine settings, documented in [vLLM engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/). EOS and custom stop strings are runtime sampling behavior, documented in [vLLM SamplingParams](https://docs.vllm.ai/en/latest/api/vllm/).

The local configuration intentionally uses a one-process engine and real host MLX. This keeps the correctness benchmark separate from a future concurrent-engine benchmark that would exercise vLLM continuous batching.
