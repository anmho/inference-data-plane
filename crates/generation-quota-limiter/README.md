# generation-quota-limiter

This crate handles admission quotas for requests, estimated prompt tokens, and reserved generated tokens. It reserves the requested output budget before dispatch, then refunds unused capacity or reconciles actual usage after completion or cancellation.

Quota is deliberately separate from model batching. A request can be admitted while the runtime later places it into a continuous batch, and a request can be rejected before it reaches that scheduler. The runtime's `max_tokens` and stop/EOS behavior remain model-runtime concerns; see [vLLM SamplingParams](https://docs.vllm.ai/en/latest/api/vllm/).

Cancellation refunds the reservation; it does not delete a result stream or acknowledge individual token events. This preserves independent replay semantics for subscribers.
