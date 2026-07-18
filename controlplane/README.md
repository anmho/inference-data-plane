# inference control plane

The Go control plane manages model lifecycle, not individual token generation. It resolves catalogued models, starts Temporal load/readiness/drain/unload workflows, and coordinates the MLX host agent.

KServe `LLMInferenceServiceConfig` is the model configuration source of truth and `LLMInferenceService` is the concrete service. KServe documents that the config resource is a reusable template and that service instances compose configuration through `baseRefs` in its [LLMInferenceService configuration guide](https://kserve.github.io/website/docs/model-serving/generative-inference/llmisvc/llmisvc-configuration) and [CRD API](https://kserve.github.io/website/docs/reference/crd-api).

Temporal is used for durable model state transitions and retries, not per-token or per-request workflows. That keeps orchestration outside the latency-sensitive Rust path; see Temporal's [workflow documentation](https://docs.temporal.io/workflows).
