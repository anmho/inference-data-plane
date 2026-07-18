# Kubernetes resources

The local overlay runs Valkey, Temporal, the Rust services, and KServe declarations with `emptyDir` storage only. It does not create a PVC or public load balancer. The KServe adapter remains a stable service while the host MLX process is stopped, and readiness follows the model endpoint.

The optional GKE vLLM overlay uses the NVIDIA model artifact and a ClusterIP-only vLLM service. `--max-model-len` is a runtime sequence ceiling; vLLM's scheduler-level batch controls are separate from the frontend's `max_tokens` admission policy. See [KServe's LLMInferenceService API](https://kserve.github.io/website/docs/reference/crd-api) and [vLLM engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/).
