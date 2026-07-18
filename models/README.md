# Model catalog

The catalog declares supported model identity, revision, tokenizer identity, context limit, output limit, and runtime endpoint metadata.

The local model is `mlx-community/SmolLM2-135M-Instruct` executed by MLX/Metal on Apple Silicon. The NVIDIA/vLLM equivalent is `HuggingFaceTB/SmolLM2-135M-Instruct`. The Rust frontend uses catalog limits for admission; the runtime remains authoritative for token usage, EOS, stop strings, and actual sequence length.

KServe documents `LLMInferenceServiceConfig` as a reusable configuration template composed into `LLMInferenceService` instances through `baseRefs` ([configuration guide](https://kserve.github.io/website/docs/model-serving/generative-inference/llmisvc/llmisvc-configuration)). MLX is designed for Apple silicon and supports CPU/GPU devices through its unified memory model ([MLX documentation](https://ml-explore.github.io/mlx/)).
