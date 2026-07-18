# ADR 0007: Tokenizer Runtime Footprint

## Status

Accepted.

## Context

Tokenizer state is CPU-side runtime state. In a multi-model server it can include vocabulary tables, merge or normalization structures, post-processing rules, and runtime allocations beyond the serialized tokenizer files. Models that share a tokenizer family may be able to share that state, while unrelated tokenizer families should not be treated as interchangeable.

## Decision

- Treat tokenizer state as part of the serving runtime footprint, alongside model weights and other host-side state.
- Keep tokenizer ID and revision in the model catalog because they are required for correctness and reproducibility.
- Do not invent a tokenizer-memory estimate or expose tokenizer eviction as an independent operation in v1. The current catalog has no measured resident-size contract.
- When multi-model placement is implemented, measure tokenizer memory in the actual runtime, add an explicit tokenizer-family key, and amortize shared state at the runtime level. Placement and eviction must account for shared state exactly once.
- Do not add ModelMesh to the current local path. ModelMesh's architecture manages models across runtime deployments and maintains cluster-level model placement metadata; that is a separate control-plane product, not a tokenizer-only optimization. See the [ModelMesh architecture](https://github.com/kserve/modelmesh-serving/blob/main/docs/architecture/README.md).

## Consequences

The current single-model MLX/vLLM path remains simple and correct. The control plane will not claim that model size alone predicts pod RSS. A future multi-model benchmark must record tokenizer load time, resident memory before/after tokenizer load, shared-family reuse, model unload behavior, and total runtime RSS.

## References

- [Hugging Face Tokenizers API](https://huggingface.co/docs/tokenizers/main/api/tokenizer)
- [Hugging Face Transformers tokenizer API](https://huggingface.co/docs/transformers/main_classes/tokenizer)
- [ModelMesh architecture](https://github.com/kserve/modelmesh-serving/blob/main/docs/architecture/README.md)
- [KServe LLMInferenceService configuration](https://kserve.github.io/website/docs/model-serving/generative-inference/llmisvc/llmisvc-configuration)
