# ADR 0005: V1 Scheduling and Cache Scope

## Status

Accepted.

## Context

KV-aware routing, LMCache, credit scheduling, preemption, and GPU placement are related but independently complex. Implementing them before replay and recovery are correct would obscure failures in the core data path.

## Decision

V1 records a stable model-and-prefix cache key and estimated prefix tokens in every job. This is observability and future routing input, not a claim of distributed KV reuse. The runtime's final tokenizer usage is authoritative; frontend counting is a conservative CPU-side admission estimate tied to a tokenizer ID and revision.

Do not implement LMCache, ModelMesh, priority eviction, credit scheduling, GPU orchestration, or multi-model placement in this milestone.

## Consequences

Repeated-prefix latency can be measured on one runtime, but cross-replica KV reuse is not guaranteed. The next cache milestone needs multiple runtimes plus cache-aware routing or transfer, and the next scheduling milestone needs explicit fairness and preemption invariants.

## References

- [vLLM Automatic Prefix Caching](https://docs.vllm.ai/en/stable/design/prefix_caching/)
- [vLLM engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/)
