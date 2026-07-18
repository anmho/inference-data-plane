# ADR 0004: Local KServe with Host MLX

## Status

Accepted.

## Context

KServe expects a Kubernetes workload, while MLX requires Apple Metal on the host. Passing a GPU into a Linux Minikube node is not the same runtime and vLLM CUDA cannot use Apple Metal.

## Options

1. Synthetic Kubernetes model: easy, but does not verify real inference.
2. CPU model inside Minikube: real, but does not exercise the desired MLX runtime.
3. KServe-managed adapter to an owned host MLX process.

## Decision

Use KServe 0.18 `LLMInferenceService`, not archived ModelMesh. Its lightweight `socat` workload exposes the stable generated ClusterIP and forwards to `host.minikube.internal:18000`. The adapter has an in-container liveness endpoint and a separate readiness probe forwarded to MLX. It therefore remains resident while the model is stopped and becomes ready as soon as MLX returns, without a container restart backoff. The model storage initializer is disabled; MLX owns model download/cache on the Mac.

Install only pinned cert-manager, Gateway API/GIE CRDs, LWS dependency, and the KServe LLM controller. Do not install MetalLB, Envoy Gateway, a public Gateway, PVCs, or the official full installer that adds unnecessary local networking.

## Consequences

The real model executes on Metal while KServe owns declaration/readiness and a stable endpoint. Temporal also probes that endpoint before completing a load workflow, avoiding stale KServe condition races. This validates lifecycle integration, not NVIDIA scheduling. Cloud vLLM uses the corresponding Hugging Face model artifact in a later milestone.
