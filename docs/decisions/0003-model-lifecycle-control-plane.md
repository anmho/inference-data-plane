# ADR 0003: Model Lifecycle Control Plane

## Status

Accepted.

## Context

Model registration, loading, draining, and process ownership are lifecycle concerns. Putting them in the Rust token path or creating a workflow per generation would add latency and couple inference availability to orchestration infrastructure.

## Options

1. Rust inference process owns model lifecycle.
2. Temporal workflow per generation.
3. Go lifecycle service with Temporal only for model state transitions.

## Decision

Keep the frontend and engine in Rust. Use Go ConnectRPC services and Temporal workflows for load, readiness wait, drain, unload, retries, and cleanup. Git-managed KServe declarations are the supported-model source of truth.

The MLX host agent launches only fixed catalog entries, tracks child processes, rejects unowned occupied ports, and stops only children it owns. It is not an arbitrary remote command executor.

## Consequences

Temporal outages do not interrupt active token streaming. Model lifecycle can be audited and retried. Per-request priority, credit eviction, and GPU placement remain separate future scheduler concerns.
