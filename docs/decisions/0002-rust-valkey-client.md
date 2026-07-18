# ADR 0002: Rust Valkey Client and Transport

## Status

Accepted.

## Context

The Rust path needs Valkey-compatible Streams, automatic cross-task pipelining, blocking reads, retries, and quota scripts. The Go `valkey-go` client is not usable from the Rust hot path.

## Options

1. `redis-rs` only: mature RESP support and blocking commands, but no equivalent automatic cross-task pipeline policy.
2. `fred` only: automatic pipelining and strong async behavior, but the existing blocking Stream parsing and Lua interfaces are more direct in `redis-rs`.
3. Split by connection role.

## Decision

Use one cloneable `fred` 10 client for automatically pipelined `XADD` publication. Use `redis-rs` dedicated blocking connections for `XREAD` and `XREADGROUP`, and a multiplexed command manager for scripts and metadata.

The connection roles are intentionally separate. Pipelining batches nearby commands into fewer writes; multiplexing permits multiple in-flight commands. Neither behavior means a blocking read should share the publication socket.

RDMA is rejected for v1. Valkey RDMA is experimental, depends on Linux RDMA hardware/drivers, and has no mature Rust client transport. It cannot improve the Mac/Minikube path or managed Valkey path.

## Consequences

The repository carries two compatible RESP clients, but each has a narrow ownership boundary. A future benchmark can replace the metadata side with `fred`; RDMA requires a separate deployment milestone and fallback design.
