# ADR 0001: Replayable Valkey Streams

## Status

Accepted.

## Context

Generation must survive client disconnects, support phone and web subscribers at the same time, and allow a caller to resume after its last observed event.

## Options

1. Valkey Pub/Sub: lowest bookkeeping, but no replay, late join, or disconnect recovery.
2. Consumer groups for requests and results: reliable work distribution, but result consumers would compete instead of independently observing the same output.
3. Consumer groups for jobs and plain Stream cursors for results: reliable single-owner work plus independent replay.

## Decision

Inbound jobs use `XREADGROUP`, `XACK`, `XPENDING`, and `XAUTOCLAIM`. Outbound events use `XREAD` from a caller-owned Stream ID. A missing cursor starts at `0-0`; a supplied cursor returns entries strictly after it. No token event is acknowledged.

Each terminal result Stream receives a 10-minute TTL. Active Streams refresh their TTL. Output-token limits, event coalescing, quotas, and expiration bound memory without trimming the beginning of a retained response.

## Consequences

Client disconnects do not cancel generation. Multiple subscribers receive identical ordered events. The API, not Valkey, remains the public security boundary.

## References

- [Valkey Streams](https://valkey.io/topics/streams-intro/)
- [Valkey XREAD](https://valkey.io/commands/xread/)
- [Valkey XREADGROUP](https://valkey.io/commands/xreadgroup/)
- [Valkey XACK](https://valkey.io/commands/xack/)
- [Valkey XAUTOCLAIM](https://valkey.io/commands/xautoclaim/)
