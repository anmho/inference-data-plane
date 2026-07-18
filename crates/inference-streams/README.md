# inference-streams

`inference-streams` is the typed Valkey transport shared by the frontend and engine. It owns protobuf job encoding, request leases, stale-job recovery, quota scripts, result retention, and reconnectable result cursors.

## Two consumption semantics

- Jobs use `XREADGROUP`. A delivered job enters the consumer group's Pending Entries List and is `XACK`ed only after terminal output and quota reconciliation. `XPENDING` and `XAUTOCLAIM` support recovery after an engine failure. See Valkey's [consumer-group documentation](https://valkey.io/topics/streams-intro/), [XREADGROUP](https://valkey.io/commands/xreadgroup/), [XACK](https://valkey.io/commands/xack/), and [XAUTOCLAIM](https://valkey.io/commands/xautoclaim/).
- Results use plain `XREAD` with a caller-owned event ID. Subscribers do not acknowledge token events, do not compete with one another, and can replay from `0-0` or resume strictly after their last event ID. See [Valkey XREAD](https://valkey.io/commands/xread/).

This split is why terminal events are durable application events rather than queue acknowledgements. The result stream receives a ten-minute terminal TTL; active streams refresh a longer working TTL.

## Batching and ordering

Valkey publication uses `fred`'s automatic pipelining, but that is transport write coalescing, not model batching. Result event order comes from Valkey Stream IDs. Model-runtime batching is owned by vLLM and documented in [its engine arguments](https://docs.vllm.ai/en/stable/configuration/engine_args/).
