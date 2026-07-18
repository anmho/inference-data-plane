# Protocols

The public `inference.v1` service uses protobuf messages over ConnectRPC's HTTP/2-compatible protocol. The API exposes unary generation, live streaming, and replay/resume through `SubscribeGeneration`; all streaming methods use the same `GenerationEvent` message.

`GenerateRequest.max_tokens` is a per-request output ceiling. `GenerateRequest.stop` carries runtime stop strings such as `[END]`; EOS and stop matching remain model-runtime behavior. A terminal event carries the runtime finish reason and is distinct from a token or broker acknowledgement.

The internal `streams.proto` schema is versioned because Valkey jobs and results can outlive one process. See the [ConnectRPC protocol documentation](https://connectrpc.com/docs/protocol/), [vLLM SamplingParams](https://docs.vllm.ai/en/latest/api/vllm/), and [Valkey Streams](https://valkey.io/topics/streams-intro/).
