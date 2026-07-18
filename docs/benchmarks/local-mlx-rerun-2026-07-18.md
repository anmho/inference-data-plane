# Local MLX Benchmark Rerun: 2026-07-18

## Environment

- Hardware: Apple M5 Pro, 18 cores, 64 GB memory
- Model: `mlx-community/SmolLM2-135M-Instruct`
- API: ConnectRPC-compatible protobuf over HTTP/2
- Broker: local Valkey Streams
- Load: 2 unary VUs and 1 streaming VU for 20 seconds

## Results

| Measurement | Result |
| --- | ---: |
| Model cold start to KServe ready | 2.268 s |
| Smoke unary latency | 201 ms |
| Smoke stream TTFT | 49 ms |
| Smoke stream completion | 252 ms |
| Completed-stream replay | 48 ms |
| k6 total requests | 136 |
| Overall request rate | 6.700 req/s |
| Unary request rate | 4.483 req/s |
| Streaming request rate | 2.217 req/s |
| Unary generated-token rate | 138.968 tokens/s |
| Streaming generated-token rate | 53.203 tokens/s |
| Aggregate generated-token rate | **192.171 tokens/s** |
| Overall latency p50 / p95 | 321 ms / 849 ms |
| Error rate | 0% |

The aggregate generated-token rate is application throughput across successful unary and streaming responses. It is not isolated single-sequence decoder throughput. The current Rust engine processes one job at a time per process, so this is not a continuous-batching benchmark.

## Startup Fix

The first attempt after restarting Minikube hit a transient cert-manager webhook connection refusal during KServe installation. `scripts/kserve-install.sh` now waits for the webhook pod and retries webhook-backed applies for a bounded period. The rerun completed successfully.

Raw output:

- [`20260718T041600Z-local-mlx.txt`](../../benchmarks/20260718T041600Z-local-mlx.txt)
- [`20260718T041600Z-local-mlx-summary.json`](../../benchmarks/20260718T041600Z-local-mlx-summary.json)
