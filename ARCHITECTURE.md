# Kubernetes Rust vLLM Inference Service - Architecture & Design

## Overview

**inference-data-plane** is a production-ready Rust-based inference service designed for high-performance, cost-efficient LLM serving on Kubernetes and Cloud Run. It provides a unified gateway for vLLM with async job processing, token budgeting, and quota enforcement.

**Repository:** https://github.com/anmho/inference-data-plane

---

## System Architecture

### Architecture Diagram

```mermaid
graph TB
    subgraph Client["External Clients"]
        Web["Web/Mobile Apps"]
        API["API Consumers"]
    end
    
    subgraph Frontend["Rust Inference Frontend"]
        Auth["Auth & Rate Limiting"]
        Budget["Token Budget Engine"]
        API_Layer["Dual API Layer<br/>OpenAI-JSON + Proto/RPC"]
    end
    
    subgraph Backend["Backend Services"]
        vLLM["vLLM GPU Server<br/>SmolLM2 135M"]
        Redis["Redis Cache<br/>Session Storage"]
    end
    
    subgraph AsyncWorker["Async Processing"]
        Temporal["Temporal Workflow<br/>Orchestration"]
        Worker["Generation Worker<br/>Rust/Temporal"]
        Queue["Task Queue"]
    end
    
    subgraph Quota["Request Control"]
        QuotaLimiter["Quota Limiter<br/>Rate Limiting"]
        TokenBudget["Token Budget<br/>Accounting"]
    end
    
    Web -->|OpenAI API| API_Layer
    API -->|Proto/RPC| API_Layer
    API_Layer -->|Check| Auth
    Auth -->|Check| Budget
    Budget -->|Check| QuotaLimiter
    QuotaLimiter -->|Valid Request| vLLM
    vLLM -->|Tokens| Redis
    Redis -->|Cache Hit| API_Layer
    
    API_Layer -->|Submit Job| Temporal
    Temporal -->|Queue Task| Queue
    Queue -->|Execute| Worker
    Worker -->|Call| vLLM
    Worker -->|Store| Redis
    
    Temporal -->|Monitor| TokenBudget
```

### Core Components

#### 1. **Rust Inference Frontend** (`rust-inference-frontdoor`)
**Repository:** [crates/rust-inference-frontdoor](https://github.com/anmho/inference-data-plane/tree/main/crates/rust-inference-frontdoor) | **Entrypoint:** [src/main.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/rust-inference-frontdoor/src/main.rs) | **Cargo:** [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/rust-inference-frontdoor/Cargo.toml)

- **Purpose:** HTTP/gRPC gateway for vLLM inference requests
- **API Layer:** Dual protocol support:
  - **OpenAI-compatible JSON/HTTP** for external clients (`/v1/chat/completions` style)
  - **Proto/RPC** for internal service-to-service communication (`application/connect+proto`) — see [inference.proto](https://github.com/anmho/inference-data-plane/blob/main/proto/inference/v1/inference.proto)
- **Features:**
  - API key authentication & quota checking via [generation-quota-limiter](https://github.com/anmho/inference-data-plane/tree/main/crates/generation-quota-limiter)
  - Token budget enforcement via [token-budget-engine](https://github.com/anmho/inference-data-plane/tree/main/crates/token-budget-engine) (pre-request validation)
  - Request/response streaming
  - Redis-backed session caching
  - Graceful error handling with structured logging
- **Dependencies:** [Axum 0.8](https://github.com/tokio-rs/axum), [Tokio](https://tokio.rs/), [Tower-HTTP](https://github.com/tower-rs/tower-http)
- **Testing:** [connect_smoke.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/rust-inference-frontdoor/src/bin/connect_smoke.rs) for smoke tests

#### 2. **Generation Worker** (`generation-worker`)
**Repository:** [crates/generation-worker](https://github.com/anmho/inference-data-plane/tree/main/crates/generation-worker) | **Entrypoint:** [src/main.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/generation-worker/src/main.rs) | **Cargo:** [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/generation-worker/Cargo.toml)

- **Purpose:** Async inference job processing via Temporal workflows
- **Components:**
  - **Local Valkey Worker** ([src/main.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/generation-worker/src/main.rs)): Redis stream consumer for local testing
  - **Cloud Run Dockerfile:** [Dockerfile](https://github.com/anmho/inference-data-plane/blob/main/crates/generation-worker/Dockerfile)
- **Features:**
  - mTLS support for Temporal (gRPC + TLS) via [temporalio SDK 0.4.0](https://github.com/temporalio/sdk-rust)
  - Wake-from-zero autoscaling via [KEDA](https://keda.sh/) — see [crema-scaledobject.yaml](https://github.com/anmho/inference-data-plane/blob/main/cloudrun/crema-scaledobject.yaml)
  - Async job queue for batch processing
  - Temporal workflow integration
- **Dependencies:** [Temporalio SDK 0.4.0](https://github.com/temporalio/sdk-rust), [Tokio](https://tokio.rs/)

#### 3. **Token Budget Engine** (`token-budget-engine`)
**Repository:** [crates/token-budget-engine](https://github.com/anmho/inference-data-plane/tree/main/crates/token-budget-engine) | **Lib:** [src/lib.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/token-budget-engine/src/lib.rs) | **Cargo:** [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/token-budget-engine/Cargo.toml)

- **Purpose:** Token accounting and budget enforcement
- **Capabilities:**
  - Approximate token counting (via [ml-tokenizers](https://docs.rs/ml-tokenizers/latest/ml_tokenizers/))
  - Budget policy enforcement (`StrictReject`, `TrialReject`)
  - Per-minute token rate limits
  - Prompt/completion token tracking
  - Test fixtures for validation ([tests/](https://github.com/anmho/inference-data-plane/tree/main/crates/token-budget-engine/tests))

#### 4. **Generation Quota Limiter** (`generation-quota-limiter`)
**Repository:** [crates/generation-quota-limiter](https://github.com/anmho/inference-data-plane/tree/main/crates/generation-quota-limiter) | **Lib:** [src/lib.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/generation-quota-limiter/src/lib.rs) | **Cargo:** [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/generation-quota-limiter/Cargo.toml)

- **Purpose:** Rate limiting and quota management
- **Features:**
  - In-memory quota tracking
  - Requests-per-minute limits
  - Token-per-minute budgeting
  - Concurrent quota checks

#### 5. **Control Plane** (`inference-control-plane`)
**Repository:** [crates/inference-control-plane](https://github.com/anmho/inference-data-plane/tree/main/crates/inference-control-plane) | **Lib:** [src/lib.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/inference-control-plane/src/lib.rs) | **Cargo:** [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/inference-control-plane/Cargo.toml)

- **Purpose:** Orchestration and lifecycle management
- **Responsibilities:**
  - Job scheduling
  - Resource allocation
  - Health monitoring
  - Policy enforcement

---

## Data Flow

### Request Path (Streaming)

```mermaid
sequenceDiagram
    participant Client
    participant Frontend as Rust Frontend
    participant Auth as Auth Layer
    participant Budget as Token Budget
    participant Quota as Quota Limiter
    participant Cache as Redis Cache
    participant vLLM as vLLM GPU

    Client->>Frontend: POST /v1/chat/completions (JSON)
    Frontend->>Auth: Verify API key
    Auth-->>Frontend: ✓ Valid
    Frontend->>Budget: Check token budget
    Budget-->>Frontend: ✓ Within limit
    Frontend->>Quota: Check rate limit
    Quota-->>Frontend: ✓ Allowed
    Frontend->>Cache: Lookup in Redis
    Cache-->>Frontend: Cache miss
    Frontend->>vLLM: Call inference
    vLLM->>vLLM: Generate tokens (streaming)
    vLLM-->>Frontend: Stream chunks (delta)
    Frontend->>Cache: Store session
    Frontend-->>Client: SSE stream response
```

### Async Path (Temporal Workers)

```mermaid
sequenceDiagram
    participant Client
    participant Frontend as Rust Frontend
    participant Temporal as Temporal Server
    participant Queue as Task Queue
    participant Worker as Generation Worker
    participant vLLM as vLLM GPU
    participant Storage as Redis/DB

    Client->>Frontend: Request with runAt/rrule
    Frontend->>Temporal: Submit workflow
    Temporal-->>Frontend: Workflow accepted
    Temporal->>Queue: Enqueue task
    Queue->>Worker: Task available
    Worker->>Temporal: Claim task
    Worker->>vLLM: Call inference
    vLLM-->>Worker: Response
    Worker->>Storage: Store result
    Worker->>Temporal: Mark complete
    Temporal-->>Frontend: Workflow complete (via poll)
```

---

## Deployment Targets

### Deployment Architecture

```mermaid
graph TB
    subgraph CloudRun["Cloud Run (Serverless)"]
        FrontendCR["Frontend Service<br/>Axum HTTP<br/>Pay-per-request"]
        vLLMCR["vLLM GPU Service<br/>Concurrent=1<br/>L4 GPU"]
        TemporalCR["Temporal Worker Pool<br/>KEDA Auto-scale<br/>0 → N replicas"]
        MemorstoreCR["Memorystore Redis<br/>Temporary<br/>Auto-cleanup"]
        FrontendCR -->|Inference| vLLMCR
        FrontendCR -->|Cache| MemorstoreCR
        TemporalCR -->|Query vLLM| vLLMCR
        TemporalCR -->|Store results| MemorstoreCR
    end
    
    subgraph GKE["Kubernetes/GKE (Cluster)"]
        FrontendK["Frontend Deployment<br/>Axum HTTP<br/>Multi-replica"]
        vLLMK["vLLM StatefulSet<br/>L4 GPU nodes<br/>High-memory"]
        TemporalK["Worker DaemonSet<br/>Pod Autoscaler<br/>Custom metrics"]
        RedisK["Redis StatefulSet<br/>Persistent<br/>Memcached HA"]
        NginxK["NGINX Ingress<br/>Load balancer<br/>Rate limiting"]
        NginxK -->|Route| FrontendK
        FrontendK -->|Inference| vLLMK
        FrontendK -->|Cache| RedisK
        TemporalK -->|Query| vLLMK
        TemporalK -->|Store| RedisK
    end
    
    subgraph External["External Services"]
        TemporalGRPC["Temporal Cloud<br/>gRPC:7233<br/>mTLS required"]
        Vault["Vault<br/>Secret storage<br/>API keys"]
        SecretMgr["GCP Secret Mgr<br/>Terraform sync<br/>Runtime injection"]
    end
    
    CloudRun -->|Register workers| TemporalGRPC
    GKE -->|Register workers| TemporalGRPC
    CloudRun -->|Fetch secrets| SecretMgr
    GKE -->|Fetch secrets| SecretMgr
    Vault -->|Sync via TF| SecretMgr
```

### Cloud Run (Serverless)
**Cloud Run Config:** [cloudrun/](https://github.com/anmho/inference-data-plane/tree/main/cloudrun)

- **Components:**
  - [frontend-service.yaml](https://github.com/anmho/inference-data-plane/blob/main/cloudrun/frontend-service.yaml) - Rust frontend service
  - [temporal-worker-pool.yaml](https://github.com/anmho/inference-data-plane/blob/main/cloudrun/temporal-worker-pool.yaml) - KEDA-managed worker pool (scales 0→N)
  - [vllm-gpu-service.yaml](https://github.com/anmho/inference-data-plane/blob/main/cloudrun/vllm-gpu-service.yaml) - vLLM GPU backend
  - [crema-scaledobject.yaml](https://github.com/anmho/inference-data-plane/blob/main/cloudrun/crema-scaledobject.yaml) - KEDA scaler configuration

- **Features:**
  - Wake-from-zero autoscaling
  - Direct VPC egress
  - Temporary Memorystore (Redis) for benchmarks
  - No idle cost when not in use

**Deployment Scripts:**
- [cloudrun-gpu-deploy.sh](https://github.com/anmho/inference-data-plane/blob/main/scripts/cloudrun-gpu-deploy.sh) - Deploy vLLM GPU backend
- [cloudrun-deploy.sh](https://github.com/anmho/inference-data-plane/blob/main/scripts/cloudrun-deploy.sh) - Deploy frontend & workers
- [cloudrun-cleanup.sh](https://github.com/anmho/inference-data-plane/blob/main/scripts/cloudrun-cleanup.sh) - Clean up resources

### Kubernetes (GKE)
**Kubernetes Config:** [k8s/](https://github.com/anmho/inference-data-plane/tree/main/k8s)

- **Manifests:**
  - Base configs: [k8s/base/](https://github.com/anmho/inference-data-plane/tree/main/k8s/base) (frontend, workers, configmaps)
    - [frontend.yaml](https://github.com/anmho/inference-data-plane/blob/main/k8s/base/frontend.yaml)
    - [inference-engine.yaml](https://github.com/anmho/inference-data-plane/blob/main/k8s/base/inference-engine.yaml)
    - [kustomization.yaml](https://github.com/anmho/inference-data-plane/blob/main/k8s/base/kustomization.yaml)
  - Overlays: 
    - [k8s/overlays/gke/](https://github.com/anmho/inference-data-plane/tree/main/k8s/overlays/gke) - Low-cost resources
    - [k8s/overlays/gke-vllm/](https://github.com/anmho/inference-data-plane/tree/main/k8s/overlays/gke-vllm) - L4 GPU tolerations

- **Features:**
  - Kustomize-based configuration management
  - Multi-environment support
  - Resource quota limits
  - GPU node affinity

**Deployment Scripts:**
- [minikube-deploy.sh](https://github.com/anmho/inference-data-plane/blob/main/scripts/minikube-deploy.sh) - Local Minikube deployment
- [k6-connect.js](https://github.com/anmho/inference-data-plane/blob/main/scripts/k6-connect.js) - Load testing with k6

---

## Token Budget & Quota System

```mermaid
graph LR
    subgraph Input["Input"]
        Req["Incoming Request<br/>with Prompt"]
    end
    
    subgraph TokenBudget["Token Budget Engine"]
        Tokenize["Tokenize Prompt<br/>ml-tokenizers"]
        EstimatePrompt["Estimate Prompt<br/>Token Count"]
        PolicyCheck["Check Policy<br/>StrictReject|TrialReject"]
        RateCheck["Check Rate Limit<br/>tokens/min"]
    end
    
    subgraph QuotaSystem["Quota Limiter"]
        RPMCheck["Check Requests/min<br/>In-memory counter"]
        TokenBudgetCheck["Check Token Budget<br/>Accounting"]
        Reserve["Reserve quota<br/>for response"]
    end
    
    subgraph Decision["Decision"]
        Accept["✓ Accept Request<br/>Execute inference"]
        Reject["✗ Reject Request<br/>Return error"]
    end
    
    subgraph Tracking["Tracking"]
        CompletionTokens["Track Completion<br/>Tokens Generated"]
        UpdateMetrics["Update Usage<br/>Metrics"]
        ReleaseQuota["Release unused<br/>quota"]
    end
    
    Req -->|Prompt| Tokenize
    Tokenize -->|Token count| EstimatePrompt
    EstimatePrompt -->|Budget OK?| PolicyCheck
    PolicyCheck -->|Valid| RateCheck
    RateCheck -->|Within RPM?| RPMCheck
    RPMCheck -->|Quota available| TokenBudgetCheck
    TokenBudgetCheck -->|Reserve tokens| Reserve
    Reserve -->|OK| Accept
    PolicyCheck -->|Exceeds policy| Reject
    RateCheck -->|Rate limit hit| Reject
    RPMCheck -->|Too many requests| Reject
    TokenBudgetCheck -->|Insufficient quota| Reject
    
    Accept -->|Inference| CompletionTokens
    CompletionTokens -->|Token count| UpdateMetrics
    UpdateMetrics -->|Finalize| ReleaseQuota
```

## Model & Configuration

### Supported Models
- **Default:** HuggingFaceTB/SmolLM2-135M-Instruct
- **Backend:** vLLM 0.6.6 (OpenAI-compatible)

### Configuration

#### Environment Variables (Frontend)
```bash
MODEL_ID=HuggingFaceTB/SmolLM2-135M-Instruct
MODEL_CONTEXT_LIMIT=8192
MODEL_DEFAULT_MAX_TOKENS=256
MODEL_MAX_OUTPUT_TOKENS=1024

REDIS_URL=redis://localhost:6379
API_KEYS_SECRET=inference-api-keys (or direct comma-separated keys)
FRONTEND_ALLOW_UNAUTH=false  # Set true for public Cloud Run access
```

#### Environment Variables (Temporal Worker)
```bash
TEMPORAL_ENABLED=true
TEMPORAL_ADDRESS=temporal-grpc.anmho.com:7233
TEMPORAL_NAMESPACE=default
TEMPORAL_TASK_QUEUE=inference-gpu
TEMPORAL_API_KEY_SECRET=temporal-api-key  # From GCP Secret Manager
TEMPORAL_TLS_CA_CERT_SECRET=temporal-ca-cert
TEMPORAL_TLS_CERT_SECRET=temporal-cert
TEMPORAL_TLS_KEY_SECRET=temporal-key
```

---

## Protocol & API Design

**Proto Definitions:** [proto/inference/v1/](https://github.com/anmho/inference-data-plane/tree/main/proto/inference/v1)

### OpenAI-Compatible API (External)
Used by external clients (web/mobile apps, API consumers). Implements OpenAI chat completion compatibility for easy client adoption.

```json
POST /v1/chat/completions
Content-Type: application/json

{
  "model": "HuggingFaceTB/SmolLM2-135M-Instruct",
  "messages": [
    {"role": "system", "content": "You are helpful."},
    {"role": "user", "content": "Hello"}
  ],
  "max_tokens": 256,
  "temperature": 0.7,
  "stream": true
}
```

See [connect_smoke.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/rust-inference-frontdoor/src/bin/connect_smoke.rs) for client usage examples.

### Internal Proto/RPC API (Service-to-Service)
Binary protocol for efficient internal communication via [Connect protocol](https://connectrpc.com/).

**Full Definition:** [inference.proto](https://github.com/anmho/inference-data-plane/blob/main/proto/inference/v1/inference.proto)

```protobuf
service InferenceService {
  rpc Generate(GenerateRequest) returns (GenerateResponse);
  rpc StreamGenerate(GenerateRequest) returns (stream GenerateChunk);
}

message GenerateRequest {
  string model = 1;
  repeated ChatMessage messages = 2;
  string prompt = 3;
  uint64 max_tokens = 4;
  float temperature = 5;
}

message GenerateChunk {
  string request_id = 1;
  string model = 2;
  string delta = 3;
  uint64 generated_tokens = 4;
  string finish_reason = 5;
}
```

---

## Deployment Flow

### Prerequisites
1. GCP project (`anmho-infra-prod`) with:
   - Cloud Run API enabled
   - Artifact Registry (Docker repository)
   - VPC with subnets
   - Service accounts with appropriate IAM roles

2. Temporal cluster with:
   - gRPC endpoint (e.g., `temporal-grpc.anmho.com:7233`)
   - API key or mTLS certificates
   - Task queue configured

3. Vault with Temporal credentials at:
   - `secret/temporal/api-key`
   - Or mTLS certificates path

### Deployment Steps

#### 1. Build & Push Images
```bash
cd /path/to/inference-data-plane
RUN_BUILD=true make cloudrun-bench-once
```

#### 2. Deploy vLLM GPU Backend
```bash
./scripts/cloudrun-gpu-deploy.sh
```

#### 3. Set Temporal Secret
```bash
TEMPORAL_API_KEY="$(vault kv get -field=value secret/temporal/api-key)"
gcloud secrets create temporal-api-key \
  --project anmho-infra-prod \
  --replication-policy automatic \
  --data-file=<(echo -n "$TEMPORAL_API_KEY")
```

#### 4. Deploy Frontend & Worker
```bash
TEMPORAL_API_KEY_SECRET=temporal-api-key make cloudrun-bench-once
```

#### 5. Run Benchmarks
```bash
# Included in bench-once:
# - Smoke tests (connect_smoke)
# - k6 load tests
# - Auto-cleanup
```

---

## Monitoring & Observability

### Metrics
- Requests per minute (frontend)
- Token rate limiting (quota limiter)
- Prompt & generated token counts (token budget engine)
- Workflow execution metrics (Temporal)
- vLLM inference latency

### Logging
- Structured JSON logging (tracing-subscriber)
- Request IDs for tracing
- Budget rejection reasons
- Temporal workflow status

### Health Checks
```bash
GET /healthz  # Frontend health
GET /readyz   # Readiness probe
```

---

## Performance Characteristics

### Latency
- **First token:** ~50-200ms (vLLM GPU inference)
- **Streaming:** Continuous delta chunks
- **Token budget check:** <1ms (in-memory)
- **Quota check:** <1ms (in-memory)

### Throughput
- **Concurrent requests:** Limited by vLLM batch size + quota
- **Token/min:** Configurable per quota policy
- **Scaling:** KEDA auto-scales workers based on queue depth

### Cost
- **Cloud Run:** Pay-per-request + CPU/memory
- **vLLM GPU:** L4 node (on-demand or spot)
- **Redis:** Temporary Memorystore during benchmarks (auto-cleanup)
- **Idle cost:** Zero (no running services)

---

## Testing & Benchmarking

**Scripts Directory:** [scripts/](https://github.com/anmho/inference-data-plane/tree/main/scripts)

### Local Testing
```bash
# Start all services (vLLM, Redis, Frontend)
./scripts/start.sh

# Run smoke tests
cargo run -q -p inference-frontend --bin connect_smoke
```

See [connect_smoke.rs](https://github.com/anmho/inference-data-plane/blob/main/crates/rust-inference-frontdoor/src/bin/connect_smoke.rs) for implementation.

### Load Testing (k6)
```bash
# Load testing with k6
./scripts/load.sh
```

Load test script: [k6-connect.js](https://github.com/anmho/inference-data-plane/blob/main/scripts/k6-connect.js)

### Cloud Run Benchmarking
```bash
# One-shot benchmark (deploy → test → cleanup)
TEMPORAL_API_KEY_SECRET=temporal-api-key make cloudrun-bench-once

# Or step-by-step:
./scripts/cloudrun-gpu-deploy.sh
./scripts/cloudrun-deploy.sh
./scripts/crema-deploy.sh
./scripts/load.sh
./scripts/cloudrun-cleanup.sh
```

**Benchmark Scripts:**
- [cloudrun-gpu-deploy.sh](https://github.com/anmho/inference-data-plane/blob/main/scripts/cloudrun-gpu-deploy.sh) - Deploy vLLM GPU backend
- [cloudrun-deploy.sh](https://github.com/anmho/inference-data-plane/blob/main/scripts/cloudrun-deploy.sh) - Deploy frontend & workers
- [crema-deploy.sh](https://github.com/anmho/inference-data-plane/blob/main/scripts/crema-deploy.sh) - Deploy KEDA scaler
- [cloudrun-cleanup.sh](https://github.com/anmho/inference-data-plane/blob/main/scripts/cloudrun-cleanup.sh) - Clean up resources
- [Makefile](https://github.com/anmho/inference-data-plane/blob/main/Makefile) - `cloudrun-bench-once` and `cloudrun-verify-idle` targets

### Idle Verification
```bash
# Verify no leftover paid resources
make cloudrun-verify-idle
```

---

## Crates & Dependencies

### Workspace Structure
**Root Manifest:** [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/Cargo.toml)

```
inference-data-plane/
├── crates/
│   ├── rust-inference-frontdoor/  # HTTP gateway (Axum)
│   │   └── [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/rust-inference-frontdoor/Cargo.toml)
│   ├── generation-worker/          # Temporal async worker
│   │   └── [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/generation-worker/Cargo.toml)
│   ├── token-budget-engine/        # Token budgeting lib
│   │   └── [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/token-budget-engine/Cargo.toml)
│   ├── generation-quota-limiter/   # Rate limiting lib
│   │   └── [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/generation-quota-limiter/Cargo.toml)
│   ├── inference-control-plane/    # Orchestration lib
│   │   └── [Cargo.toml](https://github.com/anmho/inference-data-plane/blob/main/crates/inference-control-plane/Cargo.toml)
│   └── gpu-idler/                  # GPU utility
├── proto/                          # [Protobuf definitions](https://github.com/anmho/inference-data-plane/tree/main/proto)
├── k8s/                            # [Kubernetes manifests](https://github.com/anmho/inference-data-plane/tree/main/k8s)
├── cloudrun/                       # [Cloud Run configs](https://github.com/anmho/inference-data-plane/tree/main/cloudrun)
├── scripts/                        # [Deployment automation](https://github.com/anmho/inference-data-plane/tree/main/scripts)
├── config/                         # [Configuration files](https://github.com/anmho/inference-data-plane/tree/main/config)
└── Cargo.toml                      # Workspace root
```

### Key Dependencies
- **Web:** [Axum 0.8](https://docs.rs/axum/latest/axum/), [Tower-HTTP 0.6](https://docs.rs/tower-http/latest/tower_http/)
- **Async:** [Tokio 1.x](https://tokio.rs/), [Futures 0.3](https://docs.rs/futures/latest/futures/)
- **gRPC:** [Temporalio SDK 0.4.0](https://github.com/temporalio/sdk-rust), [Tonic](https://github.com/hyperium/tonic)
- **Serialization:** [Serde](https://serde.rs/), [Prost](https://docs.rs/prost/latest/prost/)
- **Storage:** [Redis 0.32](https://docs.rs/redis/latest/redis/) (async)
- **Observability:** [Tracing](https://docs.rs/tracing/latest/tracing/), [Tracing-Subscriber](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/)
- **Token Counting:** [ml-tokenizers](https://docs.rs/ml-tokenizers/latest/ml_tokenizers/)

---

## Next Steps & Roadmap

### Immediate
- [ ] Add Terraform integration to `projects/inference/` for Vault secret syncing
- [ ] Wire up terraform-managed GCP secrets
- [ ] Set Temporal API key in anmho-infra-prod

### Short Term
- [ ] Integrate with `codex/inference-gke-atlantis` terraform branch
- [ ] Publish separate library crates (token-budget-engine, quota-limiter)
- [ ] Add more model support beyond SmolLM2

### Future
- [ ] GPU idling optimization
- [ ] Multi-tenant isolation
- [ ] Advanced monitoring dashboard
- [ ] Model fine-tuning integration

---

## References

- **GitHub:** https://github.com/anmho/inference-data-plane
- **Terraform (Kubernetes Infra):** https://github.com/anmho/terraform/tree/codex/inference-gke-atlantis
- **vLLM:** https://github.com/vllm-project/vllm
- **Temporal:** https://temporal.io
- **Cloud Run:** https://cloud.google.com/run
