#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-inference-vllm-gpu}"
IMAGE="${IMAGE:-vllm/vllm-openai:v0.6.6}"
MODEL="${MODEL:-HuggingFaceTB/SmolLM2-135M-Instruct}"
GPU_TYPE="${GPU_TYPE:-nvidia-l4}"
CPU="${CPU:-4}"
MEMORY="${MEMORY:-16Gi}"
MAX_INSTANCES="${MAX_INSTANCES:-1}"
CONCURRENCY="${CONCURRENCY:-4}"

gcloud run deploy "${SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --port 8000 \
  --cpu "${CPU}" \
  --memory "${MEMORY}" \
  --no-cpu-throttling \
  --gpu 1 \
  --gpu-type "${GPU_TYPE}" \
  --no-gpu-zonal-redundancy \
  --min-instances 0 \
  --max-instances "${MAX_INSTANCES}" \
  --concurrency "${CONCURRENCY}" \
  --ingress internal-and-cloud-load-balancing \
  --args="--model,${MODEL},--host,0.0.0.0,--port,8000,--gpu-memory-utilization,0.75,--max-model-len,4096" \
  --quiet

gcloud run services describe "${SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --format='value(status.url)'
