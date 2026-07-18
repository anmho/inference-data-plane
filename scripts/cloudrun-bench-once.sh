#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
CLEANUP_AFTER="${CLEANUP_AFTER:-true}"
RUN_BUILD="${RUN_BUILD:-false}"
RUN_K6="${RUN_K6:-true}"
MODEL="${MODEL:-HuggingFaceTB/SmolLM2-135M-Instruct}"
MAX_TOKENS="${MAX_TOKENS:-64}"
K6_DURATION="${K6_DURATION:-30s}"
K6_GENERATE_VUS="${K6_GENERATE_VUS:-1}"
K6_STREAM_VUS="${K6_STREAM_VUS:-1}"
API_KEYS_SECRET="${API_KEYS_SECRET:-inference-api-keys}"
export API_KEYS_SECRET

cleanup() {
  local status=$?
  if [[ "${CLEANUP_AFTER}" == "true" ]]; then
    ./scripts/cloudrun-cleanup.sh || true
  fi
  exit "${status}"
}
trap cleanup EXIT

if [[ -z "${TEMPORAL_API_KEY_SECRET:-}" ]]; then
  echo "TEMPORAL_API_KEY_SECRET is required for Crema wake-from-zero benchmarking." >&2
  echo "Example: TEMPORAL_API_KEY_SECRET=temporal-api-key make cloudrun-bench-once" >&2
  exit 1
fi

source ./scripts/cloudrun-preflight.sh
require_cloudrun_bench_prereqs "${PROJECT_ID}" "${REGION}" inference
require_temporal_auth "${PROJECT_ID}"

secret_purpose="$(gcloud secrets describe "${API_KEYS_SECRET}" \
  --project "${PROJECT_ID}" \
  --format='value(labels.purpose)' 2>/dev/null || true)"
if [[ -z "${secret_purpose}" ]]; then
  API_KEY="${API_KEY:-$(openssl rand -hex 16)}"
  printf "%s" "${API_KEY}" | gcloud secrets create "${API_KEYS_SECRET}" \
    --project "${PROJECT_ID}" \
    --replication-policy automatic \
    --labels "app=inference,purpose=temporary_benchmark" \
    --data-file=- >/dev/null
elif [[ "${secret_purpose}" == "temporary_benchmark" ]]; then
  API_KEY="${API_KEY:-$(gcloud secrets versions access latest --secret "${API_KEYS_SECRET}" --project "${PROJECT_ID}")}"
else
  if [[ -z "${API_KEY:-}" ]]; then
    API_KEY="$(gcloud secrets versions access latest --secret "${API_KEYS_SECRET}" --project "${PROJECT_ID}")"
  fi
fi
export API_KEY

if [[ "${RUN_BUILD}" == "true" ]]; then
  ./scripts/cloudrun-build-push.sh
fi

./scripts/cloudrun-memorystore-up.sh >/dev/null
./scripts/cloudrun-gpu-deploy.sh >/dev/null
GPU_BACKEND_URL="$(gcloud run services describe inference-vllm-gpu \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --format='value(status.url)')"
export GPU_BACKEND_URL
export SUBNET="${SUBNET:-inference-bench-egress}"
export FRONTEND_ALLOW_UNAUTH=true

frontend_url="$(./scripts/cloudrun-deploy.sh)"
./scripts/crema-deploy.sh

echo "frontend_url=${frontend_url}"
echo "smoke_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
BASE_URL="${frontend_url}" \
MODEL="${MODEL}" \
MAX_TOKENS="${MAX_TOKENS}" \
API_KEY="${API_KEY}" \
cargo run -q -p inference-frontend --bin connect_smoke
echo "smoke_end=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ "${RUN_K6}" == "true" ]]; then
  BASE_URL="${frontend_url}" \
  MODEL="${MODEL}" \
  API_KEY="${API_KEY}" \
  K6_DURATION="${K6_DURATION}" \
  K6_GENERATE_VUS="${K6_GENERATE_VUS}" \
  K6_STREAM_VUS="${K6_STREAM_VUS}" \
  ./scripts/load.sh
fi
