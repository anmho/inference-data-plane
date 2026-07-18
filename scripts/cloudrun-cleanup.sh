#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
DELETE_TEMP_NETWORK="${DELETE_TEMP_NETWORK:-true}"
DELETE_CREMA_CONFIG="${DELETE_CREMA_CONFIG:-false}"
DELETE_TEMP_SECRET="${DELETE_TEMP_SECRET:-true}"

delete_run_service() {
  local service="$1"
  if gcloud run services describe "${service}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" >/dev/null 2>&1; then
    gcloud run services delete "${service}" \
      --project "${PROJECT_ID}" \
      --region "${REGION}" \
      --quiet
  fi
}

delete_worker_pool() {
  local pool="$1"
  if CLOUDSDK_PYTHON_SITEPACKAGES=1 gcloud run worker-pools describe "${pool}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" >/dev/null 2>&1; then
    CLOUDSDK_PYTHON_SITEPACKAGES=1 gcloud run worker-pools delete "${pool}" \
      --project "${PROJECT_ID}" \
      --region "${REGION}" \
      --quiet
  fi
}

delete_run_service inference-frontend
delete_run_service inference-vllm-gpu
delete_run_service crema-autoscaler
delete_worker_pool inference-temporal-worker

if gcloud memorystore instances describe inference-bench-valkey \
  --project "${PROJECT_ID}" \
  --location "${REGION}" >/dev/null 2>&1; then
  gcloud memorystore instances delete inference-bench-valkey \
    --project "${PROJECT_ID}" \
    --location "${REGION}" \
    --quiet
fi

if [[ "${DELETE_TEMP_NETWORK}" == "true" ]]; then
  if gcloud compute networks vpc-access connectors describe inference-bench-vpc \
    --project "${PROJECT_ID}" \
    --region "${REGION}" >/dev/null 2>&1; then
    gcloud compute networks vpc-access connectors delete inference-bench-vpc \
      --project "${PROJECT_ID}" \
      --region "${REGION}" \
      --quiet
  fi

  gcloud network-connectivity service-connection-policies delete inference-bench-memorystore \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --quiet >/dev/null 2>&1 || true

  gcloud compute networks subnets delete inference-bench-egress \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --quiet >/dev/null 2>&1 || true

  gcloud compute networks subnets delete inference-bench-psc \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --quiet >/dev/null 2>&1 || true
fi

if [[ "${DELETE_CREMA_CONFIG}" == "true" ]]; then
  gcloud parametermanager parameters delete crema-config \
    --project "${PROJECT_ID}" \
    --location global \
    --quiet >/dev/null 2>&1 || true
fi

if [[ "${DELETE_TEMP_SECRET}" == "true" ]]; then
  secret_purpose="$(gcloud secrets describe inference-valkey-url \
    --project "${PROJECT_ID}" \
    --format='value(labels.purpose)' 2>/dev/null || true)"
  if [[ "${secret_purpose}" == "temporary_benchmark" ]]; then
    gcloud secrets delete inference-valkey-url \
      --project "${PROJECT_ID}" \
      --quiet >/dev/null 2>&1 || true
  fi

  api_secret_purpose="$(gcloud secrets describe inference-api-keys \
    --project "${PROJECT_ID}" \
    --format='value(labels.purpose)' 2>/dev/null || true)"
  if [[ "${api_secret_purpose}" == "temporary_benchmark" ]]; then
    gcloud secrets delete inference-api-keys \
      --project "${PROJECT_ID}" \
      --quiet >/dev/null 2>&1 || true
  fi
fi

./scripts/cloudrun-verify-idle.sh
