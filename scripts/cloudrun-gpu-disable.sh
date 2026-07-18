#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-inference-vllm-gpu}"

if ! gcloud run services describe "${SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" >/dev/null 2>&1; then
  echo "Cloud Run service ${SERVICE} does not exist in ${PROJECT_ID}/${REGION}"
  exit 0
fi

gcloud run services update "${SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --gpu 0 \
  --min-instances 0 \
  --quiet
