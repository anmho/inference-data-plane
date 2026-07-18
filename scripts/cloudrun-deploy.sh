#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
SUBNET="${SUBNET:-inference-bench-egress}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

source ./scripts/cloudrun-preflight.sh

require_secret inference-valkey-url "${PROJECT_ID}"
if [[ -n "${API_KEYS_SECRET:-}" ]]; then
  require_secret "${API_KEYS_SECRET}" "${PROJECT_ID}"
fi
require_temporal_auth "${PROJECT_ID}"
require_gcloud_resource "Cloud Run runtime service account inference-runtime@${PROJECT_ID}.iam.gserviceaccount.com" \
  gcloud iam service-accounts describe "inference-runtime@${PROJECT_ID}.iam.gserviceaccount.com" --project "${PROJECT_ID}"
require_gcloud_resource "subnet ${SUBNET} in ${REGION}" \
  gcloud compute networks subnets describe "${SUBNET}" --project "${PROJECT_ID}" --region "${REGION}"

GPU_BACKEND_URL="${GPU_BACKEND_URL:-$(gcloud run services describe inference-vllm-gpu \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --format='value(status.url)' 2>/dev/null || true)}"
if [[ -z "${GPU_BACKEND_URL}" ]]; then
  echo "GPU_BACKEND_URL is required. Deploy inference-vllm-gpu first or set GPU_BACKEND_URL." >&2
  exit 1
fi
export GPU_BACKEND_URL

./scripts/render-cloudrun.sh cloudrun/frontend-service.yaml "${tmpdir}/frontend.yaml"

gcloud run services replace "${tmpdir}/frontend.yaml" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --quiet

if [[ "${FRONTEND_ALLOW_UNAUTH:-false}" == "true" ]]; then
  gcloud run services add-iam-policy-binding inference-frontend \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --member allUsers \
    --role roles/run.invoker \
    --quiet >/dev/null
fi

worker_secret_args=(--set-secrets REDIS_URL=inference-valkey-url:latest)
if [[ -n "${TEMPORAL_API_KEY_SECRET:-}" ]]; then
  worker_secret_args+=(--set-secrets "TEMPORAL_API_KEY=${TEMPORAL_API_KEY_SECRET}:latest")
fi
if [[ -n "${TEMPORAL_TLS_CA_CERT_SECRET:-}" ]]; then
  worker_secret_args+=(--set-secrets "TEMPORAL_TLS_CA_CERT=${TEMPORAL_TLS_CA_CERT_SECRET}:latest")
fi
if [[ -n "${TEMPORAL_TLS_CERT_SECRET:-}" ]]; then
  worker_secret_args+=(--set-secrets "TEMPORAL_TLS_CERT=${TEMPORAL_TLS_CERT_SECRET}:latest")
fi
if [[ -n "${TEMPORAL_TLS_KEY_SECRET:-}" ]]; then
  worker_secret_args+=(--set-secrets "TEMPORAL_TLS_KEY=${TEMPORAL_TLS_KEY_SECRET}:latest")
fi

CLOUDSDK_PYTHON_SITEPACKAGES=1 gcloud run worker-pools deploy inference-temporal-worker \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${REGION}-docker.pkg.dev/${PROJECT_ID}/inference/inference-temporal-worker:latest" \
  --command inference-temporal-worker \
  --cpu 2 \
  --memory 1Gi \
  --instances 0 \
  --service-account "inference-runtime@${PROJECT_ID}.iam.gserviceaccount.com" \
  --network default \
  --subnet "${SUBNET}" \
  --vpc-egress private-ranges-only \
  --set-env-vars "TEMPORAL_ENABLED=true,TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-temporal-grpc.anmho.com:7233},TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE:-default},TEMPORAL_TASK_QUEUE=inference-gpu,GPU_BACKEND_URL=${GPU_BACKEND_URL},BACKEND=vllm,MODEL_ID=HuggingFaceTB/SmolLM2-135M-Instruct" \
  "${worker_secret_args[@]}" \
  --quiet

gcloud run services describe inference-frontend \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --format='value(status.url)'
