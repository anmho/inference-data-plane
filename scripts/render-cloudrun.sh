#!/usr/bin/env bash
set -euo pipefail

template="$1"
output="$2"

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
TEMPORAL_ADDRESS="${TEMPORAL_ADDRESS:-temporal-grpc.anmho.com:7233}"
TEMPORAL_NAMESPACE="${TEMPORAL_NAMESPACE:-default}"
SUBNET="${SUBNET:-inference-bench-egress}"
GPU_BACKEND_URL="${GPU_BACKEND_URL:-}"
TEMPORAL_SECRET_ENV=""
CREMA_TEMPORAL_API_KEY_FROM_ENV=""

if [[ -z "${GPU_BACKEND_URL}" ]]; then
  GPU_BACKEND_URL="$(gcloud run services describe inference-vllm-gpu \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --format='value(status.url)' 2>/dev/null || true)"
fi

append_secret_env() {
  local env_name="$1"
  local secret_name="$2"
  if [[ -n "${secret_name}" ]]; then
    TEMPORAL_SECRET_ENV="${TEMPORAL_SECRET_ENV}
            - name: ${env_name}
              valueFrom:
                secretKeyRef:
                  name: ${secret_name}
                  key: latest"
  fi
}

append_secret_env TEMPORAL_API_KEY "${TEMPORAL_API_KEY_SECRET:-}"
append_secret_env TEMPORAL_TLS_CA_CERT "${TEMPORAL_TLS_CA_CERT_SECRET:-}"
append_secret_env TEMPORAL_TLS_CERT "${TEMPORAL_TLS_CERT_SECRET:-}"
append_secret_env TEMPORAL_TLS_KEY "${TEMPORAL_TLS_KEY_SECRET:-}"
append_secret_env API_KEYS "${API_KEYS_SECRET:-}"

if [[ -n "${TEMPORAL_API_KEY_SECRET:-}" ]]; then
  CREMA_TEMPORAL_API_KEY_FROM_ENV="apiKeyFromEnv: TEMPORAL_API_KEY"
fi

tmp_render="$(mktemp)"
trap 'rm -f "${tmp_render}"' EXIT

sed \
  -e "s/PROJECT_ID/${PROJECT_ID}/g" \
  -e "s/REGION/${REGION}/g" \
  -e "s|__TEMPORAL_ADDRESS__|${TEMPORAL_ADDRESS}|g" \
  -e "s|__TEMPORAL_NAMESPACE__|${TEMPORAL_NAMESPACE}|g" \
  -e "s|__SUBNET__|${SUBNET}|g" \
  -e "s|__GPU_BACKEND_URL__|${GPU_BACKEND_URL}|g" \
  "${template}" > "${tmp_render}"

TEMPORAL_SECRET_ENV="${TEMPORAL_SECRET_ENV}" \
CREMA_TEMPORAL_API_KEY_FROM_ENV="${CREMA_TEMPORAL_API_KEY_FROM_ENV}" \
ruby -0777 -pe '
  gsub("__TEMPORAL_SECRET_ENV__", ENV.fetch("TEMPORAL_SECRET_ENV", ""))
  gsub("__CREMA_TEMPORAL_API_KEY_FROM_ENV__", ENV.fetch("CREMA_TEMPORAL_API_KEY_FROM_ENV", ""))
' "${tmp_render}" > "${output}"
