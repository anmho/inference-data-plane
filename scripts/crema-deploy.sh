#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
CREMA_SERVICE="${CREMA_SERVICE:-crema-autoscaler}"
CREMA_IMAGE="${CREMA_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/inference/crema-autoscaler:latest}"
CREMA_SA="${CREMA_SA:-crema-sa@${PROJECT_ID}.iam.gserviceaccount.com}"
CONFIG_PARAMETER="${CONFIG_PARAMETER:-crema-config}"
CONFIG_VERSION="${CONFIG_VERSION:-$(date +%Y%m%d%H%M%S)}"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

source ./scripts/cloudrun-preflight.sh
if [[ -z "${TEMPORAL_API_KEY_SECRET:-}" ]]; then
  echo "Crema Temporal scaling requires TEMPORAL_API_KEY_SECRET in this scaffold." >&2
  echo "Rust services support mTLS, but the current Crema config wires Temporal auth with apiKeyFromEnv." >&2
  exit 1
fi
require_secret "${TEMPORAL_API_KEY_SECRET}" "${PROJECT_ID}"
require_gcloud_resource "Cloud Run worker pool inference-temporal-worker" \
  CLOUDSDK_PYTHON_SITEPACKAGES=1 gcloud run worker-pools describe inference-temporal-worker --project "${PROJECT_ID}" --region "${REGION}"

./scripts/render-cloudrun.sh cloudrun/crema-scaledobject.yaml "${tmpdir}/crema-config.yaml"

if ! gcloud iam service-accounts describe "${CREMA_SA}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud iam service-accounts create crema-sa \
    --project "${PROJECT_ID}" \
    --display-name "Crema autoscaler"
fi

if ! gcloud parametermanager parameters describe "${CONFIG_PARAMETER}" \
  --project "${PROJECT_ID}" \
  --location global >/dev/null 2>&1; then
  gcloud parametermanager parameters create "${CONFIG_PARAMETER}" \
    --project "${PROJECT_ID}" \
    --location global \
    --format yaml >/dev/null 2>&1 || true
fi

gcloud parametermanager parameters versions create "${CONFIG_VERSION}" \
  --parameter "${CONFIG_PARAMETER}" \
  --project "${PROJECT_ID}" \
  --location global \
  --payload-data-from-file "${tmpdir}/crema-config.yaml" >/dev/null

gcloud run worker-pools add-iam-policy-binding inference-temporal-worker \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --member "serviceAccount:${CREMA_SA}" \
  --role roles/run.developer \
  --quiet

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${CREMA_SA}" \
  --role roles/parametermanager.parameterViewer \
  --quiet >/dev/null

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member "serviceAccount:${CREMA_SA}" \
  --role roles/secretmanager.secretAccessor \
  --quiet >/dev/null

sleep "${IAM_PROPAGATION_SLEEP_SECONDS:-10}"

crema_secret_args=()
if [[ -n "${TEMPORAL_API_KEY_SECRET:-}" ]]; then
  crema_secret_args+=(--set-secrets "TEMPORAL_API_KEY=${TEMPORAL_API_KEY_SECRET}:latest")
fi

gcloud run deploy "${CREMA_SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --image "${CREMA_IMAGE}" \
  --service-account "${CREMA_SA}" \
  --no-allow-unauthenticated \
  --no-cpu-throttling \
  --min-instances 1 \
  --max-instances 1 \
  --set-env-vars "CREMA_CONFIG=projects/${PROJECT_ID}/locations/global/parameters/${CONFIG_PARAMETER}/versions/${CONFIG_VERSION}" \
  "${crema_secret_args[@]}" \
  --quiet
