#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
ZONE="${ZONE:-us-central1-c}"
CLUSTER="${CLUSTER:-inference}"

gcloud container clusters get-credentials "${CLUSTER}" \
  --zone "${ZONE}" \
  --project "${PROJECT_ID}"
