#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
ZONE="${ZONE:-us-central1-c}"
CLUSTER="${CLUSTER:-inference}"
KUBE_CONTEXT="${KUBE_CONTEXT:-gke_${PROJECT_ID}_${ZONE}_${CLUSTER}}"
SHUTDOWN_DELAY_SECONDS="${SHUTDOWN_DELAY_SECONDS:-1}"

if [[ "${SHUTDOWN_DELAY_SECONDS}" != "0" ]]; then
  sleep "${SHUTDOWN_DELAY_SECONDS}"
fi

gcloud container clusters get-credentials "${CLUSTER}" \
  --zone "${ZONE}" \
  --project "${PROJECT_ID}"
kubectx "${KUBE_CONTEXT}" >/dev/null
kubens inference-data-plane >/dev/null

kubectl scale deployment/inference-frontend --replicas=0
kubectl scale deployment/generation-worker --replicas=0
kubectl scale deployment/vllm --replicas=0 2>/dev/null || true
