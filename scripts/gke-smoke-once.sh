#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
ZONE="${ZONE:-us-central1-c}"
CLUSTER="${CLUSTER:-inference}"
KUBE_CONTEXT="${KUBE_CONTEXT:-gke_${PROJECT_ID}_${ZONE}_${CLUSTER}}"
LOCAL_PORT="${LOCAL_PORT:-18080}"
MODEL="${MODEL:-synthetic-fast-001}"
MAX_TOKENS="${MAX_TOKENS:-96}"
SHUTDOWN_DELAY_SECONDS="${SHUTDOWN_DELAY_SECONDS:-1}"

PORT_FORWARD_PID=""

shutdown() {
  local status=$?
  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi
  SHUTDOWN_DELAY_SECONDS="${SHUTDOWN_DELAY_SECONDS}" \
    PROJECT_ID="${PROJECT_ID}" \
    ZONE="${ZONE}" \
    CLUSTER="${CLUSTER}" \
    KUBE_CONTEXT="${KUBE_CONTEXT}" \
    ./scripts/gke-shutdown.sh
  exit "${status}"
}
trap shutdown EXIT INT TERM

gcloud container clusters get-credentials "${CLUSTER}" \
  --zone "${ZONE}" \
  --project "${PROJECT_ID}"
kubectx "${KUBE_CONTEXT}" >/dev/null

KUBE_CONTEXT="${KUBE_CONTEXT}" ./scripts/gke-deploy.sh

kubens inference-data-plane >/dev/null
kubectl --context "${KUBE_CONTEXT}" port-forward svc/inference-frontend "${LOCAL_PORT}:8080" &
PORT_FORWARD_PID="$!"

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${LOCAL_PORT}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

BASE_URL="http://127.0.0.1:${LOCAL_PORT}" \
  MODEL="${MODEL}" \
  MAX_TOKENS="${MAX_TOKENS}" \
  cargo run -q -p inference-frontend --bin connect_smoke
