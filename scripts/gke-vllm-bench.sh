#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-gke_anmho-infra-prod_us-central1-c_inference}"
PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
ZONE="${ZONE:-us-central1-c}"
CLUSTER="${CLUSTER:-inference}"
LOCAL_PORT="${LOCAL_PORT:-18080}"
MODEL="${MODEL:-HuggingFaceTB/SmolLM2-135M-Instruct}"
MAX_TOKENS="${MAX_TOKENS:-64}"
CPU_NODE_POOL="${CPU_NODE_POOL:-bench-cpu}"

PORT_FORWARD_PID=""

cleanup() {
  local status=$?
  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
    wait "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi
  kubectx "${KUBE_CONTEXT}" >/dev/null || true
  kubens inference-data-plane >/dev/null || true
  kubectl --context "${KUBE_CONTEXT}" scale \
    deployment/inference-frontend \
    deployment/generation-worker \
    --replicas=0 >/dev/null 2>&1 || true
  kubectl --context "${KUBE_CONTEXT}" scale deployment/vllm --replicas=0 >/dev/null 2>&1 || true
  gcloud container clusters resize "${CLUSTER}" \
    --node-pool gpu-l4 \
    --num-nodes 0 \
    --zone "${ZONE}" \
    --project "${PROJECT_ID}" \
    --quiet >/dev/null 2>&1 || true
  gcloud container node-pools delete "${CPU_NODE_POOL}" \
    --cluster "${CLUSTER}" \
    --zone "${ZONE}" \
    --project "${PROJECT_ID}" \
    --quiet >/dev/null 2>&1 || true
  exit "${status}"
}
trap cleanup EXIT INT TERM

START_SECONDS="$(date +%s)"

if ! gcloud container node-pools describe "${CPU_NODE_POOL}" \
  --cluster "${CLUSTER}" \
  --zone "${ZONE}" \
  --project "${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud container node-pools create "${CPU_NODE_POOL}" \
    --cluster "${CLUSTER}" \
    --zone "${ZONE}" \
    --project "${PROJECT_ID}" \
    --machine-type e2-small \
    --spot \
    --num-nodes 1 \
    --disk-size 20 \
    --disk-type pd-standard \
    --service-account "inference-gke-node@${PROJECT_ID}.iam.gserviceaccount.com" \
    --quiet
fi

KUBE_CONTEXT="${KUBE_CONTEXT}" ./scripts/gke-vllm-deploy.sh
READY_SECONDS="$(date +%s)"

kubens inference-data-plane >/dev/null
kubectl --context "${KUBE_CONTEXT}" port-forward svc/inference-frontend "${LOCAL_PORT}:8080" &
PORT_FORWARD_PID="$!"

for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:${LOCAL_PORT}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo "cold_start_seconds=$((READY_SECONDS - START_SECONDS))"
BASE_URL="http://127.0.0.1:${LOCAL_PORT}" \
  MODEL="${MODEL}" \
  MAX_TOKENS="${MAX_TOKENS}" \
  cargo run -q -p inference-frontend --bin connect_smoke

BASE_URL="http://127.0.0.1:${LOCAL_PORT}" \
  MODEL="${MODEL}" \
  GENERATE_VUS="${GENERATE_VUS:-1}" \
  STREAM_VUS="${STREAM_VUS:-1}" \
  DURATION="${DURATION:-30s}" \
  REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-120s}" \
  ./scripts/load.sh
