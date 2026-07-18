#!/usr/bin/env bash
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-inference}"
MODEL="mlx-community/SmolLM2-135M-Instruct"
CONTROL_PORT_FORWARD_PID=""
FRONTEND_PORT_FORWARD_PID=""

cleanup() {
  if [[ -n "${FRONTEND_PORT_FORWARD_PID}" ]]; then kill "${FRONTEND_PORT_FORWARD_PID}" >/dev/null 2>&1 || true; fi
  if [[ -n "${CONTROL_PORT_FORWARD_PID}" ]]; then kill "${CONTROL_PORT_FORWARD_PID}" >/dev/null 2>&1 || true; fi
  ./scripts/host-agent-stop.sh || true
}
trap cleanup EXIT INT TERM

./scripts/host-agent-start.sh
./scripts/minikube-deploy.sh
kubectx "${PROFILE}" >/dev/null
kubens inference-data-plane >/dev/null

kubectl port-forward service/inference-model-control-plane 18100:18100 >.run/control-plane-port-forward.log 2>&1 &
CONTROL_PORT_FORWARD_PID=$!
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:18100/healthz >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS http://127.0.0.1:18100/healthz >/dev/null

MODEL_LOAD_STARTED_MS="$(perl -MTime::HiRes=time -e 'printf "%.0f", time * 1000')"
curl -fsS \
  -H 'Content-Type: application/json' \
  -H 'Connect-Protocol-Version: 1' \
  --data "{\"modelId\":\"${MODEL}\"}" \
  http://127.0.0.1:18100/inference.control.v1.ModelControlService/LoadModel | jq .

for _ in $(seq 1 300); do
  if curl -fsS http://127.0.0.1:18000/v1/models >/dev/null 2>&1 && \
    [[ "$(kubectl get deployment smollm2-135m-kserve -o jsonpath='{.status.conditions[?(@.type=="Available")].status}' 2>/dev/null)" == "True" ]]; then
    break
  fi
  sleep 1
done
curl -fsS http://127.0.0.1:18000/v1/models >/dev/null
kubectl wait --for=condition=Available deployment/smollm2-135m-kserve --timeout=300s
MODEL_READY_MS="$(perl -MTime::HiRes=time -e 'printf "%.0f", time * 1000')"
echo "cold_start_ms=$((MODEL_READY_MS - MODEL_LOAD_STARTED_MS))"

kubectl port-forward service/inference-frontend 8080:8080 >.run/frontend-port-forward.log 2>&1 &
FRONTEND_PORT_FORWARD_PID=$!
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8080/healthz >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS http://127.0.0.1:8080/healthz >/dev/null
BASE_URL=http://127.0.0.1:8080 API_KEY=dev-key MODEL="${MODEL}" cargo run -q -p inference-frontend --bin connect_smoke
if [[ "${RUN_BENCH:-false}" == "true" ]]; then
  BASE_URL=http://127.0.0.1:8080 API_KEY=dev-key MODEL="${MODEL}" CONFIG_FILE=./config/local.yaml ./scripts/load.sh
fi
