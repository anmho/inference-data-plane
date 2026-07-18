#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-gke_anmho-infra-prod_us-central1-c_inference}"
PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
ZONE="${ZONE:-us-central1-c}"
CLUSTER="${CLUSTER:-inference}"

diagnose_vllm_rollout() {
  echo "vLLM did not become ready. Current pods:" >&2
  kubectl --context "${KUBE_CONTEXT}" get pods -o wide >&2 || true
  echo "Recent namespace events:" >&2
  kubectl --context "${KUBE_CONTEXT}" get events --sort-by=.lastTimestamp | tail -40 >&2 || true
  local pod
  pod="$(kubectl --context "${KUBE_CONTEXT}" get pods \
    -l app=vllm \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${pod}" ]]; then
    echo "vLLM pod details:" >&2
    kubectl --context "${KUBE_CONTEXT}" describe pod "${pod}" >&2 || true
  fi
}

gcloud container clusters get-credentials "${CLUSTER}" \
  --zone "${ZONE}" \
  --project "${PROJECT_ID}"
kubectx "${KUBE_CONTEXT}" >/dev/null

kubectl --context "${KUBE_CONTEXT}" apply -k k8s/overlays/gke-vllm
kubens inference-data-plane >/dev/null
kubectl --context "${KUBE_CONTEXT}" rollout status deployment/generation-worker --timeout=600s
kubectl --context "${KUBE_CONTEXT}" rollout status deployment/inference-frontend --timeout=600s
if ! kubectl --context "${KUBE_CONTEXT}" rollout status deployment/vllm --timeout=900s; then
  diagnose_vllm_rollout
  exit 1
fi
