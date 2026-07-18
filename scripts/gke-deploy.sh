#!/usr/bin/env bash
set -euo pipefail

KUBECTL=(kubectl)
if [[ -n "${KUBE_CONTEXT:-}" ]]; then
  KUBECTL+=(--context "${KUBE_CONTEXT}")
  kubectx "${KUBE_CONTEXT}" >/dev/null
fi

"${KUBECTL[@]}" apply -k k8s/overlays/gke
kubens inference-data-plane >/dev/null
"${KUBECTL[@]}" rollout status deployment/generation-worker --timeout=180s
"${KUBECTL[@]}" rollout status deployment/inference-frontend --timeout=180s
