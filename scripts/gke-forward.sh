#!/usr/bin/env bash
set -euo pipefail

KUBECTL=(kubectl)
if [[ -n "${KUBE_CONTEXT:-}" ]]; then
  KUBECTL+=(--context "${KUBE_CONTEXT}")
  kubectx "${KUBE_CONTEXT}" >/dev/null
fi

kubens inference-data-plane >/dev/null
"${KUBECTL[@]}" port-forward svc/inference-frontend 8080:8080
