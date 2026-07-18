#!/usr/bin/env bash
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-inference}"

for _ in $(seq 1 120); do
  if docker info >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker info >/dev/null

if ! minikube status -p "${PROFILE}" >/dev/null 2>&1; then
  minikube start \
    -p "${PROFILE}" \
    --driver=docker \
    --kubernetes-version=v1.34.0 \
    --cpus=8 \
    --memory=16384 \
    --disk-size=20g
fi

kubectx "${PROFILE}" >/dev/null
kubens default >/dev/null
kubectl wait --for=condition=Ready node --all --timeout=300s
