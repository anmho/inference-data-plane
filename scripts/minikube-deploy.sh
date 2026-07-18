#!/usr/bin/env bash
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-inference}"

./scripts/minikube-start.sh
./scripts/kserve-install.sh

kubectx "${PROFILE}" >/dev/null
kubectl apply -f k8s/base/namespace.yaml
kubens inference-data-plane >/dev/null

eval "$(minikube -p "${PROFILE}" docker-env)"
docker build -t inference-data-plane/frontend:dev -f crates/inference-frontend/Dockerfile .
docker build -t inference-data-plane/inference-engine:dev -f crates/inference-engine/Dockerfile .
docker build -t inference-data-plane/model-control-plane:dev -f controlplane/Dockerfile .

kubectl apply -k k8s/overlays/local
kubectl apply -f models/smollm2-local.yaml
kubectl rollout restart \
  deployment/inference-frontend \
  deployment/inference-engine \
  deployment/inference-model-control-plane
kubectl rollout status deployment/inference-frontend --timeout=300s
kubectl rollout status deployment/inference-engine --timeout=300s
kubectl rollout status deployment/inference-model-control-plane --timeout=300s

if kubectl get service --all-namespaces -o jsonpath='{range .items[?(@.spec.type=="LoadBalancer")]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}' | grep -q .; then
  echo "Refusing local deployment: a public LoadBalancer service exists" >&2
  exit 1
fi
if kubectl get pvc --all-namespaces -o name | grep -q .; then
  echo "Refusing local deployment: a PVC exists" >&2
  exit 1
fi
