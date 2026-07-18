#!/usr/bin/env bash
set -euo pipefail

minikube start
eval "$(minikube docker-env)"

docker build -t inference-data-plane/frontend:dev -f crates/rust-inference-frontdoor/Dockerfile .
docker build -t inference-data-plane/generation-worker:dev -f crates/generation-worker/Dockerfile .

kubectl apply -k k8s/base
kubens inference-data-plane >/dev/null
kubectl rollout restart deployment/inference-frontend deployment/generation-worker
kubectl rollout status deployment/generation-worker --timeout=120s
kubectl rollout status deployment/inference-frontend --timeout=120s

echo "Run in another shell:"
echo "kubens inference-data-plane && kubectl port-forward svc/inference-frontend 8080:8080"
echo "Then:"
echo "BASE_URL=http://localhost:8080 ./scripts/e2e.sh"
