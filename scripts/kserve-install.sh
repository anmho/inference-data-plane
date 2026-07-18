#!/usr/bin/env bash
set -euo pipefail

PROFILE="${MINIKUBE_PROFILE:-inference}"
KSERVE_VERSION="v0.18.0"
CERT_MANAGER_VERSION="v1.17.0"
GATEWAY_API_VERSION="v1.4.1"
GIE_VERSION="v1.3.1"
LWS_VERSION="v0.8.0"
CACHE_DIR=".cache/kserve/${KSERVE_VERSION}"

kubectx "${PROFILE}" >/dev/null

if ! kubectl get crd certificates.cert-manager.io >/dev/null 2>&1; then
  kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
fi
kubens cert-manager >/dev/null
kubectl rollout status deployment/cert-manager --timeout=300s
kubectl rollout status deployment/cert-manager-cainjector --timeout=300s
kubectl rollout status deployment/cert-manager-webhook --timeout=300s

if ! kubectl get crd gateways.gateway.networking.k8s.io >/dev/null 2>&1; then
  kubectl apply -f "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml"
fi
if ! kubectl get crd inferencepools.inference.networking.x-k8s.io >/dev/null 2>&1; then
  kubectl apply -f "https://github.com/kubernetes-sigs/gateway-api-inference-extension/releases/download/${GIE_VERSION}/manifests.yaml"
fi
if ! kubectl get crd leaderworkersets.leaderworkerset.x-k8s.io >/dev/null 2>&1; then
  kubectl apply --server-side -f "https://github.com/kubernetes-sigs/lws/releases/download/${LWS_VERSION}/manifests.yaml"
fi

if [[ ! -d "${CACHE_DIR}/.git" ]]; then
  mkdir -p "$(dirname "${CACHE_DIR}")"
  git clone --depth 1 --branch "${KSERVE_VERSION}" https://github.com/kserve/kserve.git "${CACHE_DIR}"
fi

kubectl apply --server-side --force-conflicts -k "${CACHE_DIR}/config/crd/full/clusterstoragecontainer"
kubectl apply --server-side --force-conflicts -k "${CACHE_DIR}/config/crd/full/llmisvc"
kubectl wait --for=condition=Established crd/clusterstoragecontainers.serving.kserve.io --timeout=120s
kubectl wait --for=condition=Established crd/llminferenceservices.serving.kserve.io --timeout=120s
kubectl apply --server-side --force-conflicts -k "${CACHE_DIR}/config/overlays/standalone/llmisvc"
kubectl apply --server-side -k "${CACHE_DIR}/config/llmisvcconfig"
# The v0.18 source overlay references the mutable `latest` tag. Pin the running
# controller to the matching release so its API expectations match the CRDs.
kubectl -n kserve set image deployment/llmisvc-controller-manager \
  manager="kserve/llmisvc-controller:${KSERVE_VERSION}"
kubens kserve >/dev/null
kubectl rollout status deployment/llmisvc-controller-manager --timeout=300s

# This local path uses the generated workload ClusterIP directly. It intentionally
# does not install Envoy Gateway, MetalLB, a Gateway, or a public LoadBalancer.
if kubectl get service --all-namespaces -o jsonpath='{range .items[?(@.spec.type=="LoadBalancer")]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}' | grep -q .; then
  echo "KServe dependency installation created an unexpected LoadBalancer" >&2
  exit 1
fi
