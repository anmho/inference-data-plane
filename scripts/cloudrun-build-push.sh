#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
REPOSITORY="${REPOSITORY:-inference}"
TAG="${TAG:-latest}"
PLATFORM="${PLATFORM:-linux/amd64}"
REGISTRY="${REGION}-docker.pkg.dev"
IMAGE_PREFIX="${REGISTRY}/${PROJECT_ID}/${REPOSITORY}"

gcloud auth configure-docker "${REGISTRY}" --quiet

docker buildx build \
  --platform "${PLATFORM}" \
  --push \
  -t "${IMAGE_PREFIX}/inference-frontend:${TAG}" \
  -f crates/rust-inference-frontdoor/Dockerfile .

docker buildx build \
  --platform "${PLATFORM}" \
  --push \
  -t "${IMAGE_PREFIX}/inference-temporal-worker:${TAG}" \
  -f crates/generation-worker/Dockerfile.temporal .

docker buildx build \
  --platform "${PLATFORM}" \
  --push \
  -t "${IMAGE_PREFIX}/crema-autoscaler:${TAG}" \
  -f cloudrun/crema.Dockerfile .
