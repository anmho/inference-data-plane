#!/usr/bin/env bash
set -euo pipefail

source ./scripts/load-config.sh

BASE_URL="${BASE_URL:-http://localhost:8080}"
API_KEY="${API_KEY:-dev-key}"
MODEL="${MODEL:-${MODEL_ID:-mlx-community/SmolLM2-135M-Instruct}}"
BENCHMARK_DIR="${BENCHMARK_DIR:-benchmarks}"
BENCHMARK_ID="${BENCHMARK_ID:-$(date -u +%Y%m%dT%H%M%SZ)-local-mlx}"
mkdir -p "${BENCHMARK_DIR}"

env -u K6_DURATION -u K6_VUS -u K6_ITERATIONS -u K6_STAGES \
GENERATE_VUS="${GENERATE_VUS:-${K6_GENERATE_VUS:-2}}" \
STREAM_VUS="${STREAM_VUS:-${K6_STREAM_VUS:-1}}" \
DURATION="${DURATION:-${K6_DURATION:-20s}}" \
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-${K6_REQUEST_TIMEOUT:-30s}}" \
BASE_URL="${BASE_URL}" \
API_KEY="${API_KEY}" \
MODEL="${MODEL}" \
k6 run \
  --summary-export "${BENCHMARK_DIR}/${BENCHMARK_ID}-summary.json" \
  scripts/k6-connect.js | tee "${BENCHMARK_DIR}/${BENCHMARK_ID}.txt"
