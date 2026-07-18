#!/usr/bin/env bash
set -euo pipefail

source ./scripts/load-config.sh

BASE_URL="${BASE_URL:-http://localhost:8080}"
API_KEY="${API_KEY:-dev-key}"
MODEL="${MODEL:-${MODEL_ID:-mlx-community/SmolLM2-135M-Instruct}}"

GENERATE_VUS="${GENERATE_VUS:-${K6_GENERATE_VUS:-2}}" \
STREAM_VUS="${STREAM_VUS:-${K6_STREAM_VUS:-1}}" \
DURATION="${DURATION:-${K6_DURATION:-20s}}" \
REQUEST_TIMEOUT="${REQUEST_TIMEOUT:-${K6_REQUEST_TIMEOUT:-30s}}" \
BASE_URL="${BASE_URL}" \
API_KEY="${API_KEY}" \
MODEL="${MODEL}" \
k6 run scripts/k6-connect.js
