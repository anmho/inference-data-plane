#!/usr/bin/env bash
set -euo pipefail

source ./scripts/load-config.sh

BASE_URL="${BASE_URL:-http://localhost:8080}"
API_KEY="${API_KEY:-dev-key}"
MODEL="${MODEL:-${MODEL_ID:-mlx-community/SmolLM2-135M-Instruct}}"

echo "health:"
curl -sS "${BASE_URL}/healthz"
echo

BASE_URL="${BASE_URL}" API_KEY="${API_KEY}" MODEL="${MODEL}" \
  cargo run -q -p inference-frontend --bin connect_smoke
