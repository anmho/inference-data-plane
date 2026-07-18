#!/usr/bin/env bash
set -euo pipefail

source ./scripts/load-config.sh

if [[ "${BACKEND:-mlx}" != "mlx" ]]; then
  echo "Skipping MLX server for BACKEND=${BACKEND}"
  exit 0
fi

MODEL="${MODEL:-${MODEL_ID:-mlx-community/SmolLM2-135M-Instruct}}"
HOST="${MLX_HOST:-127.0.0.1}"
PORT="${MLX_PORT:-18000}"
RUN_DIR=".run"
LOG_FILE="${RUN_DIR}/mlx-lm.log"
PID_FILE="${RUN_DIR}/mlx-lm.pid"

mkdir -p "${RUN_DIR}"

if lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "MLX server already listening on ${HOST}:${PORT}"
  curl -sS "http://${HOST}:${PORT}/v1/models" | jq .
  exit 0
fi

if ! command -v mlx_lm.server >/dev/null 2>&1; then
  echo "mlx_lm.server not found. Install it with: uv tool install mlx-lm" >&2
  exit 1
fi

echo "Starting MLX server for ${MODEL} on ${HOST}:${PORT}"
nohup mlx_lm.server \
  --model "${MODEL}" \
  --host "${HOST}" \
  --port "${PORT}" \
  >"${LOG_FILE}" 2>&1 &
echo "$!" > "${PID_FILE}"

for _ in $(seq 1 120); do
  if curl -fsS "http://${HOST}:${PORT}/v1/models" >/dev/null 2>&1; then
    echo "MLX server ready"
    curl -sS "http://${HOST}:${PORT}/v1/models" | jq .
    exit 0
  fi
  sleep 1
done

echo "MLX server did not become ready. Recent logs:" >&2
tail -80 "${LOG_FILE}" >&2
exit 1
