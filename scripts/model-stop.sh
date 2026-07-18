#!/usr/bin/env bash
set -euo pipefail

source ./scripts/load-config.sh

PORT="${MLX_PORT:-18000}"
PID_FILE=".run/mlx-lm.pid"

if [[ -f "${PID_FILE}" ]]; then
  PID="$(cat "${PID_FILE}")"
  if kill -0 "${PID}" >/dev/null 2>&1; then
    echo "Stopping MLX server pid ${PID}"
    kill "${PID}"
  fi
  rm -f "${PID_FILE}"
fi

PIDS="$(lsof -tiTCP:"${PORT}" -sTCP:LISTEN || true)"
if [[ -n "${PIDS}" ]]; then
  echo "Stopping remaining listener(s) on port ${PORT}: ${PIDS}"
  kill ${PIDS}
fi
