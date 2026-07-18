#!/usr/bin/env bash
set -euo pipefail

RUN_DIR=".run"
PID_FILE="${RUN_DIR}/mlx-host-agent.pid"
LOG_FILE="${RUN_DIR}/mlx-host-agent.log"

mkdir -p "${RUN_DIR}"
if [[ -f "${PID_FILE}" ]] && kill -0 "$(cat "${PID_FILE}")" >/dev/null 2>&1; then
  echo "MLX host agent is already running"
  exit 0
fi
if lsof -nP -iTCP:18001 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port 18001 is occupied by a process this project does not own" >&2
  exit 1
fi

go -C controlplane build -o ../.run/mlx-host-agent ./cmd/mlx-host-agent
CONFIG_FILE=config/host-agent.local.yaml ./.run/mlx-host-agent >"${LOG_FILE}" 2>&1 &
echo "$!" >"${PID_FILE}"
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:18001/healthz >/dev/null 2>&1; then
    echo "MLX host agent ready"
    exit 0
  fi
  sleep 1
done
tail -80 "${LOG_FILE}" >&2
exit 1
