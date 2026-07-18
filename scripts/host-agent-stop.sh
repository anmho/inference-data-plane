#!/usr/bin/env bash
set -euo pipefail

PID_FILE=".run/mlx-host-agent.pid"
if [[ ! -f "${PID_FILE}" ]]; then
  exit 0
fi
PID="$(cat "${PID_FILE}")"
if kill -0 "${PID}" >/dev/null 2>&1; then
  kill -TERM "${PID}"
  for _ in $(seq 1 30); do
    if ! kill -0 "${PID}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi
rm -f "${PID_FILE}"
