#!/usr/bin/env bash
set -euo pipefail

CONFIG_FILE="${CONFIG_FILE:-${CONFIG:-config/local.yaml}}"
export CONFIG_FILE

if [[ -f "${CONFIG_FILE}" ]]; then
  eval "$(./scripts/export-config.rb)"
  export FRONTEND_BIND_ADDR REDIS_URL MODEL_ID BACKEND MLX_BASE_URL SYNTHETIC_TOKEN_DELAY_MS
  export API_KEY K6_GENERATE_VUS K6_STREAM_VUS K6_DURATION K6_REQUEST_TIMEOUT STREAM_MAX_TOKENS
fi
