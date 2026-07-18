#!/usr/bin/env bash
set -euo pipefail

source ./scripts/load-config.sh

CONFIG_FILE="${CONFIG_FILE}" docker compose up --build -d
./scripts/e2e.sh
