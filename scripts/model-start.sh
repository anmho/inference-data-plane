#!/usr/bin/env bash
set -euo pipefail

./scripts/host-agent-start.sh
echo "The host agent owns MLX. Load a catalogued model through ModelControlService.LoadModel."
