#!/usr/bin/env bash
set -euo pipefail

source ./scripts/load-config.sh

./scripts/model-start.sh
./scripts/stack-start.sh
