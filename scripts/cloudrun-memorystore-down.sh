#!/usr/bin/env bash
set -euo pipefail

DELETE_TEMP_NETWORK="${DELETE_TEMP_NETWORK:-true}" ./scripts/cloudrun-cleanup.sh
