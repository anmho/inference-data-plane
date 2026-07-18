#!/usr/bin/env bash
set -euo pipefail

NAME="inference-valkey-test"
cleanup() {
  docker rm -f "${NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup
docker run --rm -d --name "${NAME}" -p 16379:6379 valkey/valkey:9.1.0-alpine >/dev/null
for _ in $(seq 1 30); do
  if docker exec "${NAME}" valkey-cli ping 2>/dev/null | grep -q PONG; then break; fi
  sleep 1
done
docker exec "${NAME}" valkey-cli ping | grep -q PONG
VALKEY_TEST_URL=redis://127.0.0.1:16379 cargo test -p inference-streams --test valkey
