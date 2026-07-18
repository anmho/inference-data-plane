#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
NETWORK="${NETWORK:-default}"
EGRESS_SUBNET="${EGRESS_SUBNET:-inference-bench-egress}"
EGRESS_RANGE="${EGRESS_RANGE:-10.12.0.0/26}"
PSC_SUBNET="${PSC_SUBNET:-inference-bench-psc}"
PSC_RANGE="${PSC_RANGE:-10.12.1.0/24}"
POLICY="${POLICY:-inference-bench-memorystore}"
INSTANCE="${INSTANCE:-inference-bench-valkey}"
SECRET="${SECRET:-inference-valkey-url}"
NODE_TYPE="${NODE_TYPE:-shared-core-nano}"
SHARD_COUNT="${SHARD_COUNT:-1}"
REPLICA_COUNT="${REPLICA_COUNT:-0}"
ALLOW_SECRET_OVERWRITE="${ALLOW_SECRET_OVERWRITE:-false}"

network_uri="projects/${PROJECT_ID}/global/networks/${NETWORK}"
egress_subnet_uri="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${EGRESS_SUBNET}"
psc_subnet_uri="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${PSC_SUBNET}"

if ! gcloud compute networks subnets describe "${EGRESS_SUBNET}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" >/dev/null 2>&1; then
  gcloud compute networks subnets create "${EGRESS_SUBNET}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --network "${NETWORK}" \
    --range "${EGRESS_RANGE}" \
    --enable-private-ip-google-access \
    --quiet
fi

if ! gcloud compute networks subnets describe "${PSC_SUBNET}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" >/dev/null 2>&1; then
  gcloud compute networks subnets create "${PSC_SUBNET}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --network "${NETWORK}" \
    --range "${PSC_RANGE}" \
    --purpose PRIVATE_SERVICE_CONNECT \
    --quiet
fi

if ! gcloud network-connectivity service-connection-policies describe "${POLICY}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" >/dev/null 2>&1; then
  gcloud network-connectivity service-connection-policies create "${POLICY}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --network "${network_uri}" \
    --service-class gcp-memorystore \
    --subnets "${psc_subnet_uri}" \
    --psc-connection-limit 4 \
    --producer-instance-location none \
    --quiet
fi

if ! gcloud memorystore instances describe "${INSTANCE}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}" >/dev/null 2>&1; then
  gcloud memorystore instances create "${INSTANCE}" \
    --project "${PROJECT_ID}" \
    --location "${REGION}" \
    --mode cluster-disabled \
    --node-type "${NODE_TYPE}" \
    --shard-count "${SHARD_COUNT}" \
    --replica-count "${REPLICA_COUNT}" \
    --authorization-mode auth-disabled \
    --transit-encryption-mode transit-encryption-disabled \
    --psc-auto-connections "network=${network_uri},projectId=${PROJECT_ID}" \
    --labels "app=inference,purpose=temporary_benchmark" \
    --quiet
fi

json="$(gcloud memorystore instances describe "${INSTANCE}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --format=json)"

host="$(MEMORYSTORE_JSON="${json}" ruby -rjson -e '
  data = JSON.parse(ENV.fetch("MEMORYSTORE_JSON"))
  stack = [data]
  until stack.empty?
    value = stack.pop
    case value
    when Hash
      if value["ipAddress"].to_s != ""
        puts value["ipAddress"]
        exit
      end
      stack.concat(value.values)
    when Array
      stack.concat(value)
    end
  end
')"

if [[ -z "${host}" ]]; then
  echo "Could not find a Memorystore PSC ipAddress for ${INSTANCE}." >&2
  exit 1
fi

url="redis://${host}:6379"
if gcloud secrets describe "${SECRET}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  secret_purpose="$(gcloud secrets describe "${SECRET}" \
    --project "${PROJECT_ID}" \
    --format='value(labels.purpose)' 2>/dev/null || true)"
  if [[ "${secret_purpose}" != "temporary_benchmark" && "${ALLOW_SECRET_OVERWRITE}" != "true" ]]; then
    echo "Secret ${SECRET} already exists and is not labeled purpose=temporary_benchmark." >&2
    echo "Refusing to overwrite it. Set SECRET to a benchmark-only name or ALLOW_SECRET_OVERWRITE=true." >&2
    exit 1
  fi
  printf "%s" "${url}" | gcloud secrets versions add "${SECRET}" \
    --project "${PROJECT_ID}" \
    --data-file=- >/dev/null
else
  printf "%s" "${url}" | gcloud secrets create "${SECRET}" \
    --project "${PROJECT_ID}" \
    --replication-policy automatic \
    --labels "app=inference,purpose=temporary_benchmark" \
    --data-file=- >/dev/null
fi

echo "${url}"
