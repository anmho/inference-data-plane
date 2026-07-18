#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-anmho-infra-prod}"
REGION="${REGION:-us-central1}"
failed=0

check_empty() {
  local label="$1"
  local command="$2"
  local output
  output="$(eval "${command}")"
  if [[ -n "${output}" ]]; then
    echo "${label}:"
    echo "${output}"
    failed=1
  else
    echo "${label}: none"
  fi
}

check_absent_or_deleting() {
  local label="$1"
  local command="$2"
  local output
  if output="$(eval "${command}" 2>/dev/null)"; then
    case "${output}" in
      DELETING)
        echo "${label}: DELETING"
        ;;
      "")
        echo "${label}: present"
        failed=1
        ;;
      *)
        echo "${label}: ${output}"
        failed=1
        ;;
    esac
  else
    echo "${label}: absent"
  fi
}

check_empty "Cloud Run inference/Crema services" \
  "gcloud run services list --project '${PROJECT_ID}' --region '${REGION}' --format='value(metadata.name)' | rg '^(inference-|crema-autoscaler)' || true"

check_empty "Cloud Run inference worker pools" \
  "CLOUDSDK_PYTHON_SITEPACKAGES=1 gcloud run worker-pools list --project '${PROJECT_ID}' --region '${REGION}' --format='value(metadata.name)' | rg '^inference-' || true"

check_empty "GCE instances" \
  "gcloud compute instances list --project '${PROJECT_ID}' --format='value(name)'"

check_absent_or_deleting "Temporary Memorystore Valkey" \
  "gcloud memorystore instances describe inference-bench-valkey --project '${PROJECT_ID}' --location '${REGION}' --format='value(state)'"

check_absent_or_deleting "Temporary VPC connector" \
  "gcloud compute networks vpc-access connectors describe inference-bench-vpc --project '${PROJECT_ID}' --region '${REGION}' --format='value(state)'"

check_absent_or_deleting "Temporary Cloud Run egress subnet" \
  "gcloud compute networks subnets describe inference-bench-egress --project '${PROJECT_ID}' --region '${REGION}' --format='value(purpose)'"

check_absent_or_deleting "Temporary Memorystore PSC subnet" \
  "gcloud compute networks subnets describe inference-bench-psc --project '${PROJECT_ID}' --region '${REGION}' --format='value(purpose)'"

temp_secret_purpose="$(gcloud secrets describe inference-valkey-url \
  --project "${PROJECT_ID}" \
  --format='value(labels.purpose)' 2>/dev/null || true)"
if [[ "${temp_secret_purpose}" == "temporary_benchmark" ]]; then
  echo "Temporary Valkey URL secret: present"
  failed=1
else
  echo "Temporary Valkey URL secret: absent"
fi

temp_api_secret_purpose="$(gcloud secrets describe inference-api-keys \
  --project "${PROJECT_ID}" \
  --format='value(labels.purpose)' 2>/dev/null || true)"
if [[ "${temp_api_secret_purpose}" == "temporary_benchmark" ]]; then
  echo "Temporary API key secret: present"
  failed=1
else
  echo "Temporary API key secret: absent"
fi

exit "${failed}"
