#!/usr/bin/env bash

require_gcloud_resource() {
  local description="$1"
  shift
  if ! "$@" >/dev/null 2>&1; then
    echo "Missing required ${description}." >&2
    echo "Command failed: $*" >&2
    exit 1
  fi
}

require_gcloud_output() {
  local description="$1"
  shift
  local output
  if ! output="$("$@" 2>/dev/null)" || [[ -z "${output}" ]]; then
    echo "Missing required ${description}." >&2
    echo "Command failed or returned no output: $*" >&2
    exit 1
  fi
}

require_secret() {
  local secret_name="$1"
  local project_id="$2"
  require_gcloud_resource "Secret Manager secret ${secret_name}" \
    gcloud secrets describe "${secret_name}" --project "${project_id}"
}

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "Missing required command: ${command_name}" >&2
    exit 1
  fi
}

require_enabled_service() {
  local service_name="$1"
  local project_id="$2"
  require_gcloud_output "enabled API ${service_name}" \
    gcloud services list --enabled --project "${project_id}" "--filter=config.name=${service_name}" "--format=value(config.name)"
}

require_cloudrun_bench_prereqs() {
  local project_id="$1"
  local region="$2"
  local repository="${3:-inference}"

  require_command gcloud
  require_command ruby
  require_command cargo
  require_command k6
  require_command openssl

  if [[ "${RUN_BUILD:-false}" == "true" ]]; then
    require_command docker
  fi

  for service in \
    artifactregistry.googleapis.com \
    compute.googleapis.com \
    memorystore.googleapis.com \
    networkconnectivity.googleapis.com \
    parametermanager.googleapis.com \
    run.googleapis.com \
    secretmanager.googleapis.com; do
    require_enabled_service "${service}" "${project_id}"
  done

  require_gcloud_resource "Artifact Registry repo ${repository} in ${region}" \
    gcloud artifacts repositories describe "${repository}" --project "${project_id}" --location "${region}"
  require_gcloud_resource "Cloud Run runtime service account inference-runtime@${project_id}.iam.gserviceaccount.com" \
    gcloud iam service-accounts describe "inference-runtime@${project_id}.iam.gserviceaccount.com" --project "${project_id}"

  if [[ "${RUN_BUILD:-false}" != "true" ]]; then
    for image in inference-frontend inference-temporal-worker crema-autoscaler; do
      require_gcloud_resource "image ${image}:latest in ${repository}" \
        gcloud artifacts docker images describe "${region}-docker.pkg.dev/${project_id}/${repository}/${image}:latest"
    done
  fi
}

temporal_auth_mode() {
  local has_api_key=0
  local has_ca=0
  local has_cert=0
  local has_key=0

  [[ -n "${TEMPORAL_API_KEY_SECRET:-}" ]] && has_api_key=1
  [[ -n "${TEMPORAL_TLS_CA_CERT_SECRET:-}" ]] && has_ca=1
  [[ -n "${TEMPORAL_TLS_CERT_SECRET:-}" ]] && has_cert=1
  [[ -n "${TEMPORAL_TLS_KEY_SECRET:-}" ]] && has_key=1

  if [[ "${has_api_key}" == 1 ]]; then
    echo "api_key"
    return
  fi

  if [[ "${has_cert}" == 1 && "${has_key}" == 1 ]]; then
    echo "mtls"
    return
  fi

  if [[ "${has_ca}" == 1 || "${has_cert}" == 1 || "${has_key}" == 1 ]]; then
    echo "partial_mtls"
    return
  fi

  echo "none"
}

require_temporal_auth() {
  local project_id="$1"
  local mode
  mode="$(temporal_auth_mode)"

  case "${mode}" in
    api_key)
      require_secret "${TEMPORAL_API_KEY_SECRET}" "${project_id}"
      ;;
    mtls)
      if [[ -n "${TEMPORAL_TLS_CA_CERT_SECRET:-}" ]]; then
        require_secret "${TEMPORAL_TLS_CA_CERT_SECRET}" "${project_id}"
      fi
      require_secret "${TEMPORAL_TLS_CERT_SECRET}" "${project_id}"
      require_secret "${TEMPORAL_TLS_KEY_SECRET}" "${project_id}"
      ;;
    partial_mtls)
      echo "Temporal mTLS config is incomplete." >&2
      echo "Set TEMPORAL_TLS_CERT_SECRET and TEMPORAL_TLS_KEY_SECRET together; TEMPORAL_TLS_CA_CERT_SECRET is optional." >&2
      exit 1
      ;;
    none)
      echo "Temporal credentials are required for the serverless Cloud Run path." >&2
      echo "Set TEMPORAL_API_KEY_SECRET, or set TEMPORAL_TLS_CERT_SECRET and TEMPORAL_TLS_KEY_SECRET." >&2
      exit 1
      ;;
  esac
}
