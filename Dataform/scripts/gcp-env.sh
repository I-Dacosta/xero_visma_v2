#!/usr/bin/env bash
set -euo pipefail

DATAFORM_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKFLOW_SETTINGS_FILE="${WORKFLOW_SETTINGS_FILE:-${DATAFORM_ROOT}/workflow_settings.yaml}"

workflow_setting() {
  local key="$1"
  if [[ -f "${WORKFLOW_SETTINGS_FILE}" ]]; then
    awk -F': *' -v key="$key" '$1 == key { print $2; exit }' "${WORKFLOW_SETTINGS_FILE}"
  fi
}

default_project="$(workflow_setting defaultProject || true)"
default_location="$(workflow_setting defaultLocation || true)"

export GCP_PROJECT_ID="${GCP_PROJECT_ID:-${default_project:-prj-dw-dev}}"
export BQ_PROJECT_ID="${BQ_PROJECT_ID:-${GCP_PROJECT_ID}}"
export BQ_LOCATION="${BQ_LOCATION:-${default_location:-europe-north2}}"
export GCP_REGION="${GCP_REGION:-europe-north1}"
export DATAFLOW_REGION="${DATAFLOW_REGION:-${GCP_REGION}}"
export DATAFORM_LOCATION="${DATAFORM_LOCATION:-europe-north1}"
export DATAFORM_REPOSITORY="${DATAFORM_REPOSITORY:-Datawarehouse}"
export DATAFORM_WORKSPACE="${DATAFORM_WORKSPACE:-Dev}"

export DATAFORM_REPOSITORY_RESOURCE="projects/${GCP_PROJECT_ID}/locations/${DATAFORM_LOCATION}/repositories/${DATAFORM_REPOSITORY}"
export DATAFORM_WORKSPACE_RESOURCE="${DATAFORM_REPOSITORY_RESOURCE}/workspaces/${DATAFORM_WORKSPACE}"

require_command() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "Missing required command: ${cmd}" >&2
    exit 1
  fi
}

ensure_gcloud_auth() {
  if ! gcloud auth print-access-token >/dev/null 2>&1; then
    echo "gcloud is not authenticated. Run ./scripts/auth-gcp.sh first." >&2
    exit 1
  fi
}

gcp_access_token() {
  ensure_gcloud_auth
  gcloud auth print-access-token
}

dataform_api_get() {
  local path="$1"
  curl -sS \
    -H "Authorization: Bearer $(gcp_access_token)" \
    "https://dataform.googleapis.com/v1beta1/${path}"
}

dataform_api_post() {
  local path="$1"
  local payload="$2"
  curl -sS \
    -X POST \
    -H "Authorization: Bearer $(gcp_access_token)" \
    -H "Content-Type: application/json" \
    -d "${payload}" \
    "https://dataform.googleapis.com/v1beta1/${path}"
}
