#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

require_command dataform
ensure_gcloud_auth

args=(
  "${DATAFORM_ROOT}"
  "--default-database=${GCP_PROJECT_ID}"
  "--default-location=${BQ_LOCATION}"
)

if [[ -f "${DATAFORM_ROOT}/.df-credentials.json" ]]; then
  args+=("--credentials=${DATAFORM_ROOT}/.df-credentials.json")
fi

exec dataform run "${args[@]}" "$@"
