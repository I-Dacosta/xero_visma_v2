#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

require_command bq
ensure_gcloud_auth

if [[ $# -eq 0 ]]; then
  echo "Usage: ./scripts/bq-query.sh 'select 1 as ok'" >&2
  exit 1
fi

exec bq query \
  --project_id="${BQ_PROJECT_ID}" \
  --location="${BQ_LOCATION}" \
  --use_legacy_sql=false \
  "$@"
