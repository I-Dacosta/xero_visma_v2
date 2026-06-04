#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/tail-dataflow-logs.sh <job-id> [--severity ERROR] [--filter 'textPayload:\"foo\"']
EOF
}

require_command gcloud
ensure_gcloud_auth

job_id=""
severity=""
extra_filter=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --severity)
      severity="$2"
      shift 2
      ;;
    --filter)
      extra_filter="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "${job_id}" ]]; then
        job_id="$1"
        shift
      else
        echo "Unexpected argument: $1" >&2
        usage >&2
        exit 1
      fi
      ;;
  esac
done

if [[ -z "${job_id}" ]]; then
  usage >&2
  exit 1
fi

if ! gcloud beta logging tail --help >/dev/null 2>&1; then
  echo "gcloud beta logging tail is unavailable. Install it with: gcloud components install beta --quiet" >&2
  exit 1
fi

filter="(resource.type=\"dataflow_job\" OR resource.type=\"dataflow_step\") AND resource.labels.job_id=\"${job_id}\""

if [[ -n "${severity}" ]]; then
  filter="${filter} AND severity>=${severity}"
fi

if [[ -n "${extra_filter}" ]]; then
  filter="${filter} AND ${extra_filter}"
fi

exec gcloud beta logging tail "${filter}" \
  --project="${GCP_PROJECT_ID}" \
  --buffer-window=2s
