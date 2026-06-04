#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/watch-dataflow-job.sh <job-id> [--interval 10] [--once]
EOF
}

require_command gcloud
require_command python3
ensure_gcloud_auth

interval=10
once=false
job_id=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval)
      interval="$2"
      shift 2
      ;;
    --once)
      once=true
      shift
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

while true; do
  response="$(gcloud dataflow jobs describe "${job_id}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${DATAFLOW_REGION}" \
    --format=json)"

  clear
  echo "Dataflow Job"
  echo "timestamp: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo
  echo "${response}" | python3 -c '
import json, sys
obj = json.load(sys.stdin)
fields = (
    ("id", obj.get("id")),
    ("name", obj.get("name")),
    ("type", obj.get("type")),
    ("currentState", obj.get("currentState")),
    ("createTime", obj.get("createTime")),
    ("currentStateTime", obj.get("currentStateTime")),
)
for key, value in fields:
    print("{}: {}".format(key, value))
'

  state="$(echo "${response}" | python3 -c '
import json, sys
print(json.load(sys.stdin).get("currentState", "UNKNOWN"))
')"

  if [[ "${once}" == "true" ]]; then
    exit 0
  fi

  case "${state}" in
    JOB_STATE_DONE|JOB_STATE_FAILED|JOB_STATE_CANCELLED|JOB_STATE_DRAINED|JOB_STATE_UPDATED)
      exit 0
      ;;
  esac

  sleep "${interval}"
done
