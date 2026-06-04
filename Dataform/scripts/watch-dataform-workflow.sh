#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/watch-dataform-workflow.sh <workflow-invocation-id-or-full-name> [--interval 10] [--once]
EOF
}

require_command curl
require_command gcloud
require_command python3
ensure_gcloud_auth

interval=10
once=false
workflow_name=""

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
      if [[ -z "${workflow_name}" ]]; then
        workflow_name="$1"
        shift
      else
        echo "Unexpected argument: $1" >&2
        usage >&2
        exit 1
      fi
      ;;
  esac
done

if [[ -z "${workflow_name}" ]]; then
  usage >&2
  exit 1
fi

if [[ "${workflow_name}" != projects/* ]]; then
  workflow_name="${DATAFORM_REPOSITORY_RESOURCE}/workflowInvocations/${workflow_name}"
fi

while true; do
  response="$(dataform_api_get "${workflow_name}")"

  clear
  echo "Dataform Workflow Invocation"
  echo "timestamp: $(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo
  echo "${response}" | python3 -c '
import json, sys
obj = json.load(sys.stdin)
fields = (
    ("name", obj.get("name")),
    ("state", obj.get("state")),
    ("invocationTiming.startTime", obj.get("invocationTiming", {}).get("startTime")),
    ("invocationTiming.endTime", obj.get("invocationTiming", {}).get("endTime")),
    ("compilationResult", obj.get("compilationResult")),
)
for key, value in fields:
    print("{}: {}".format(key, value))
'

  state="$(echo "${response}" | python3 -c '
import json, sys
print(json.load(sys.stdin).get("state", "UNKNOWN"))
')"

  if [[ "${once}" == "true" ]]; then
    exit 0
  fi

  case "${state}" in
    SUCCEEDED|FAILED|CANCELLED)
      exit 0
      ;;
  esac

  sleep "${interval}"
done
