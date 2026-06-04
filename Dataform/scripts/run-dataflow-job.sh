#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-dataflow-job.sh --job-name my-job --classic-template gs://bucket/template [gcloud dataflow flags...]
  ./scripts/run-dataflow-job.sh --job-name my-job --flex-template gs://bucket/flex-template.json [gcloud dataflow flags...]

Examples:
  ./scripts/run-dataflow-job.sh \
    --job-name bronze-visma-20260418-1 \
    --classic-template gs://my-bucket/templates/bronze \
    --parameters input=gs://my-bucket/in,output=gs://my-bucket/out

  ./scripts/run-dataflow-job.sh \
    --job-name bronze-visma-flex-20260418-1 \
    --flex-template gs://my-bucket/templates/bronze-flex.json \
    --parameters input=gs://my-bucket/in,output=gs://my-bucket/out \
    --temp-location=gs://my-bucket/tmp
EOF
}

require_command gcloud
require_command python3
ensure_gcloud_auth

job_name=""
template_mode=""
template_path=""
passthrough=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --job-name)
      job_name="$2"
      shift 2
      ;;
    --classic-template)
      template_mode="classic"
      template_path="$2"
      shift 2
      ;;
    --flex-template)
      template_mode="flex"
      template_path="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      passthrough+=("$1")
      shift
      ;;
  esac
done

if [[ -z "${job_name}" || -z "${template_mode}" || -z "${template_path}" ]]; then
  usage >&2
  exit 1
fi

if [[ "${template_mode}" == "classic" ]]; then
  output="$(gcloud dataflow jobs run "${job_name}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${DATAFLOW_REGION}" \
    --gcs-location="${template_path}" \
    --format=json \
    "${passthrough[@]}")"
else
  output="$(gcloud dataflow flex-template run "${job_name}" \
    --project="${GCP_PROJECT_ID}" \
    --region="${DATAFLOW_REGION}" \
    --template-file-gcs-location="${template_path}" \
    --format=json \
    "${passthrough[@]}")"
fi

job_id="$(echo "${output}" | python3 -c '
import json, sys
obj = json.load(sys.stdin)
print(obj.get("id", ""))
')"

if [[ -z "${job_id}" ]]; then
  echo "${output}"
  echo "Unable to determine Dataflow job id from gcloud output." >&2
  exit 1
fi

echo "jobId: ${job_id}"
echo "jobName: ${job_name}"
echo
echo "Next:"
echo "  ./scripts/watch-dataflow-job.sh ${job_id}"
echo "  ./scripts/tail-dataflow-logs.sh ${job_id}"
