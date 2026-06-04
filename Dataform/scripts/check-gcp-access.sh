#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

for cmd in gcloud bq dataform curl python3; do
  require_command "${cmd}"
done

ensure_gcloud_auth

gcloud config set project "${GCP_PROJECT_ID}" >/dev/null
gcloud config set compute/region "${GCP_REGION}" >/dev/null

workflow_default_dataset="$(workflow_setting defaultDataset || true)"

echo "GCloud"
gcloud auth list --filter=status:ACTIVE --format="table(account,status)"
gcloud config list --format="text(core.account,core.project,compute.region)"

echo
echo "BigQuery Datasets (${BQ_PROJECT_ID})"
bq ls --project_id="${BQ_PROJECT_ID}"

if [[ -n "${workflow_default_dataset}" ]]; then
  echo
  echo "Workflow Defaults"
  echo "defaultProject: ${GCP_PROJECT_ID}"
  echo "defaultDataset: ${workflow_default_dataset}"
  echo "defaultLocation: ${BQ_LOCATION}"
  if bq show --project_id="${BQ_PROJECT_ID}" "${workflow_default_dataset}" >/dev/null 2>&1; then
    echo "default dataset: ok"
  else
    echo "warning: workflow_settings.yaml defaultDataset '${workflow_default_dataset}' is not present in BigQuery" >&2
  fi
fi

echo
echo "BigQuery Smoke Test (${BQ_LOCATION})"
bq query \
  --project_id="${BQ_PROJECT_ID}" \
  --location="${BQ_LOCATION}" \
  --use_legacy_sql=false \
  'select 1 as ok, current_date() as today'

echo
echo "Dataform Repository"
dataform_api_get "${DATAFORM_REPOSITORY_RESOURCE}" | python3 -c '
import json, sys
obj = json.load(sys.stdin)
print("name: {}".format(obj.get("name")))
print("serviceAccount: {}".format(obj.get("serviceAccount")))
'

echo
echo "Dataform Workspaces"
dataform_api_get "${DATAFORM_REPOSITORY_RESOURCE}/workspaces" | python3 -c '
import json, sys
obj = json.load(sys.stdin)
for item in obj.get("workspaces", []):
    print(item.get("name"))
'

echo
echo "Dataflow Jobs (${DATAFLOW_REGION})"
gcloud dataflow jobs list --project="${GCP_PROJECT_ID}" --region="${DATAFLOW_REGION}"

echo
echo "Local Dataform Compile"
dataform compile "${DATAFORM_ROOT}" >/dev/null
echo "compile ok"
