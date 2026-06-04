#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

require_command gcloud

if ! gcloud auth print-access-token >/dev/null 2>&1; then
  gcloud auth login
fi

gcloud config set project "${GCP_PROJECT_ID}" >/dev/null
gcloud config set compute/region "${GCP_REGION}" >/dev/null

echo "Active account:"
gcloud auth list --filter=status:ACTIVE --format="table(account,status)"

echo
echo "Active gcloud config:"
gcloud config list --format="text(core.account,core.project,compute.region)"
