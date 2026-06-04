#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/gcp-env.sh"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/dataform-workflow-invoke.sh --target fact_sales_order [--target prj-dw-dev.dw_1_silver_visma.fact_sales_order_line] [--include-deps] [--include-dependents]

Notes:
  - This runs the managed Dataform workspace in Google Cloud, not the local CLI runner.
  - Use fully qualified project.dataset.action names when action names are ambiguous.
EOF
}

require_command curl
require_command gcloud
require_command python3
ensure_gcloud_auth

targets=()
include_deps=false
include_dependents=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      targets+=("$2")
      shift 2
      ;;
    --include-deps)
      include_deps=true
      shift
      ;;
    --include-dependents)
      include_dependents=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ ${#targets[@]} -eq 0 ]]; then
  echo "At least one --target is required to avoid accidental full-repo runs." >&2
  usage >&2
  exit 1
fi

compilation_json="$(dataform_api_post \
  "${DATAFORM_REPOSITORY_RESOURCE}/compilationResults" \
  "{\"workspace\":\"${DATAFORM_WORKSPACE_RESOURCE}\"}")"

compilation_result="$(echo "${compilation_json}" | python3 -c '
import json, sys
print(json.load(sys.stdin)["name"])
')"

payload="$(python3 - "${compilation_result}" "${include_deps}" "${include_dependents}" "${targets[@]}" <<'PY'
import json
import sys

compilation_result = sys.argv[1]
include_deps = sys.argv[2].lower() == "true"
include_dependents = sys.argv[3].lower() == "true"
targets = sys.argv[4:]

included_targets = []
for raw in targets:
    parts = raw.split(".")
    if len(parts) == 1:
        target = {"name": parts[0]}
    elif len(parts) == 2:
        target = {"schema": parts[0], "name": parts[1]}
    elif len(parts) == 3:
        target = {"database": parts[0], "schema": parts[1], "name": parts[2]}
    else:
        raise SystemExit(f"Invalid target: {raw}")
    included_targets.append(target)

payload = {
    "compilationResult": compilation_result,
    "invocationConfig": {
        "includedTargets": included_targets,
        "transitiveDependenciesIncluded": include_deps,
        "transitiveDependentsIncluded": include_dependents,
    },
}
print(json.dumps(payload))
PY
)"

invocation_json="$(dataform_api_post \
  "${DATAFORM_REPOSITORY_RESOURCE}/workflowInvocations" \
  "${payload}")"

echo "${invocation_json}" | python3 -c '
import json, sys
obj = json.load(sys.stdin)
print("workflowInvocation: {}".format(obj.get("name")))
print("state: {}".format(obj.get("state")))
print()
print("Next:")
print("  ./scripts/watch-dataform-workflow.sh {}".format(obj.get("name")))
'
