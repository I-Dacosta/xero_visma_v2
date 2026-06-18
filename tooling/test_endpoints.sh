#!/usr/bin/env bash
#
# Smoke-test the xero raw->GCS uploader CLI in Docker (safe / offline-by-default).
#
# Steps:
#   1. healthcheck — mints a custom-connection token per tenant (no writes)
#   2. dry-run sync — fetches one entity and writes the raw object layout to
#      ./out via LocalDirSink (NO GCS, NO cloud writes, NO secrets beyond Xero)
#
# Usage:
#   ./tooling/test_endpoints.sh                       # accounts, all tenants
#   TENANT=<uuid> ENTITY=accounts ./tooling/test_endpoints.sh
#
# Prereqs: docker; a built image (`docker build -t xero-service-v2:local .`);
# and a .env with XERO_ORG_N_* custom-connection credentials.
#
# NOTE: ENTITY runs in --full (no-filter) mode, so prefer a small master entity
# (accounts, items, tax_rates). For a real GCS write see docs/OPERATIONS.md.
set -euo pipefail

IMAGE="${IMAGE:-xero-service-v2:local}"
ENTITY="${ENTITY:-accounts}"
ENV_FILE="${ENV_FILE:-.env}"
OUT_DIR="${OUT_DIR:-./out}"

run() {
  docker run --rm \
    --env-file "$ENV_FILE" \
    -e RUST_LOG="${RUST_LOG:-info,xero_cli=info,xero_sync=info}" \
    --user "$(id -u):$(id -g)" \
    -v "$PWD/$OUT_DIR:/out" \
    "$IMAGE" "$@"
}

mkdir -p "$OUT_DIR"

echo "── healthcheck (token per tenant; no writes)"
run healthcheck || echo "  (healthcheck reported an error — continuing to dry-run)"

echo
echo "── dry-run sync: --full --entity ${ENTITY} ${TENANT:+--tenant <set>} → ${OUT_DIR}"
if [[ -n "${TENANT:-}" ]]; then
  run sync --full --entity "$ENTITY" --tenant "$TENANT" --dry-run --local-dir /out
else
  run sync --full --entity "$ENTITY" --dry-run --local-dir /out
fi

echo
echo "── raw object layout written under ${OUT_DIR}:"
find "$OUT_DIR" -type f | sed "s|^|  |"
