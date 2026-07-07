"""
GCS → staging backfill.

Reads all raw JSON files for each Xero endpoint from the GCS bucket and writes
parsed, typed rows into the staging_xero BigQuery dataset via the entity parsers.

This is the batch equivalent of the Cloud Function: instead of processing one
meta file per event, it walks every meta file for an endpoint and processes the
full history in one run.

Usage:
    python -m etl.backfill_gcs                 # all endpoints with parsers
    python -m etl.backfill_gcs accounts users  # specific endpoints only

Reads from:  gs://aquatiq-dw-dev-storage/raw/xero/...
Writes to:   prj-dw-dev.staging_xero.*

Parser policy (2026-07-07): parsers exist ONLY for endpoints that are actually
present in the GCS bucket. Speculative parsers ported from the old BigQuery
bronze project were removed. When a NEW endpoint appears in the bucket without a
parser, this script warns loudly (drift detection) — that is the signal to build
the parser then, guided by the real payload rather than the frozen old project.
Removed parsers remain in git history and their payloads are documented in
docs/SILVER_XERO.md, so rebuilding is a quick `git show` + backfill.
"""

import sys
import time
import logging

from google.cloud import storage

from etl.common.gcs_reader import GCSReader
from etl.common.bq_writer import BQWriter

# Entity parsers — one per endpoint currently present in the GCS bucket.
import etl.xero.accounts            as accounts
import etl.xero.bank_transactions   as bank_transactions
import etl.xero.branding_themes     as branding_themes
import etl.xero.contacts            as contacts
import etl.xero.credit_notes        as credit_notes
import etl.xero.currencies          as currencies
import etl.xero.invoices            as invoices
import etl.xero.items               as items
import etl.xero.manual_journals     as manual_journals
import etl.xero.organisations       as organisations
import etl.xero.payments            as payments
import etl.xero.purchase_orders     as purchase_orders
import etl.xero.quotes              as quotes
import etl.xero.tax_rates           as tax_rates
import etl.xero.tracking_categories as tracking_categories
import etl.xero.users               as users

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET          = "aquatiq-dw-dev-storage"
PROJECT         = "prj-dw-dev"
STAGING_DATASET = "staging_xero"
VENDOR          = "xero"

# Endpoint -> parser module. Every entry corresponds to an endpoint present in
# the GCS bucket. Keep this map in sync with the bucket via the drift check below.
PARSERS = {
    "accounts":             accounts,
    "bank_transactions":    bank_transactions,
    "branding_themes":      branding_themes,
    "contacts":             contacts,
    "credit_notes":         credit_notes,
    "currencies":           currencies,
    "invoices":             invoices,
    "items":                items,
    "manual_journals":      manual_journals,
    "organisations":        organisations,
    "payments":             payments,
    "purchase_orders":      purchase_orders,
    "quotes":               quotes,
    "tax_rates":            tax_rates,
    "tracking_categories":  tracking_categories,
    "users":                users,
}

# Bucket endpoints we knowingly do not parse (and why). Kept out of the drift
# warning so genuinely-new endpoints stand out.
KNOWN_UNPARSED = {
    "bills": "subset of invoices (ACCPAY) — already covered by staging_xero.invoices",
}


class _BatchReader:
    """Yields pre-collected GCS records regardless of the table name passed."""
    def __init__(self, records: list[dict], project: str):
        self.project = project
        self._records = records

    def iter_records(self, table: str, **kwargs):  # noqa: ARG002
        yield from self._records


def list_bucket_endpoints() -> set[str]:
    """Return the distinct endpoint folder names currently in the GCS bucket."""
    client = storage.Client(project=PROJECT)
    endpoints = set()
    for b in client.list_blobs(BUCKET, prefix=f"raw/{VENDOR}/"):
        parts = b.name.split("/")
        if len(parts) > 4:
            endpoints.add(parts[4])
    return endpoints


def check_for_new_endpoints(bucket_endpoints: set[str]) -> list[str]:
    """
    Drift detection: warn about any bucket endpoint that has neither a parser
    nor a known-unparsed reason. Returns the list of unrecognised endpoints.
    """
    unrecognised = sorted(
        e for e in bucket_endpoints
        if e not in PARSERS and e not in KNOWN_UNPARSED
    )
    for e in unrecognised:
        logger.warning(
            "NEW ENDPOINT DETECTED IN BUCKET: '%s' has no parser. "
            "Inspect a payload and build etl/xero/%s.py, then add it to PARSERS.",
            e, e,
        )
    for e in sorted(bucket_endpoints & set(KNOWN_UNPARSED)):
        logger.info("Skipping known-unparsed endpoint '%s' (%s)", e, KNOWN_UNPARSED[e])
    return unrecognised


def backfill_endpoint(endpoint: str, gcs: GCSReader, writer: BQWriter) -> dict:
    module = PARSERS[endpoint]
    records = list(gcs.iter_records(VENDOR, endpoint))
    if not records:
        return {"records": 0}
    reader = _BatchReader(records, project=PROJECT)
    result = module.run(reader, writer)
    result["records"] = len(records)
    return result


def main(endpoints: list[str] | None) -> None:
    gcs    = GCSReader(bucket=BUCKET, project=PROJECT)
    writer = BQWriter(project=PROJECT, dataset=STAGING_DATASET)

    # Always run drift detection against the live bucket first.
    bucket_endpoints = list_bucket_endpoints()
    new_endpoints = check_for_new_endpoints(bucket_endpoints)
    if new_endpoints:
        print(f"\n  ⚠️  {len(new_endpoints)} unparsed endpoint(s) in bucket: "
              f"{', '.join(new_endpoints)} — see warnings above.\n")

    requested = endpoints or list(PARSERS.keys())

    passed = failed = 0
    total_start = time.time()

    for endpoint in requested:
        if endpoint not in PARSERS:
            reason = KNOWN_UNPARSED.get(endpoint, "no parser")
            print(f"  SKIP  {endpoint:<22} ({reason})")
            continue
        t0 = time.time()
        try:
            result = backfill_endpoint(endpoint, gcs, writer)
            elapsed = time.time() - t0
            print(f"  OK    {endpoint:<22} {str(result):<55} {elapsed:.1f}s")
            passed += 1
        except Exception as e:
            elapsed = time.time() - t0
            print(f"  FAIL  {endpoint:<22} {e}  {elapsed:.1f}s")
            failed += 1

    print(f"\n{passed} ok, {failed} failed — total {time.time() - total_start:.0f}s")


if __name__ == "__main__":
    main(sys.argv[1:] or None)
