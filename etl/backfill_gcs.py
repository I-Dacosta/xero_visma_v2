"""
GCS → staging backfill.

Reads all raw JSON files for each Xero endpoint from the GCS bucket and writes
parsed, typed rows into the staging_xero BigQuery dataset via the entity parsers.

This is the batch equivalent of the Cloud Function: instead of processing one
meta file per event, it walks every meta file for an endpoint and processes the
full history in one run.

Usage:
    python etl/backfill_gcs.py                 # all endpoints
    python etl/backfill_gcs.py accounts users  # specific endpoints only

Reads from:  gs://aquatiq-dw-dev-storage/raw/xero/...
Writes to:   prj-dw-dev.staging_xero.*
"""

import sys
import time
import logging

from etl.common.gcs_reader import GCSReader
from etl.common.bq_writer import BQWriter

# Entity parsers
import etl.xero.accounts            as accounts
import etl.xero.bank_transactions   as bank_transactions
import etl.xero.branding_themes     as branding_themes
import etl.xero.contacts            as contacts
import etl.xero.credit_notes        as credit_notes
import etl.xero.currencies          as currencies
import etl.xero.invoices            as invoices
import etl.xero.items               as items
import etl.xero.journals            as journals
import etl.xero.manual_journals     as manual_journals
import etl.xero.organisations       as organisations
import etl.xero.payment_services     as payment_services
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

# Map GCS endpoint name -> parser module.
# Only endpoints present in the bucket AND with a parser are included.
# NOTE: "bills" exists in the bucket but has no parser yet (Xero bills are
#        ACCPAY invoices — needs its own parser or routing to invoices).
PARSERS = {
    "accounts":             accounts,
    "bank_transactions":    bank_transactions,
    "branding_themes":      branding_themes,
    "contacts":             contacts,
    "credit_notes":         credit_notes,
    "currencies":           currencies,
    "invoices":             invoices,
    "items":                items,
    "journals":             journals,
    "manual_journals":      manual_journals,
    "organisations":        organisations,
    "payment_services":     payment_services,
    "payments":             payments,
    "purchase_orders":      purchase_orders,
    "quotes":               quotes,
    "tax_rates":            tax_rates,
    "tracking_categories":  tracking_categories,
    "users":                users,
}


class _BatchReader:
    """Yields pre-collected GCS records regardless of the table name passed."""
    def __init__(self, records: list[dict], project: str):
        self.project = project
        self._records = records

    def iter_records(self, table: str, **kwargs):  # noqa: ARG002
        yield from self._records


def backfill_endpoint(endpoint: str, gcs: GCSReader, writer: BQWriter) -> dict:
    module = PARSERS[endpoint]
    records = list(gcs.iter_records(VENDOR, endpoint))
    if not records:
        return {"records": 0}
    reader = _BatchReader(records, project=PROJECT)
    result = module.run(reader, writer)
    result["records"] = len(records)
    return result


def main(endpoints: list[str]) -> None:
    gcs    = GCSReader(bucket=BUCKET, project=PROJECT)
    writer = BQWriter(project=PROJECT, dataset=STAGING_DATASET)

    passed = failed = 0
    total_start = time.time()

    for endpoint in endpoints:
        if endpoint not in PARSERS:
            print(f"  SKIP  {endpoint:<22} (no parser)")
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
    requested = sys.argv[1:] or list(PARSERS.keys())
    main(requested)
