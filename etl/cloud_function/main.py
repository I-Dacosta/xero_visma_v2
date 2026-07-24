"""
Cloud Function — GCS meta file trigger.

Triggered when a .meta.json file is written to gs://prj-dw-dev-raw.
Data file writes are ignored (filtered by the .meta.json suffix check).

Path format:
    raw/{vendor}/{tenant_id}/2.0/{endpoint}/{date}/{ts}_{run}_{page}.json.meta.json

Flow:
    .meta.json written to GCS
    → Cloud Function fires
    → read meta file (tenant_id, endpoint, synced_at)
    → derive data file path (strip .meta.json)
    → GCSReader yields one record per item in the records array
    → route to correct entity parser
    → parser.run() MERGEs all rows into dw_2_staging_{vendor}

_BatchReader wraps a pre-fetched list of records so every parser's run()
function works without modification — parsers do not know whether they
are reading from BQ bronze, GCS, or a test fixture.
"""

import logging
import functions_framework

from etl.common.gcs_reader import GCSReader
from etl.common.bq_writer import BQWriter

# Xero parsers — one per endpoint currently present in the GCS bucket.
# Parsers are added only when their endpoint actually appears in the bucket;
# a new unparsed endpoint triggers a drift warning (see process_gcs_upload).
import etl.xero.accounts            as _accounts
import etl.xero.bank_transactions   as _bank_transactions
import etl.xero.bank_transfers      as _bank_transfers
import etl.xero.branding_themes     as _branding_themes
import etl.xero.contacts            as _contacts
import etl.xero.credit_notes        as _credit_notes
import etl.xero.currencies          as _currencies
import etl.xero.invoices            as _invoices
import etl.xero.items               as _items
import etl.xero.journals            as _journals
import etl.xero.manual_journals     as _manual_journals
import etl.xero.organisations       as _organisations
import etl.xero.overpayments        as _overpayments
import etl.xero.payments            as _payments
import etl.xero.purchase_orders     as _purchase_orders
import etl.xero.quotes              as _quotes
import etl.xero.reports             as _reports
import etl.xero.tax_rates           as _tax_rates
import etl.xero.tracking_categories as _tracking_categories
import etl.xero.users               as _users

logger = logging.getLogger(__name__)

GCS_BUCKET = "prj-dw-dev-raw"
PROJECT     = "prj-dw-dev"

XERO_PARSERS: dict = {
    "accounts":             _accounts,
    "bank_transactions":    _bank_transactions,
    "bank_transfers":       _bank_transfers,
    "branding_themes":      _branding_themes,
    "contacts":             _contacts,
    "credit_notes":         _credit_notes,
    "currencies":           _currencies,
    "invoices":             _invoices,
    "items":                _items,
    "journals":             _journals,
    "manual_journals":      _manual_journals,
    "organisations":        _organisations,
    "overpayments":         _overpayments,
    "payments":             _payments,
    "purchase_orders":      _purchase_orders,
    "quotes":               _quotes,
    "report_balance_sheet":      _reports,
    "report_bank_summary":       _reports,
    "report_budget_summary":     _reports,
    "report_executive_summary": _reports,
    "report_profit_and_loss":    _reports,
    "report_trial_balance":      _reports,
    "tax_rates":            _tax_rates,
    "tracking_categories":  _tracking_categories,
    "users":                _users,
}

VENDOR_PARSERS: dict = {
    "xero": XERO_PARSERS,
    # "visma": VISMA_PARSERS,  # added when Visma parsers are built
}

# Bucket endpoints we knowingly do not parse (kept out of drift warnings).
KNOWN_UNPARSED = {
    "bills": "subset of invoices (ACCPAY) — covered by staging_xero.invoices",
}


class _BatchReader:
    """
    Wraps a pre-fetched list of records so parser run() functions work
    without modification. The table argument to iter_records() is ignored —
    records are already filtered by entity type by the Cloud Function.
    """
    def __init__(self, records: list[dict], project: str):
        self.project = project
        self._records = records

    def iter_records(self, table: str, **kwargs):  # noqa: ARG002
        yield from self._records


@functions_framework.cloud_event
def process_gcs_upload(cloud_event):
    """Entry point — fires on every GCS object finalise event."""
    data        = cloud_event.data
    bucket_name = data["bucket"]
    object_name = data["name"]

    # Only process meta files; silently ignore data file triggers
    if not object_name.endswith(".meta.json"):
        return

    logger.info("Processing meta file: gs://%s/%s", bucket_name, object_name)

    # Read both files and yield records
    gcs_reader = GCSReader(bucket=bucket_name, project=PROJECT)
    records = list(gcs_reader.iter_records_from_meta(object_name))

    if not records:
        logger.info("No records extracted from %s — skipping", object_name)
        return

    # Extract routing info from the first record (all records share vendor/endpoint)
    # Path: raw/{vendor}/{tenant_id}/2.0/{endpoint}/...
    parts = object_name.split("/")
    vendor   = parts[1] if len(parts) > 1 else None
    endpoint = parts[4] if len(parts) > 4 else None

    if not vendor or not endpoint:
        logger.warning("Could not parse vendor/endpoint from path: %s", object_name)
        return

    vendor_map = VENDOR_PARSERS.get(vendor)
    if not vendor_map:
        logger.warning("No parsers registered for vendor: %s", vendor)
        return

    parser = vendor_map.get(endpoint)
    if not parser:
        if endpoint in KNOWN_UNPARSED:
            logger.info(
                "Skipping known-unparsed endpoint %s/%s (%s)",
                vendor, endpoint, KNOWN_UNPARSED[endpoint],
            )
        else:
            # Drift detection: a new endpoint is landing in the bucket that we
            # do not yet parse. Warn loudly so a parser gets built.
            logger.warning(
                "NEW ENDPOINT DETECTED: %s/%s has no parser (file: %s). "
                "Build etl/xero/%s.py and add it to XERO_PARSERS.",
                vendor, endpoint, object_name, endpoint,
            )
        return

    staging_dataset = f"staging_{vendor}"
    writer      = BQWriter(project=PROJECT, dataset=staging_dataset)
    batch_reader = _BatchReader(records, project=PROJECT)

    result = parser.run(batch_reader, writer)
    logger.info(
        "Processed gs://%s/%s — %d records → %s",
        bucket_name, object_name, len(records), result
    )
