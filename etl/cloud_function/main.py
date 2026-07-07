"""
Cloud Function — GCS meta file trigger.

Triggered when a .meta.json file is written to gs://aquatiq-dw-dev-storage.
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

# Xero parsers
import etl.xero.accounts            as _accounts
import etl.xero.bank_transactions   as _bank_transactions
import etl.xero.bank_transfers      as _bank_transfers
import etl.xero.batch_payments      as _batch_payments
import etl.xero.branding_themes     as _branding_themes
import etl.xero.budgets             as _budgets
import etl.xero.contact_groups      as _contact_groups
import etl.xero.contacts            as _contacts
import etl.xero.credit_notes        as _credit_notes
import etl.xero.currencies          as _currencies
import etl.xero.expense_claims      as _expense_claims
import etl.xero.invoices            as _invoices
import etl.xero.items               as _items
import etl.xero.journals            as _journals
import etl.xero.linked_transactions as _linked_transactions
import etl.xero.manual_journals     as _manual_journals
import etl.xero.organisations       as _organisations
import etl.xero.overpayments        as _overpayments
import etl.xero.payment_services     as _payment_services
import etl.xero.payments            as _payments
import etl.xero.prepayments         as _prepayments
import etl.xero.purchase_orders     as _purchase_orders
import etl.xero.quotes              as _quotes
import etl.xero.receipts            as _receipts
import etl.xero.repeating_invoices  as _repeating_invoices
import etl.xero.tax_rates           as _tax_rates
import etl.xero.tracking_categories as _tracking_categories
import etl.xero.users               as _users

logger = logging.getLogger(__name__)

GCS_BUCKET = "aquatiq-dw-dev-storage"
PROJECT     = "prj-dw-dev"

XERO_PARSERS: dict = {
    "accounts":             _accounts,
    "bank_transactions":    _bank_transactions,
    "bank_transfers":       _bank_transfers,
    "batch_payments":       _batch_payments,
    "branding_themes":      _branding_themes,
    "budgets":              _budgets,
    "contact_groups":       _contact_groups,
    "contacts":             _contacts,
    "credit_notes":         _credit_notes,
    "currencies":           _currencies,
    "expense_claims":       _expense_claims,
    "invoices":             _invoices,
    "items":                _items,
    "journals":             _journals,
    "linked_transactions":  _linked_transactions,
    "manual_journals":      _manual_journals,
    "organisations":        _organisations,
    "overpayments":         _overpayments,
    "payment_services":     _payment_services,
    "payments":             _payments,
    "prepayments":          _prepayments,
    "purchase_orders":      _purchase_orders,
    "quotes":               _quotes,
    "receipts":             _receipts,
    "repeating_invoices":   _repeating_invoices,
    "tax_rates":            _tax_rates,
    "tracking_categories":  _tracking_categories,
    "users":                _users,
}

VENDOR_PARSERS: dict = {
    "xero": XERO_PARSERS,
    # "visma": VISMA_PARSERS,  # added when Visma parsers are built
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
        logger.warning("No parser for %s/%s", vendor, endpoint)
        return

    staging_dataset = f"dw_2_staging_{vendor}"
    writer      = BQWriter(project=PROJECT, dataset=staging_dataset)
    batch_reader = _BatchReader(records, project=PROJECT)

    result = parser.run(batch_reader, writer)
    logger.info(
        "Processed gs://%s/%s — %d records → %s",
        bucket_name, object_name, len(records), result
    )
