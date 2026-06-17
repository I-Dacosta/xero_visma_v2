"""
Cloud Function — GCS write trigger.

Triggered when a new JSON file lands in gs://prj-dw-dev-raw.
Expected path format:
    raw/{vendor}/{tenant_id}/v1/{entity_type}/{date}/{record_id}.json

Flow:
    GCS write event
    → parse path (vendor, tenant_id, entity_type, record_id)
    → download JSON file
    → build record dict (same shape as BQReader output)
    → route to correct parser module via _SingleRecordReader
    → parser.run() writes to dw_2_staging_{vendor}

_SingleRecordReader wraps a single GCS-sourced record so every parser's
run() function works without modification — parsers do not know whether
they are reading from BQ bronze or GCS.

Note: contact_groups.parse_members_from_contacts() creates its own BQReader
internally. In Cloud Function context this is a no-op because that path
is only triggered by xero_contact_groups files; the cross-reference from
xero_contacts is populated separately when contacts files are processed.
Full membership union requires a batch run via bq_reader.
"""

import json
import logging
import re
from datetime import datetime, timezone

import functions_framework
from google.cloud import storage

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

PROJECT = "prj-dw-dev"

# raw/{vendor}/{tenant_id}/v{n}/{entity_type}/{date}/{record_id}.json
_PATH_RE = re.compile(
    r"raw/(?P<vendor>[^/]+)"
    r"/(?P<tenant_id>[^/]+)"
    r"/v\d+"
    r"/(?P<entity_type>[^/]+)"
    r"/[^/]+"                   # date
    r"/(?P<record_id>[^/]+)\.json$"
)

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
    # "visma": VISMA_PARSERS,  # added when Visma ETL is built
}


class _SingleRecordReader:
    """
    Thin adapter so a single GCS-sourced record can be passed to any
    parser's run() function without modification.

    Mimics BQReader.iter_records() — yields one record regardless of
    the table name or filters passed. The table argument is ignored
    because the Cloud Function already knows which entity it is processing.
    """

    def __init__(self, record: dict, project: str):
        self.project = project
        self._record = record

    def iter_records(self, table: str, **kwargs):  # noqa: ARG002
        yield self._record


@functions_framework.cloud_event
def process_gcs_upload(cloud_event):
    """Entry point: triggered on every GCS object finalise event."""
    data        = cloud_event.data
    bucket_name = data["bucket"]
    object_name = data["name"]

    m = _PATH_RE.search(object_name)
    if not m:
        logger.warning("Skipping unrecognised GCS path: %s", object_name)
        return

    vendor      = m.group("vendor")
    tenant_id   = m.group("tenant_id")
    entity_type = m.group("entity_type")
    record_id   = m.group("record_id")

    # Download JSON
    gcs  = storage.Client()
    blob = gcs.bucket(bucket_name).blob(object_name)
    blob.reload()
    payload = json.loads(blob.download_as_text())

    now = datetime.now(tz=timezone.utc)
    record = {
        "tenant_id":    tenant_id,
        "record_id":    record_id,
        "payload":      payload,
        "first_seen_at": blob.time_created or now,
        "last_seen_at":  blob.updated or now,
        "synced_at":     now,
    }

    # Route
    vendor_map = VENDOR_PARSERS.get(vendor)
    if vendor_map is None:
        logger.warning("No parsers registered for vendor: %s", vendor)
        return

    parser = vendor_map.get(entity_type)
    if parser is None:
        logger.warning("No parser for %s/%s", vendor, entity_type)
        return

    staging_dataset = f"dw_2_staging_{vendor}"
    writer = BQWriter(project=PROJECT, dataset=staging_dataset)
    reader = _SingleRecordReader(record, project=PROJECT)

    result = parser.run(reader, writer)
    logger.info("Processed gs://%s/%s → %s", bucket_name, object_name, result)
