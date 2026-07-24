"""
Xero overpayments parser.

Reads from:   dw_1_bronze_xero.xero_overpayments
Writes to:    dw_2_staging_xero.overpayments       (header)
              dw_2_staging_xero.overpayment_lines  (LineItems[])

Restored 2026-07-24 (removed 2026-07-07 under the bucket-driven parser
policy, when bronze was still empty) after tracing
`journals.source_type IN ('APOVERPAYMENT', 'AROVERPAYMENT')` postings back
to this endpoint — see docs/DWH_ARCHITECTURE.md. Verified against a live
non-empty payload before restoring: every original header field is still
present. Added LineItems[] unpacking (not in the 2026-06 header-only
version) — the account-level grain it carries is what the GL reconciliation
needs; header alone only has the total.
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)
BRONZE_TABLE = "xero_overpayments"
HEADER_TABLE = "overpayments"
LINE_TABLE   = "overpayment_lines"


def _t(items, index, field):
    return items[index].get(field) if index < len(items) else None


def parse_header(record):
    p = record["payload"]
    contact = p.get("Contact") or {}
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],
        "overpayment_id": p.get("OverpaymentID"),
        "overpayment_type": p.get("Type"), "status": p.get("Status"),
        "line_amount_types": p.get("LineAmountTypes"),
        # Force STRING — some orgs enter purely numeric references, which
        # Xero serializes unquoted, so the raw value can be str or number
        # depending on tenant (hit this exact type race restoring this
        # parser 2026-07-24; see the matching note in bank_transfers.py).
        "reference": str(p["Reference"]) if p.get("Reference") is not None else None,
        "contact_id": contact.get("ContactID"), "contact_name": contact.get("Name"),
        "overpayment_date": parse_xero_datetime(p.get("Date")),
        "updated_at": parse_xero_datetime(p.get("UpdatedDateUTC")),
        "currency_code": p.get("CurrencyCode"), "currency_rate": p.get("CurrencyRate"),
        "sub_total": p.get("SubTotal"), "total_tax": p.get("TotalTax"),
        "total": p.get("Total"), "remaining_credit": p.get("RemainingCredit"),
        "has_attachments": p.get("HasAttachments"),
        "line_item_count": len(p.get("LineItems") or []),
        "allocation_count": len(p.get("Allocations") or []),
        "payment_count": len(p.get("Payments") or []),
    }


def parse_lines(record):
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for line in p.get("LineItems") or []:
        tracking = line.get("Tracking") or []
        result.append({
            "tenant_id":                tenant_id,
            "record_id":                record_id,
            "overpayment_id":           p.get("OverpaymentID"),
            "line_item_id":             line.get("LineItemID"),
            "account_id":               line.get("AccountID"),
            "account_code":             line.get("AccountCode"),
            "description":              line.get("Description"),
            "quantity":                 line.get("Quantity"),
            "unit_amount":              line.get("UnitAmount"),
            "line_amount":              line.get("LineAmount"),
            "tax_amount":               line.get("TaxAmount"),
            "tax_type":                 line.get("TaxType"),
            "tracking_category_1_id":   _t(tracking, 0, "TrackingCategoryID"),
            "tracking_category_1_name": _t(tracking, 0, "Name"),
            "tracking_option_1_id":     _t(tracking, 0, "TrackingOptionID"),
            "tracking_option_1_name":   _t(tracking, 0, "Option"),
            "tracking_category_2_id":   _t(tracking, 1, "TrackingCategoryID"),
            "tracking_category_2_name": _t(tracking, 1, "Name"),
            "tracking_option_2_id":     _t(tracking, 1, "TrackingOptionID"),
            "tracking_option_2_name":   _t(tracking, 1, "Option"),
        })
    return result


def run(reader, writer, tenant_id=None, limit=None):
    headers, lines = [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        lines.extend(parse_lines(record))
    if headers:
        writer.merge(HEADER_TABLE, headers)
    if lines:
        writer.merge(LINE_TABLE, lines,
                     key_columns=("tenant_id", "record_id", "line_item_id"))
    logger.info("overpayments: %d headers, %d lines", len(headers), len(lines))
    return {"headers": len(headers), "lines": len(lines)}
