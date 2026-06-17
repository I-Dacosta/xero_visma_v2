"""
Xero bank transactions parser.

Reads from:   dw_1_bronze_xero.xero_bank_transactions
Writes to:    dw_2_staging_xero.bank_transactions         (header)
              dw_2_staging_xero.bank_transaction_lines    (line items)

Grain:
    bank_transactions       — 1 row per (tenant_id, record_id)
    bank_transaction_lines  — 1 row per (tenant_id, record_id, line_item_id)
"""

import logging
from typing import Any

from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime, parse_iso_date

logger = logging.getLogger(__name__)

BRONZE_TABLE  = "xero_bank_transactions"
HEADER_TABLE  = "bank_transactions"
LINE_TABLE    = "bank_transaction_lines"


def _tracking(items: list[dict], index: int, field: str) -> Any:
    """Safely extract a field from a tracking category by index."""
    if index < len(items):
        return items[index].get(field)
    return None


def parse_header(record: dict) -> dict:
    """Parse a bronze record into a bank_transactions staging row."""
    p = record["payload"]
    bank_account = p.get("BankAccount") or {}
    contact      = p.get("Contact") or {}

    return {
        "tenant_id":                record["tenant_id"],
        "record_id":                record["record_id"],
        "synced_at":                record["last_seen_at"],
        "first_seen_at":            record["first_seen_at"],

        # Natural key
        "bank_transaction_id":      p.get("BankTransactionID"),

        # Foreign keys
        "bank_account_id":          bank_account.get("AccountID"),
        "bank_account_code":        bank_account.get("Code"),
        "bank_account_name":        bank_account.get("Name"),
        "contact_id":               contact.get("ContactID"),
        "contact_name":             contact.get("Name"),

        # Type & status
        "transaction_type":         p.get("Type"),
        "status":                   p.get("Status"),
        "line_amount_types":        p.get("LineAmountTypes"),
        "reference":                p.get("Reference"),
        "url":                      p.get("URL"),

        # Dates
        "transaction_date":         parse_xero_datetime(p.get("Date")),
        "transaction_date_local":   parse_iso_date(p.get("DateString")),
        "updated_at":               parse_xero_datetime(p.get("UpdatedDateUTC")),

        # Currency
        "currency_code":            p.get("CurrencyCode"),
        "currency_rate":            p.get("CurrencyRate"),

        # Amounts
        "sub_total":                p.get("SubTotal"),
        "total_tax":                p.get("TotalTax"),
        "total":                    p.get("Total"),

        # Flags
        "is_reconciled":            p.get("IsReconciled"),
        "has_attachments":          p.get("HasAttachments"),

        # Summary count
        "line_item_count":          len(p.get("LineItems") or []),
    }


def parse_lines(record: dict) -> list[dict]:
    """Parse LineItems[] into bank_transaction_lines staging rows."""
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    txn_id    = p.get("BankTransactionID")
    lines     = p.get("LineItems") or []
    result    = []

    for line in lines:
        tracking = line.get("Tracking") or []
        result.append({
            "tenant_id":                tenant_id,
            "record_id":                record_id,
            "bank_transaction_id":      txn_id,
            "line_item_id":             line.get("LineItemID"),

            # Account
            "account_id":               line.get("AccountID"),
            "account_code":             line.get("AccountCode"),

            # Amounts
            "description":              line.get("Description"),
            "quantity":                 line.get("Quantity"),
            "unit_amount":              line.get("UnitAmount"),
            "line_amount":              line.get("LineAmount"),
            "tax_amount":               line.get("TaxAmount"),
            "tax_type":                 line.get("TaxType"),

            # Tracking category 1
            "tracking_category_1_id":   _tracking(tracking, 0, "TrackingCategoryID"),
            "tracking_category_1_name": _tracking(tracking, 0, "Name"),
            "tracking_option_1_id":     _tracking(tracking, 0, "TrackingOptionID"),
            "tracking_option_1_name":   _tracking(tracking, 0, "Option"),

            # Tracking category 2
            "tracking_category_2_id":   _tracking(tracking, 1, "TrackingCategoryID"),
            "tracking_category_2_name": _tracking(tracking, 1, "Name"),
            "tracking_option_2_id":     _tracking(tracking, 1, "TrackingOptionID"),
            "tracking_option_2_name":   _tracking(tracking, 1, "Option"),
        })

    return result


def run(
    reader: BQReader,
    writer: BQWriter,
    tenant_id: str | None = None,
    limit: int | None = None,
) -> dict:
    """
    Read all bank transactions from bronze, parse, and write to staging.

    Returns:
        {"headers": N, "lines": N}
    """
    headers = []
    lines   = []

    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        lines.extend(parse_lines(record))

    logger.info("Parsed %d headers, %d lines", len(headers), len(lines))

    writer.merge(HEADER_TABLE, headers)
    if lines:
        writer.merge(
            LINE_TABLE,
            lines,
            key_columns=("tenant_id", "record_id", "line_item_id"),
        )

    return {"headers": len(headers), "lines": len(lines)}
