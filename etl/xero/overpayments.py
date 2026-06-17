"""Xero overpayments parser. Bronze currently empty."""
import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)
BRONZE_TABLE = "xero_overpayments"
HEADER_TABLE = "overpayments"

def parse_header(record):
    p = record["payload"]
    contact = p.get("Contact") or {}
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],
        "overpayment_id": p.get("OverpaymentID"),
        "overpayment_type": p.get("Type"), "status": p.get("Status"),
        "line_amount_types": p.get("LineAmountTypes"),
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

def run(reader, writer, tenant_id=None, limit=None):
    headers = [parse_header(r) for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    if headers:
        writer.merge(HEADER_TABLE, headers)
    logger.info("overpayments: %d rows", len(headers))
    return {"headers": len(headers)}
