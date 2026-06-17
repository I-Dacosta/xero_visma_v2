"""
Xero repeating invoices parser.

Reads from:   dw_1_bronze_xero.xero_repeating_invoices
Writes to:    dw_2_staging_xero.repeating_invoices        (header + Schedule{} inlined)
              dw_2_staging_xero.repeating_invoice_lines   (LineItems[])

Note: Schedule.NextScheduledDateString is a bare YYYY-MM-DD string — use
      parse_iso_date, NOT parse_iso_datetime. parse_xero_datetime handles
      the /Date()/ fields within Schedule.
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime, parse_iso_date

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_repeating_invoices"
HEADER_TABLE = "repeating_invoices"
LINE_TABLE   = "repeating_invoice_lines"


def _t(items, i, f): return items[i].get(f) if i < len(items) else None


def parse_header(record):
    p        = record["payload"]
    contact  = p.get("Contact") or {}
    schedule = p.get("Schedule") or {}
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],

        "repeating_invoice_id": p.get("RepeatingInvoiceID"),
        "invoice_type": p.get("Type"), "status": p.get("Status"),
        "reference": p.get("Reference"), "line_amount_types": p.get("LineAmountTypes"),
        "currency_code": p.get("CurrencyCode"),
        "contact_id": contact.get("ContactID"), "contact_name": contact.get("Name"),

        "sub_total": p.get("SubTotal"), "total_tax": p.get("TotalTax"), "total": p.get("Total"),
        "has_attachments": p.get("HasAttachments"),
        "approved_for_sending": p.get("ApprovedForSending"),
        "include_pdf": p.get("IncludePDF"),

        # Schedule flattened
        "schedule_unit": schedule.get("Unit"),
        "schedule_period": schedule.get("Period"),
        "schedule_due_date_type": schedule.get("DueDateType"),
        "schedule_due_date_offset": schedule.get("DueDate"),
        "schedule_start_date": parse_xero_datetime(schedule.get("StartDate")),
        "schedule_end_date": parse_xero_datetime(schedule.get("EndDate")),
        "next_scheduled_date": parse_xero_datetime(schedule.get("NextScheduledDate")),
        # ⚠️ bare YYYY-MM-DD — not T00:00:00
        "next_scheduled_date_local": parse_iso_date(schedule.get("NextScheduledDateString")),

        "line_item_count": len(p.get("LineItems") or []),
    }


def parse_lines(record):
    p = record["payload"]
    result = []
    for line in p.get("LineItems") or []:
        tracking = line.get("Tracking") or []
        result.append({
            "tenant_id": record["tenant_id"], "record_id": record["record_id"],
            "repeating_invoice_id": p.get("RepeatingInvoiceID"),
            "line_item_id": line.get("LineItemID"),
            "account_id": line.get("AccountID"), "account_code": line.get("AccountCode"),
            "description": line.get("Description"),
            "quantity": line.get("Quantity"), "unit_amount": line.get("UnitAmount"),
            "line_amount": line.get("LineAmount"), "tax_amount": line.get("TaxAmount"),
            "tax_type": line.get("TaxType"),
            "tracking_category_1_id": _t(tracking, 0, "TrackingCategoryID"),
            "tracking_category_1_name": _t(tracking, 0, "Name"),
            "tracking_option_1_id": _t(tracking, 0, "TrackingOptionID"),
            "tracking_option_1_name": _t(tracking, 0, "Option"),
            "tracking_category_2_id": _t(tracking, 1, "TrackingCategoryID"),
            "tracking_category_2_name": _t(tracking, 1, "Name"),
            "tracking_option_2_id": _t(tracking, 1, "TrackingOptionID"),
            "tracking_option_2_name": _t(tracking, 1, "Option"),
        })
    return result


def run(reader, writer, tenant_id=None, limit=None):
    headers, lines = [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        lines.extend(parse_lines(record))
    writer.merge(HEADER_TABLE, headers)
    if lines:
        writer.merge(LINE_TABLE, lines,
                     key_columns=("tenant_id", "record_id", "line_item_id"))
    logger.info("repeating_invoices: %d headers, %d lines", len(headers), len(lines))
    return {"headers": len(headers), "lines": len(lines)}
