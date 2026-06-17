"""
Xero credit notes parser.

Reads from:   dw_1_bronze_xero.xero_credit_notes
Writes to:    dw_2_staging_xero.credit_notes              (header)
              dw_2_staging_xero.credit_note_lines         (LineItems[])
              dw_2_staging_xero.credit_note_allocations   (Allocations[])
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime, parse_iso_date

logger = logging.getLogger(__name__)

BRONZE_TABLE      = "xero_credit_notes"
HEADER_TABLE      = "credit_notes"
LINE_TABLE        = "credit_note_lines"
ALLOCATION_TABLE  = "credit_note_allocations"


def _t(items, index, field):
    return items[index].get(field) if index < len(items) else None


def parse_header(record: dict) -> dict:
    p       = record["payload"]
    contact = p.get("Contact") or {}
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "credit_note_id":       p.get("CreditNoteID"),
        "credit_note_number":   p.get("CreditNoteNumber"),

        "contact_id":           contact.get("ContactID"),
        "contact_name":         contact.get("Name"),

        "credit_note_type":     p.get("Type"),
        "status":               p.get("Status"),
        "line_amount_types":    p.get("LineAmountTypes"),
        "reference":            p.get("Reference"),

        "credit_note_date":     parse_xero_datetime(p.get("Date")),
        "credit_note_date_local": parse_iso_date(p.get("DateString")),
        "due_date":             parse_xero_datetime(p.get("DueDate")),
        "fully_paid_on_date":   parse_xero_datetime(p.get("FullyPaidOnDate")),
        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),

        "currency_code":        p.get("CurrencyCode"),
        "currency_rate":        p.get("CurrencyRate"),
        "sub_total":            p.get("SubTotal"),
        "total_tax":            p.get("TotalTax"),
        "total":                p.get("Total"),
        "remaining_credit":     p.get("RemainingCredit"),

        "sent_to_contact":      p.get("SentToContact"),
        "has_attachments":      p.get("HasAttachments"),
        "has_errors":           p.get("HasErrors"),

        "line_item_count":      len(p.get("LineItems") or []),
        "allocation_count":     len(p.get("Allocations") or []),
        "payment_count":        len(p.get("Payments") or []),
    }


def parse_lines(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for line in p.get("LineItems") or []:
        tracking = line.get("Tracking") or []
        result.append({
            "tenant_id":                tenant_id,
            "record_id":                record_id,
            "credit_note_id":           p.get("CreditNoteID"),
            "line_item_id":             line.get("LineItemID"),
            "account_id":               line.get("AccountID"),
            "account_code":             line.get("AccountCode"),
            "description":              line.get("Description"),
            "quantity":                 line.get("Quantity"),
            "unit_amount":              line.get("UnitAmount"),
            "line_amount":              line.get("LineAmount"),
            "tax_amount":               line.get("TaxAmount"),
            "tax_type":                 line.get("TaxType"),
            "discount_rate":            line.get("DiscountRate"),
            "discount_amount":          line.get("DiscountAmount"),
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


def parse_allocations(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for alloc in p.get("Allocations") or []:
        invoice = alloc.get("Invoice") or {}
        result.append({
            "tenant_id":        tenant_id,
            "record_id":        record_id,
            "credit_note_id":   p.get("CreditNoteID"),
            "allocation_id":    alloc.get("AllocationID"),
            "invoice_id":       invoice.get("InvoiceID"),
            "invoice_number":   invoice.get("InvoiceNumber"),
            "allocated_amount": alloc.get("Amount"),
            "is_deleted":       alloc.get("IsDeleted"),
            "allocation_date":  parse_xero_datetime(alloc.get("Date")),
        })
    return result


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers, lines, allocations = [], [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        lines.extend(parse_lines(record))
        allocations.extend(parse_allocations(record))
    writer.merge(HEADER_TABLE, headers)
    if lines:
        writer.merge(LINE_TABLE, lines,
                     key_columns=("tenant_id", "record_id", "line_item_id"))
    if allocations:
        writer.merge(ALLOCATION_TABLE, allocations,
                     key_columns=("tenant_id", "record_id", "allocation_id"))
    logger.info("credit_notes: %d headers, %d lines, %d allocations",
                len(headers), len(lines), len(allocations))
    return {"headers": len(headers), "lines": len(lines), "allocations": len(allocations)}
