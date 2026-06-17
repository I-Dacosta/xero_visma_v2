"""
Xero invoices parser.

Reads from:   dw_1_bronze_xero.xero_invoices
Writes to:    dw_2_staging_xero.invoices              (header)
              dw_2_staging_xero.invoice_lines         (LineItems[])
              dw_2_staging_xero.invoice_payments      (Payments[])

Types: ACCREC (AR invoice to customer), ACCPAY (AP bill from supplier)
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime, parse_iso_date

logger = logging.getLogger(__name__)

BRONZE_TABLE   = "xero_invoices"
HEADER_TABLE   = "invoices"
LINE_TABLE     = "invoice_lines"
PAYMENT_TABLE  = "invoice_payments"


def _tracking(items, index, field):
    return items[index].get(field) if index < len(items) else None


def parse_header(record: dict) -> dict:
    p       = record["payload"]
    contact = p.get("Contact") or {}
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "invoice_id":           p.get("InvoiceID"),
        "invoice_number":       p.get("InvoiceNumber"),

        "contact_id":           contact.get("ContactID"),
        "contact_name":         contact.get("Name"),
        "branding_theme_id":    p.get("BrandingThemeID"),

        "invoice_type":         p.get("Type"),
        "status":               p.get("Status"),
        "line_amount_types":    p.get("LineAmountTypes"),
        "reference":            p.get("Reference"),

        "invoice_date":         parse_xero_datetime(p.get("Date")),
        "invoice_date_local":   parse_iso_date(p.get("DateString")),
        "due_date":             parse_xero_datetime(p.get("DueDate")),
        "due_date_local":       parse_iso_date(p.get("DueDateString")),
        "fully_paid_on_date":   parse_xero_datetime(p.get("FullyPaidOnDate")),
        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),

        "currency_code":        p.get("CurrencyCode"),
        "currency_rate":        p.get("CurrencyRate"),

        "sub_total":            p.get("SubTotal"),
        "total_tax":            p.get("TotalTax"),
        "total":                p.get("Total"),
        "amount_due":           p.get("AmountDue"),
        "amount_paid":          p.get("AmountPaid"),
        "amount_credited":      p.get("AmountCredited"),

        "sent_to_contact":      p.get("SentToContact"),
        "is_discounted":        p.get("IsDiscounted"),
        "has_attachments":      p.get("HasAttachments"),
        "has_errors":           p.get("HasErrors"),

        "line_item_count":      len(p.get("LineItems") or []),
        "payment_count":        len(p.get("Payments") or []),
        "credit_note_count":    len(p.get("CreditNotes") or []),
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
            "invoice_id":               p.get("InvoiceID"),
            "line_item_id":             line.get("LineItemID"),
            "account_id":               line.get("AccountID"),
            "account_code":             line.get("AccountCode"),
            "item_code":                line.get("ItemCode"),
            "description":              line.get("Description"),
            "quantity":                 line.get("Quantity"),
            "unit_amount":              line.get("UnitAmount"),
            "line_amount":              line.get("LineAmount"),
            "tax_amount":               line.get("TaxAmount"),
            "tax_type":                 line.get("TaxType"),
            "discount_rate":            line.get("DiscountRate"),
            "discount_amount":          line.get("DiscountAmount"),
            "tracking_category_1_id":   _tracking(tracking, 0, "TrackingCategoryID"),
            "tracking_category_1_name": _tracking(tracking, 0, "Name"),
            "tracking_option_1_id":     _tracking(tracking, 0, "TrackingOptionID"),
            "tracking_option_1_name":   _tracking(tracking, 0, "Option"),
            "tracking_category_2_id":   _tracking(tracking, 1, "TrackingCategoryID"),
            "tracking_category_2_name": _tracking(tracking, 1, "Name"),
            "tracking_option_2_id":     _tracking(tracking, 1, "TrackingOptionID"),
            "tracking_option_2_name":   _tracking(tracking, 1, "Option"),
        })
    return result


def parse_payments(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for pmt in p.get("Payments") or []:
        result.append({
            "tenant_id":    tenant_id,
            "record_id":    record_id,
            "invoice_id":   p.get("InvoiceID"),
            "payment_id":   pmt.get("PaymentID"),
            "amount":       pmt.get("Amount"),
            "currency_rate": pmt.get("CurrencyRate"),
            "reference":    pmt.get("Reference"),
            "payment_date": parse_xero_datetime(pmt.get("Date")),
        })
    return result


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers, lines, payments = [], [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        lines.extend(parse_lines(record))
        payments.extend(parse_payments(record))

    writer.merge(HEADER_TABLE, headers)
    if lines:
        writer.merge(LINE_TABLE, lines,
                     key_columns=("tenant_id", "record_id", "line_item_id"))
    if payments:
        writer.merge(PAYMENT_TABLE, payments,
                     key_columns=("tenant_id", "record_id", "payment_id"))

    logger.info("invoices: %d headers, %d lines, %d payments",
                len(headers), len(lines), len(payments))
    return {"headers": len(headers), "lines": len(lines), "payments": len(payments)}
