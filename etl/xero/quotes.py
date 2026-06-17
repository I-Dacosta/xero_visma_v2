"""
Xero quotes parser.

Reads from:   dw_1_bronze_xero.xero_quotes
Writes to:    dw_2_staging_xero.quotes        (header)
              dw_2_staging_xero.quote_lines   (LineItems[])

⚠️ Dates have NO timezone offset: /Date(ms)/ — use permissive regex via
   parse_xero_datetime which already handles both formats.

Note: Quote lines may not carry AccountCode or AccountID.
      DiscountAmount appears (not DiscountRate).
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime, parse_iso_date

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_quotes"
HEADER_TABLE = "quotes"
LINE_TABLE   = "quote_lines"


def _t(items, index, field):
    return items[index].get(field) if index < len(items) else None


def parse_header(record: dict) -> dict:
    p       = record["payload"]
    contact = p.get("Contact") or {}
    return {
        "tenant_id":        record["tenant_id"],
        "record_id":        record["record_id"],
        "synced_at":        record["last_seen_at"],
        "first_seen_at":    record["first_seen_at"],

        "quote_id":         p.get("QuoteID"),
        "quote_number":     p.get("QuoteNumber"),

        "contact_id":       contact.get("ContactID"),
        "contact_name":     contact.get("Name"),
        "contact_email":    contact.get("EmailAddress"),
        "branding_theme_id": p.get("BrandingThemeID"),

        "status":           p.get("Status"),
        "reference":        p.get("Reference"),
        "line_amount_types": p.get("LineAmountTypes"),
        "terms":            p.get("Terms"),

        "quote_date":       parse_xero_datetime(p.get("Date")),
        "quote_date_local": parse_iso_date(p.get("DateString")),
        "expiry_date":      parse_xero_datetime(p.get("ExpiryDate")),
        "expiry_date_local": parse_iso_date(p.get("ExpiryDateString")),
        "updated_at":       parse_xero_datetime(p.get("UpdatedDateUTC")),

        "currency_code":    p.get("CurrencyCode"),
        "currency_rate":    p.get("CurrencyRate"),
        "sub_total":        p.get("SubTotal"),
        "total_tax":        p.get("TotalTax"),
        "total":            p.get("Total"),
        "total_discount":   p.get("TotalDiscount"),

        "line_item_count":  len(p.get("LineItems") or []),
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
            "quote_id":                 p.get("QuoteID"),
            "line_item_id":             line.get("LineItemID"),
            "description":              line.get("Description"),
            "quantity":                 line.get("Quantity"),
            "unit_amount":              line.get("UnitAmount"),
            "line_amount":              line.get("LineAmount"),
            "tax_amount":               line.get("TaxAmount"),
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


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers, lines = [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        lines.extend(parse_lines(record))
    writer.merge(HEADER_TABLE, headers)
    if lines:
        writer.merge(LINE_TABLE, lines,
                     key_columns=("tenant_id", "record_id", "line_item_id"))
    logger.info("quotes: %d headers, %d lines", len(headers), len(lines))
    return {"headers": len(headers), "lines": len(lines)}
