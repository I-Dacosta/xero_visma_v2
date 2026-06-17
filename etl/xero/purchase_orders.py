"""
Xero purchase orders parser.

Reads from:   dw_1_bronze_xero.xero_purchase_orders
Writes to:    dw_2_staging_xero.purchase_orders        (header)
              dw_2_staging_xero.purchase_order_lines   (LineItems[])
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime, parse_iso_date

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_purchase_orders"
HEADER_TABLE = "purchase_orders"
LINE_TABLE   = "purchase_order_lines"


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

        "purchase_order_id":    p.get("PurchaseOrderID"),
        "purchase_order_number": p.get("PurchaseOrderNumber"),
        "purchase_order_type":  p.get("Type"),

        "contact_id":           contact.get("ContactID"),
        "contact_name":         contact.get("Name"),
        "branding_theme_id":    p.get("BrandingThemeID"),

        "status":               p.get("Status"),
        "reference":            p.get("Reference"),
        "line_amount_types":    p.get("LineAmountTypes"),

        "attention_to":         p.get("AttentionTo"),
        "telephone":            p.get("Telephone"),
        "delivery_address":     p.get("DeliveryAddress"),
        "delivery_instructions": p.get("DeliveryInstructions"),

        "order_date":           parse_xero_datetime(p.get("Date")),
        "order_date_local":     parse_iso_date(p.get("DateString")),
        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),

        "currency_code":        p.get("CurrencyCode"),
        "currency_rate":        p.get("CurrencyRate"),
        "sub_total":            p.get("SubTotal"),
        "total_tax":            p.get("TotalTax"),
        "total":                p.get("Total"),

        "is_discounted":        p.get("IsDiscounted"),
        "has_attachments":      p.get("HasAttachments"),
        "has_errors":           p.get("HasErrors"),
        "line_item_count":      len(p.get("LineItems") or []),
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
            "purchase_order_id":        p.get("PurchaseOrderID"),
            "line_item_id":             line.get("LineItemID"),
            "item_code":                line.get("ItemCode"),
            "account_code":             line.get("AccountCode"),
            "description":              line.get("Description"),
            "quantity":                 line.get("Quantity"),
            "unit_amount":              line.get("UnitAmount"),
            "line_amount":              line.get("LineAmount"),
            "tax_amount":               line.get("TaxAmount"),
            "tax_type":                 line.get("TaxType"),
            "discount_rate":            line.get("DiscountRate"),
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
    logger.info("purchase_orders: %d headers, %d lines", len(headers), len(lines))
    return {"headers": len(headers), "lines": len(lines)}
