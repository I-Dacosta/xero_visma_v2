"""
Xero items parser.

Reads from:   dw_1_bronze_xero.xero_items
Writes to:    dw_2_staging_xero.items   (flat — PurchaseDetails/SalesDetails inlined)
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_items"
HEADER_TABLE = "items"


def parse_header(record: dict) -> dict:
    p       = record["payload"]
    sales   = p.get("SalesDetails") or {}
    purchase = p.get("PurchaseDetails") or {}
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "item_id":              p.get("ItemID"),
        "item_code":            p.get("Code"),
        "item_name":            p.get("Name"),
        "description":          p.get("Description"),
        "purchase_description": p.get("PurchaseDescription"),

        "is_sold":              p.get("IsSold"),
        "is_purchased":         p.get("IsPurchased"),
        "is_tracked_as_inventory": p.get("IsTrackedAsInventory"),

        "quantity_on_hand":     p.get("QuantityOnHand"),
        "total_cost_pool":      p.get("TotalCostPool"),

        "sales_account_code":   sales.get("AccountCode"),
        "sales_tax_type":       sales.get("TaxType"),
        "sales_unit_price":     sales.get("UnitPrice"),

        "cogs_account_code":    purchase.get("COGSAccountCode"),
        "purchase_tax_type":    purchase.get("TaxType"),
        "purchase_unit_price":  purchase.get("UnitPrice"),

        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),
    }


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)
    logger.info("items: %d rows", len(headers))
    return {"headers": len(headers)}
