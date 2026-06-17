"""
Xero tax rates parser.

Reads from:   dw_1_bronze_xero.xero_tax_rates
Writes to:    dw_2_staging_xero.tax_rates   (flat — TaxComponents[0] inlined)

Note: record_id = TaxType string (e.g. "INPUT2"), not a GUID.
      TaxComponents[] always has exactly one element in practice.
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_tax_rates"
HEADER_TABLE = "tax_rates"


def parse_header(record: dict) -> dict:
    p  = record["payload"]
    tc = (p.get("TaxComponents") or [{}])[0]
    return {
        "tenant_id":                    record["tenant_id"],
        "record_id":                    record["record_id"],
        "synced_at":                    record["last_seen_at"],
        "first_seen_at":                record["first_seen_at"],

        "tax_type":                     p.get("TaxType"),
        "tax_name":                     p.get("Name"),
        "status":                       p.get("Status"),
        "is_active":                    p.get("Status") == "ACTIVE",
        "report_tax_type":              p.get("ReportTaxType"),
        "display_tax_rate":             p.get("DisplayTaxRate"),
        "effective_rate":               p.get("EffectiveRate"),

        "can_apply_to_assets":          p.get("CanApplyToAssets"),
        "can_apply_to_equity":          p.get("CanApplyToEquity"),
        "can_apply_to_expenses":        p.get("CanApplyToExpenses"),
        "can_apply_to_liabilities":     p.get("CanApplyToLiabilities"),
        "can_apply_to_revenue":         p.get("CanApplyToRevenue"),

        "tax_component_name":           tc.get("Name"),
        "tax_component_rate":           tc.get("Rate"),
        "tax_component_is_compound":    tc.get("IsCompound"),
        "tax_component_is_non_recoverable": tc.get("IsNonRecoverable"),
    }


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)
    logger.info("tax_rates: %d rows", len(headers))
    return {"headers": len(headers)}
