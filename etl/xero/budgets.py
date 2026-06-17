"""
Xero budgets parser.

Reads from:   dw_1_bronze_xero.xero_budgets
Writes to:    dw_2_staging_xero.budgets        (header)
              dw_2_staging_xero.budget_lines   (BudgetLines[] × BudgetBalances[])

⚠️ BudgetLines[] is only present if the Rust sync calls
   GET /Budgets/{ID}?BudgetLines=true per budget. Without this,
   budget_lines will always be empty. See DWH_ARCHITECTURE.md.
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime, parse_iso_date

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_budgets"
HEADER_TABLE = "budgets"
LINE_TABLE   = "budget_lines"


def parse_header(record):
    p = record["payload"]
    tracking = (p.get("Tracking") or [{}])[0]
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],
        "budget_id": p.get("BudgetID"),
        "budget_type": p.get("Type"),
        "description": p.get("Description"),
        "updated_at": parse_xero_datetime(p.get("UpdatedDateUTC")),
        "tracking_category_id": tracking.get("TrackingCategoryID"),
        "tracking_category_name": tracking.get("TrackingCategoryName"),
        "tracking_option_id": tracking.get("TrackingOptionID"),
        "tracking_option_name": tracking.get("Name"),
        "budget_line_count": len(p.get("BudgetLines") or []),
    }


def parse_lines(record):
    p = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result = []
    for line in p.get("BudgetLines") or []:
        for balance in line.get("BudgetBalances") or []:
            result.append({
                "tenant_id":        tenant_id,
                "record_id":        record_id,
                "budget_id":        p.get("BudgetID"),
                "account_id":       line.get("AccountID"),
                "account_code":     line.get("AccountCode"),
                "period":           parse_xero_datetime(balance.get("Period")),
                "period_date_local": parse_iso_date(balance.get("PeriodDate")),
                "amount":           balance.get("Amount"),
                "unit_amount":      balance.get("UnitAmount"),
                "notes":            balance.get("Notes"),
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
                     key_columns=("tenant_id", "record_id", "account_id", "period"))
    logger.info("budgets: %d headers, %d lines", len(headers), len(lines))
    return {"headers": len(headers), "lines": len(lines)}
