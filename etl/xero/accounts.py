"""
Xero accounts (chart of accounts) parser.

Reads from:   dw_1_bronze_xero.xero_accounts
Writes to:    dw_2_staging_xero.accounts   (flat — no nested arrays)

Note: bs_pl and fsli_1 are pre-derived here using the same labels as Visma
      so the ODS cross-provider layer can UNION both without extra logic.
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_accounts"
HEADER_TABLE = "accounts"

_BS_PL = {
    "ASSET": "BS", "LIABILITY": "BS", "EQUITY": "BS",
    "REVENUE": "P&L", "EXPENSE": "P&L",
}
_FSLI_1 = {
    "ASSET": "Assets",
    "LIABILITY": "Equity and liabilities",
    "EQUITY": "Equity and liabilities",
    "REVENUE": "Revenue",
    "EXPENSE": "Operating expenses",
}


def parse_header(record: dict) -> dict:
    p     = record["payload"]
    cls   = p.get("Class") or ""
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "account_id":           p.get("AccountID"),
        "account_code":         p.get("Code"),
        "account_name":         p.get("Name"),
        "account_type":         p.get("Type"),
        "account_class":        cls,
        "status":               p.get("Status"),
        "is_active":            p.get("Status") == "ACTIVE",
        "tax_type":             p.get("TaxType"),
        "bank_account_type":    p.get("BankAccountType"),
        "account_description":  p.get("Description"),

        "reporting_code":       p.get("ReportingCode"),
        "reporting_name":       p.get("ReportingName"),
        "reporting_code_name":  p.get("ReportingCodeName"),

        "show_in_expense_claims":       p.get("ShowInExpenseClaims"),
        "enable_payments_to_account":   p.get("EnablePaymentsToAccount"),
        "add_to_watchlist":             p.get("AddToWatchlist"),
        "has_attachments":              p.get("HasAttachments"),

        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),

        # Pre-derived for cross-provider harmonisation
        "bs_pl":    _BS_PL.get(cls),
        "fsli_1":   _FSLI_1.get(cls),
    }


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)
    logger.info("accounts: %d rows", len(headers))
    return {"headers": len(headers)}
