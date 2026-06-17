"""
Xero manual journals parser.

Reads from:   dw_1_bronze_xero.xero_manual_journals
Writes to:    dw_2_staging_xero.manual_journals        (header)
              dw_2_staging_xero.manual_journal_lines   (JournalLines[])

Note: Manual journal lines use Tracking[] (not TrackingCategories[]).
      Lines have no LineItemID — grain is (tenant_id, record_id, account_id).
      IsBlank=True lines are filtered out.
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_manual_journals"
HEADER_TABLE = "manual_journals"
LINE_TABLE   = "manual_journal_lines"


def _t(items, index, field):
    return items[index].get(field) if index < len(items) else None


def parse_header(record: dict) -> dict:
    p = record["payload"]
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "manual_journal_id":    p.get("ManualJournalID"),
        "narration":            p.get("Narration"),
        "status":               p.get("Status"),
        "line_amount_types":    p.get("LineAmountTypes"),

        "journal_date":         parse_xero_datetime(p.get("Date")),
        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),

        "debit_total":          p.get("DebitTotal"),
        "credit_total":         p.get("CreditTotal"),

        "show_on_cash_basis_reports": p.get("ShowOnCashBasisReports"),
        "has_attachments":      p.get("HasAttachments"),
        "journal_line_count":   len(p.get("JournalLines") or []),
    }


def parse_lines(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for line in p.get("JournalLines") or []:
        if line.get("IsBlank"):
            continue
        tracking = line.get("Tracking") or []
        result.append({
            "tenant_id":                tenant_id,
            "record_id":                record_id,
            "manual_journal_id":        p.get("ManualJournalID"),
            "account_id":               line.get("AccountID"),
            "account_code":             line.get("AccountCode"),
            "description":              line.get("Description"),
            "line_amount":              line.get("LineAmount"),
            "tax_amount":               line.get("TaxAmount"),
            "tax_type":                 line.get("TaxType"),
            "is_blank":                 line.get("IsBlank"),
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
                     key_columns=("tenant_id", "record_id", "account_id"))
    logger.info("manual_journals: %d headers, %d lines", len(headers), len(lines))
    return {"headers": len(headers), "lines": len(lines)}
