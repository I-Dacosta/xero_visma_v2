"""
Xero journals parser.

Reads from:   dw_1_bronze_xero.xero_journals
Writes to:    dw_2_staging_xero.journals        (header)
              dw_2_staging_xero.journal_lines   (JournalLines[])

⚠️ Journal lines use TrackingCategories[] not Tracking[] (Xero inconsistency).
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_journals"
HEADER_TABLE = "journals"
LINE_TABLE   = "journal_lines"


def _tc(items, index, field):
    return items[index].get(field) if index < len(items) else None


def parse_header(record: dict) -> dict:
    p = record["payload"]
    return {
        "tenant_id":        record["tenant_id"],
        "record_id":        record["record_id"],
        "synced_at":        record["last_seen_at"],
        "first_seen_at":    record["first_seen_at"],

        "journal_id":       p.get("JournalID"),
        "journal_number":   p.get("JournalNumber"),
        "source_id":        p.get("SourceID"),
        "source_type":      p.get("SourceType"),
        "reference":        p.get("Reference"),

        "journal_date":     parse_xero_datetime(p.get("JournalDate")),
        "created_at":       parse_xero_datetime(p.get("CreatedDateUTC")),

        "journal_line_count": len(p.get("JournalLines") or []),
    }


def parse_lines(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for line in p.get("JournalLines") or []:
        # Journals use TrackingCategories[], not Tracking[]
        tc = line.get("TrackingCategories") or []
        result.append({
            "tenant_id":                tenant_id,
            "record_id":                record_id,
            "journal_id":               p.get("JournalID"),
            "journal_line_id":          line.get("JournalLineID"),
            "account_id":               line.get("AccountID"),
            "account_code":             line.get("AccountCode"),
            "account_name":             line.get("AccountName"),
            "account_type":             line.get("AccountType"),
            "description":              line.get("Description"),
            "gross_amount":             line.get("GrossAmount"),
            "net_amount":               line.get("NetAmount"),
            "tax_amount":               line.get("TaxAmount"),
            "tracking_category_1_id":   _tc(tc, 0, "TrackingCategoryID"),
            "tracking_category_1_name": _tc(tc, 0, "Name"),
            "tracking_option_1_name":   _tc(tc, 0, "Option"),
            "tracking_category_2_id":   _tc(tc, 1, "TrackingCategoryID"),
            "tracking_category_2_name": _tc(tc, 1, "Name"),
            "tracking_option_2_name":   _tc(tc, 1, "Option"),
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
                     key_columns=("tenant_id", "record_id", "journal_line_id"))
    logger.info("journals: %d headers, %d lines", len(headers), len(lines))
    return {"headers": len(headers), "lines": len(lines)}
