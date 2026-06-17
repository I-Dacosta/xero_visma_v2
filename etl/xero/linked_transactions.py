"""
Xero linked transactions (billable expenses) parser.

Reads from:   dw_1_bronze_xero.xero_linked_transactions
Writes to:    dw_2_staging_xero.linked_transactions   (flat — no nested arrays)
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_linked_transactions"
HEADER_TABLE = "linked_transactions"


def parse_header(record: dict) -> dict:
    p = record["payload"]
    return {
        "tenant_id":                    record["tenant_id"],
        "record_id":                    record["record_id"],
        "synced_at":                    record["last_seen_at"],
        "first_seen_at":                record["first_seen_at"],

        "linked_transaction_id":        p.get("LinkedTransactionID"),
        "source_transaction_id":        p.get("SourceTransactionID"),
        "source_line_item_id":          p.get("SourceLineItemID"),
        "source_transaction_type":      p.get("SourceTransactionTypeCode"),
        "contact_id":                   p.get("ContactID"),
        "linked_transaction_type":      p.get("Type"),
        "status":                       p.get("Status"),
        "updated_at":                   parse_xero_datetime(p.get("UpdatedDateUTC")),
    }


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)
    logger.info("linked_transactions: %d rows", len(headers))
    return {"headers": len(headers)}
