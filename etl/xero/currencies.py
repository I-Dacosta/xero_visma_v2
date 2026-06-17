"""Xero currencies parser."""
import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter

logger = logging.getLogger(__name__)
BRONZE_TABLE = "xero_currencies"
HEADER_TABLE = "currencies"

def parse_header(record):
    p = record["payload"]
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],
        "currency_code": p.get("Code"), "currency_description": p.get("Description"),
    }

def run(reader, writer, tenant_id=None, limit=None):
    headers = [parse_header(r) for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)
    return {"headers": len(headers)}
