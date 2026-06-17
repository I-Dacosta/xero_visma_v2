"""
Xero users parser.

Reads from:   dw_1_bronze_xero.xero_users
Writes to:    dw_2_staging_xero.users   (flat — no nested arrays)
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_users"
HEADER_TABLE = "users"


def parse_header(record: dict) -> dict:
    p = record["payload"]
    return {
        "tenant_id":        record["tenant_id"],
        "record_id":        record["record_id"],
        "synced_at":        record["last_seen_at"],
        "first_seen_at":    record["first_seen_at"],

        "user_id":          p.get("UserID"),
        "global_user_id":   p.get("GlobalUserID"),
        "first_name":       p.get("FirstName"),
        "last_name":        p.get("LastName"),
        "email_address":    p.get("EmailAddress"),
        "organisation_role": p.get("OrganisationRole"),
        "is_subscriber":    p.get("IsSubscriber"),
        "updated_at":       parse_xero_datetime(p.get("UpdatedDateUTC")),
    }


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)
    logger.info("users: %d rows", len(headers))
    return {"headers": len(headers)}
