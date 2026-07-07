"""
Xero payment services parser. Bronze/GCS currently empty.

Reads from:   GCS raw payment_services files (or dw_1_bronze_xero.xero_payment_services in dev)
Writes to:    staging_xero.payment_services   (flat — no nested arrays)
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_payment_services"
HEADER_TABLE = "payment_services"


def parse_header(record: dict) -> dict:
    p = record["payload"]
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "payment_service_id":   p.get("PaymentServiceID"),
        "payment_service_name": p.get("PaymentServiceName"),
        "payment_service_url":  p.get("PaymentServiceUrl"),
        "payment_service_type": p.get("PaymentServiceType"),
        "pay_now_text":         p.get("PayNowText"),
    }


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    if headers:
        writer.merge(HEADER_TABLE, headers)
    logger.info("payment_services: %d rows", len(headers))
    return {"headers": len(headers)}
