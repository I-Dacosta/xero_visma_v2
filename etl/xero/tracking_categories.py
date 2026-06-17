"""
Xero tracking categories parser.

Reads from:   dw_1_bronze_xero.xero_tracking_categories
Writes to:    dw_2_staging_xero.tracking_categories   (header)
              dw_2_staging_xero.tracking_options       (Options[])

Note: record_id = TrackingCategoryID.
      Deleted options are included (is_deleted flag preserved for filtering).
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter

logger = logging.getLogger(__name__)

BRONZE_TABLE  = "xero_tracking_categories"
HEADER_TABLE  = "tracking_categories"
OPTION_TABLE  = "tracking_options"


def parse_header(record: dict) -> dict:
    p = record["payload"]
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "tracking_category_id": p.get("TrackingCategoryID"),
        "category_name":        p.get("Name"),
        "status":               p.get("Status"),
        "is_active":            p.get("Status") == "ACTIVE",
        "option_count":         len(p.get("Options") or []),
    }


def parse_options(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for opt in p.get("Options") or []:
        result.append({
            "tenant_id":            tenant_id,
            "record_id":            record_id,
            "tracking_category_id": p.get("TrackingCategoryID"),
            "category_name":        p.get("Name"),
            "tracking_option_id":   opt.get("TrackingOptionID"),
            "option_name":          opt.get("Name"),
            "status":               opt.get("Status"),
            "is_active":            opt.get("IsActive"),
            "is_archived":          opt.get("IsArchived"),
            "is_deleted":           opt.get("IsDeleted"),
        })
    return result


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers, options = [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        options.extend(parse_options(record))
    writer.merge(HEADER_TABLE, headers)
    if options:
        writer.merge(OPTION_TABLE, options,
                     key_columns=("tenant_id", "record_id", "tracking_option_id"))
    logger.info("tracking_categories: %d headers, %d options", len(headers), len(options))
    return {"headers": len(headers), "options": len(options)}
