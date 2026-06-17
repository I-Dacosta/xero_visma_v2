"""
Xero branding themes parser.

Reads from:   dw_1_bronze_xero.xero_branding_themes
Writes to:    dw_2_staging_xero.branding_themes                  (header)
              dw_2_staging_xero.branding_theme_payment_services  (PaymentServices[])
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE  = "xero_branding_themes"
HEADER_TABLE  = "branding_themes"
SERVICE_TABLE = "branding_theme_payment_services"


def parse_header(record):
    p = record["payload"]
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],
        "branding_theme_id": p.get("BrandingThemeID"),
        "theme_name": p.get("Name"),
        "theme_type": p.get("Type"),
        "sort_order": p.get("SortOrder"),
        "logo_url": p.get("LogoUrl"),
        "created_at": parse_xero_datetime(p.get("CreatedDateUTC")),
        "payment_service_count": len(p.get("PaymentServices") or []),
    }


def parse_services(record):
    p = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result = []
    for svc in p.get("PaymentServices") or []:
        result.append({
            "tenant_id": tenant_id, "record_id": record_id,
            "branding_theme_id": p.get("BrandingThemeID"),
            "payment_service_id": svc.get("PaymentServiceID"),
            "payment_service_name": svc.get("PaymentServiceName"),
            "payment_service_type": svc.get("PaymentServiceType"),
            "payment_service_url": svc.get("PaymentServiceUrl"),
        })
    return result


def run(reader, writer, tenant_id=None, limit=None):
    headers, services = [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        services.extend(parse_services(record))
    writer.merge(HEADER_TABLE, headers)
    if services:
        writer.merge(SERVICE_TABLE, services,
                     key_columns=("tenant_id", "record_id", "payment_service_id"))
    logger.info("branding_themes: %d headers, %d services", len(headers), len(services))
    return {"headers": len(headers), "services": len(services)}
