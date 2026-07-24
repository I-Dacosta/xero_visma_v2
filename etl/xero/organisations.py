"""Xero organisations parser. Bronze currently empty."""
import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)
BRONZE_TABLE = "xero_organisations"
HEADER_TABLE = "organisations"

def parse_header(record):
    p = record["payload"]
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],
        "organisation_id": p.get("OrganisationID"),
        "organisation_name": p.get("Name"),
        "legal_name": p.get("LegalName"),
        "organisation_type": p.get("OrganisationType"),
        "status": p.get("OrganisationStatus"),
        "base_currency": p.get("BaseCurrency"),
        "country_code": p.get("CountryCode"),
        "tax_number": p.get("TaxNumber"),
        "registration_number": p.get("RegistrationNumber"),
        "is_demo_company": p.get("IsDemoCompany"),
        "created_at": parse_xero_datetime(p.get("CreatedDateUTC")),
        # Needed to bound Trial Balance's "YTD Debit/Credit" columns, which
        # reset at fiscal year start, not calendar/all-time (verified
        # 2026-07-24 against real GL reconciliation — some accounts matched
        # our all-time balance exactly, others diverged by exactly their
        # pre-current-fiscal-year accumulation).
        "financial_year_end_day":   p.get("FinancialYearEndDay"),
        "financial_year_end_month": p.get("FinancialYearEndMonth"),
    }

def run(reader, writer, tenant_id=None, limit=None):
    headers = [parse_header(r) for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    if headers:
        writer.merge(HEADER_TABLE, headers)
    logger.info("organisations: %d rows", len(headers))
    return {"headers": len(headers)}
