"""
Xero contacts parser.

Reads from:   dw_1_bronze_xero.xero_contacts
Writes to:    dw_2_staging_xero.contacts              (header)
              dw_2_staging_xero.contact_addresses     (Addresses[])
              dw_2_staging_xero.contact_phones        (Phones[] — non-empty only)

Note: Contact group membership (ContactGroups[]) is handled by contact_groups.py
      which UNIONs both endpoints. Do not duplicate it here.
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE   = "xero_contacts"
HEADER_TABLE   = "contacts"
ADDRESS_TABLE  = "contact_addresses"
PHONE_TABLE    = "contact_phones"


def parse_header(record: dict) -> dict:
    p = record["payload"]
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "contact_id":           p.get("ContactID"),
        "contact_number":       p.get("ContactNumber"),
        "account_number":       p.get("AccountNumber"),
        "contact_name":         p.get("Name"),
        "first_name":           p.get("FirstName"),
        "last_name":            p.get("LastName"),
        "email_address":        p.get("EmailAddress"),
        "website":              p.get("Website"),

        "status":               p.get("ContactStatus"),
        "is_active":            p.get("ContactStatus") == "ACTIVE",
        "is_customer":          p.get("IsCustomer"),
        "is_supplier":          p.get("IsSupplier"),

        "default_currency":     p.get("DefaultCurrency"),
        "default_discount_pct": p.get("Discount"),
        "tax_number":           p.get("TaxNumber"),
        "ar_tax_type":          p.get("AccountsReceivableTaxType"),
        "ap_tax_type":          p.get("AccountsPayableTaxType"),

        "branding_theme_id":    (p.get("BrandingTheme") or {}).get("BrandingThemeID"),

        "ar_outstanding":       (p.get("Balances") or {}).get("AccountsReceivable", {}).get("Outstanding"),
        "ar_overdue":           (p.get("Balances") or {}).get("AccountsReceivable", {}).get("Overdue"),
        "ap_outstanding":       (p.get("Balances") or {}).get("AccountsPayable", {}).get("Outstanding"),
        "ap_overdue":           (p.get("Balances") or {}).get("AccountsPayable", {}).get("Overdue"),

        "has_attachments":      p.get("HasAttachments"),
        "has_validation_errors": p.get("HasValidationErrors"),

        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),

        "address_count":        len(p.get("Addresses") or []),
        "phone_count":          len(p.get("Phones") or []),
        "group_count":          len(p.get("ContactGroups") or []),
    }


def parse_addresses(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for addr in p.get("Addresses") or []:
        result.append({
            "tenant_id":        tenant_id,
            "record_id":        record_id,
            "contact_id":       p.get("ContactID"),
            "address_type":     addr.get("AddressType"),
            "attention_to":     addr.get("AttentionTo"),
            "address_line_1":   addr.get("AddressLine1"),
            "address_line_2":   addr.get("AddressLine2"),
            "address_line_3":   addr.get("AddressLine3"),
            "address_line_4":   addr.get("AddressLine4"),
            "city":             addr.get("City"),
            "region":           addr.get("Region"),
            "postal_code":      addr.get("PostalCode"),
            "country":          addr.get("Country"),
        })
    return result


def parse_phones(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for phone in p.get("Phones") or []:
        number = phone.get("PhoneNumber") or ""
        if not number.strip():
            continue  # skip placeholder entries with no number
        result.append({
            "tenant_id":        tenant_id,
            "record_id":        record_id,
            "contact_id":       p.get("ContactID"),
            "phone_type":       phone.get("PhoneType"),
            "country_code":     phone.get("PhoneCountryCode"),
            "area_code":        phone.get("PhoneAreaCode"),
            "phone_number":     number,
        })
    return result


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers, addresses, phones = [], [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        addresses.extend(parse_addresses(record))
        phones.extend(parse_phones(record))
    writer.merge(HEADER_TABLE, headers)
    if addresses:
        writer.merge(ADDRESS_TABLE, addresses,
                     key_columns=("tenant_id", "record_id", "address_type"))
    if phones:
        writer.merge(PHONE_TABLE, phones,
                     key_columns=("tenant_id", "record_id", "phone_type"))
    logger.info("contacts: %d headers, %d addresses, %d phones",
                len(headers), len(addresses), len(phones))
    return {"headers": len(headers), "addresses": len(addresses), "phones": len(phones)}
