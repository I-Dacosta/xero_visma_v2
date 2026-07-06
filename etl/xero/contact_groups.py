"""
Xero contact groups parser.

Reads from:   GCS raw contact_groups files (or dw_1_bronze_xero.xero_contact_groups in dev)
Writes to:    staging_xero.contact_groups          (header)
              staging_xero.contact_group_members   (Contacts[] within this endpoint only)

Pure staging: this parser unpacks ONLY the contact_groups endpoint. The
Contacts[] array inside each group becomes contact_group_members rows.

It does NOT read the contacts endpoint or UNION across sources — that cross-
endpoint reconciliation is an ODS-layer join (see docs/DWH_ARCHITECTURE.md,
"Staging Layer Purity"). The contacts endpoint's own ContactGroups[] array is
unpacked separately by contacts.py into contact_group_memberships.
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter

logger = logging.getLogger(__name__)

BRONZE_TABLE  = "xero_contact_groups"
HEADER_TABLE  = "contact_groups"
MEMBER_TABLE  = "contact_group_members"


def parse_header(record):
    p = record["payload"]
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],
        "contact_group_id": p.get("ContactGroupID"),
        "group_name": p.get("Name"),
        "status": p.get("Status"),
        "is_active": p.get("Status") == "ACTIVE",
        "member_count": len(p.get("Contacts") or []),
    }


def parse_members(record) -> list[dict]:
    """Unpack the Contacts[] array inside a single ContactGroup record."""
    p = record["payload"]
    result = []
    for contact in p.get("Contacts") or []:
        result.append({
            "tenant_id":        record["tenant_id"],
            "contact_group_id": p.get("ContactGroupID"),
            "contact_id":       contact.get("ContactID"),
            "contact_name":     contact.get("Name"),
        })
    return result


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers, members = [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        members.extend(parse_members(record))

    writer.merge(HEADER_TABLE, headers)
    if members:
        writer.merge(MEMBER_TABLE, members,
                     key_columns=("tenant_id", "contact_group_id", "contact_id"))

    logger.info("contact_groups: %d headers, %d members", len(headers), len(members))
    return {"headers": len(headers), "members": len(members)}
