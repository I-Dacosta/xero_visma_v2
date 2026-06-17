"""
Xero contact groups parser.

Reads from:   dw_1_bronze_xero.xero_contact_groups
Writes to:    dw_2_staging_xero.contact_groups          (header)
              dw_2_staging_xero.contact_group_members   (Contacts[] + xero_contacts ContactGroups[])

Note: contact_group_members UNIONs both endpoints — the group endpoint gives
      Contacts[] per group; contacts.py does NOT write group membership to avoid
      duplication. All membership rows flow through this module.
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


def parse_members_from_groups(reader: BQReader) -> list[dict]:
    """Source A: Contacts[] within each ContactGroup record."""
    result = []
    for record in reader.iter_records(BRONZE_TABLE):
        p = record["payload"]
        for contact in p.get("Contacts") or []:
            result.append({
                "tenant_id":        record["tenant_id"],
                "contact_group_id": p.get("ContactGroupID"),
                "contact_id":       contact.get("ContactID"),
                "contact_name":     contact.get("Name"),
                "contact_group_name": None,
                "_endpoint":        "xero_contact_groups",
            })
    return result


def parse_members_from_contacts(reader: BQReader) -> list[dict]:
    """Source B: ContactGroups[] within each Contact record."""
    result = []
    contacts_reader = BQReader(project=reader.project, dataset=reader.dataset)
    for record in contacts_reader.iter_records("xero_contacts"):
        p = record["payload"]
        for grp in p.get("ContactGroups") or []:
            result.append({
                "tenant_id":        record["tenant_id"],
                "contact_group_id": grp.get("ContactGroupID"),
                "contact_id":       p.get("ContactID"),
                "contact_name":     p.get("Name"),
                "contact_group_name": grp.get("Name"),
                "_endpoint":        "xero_contacts",
            })
    return result


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)

    # UNION both membership sources, deduplicate on (tenant_id, contact_group_id, contact_id)
    from_groups   = parse_members_from_groups(reader)
    from_contacts = parse_members_from_contacts(reader)
    all_members   = from_groups + from_contacts

    # Deduplicate in Python — last writer wins (contacts endpoint has group name)
    seen: dict[tuple, dict] = {}
    for m in all_members:
        key = (m["tenant_id"], m["contact_group_id"], m["contact_id"])
        existing = seen.get(key)
        if existing is None:
            seen[key] = m
        else:
            # Prefer whichever has the group name
            if m.get("contact_group_name") and not existing.get("contact_group_name"):
                seen[key]["contact_group_name"] = m["contact_group_name"]

    members = list(seen.values())
    # Remove _endpoint before writing (internal routing only)
    for m in members:
        m.pop("_endpoint", None)

    if members:
        writer.merge(MEMBER_TABLE, members,
                     key_columns=("tenant_id", "contact_group_id", "contact_id"))

    logger.info("contact_groups: %d headers, %d members", len(headers), len(members))
    return {"headers": len(headers), "members": len(members)}
