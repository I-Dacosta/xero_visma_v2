"""
Xero bank transfers parser.

Reads from:   dw_1_bronze_xero.xero_bank_transfers
Writes to:    dw_2_staging_xero.bank_transfers   (flat — no nested arrays)

Restored 2026-07-24 (removed 2026-07-07 under the bucket-driven parser
policy) after tracing `journals.source_type = 'TRANSFER'` postings back to
this endpoint — see docs/DWH_ARCHITECTURE.md. Verified against a live
non-empty payload before restoring: every original field is still present.
Added reference/status/*_is_reconciled, which the 2026-06 version didn't
capture (staging purity — capture every available scalar).
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_bank_transfers"
HEADER_TABLE = "bank_transfers"


def parse_header(record: dict) -> dict:
    p        = record["payload"]
    from_acc = p.get("FromBankAccount") or {}
    to_acc   = p.get("ToBankAccount") or {}
    return {
        "tenant_id":                record["tenant_id"],
        "record_id":                record["record_id"],
        "synced_at":                record["last_seen_at"],
        "first_seen_at":            record["first_seen_at"],

        "bank_transfer_id":         p.get("BankTransferID"),
        "from_bank_account_id":     from_acc.get("AccountID"),
        "from_bank_account_code":   from_acc.get("Code"),
        "from_bank_account_name":   from_acc.get("Name"),
        "to_bank_account_id":       to_acc.get("AccountID"),
        "to_bank_account_code":     to_acc.get("Code"),
        "to_bank_account_name":     to_acc.get("Name"),
        "from_bank_transaction_id": p.get("FromBankTransactionID"),
        "to_bank_transaction_id":   p.get("ToBankTransactionID"),
        "from_is_reconciled":       p.get("FromIsReconciled"),
        "to_is_reconciled":         p.get("ToIsReconciled"),

        # Reference is free text but some orgs enter purely numeric values —
        # Xero's own JSON serializes those unquoted, so the raw value can be
        # a Python str or a number depending on tenant. Force STRING so the
        # column type doesn't depend on which tenant's batch creates the
        # table first (hit this exact race restoring this parser 2026-07-24).
        "reference":                str(p["Reference"]) if p.get("Reference") is not None else None,
        "status":                   p.get("Status"),

        "transfer_date":            parse_xero_datetime(p.get("Date")),
        "created_at":               parse_xero_datetime(p.get("CreatedDateUTC")),
        "updated_at":               parse_xero_datetime(p.get("UpdatedDateUTC")),

        "amount":                   p.get("Amount"),
        "currency_rate":            p.get("CurrencyRate"),
        "has_attachments":          p.get("HasAttachments"),
    }


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)
    logger.info("bank_transfers: %d rows", len(headers))
    return {"headers": len(headers)}
