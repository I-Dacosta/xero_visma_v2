"""
Xero payments parser.

Reads from:   dw_1_bronze_xero.xero_payments
Writes to:    dw_2_staging_xero.payments   (flat — no nested arrays)

Flattens: Account{}, Invoice{}, Invoice.Contact{}, BatchPayment{}
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_payments"
HEADER_TABLE = "payments"


def parse_header(record: dict) -> dict:
    p            = record["payload"]
    account      = p.get("Account") or {}
    invoice      = p.get("Invoice") or {}
    inv_contact  = invoice.get("Contact") or {}
    batch        = p.get("BatchPayment") or {}
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "payment_id":           p.get("PaymentID"),
        "payment_type":         p.get("PaymentType"),
        "status":               p.get("Status"),
        "reference":            p.get("Reference"),

        "account_id":           account.get("AccountID"),
        "account_code":         account.get("Code"),

        "invoice_id":           invoice.get("InvoiceID"),
        "invoice_number":       invoice.get("InvoiceNumber"),
        "invoice_type":         invoice.get("Type"),
        "contact_id":           inv_contact.get("ContactID"),
        "contact_name":         inv_contact.get("Name"),

        "batch_payment_id":     p.get("BatchPaymentID") or batch.get("BatchPaymentID"),

        "payment_date":         parse_xero_datetime(p.get("Date")),
        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),

        "amount":               p.get("Amount"),
        "bank_amount":          p.get("BankAmount"),
        "currency_rate":        p.get("CurrencyRate"),

        "is_reconciled":        p.get("IsReconciled"),
        "has_account":          p.get("HasAccount"),
        "has_validation_errors": p.get("HasValidationErrors"),
    }


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers = [parse_header(r)
               for r in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit)]
    writer.merge(HEADER_TABLE, headers)
    logger.info("payments: %d rows", len(headers))
    return {"headers": len(headers)}
