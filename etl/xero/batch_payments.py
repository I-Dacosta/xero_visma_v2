"""
Xero batch payments parser.

Reads from:   dw_1_bronze_xero.xero_batch_payments
Writes to:    dw_2_staging_xero.batch_payments        (header)
              dw_2_staging_xero.batch_payment_lines   (Payments[])
"""

import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime, parse_iso_date

logger = logging.getLogger(__name__)

BRONZE_TABLE = "xero_batch_payments"
HEADER_TABLE = "batch_payments"
LINE_TABLE   = "batch_payment_lines"


def parse_header(record: dict) -> dict:
    p       = record["payload"]
    account = p.get("Account") or {}
    return {
        "tenant_id":            record["tenant_id"],
        "record_id":            record["record_id"],
        "synced_at":            record["last_seen_at"],
        "first_seen_at":        record["first_seen_at"],

        "batch_payment_id":     p.get("BatchPaymentID"),
        "bank_account_id":      account.get("AccountID"),
        "account_currency_code": account.get("CurrencyCode"),

        "batch_payment_type":   p.get("Type"),
        "status":               p.get("Status"),
        "reference":            p.get("Reference"),
        "narrative":            p.get("Narrative"),
        "code":                 p.get("Code"),

        "payment_date":         parse_xero_datetime(p.get("Date")),
        "payment_date_local":   parse_iso_date(p.get("DateString")),
        "updated_at":           parse_xero_datetime(p.get("UpdatedDateUTC")),

        "total_amount":         p.get("TotalAmount"),
        "payment_count":        len(p.get("Payments") or []),
        "is_reconciled":        p.get("IsReconciled"),
        "reconciled_statement_line_id": p.get("ReconciledStatementLineId"),
    }


def parse_lines(record: dict) -> list[dict]:
    p         = record["payload"]
    tenant_id = record["tenant_id"]
    record_id = record["record_id"]
    result    = []
    for pmt in p.get("Payments") or []:
        invoice = pmt.get("Invoice") or {}
        result.append({
            "tenant_id":            tenant_id,
            "record_id":            record_id,
            "batch_payment_id":     p.get("BatchPaymentID"),
            "payment_id":           pmt.get("PaymentID"),
            "invoice_id":           invoice.get("InvoiceID"),
            "invoice_currency_code": invoice.get("CurrencyCode"),
            "invoice_has_errors":   invoice.get("HasErrors"),
            "invoice_is_discounted": invoice.get("IsDiscounted"),
            "amount":               pmt.get("Amount"),
            "bank_amount":          pmt.get("BankAmount"),
        })
    return result


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers, lines = [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        lines.extend(parse_lines(record))
    writer.merge(HEADER_TABLE, headers)
    if lines:
        writer.merge(LINE_TABLE, lines,
                     key_columns=("tenant_id", "record_id", "payment_id"))
    logger.info("batch_payments: %d headers, %d lines", len(headers), len(lines))
    return {"headers": len(headers), "lines": len(lines)}
