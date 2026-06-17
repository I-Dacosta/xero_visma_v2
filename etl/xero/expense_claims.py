"""
Xero expense claims parser. Bronze currently empty.

Writes to:    dw_2_staging_xero.expense_claims              (header)
              dw_2_staging_xero.expense_claim_receipts      (Receipts[])
              dw_2_staging_xero.expense_claim_receipt_lines (Receipts[] × LineItems[])
"""
import logging
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_xero_datetime

logger = logging.getLogger(__name__)
BRONZE_TABLE  = "xero_expense_claims"
HEADER_TABLE  = "expense_claims"
RECEIPT_TABLE = "expense_claim_receipts"
LINE_TABLE    = "expense_claim_receipt_lines"

def _t(items, i, f): return items[i].get(f) if i < len(items) else None

def parse_header(record):
    p = record["payload"]
    user = p.get("User") or {}
    return {
        "tenant_id": record["tenant_id"], "record_id": record["record_id"],
        "synced_at": record["last_seen_at"], "first_seen_at": record["first_seen_at"],
        "expense_claim_id": p.get("ExpenseClaimID"),
        "user_id": user.get("UserID"), "user_first_name": user.get("FirstName"),
        "user_last_name": user.get("LastName"), "user_email": user.get("EmailAddress"),
        "user_organisation_role": user.get("OrganisationRole"),
        "status": p.get("Status"),
        "reporting_date": parse_xero_datetime(p.get("ReportingDate")),
        "payment_due_date": parse_xero_datetime(p.get("PaymentDueDate")),
        "updated_at": parse_xero_datetime(p.get("UpdatedDateUTC")),
        "total": p.get("Total"), "amount_due": p.get("AmountDue"), "amount_paid": p.get("AmountPaid"),
        "receipt_count": len(p.get("Receipts") or []),
        "payment_count": len(p.get("Payments") or []),
    }

def parse_receipts(record):
    p = record["payload"]
    result = []
    for receipt in p.get("Receipts") or []:
        contact = receipt.get("Contact") or {}
        user = receipt.get("User") or {}
        result.append({
            "tenant_id": record["tenant_id"], "record_id": record["record_id"],
            "expense_claim_id": p.get("ExpenseClaimID"),
            "receipt_id": receipt.get("ReceiptID"),
            "contact_id": contact.get("ContactID"), "contact_name": contact.get("Name"),
            "user_id": user.get("UserID"), "user_email": user.get("EmailAddress"),
            "status": receipt.get("Status"), "reference": receipt.get("Reference"),
            "line_amount_types": receipt.get("LineAmountTypes"), "url": receipt.get("Url"),
            "receipt_date": parse_xero_datetime(receipt.get("Date")),
            "sub_total": receipt.get("SubTotal"), "total_tax": receipt.get("TotalTax"),
            "total": receipt.get("Total"), "has_attachments": receipt.get("HasAttachments"),
            "line_item_count": len(receipt.get("LineItems") or []),
        })
    return result

def parse_lines(record):
    p = record["payload"]
    result = []
    for receipt in p.get("Receipts") or []:
        receipt_id = receipt.get("ReceiptID")
        for line in receipt.get("LineItems") or []:
            tracking = line.get("Tracking") or []
            result.append({
                "tenant_id": record["tenant_id"], "record_id": record["record_id"],
                "expense_claim_id": p.get("ExpenseClaimID"), "receipt_id": receipt_id,
                "line_item_id": line.get("LineItemID"),
                "account_id": line.get("AccountID"), "account_code": line.get("AccountCode"),
                "description": line.get("Description"), "quantity": line.get("Quantity"),
                "unit_amount": line.get("UnitAmount"), "line_amount": line.get("LineAmount"),
                "tax_amount": line.get("TaxAmount"), "tax_type": line.get("TaxType"),
                "tracking_category_1_id": _t(tracking, 0, "TrackingCategoryID"),
                "tracking_category_1_name": _t(tracking, 0, "Name"),
                "tracking_option_1_id": _t(tracking, 0, "TrackingOptionID"),
                "tracking_option_1_name": _t(tracking, 0, "Option"),
                "tracking_category_2_id": _t(tracking, 1, "TrackingCategoryID"),
                "tracking_category_2_name": _t(tracking, 1, "Name"),
                "tracking_option_2_id": _t(tracking, 1, "TrackingOptionID"),
                "tracking_option_2_name": _t(tracking, 1, "Option"),
            })
    return result

def run(reader, writer, tenant_id=None, limit=None):
    headers, receipts, lines = [], [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        receipts.extend(parse_receipts(record))
        lines.extend(parse_lines(record))
    if headers:
        writer.merge(HEADER_TABLE, headers)
    if receipts:
        writer.merge(RECEIPT_TABLE, receipts, key_columns=("tenant_id", "record_id", "receipt_id"))
    if lines:
        writer.merge(LINE_TABLE, lines, key_columns=("tenant_id", "record_id", "receipt_id", "line_item_id"))
    logger.info("expense_claims: %d headers, %d receipts, %d lines", len(headers), len(receipts), len(lines))
    return {"headers": len(headers), "receipts": len(receipts), "lines": len(lines)}
