"""
Endpoint configuration — mappings needed to extract records from GCS data files.

ARRAY_KEYS
    Maps the lowercase endpoint name (from the GCS path and meta file x-endpoint)
    to the PascalCase key used in the Xero API response body where the records array lives.
    e.g. "accounts" -> "Accounts", "bank_transactions" -> "BankTransactions"

RECORD_ID_FIELDS
    Maps the lowercase endpoint name to the field name inside each record that
    serves as the unique record identifier. This becomes record["record_id"] in
    the envelope passed to every parser, and is used as part of the MERGE key.
"""

ARRAY_KEYS: dict[str, str] = {
    "accounts":             "Accounts",
    "bank_transactions":    "BankTransactions",
    "bank_transfers":       "BankTransfers",
    "batch_payments":       "BatchPayments",
    "branding_themes":      "BrandingThemes",
    "budgets":              "Budgets",
    "contact_groups":       "ContactGroups",
    "contacts":             "Contacts",
    "credit_notes":         "CreditNotes",
    "currencies":           "Currencies",
    "expense_claims":       "ExpenseClaims",
    "invoices":             "Invoices",
    "items":                "Items",
    "journals":             "Journals",
    "linked_transactions":  "LinkedTransactions",
    "manual_journals":      "ManualJournals",
    "organisations":        "Organisations",
    "overpayments":         "Overpayments",
    "payment_services":     "PaymentServices",
    "payments":             "Payments",
    "prepayments":          "Prepayments",
    "purchase_orders":      "PurchaseOrders",
    "quotes":               "Quotes",
    "receipts":             "Receipts",
    "repeating_invoices":   "RepeatingInvoices",
    "tax_rates":            "TaxRates",
    "tracking_categories":  "TrackingCategories",
    "users":                "Users",
}

RECORD_ID_FIELDS: dict[str, str] = {
    "accounts":             "AccountID",
    "bank_transactions":    "BankTransactionID",
    "bank_transfers":       "BankTransferID",
    "batch_payments":       "BatchPaymentID",
    "branding_themes":      "BrandingThemeID",
    "budgets":              "BudgetID",
    "contact_groups":       "ContactGroupID",
    "contacts":             "ContactID",
    "credit_notes":         "CreditNoteID",
    "currencies":           "Code",           # currencies use Code as the natural key
    "expense_claims":       "ExpenseClaimID",
    "invoices":             "InvoiceID",
    "items":                "ItemID",
    "journals":             "JournalID",
    "linked_transactions":  "LinkedTransactionID",
    "manual_journals":      "ManualJournalID",
    "organisations":        "OrganisationID",
    "overpayments":         "OverpaymentID",
    "payment_services":     "PaymentServiceID",
    "payments":             "PaymentID",
    "prepayments":          "PrepaymentID",
    "purchase_orders":      "PurchaseOrderID",
    "quotes":               "QuoteID",
    "receipts":             "ReceiptID",
    "repeating_invoices":   "RepeatingInvoiceID",
    "tax_rates":            "TaxType",        # tax_rates use TaxType as the natural key
    "tracking_categories":  "TrackingCategoryID",
    "users":                "UserID",
}
