use serde::{Deserialize, Serialize};
use std::fmt;

// ── TenantId ──────────────────────────────────────────────────────────────────

/// A Xero organisation (tenant) ID, e.g. `"a5f3b2..."`
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TenantId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TenantId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

// ── EntityType ────────────────────────────────────────────────────────────────

/// Xero resources this service can address.
/// `all()` returns the default Accounting API sync set; payroll-only or logical
/// aliases can remain parseable without being part of full-sync iteration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    // GL & Ledger
    Accounts,
    Journals,
    ManualJournals,
    TaxRates,
    TrackingCategories,

    // AR (Accounts Receivable)
    Contacts,
    Invoices,
    CreditNotes,
    Quotes,
    Payments,
    RepeatingInvoices,

    // AP (Accounts Payable)
    Bills,
    PurchaseOrders,
    Receipts,
    ExpenseClaims,
    BatchPayments,
    LinkedTransactions,

    // Cash & Banking
    BankTransactions,
    BankTransfers,

    // Inventory & Items
    Items,

    // Advanced
    Prepayments,
    Overpayments,
    Employees,

    // Organisation & Configuration
    Currencies,
    Organisations,
    BrandingThemes,
    ContactGroups,
    Budgets,
    PaymentServices,
    Users,
}

impl EntityType {
    /// The API path segment used in Xero REST calls (e.g. `Invoices`).
    pub fn xero_path(&self) -> &'static str {
        match self {
            Self::Accounts => "Accounts",
            Self::BankTransactions => "BankTransactions",
            Self::BankTransfers => "BankTransfers",
            Self::BatchPayments => "BatchPayments",
            Self::Bills => "Bills",
            Self::BrandingThemes => "BrandingThemes",
            Self::Budgets => "Budgets",
            Self::ContactGroups => "ContactGroups",
            Self::Contacts => "Contacts",
            Self::CreditNotes => "CreditNotes",
            Self::Currencies => "Currencies",
            Self::Employees => "Employees",
            Self::ExpenseClaims => "ExpenseClaims",
            Self::Invoices => "Invoices",
            Self::Items => "Items",
            Self::Journals => "Journals",
            Self::LinkedTransactions => "LinkedTransactions",
            Self::ManualJournals => "ManualJournals",
            Self::Organisations => "Organisation",
            Self::Overpayments => "Overpayments",
            Self::Payments => "Payments",
            Self::PaymentServices => "PaymentServices",
            Self::Prepayments => "Prepayments",
            Self::PurchaseOrders => "PurchaseOrders",
            Self::Quotes => "Quotes",
            Self::Receipts => "Receipts",
            Self::RepeatingInvoices => "RepeatingInvoices",
            Self::TaxRates => "TaxRates",
            Self::TrackingCategories => "TrackingCategories",
            Self::Users => "Users",
        }
    }

    /// The snake_case key used in database columns / checkpoints.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accounts => "accounts",
            Self::BankTransactions => "bank_transactions",
            Self::BankTransfers => "bank_transfers",
            Self::BatchPayments => "batch_payments",
            Self::Bills => "bills",
            Self::BrandingThemes => "branding_themes",
            Self::Budgets => "budgets",
            Self::ContactGroups => "contact_groups",
            Self::Contacts => "contacts",
            Self::CreditNotes => "credit_notes",
            Self::Currencies => "currencies",
            Self::Employees => "employees",
            Self::ExpenseClaims => "expense_claims",
            Self::Invoices => "invoices",
            Self::Items => "items",
            Self::Journals => "journals",
            Self::LinkedTransactions => "linked_transactions",
            Self::ManualJournals => "manual_journals",
            Self::Organisations => "organisations",
            Self::Overpayments => "overpayments",
            Self::Payments => "payments",
            Self::PaymentServices => "payment_services",
            Self::Prepayments => "prepayments",
            Self::PurchaseOrders => "purchase_orders",
            Self::Quotes => "quotes",
            Self::Receipts => "receipts",
            Self::RepeatingInvoices => "repeating_invoices",
            Self::TaxRates => "tax_rates",
            Self::TrackingCategories => "tracking_categories",
            Self::Users => "users",
        }
    }

    /// Canonical record identifier field in each Xero payload item.
    pub fn id_field(&self) -> &'static str {
        match self {
            Self::Accounts => "AccountID",
            Self::BankTransactions => "BankTransactionID",
            Self::BankTransfers => "BankTransferID",
            Self::BatchPayments => "BatchPaymentID",
            Self::Bills => "InvoiceID",
            Self::BrandingThemes => "BrandingThemeID",
            Self::Budgets => "BudgetID",
            Self::ContactGroups => "ContactGroupID",
            Self::Contacts => "ContactID",
            Self::CreditNotes => "CreditNoteID",
            Self::Currencies => "CurrencyCode",
            Self::Employees => "EmployeeID",
            Self::ExpenseClaims => "ExpenseClaimID",
            Self::Invoices => "InvoiceID",
            Self::Items => "ItemID",
            Self::Journals => "JournalID",
            Self::LinkedTransactions => "LinkedTransactionID",
            Self::ManualJournals => "ManualJournalID",
            Self::Organisations => "OrganisationID",
            Self::Overpayments => "OverpaymentID",
            Self::Payments => "PaymentID",
            Self::PaymentServices => "PaymentServiceID",
            Self::Prepayments => "PrepaymentID",
            Self::PurchaseOrders => "PurchaseOrderID",
            Self::Quotes => "QuoteID",
            Self::Receipts => "ReceiptID",
            Self::RepeatingInvoices => "RepeatingInvoiceID",
            Self::TaxRates => "TaxType",
            Self::TrackingCategories => "TrackingCategoryID",
            Self::Users => "UserID",
        }
    }

    /// Accounting API entity types used for full-sync iteration.
    pub fn all() -> &'static [EntityType] {
        &[
            Self::Accounts,
            Self::BankTransactions,
            Self::BankTransfers,
            Self::BatchPayments,
            Self::BrandingThemes,
            Self::Budgets,
            Self::ContactGroups,
            Self::Contacts,
            Self::CreditNotes,
            Self::Currencies,
            Self::ExpenseClaims,
            Self::Invoices,
            Self::Items,
            Self::Journals,
            Self::LinkedTransactions,
            Self::ManualJournals,
            Self::Organisations,
            Self::Overpayments,
            Self::Payments,
            Self::PaymentServices,
            Self::Prepayments,
            Self::PurchaseOrders,
            Self::Quotes,
            Self::Receipts,
            Self::RepeatingInvoices,
            Self::TaxRates,
            Self::TrackingCategories,
            Self::Users,
        ]
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for EntityType {
    type Err = crate::Error;

    fn from_str(s: &str) -> crate::Result<Self> {
        match s {
            "accounts" => Ok(Self::Accounts),
            "bank_transactions" => Ok(Self::BankTransactions),
            "bank_transfers" => Ok(Self::BankTransfers),
            "batch_payments" => Ok(Self::BatchPayments),
            "bills" => Ok(Self::Bills),
            "branding_themes" => Ok(Self::BrandingThemes),
            "budgets" => Ok(Self::Budgets),
            "contact_groups" => Ok(Self::ContactGroups),
            "contacts" => Ok(Self::Contacts),
            "credit_notes" => Ok(Self::CreditNotes),
            "currencies" => Ok(Self::Currencies),
            "employees" => Ok(Self::Employees),
            "expense_claims" => Ok(Self::ExpenseClaims),
            "invoices" => Ok(Self::Invoices),
            "items" => Ok(Self::Items),
            "journals" => Ok(Self::Journals),
            "linked_transactions" => Ok(Self::LinkedTransactions),
            "manual_journals" => Ok(Self::ManualJournals),
            "organisations" => Ok(Self::Organisations),
            "overpayments" => Ok(Self::Overpayments),
            "payments" => Ok(Self::Payments),
            "payment_services" => Ok(Self::PaymentServices),
            "prepayments" => Ok(Self::Prepayments),
            "purchase_orders" => Ok(Self::PurchaseOrders),
            "quotes" => Ok(Self::Quotes),
            "receipts" => Ok(Self::Receipts),
            "repeating_invoices" => Ok(Self::RepeatingInvoices),
            "tax_rates" => Ok(Self::TaxRates),
            "tracking_categories" => Ok(Self::TrackingCategories),
            "users" => Ok(Self::Users),
            other => Err(crate::Error::Config(format!("unknown entity: {other}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_round_trips_from_str() {
        let e = "invoices".parse::<EntityType>().unwrap();
        assert_eq!(e, EntityType::Invoices);
        assert_eq!(e.as_str(), "invoices");
        assert_eq!(e.xero_path(), "Invoices");
    }

    #[test]
    fn unknown_entity_returns_error() {
        assert!("banana".parse::<EntityType>().is_err());
    }

    #[test]
    fn entity_all_includes_accounting_api_sync_endpoints() {
        assert_eq!(EntityType::all().len(), 28);
        assert!(!EntityType::all().contains(&EntityType::Bills));
        assert!(!EntityType::all().contains(&EntityType::Employees));
    }

    #[test]
    fn entity_id_field_is_mapped() {
        assert_eq!(EntityType::Invoices.id_field(), "InvoiceID");
        assert_eq!(EntityType::Contacts.id_field(), "ContactID");
        assert_eq!(EntityType::TaxRates.id_field(), "TaxType");
    }
}
