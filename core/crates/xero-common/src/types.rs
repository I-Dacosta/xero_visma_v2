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

    // Reports (Accounting API `Reports/*`). Parameterized point-in-time
    // payloads — parseable and individually syncable, but deliberately NOT
    // part of `all()` (the full-sync iteration set). Each is fetched via
    // `fetch_report` and stored as an immutable snapshot keyed by
    // (report, param-signature, run-date). See `all_reports()`.
    ReportProfitAndLoss,
    ReportBalanceSheet,
    ReportTrialBalance,
    ReportAgedReceivablesByContact,
    ReportAgedPayablesByContact,
    ReportBankSummary,
    ReportBudgetSummary,
    ReportExecutiveSummary,
}

impl EntityType {
    /// The API path segment used in Xero REST calls (e.g. `Invoices`).
    pub fn xero_path(&self) -> &'static str {
        match self {
            Self::Accounts => "Accounts",
            Self::BankTransactions => "BankTransactions",
            Self::BankTransfers => "BankTransfers",
            Self::BatchPayments => "BatchPayments",
            // Xero does not expose a standalone /Bills Accounting API path.
            // Bills are purchase invoices, fetched from /Invoices with
            // Type=="ACCPAY" added by xero-client.
            Self::Bills => "Invoices",
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

            // Reports — note the slash. A BQ table id must NOT contain `/`;
            // callers that derive a table name from this MUST sanitize it
            // (see `xero_state::bq_sink` `sanitize_table_segment`).
            Self::ReportProfitAndLoss => "Reports/ProfitAndLoss",
            Self::ReportBalanceSheet => "Reports/BalanceSheet",
            Self::ReportTrialBalance => "Reports/TrialBalance",
            Self::ReportAgedReceivablesByContact => "Reports/AgedReceivablesByContact",
            Self::ReportAgedPayablesByContact => "Reports/AgedPayablesByContact",
            Self::ReportBankSummary => "Reports/BankSummary",
            Self::ReportBudgetSummary => "Reports/BudgetSummary",
            Self::ReportExecutiveSummary => "Reports/ExecutiveSummary",
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

            // Reports
            Self::ReportProfitAndLoss => "report_profit_and_loss",
            Self::ReportBalanceSheet => "report_balance_sheet",
            Self::ReportTrialBalance => "report_trial_balance",
            Self::ReportAgedReceivablesByContact => "report_aged_receivables_by_contact",
            Self::ReportAgedPayablesByContact => "report_aged_payables_by_contact",
            Self::ReportBankSummary => "report_bank_summary",
            Self::ReportBudgetSummary => "report_budget_summary",
            Self::ReportExecutiveSummary => "report_executive_summary",
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

            // Reports have no natural per-record id. This is a placeholder;
            // `record_id_for_entity` special-cases reports and synthesizes a
            // stable id from (report, param-signature, run-date) instead of
            // reading this field. `ReportID` is present on Xero report
            // payloads but is NOT stable across runs, so it is never used.
            Self::ReportProfitAndLoss
            | Self::ReportBalanceSheet
            | Self::ReportTrialBalance
            | Self::ReportAgedReceivablesByContact
            | Self::ReportAgedPayablesByContact
            | Self::ReportBankSummary
            | Self::ReportBudgetSummary
            | Self::ReportExecutiveSummary => "ReportID",
        }
    }

    /// `true` if this entity is a Xero `Reports/*` resource — a parameterized
    /// point-in-time snapshot rather than an incrementally-synced list. Used
    /// to route fetch dispatch (→ `fetch_report`), synthesize a stable
    /// `record_id`, sanitize the BQ table id, and bypass the watermark.
    pub fn is_report(&self) -> bool {
        matches!(
            self,
            Self::ReportProfitAndLoss
                | Self::ReportBalanceSheet
                | Self::ReportTrialBalance
                | Self::ReportAgedReceivablesByContact
                | Self::ReportAgedPayablesByContact
                | Self::ReportBankSummary
                | Self::ReportBudgetSummary
                | Self::ReportExecutiveSummary
        )
    }

    /// The report entity types. Deliberately SEPARATE from [`Self::all`]:
    /// reports are invoked explicitly (monthly close / daily aged), never as
    /// part of the default full-sync iteration, so `all().len()` stays 28.
    pub fn all_reports() -> &'static [EntityType] {
        &[
            Self::ReportProfitAndLoss,
            Self::ReportBalanceSheet,
            Self::ReportTrialBalance,
            Self::ReportAgedReceivablesByContact,
            Self::ReportAgedPayablesByContact,
            Self::ReportBankSummary,
            Self::ReportBudgetSummary,
            Self::ReportExecutiveSummary,
        ]
    }

    /// Report entities synced by default. Excludes the per-contact aged reports
    /// (`AgedReceivablesByContact` / `AgedPayablesByContact`), which Xero only
    /// serves per `contactId`; aging is instead derived downstream from raw
    /// invoices/credit-notes/payments. Callers can still request them explicitly
    /// with a supplied `contactId`.
    pub fn reports_default() -> &'static [EntityType] {
        &[
            Self::ReportProfitAndLoss,
            Self::ReportBalanceSheet,
            Self::ReportTrialBalance,
            Self::ReportBankSummary,
            Self::ReportBudgetSummary,
            Self::ReportExecutiveSummary,
        ]
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

    /// `true` if this entity is part of the master-data set — slowly-changing
    /// reference/configuration entities that are re-synced wide (full) rather
    /// than incrementally. See [`Self::master_data`] for the exact set.
    pub fn is_master(&self) -> bool {
        Self::master_data().contains(self)
    }

    /// Master-data entity types: slowly-changing reference/configuration data
    /// that is periodically re-synced in full. Deliberately EXCLUDES
    /// `Employees` (payroll, not part of the master Accounting set).
    pub fn master_data() -> &'static [EntityType] {
        &[
            Self::Accounts,
            Self::Contacts,
            Self::Items,
            Self::TaxRates,
            Self::TrackingCategories,
            Self::Currencies,
            Self::Organisations,
            Self::Users,
            Self::BrandingThemes,
        ]
    }

    /// Entity types whose records carry an open/closed lifecycle status, so a
    /// rolling re-sync must revisit recently-touched records to catch status
    /// transitions (e.g. an invoice moving to PAID). Currently
    /// `Invoices` and `Bills`.
    pub fn open_status() -> &'static [EntityType] {
        &[Self::Invoices, Self::Bills]
    }

    /// `true` if this entity supports a Xero `where=Date>=…` business-date
    /// window, so it can be backfilled / rolling-full'd in date chunks.
    ///
    /// Everything else — master/reference data (no `Date` field) and the
    /// offset/no-`where` endpoints (e.g. Journals) — must be pulled in full
    /// (no date filter); applying a `Date` filter to them errors or is ignored.
    pub fn is_date_windowable(&self) -> bool {
        matches!(
            self,
            Self::Invoices
                | Self::Bills
                | Self::CreditNotes
                | Self::Quotes
                | Self::Payments
                | Self::Overpayments
                | Self::Prepayments
                | Self::BankTransactions
                | Self::BankTransfers
                | Self::ManualJournals
                | Self::PurchaseOrders
                | Self::Receipts
                | Self::BatchPayments
        )
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
            "report_profit_and_loss" => Ok(Self::ReportProfitAndLoss),
            "report_balance_sheet" => Ok(Self::ReportBalanceSheet),
            "report_trial_balance" => Ok(Self::ReportTrialBalance),
            "report_aged_receivables_by_contact" => Ok(Self::ReportAgedReceivablesByContact),
            "report_aged_payables_by_contact" => Ok(Self::ReportAgedPayablesByContact),
            "report_bank_summary" => Ok(Self::ReportBankSummary),
            "report_budget_summary" => Ok(Self::ReportBudgetSummary),
            "report_executive_summary" => Ok(Self::ReportExecutiveSummary),
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
    fn bills_alias_uses_invoices_api_path() {
        let e = "bills".parse::<EntityType>().unwrap();
        assert_eq!(e, EntityType::Bills);
        assert_eq!(e.as_str(), "bills");
        assert_eq!(e.xero_path(), "Invoices");
        assert_eq!(e.id_field(), "InvoiceID");
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

    // ---- Reports (WS3) ----

    #[test]
    fn report_variants_round_trip_from_str() {
        for (s, want, path) in [
            (
                "report_profit_and_loss",
                EntityType::ReportProfitAndLoss,
                "Reports/ProfitAndLoss",
            ),
            (
                "report_balance_sheet",
                EntityType::ReportBalanceSheet,
                "Reports/BalanceSheet",
            ),
            (
                "report_trial_balance",
                EntityType::ReportTrialBalance,
                "Reports/TrialBalance",
            ),
            (
                "report_aged_receivables_by_contact",
                EntityType::ReportAgedReceivablesByContact,
                "Reports/AgedReceivablesByContact",
            ),
            (
                "report_aged_payables_by_contact",
                EntityType::ReportAgedPayablesByContact,
                "Reports/AgedPayablesByContact",
            ),
            (
                "report_bank_summary",
                EntityType::ReportBankSummary,
                "Reports/BankSummary",
            ),
            (
                "report_budget_summary",
                EntityType::ReportBudgetSummary,
                "Reports/BudgetSummary",
            ),
            (
                "report_executive_summary",
                EntityType::ReportExecutiveSummary,
                "Reports/ExecutiveSummary",
            ),
        ] {
            let parsed = s.parse::<EntityType>().unwrap();
            assert_eq!(parsed, want, "parse {s}");
            assert_eq!(parsed.as_str(), s, "as_str round-trip for {s}");
            assert_eq!(parsed.xero_path(), path, "xero_path for {s}");
            assert!(parsed.is_report(), "{s} must be a report");
        }
    }

    #[test]
    fn report_variants_are_not_in_all_but_are_in_all_reports() {
        // The 28-entity full-sync invariant MUST be preserved: reports are
        // explicit-invoke only.
        assert_eq!(EntityType::all().len(), 28);
        for r in EntityType::all_reports() {
            assert!(
                !EntityType::all().contains(r),
                "{} must not be in all()",
                r.as_str()
            );
            assert!(r.is_report(), "{} must report is_report()=true", r.as_str());
        }
        assert_eq!(EntityType::all_reports().len(), 8);
    }

    #[test]
    fn non_report_entities_are_not_reports() {
        assert!(!EntityType::Invoices.is_report());
        assert!(!EntityType::ContactGroups.is_report());
        assert!(!EntityType::Budgets.is_report());
    }

    // ---- Master data (raw-GCS) ----

    #[test]
    fn master_data_has_exactly_nine_members() {
        assert_eq!(EntityType::master_data().len(), 9);
    }

    #[test]
    fn master_data_contains_expected_set() {
        let expected = [
            EntityType::Accounts,
            EntityType::Contacts,
            EntityType::Items,
            EntityType::TaxRates,
            EntityType::TrackingCategories,
            EntityType::Currencies,
            EntityType::Organisations,
            EntityType::Users,
            EntityType::BrandingThemes,
        ];
        for entity in expected {
            assert!(
                EntityType::master_data().contains(&entity),
                "{} must be master data",
                entity.as_str()
            );
            assert!(
                entity.is_master(),
                "{} must report is_master()",
                entity.as_str()
            );
        }
    }

    #[test]
    fn master_data_excludes_employees() {
        assert!(!EntityType::master_data().contains(&EntityType::Employees));
        assert!(!EntityType::Employees.is_master());
    }

    #[test]
    fn non_master_entities_are_not_master() {
        assert!(!EntityType::Invoices.is_master());
        assert!(!EntityType::Bills.is_master());
        assert!(!EntityType::Journals.is_master());
        assert!(!EntityType::ReportBalanceSheet.is_master());
    }

    // ---- Open-status entities (raw-GCS) ----

    #[test]
    fn open_status_is_invoices_and_bills() {
        assert_eq!(EntityType::open_status().len(), 2);
        assert!(EntityType::open_status().contains(&EntityType::Invoices));
        assert!(EntityType::open_status().contains(&EntityType::Bills));
    }

    #[test]
    fn open_status_excludes_unrelated_entities() {
        assert!(!EntityType::open_status().contains(&EntityType::Accounts));
        assert!(!EntityType::open_status().contains(&EntityType::Payments));
    }
}
