# Xero Silver Layer — Build Notes

_Last updated: 2026-06-03. Silver layer complete — all 46 tables built and materialised._

---

## Overview

This document covers the design, implementation, issues encountered, and forward considerations for the `dw_1_silver_xero` Dataform layer. It is the counterpart to the existing Visma silver layer (`dw_1_silver_visma`) and mirrors its folder structure and conventions.

All files live under:
```
Dataform/definitions/silver/xero/
  dimentinals/   ← dimension tables
  facts/         ← fact tables
```

> Note: the folder name `dimentinals` matches the existing Visma silver spelling intentionally, for consistency.

---

## Bronze Layer — How It Actually Works

The bronze Xero tables in BigQuery do **not** use the field-level schema shown in the `.sqlx` declaration files. The declarations are documentation artefacts only.

The actual BQ table schema for every Xero entity is a generic envelope:

| Column | Type | Description |
|---|---|---|
| `tenant_id` | STRING | Xero organisation/tenant GUID |
| `record_id` | STRING | Entity primary key (e.g. AccountID) |
| `payload` | STRING | Full Xero API JSON response, serialised as a string |
| `first_seen_at` | TIMESTAMP | First time this record was synced |
| `last_seen_at` | TIMESTAMP | Most recent sync timestamp |
| `last_run_id` | STRING | UUID of the sync run that last wrote this row |
| `synced_at` | TIMESTAMP | BQ write timestamp |

Source: `core/crates/xero-state/src/bq_sink.rs`.

**BQ table naming convention:** `xero_` + snake_case entity name.  
Examples: `xero_accounts`, `xero_bank_transactions`, `xero_credit_notes`.

### Bronze tables with no data (as of 2026-06-03)

The following bronze tables exist in `dw_1_bronze_xero` but are **empty** — confirmed by direct BQ query. Silver tables have been built for all of them and will auto-populate once the sync starts writing data.

| Bronze table | Silver table(s) | Likely reason empty |
|---|---|---|
| `xero_currencies` | `dim_currency` | Sync endpoint not yet enabled |
| `xero_expense_claims` | `fact_expense_claim`, `fact_expense_claim_receipt`, `fact_expense_claim_receipt_line` | Sync endpoint not yet enabled |
| `xero_organisations` | `dim_organisation` | Sync endpoint not yet enabled |
| `xero_overpayments` | `fact_overpayment` | No overpayments in this org, or endpoint not enabled |
| `xero_payment_services` | `dim_payment_service` | No payment services configured, or endpoint not enabled |
| `xero_prepayments` | `fact_prepayment` | No prepayments in this org, or endpoint not enabled |
| `xero_receipts` | `fact_receipt` | Sync endpoint not yet enabled |

**Additionally:** `xero_budgets` has header rows but `BudgetLines[]` is empty in every payload — the sync must call `GET /Budgets/{BudgetID}?BudgetLines=true` per budget. See Issues section.

---

## Established SQL Patterns

These patterns are used consistently across all silver Xero tables.

### Deduplication (latest record per entity)
```sql
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY tenant_id, record_id
  ORDER BY last_seen_at DESC
) = 1
```

### Xero date parsing — standard format (with timezone offset)
Most Xero dates are `/Date(milliseconds+0000)/`. Two fields are usually present — the raw timestamp and an ISO local date string:
```sql
-- UTC timestamp
TIMESTAMP_MILLIS(
  SAFE_CAST(
    REGEXP_EXTRACT(JSON_VALUE(payload, '$.SomeDate'),
      r'/Date\((\d+)[+-]\d{4}\)/') AS INT64)
) AS some_date,

-- Local calendar date from the DateString companion field
DATE(PARSE_DATETIME('%Y-%m-%dT%H:%M:%S',
  JSON_VALUE(payload, '$.SomeDateString'))) AS some_date_local
```

### ⚠️ Xero date parsing — quotes format (NO timezone offset)
`xero_quotes` dates omit the timezone: `/Date(1774483200000)/` — the standard regex above will NOT match and returns NULL silently. Use the permissive regex for quotes (and defensively for any new entity):
```sql
TIMESTAMP_MILLIS(
  SAFE_CAST(
    REGEXP_EXTRACT(JSON_VALUE(payload, '$.Date'),
      r'/Date\((\d+)(?:[+-]\d{4})?\)/') AS INT64)
) AS some_date
```

### ⚠️ Xero date parsing — bare date strings (no time component)
`xero_repeating_invoices` `Schedule.NextScheduledDateString` is a bare `YYYY-MM-DD` string (e.g. `"2026-06-04"`), not the `T00:00:00` format. `PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', ...)` will throw. Use:
```sql
SAFE_CAST(JSON_VALUE(payload, '$.Schedule.NextScheduledDateString') AS DATE) AS next_scheduled_date_local
```

Always use `SAFE_CAST` so malformed or absent values produce `NULL` rather than errors.

### Single-level array unnesting
```sql
CROSS JOIN UNNEST(JSON_QUERY_ARRAY(r.payload, '$.Items')) AS item
```

### Double-level array unnesting (e.g. BudgetLines × BudgetBalances, Receipts × LineItems)
```sql
-- Step 1
CROSS JOIN UNNEST(JSON_QUERY_ARRAY(r.payload, '$.OuterArray')) AS outer_item
-- Step 2 (in a subsequent CTE)
CROSS JOIN UNNEST(JSON_QUERY_ARRAY(outer_item, '$.InnerArray')) AS inner_item
```

### Tracking categories — most entities use `Tracking`
```sql
JSON_VALUE(line, '$.Tracking[0].TrackingCategoryID') AS tracking_category_1_id,
JSON_VALUE(line, '$.Tracking[0].Name')               AS tracking_category_1_name,
JSON_VALUE(line, '$.Tracking[0].TrackingOptionID')   AS tracking_option_1_id,
JSON_VALUE(line, '$.Tracking[0].Option')             AS tracking_option_1_name,
JSON_VALUE(line, '$.Tracking[1].TrackingCategoryID') AS tracking_category_2_id,
JSON_VALUE(line, '$.Tracking[1].Name')               AS tracking_category_2_name,
JSON_VALUE(line, '$.Tracking[1].TrackingOptionID')   AS tracking_option_2_id,
JSON_VALUE(line, '$.Tracking[1].Option')             AS tracking_option_2_name,
```

### ⚠️ Tracking categories — journals use `TrackingCategories` (different key name)
`xero_journals` `JournalLines` use `TrackingCategories` not `Tracking`. Manual journals use `Tracking` (standard). This is a Xero API inconsistency:
```sql
-- System journals (xero_journals) only:
JSON_VALUE(line, '$.TrackingCategories[0].TrackingCategoryID') AS tracking_category_1_id,
JSON_VALUE(line, '$.TrackingCategories[0].Name')               AS tracking_category_1_name,
JSON_VALUE(line, '$.TrackingCategories[0].Option')             AS tracking_option_1_name,
```

### Summary counts on parent tables
```sql
ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.LineItems')) AS line_item_count
```
Avoids joining the child table for simple row-count checks; also confirms nested data was actually synced.

### NULL in UNION ALL — always cast explicitly
BigQuery infers bare `NULL` as `INT64`. In any UNION ALL where one branch has `NULL` for a STRING column:
```sql
CAST(NULL AS STRING) AS column_name  -- not just NULL
```

---

## Complete Table Inventory

All 46 tables built and materialised as of 2026-06-03.

### Dimensions (`dimentinals/`)

| Table | Source bronze table | Notes |
|---|---|---|
| `dim_account` | `xero_accounts` | Pre-derives `bs_pl` and `fsli_1` with same labels as Visma for gold UNION |
| `dim_branding_theme` | `xero_branding_themes` | |
| `dim_branding_theme_payment_service` | `xero_branding_themes` | Unnests `PaymentServices[]` |
| `dim_contact_group` | `xero_contact_groups` | |
| `dim_contact_group_member` | `xero_contact_groups` + `xero_contacts` | UNIONs both endpoints — see design notes |
| `dim_contact` | `xero_contacts` | Includes AR/AP balance snapshots (point-in-time at last sync) |
| `dim_contact_address` | `xero_contacts` | Unnests `Addresses[]`; one row per contact + address type |
| `dim_contact_phone` | `xero_contacts` | Unnests `Phones[]`; filters empty numbers |
| `dim_currency` | `xero_currencies` | ⚠️ Bronze empty |
| `dim_item` | `xero_items` | Flattens `PurchaseDetails{}` and `SalesDetails{}` inline |
| `dim_organisation` | `xero_organisations` | ⚠️ Bronze empty |
| `dim_payment_service` | `xero_payment_services` | ⚠️ Bronze empty |
| `dim_repeating_invoice` | `xero_repeating_invoices` | Templates only; `Schedule{}` flattened inline |
| `dim_repeating_invoice_line` | `xero_repeating_invoices` | Unnests `LineItems[]` |
| `dim_tax_rate` | `xero_tax_rates` | `TaxComponents[0]` flattened inline (always 1 component) |
| `dim_tracking_category` | `xero_tracking_categories` | |
| `dim_tracking_option` | `xero_tracking_categories` | Unnests `Options[]`; filters deleted options |
| `dim_user` | `xero_users` | Flat — no nested arrays |

### Facts (`facts/`)

| Table | Source bronze table | Grain | Notes |
|---|---|---|---|
| `fact_bank_transaction` | `xero_bank_transactions` | 1 row per transaction | |
| `fact_bank_transfer` | `xero_bank_transfers` | 1 row per transfer | |
| `fact_batch_payment` | `xero_batch_payments` | 1 row per batch | |
| `fact_batch_payment_line` | `xero_batch_payments` | 1 row per batch + payment | Unnests `Payments[]` |
| `fact_budget` | `xero_budgets` | 1 row per budget | |
| `fact_budget_line` | `xero_budgets` | 1 row per budget + account + period | ⚠️ Empty — needs API fix (see Issues) |
| `fact_credit_note` | `xero_credit_notes` | 1 row per credit note | |
| `fact_credit_note_allocation` | `xero_credit_notes` | 1 row per credit note + allocation | Unnests `Allocations[]` |
| `fact_credit_note_line` | `xero_credit_notes` | 1 row per credit note + line | Unnests `LineItems[]` |
| `fact_expense_claim` | `xero_expense_claims` | 1 row per claim | ⚠️ Bronze empty |
| `fact_expense_claim_receipt` | `xero_expense_claims` | 1 row per claim + receipt | ⚠️ Bronze empty |
| `fact_expense_claim_receipt_line` | `xero_expense_claims` | 1 row per claim + receipt + line | ⚠️ Bronze empty |
| `fact_invoice` | `xero_invoices` | 1 row per invoice | |
| `fact_invoice_line` | `xero_invoices` | 1 row per invoice + line | Unnests `LineItems[]` |
| `fact_invoice_payment` | `xero_invoices` | 1 row per invoice + payment | Unnests `Payments[]` |
| `fact_journal` | `xero_journals` | 1 row per journal | |
| `fact_journal_line` | `xero_journals` | 1 row per journal + line | ⚠️ Uses `TrackingCategories[]` not `Tracking[]` |
| `fact_linked_transaction` | `xero_linked_transactions` | 1 row per linked transaction | Flat — no nested arrays |
| `fact_manual_journal` | `xero_manual_journals` | 1 row per manual journal | |
| `fact_manual_journal_line` | `xero_manual_journals` | 1 row per journal + account | No `LineItemID`; keyed by account; filters `IsBlank=true` |
| `fact_overpayment` | `xero_overpayments` | 1 row per overpayment | ⚠️ Bronze empty |
| `fact_payment` | `xero_payments` | 1 row per payment | `Invoice.Contact.ContactID` is double-nested |
| `fact_prepayment` | `xero_prepayments` | 1 row per prepayment | ⚠️ Bronze empty |
| `fact_purchase_order` | `xero_purchase_orders` | 1 row per PO | |
| `fact_purchase_order_line` | `xero_purchase_orders` | 1 row per PO + line | PO lines may lack `AccountID` (only `AccountCode`) |
| `fact_quote` | `xero_quotes` | 1 row per quote | ⚠️ No-offset date regex required |
| `fact_quote_line` | `xero_quotes` | 1 row per quote + line | Quote lines lack `AccountCode`/`AccountID` |
| `fact_receipt` | `xero_receipts` | 1 row per receipt | ⚠️ Bronze empty |

---

## Issues Encountered & Resolutions

### 1. GCP Authentication — ADC expired
**Symptom:** `Error creating BigQuery client — Please check your authentication.`  
**Cause:** Application Default Credentials refresh token had expired.  
**Fix:** Run interactively in a terminal:
```bash
gcloud auth application-default login \
  --scopes=https://www.googleapis.com/auth/cloud-platform
```

### 2. GCP Project ID not detected
**Symptom:** `Unable to detect a Project Id in the current environment.`  
**Cause:** `GOOGLE_CLOUD_PROJECT` env var not set; ADC `quota_project_id` is not always picked up.  
**Fix:** Add to `~/.zshrc` and relaunch VS Code from a fresh terminal:
```bash
export GOOGLE_CLOUD_PROJECT=prj-dw-dev
```

### 3. Dataform preview hangs forever
**Symptom:** Preview panel opens but spins indefinitely.  
**Cause:** Most commonly triggered by empty result sets or stale BQ auth in the extension.  
**Fix (fastest):** `Cmd+Shift+P` → `Developer: Reload Window`.  
**Alternative:** `pkill -f "dataform" && pkill -f "@dataform"` — extension restarts server automatically.

### 4. `NULL` in UNION ALL typed as INT64
**Symptom:** `Column N in UNION ALL has incompatible types: INT64, STRING`  
**Cause:** BigQuery infers bare `NULL` as `INT64`.  
**Fix:** `CAST(NULL AS STRING)` wherever NULL is a placeholder for a STRING column.  
**Affected table:** `dim_contact_group_member`

### 5. Budget lines empty despite header data
**Symptom:** `fact_budget_line` is empty; `fact_budget` has rows.  
**Cause:** `/Budgets` endpoint does not return `BudgetLines[]` unless `?BudgetLines=true` is passed per-budget fetch.  
**Fix needed:** Update Rust sync service to call `GET /Budgets/{BudgetID}?BudgetLines=true` for each budget.

### 6. `Schedule.NextScheduledDateString` is a bare date string
**Symptom:** `Failed to parse input string "2026-06-04"` on `dim_repeating_invoice`.  
**Cause:** `PARSE_DATETIME('%Y-%m-%dT%H:%M:%S', ...)` expects a time component; this field is `YYYY-MM-DD` only.  
**Fix:** `SAFE_CAST(JSON_VALUE(payload, '$.Schedule.NextScheduledDateString') AS DATE)`.  
**Lesson:** Not all `*String` date companions follow the `T00:00:00` pattern. Use `SAFE_CAST AS DATE` for any field that is date-only.

### 7. Quote dates have no timezone offset
**Symptom:** Date columns NULL on all `fact_quote` / `fact_quote_line` rows.  
**Cause:** `xero_quotes` encodes dates as `/Date(ms)/` without `+0000` — standard regex `[+-]\d{4}` does not match.  
**Fix:** Permissive regex `r'/Date\((\d+)(?:[+-]\d{4})?\)/'` on all date fields in quotes.

### 8. Manual journal lines have no LineItemID
**Cause:** Xero manual journal lines are keyed by account, not by a generated line ID.  
**Impact:** `fact_manual_journal_line` grain is `tenant_id + manual_journal_id + account_id`. If a journal posts to the same account twice (unusual but possible), rows will merge. `IsBlank=true` lines filtered out at query time.

---

## Design Decisions

### `dim_contact_group_member` — dual source UNION
UNIONs `xero_contact_groups` (group-centric) and `xero_contacts` (contact-centric). Neither endpoint alone is authoritative. Deduplication via `GROUP BY tenant_id, contact_group_id, contact_id` with `MAX()` for names. `CAST(NULL AS STRING)` required in the groups branch for `contact_group_name`.

### Contact sub-tables
`dim_contact_address` and `dim_contact_phone` are separate tables because Xero allows multiple per contact. `address_count` and `phone_count` on `dim_contact` give header-level summaries without joins.

### Gold-layer harmonisation
`dim_account` pre-derives `bs_pl` (`'BS'`/`'P&L'`) and `fsli_1` (`'Assets'`, `'Equity and liabilities'`, `'Revenue'`, `'Operating expenses'`) using the same label values as `dw_1_silver_visma.dim_account`. Gold-layer UNION needs no extra CASE logic. `_source = 'xero'` column on every table distinguishes origin.

### Repeating invoices in `dimentinals/`
Repeating invoices are billing templates, not transactions. They live in `dimentinals/` not `facts/`.

### Tax components inline on `dim_tax_rate`
`TaxComponents[]` always has exactly one element in practice. Flattened as `tax_component_name`, `tax_component_rate` etc. to avoid a redundant child table.

### Overpayments and prepayments — header only
`fact_overpayment` and `fact_prepayment` are header tables only. `LineItems[]` and `Allocations[]` child tables should be added when the bronze tables become non-empty and line-level analysis is needed.

---

## Running the Silver Layer

### Full xero tag run
```bash
cd /Users/mikefriedman/Documents/DWH_Aquatiq/xero_visma_v2/Dataform
dataform run --tags xero
```

### Dry run first (always recommended)
```bash
dataform run --tags xero --dry-run
```

### Inspect a bronze payload before writing SQL
```bash
/opt/homebrew/bin/python3.13 - <<'EOF'
from google.cloud import bigquery; import json
client = bigquery.Client(project="prj-dw-dev")
rows = list(client.query(
  "SELECT payload FROM `prj-dw-dev.dw_1_bronze_xero.xero_TABLE_NAME` LIMIT 1"
).result())
if not rows:
    print("EMPTY")
else:
    p = json.loads(rows[0].payload)
    for k, v in p.items():
        if isinstance(v, list):
            inner = f" | item keys: {list(v[0].keys())}" if v and isinstance(v[0], dict) else ""
            print(f"{k}: [array len={len(v)}]{inner}")
        elif isinstance(v, dict):
            print(f"{k}: {{nested}} keys={list(v.keys())}")
        else:
            print(f"{k}: {type(v).__name__} = {repr(v)[:80]}")
EOF
```

### Check which silver tables exist in BQ
```bash
/opt/homebrew/bin/python3.13 - <<'EOF'
from google.cloud import bigquery
client = bigquery.Client(project="prj-dw-dev")
for r in client.query(
  "SELECT table_name FROM `prj-dw-dev.dw_1_silver_xero.INFORMATION_SCHEMA.TABLES` ORDER BY 1"
).result():
    print(r.table_name)
EOF
```

### Check row counts across all silver tables
```bash
/opt/homebrew/bin/python3.13 - <<'EOF'
from google.cloud import bigquery
client = bigquery.Client(project="prj-dw-dev")
tables = [r.table_name for r in client.query(
  "SELECT table_name FROM `prj-dw-dev.dw_1_silver_xero.INFORMATION_SCHEMA.TABLES` ORDER BY 1"
).result()]
for t in tables:
    n = list(client.query(f"SELECT COUNT(*) AS n FROM `prj-dw-dev.dw_1_silver_xero.{t}`").result())[0].n
    flag = " ← EMPTY" if n == 0 else ""
    print(f"{t}: {n}{flag}")
EOF
```

---

## Going Forward

1. **Fix Rust sync for budget lines** — `GET /Budgets/{BudgetID}?BudgetLines=true` per budget to populate `fact_budget_line`.

2. **Enable missing sync endpoints** — currencies, expense claims, organisations, receipts are all empty; confirm and enable in the sync service.

3. **Add line tables for overpayments and prepayments** — `fact_overpayment_line` and `fact_prepayment_line` when those bronze tables become non-empty.

4. **Gold layer harmonisation** — now that silver is complete, build:
   - `gold/finance/dim_account` — UNION of `dw_1_silver_xero.dim_account` and `dw_1_silver_visma.dim_account` using pre-derived `bs_pl`/`fsli_1` labels
   - `gold/finance/dim_contact` (or `dim_customer`/`dim_supplier`) — UNION of Xero contacts and Visma customers/suppliers
   - `gold/finance/fact_invoice` — UNION of Xero invoices and Visma customer invoices
   - `gold/finance/fact_payment` — UNION of Xero payments and Visma customer payments

5. **Dataform run after any new tables** — `dataform run --tags xero` after each session.
