# Data Warehouse Architecture

_Created: 2026-06-04. Updated: 2026-07-03 — staging_xero backfilled from GCS (16 endpoints, 345k rows). Staging layer purity enforced: derivations and cross-endpoint joins removed._

---

## Background & Why We Changed Direction

The original approach streamed Xero API responses directly into BigQuery tables using a generic envelope schema (`tenant_id`, `record_id`, `payload` STRING, timestamps). Dataform SQL then parsed the JSON payload column using `JSON_VALUE()` calls to produce a "silver" layer.

**Why this was abandoned:**

- Storing raw JSON strings in BigQuery rows is wasteful — you pay BQ column storage rates for data that is structurally identical to what you already have in the API response
- The `payload` column is opaque to BQ's column-level optimisations (no pruning, no compression benefit)
- Dataform's `JSON_VALUE()` chains are verbose and harder to maintain than Python dict access
- The fundamental insight: **if the data needs to be unpacked anyway, unpack it before it hits BigQuery, not after**

The correct tool for JSON parsing is Python, not SQL. The correct tool for joining clean tables is SQL (Dataform). Each tool does what it's good at.

---

## Architecture Overview

```
Xero / Visma API
       │
       ▼
 GCS Bucket (raw)          ← source of truth, audit trail, replayable
       │
       │  GCS write event
       ▼
 Cloud Function             ← orchestration trigger (one per provider/entity)
       │
       │  Python
       ▼
 Python Parsing Scripts     ← JSON → typed fields; handles nesting, dates, arrays
       │
       │  BQ write (insert/upsert)
       ▼
┌─────────────────────────────────────────────────────┐
│  STAGING LAYER  (dw_2_staging_*)                    │
│  Clean, typed BQ tables. 1-2 tables per endpoint.  │
│  Facts and dimensions defined here.                 │
│  No JSON anywhere.                                  │
└─────────────────┬───────────────────────────────────┘
                  │  Dataform
                  ▼
┌─────────────────────────────────────────────────────┐
│  ODS — LAYER 0  (dw_3_ods_l0_*)                    │
│  Joined tables within a single provider.            │
│  e.g. Xero master table, Visma master table.        │
└─────────────────┬───────────────────────────────────┘
                  │  Dataform
                  ▼
┌─────────────────────────────────────────────────────┐
│  ODS — LAYER 1  (dw_3_ods_l1_*)                    │
│  Joined tables across providers.                    │
│  e.g. Xero + Visma invoices unified.                │
│  Additional layers added as new providers arrive.   │
└─────────────────┬───────────────────────────────────┘
                  │  Dataform
                  ▼
┌─────────────────────────────────────────────────────┐
│  DATA MART  (dw_4_mart_*)                           │
│  Column selections + aggregations for BI.           │
│  No joins here — pre-joined from ODS.               │
│  1 large table or smaller focused tables            │
│  (1 per report / dashboard).                        │
└─────────────────────────────────────────────────────┘
```

---

## Layer Definitions

### GCS Raw Storage

- One JSON file per API record, per sync run
- File metadata carries `tenant_id` and `record_id` (as GCS object metadata attributes, not inside the JSON body)
- Folder structure to be defined (e.g. `gs://bucket/xero/bank_transactions/YYYY-MM-DD/tenant_id/record_id.json`)
- Acts as the permanent audit trail — if anything goes wrong downstream, replay from GCS
- Nothing in GCS is ever deleted or overwritten; new syncs produce new files

### Cloud Function (Orchestration)

- Triggered by GCS write event (one function per provider, or one per entity type)
- Reads the new file from GCS
- Calls the relevant Python parsing module
- Handles retries, error logging, dead-letter routing for malformed files
- Does not do any business logic — purely triggers and routes

### Python Parsing Scripts

- One module per Xero/Visma entity (e.g. `parse_bank_transactions.py`)
- Input: raw JSON dict + `tenant_id` + `record_id` from file metadata
- Output: one or more dicts of typed, flat fields ready for BQ insert
- Responsibilities:
  - Unpack nested objects (e.g. `Contact.ContactID`)
  - Unnest arrays into separate output rows (e.g. `LineItems[]`)
  - Parse Xero `/Date(ms±offset)/` timestamps into Python `datetime`
  - Cast strings to correct types (FLOAT, BOOL, INT)
  - Handle missing/null fields gracefully
- Writes to BQ staging tables via the BQ Python client (insert/upsert)
- **All field-level knowledge from the Dataform silver work is preserved here** — same fields, same nesting patterns, same date quirks — just implemented in Python

### Staging Layer (`staging_*`)

- One BQ dataset per provider: `staging_xero`, `staging_visma`
- 1-N BQ tables per API endpoint
  - Header table (one row per record): e.g. `bank_transactions`
  - Line/child table per nested array (one row per record + item): e.g. `bank_transaction_lines`
- Columns are fully typed (TIMESTAMP, FLOAT64, BOOL, STRING, DATE) — no JSON
- Deduplication handled by the Python writer (MERGE on `tenant_id` + `record_id`)
- This layer is append-friendly and incrementally updated by the Cloud Function

#### Staging Layer Purity (rule established 2026-07-03)

**The staging layer stores raw data as fully unpacked as possible — and nothing more. All joins and all derivations belong in ODS.**

What staging IS allowed to do:
- **Unpack nested arrays** into separate child tables (e.g. `LineItems[]` → `invoice_lines`). One row per array item.
- **Flatten single nested objects** to extract their fields, including foreign-key IDs (e.g. `Contact.ContactID` → `contact_id`). The nested object is part of the same record's payload, so this is unpacking, not a join.
- **Denormalise convenience names** from those same in-payload nested objects (e.g. `Contact.Name` → `contact_name`). Kept as a pragmatic exception — harmless because the value already lives inside the record; it does not require reading another table.

What staging is NOT allowed to do (these are ODS concerns):
- **Derive/compute classifications** — no business-logic mappings. (Removed `bs_pl`, `fsli_1` from `accounts`.)
- **Join or UNION across endpoints/sources** — each parser reads exactly one endpoint. (Removed the `contact_groups` ↔ `contacts` UNION.)

**Changes made to enforce this (2026-07-03):**

| File | Change |
|---|---|
| `etl/xero/accounts.py` | Removed derived `bs_pl` and `fsli_1` columns + their lookup dicts. `account_class` kept raw (ASSET/LIABILITY/EQUITY/REVENUE/EXPENSE). Dropped & re-backfilled `staging_xero.accounts` so the columns are gone. |
| `etl/xero/contact_groups.py` | Rewritten to pure single-endpoint unpacking. `contact_group_members` now sourced ONLY from the groups endpoint's `Contacts[]`. Removed the cross-endpoint UNION with `xero_contacts` and the Python dedup logic. |
| `etl/xero/contacts.py` | Added `contact_group_memberships` child table from the contacts endpoint's own `ContactGroups[]`. This is the contact-centric view; the group-centric view is `contact_groups.contact_group_members`. |

**The two group-membership views are reconciled in ODS, not staging:**
- `staging_xero.contact_group_members` — group-centric (from contact_groups endpoint)
- `staging_xero.contact_group_memberships` — contact-centric (from contacts endpoint)
- Neither endpoint alone is authoritative; the ODS layer UNIONs and dedups them.

Denormalised names that were deliberately KEPT (harmless, per the rule above): `bank_transactions.contact_name` / `.bank_account_name`, `invoices.contact_name`, `payments.contact_name`, etc.

### ODS — Operational Data Store (`dw_3_ods_*`)

Managed by **Dataform**. This is where Dataform's dependency graph earns its value.

**Layer 0 (`dw_3_ods_l0_*`)** — within-provider joins:
- Joins staging tables within a single provider into unified master tables
- e.g. Xero: join `invoices` + `invoice_lines` + `contacts` + `accounts` into a single enriched invoice table
- e.g. Visma: equivalent master tables from Visma staging

**Layer 1 (`dw_3_ods_l1_*`)** — cross-provider joins:
- Unifies equivalent entities across providers
- e.g. `invoices` = Xero ACCREC invoices UNION Visma customer invoices, with a common schema
- New layers (L2, L3 etc.) can be added as new providers or data sources come online
- Schema harmonisation happens here (common column names, common classification labels like `bs_pl`, `fsli_1`)

### Data Mart (`dw_4_mart_*`)

- Purpose-built tables for BI tools (Power BI, Looker, Superset)
- **No joins** — all joining is done upstream in ODS; selects are fast
- Column selections: only the columns a given report actually needs
- Aggregations: pre-computed sums, counts, averages where needed
- Two approaches (can coexist):
  - One large wide table covering most BI needs
  - Smaller focused tables, one per report or dashboard
- Refreshed by Dataform on a schedule or triggered from ODS completion

---

## What Dataform Is Used For (New Scope)

Dataform is **not** used for JSON parsing or staging population (that's Python now). It is used for:

- ODS Layer 0: within-provider joins and enrichment
- ODS Layer 1: cross-provider harmonisation
- Data Mart: final column selection and aggregation
- Dependency graph: ensures ODS tables rebuild in the right order when staging tables update
- Scheduling: Dataform runs triggered after Cloud Function confirms staging write complete

---

## What Is Preserved From Previous Work

The Dataform silver layer work (46 `.sqlx` files in `Dataform/definitions/silver/xero/`) is **kept as-is** and serves as:

1. **Field reference** — every field name, nesting path, data type, and quirk is documented in SQL. The Python parsing scripts translate this directly.
2. **Historical record** — shows the progression of thinking and the full API field inventory
3. **Fallback** — if the GCS/Python approach is ever paused, the BQ streaming + Dataform path still exists

`docs/STAGING_XERO.md` remains the canonical reference for Xero API payload structures, date format quirks, and entity-level notes.

---

## Key Technical Decisions

| Decision | Choice | Reason |
|---|---|---|
| Raw data storage | GCS | Cheap, durable, replayable; no BQ storage cost for unprocessed data |
| JSON parsing | Python | More readable than SQL JSON functions; better type handling; easier to test |
| Orchestration trigger | Cloud Function on GCS write | Event-driven, no polling, scales to zero |
| Staging → ODS → Mart | Dataform | Dependency graph, scheduling, dry-run compile checks |
| Deduplication | Python upsert on `tenant_id + record_id` | Staging tables stay current without full reloads |
| Layer naming | staging / ods / mart | Clearer intent than silver/gold; standard DWH terminology |

---

## All Decisions Resolved

| Question | Decision |
|---|---|
| Raw data storage | GCS bucket — `raw/{vendor}/{tenant_id}/v1/{entity_type}/{date}/{record_id}.json` |
| GCS as primary filter | Yes — folder structure covers 90% of access patterns; BQ catalog for edge cases; GCS metadata for auditing only |
| Visma data source | Same GCS pattern — `raw/visma/{tenant_id}/v1/{entity_type}/{date}/` |
| JSON parsing tool | Python (not Dataform SQL) |
| Deduplication strategy | MERGE via temp table on `tenant_id + record_id` |
| Python development approach | Build and test against existing BQ bronze tables as stand-in; swap source to GCS files once Rust GCS writer is ready |
| Python scripts location | `etl/` folder at repo root — new folder, nothing overwritten |
| Metadata fields | Each GCS file carries `tenant_id` and `record_id` as object metadata attributes; also present in the JSON body |

---

## GCS File Format & Metadata (UPDATED 2026-06-29)

### Bucket
`gs://aquatiq-dw-dev-storage` (not `prj-dw-dev-raw` as originally planned)

### Path format
```
raw/{vendor}/{tenant_id}/2.0/{entity_type}/{date}/{timestamp}_{run_id_short}_{page}.json
```

Example:
```
raw/xero/19b25bd5-431a-4af4-8ecf-7a5a75cbcc5c/2.0/accounts/2026-06-18/20260618T134051Z_9c0e3d_p001.json
```

### Two files per API call
Each sync produces a **data file** and a **metadata file**:
- `20260618T134051Z_9c0e3d_p001.json` — the API response (array of records)
- `20260618T134051Z_9c0e3d_p001.json.meta.json` — sync context

### Metadata file structure
```json
{
  "x-api-version": "2.0",
  "x-endpoint": "accounts",
  "x-http-status": "200",
  "x-org-name": "Aqua Pharma Australia Pty Ltd",
  "x-page": "1",
  "x-record-count": "126",
  "x-run-id": "9c0e3d86-4c84-4d69-8587-ef085eeb20de",
  "x-sync-type": "master",
  "x-synced-at": "2026-06-18T13:40:56.194109096+00:00",
  "x-tenant-id": "19b25bd5-431a-4af4-8ecf-7a5a75cbcc5c",
  "x-vendor": "xero"
}
```

### Data file structure
The data file is an API response wrapper containing a **batch of records** in an endpoint-specific array:
```json
{
  "Id": "5a53fde9-...",
  "Status": "OK",
  "ProviderName": "Aqua Pharma Pty Ltd Data Warehouse Server",
  "DateTimeUTC": "/Date(1781790056419)/",
  "pagination": { "page": 1, "pageSize": 100, "pageCount": 1, "itemCount": 56 },
  "Accounts": [ { ... }, { ... } ]
}
```

The records array key is always the **PascalCase endpoint name**: `Accounts`, `Invoices`, `BankTransactions` etc.
Some endpoints include a `pagination` object; others don't. Both forms are handled the same way.

### Trigger strategy
The Cloud Function triggers on `.meta.json` file writes **only** (data file triggers are ignored). When a meta file lands:
1. Read the meta file → extract `x-tenant-id`, `x-endpoint`, `x-synced-at`, `x-run-id`
2. Derive the data file path by stripping `.meta.json` from the trigger path
3. Read the data file → extract the records array using the PascalCase endpoint key
4. Loop over all records in the array and send each through the entity parser
5. MERGE all parsed rows into the staging table in one batch

This means one Cloud Function invocation processes a full page of records, not one record at a time.

---

## Deduplication — MERGE via Temp Table

The Python BQ writer follows this pattern for every entity:

1. Parse the incoming JSON into typed fields
2. Write the parsed row(s) to a short-lived BQ temp table (e.g. `dw_2_staging_xero._tmp_bank_transactions_{run_id}`)
3. Run a `MERGE` statement joining the temp table to the staging table on `tenant_id + record_id`
   - `WHEN MATCHED` → UPDATE all fields
   - `WHEN NOT MATCHED` → INSERT new row
4. Drop the temp table

This guarantees exactly one current row per record in staging at all times. If the Cloud Function fails mid-write, the staging table is untouched — the MERGE only commits once the temp table is fully written.

---

## ETL Project Structure (`etl/`) — BUILT

```
etl/
  common/
    __init__.py
    date_parser.py          ← Xero /Date(ms±offset)/ → datetime (17 tests passing)
    bq_writer.py            ← MERGE via temp table; schema-aware to prevent type mismatches
    bq_reader.py            ← BQ bronze stand-in for GCS during development (7 tests passing)
  xero/
    __init__.py
    accounts.py
    bank_transactions.py    ← proof-of-concept; 9 tests passing end-to-end
    bank_transfers.py
    batch_payments.py
    branding_themes.py
    budgets.py
    contact_groups.py       ← dual-source UNION (contact_groups + contacts endpoints)
    contacts.py
    credit_notes.py
    currencies.py
    expense_claims.py       ← bronze empty; parser ready
    invoices.py
    items.py
    journals.py             ← uses TrackingCategories[] not Tracking[]
    linked_transactions.py
    manual_journals.py
    organisations.py        ← bronze empty; parser ready
    overpayments.py         ← bronze empty; parser ready
    payments.py
    prepayments.py          ← bronze empty; parser ready
    purchase_orders.py
    quotes.py               ← no-offset /Date(ms)/ handled by permissive regex
    receipts.py             ← bronze empty; parser ready
    repeating_invoices.py   ← Schedule.NextScheduledDateString is bare YYYY-MM-DD
    tax_rates.py
    tracking_categories.py
    users.py
  visma/
    __init__.py             ← placeholder; parsers added when Visma GCS write is ready
  cloud_function/
    main.py                 ← GCS trigger; _SingleRecordReader adapter; VENDOR_PARSERS dispatch
    requirements.txt        ← functions-framework, google-cloud-storage, google-cloud-bigquery
  tests/
    test_date_parser.py         ← 17 tests
    test_bq_writer.py           ← 4 tests (real BQ MERGE)
    test_bq_reader.py           ← 7 tests (real bronze data)
    test_bank_transactions.py   ← 9 tests (end-to-end through staging)
```

All 20 Xero entity parsers tested against real bronze data — 20/20 passing.

---

## Development Sequence

### Phase 1 — ETL Pipeline ✅ COMPLETE

| Step | Status |
|---|---|
| Create `dw_2_staging_xero` BQ dataset | ✅ Done |
| Build `common/date_parser.py` | ✅ Done — 17 tests passing |
| Build `common/bq_writer.py` | ✅ Done — 4 tests passing; schema-aware temp table |
| Build `common/bq_reader.py` | ✅ Done — 7 tests passing; QUALIFY deduplication added |
| Build `xero/bank_transactions.py` (proof of concept) | ✅ Done — 9 tests passing end-to-end |
| Build remaining 19 Xero entity parsers | ✅ Done — 20/20 passing against real data |
| Build `cloud_function/main.py` | ✅ Done — updated for new GCS structure; `_BatchReader` for per-file batch processing |
| Build `common/gcs_reader.py` | ✅ Done — reads meta + data files, yields records; 7 tests passing |
| Build `common/endpoint_config.py` | ✅ Done — 28 endpoints mapped |
| Schema evolution in `bq_writer.py` | ✅ Done — auto-detect + ALTER TABLE for new API fields |
| Run full historical backfill | ✅ Done — 27/27 entities, 278s, all staging tables populated |
| End-to-end GCS → staging test | ✅ Done — 126 accounts from `aquatiq-dw-dev-storage` → staging confirmed |
| Deploy Cloud Function | ⏳ Pending — packaging of `etl/` with function source to be resolved |

**Bugs found and fixed during backfill:**

1. **Bronze table has duplicate records** — the bronze BQ table stores every sync run for each entity, so the same `(tenant_id, record_id)` can appear multiple times with different timestamps. Without deduplication, the temp table had duplicate keys and BQ MERGE failed with `UPDATE/MERGE must match at most one source row for each target row`. **Fix:** added `QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_id, record_id ORDER BY last_seen_at DESC) = 1` to `BQReader.iter_records()` so only the latest version of each record is returned.

2. **Schema mismatch from early test runs** — staging tables created during development (with `limit=5`) had some columns typed as INT64 because the small sample happened to have numeric-only values (e.g. account codes like `803`). The full backfill had alphanumeric values (e.g. `100-008`) that couldn't be cast to INT64. **Fix:** dropped affected staging tables so the full backfill recreated them from scratch with correct types. The schema-aware `bq_writer.py` then prevents this happening again on incremental updates.

**Other implementation notes discovered during Phase 1:**
- `bq_writer.py` uses the existing staging table schema for the temp write (not autodetect) — prevents type drift on incremental updates
- `contact_groups.py` reads from both `xero_contact_groups` AND `xero_contacts` in batch mode; in Cloud Function (single-record) mode only the direct group→contacts relationship is written per event
- `journals.py` uses `TrackingCategories[]` not `Tracking[]` — Xero API inconsistency only affecting system-generated journals
- `repeating_invoices.py` `Schedule.NextScheduledDateString` is bare `YYYY-MM-DD` (not `T00:00:00`) — use `parse_iso_date`, not `parse_iso_datetime`
- `quotes.py` dates have no timezone offset `/Date(ms)/` — permissive regex `(?:[+-]\d{4})?` handles both formats

---

## BigQuery Dataset Structure (as of 2026-06-29)

### Active datasets

| Dataset | Purpose |
|---|---|
| `staging_xero` | Parsed, typed Xero API records — one table per endpoint |
| `staging_visma` | Parsed, typed Visma API records — one table per endpoint |
| `ods_xero` | Xero-specific intermediate and final joins (self-contained) |
| `ods_visma` | Visma-specific intermediate and final joins (self-contained) |
| `ods` | Cross-provider unified tables (consumes final output from ods_xero, ods_visma) |
| `datamart` | BI-ready, no joins — column selections and aggregations from ODS |

### Deprecated datasets (safe to delete after verification)
All data copied to `deprecated_*` versions. Original datasets still exist in BQ but should be deleted by a GCP admin (requires `bigquery.datasets.delete` permission — do from the BQ console):

| Original dataset | Deprecated copy | Tables |
|---|---|---|
| `dw_1_bronze_xero` | `deprecated_dw_1_bronze_xero` | 29 |
| `dw_2_staging_xero` | `deprecated_dw_2_staging_xero` | 36 |
| `dw_1_silver_xero` | `deprecated_dw_1_silver_xero` | 46 |

Visma datasets (`dw_1_silver_visma`, `dw_1_silver_visma_global`) are **untouched**.

---

## Staging Layer — Current State (GCS backfill 2026-07-03)

`staging_xero` populated **from the GCS bucket** (`aquatiq-dw-dev-storage`) via `etl/backfill_gcs.py`. 16 endpoints, 0 failures, ~21 min, **345,515 rows across 27 tables**. This is real multi-tenant data (6 organisations) — much larger than the earlier single-tenant bronze sample.

Run with: `python -m etl.backfill_gcs [endpoint ...]` (no args = all endpoints).

| Staging table(s) | Rows |
|---|---|
| `quotes` / `quote_lines` | 38,862 / 107,202 |
| `purchase_orders` / `purchase_order_lines` | 31,416 / 68,918 |
| `invoices` / `invoice_lines` / `invoice_payments` | 15,294 / 26,845 / 13,603 |
| `payments` | 14,989 |
| `bank_transactions` / `bank_transaction_lines` | 4,985 / 5,150 |
| `manual_journals` / `manual_journal_lines` | 1,731 / 6,541 |
| `items` | 1,999 |
| `contacts` / `contact_addresses` / `contact_phones` / `contact_group_memberships` | 1,675 / 3,350 / 333 / 1 |
| `accounts` | 786 |
| `credit_notes` / `credit_note_lines` / `credit_note_allocations` | 457 / 646 / 412 |
| `users` | 140 |
| `tax_rates` | 82 |
| `tracking_categories` / `tracking_options` | 6 / 46 |
| `currencies` | 27 |
| `branding_themes` | 14 |
| `organisations` | 6 |

**Endpoints present in GCS but intentionally NOT parsed:**
- `bills` — **SKIPPED (decided 2026-07-06).** Verified by inspecting both folders: the `bills` folder is a complete subset of `invoices`. The `invoices` folder returns both types (11,214 ACCPAY + 4,080 ACCREC = 15,293 distinct IDs); the `bills` folder returns only the same 11,214 ACCPAY records (11,213 distinct, all also in invoices; zero unique to bills). `staging_xero.invoices` therefore already holds every bill. Parsing `bills` would re-MERGE identical records for no gain. Any "bills" view downstream is simply `WHERE invoice_type = 'ACCPAY'` on `staging_xero.invoices`.

**Endpoints with parsers but NOT in the GCS bucket yet** (will populate when synced): `bank_transfers`, `batch_payments`, `budgets`, `contact_groups`, `expense_claims`, `linked_transactions`, `overpayments`, `payment_services`, `prepayments`, `receipts`, `repeating_invoices`.

**Note on Journals:** the old bronze data had a `journals` endpoint (system GL journals). It is not currently in the GCS bucket endpoint list — confirm with colleague whether journals will be synced (it is the GL source of truth and important for ODS finance tables).

### Immediate next steps (updated 2026-06-29)

**A. New Dataform branch** ✅
All Dataform work goes on branch `Datawarehouse/Dev-Etl-JSON`.

**B. `etl/common/gcs_reader.py`** ✅ Done
Reads both the `.meta.json` and data files from GCS. Yields one record dict per item in the records array, in the same shape as `bq_reader.py` so all parsers work unchanged. Tested against real bucket — 126 accounts records extracted and parsed correctly.

**C. `etl/common/endpoint_config.py`** ✅ Done
Explicit mappings for all 28 endpoints:
- Endpoint name → PascalCase array key (`"accounts"` → `"Accounts"`)
- Endpoint name → record ID field (`"accounts"` → `"AccountID"`)

**D. Updated `etl/cloud_function/main.py`** ✅ Done
- Triggers on `.meta.json` files only (data file triggers silently ignored)
- `_BatchReader` wraps the full list of records from a file so all parsers work unchanged
- Routes by vendor/endpoint parsed from the GCS path
- New bucket `aquatiq-dw-dev-storage` and `2.0` version path

**E. `bq_writer.py` — schema evolution support** ✅ Done (3 improvements)
See Schema Evolution section below.

**F. `etl/xero/accounts.py`** ✅ Done
Added `reporting_code_updated_at` field (new in live API responses).

**G. Cloud Function packaging** ⏳ Pending
`cloud_function/main.py` imports from `etl.xero.*` — the `etl/` parent package must be bundled with the function before deploying.

**H. End-to-end test** ✅ Confirmed
GCS (`aquatiq-dw-dev-storage`) → `gcs_reader.py` → `accounts.py` → `dw_2_staging_xero.accounts` — 126 records, schema evolution handled automatically.

---

## Schema Evolution — How New API Fields Are Handled

When the Xero API adds a new field to a response (e.g. `ReportingCodeUpdatedUTC`), the staging table will not have that column yet. `bq_writer.py` handles this automatically in three steps:

1. **Detect new fields** — compare the data's field names against the existing staging table schema. If any are new, log them and switch the temp table write from schema-bound to autodetect.

2. **Autodetect temp table** — BQ infers types for all fields including the new ones. Existing fields retain their correct types from the data values.

3. **`ALTER TABLE` target** — before running the MERGE, add the new column(s) to the staging table using `ALTER TABLE ADD COLUMN IF NOT EXISTS` (idempotent). The column type is taken from the autodetected temp table schema.

After these three steps the MERGE runs normally — both temp and target have the new columns. **No manual DDL or intervention is required when the API adds fields.**

Log output when schema evolution fires:
```
INFO: New fields detected (schema evolution) — using autodetect: ['reporting_code_updated_at']
INFO: Schema evolution: added 1 column(s) to dw_2_staging_xero.accounts: ['reporting_code_updated_at']
INFO: Merged 126 row(s) into accounts
```

## Open Items — To Check Later

Things known to be incomplete or pending external input, as of 2026-07-06:

### Journals not yet in GCS (GL source of truth)
- The Xero `/Journals` endpoint (the general ledger — where every transaction posts) is **not yet in the bucket** for any tenant, including the three expected ones (`19b25bd5…`, `83adbd31…`, `9dc5d3f0…`). Confirmed by listing every endpoint folder per tenant.
- Colleague confirmed journals **will** arrive eventually but the sync isn't writing them yet (access issues on their side).
- **Do not confuse `journals` (GL) with `manual_journals` (hand-entered only)** — both are separate Xero endpoints; only `manual_journals` is currently synced.
- **Parser was removed 2026-07-07** under the bucket-driven policy (see below). Payload format is verified against `dw_1_bronze_xero.xero_journals` and documented in `docs/STAGING_XERO.md`. `endpoint_config.py` still carries the `Journals` / `JournalID` mapping.
- **When journals land:** the drift detector will flag it. Restore the parser with `git show <pre-2026-07-07-commit>:etl/xero/journals.py > etl/xero/journals.py`, add it back to the PARSERS maps in `backfill_gcs.py` and `cloud_function/main.py`, then run `python -m etl.backfill_gcs journals`.
- **Multi-tenant caveat:** colleague noted journals access is failing for tenants beyond the three named (`19b25bd5…`, `83adbd31…`, `9dc5d3f0…`). Verify all expected tenants produce journals once the sync is fixed.
- **Keep `dw_1_bronze_xero` (or its deprecated copy) until journals are live** — it's the reference payload for rebuilding the parser.

### Parser policy — bucket-driven, not project-driven (changed 2026-07-07)

**A parser exists only for an endpoint that is actually present in the GCS bucket.** The GCS bucket — not the old BigQuery bronze project — is the source of truth for which endpoints exist.

Previously we carried 28 parsers, all ported from the frozen `dw_1_bronze_xero` project (its 29 tables). Only 16 of those endpoints are actually in the GCS bucket. The other 12 were speculative — built against a project that no longer drives the pipeline. They have been **removed** to keep the parser set honest.

**Removed 2026-07-07** (were old-project-only, absent from GCS): `bank_transfers`, `batch_payments`, `budgets`, `contact_groups`, `expense_claims`, `journals`, `linked_transactions`, `overpayments`, `payment_services`, `prepayments`, `receipts`, `repeating_invoices`.

These are **preserved in git history** (commit before this change) and their payloads are documented in `docs/STAGING_XERO.md`. Rebuilding any one is a quick `git show <commit>:etl/xero/<name>.py` + re-add to the two PARSERS maps.

> Note: `journals` was removed too, even though it is confirmed coming. When it lands, the drift detector (below) flags it and we restore the parser from git — no need to carry it speculatively in the meantime. Its payload is verified and documented.

### Drift detection — get warned when a new endpoint appears

Both entry points now compare bucket endpoints against the parser set and warn on anything unrecognised:

- **`backfill_gcs.py`** — on every run, lists all bucket endpoints and logs `NEW ENDPOINT DETECTED IN BUCKET: '<x>' has no parser…` for any endpoint that has neither a parser nor a `KNOWN_UNPARSED` entry. Prints a summary banner if any are found.
- **`cloud_function/main.py`** — per meta-file event, if the endpoint has no parser it logs `NEW ENDPOINT DETECTED: <vendor>/<endpoint> has no parser…` (unless it's in `KNOWN_UNPARSED`).

`KNOWN_UNPARSED` (endpoints intentionally skipped, kept out of warnings): `bills` — proven subset of `invoices` (ACCPAY).

**Current parser inventory: 16 parsers, all backed by live GCS data.** When your colleague adds or renames a sync endpoint, the next backfill run or Cloud Function event surfaces it automatically — that is the signal to build the parser from the real payload.

### Cloud Function not yet deployed
- `cloud_function/main.py` imports from `etl.xero.*` — the `etl/` package must be bundled with the function source before deploy. Resolve packaging (copy `etl/` into the function dir, or restructure with `pyproject.toml`).
- Until deployed, staging is populated via manual `backfill_gcs.py` runs. That's fine for now; deploy when ready for event-driven ingestion.

### Deprecated datasets await deletion
- `dw_1_bronze_xero`, `dw_2_staging_xero`, `dw_1_silver_xero` were copied to `deprecated_*` but the originals still exist (delete requires `bigquery.datasets.delete`, which the local creds lack). A GCP admin should delete the three originals from the BQ console. (Note: `dw_1_bronze_xero` is still useful right now as the reference for the journals payload format — keep until journals are live.)

### Denormalised-name exception is deliberate
- Staging keeps convenience name columns (`contact_name`, `bank_account_name`, etc.) even though pure theory would push them to ODS. This was an explicit decision (2026-07-03). If a future reviewer flags them as "joins in staging," they are not — the values come from the same record's own payload. See "Staging Layer Purity".

---

### Phase 2 — ODS in Dataform

10. **Create ODS L0 Dataform tables** — within-provider joins within Xero staging (e.g. invoices + contacts + accounts enriched into a master invoice view). Note: ODS L0 complexity will differ per provider — Xero and Visma get their own `ods_xero` / `ods_visma` datasets with independent intermediate joins.
11. **Create ODS L1 Dataform tables** — cross-provider harmonisation (Xero + Visma invoices, payments, contacts unified with common schema). **This is where `bs_pl`/`fsli_1` classification and the contact-group-membership reconciliation now live** (moved out of staging).
12. **Wire Dataform trigger** — Cloud Function calls Dataform API after staging write completes (or run Dataform on a schedule)

### Phase 3 — Data Mart

13. **Define BI requirements** — which reports need which columns
14. **Build Data Mart Dataform tables** — column selections and pre-aggregations from ODS L1; no joins here
15. **Connect BI tool** — Superset / Power BI pointed at Data Mart tables

---

## What Is Preserved From Previous Work

- `Dataform/definitions/silver/xero/` — all 46 `.sqlx` files kept as field-level reference. The Python parsers translate these directly: `JSON_VALUE(payload, '$.Field')` → `record.get('Field')`
- `docs/STAGING_XERO.md` — canonical reference for all Xero payload structures, date quirks, nesting patterns, and entity-level notes. **Read this before writing any parser.**
- `Dataform/definitions/gold/` — kept as reference for ODS/Data Mart design (these become Phase 2 and 3)

## Infrastructure — Provisioned (2026-06-04)

All infrastructure created and ready.

| Resource | Details |
|---|---|
| GCS bucket | `gs://prj-dw-dev-raw` — project `prj-dw-dev`, region `europe-north2`, STANDARD storage class |
| BQ staging dataset | `prj-dw-dev.dw_2_staging_xero` — region `europe-north2` |
| Service account | `dwh-etl-pipeline@prj-dw-dev.iam.gserviceaccount.com` |
| SA roles | `roles/storage.objectViewer` (GCS read), `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` (BQ write) |

No open infrastructure questions remain. ETL pipeline built and tested.

---

## Cloud Function — Deployment

The Cloud Function is in `etl/cloud_function/`. It is a 2nd-gen Cloud Function (HTTP/event-driven) using the `functions-framework`.

### Deploy command

```bash
gcloud functions deploy xero-etl-pipeline \
  --gen2 \
  --region=europe-north1 \
  --runtime=python313 \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=prj-dw-dev-raw" \
  --entry-point=process_gcs_upload \
  --source=etl/cloud_function \
  --service-account=dwh-etl-pipeline@prj-dw-dev.iam.gserviceaccount.com \
  --set-env-vars=GOOGLE_CLOUD_PROJECT=prj-dw-dev \
  --memory=512MB \
  --timeout=300s \
  --project=prj-dw-dev
```

### Notes
- `--source` points to `etl/cloud_function/` which contains `main.py` and `requirements.txt`
- The `etl/` parent package (parsers) must be bundled — either copy `etl/` into `cloud_function/` before deploy, or restructure as a proper package with a top-level `requirements.txt`
- Trigger fires on every `google.cloud.storage.object.v1.finalized` event in the bucket — i.e. on every new or overwritten file
- Memory: 512MB is sufficient for single-record parsing; increase if batch replays are run via the function
- Timeout: 300s gives headroom for the BQ MERGE on large line-item arrays (e.g. quotes with 100+ lines)

### Running a full backfill (batch mode)

To process all existing bronze records into staging without waiting for GCS events:

```bash
cd /Users/mikefriedman/Documents/DWH_Aquatiq/xero_visma_v2
/opt/homebrew/bin/python3.13 - <<'EOF'
from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
import etl.xero.invoices as invoices
# ... import other parsers

reader = BQReader(project="prj-dw-dev", dataset="dw_1_bronze_xero")
writer = BQWriter(project="prj-dw-dev", dataset="dw_2_staging_xero")

result = invoices.run(reader, writer)
print(result)
EOF
```

### Adding Visma parsers

1. Create `etl/visma/` modules following the same pattern as `etl/xero/`
2. Add to `VENDOR_PARSERS` in `cloud_function/main.py`:
   ```python
   from etl.visma import customer_invoices as _visma_customer_invoices
   VISMA_PARSERS = {"customer_invoices": _visma_customer_invoices, ...}
   VENDOR_PARSERS["visma"] = VISMA_PARSERS
   ```
3. Create `dw_2_staging_visma` BQ dataset
4. Redeploy the Cloud Function
