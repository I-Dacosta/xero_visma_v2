# Data Warehouse Architecture

_Created: 2026-06-04. Updated: 2026-06-04 — Phase 1 complete. Full backfill done. dw_2_staging_xero populated. Next: deploy Cloud Function once Rust GCS writer is ready, then ODS._

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

### Staging Layer (`dw_2_staging_*`)

- One BQ dataset per provider: `dw_2_staging_xero`, `dw_2_staging_visma`
- 1-2 BQ tables per API endpoint
  - Header table (one row per record): e.g. `bank_transactions`
  - Line table where arrays exist (one row per record + line): e.g. `bank_transaction_lines`
- Columns are fully typed (TIMESTAMP, FLOAT64, BOOL, STRING, DATE) — no JSON
- Facts and dimensions are defined at this layer
- Deduplication handled by the Python writer (upsert on `tenant_id` + `record_id`)
- This layer is append-friendly and incrementally updated by the Cloud Function

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

`docs/SILVER_XERO.md` remains the canonical reference for Xero API payload structures, date format quirks, and entity-level notes.

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

## GCS File Format & Metadata

Each raw file is a single JSON object (one API record per file):

```
gs://bucket/raw/xero/{tenant_id}/v1/{entity_type}/{date}/{record_id}.json
```

Example:
```
gs://bucket/raw/xero/9dc5d3f0-68b1-4811-a38d-9efbb5990604/v1/bank_transactions/2026-06-04/a1c669bb-fa0d-4d5b-bf2f-97da2d82d879.json
```

GCS object metadata attributes (set by the sync service on upload):
- `tenant_id`: `9dc5d3f0-68b1-4811-a38d-9efbb5990604`
- `record_id`: `a1c669bb-fa0d-4d5b-bf2f-97da2d82d879`

The JSON body itself does not need to carry these fields — the Cloud Function extracts them from the file path or metadata. During the BQ-bronze development phase, these are taken from the `tenant_id` and `record_id` columns in the bronze table.

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
| Build `cloud_function/main.py` | ✅ Done — path routing tested; `_SingleRecordReader` adapter works |
| Run full historical backfill | ✅ Done — 27/27 entities, 278s, all staging tables populated |
| Deploy Cloud Function | ⏳ Blocked — waiting on Rust GCS writer |
| Swap `bq_reader.py` → `gcs_reader.py` | ⏳ After Rust GCS writer is ready |

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

## Staging Layer — Current State (backfill run 2026-06-04)

All historical bronze data loaded into `dw_2_staging_xero`. 27/27 entities processed in 278 seconds.

| Staging table(s) | Rows |
|---|---|
| `journals` | 24,130 |
| `journal_lines` | 72,485 |
| `invoices` | 12,894 |
| `invoice_lines` | 22,779 |
| `invoice_payments` | 11,616 |
| `payments` | 12,871 |
| `bank_transactions` | 4,659 |
| `bank_transaction_lines` | 4,818 |
| `manual_journals` | 1,556 |
| `manual_journal_lines` | 5,653 |
| `quotes` | 1,119 |
| `quote_lines` | 3,075 |
| `purchase_orders` | 897 |
| `purchase_order_lines` | 1,976 |
| `linked_transactions` | 468 |
| `items` | 327 |
| `credit_notes` | 361 |
| `credit_note_lines` | 520 |
| `credit_note_allocations` | 349 |
| `contacts` | 265 |
| `contact_addresses` | 530 |
| `contact_phones` | 96 |
| `accounts` | 271 |
| `batch_payments` | 82 |
| `batch_payment_lines` | 528 |
| `bank_transfers` | 82 |
| `users` | 42 |
| `tax_rates` | 18 |
| `tracking_categories` | 2 |
| `tracking_options` | 12 |
| `branding_themes` | 2 |
| `budgets` | 2 |
| `contact_groups` | 1 |
| `contact_group_members` | 1 |
| `repeating_invoices` | 1 |
| `repeating_invoice_lines` | 3 |
| Empty (bronze not yet populated) | `currencies`, `expense_claims`, `organisations`, `overpayments`, `prepayments`, `receipts` |

### Immediate next steps (before Phase 2)

**A. Rust GCS writer (colleague's work — blocks Cloud Function)**
- Update the Rust sync service to write each API response as a JSON file to `gs://prj-dw-dev-raw/raw/xero/{tenant_id}/v1/{entity_type}/{date}/{record_id}.json`
- Once this is live, deploy the Cloud Function (see deployment section below) and new records will flow automatically into staging

**B. Cloud Function packaging**
- The `cloud_function/main.py` imports from `etl.xero.*` — the `etl/` parent package must be bundled with the function source. Two options:
  1. Copy `etl/` into `etl/cloud_function/etl/` before deploying (simple but manual)
  2. Move Cloud Function to repo root with a proper `pyproject.toml` (cleaner long-term)
- Resolve this before deploying

**C. `gcs_reader.py`** — write this module to read JSON directly from GCS objects (mirrors `bq_reader.py` interface). Swap in once Rust GCS writer is live. Zero changes needed to any parser.

### Phase 2 — ODS in Dataform

10. **Create ODS L0 Dataform tables** — within-provider joins within Xero staging (e.g. invoices + contacts + accounts enriched into a master invoice view)
11. **Create ODS L1 Dataform tables** — cross-provider harmonisation (Xero + Visma invoices, payments, contacts unified with common schema)
12. **Wire Dataform trigger** — Cloud Function calls Dataform API after staging write completes (or run Dataform on a schedule)

### Phase 3 — Data Mart

13. **Define BI requirements** — which reports need which columns
14. **Build Data Mart Dataform tables** — column selections and pre-aggregations from ODS L1; no joins here
15. **Connect BI tool** — Superset / Power BI pointed at Data Mart tables

---

## What Is Preserved From Previous Work

- `Dataform/definitions/silver/xero/` — all 46 `.sqlx` files kept as field-level reference. The Python parsers translate these directly: `JSON_VALUE(payload, '$.Field')` → `record.get('Field')`
- `docs/SILVER_XERO.md` — canonical reference for all Xero payload structures, date quirks, nesting patterns, and entity-level notes. **Read this before writing any parser.**
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
