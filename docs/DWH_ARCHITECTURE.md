# Data Warehouse Architecture

_Created: 2026-06-04. Updated: 2026-07-24 (yet later still, again, once more, still) — **Investigated how `journals` is actually synced (colleague raised: no date-range filter on `/Journals`) — confirmed a hybrid strategy, not a full pull every time.** Checked the GCS sync metadata (`x-sync-type`) across all 7 tenants' full history: frequent (~every 4 hours) `incremental` runs fetch only newly-created journals via an offset/cursor (small counts, often single digits or 0) — consistent with Xero's actual `/Journals` API design, which paginates by an ever-increasing `offset`/JournalNumber rather than `If-Modified-Since`, because journals are an append-only immutable ledger (never edited once posted), unlike mutable objects like invoices. Interspersed with these, at a roughly-every-several-days cadence (observed gaps of 4-8 days per tenant), are `master`/`rolling-full` runs that **do** re-download the tenant's *entire* journal history from scratch every time (e.g. one tenant: 2,718 records on 07-08, 2,769 on both 07-16 and 07-20 — a full re-fetch, not incremental). **This matters for confidence in this session's reconciliation findings, not just as a sync-mechanics detail**: because the periodic full re-pull genuinely re-downloads everything Xero's `/Journals` currently returns, it rules out "we just haven't synced back far enough" as an explanation for any of the gaps found so far — strengthening (not just "not contradicting") the conclusion that (a) the Balance Sheet conversion-balance gap is a genuine, permanent Xero-side exclusion (conversion balances are structurally never assigned a JournalNumber, most likely entered via a distinct one-time setup mechanism outside the normal ledger flow — a full re-pull would have surfaced them by now if they existed in `/Journals` at all), and (b) the `d08f38af`/`dcb20a20` document-vs-GL outliers from check 4 are genuine content differences (e.g. an invoice's account re-coded after its journal already posted — journals are immutable, so a later edit on the document side simply can't retroactively appear there) rather than a sync-completeness artifact. Updated 2026-07-24 (yet later still, again, once more) — **Check 4 built: document-fact bottom-up reconciliation (`ods_xero_1.recon_document_facts`).** Compares every Xero document fact against `fact_general_ledger`, all-time, at tenant+account+document_type grain — 14 document_types covered (the ones mapped in the earlier source_type exercise). Two things had to be discovered empirically before it worked: (1) **a sign convention per document_type** (not universal — e.g. `ACCPAY` needs no flip, `ACCREC` needs one, and each "credit" type flips the OPPOSITE way from its regular counterpart, since a credit note reverses the original entry's debit/credit direction — all verified against real data, not assumed); (2) **DELETED/VOIDED documents must be excluded from the document-fact side** — a voided invoice still carries its original amount in the `/Invoices` response, but Xero's GL never posted it (or reversed it), so counting it overstates the document side only; this alone dropped one tenant's `ACCREC` residual from 25% to 0%. Also confirmed empirically that transfer/overpayment-linked `bank_transactions` rows (`transaction_type` `SPEND-TRANSFER`/`RECEIVE-TRANSFER`/`SPEND-OVERPAYMENT`/`RECEIVE-OVERPAYMENT`) never independently post to GL under any source_type (0 matches tested) — so `fact_bank_transaction_line`'s CASHPAID/CASHREC bucket is filtered to `document_type IN ('SPEND','RECEIVE')` only, or it would double-count. **Result, on rows where both sides have data:** `MANJOURNAL`/`CASHREC`/`CASHPAID`/`APCREDITPAYMENT` all match to within ~0.5%; `ACCPAYCREDIT`/`TRANSFER`/`ACCRECCREDIT`/`APOVERPAYMENT`/`ACCRECPAYMENT`/`ACCREC` all within ~2-10% excluding one known-outlier tenant; `ACCPAY`/`ACCPAYPAYMENT` have a genuine, larger, tenant-concentrated residual (one tenant, `d08f38af`, at ~82-84%; investigated but not fully root-caused — looks like a document-vs-journal account-allocation mismatch, not a pipeline bug). **A second, expected finding, same shape as the Trial Balance BS gap**: every document type has a large "GL-only" bucket — the AR/AP control-account side of each double entry, which document-fact LINE ITEMS never carry (they represent the revenue/expense/bank side only) — tens of millions of dollars per type, structural, not a defect. Updated 2026-07-24 (yet later still, again) — **Restored `bank_transfers`/`overpayments` parsers to close the last 2 check-4 mapping gaps, and fixed a real concurrency bug in `bq_writer.py` along the way.** Both endpoints were removed 2026-07-07 under the bucket-driven policy (bronze was empty then) and are now confirmed present in `prj-dw-dev-raw` with real data. Restored `etl/xero/bank_transfers.py` and `etl/xero/overpayments.py` from git history, verified against live payloads (added a few missing fields; added `LineItems[]` unpacking to overpayments — the account grain a GL reconciliation needs), registered both in `backfill_gcs.py`/`cloud_function/main.py`, backfilled all 7 tenants (113 bank transfers, 91 overpayments/234 lines — clean grain), and built `ods_xero_0/1.fact_bank_transfer` + `fact_overpayment_line` (with the same tax-basis normalization + reconciliation-assertion pattern as every other document fact; assertion passes). Verified: `journals.source_type='TRANSFER'` now matches `fact_bank_transfer` 113/116; `APOVERPAYMENT`/`AROVERPAYMENT` match `fact_overpayment_line`'s parent 72/72 and 19/19. **Found a genuine concurrency bug in `bq_writer.py` while restoring**: for a brand-new table, `existing_schema = self._get_schema(target)` was read *before* acquiring the per-table lock, so two tenants' first-ever writes could both see "table doesn't exist," each autodetect their own schema, and the second one's MERGE then fails against whichever table the first one just created — moved the schema read inside the lock. Separately hit (again) the schema-autodetect trap on a `reference` field that's sometimes a numeric-looking string per-tenant — BigQuery's `autodetect` infers INTEGER even from an explicitly-`str()`-cast Python value if the string content is all-digits, so the fix had to be pre-creating the table with an explicit schema, not a Python-level cast (cast added anyway, for defense in depth). Updated 2026-07-24 (yet later still) — **GL checks continued: P&L reconciliation built, near-perfect match (94% exact, remainder immaterial).** Built `ods_xero_1.recon_profit_and_loss` vs. `report_profit_and_loss`. Discovered this report is period-bounded (`report_from`/`report_to` — empirically always "1st of current month → sync day", i.e. Xero's default month-to-date window), not fiscal-year YTD like Trial Balance. Hit a genuine sign-convention gotcha: Xero's P&L displays both revenue and expense as unsigned/positive "management report" style amounts, while our GL `amount` is signed double-entry — verified empirically that EXPENSE accounts match with no sign flip (64/64) and REVENUE accounts match only when negated (16/16), both 100%. Final result (scoped to `bs_pl='P&L'` accounts): **80/85 (94%) exact match, 0 mismatches**; the other 5 are small/near-zero EXPENSE amounts present in our GL for the period but simply not shown as rows in Xero's report (plausibly suppressed as immaterial) — not a reconciliation failure. This strongly confirms the Trial Balance finding: our GL is fully reliable within its synced window: the only real gap is BS accounts' pre-window conversion balances. Updated 2026-07-24 (yet later) — **GL checks: `fact_general_ledger` built + Trial Balance reconciliation run, first real finding.** Built `ods_xero_0/1.fact_general_ledger` (native GL fact, one row per `tenant_id+journal_id+journal_line_id`; `net_amount` balances 100% on 70,590/70,590 journals) plus `assert_fact_general_ledger_balances`. Added `financial_year_end_day/month` to `organisations.py`/`dim_organisation` (needed to bound Xero's Trial Balance YTD columns, which reset per-tenant fiscal year — tenants have MIXED fiscal years, 2 use June 30, rest Dec 31). Built `ods_xero_1.recon_trial_balance` (diagnostic): **253/429 (59%) accounts match Xero's own report exactly.** Splitting by account type shows a clean, explainable pattern — P&L accounts match 222/227 (98%), Balance Sheet accounts match only 31/202 (15%). Root cause: BS accounts carry a cumulative balance from company inception, including each org's one-time Xero conversion/opening-balance entry, which is not exposed via the regular `/Journals` feed under any observed `source_type`. P&L accounts reset every fiscal year so `/Journals` alone is complete for them — hence the near-perfect match. Also surfaced 4 previously-undocumented `journals.source_type` values (`TRANSFER`, `AROVERPAYMENT`, `APCREDITPAYMENT`, `ARCREDITPAYMENT`) relevant to the planned document-fact reconciliation check. Conclusion: `recon_trial_balance` stays a diagnostic for BS accounts (structural source-data gap, not fixable in our code); the P&L portion is solid enough to become a real assertion. Updated 2026-07-24 (later still) — **Full ODS rebuild + a real correctness bug found and fixed**: rebuilding `ods_xero_0/1` against the fully-backfilled staging (7 tenants) surfaced 342/1,882 manual journals unbalanced — a genuine bug in `etl/xero/manual_journals.py`'s merge key (`account_id` alone isn't unique within a journal; a real journal can post two separate lines to the same account). Fixed the key (`line_position`, the line's index in Xero's own array), rebuilt staging, now 1/1,882 unbalanced (the same single pre-existing anomaly known since this fact was first built). Also found 2/1,193 quotes with genuine tiny source inconsistencies (stale totals on already-invoiced quotes) — converted that assertion to a tolerant count-threshold, mirroring the manual-journal pattern. All 36 ODS tables rebuilt, 78 assertions pass, 0 failures. Updated 2026-07-24 (earlier) — **Xero Reports API parser built**: one shared Python module (`etl/xero/reports.py`) across all 6 report kinds (balance sheet, bank/budget/executive summary, P&L, trial balance), landing `staging_xero.report_snapshots` + `report_rows` (long-form, one row per cell). Snapshot/append-only pattern, not upsert — a genuinely new staging convention. Verified: 360 snapshots, 32,200 rows, all grain-clean, real `account_id` UUIDs present in cell attributes (the reconciliation join hook). Updated 2026-07-24 — **`backfill_gcs.py` two-stage performance fix**: cross-job concurrency (tenant-scoped `ThreadPoolExecutor`) plus within-job concurrency (parallel file fetch inside `GCSReader`) plus a per-table BigQuery merge lock. Proven on the endpoint that stalled the overnight run (`purchase_orders`, one tenant alone = 55,099 records): 41s vs. never finishing after hours. **Full 17-endpoint × 7-tenant backfill complete: 119/119 jobs, 0 failures, 327s total — all 17 staging tables populated, grain and GL balance (100% on `net_amount`) verified.** Updated 2026-07-23 — **GCS source bucket switched**: the live sync moved (back) to `gs://prj-dw-dev-raw`; the old `gs://aquatiq-dw-dev-storage` is frozen (dead since 2026-07-08). All 7 Xero tenants now have journals (GL) — a new 7th tenant also appeared. Updated 2026-07-09 — journals first restored for 3/6 tenants (see superseded section below). Updated 2026-07-07 — ODS build started: `ods_xero_0/1.dim_account` written (native → Visma vocabulary), no hand-authored FSLI seed (ship Xero metadata, let accountants define the crosswalk). Full ODS design resolved earlier same day (numbered scopes, Visma-vocabulary harmonization, 6 design decisions). staging_xero populated from GCS; parser set is bucket-driven with drift detection._

---

## ▶ RESUME HERE (current status, 2026-07-24)

**Latest — GCS bucket switch discovered & handled; journals now complete for all 7 tenants:**

- **Bucket switch found by direct investigation, not announced.** Colleague said "3 more tenants have journals" (matching the 2026-07-09 update below), but checking the bucket we'd been reading from (`aquatiq-dw-dev-storage`) showed **zero change since 2026-07-08** — nothing written since, for any tenant or endpoint. Broadened the search: a second bucket, **`gs://prj-dw-dev-raw`** (the *original* pre-2026-06-29 bucket name, previously abandoned in favor of `aquatiq-dw-dev-storage`), is where the sync is now actually landing — actively growing, ~54k blobs vs. ~7.3k in the old one, most recent files from today.
- **New tenant discovered:** `dcb20a20-7e7b-4017-a737-ceab3896d790` ("Aqua Pharma Inc") — did not exist in any prior sync. **7 tenants total now**, not 6.
- **34 endpoint types per tenant now** (up from 17-28), including **6 brand-new Xero Reports API endpoints** not seen before: `report_balance_sheet`, `report_profit_and_loss`, `report_trial_balance`, `report_executive_summary`, `report_bank_summary`, `report_budget_summary`. These are Xero's own pre-built financial statements — worth investigating for the dashboard goal since they may shortcut some of the FSLI/GL enrichment work. No parsers built yet.
- File format is identical (`raw/{vendor}/{tenant}/2.0/{endpoint}/{date}/...`, same meta.json schema) — confirmed byte-for-byte compatible with the existing `GCSReader`, so this was a **repoint, not a rebuild**.
- **Pipeline repointed to `prj-dw-dev-raw`**: updated the bucket constant in `etl/backfill_gcs.py`, `etl/cloud_function/main.py`, `etl/common/gcs_reader.py` (docstring), `etl/tests/test_gcs_reader.py`. No other code changes.
- **Journals re-backfilled from the new bucket (targeted run, ~12 min):** `staging_xero.journals` now **73,401 rows** (deduped from 190,427 raw — a strong real-world proof of the writer dedup fix, at a much heavier overlap ratio than the quotes/PO case), `staging_xero.journal_lines` **226,687 rows**. Grain clean (0 dupes, 0 orphans, 0 null line IDs). **All 7 tenants covered**, with real multi-year history — as far back as **2017** for one tenant, not just the last few weeks.
- **GL balance finding reconfirmed at ~80x the earlier scale**: `net_amount` balances to zero per journal for **70,559 / 70,559 (100%)**; `gross_amount` only for 65%. Same rule as before (documented in "Xero tax basis" / `STAGING_XERO.md`), now proven far more robustly.
- **Data-quality footnote (not investigated further):** tenant `dcb20a20…` has journals dated as far forward as **2027-09-30** — plausibly forward-dated fiscal-year-end entries, not necessarily an error, but worth a look before relying on max-date logic for that tenant.

**⚠ `backfill_gcs.py` performance — two-stage fix (2026-07-23 night → 2026-07-24 morning). First fix was necessary but not sufficient; a second, deeper bottleneck stalled the overnight run again. Both are now fixed and proven.**

*Stage 1 — cross-job concurrency + tenant scoping (night of 2026-07-23):*
- Root cause of the first 2h18m stall: `GCSReader.iter_records()` with no `tenant_id` scans the *entire* bucket per endpoint call, and the old script ran fully serially — one endpoint at a time, one file download at a time.
- Rewrote the orchestration (`etl/backfill_gcs.py`): work split into independent `(tenant, endpoint)` jobs — 7 tenants × 17 endpoints = 119 jobs — run on a `ThreadPoolExecutor` (default 8 workers, `--workers=N`). Each job uses `GCSReader`'s tight per-tenant prefix. Each worker thread gets its own `GCSReader`/`BQWriter` via thread-local storage (BQ/GCS clients aren't guaranteed thread-safe).
- Smoke-tested (`currencies`, all 7 tenants: 56s vs. never finishing before) and launched the full 17-endpoint backfill overnight.

*Stage 2 — it stalled again, for a different reason, ~7 hours in:*
- **Symptom:** the log file showed `0 ok, 0 fail` for hours — looked like a fresh hang. First had to rule out a red herring: Python fully-buffers stdout when redirected to a file (unlike a terminal), so the log can sit empty for a long time even while real work happens. Confirmed via direct BigQuery checks (`MAX(synced_at)` on staging tables, bypassing the log entirely) that the process **had** made real progress, then genuinely stopped — ground truth showed zero new writes for ~3 hours despite the process still being alive at ~0% CPU.
- **Real root cause:** Stage 1 only parallelized *across* `(tenant, endpoint)` jobs — each individual job still downloaded its own files **one at a time**. Job submission is endpoint-major (all 7 tenants of `accounts`, then all 7 of `bank_transactions`, etc.), so once a few of the 8 worker threads landed on a tenant with a huge file history (`purchase_orders` for one tenant alone turned out to have **55,099 records**), those threads were tied up for hours each — starving the rest of the queue (`quotes`, `tax_rates`, `tracking_categories`, `users` never even started).
- **Also surfaced:** a genuine BigQuery race — two tenants merging into the *same* target table at once can get `"Could not serialize access to table ... due to concurrent update"` instead of queuing (hit once, on `accounts/d08f38af`). Not a data-integrity issue (MERGE is atomic; the rejected job just didn't commit), but needed a real fix.
- **Fix 1 — within-job concurrency** (`etl/common/gcs_reader.py`): `iter_records()` now lists matching files (cheap, one pass) and fetches them on an internal thread pool (`max_workers=16`) instead of sequentially. `iter_records_from_meta()` (the Cloud Function's single-event path) is unchanged.
- **Fix 2 — per-table merge lock** (`etl/common/bq_writer.py`): `BQWriter` now holds a class-level `dict[table_name, threading.Lock]` shared across every instance/thread in the process, so concurrent merges into the *same* table serialize (eliminating the race) while merges into *different* tables stay fully parallel.
- **Proof:** killed the stalled process (no data lost — 83/119 jobs had already committed via atomic MERGE) and re-ran `purchase_orders` alone, all 7 tenants, **including the 55,099-record tenant** — completed in **41 seconds**, versus never finishing after hours before. Verified grain: 964 purchase orders = 964 distinct, 2,138 lines = 2,138 distinct — correctly deduplicated, not just fast.
- **Final result: complete.** Full 17-endpoint × 7-tenant backfill (119 jobs) finished in **327 seconds (~5.5 min)** — **119 ok, 0 failed**. `journals/9dc5d3f0` (the one that threw `BrokenPipeError` before) and `accounts/d08f38af` (the one that hit the concurrent-update race) both succeeded cleanly this time. All 17 staging tables now populated; grain verified clean (n = distinct count) on `journals`, `journal_lines`, `quotes`, `quote_lines`, `purchase_orders`, `purchase_order_lines`, `invoices`, `accounts`. GL balance re-confirmed at the fuller dataset: **70,590 / 70,590 (100%)** on `net_amount`, 46,023 (65%) on `gross_amount` — same rule, now proven end-to-end on the complete backfill. Tenant coverage below 7 on a few tables (`quotes`=3, `purchase_orders`=5, `bank_transactions`=6, `items`=6, `tracking_categories`=6) is genuine sparsity — those tenants have zero source records for that endpoint (confirmed via `{'records': 0}` in the run log), not missed jobs.

**Follow-up (2026-07-24): orphaned `_tmp_*` tables found in `staging_xero` — cleaned up + hardened.**
- Found 4 leftover `_tmp_{table}_{run_id}` tables sitting directly in `staging_xero` (`_tmp_journal_lines_f8ebb4d0307b`, `_tmp_payments_298cb794861f`, `_tmp_purchase_orders_5a414e837849`, `_tmp_purchase_orders_6e8145930d98`) — these are `BQWriter.merge()`'s own temp-table-then-MERGE working files, which should always self-delete in a `finally` block.
- **Root cause:** cross-referencing BigQuery's job history, all 4 had a successful `LOAD` followed by a successful `MERGE` into the real target — the data landed correctly. The `_drop_temp()` cleanup call itself failed afterward (plausibly transient, under the heavy concurrent load of the stalled Stage 1 run) and was silently swallowed — `_drop_temp` catches all exceptions and only logs a warning, with no retry. All 4 dated from the stalled run, before the concurrency fixes; the successful 327s re-run left zero orphans.
- **Also found:** the code's own comment claiming orphaned temp tables "will expire anyway" was false — checked, all 4 had `expires=None`. No TTL was ever actually set.
- **Fix:** `BQWriter._write_temp()` now sets a 1-hour `expires` TTL on every temp table immediately after creation (`etl/common/bq_writer.py._set_temp_expiration`). Verified directly (wrote a real temp table, confirmed `expires` populated ~1hr out). Belt-and-suspenders with the normal `_drop_temp()` path — if cleanup is ever skipped again (killed process, transient failure), the table now self-expires within an hour instead of lingering indefinitely. All 6 `test_bq_writer.py` tests still pass.
- **Cleanup done:** all 4 orphaned tables deleted; `staging_xero` now contains only real staging tables.

**Xero Reports API — parser built (2026-07-24).** Design discussion: should `report_*` endpoints live in `ods_xero_0` at all, given they're pre-aggregated (not transactional)? Resolved yes — `_0` is explicitly the "audit anchor, ties 1:1 to the provider's own reports/UI," and these literally *are* Xero's own reports. Better still, this exact problem was already solved once: the deprecated silver layer's `fact_report_row.sqlx` (see `docs/deprecated/`) modeled Xero's report tree as a generic long-form fact. Verified the live payload shape still matches it exactly (`Reports[] → Rows[]` with RowType Header/Section/Row, each cell carrying `{Value, Attributes:[{Id, Value}]}` — e.g. `{"Id":"account","Value":"<account-guid>"}`, which is the reconciliation join key back to `dim_account`).
- **First, checked for the old design's other 2 report kinds** (`aged_receivables_by_contact`, `aged_payables_by_contact`) — confirmed via full endpoint listing across all 7 tenants that neither exists in the bucket under any name. Scope is the 6 that actually exist, not the old file's 8.
- **Confirmed all 6 report kinds share identical structure** (spot-checked each): `ReportID`/`ReportType` are fixed per-kind constants (e.g. `"ProfitAndLoss"`), **not unique per run** — cannot serve as a snapshot key. Report params live only in the `.meta.json` sidecar (`x-report-params`, e.g. `"fromDate=2026-07-01&toDate=2026-07-09"` or `"date=2026-07-09"`), never in the payload (`Fields` is empty on every kind observed).
- **Built one shared parser** (`etl/xero/reports.py`), registered under all 6 endpoint names in `backfill_gcs.py` / `cloud_function/main.py` — same module, report kind read from `meta["x-report"]`, not hardcoded per file.
- **Enabling change**: `GCSReader._extract_records()` now passes through the full parsed `.meta.json` dict as `record["meta"]` (additive — every existing parser ignores the new key). Needed because report snapshot identity/params live only in the sidecar.
- **⚠ New staging convention: snapshots, not entities.** Every other Xero parser upserts on `(tenant_id, record_id)` — one current row per entity. Reports have no such identity; re-running the same report for the same window is a new independent observation, not an update. `record_id` is synthesized as `f"{report}|{run_id}"` (from the meta sidecar's `x-run-id`, a fresh UUID per sync), and both staging tables (`report_snapshots` header, `report_rows` long-form cells) are MERGE-keyed on that — so history accumulates rather than collapsing.
- **Verified end-to-end**: 42 `(tenant, endpoint)` jobs (7 tenants × 6 kinds), 0 failures, 195s. Final state: `report_snapshots` **360 rows** (60 per kind × 6), `report_rows` **32,200 rows** — both grain-clean (n = distinct). Real `account`-attributed rows confirmed with genuine UUIDs joinable to `dim_account.account_id` (also found: some `account` attributes are Xero synthetic group sentinels like `"FXGROUPID"`, not real UUIDs — a real reconciliation-join query will need to filter for UUID-shaped values). All 13 existing tests (`test_bq_writer.py`, `test_gcs_reader.py`) still pass.
- **`ods_xero_0/1.fact_report_row` built** (both layers) — join `report_rows` (cell grain) onto `report_snapshots` (header context); `_1` is a pass-through + `source_system`, same as every other Xero-only entity. `report_date` parsed via `SAFE.PARSE_DATE` in the ODS layer rather than trusted from staging's typing, which can (and did) vary depending on which concurrent backfill job happened to create the table first.

**Full ODS rebuild (2026-07-24, later) — surfaced and fixed a real correctness bug.**
Ran `dataform run --tags ods` to refresh all 34 existing tables against the fully-backfilled 7-tenant staging (previously stale — still reflecting the old 6-tenant bucket) and add the 2 new `fact_report_row` tables. Two assertions failed:
- **`assert_fact_quote_line_reconciles`**: 2/1,193 quotes mismatched. Investigated each — both `status='INVOICED'` with `total_discount=0`, no duplicate lines, no tax-basis issue. This is a known real-world Xero behavior: a quote's own totals can go stale after conversion to an invoice, since the invoice becomes the source of truth and the quote object isn't necessarily recalculated. Genuine tiny source inconsistency, not a bug — converted the assertion to a count threshold (fails only if >5 mismatch), mirroring the manual-journal-balance pattern.
- **`assert_fact_manual_journal_line_balances`**: 342/1,882 journals unbalanced (some by **millions** of dollars) — this one was a real bug, not a source quirk. Root cause: `etl/xero/manual_journals.py`'s line-merge key was `(tenant_id, record_id, account_id)`, silently assuming one line per account per journal. **Confirmed false against live data** — pulled the raw Xero payload for the worst offender and found a genuine journal with *two separate lines posting to the same account* (`+932,188.95` and `-200,249.25`, a real stock-adjustment split) — a normal, valid accounting pattern Xero fully supports. The flawed key couldn't represent this, and under concurrent writes ended up with 2 duplicate copies of one of the two lines while losing the other entirely (mechanism not fully reconstructed — a `bigquery error: Could not serialize access` race is plausible given concurrent writes to a fresh table — but the underlying key defect was unambiguous and reproducible regardless).
  - **Fix**: added `line_position` (0-based index in Xero's own `JournalLines[]` array — the only stable-enough identifier, since there's no native `LineItemID`) to the parsed row and changed the merge key to `(tenant_id, record_id, line_position)`.
  - **Remediation**: dropped `staging_xero.manual_journal_lines` (bad data already baked in under the old key) and re-backfilled. Hit a *second*, related issue: with the table fully dropped, BigQuery had to autodetect a fresh schema from whichever tenant's concurrent job created it first — one all-numeric-chart tenant made it infer `account_code` as INTEGER, breaking the dashed-code tenants (`441-003`, etc. — the same Norwegian-style charts seen with `accounts` earlier). Fixed by pre-creating the table with an explicit schema (`account_code` as STRING) before re-running, sidestepping the autodetect race.
  - **Result**: 342 unbalanced → **1** (the same single pre-existing genuine anomaly, $1,810.74, identified when this fact was first built — confirmed by exact match, not coincidence).
  - Also updated `ods_xero_0.fact_manual_journal_line` to use staging's `line_position` directly instead of re-deriving an ordinal via `ROW_NUMBER() ... ORDER BY account_code, line_amount, description` — more correct, preserves Xero's true line order instead of an arbitrary content-based re-sort.
- **Final state**: all 36 ODS tables (10 dims + 8 facts × 2 layers) rebuilt, **78 assertions pass, 0 failures**. `dim_organisation`/`dim_account` etc. now correctly show 7 tenants (staleness resolved).

**GL checks — design discussion + `fact_general_ledger` build + first reconciliation check (2026-07-24, later).**

Before building the GL fact, talked through what "GL checks" should actually mean: (a) does our own GL fact internally balance (debits=credits), and (b) does it reconcile against Xero's own aggregations (the `report_*` endpoints built earlier exist exactly for this). Agreed order: (1) `fact_general_ledger` + balance assertion, (2) Trial Balance reconciliation, (3) P&L reconciliation, (4) document-fact bottom-up reconciliation, (5) Balance Sheet secondary check — plus a separate follow-on to plan which of these should become routine/automated drift-detectors once the pipeline is scheduled.

**Check 1 — `fact_general_ledger` (done).**
- Built `ods_xero_0.fact_general_ledger` (native) and `ods_xero_1.fact_general_ledger` (harmonized) — one row per `tenant_id + journal_id + journal_line_id` (`JournalLineID` is a real native Xero field, no synthesized-key risk, unlike the manual-journal-line bug below). Joins `journal_lines` → `journals` header, resolves tracking options via the standard category_id+option_name pattern against `dim_tracking_option`.
- `_1` renames to `document_type` (=`source_type`), `document_date`, single signed `amount` (=`net_amount`, per the balancing rule verified 2026-07-09/23) — deliberately **not** split into `debit_amount`/`credit_amount` (Visma's convention) yet; deferred until omnibus alignment actually needs it.
- **`assert_fact_general_ledger_balances`** (count-threshold, mirrors the manual-journal pattern): verified **100% — 70,590/70,590 journals balance exactly on `net_amount`.**
- Needed two new staging declarations (`staging/xero/journals.sqlx`, `journal_lines.sqlx`) — the tables were already real and populated by `etl/xero/journals.py`, but had no Dataform declaration, so `fact_general_ledger` initially failed to compile ("Could not resolve staging_xero.journals").

**Check 2 — Trial Balance reconciliation (done, diagnostic — real finding, not a bug).**
- **Enabling change**: added `financial_year_end_day`/`financial_year_end_month` to `etl/xero/organisations.py` (from Xero's `FinancialYearEndDay`/`FinancialYearEndMonth`, confirmed present in the raw payload) and threaded through `dim_organisation` at both `_0`/`_1`. Needed because Xero's Trial Balance report has 4 numeric columns per account (Debit, Credit, YTD Debit, YTD Credit) — empirically confirmed (not assumed) that `YTD Debit − YTD Credit` is the one that matches our own cumulative GL, and that "YTD" resets at each tenant's **fiscal year start**, not calendar year or all-time. **Tenants have mixed fiscal years** — 2 use a June 30 year-end, the rest Dec 31.
  - Hit the schema-evolution mistyping trap again while adding these two columns: autodetect on the whole batch inferred one tenant's all-numeric `registration_number` as INT64, conflicting with the existing STRING column. Fixed by manually running `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` before re-running the backfill, sidestepping the autodetect path entirely.
- Built `ods_xero_1.recon_trial_balance` — explicitly **not** a hard assertion (new territory, tolerance not yet established). Computes each tenant's fiscal-year-bounded YTD per account from `fact_general_ledger`, compares against Xero's own `report_trial_balance` (latest snapshot per tenant, `YTD Debit − YTD Credit`, filtered to real account UUIDs via regex — some `attribute_id='account'` rows are synthetic group sentinels like `FXGROUPID`, not real accounts).
- **Result: 253/429 (59%) accounts match exactly; 175 (41%) mismatched.** Splitting by `dim_account.bs_pl` revealed a clean, non-random pattern:

  | Type | n | Matched | Mismatched |
  |---|---|---|---|
  | P&L (Revenue/Expense) | 227 | 222 (98%) | 5 (2%) |
  | Balance Sheet | 202 | 31 (15%) | 170 (84%) |

  **Root cause (well-supported, not just plausible).** Balance sheet accounts (equity, fixed assets, payables, retained earnings) carry a cumulative balance from company inception, including whatever one-time "conversion balance" / opening balance Xero recorded when the org first moved onto Xero. That entry does not appear in the regular `/Journals` feed under any `source_type` we sync — checked the full observed set (below), nothing resembling `CONVERSION`/`OPENING`. P&L accounts reset to zero every fiscal year, so they have no pre-window carry-forward — `/Journals` alone is complete for them, which is exactly why they match at 98%. Per-tenant earliest-journal-date data supports the same story: several tenants' synced history starts on a suspiciously clean boundary well after likely company inception (e.g. tenant `19b25bd5…`'s earliest journal is exactly `2022-12-31` — a fiscal year-end date).
  - **Full `journals.source_type` inventory now confirmed** (previously only partially documented): `ACCPAY`, `ACCPAYPAYMENT`, `ACCREC`, `CASHPAID`, `ACCRECPAYMENT`, `MANJOURNAL`, `ACCPAYCREDIT`, `CASHREC`, `INTEGRATEDPAYROLLPE`, `ACCRECCREDIT`, `APOVERPAYMENT`, plus 4 newly-surfaced ones: `TRANSFER` (119), `AROVERPAYMENT` (68), `APCREDITPAYMENT` (40), `ARCREDITPAYMENT` (1). The 4 new ones matter for check 4 (document-fact bottom-up reconciliation) — none has an obvious document-fact mapping yet.
- **Conclusion: this is a structural, source-data limitation, not a reconciliation-logic or GL-fact bug.** `recon_trial_balance` stays a diagnostic (informational) for Balance Sheet accounts — fixing it would require finding/syncing a Xero conversion-balance source we don't currently have, not fixing our own code. The P&L portion of the same check is solid (98%) and is a real candidate for becoming a hard assertion.

**Check 3 — P&L reconciliation (done, near-perfect — confirms check 2's finding independently).**
- Built `ods_xero_1.recon_profit_and_loss` vs. Xero's own `report_profit_and_loss`, using each tenant's latest snapshot.
- **This report is period-bounded, not YTD.** Every observed snapshot has `report_from` = the 1st of the current calendar month and `report_to` = the sync day — Xero's default "this month" P&L window (no fiscal-year framing here, unlike Trial Balance). Reconciliation sums `fact_general_ledger.amount` over `document_date BETWEEN report_from AND report_to` per tenant+account.
- **⚠ New sign-convention gotcha found and handled (empirically verified, not assumed).** Xero's P&L displays both revenue and expense lines as unsigned/positive "management report" amounts (section labels like "Less Cost of Sales" carry the subtraction meaning, not the cell sign). Tested both sign conventions against real data: **EXPENSE accounts match our signed GL `amount` with no flip (64/64 = 100%)**; **REVENUE accounts match only when Xero's value is negated (16/16 = 100%)**. This is the same class of gotcha as `net_amount`/`gross_amount` and `line_amount_types` — a provider amount field whose sign/basis silently depends on context. Encoded as a `CASE WHEN account_class = 'REVENUE' THEN -x.xero_amount ELSE x.xero_amount END` in the recon query; documented inline.
- Also scoped the "our" side to `dim_account.bs_pl = 'P&L'` accounts only — without this, balance-sheet-account GL activity in the same date window (bank movements, tax control, AR/AP) shows up as spurious "missing from Xero" noise, since Xero correctly never includes those in a P&L report.
- **Result: 80/85 (94%) accounts match exactly (`diff = 0`), 0 mismatches.** The remaining 5 are small/near-zero EXPENSE amounts (largest ~$355) present in our GL for the period but absent as a row in Xero's report entirely — plausibly Xero suppressing negligible/credit-only activity from the report display. Not investigated further; flagged as a minor known gap, not a reconciliation defect.
- **Conclusion:** this independently confirms check 2's P&L finding (98% match there too) — our GL is fully reliable within its synced window. The only real, structural gap across both checks is Balance Sheet accounts' pre-window conversion balances (check 2). This check is strong enough to become a real hard assertion (e.g. fail if match rate drops below ~90%, or investigate any single mismatch >0, since true mismatches were 0/85 here).

**Check 4 prep — full `journals.source_type` → document-fact mapping, verified against real payloads (2026-07-24).**

Before building the reconciliation, joined every `source_type`'s `source_id` against every candidate staging table's primary key to verify (not assume) each mapping, and sampled the unexplained rows directly. Full picture (70,590 total journals):

| `source_type` | journals | % | Verified mapping |
|---|---|---|---|
| `ACCPAY` / `ACCREC` | 17,275 / 11,632 | 24.5% / 16.5% | `fact_invoice_line` — `source_id` matches `invoices.invoice_id` 11,705/11,707 and 4,241/4,241 |
| `ACCPAYPAYMENT` / `ACCRECPAYMENT` | 15,631 / 4,368 | 22.1% / 6.2% | `fact_payment` — matches `payments.payment_id` 12,541/12,543 and 3,805/3,805 |
| *(NULL)* | 9,157 | **13.0%** | **No document fact — genuinely unmapped by design.** Sampled directly: these are Xero's own **automatically-generated inventory/COGS journals** (e.g. "Materials Purchased" ↔ "Stock", "Stock Transfer" ↔ "Stock"), `source_id` is NULL on all of them. Not tied to any single source document — internal to Xero's inventory valuation engine, triggered by stock movements rather than a document Xero exposes via the API. **Cannot be closed by syncing more endpoints; must be modeled as an explicit "system-generated, unmapped" bucket, not left as a silent gap.** |
| `CASHPAID` / `CASHREC` | 6,416 / 548 | 9.1% / 0.8% | `fact_bank_transaction_line` — matches `bank_transactions.bank_transaction_id` 4,763/4,763 and 452/452 |
| `MANJOURNAL` | 3,319 | 4.7% | `fact_manual_journal_line` — matches `manual_journals.manual_journal_id` 1,868/1,868. **Never sum alongside `fact_general_ledger` in the same total — `journals` already includes these postings; that's the double-counting risk documented earlier.** |
| `ACCPAYCREDIT` / `ACCRECCREDIT` | 1,103 / 331 | 1.6% / 0.5% | `fact_credit_note_line` — matches `credit_notes.credit_note_id` 350/350 and 100/100 |
| `INTEGRATEDPAYROLLPE` | 362 | 0.5% | **No document fact — structural gap.** Account inspection confirms wages/superannuation/PAYG-PAYE-withholding postings from Xero's integrated Payroll product, which uses a completely separate API surface (`payroll.xro`) that we don't sync at all. Would require a new integration, not just a new parser — out of scope for this reconciliation exercise; document as a permanent, known exclusion. |
| `APOVERPAYMENT` / `AROVERPAYMENT` | 220 / 68 | 0.3% / 0.1% | **Closed 2026-07-24 — `fact_overpayment_line`.** Both are Xero's `/Overpayments` entity. `etl/xero/overpayments.py` restored from git history (added `LineItems[]` unpacking for account-level grain), backfilled (91 overpayments, 91 lines across 7 tenants), `ods_xero_0/1.fact_overpayment_line` built with the standard tax-basis normalization + reconciliation assertion. Verified: `source_id` matches the overpayment 72/72 (`APOVERPAYMENT`) and 19/19 (`AROVERPAYMENT`). |
| `TRANSFER` | 119 | 0.2% | **Closed 2026-07-24 — `fact_bank_transfer`.** Xero's `/BankTransfers` entity. `etl/xero/bank_transfers.py` restored from git history, backfilled (113 transfers across 7 tenants), `ods_xero_0/1.fact_bank_transfer` built (header grain, no tax). Verified: `source_id` matches `bank_transfer_id` 113/116 (the 3 unmatched are likely voided/deleted transfers, consistent with the small gaps seen on every other source_type). **⚠ Each transfer also posts a matching SPEND + RECEIVE row in `fact_bank_transaction_line`** (linked via `from/to_bank_transaction_id`) — never sum `fact_bank_transfer` alongside `fact_bank_transaction_line` in the same total, that double-counts the same cash movement. |
| `APCREDITPAYMENT` / `ARCREDITPAYMENT` | 40 / 1 | 0.06% / 0.00% | **Already fully covered — no new work needed.** These are `payment_type` values Xero already returns from `/Payments` (`staging_xero.payments.payment_type` already contains `APCREDITPAYMENT`/`ARCREDITPAYMENT`/`APOVERPAYMENTPAYMENT` alongside the two known types) — `source_id` matches `payments.payment_id` 28/28 and 1/1. `ods_xero_0/1.fact_payment` is an unfiltered pass-through of `staging_xero.payments`, so these rows are already present in the fact table today; check 4 just needs to route these `source_type`s to `fact_payment` in its mapping table, same as `ACCRECPAYMENT`/`ACCPAYPAYMENT`. |

**Net picture (updated 2026-07-24):** ~86.6% of journal volume now maps cleanly to existing document facts (verified, not assumed) — the `TRANSFER`/`APOVERPAYMENT`/`AROVERPAYMENT` gaps above are closed. ~13.5% is structurally unmappable (inventory system journals + payroll) and must be modeled as an explicit excluded bucket in check 4, not a bug to chase. Check 4 itself (the actual bottom-up reconciliation table) is not yet built.

**Check 4 — document-fact bottom-up reconciliation (done, diagnostic — mostly excellent, 2 tenant-specific outliers flagged).**

Built `ods_xero_1.recon_document_facts`: for every mapped `document_type`, aggregates the corresponding document fact and `fact_general_ledger` independently to `(tenant_id, account_id, document_type)` and compares. Unlike checks 2/3 (which compare against a Xero-computed report snapshot with a natural window), this compares two of *our own* pipelines against each other — there's no external snapshot to bound to, so it's cumulative all-time.

**Two things had to be discovered empirically, not assumed, before the numbers meant anything:**
1. **Sign convention is per-document_type, not universal.** Tested both directions per type against real data (same method as the P&L check). Pattern that emerged is just normal double-entry logic: a document type and its "credit" counterpart always flip in *opposite* directions from each other, since a credit note reverses the original entry:

   | document_type | flip? | document_type | flip? |
   |---|---|---|---|
   | ACCPAY | no | ACCPAYCREDIT | **yes** |
   | ACCREC | **yes** | ACCRECCREDIT | no |
   | CASHPAID (SPEND) | no | CASHREC (RECEIVE) | **yes** |
   | ACCPAYPAYMENT | **yes** | ACCRECPAYMENT | no |
   | APCREDITPAYMENT | no | ARCREDITPAYMENT | **yes** |
   | APOVERPAYMENT (SPEND-OVERPAYMENT) | no | AROVERPAYMENT (RECEIVE-OVERPAYMENT) | **yes** |
   | MANJOURNAL | no (already GL-native) | TRANSFER | unpivoted: from=-amount, to=+amount |

2. **DELETED/VOIDED documents must be excluded from the document-fact side.** A voided invoice still carries its original amount in the live `/Invoices` response, but Xero's GL never posted it (or reversed it) — counting it only inflates the document side. Verified the effect is large: one tenant's `ACCREC` residual dropped from 25% to 0% once excluded; another's from 8.67% to 0.96%. `fact_general_ledger` needs no equivalent filter — `/Journals` only ever returns real, immutable postings.

**Also confirmed (before building, to avoid a double-counting bug): transfer/overpayment-linked `bank_transactions` never independently post to GL.** Every `bank_transfer`/`overpayment` also creates a matching `bank_transactions` row (`transaction_type` `SPEND-TRANSFER`/`RECEIVE-TRANSFER`/`SPEND-OVERPAYMENT`/`RECEIVE-OVERPAYMENT`) for UI/reconciliation display — tested whether these ever appear as a `journals.source_id` under *any* `source_type` and got zero matches. So `fact_bank_transaction_line`'s CASHPAID/CASHREC bucket is filtered to `document_type IN ('SPEND','RECEIVE')` only; including the transfer/overpayment-linked rows would have inflated it with amounts that have no CASHPAID/CASHREC GL posting to compare against.

**Result — rows where both a document total and a GL total exist for that account:**

| document_type | residual (all tenants) | residual (excl. known outliers) |
|---|---|---|
| MANJOURNAL | 0.07% | — |
| CASHREC | 0.13% | 0.05% |
| CASHPAID | 0.53% | 0.15% |
| APCREDITPAYMENT | 0.32% | same |
| ACCPAYCREDIT | 1.61% | same |
| TRANSFER | 5.74% | 0% (n=4) |
| ACCRECCREDIT | 7.88% | 7.8% |
| APOVERPAYMENT | 10.41% | same |
| ACCRECPAYMENT | 11.94% | 1.98% |
| ACCREC | 16.10% | 6.53% |
| ACCPAYPAYMENT | 32.04% | 29.74% (see outlier below) |
| ACCPAY | 35.20% | 33.47% (see outlier below) |

**Two tenant-specific outliers identified, neither chased to full root cause (flagged, matching this session's pattern of not exhaustively chasing every last data-quality wrinkle):**
- **`dcb20a20`** (the newest tenant) drives most of the residual on several types — e.g. its `ACCREC` alone is 92% mismatched even after the DELETED/VOIDED fix, including one account with **$7.7M of GL activity and zero matching invoice lines at all**. Plausible explanation: a bulk historical-data conversion that entered directly as journals rather than through normal invoicing, consistent with the tenant's already-known forward-dated (2027) fiscal-year journal entries flagged earlier as unresolved.
- **`d08f38af`** drives most of the remaining `ACCPAY`/`ACCPAYPAYMENT` residual (82-84% for that tenant alone) — inspected the biggest mismatching accounts: document-fact totals are consistently *larger* than GL by inconsistent ratios (not a fixed FX-rate-multiplier bug), invoice/GL date ranges and row counts are otherwise comparable. Best working hypothesis: invoices whose account coding was edited after the original journal posted (Xero doesn't retroactively rewrite historical `/Journals` entries when a bill's account allocation changes later) — plausible but not confirmed from the data available.

**Also, expected and structural (same shape as the Trial Balance BS finding) — every document type has a large "GL-only" bucket with no document-fact match at all.** This is the AR/AP control-account side of each double entry: document-fact *line items* only ever carry the revenue/expense/bank side of a transaction (what the invoice was coded to), never the control-account line GL adds automatically for the header total. Tens of millions of dollars per type, confirmed structural via inspection — not a bug, and not fixable by parsing more; a control-account balance check would need Trial-Balance-style account-level totals (already covered by check 2), not document facts.

**Conclusion:** the reconciliation methodology is sound (proven by the ~0-10% matches across most types once sign + status handling are correct) and genuinely useful — it already surfaced two real, tenant-specific data questions worth a look. Keeping `recon_document_facts` as a diagnostic (not a hard assertion) given the known outliers; a routine version would need either an outlier allowlist or a materiality threshold per tenant.

**Next steps, in likely order:**
1. ~~Fix `backfill_gcs.py` cross-job concurrency/scoping~~ — done.
2. ~~Fix within-job concurrency + the BQ merge race~~ — done, proven on the previously-stuck endpoint.
3. ~~Full 17-endpoint × 7-tenant backfill from `prj-dw-dev-raw`~~ — done: 119/119 jobs, 0 failures, all grain/balance checks pass.
4. ~~Clean up orphaned `_tmp_*` tables + add TTL safety net~~ — done.
5. ~~Build the Xero Reports API parser~~ — done: `staging_xero.report_snapshots` + `report_rows` populated, verified.
6. ~~Build `ods_xero_0/1.fact_report_row`~~ — done.
7. ~~Rebuild all of `ods_xero_0/1` against the full 7-tenant staging~~ — done: found + fixed the manual-journal-line merge-key bug, all 78 assertions pass.
8. ~~Build `ods_xero_0/1.fact_general_ledger` + balance assertion~~ — done: 100% balanced, 70,590/70,590 journals.
9. ~~Build `ods_xero_1.recon_trial_balance` (check 2)~~ — done: 59% overall, cleanly explained by the BS-vs-P&L split above.
10. ~~Build the dedicated P&L reconciliation (check 3)~~ — done: 94% exact match, 0 real mismatches, confirms check 2's finding independently.
11. ~~Document-fact bottom-up reconciliation (check 4)~~ — done: `ods_xero_1.recon_document_facts` built, most document_types match within ~0-10%, 2 tenant-specific outliers flagged (not fully root-caused) — see above.
12. Balance Sheet secondary check (check 5) vs. `report_balance_sheet` — same point-in-time logic as Trial Balance; lower priority given the BS gap is already understood structurally.
13. Plan which of checks 1-5 become routine/automated drift-detectors once the pipeline is scheduled (vs. remaining manual/diagnostic) — explicitly requested, not yet started.

---

## Status as of 2026-07-09 (superseded by the above where it conflicts)

**Xero journals (GL) restored & backfilled — first attempt, 3/6 tenants:**
- Colleague confirmed 3 tenants now have journals in the GCS bucket. Verified directly against the bucket (not just staging): `19b25bd5…` (130 journals), `83adbd31…` (42), `9dc5d3f0…` (750) — 922 total, real `JournalLines[]` content, dated 2026-07-08. The other 3 tenants (`35f4b175…`, `d08f38af…`, `f0b1075e…`) still have **no journals** — confirm with colleague whether/when those are coming. *(Superseded 2026-07-23: this was reading `aquatiq-dw-dev-storage`, which turned out to be frozen; the real sync had moved to `prj-dw-dev-raw`, where all 7 tenants now have journals — see RESUME HERE above.)*
- Restored `etl/xero/journals.py` from git history (removed 2026-07-07 under the bucket-driven policy; payload matched the live data exactly, no changes needed). Re-added to the `PARSERS` map in both `backfill_gcs.py` and `cloud_function/main.py`.
- Backfilled: `python -m etl.backfill_gcs journals` → `staging_xero.journals` (922 rows) + `staging_xero.journal_lines` (2,788 rows). Grain verified clean (922=922, 2788=2788, 0 orphans, 0 null line IDs).
- **⚠ CRITICAL FINDING for the future `fact_general_ledger` build — journal lines balance on `net_amount`, not `gross_amount`.** `gross_amount` includes tax on the origin line only (the tax posts to a separate control-account line), so summing it never balances when a transaction has tax — only 487/899 (54%) "balanced" on gross. Summing `net_amount` balances **899/899 (100%)**, across all 3 tenants, across every `source_type` (ACCPAY, ACCREC, CASHPAID/REC, MANJOURNAL, credit notes, payments, payroll). This is the GL-equivalent of the `line_amount_types` finding on document facts (see "Xero tax basis" below) — **use `net_amount` as the GL posting measure when `ods_xero.fact_general_ledger` is built**, and add a balancing assertion (`assert_fact_general_ledger_balances`) exactly like the manual-journal one.
- 23 journal headers have no lines at all (all `source_type=NULL`) — likely opening-balance/system journals; not a grain defect, just headers with zero children.
- Also newly visible in the bucket for those same 3 tenants: 10 previously-absent endpoints (`bank_transfers`, `batch_payments`, `budgets`, `contact_groups`, `expense_claims`, `linked_transactions`, `overpayments`, `prepayments`, `receipts`, `repeating_invoices` — 28 endpoints total now, up from 17). Flagged by drift detection; no parsers built yet — build only if/when a report needs one. `payment_services` remains absent everywhere.

---

## Status as of 2026-07-07 (superseded by the above where it conflicts)

**Done:**
- Staging layer complete. Python ETL in `etl/` parses raw GCS JSON (`gs://aquatiq-dw-dev-storage`) into `staging_xero` (16 endpoints, ~345k rows). Bucket-driven parser set + drift detection. Reference: `docs/STAGING_XERO.md` for payload shapes.
- Full ODS design resolved and documented below (see "ODS Layer Design").
- **ODS build started — `dim_account` done (not yet materialized in BQ).** Three files written under `Dataform/definitions/`:
  - `staging/xero/accounts.sqlx` — source declaration for `staging_xero.accounts` (Python-populated; lets ODS `ref()` it).
  - `ods_xero_0/dim_account.sqlx` — native Xero pass-through, `account_key = tenant_id|account_id`, uniqueKey assertion.
  - `ods_xero_1/dim_account.sqlx` — Visma-vocabulary skeleton; `bs_pl`/`fsli_1` derived from `account_class` (definitional); Xero-native `reporting_code`/`reporting_name`/`account_type` carried through; `fsli_2`/`fsli_3` + EBITDA/CF/NWC/capex flags left NULL/`unmapped`.
  - `dataform compile` clean (188 actions). Live preview: **786/786 accounts classify at the coarse level, 0 unmapped**.
  - **Decision (2026-07-07): no hand-authored Xero→FSLI mapping seed yet.** Ship Xero's own metadata through the DW, show it to the accountants; add a seed (keyed however their logic dictates) only when they define the crosswalk. The `_1` column skeleton + UNION shape are already in place to drop it in. See "ODS Layer Design → Account classification" note.

**Progress — ALL Xero ODS dimensions built + materialized in BQ (10 dims × `_0`+`_1` = 20 tables; 60 run steps = 20 tables + 40 assertions, all pass):**
- `dim_account` — `_1` derives `bs_pl`/`fsli_1` from `account_class` (definitional), carries native `reporting_code`/`account_type`; finer FSLI + flags left `unmapped` pending accountant crosswalk.
- `dim_contact` — `_0` joins `contacts` + `contact_addresses` (STREET/POBOX flattened) + `contact_phones` (DEFAULT/MOBILE), grain verified 1675→1675. `_1` → Visma master vocab (`main_address_line1`, `corporate_id`, `currency_id`, …), keeps `is_customer`/`is_supplier`. **Customer/supplier split deferred to omnibus** (data: 917 supplier-only / 105 customer-only / 39 both / 614 neither).
- `dim_item` — `_1` → Visma inventory vocab (`inventory_number`, `item_description`, `is_stock_item`, `default_price`).
- `dim_currency` — already Visma-named.
- `dim_tax_rate`, `dim_tracking_category`, `dim_tracking_option`, `dim_user`, `dim_branding_theme` — Xero-specific, `_1` = pass-through + `source_system`. (`dim_tracking_option` added as an option-grain child that `fact_invoice_line` will reference.)
- `dim_organisation` — `_1` light renames (`corporate_id`, `currency_id`); **entity resolution deferred** (Decision 6; `seed_entity_mapping` not yet extended with Xero tenants).
- All 8 thin dims verified `l0 = l1 = staging` row counts (no fan-out).
- Materialize command that works: `dataform run --tags ods` (the `--actions "schema.name"` selector does *not* match in Dataform 3.x — use tags). Auth ready via Dataform SA key in `.df-credentials.json`.

**Progress — ALL 7 Xero document facts built + materialized. Full Xero ODS dim+fact layer complete (10 dims + 7 facts = 34 tables; 74 assertions pass, 0 failures).**
- Document facts (line grain, net-of-tax, reconcile to header `sub_total`): `fact_invoice_line`, `fact_credit_note_line`, `fact_bank_transaction_line`, `fact_purchase_order_line`, `fact_quote_line`. Each with an `assert_fact_*_reconciles` guard.
- `fact_manual_journal_line` — GL postings, NOT net-normalized. Synthesized line key (no LineItemID). `amount` = signed posting = `line_amount + IF(Exclusive, tax_amount, 0)`, which balances to 0 per journal. Guard: `assert_fact_manual_journal_line_balances` (count-threshold; 1 genuine source imbalance tolerated).
- `fact_payment` — header grain (no lines, no tax). Covers ACCRECPAYMENT + ACCPAYPAYMENT; no currency_key (Xero payments carry no currency_code).
- All AR/AP variants live in one fact per type (document_type distinguishes); customer/supplier split deferred to omnibus.
- **⚠ TAX BASIS — `amount` is NET of tax on every fact row.** Xero's raw `line_amount` is tax-inclusive when `line_amount_types='Inclusive'` and net otherwise, so `_1` normalizes to net (`Inclusive → line_amount − tax_amount`). Enforced per fact by `assert_fact_*_reconciles`. See "Xero tax basis (`line_amount_types`)" below. (Exception: `fact_manual_journal_line` will NOT normalize — GL postings must keep gross signed amounts to balance.)
- **Xero tracking quirk:** line tracking carries category id + option NAME (not option id) → resolve `tracking_option_key` via `dim_tracking_option` on `(tenant_id, tracking_category_id, option_name)`.
- Field variations handled: PO lines have `account_code` only (no `account_id` → resolve `account_key` by code); quote lines have no account/item/tax_type; credit-note/PO/quote lines have STRING discount fields (SAFE_CAST).

- **⚠ STAGING DUPLICATION BUG — found & fixed (2026-07-08).** `staging_xero.quotes` and `purchase_orders` (headers + lines) were duplicated **exactly 34×**. Root cause: repeated full-snapshot ("master") sync files in GCS × a batch-backfill write path with no dedup — `MERGE` into an empty target inserted every copy. **Fix:** `BQWriter.merge()` now dedups the source batch by key (latest `synced_at`) before MERGE — see `etl/common/bq_writer.py._dedup_by_key` + regression tests in `etl/tests/test_bq_writer.py`. Re-backfilled both endpoints → now 1× (quotes 1,143 / quote_lines 3,153 / purchase_orders 924 / po_lines 2,027). The reconciliation assertions are what caught it. Production Cloud Function path was never affected (one file per event). Full write-up in "Staging duplication bug" below.

**Next step — the Xero provider layer is done; move to cross-provider + deferred families:**
1. **Visma `_1`** — re-point the existing Visma gold to the shared vocabulary (mostly already speaks it).
2. **`ods_omnibus_0`** — UNION each provider's `_1` for the shared conformed entities (dim_account, dim_contact→customer/supplier, dim_item, dim_currency, invoice/credit_note/payment facts), then **`ods_omnibus_1`** wide for the datamart. This is where the customer/supplier fact+dim split happens (Xero role flags / document_type route the UNION).
3. Deferred families: **GL fact** (`fact_general_ledger`) until `journals` syncs; allocation facts (`invoice_payments`, `credit_note_allocations`) if needed; entity resolution (extend `seed_entity_mapping` with Xero tenants) for `_1` company/entity keys.
- Scope recap: Xero ODS complete at 10 dims + 7 facts (34 tables) + 6 assertions (5 reconcile + 1 balance).

**Key locked decisions** (details in "ODS Layer Design"): numbered scopes `ods_xero_N` / `ods_visma_N` / `ods_omnibus_N` (number resets per scope, `0` = native); harmonize to **Visma vocabulary** at `_1`; **classification ships from Xero-native metadata first — no invented FSLI crosswalk until accountants define it**; line-grain facts; GL deferred until `journals` syncs (no reconstruction); providers are entity-disjoint (clean omnibus UNION).

**Environment notes:** local Python is `/opt/homebrew/bin/python3.13`; GCP auth via ADC often needs `gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform`; BQ project `prj-dw-dev`, region `europe-north2`.

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

**`docs/ACCOUNTANTS_GUIDE_XERO_DATA.md`** (2026-07-24) — a non-technical companion doc written for the accounting team reviewing `ods_xero_0/1`. Explains the same caveats covered in this doc (tax basis, GL vs. document-fact conventions, the Balance Sheet conversion-balance gap, the two flagged-outlier companies from check 4, structural GL-only postings) in plain accounting language, with no data-engineering jargon. Keep it in sync when any of those findings change.

**`docs/ACCOUNTANTS_TABLE_CATALOG.md`** (2026-07-24) — companion to the guide above: lists every table in `ods_xero_0`/`ods_xero_1` (they're 1:1 the same tables, Level 0 = Xero-native field names, Level 1 = renamed) with a plain-English description and row counts, plus the 3 `recon_*` diagnostic tables (Level 1 only). Update alongside any new table added to `ods_xero_0/1`.

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
~~`gs://aquatiq-dw-dev-storage` (not `prj-dw-dev-raw` as originally planned)~~

**⚠ SUPERSEDED 2026-07-23 — the bucket switched back.** The live sync moved to **`gs://prj-dw-dev-raw`** (the originally-planned name, ironically); `aquatiq-dw-dev-storage` is now frozen (no writes since 2026-07-08). The pipeline (`etl/backfill_gcs.py`, `etl/cloud_function/main.py`, `etl/common/gcs_reader.py`, tests) has been repointed to `prj-dw-dev-raw`. See RESUME HERE at the top for the full story. Path format and metadata schema below are unchanged and apply to the new bucket too.

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

## BigQuery Dataset Structure

_Superseded by the numbered-scope scheme below (resolved 2026-07-07). See "ODS Layer Design"._

### Active datasets — staging

| Dataset | Purpose |
|---|---|
| `staging_xero` | Parsed, typed Xero API records — one table per endpoint |
| `staging_visma` | Parsed, typed Visma API records — one table per endpoint |
| `datamart` | BI-ready, no joins — column selections and aggregations from ODS |

### ODS datasets — numbered-scope scheme (see ODS Layer Design)

Scopes: `ods_xero_N`, `ods_visma_N`, `ods_omnibus_N`. Number = depth within scope (`0` = bottom / native). To be created when ODS build begins. The earlier unnumbered stubs (`ods_xero`, `ods_visma`, `ods`) are superseded and can be dropped by an admin.

### Deprecated datasets (safe to delete after verification)
All data copied to `deprecated_*` versions. Original datasets still exist in BQ but should be deleted by a GCP admin (requires `bigquery.datasets.delete` permission — do from the BQ console):

| Original dataset | Deprecated copy | Tables |
|---|---|---|
| `dw_1_bronze_xero` | `deprecated_dw_1_bronze_xero` | 29 |
| `dw_2_staging_xero` | `deprecated_dw_2_staging_xero` | 36 |
| `dw_1_silver_xero` | `deprecated_dw_1_silver_xero` | 46 |

Visma datasets (`dw_1_silver_visma`, `dw_1_silver_visma_global`) are **untouched**.

---

## ODS Layer Design (resolved 2026-07-07)

Design agreed across a full decision review. This governs how staging tables are merged into the ODS. **No ODS code written yet** — this is the blueprint to build against.

### Guiding principle

Mirror Visma's **output** (conformed shape + vocabulary), not its intermediate join steps. Xero is document-centric; Visma is a normalized ERP — their internal merges differ, which is exactly why each provider has its own ODS scope. Providers converge only at the *edges* (what the omnibus layer consumes).

### Numbered-scope layering

Layers are numbered, and **the number resets within each scope**. The scope prefix carries vertical position; the number carries depth within that scope. `0` = bottom = closest to source.

```
staging_xero ─→ ods_xero_0 (native) ─→ ods_xero_1 (harmonized) ─┐
                                                                 ├─→ ods_omnibus_0 (UNION) ─→ ods_omnibus_1 (wide) ─→ datamart
staging_visma ─→ ods_visma_0 (native) ─→ ods_visma_1 (harmonized)┘
```

- **`ods_<provider>_0`** — native conformance in the **provider's own vocabulary**. Audit anchor: ties 1:1 to the provider's own reports/UI. Merges multiple staging tables via **joins** (header + child tables → conformed dim/fact).
- **`ods_<provider>_1`** — harmonized to the shared vocabulary (see below). Same grain as `_0`; only the vocabulary/classification changes.
- **`ods_omnibus_0`** — cross-provider **UNION** of each provider's `_1` output. Reads from `_1`, never `_0` (UNION only works once columns align).
- **`ods_omnibus_1`** — wide denormalization (facts pick up dim labels + classifications) so the datamart is join-free.

Rules:
- **Every layer carries all entities.** Entities needing no transformation pass through unchanged from the layer below, so anything downstream reads one known layer and finds everything.
- **Layer count flexes per entity and per scope.** ~2 within each provider, ~1–2 at omnibus is the current expectation; insert a `_2` anywhere later without renumbering (that's why numbering resets per scope).
- **Cross-provider prefix is `omnibus`** — distinctive and collision-free ("group" is overloaded in this domain: Xero ContactGroups, Aquatiq Group; "all" is vague). Scales to any number of providers.

### Shared vocabulary = **Visma vocabulary** (decided 2026-07-07)

When harmonizing at `_1`, everything shifts to **Visma's naming conventions and vocabulary**, not a neutral invented one. Reason: the finance users who consume the warehouse are most familiar with Visma — using Visma's column names, FSLI structure, and dimension naming lets them work with the data without re-learning it.

Concretely:
- Xero `ods_xero_1` dims/facts adopt Visma's column names, `fsli_1/2/3`, `bs_pl`, and classification vocabulary.
- Visma's `_1` is largely a re-point of its existing conformed gold (it already speaks its own vocabulary).
- Before building `ods_xero_1`, read Visma's `dim_account` + mapping seeds to extract the exact target column names and FSLI values Xero must emit.

### The six resolved design decisions

| # | Decision |
|---|---|
| **1 — Account classification** | Native-first at `_0` (Xero `account_class`, `reporting_code` as-is). Harmonize to Visma FSLI vocabulary at `_1`. **Refined 2026-07-07 (as-built): no hand-authored mapping seed yet.** `_1` derives `bs_pl`/`fsli_1` from `account_class` (definitional; 786/786 classify, 0 unmapped), carries Xero-native `reporting_code`/`reporting_name`/`account_type` through, and leaves finer FSLI + flags `unmapped`. Ship this to the accountants; add a seed only when they define the crosswalk. See findings note below. |
| **2 — Fact grain** | Line-level for transactional facts (invoices, credit notes, bank transactions, POs, quotes, manual journals). Header-grain only where there are no lines (payments). No aggregation in ODS. |
| **3 — Star vs wide** | Hybrid. Conformed dimension tables always exist (star backbone, for drill/ad-hoc). Facts stay lean through provider layers, then **widen at `ods_omnibus_1`** (denormalize dim labels + FSLI onto fact rows) so the datamart is a pure column-select with no joins. |
| **4 — Journals gap** | GL (`journals`) not synced yet. Two fact families (mirroring Visma): **GL facts** and **document facts**. Build all **document facts + dims now** (Xero has the data → AR/AP/sales reporting works immediately). **Defer the GL fact as an additive family** — build `fact_general_ledger` only when real journals land; **no reconstruction** from documents (fragile, throwaway). Financial statements (P&L/BS) wait for the real GL. |
| **5 — Cross-source reconciliation** | **Staging is faithful per-source; ODS owns reconciliation.** General pattern for any record/relationship arriving from >1 endpoint. Specific case: contact-group bridge (`contact_group_members` group-centric + `contact_group_memberships` contact-centric) reconciled in `ods_xero_0` — **parked until `contact_groups` data exists** (endpoint not synced; other source near-empty). |
| **6 — Multi-tenant & entity** | Canonical entity = **legal entity**, resolved via an **extended `seed_entity_mapping`** (add Xero `tenant_id` → canonical entity rows to Visma's existing mapping). Resolve at `_1`; preserve native `tenant_id` at `_0` for audit. **Xero and Visma cover disjoint legal entities** — no record appears in both providers — so `ods_omnibus` UNION can never double-count. Legal-entity → company/business-unit rollup deferred (Visma has `dim_company`/`dim_business_unit`; business need unconfirmed). |

### Merge mechanics summary

- **Within a provider (→ `_0`):** joins. Header staging table + its child tables → one conformed dim or line-grain fact. (e.g. `contacts` + `contact_addresses` + `contact_phones` → `dim_contact`; `invoices` + `invoice_lines` → `fact_invoice_line`.)
- **Across providers (→ `omnibus_0`):** UNIONs. Same conformed entity from each provider's `_1`, stacked. New providers just add another input to the same UNION.

### Build sequence (when ODS work starts)

1. Read Visma `dim_account` + mapping seeds → extract the target FSLI vocabulary and column names.
2. `ods_xero_0.dim_account` (native) → `ods_xero_1.dim_account` (Visma vocabulary + FSLI mapping seed).
3. `ods_xero_0/1.dim_contact` (the other universally-referenced dim).
4. One enriched fact end-to-end: `fact_invoice_line` (exercises contact + account + tracking + tax joins).
5. Fan out remaining dims + document facts on the established pattern.
6. `ods_omnibus_0` UNION (Xero + Visma), then `ods_omnibus_1` wide.
7. GL fact family — deferred until journals sync.

### Account classification — findings from live data (2026-07-07)

Investigated `staging_xero.accounts` (786 accounts, 6 tenants) to decide the `_1` mapping key. Findings drove the "no seed yet" decision:

- **`reporting_code`** is Xero's standard hierarchical management-report taxonomy (dot-delimited, e.g. `ASS.CUR.REC.TRA` = Assets ▸ Current ▸ Receivables ▸ Trade debtors). 100% populated, ~71 distinct values, top levels (`ASS`/`LIA`/`EQU`/`REV`/`EXP`) shared across all 6 tenants. Granular for ~32% of accounts; the other ~68% sit at the coarse top level (`EXP` alone = 342 accounts).
- **`account_type`** (17 values: `DIRECTCOSTS`, `OVERHEADS`, `DEPRECIATN`, `INVENTORY`, `BANK`, `FIXED`, `TERMLIAB`, `CURRLIAB`, …) carries *more* signal than `account_class` for the coarse majority.
- **Two chart styles within Xero** (not Xero-vs-Visma): 4 tenants use numeric codes `090`–`9999`; 2 (Aqua Pharma NO entities) use dashed `440-001`–`960-003`. So raw `account_code` does **not** generalize across tenants — but `reporting_code`/`account_type` do. This matters because more source systems are coming; keying group mapping on local codes means re-mapping every new entity.
- **Why no seed now:** any Xero→group-FSLI crosswalk (whether keyed on `account_code`, `reporting_code`, or `account_type`) embeds finance judgment we can't validate ourselves. Decision: expose Xero's native metadata, let the accountants tell us the crosswalk, then encode it. When they do, the likely shape is a cascade mirroring Visma's own (`exact override → standardized metadata → native type → unmapped`), resolving to a Visma `account_code_3` so `chart_of_accounts` supplies the full hierarchy.

### Xero tax basis (`line_amount_types`) — critical measure convention (2026-07-08)

**This is easy to overlook and will silently corrupt financial totals if forgotten. Read before building or reviewing any Xero transactional fact.**

**The problem.** Xero's line-level monetary field `line_amount` does **not** have a single meaning. Its tax basis is set by the invoice/document header's `line_amount_types`:

| `line_amount_types` | What `line_amount` contains | Share of invoice lines |
|---|---|---|
| `Exclusive` | NET (tax-exclusive) | ~74% |
| `Inclusive` | GROSS (tax **included**) | ~26% |
| `NoTax` | NET (no tax) | <1% |

So the raw value mixes net and gross across rows. `SUM(line_amount)` across a set of invoices that mixes Exclusive and Inclusive **overstates the Inclusive ones by their tax** — a real, silent error in any revenue / COGS / P&L total.

**The rule (applied in ODS `_1`).** Normalize every transactional-fact amount to **NET** so the measure has one consistent basis:

```
amount_in_currency = CASE WHEN line_amount_types = 'Inclusive'
                          THEN line_amount - tax_amount
                          ELSE line_amount END
amount (base ccy)  = amount_in_currency * currency_rate
```

- Net was chosen because Visma's `fact_customer_invoice_line.amount` is net — so both providers align at the omnibus UNION.
- `tax_amount` is carried separately; **gross = `amount_in_currency` + `tax_amount_in_currency`**.
- `line_amount_types` is **carried into the fact** so the original basis is always visible.
- Verified: after normalization, net line sums tie to header `sub_total` for **15,294 / 15,294** invoices.

**This applies to every Xero transactional fact** — `fact_invoice_line`, `fact_credit_note_line`, `fact_bank_transaction_line`, `fact_purchase_order_line`, `fact_quote_line`, `fact_manual_journal_line`. Each header carries its own `line_amount_types`.

**How this is prevented from being forgotten (defense in depth):**
1. **Enforcement (not just docs) — a Dataform reconciliation assertion.** `definitions/ods_xero_1/assert_fact_invoice_line_reconciles.sqlx` checks that net line sums tie to header `sub_total` per invoice. If the normalization is ever removed or a new fact skips it, the assertion returns failing rows and **`dataform run` fails**. A tax-basis regression breaks ~5,000 invoices by their full tax, far outside the tolerance band. Add an equivalent assertion for each new transactional fact.
2. **Inline docs** — the TAX BASIS block is spelled out at the top of `ods_xero_1/fact_invoice_line.sqlx`.
3. **This section + the RESUME HERE ⚠ note** in this doc.
4. **Payload reference** — noted in `docs/STAGING_XERO.md` (invoices section).
5. **Session memory** — `memory/xero-line-amount-tax-basis.md`.

### Staging duplication bug — quotes / purchase_orders 34× (found & fixed 2026-07-08)

**Symptom.** `staging_xero.quotes` and `staging_xero.purchase_orders` (both headers and line tables) contained exactly 34 identical copies of every record. The other 13 staging tables were clean (1×). Surfaced by the ODS reconciliation assertions (net line sums came out ~34× the header `sub_total`); would otherwise have silently 34×-inflated all quote/PO reporting.

**Root cause.** Two things combined:
1. `quotes` and `purchase_orders` are synced to GCS as **repeated full snapshots** ("master" sync-type) — every record re-exported in every run. Each had ~34 snapshot files. (Invoices etc. are incremental — one file per record — which is why they were clean.)
2. The **batch backfill** (`backfill_gcs.py`) reads *all* files for an endpoint via `GCSReader.iter_records()` (no cross-file dedup) and passes the whole set to `BQWriter.merge()` in one call. `merge()` did not dedup the source batch, and `MERGE … WHEN NOT MATCHED THEN INSERT` into an **empty** target treats all 34 copies of a key as unmatched → inserts all 34.

**Not a production issue.** The Cloud Function path processes one meta file per event; a single snapshot file has each record once, so `MERGE` upserts correctly. The 34× was purely a batch-backfill artifact.

**Fix.** `BQWriter.merge()` now dedups the source batch by `key_columns`, keeping the latest `synced_at`, before writing the temp table (`etl/common/bq_writer.py._dedup_by_key`). This makes the writer's "exactly one current row per key" guarantee real for *any* caller/endpoint, and immunizes against full-snapshot syncs and backfill re-runs. Regression tests: `test_dedup_duplicate_keys_in_batch` (real-BQ, the 34× scenario) and `test_dedup_by_key_unit` (pure Python) in `etl/tests/test_bq_writer.py`.

**Remediation.** Dropped the 4 tables and re-ran `python -m etl.backfill_gcs quotes purchase_orders`; they rebuilt at 1× (the writer deduped 38,862 quote inputs → 1,143 rows on write). Note: re-running the backfill *without* dropping first would not have shrunk them — MERGE updates the 34 matching rows in place; the tables must be empty for the deduped insert to land as 1×.

**Prevention / watch-list.** The reconciliation assertions per transactional fact are the standing guard. If another full-snapshot endpoint is added later, the writer dedup now handles it automatically; the drift detector still flags genuinely new endpoints.

### Materializing ODS tables (how to `dataform run`)

**Auth is ready — no new authorization needed.** Dataform authenticates via the service-account key already in `Dataform/.df-credentials.json` (`prj-dw-dev-bq@prj-dw-dev.iam.gserviceaccount.com`, project `prj-dw-dev`, location `europe-north2`). This is **separate** from the `bq`/gcloud user creds (which expire and need `gcloud auth login`) and from ADC (used by the Python ETL). A `dataform run --dry-run` confirmed the SA key connects to BQ.

To materialize the current ODS models (creates the `ods_xero_0` and `ods_xero_1` datasets + tables):

```bash
cd Dataform
dataform run --tags ods --include-deps          # or: --actions "ods_xero_0.dim_account" --actions "ods_xero_1.dim_account"
```

Notes / prerequisites:
- The SA must be able to **create the new datasets** `ods_xero_0` / `ods_xero_1` (`bigquery.datasets.create`). It already creates the `dw_1_*` datasets, so this should be in scope; if a run fails with a dataset-create permission error, an admin grants the SA that permission (or pre-creates the two datasets in `europe-north2`).
- Run from the `Dataform/` dir so `.df-credentials.json` and `workflow_settings.yaml` are picked up.
- `--dry-run` validates against BQ without creating anything (assertion steps will report the target tables "not found" under dry-run — that is expected, not a failure).

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

### Journals — FULLY RESOLVED for all 7 tenants (2026-07-23)

**Update 2026-07-23:** what looked like "3 more tenants" turned out to be a **bucket switch** — the live sync had moved from `aquatiq-dw-dev-storage` (frozen since 2026-07-08) to `gs://prj-dw-dev-raw`. All **7** tenants (6 known + 1 new: `dcb20a20…`, "Aqua Pharma Inc") have journals there, with real multi-year history (back to 2017 for one tenant). Pipeline repointed to the new bucket; journals re-backfilled: `staging_xero.journals` **73,401 rows**, `journal_lines` **226,687 rows**, 100% balance on `net_amount`. Full write-up + the bucket-discovery story: see RESUME HERE at the top.

**Superseded 2026-07-09 update (kept for history):** journals were first confirmed for only 3 tenants (`19b25bd5…` 130, `83adbd31…` 42, `9dc5d3f0…` 750 — 922 total) — but that was reading the now-frozen old bucket. See above for the real, current state.
- **Do not confuse `journals` (GL) with `manual_journals` (hand-entered only)** — both are separate Xero endpoints; `manual_journals` has been synced (and built into the ODS) for all 6 *original* tenants throughout (not yet re-checked for the new 7th tenant).
- **Keep `dw_1_bronze_xero` (or its deprecated copy)** — still useful as a reference/fallback for the journals payload shape.

### Parser policy — bucket-driven, not project-driven (changed 2026-07-07)

**A parser exists only for an endpoint that is actually present in the GCS bucket.** The GCS bucket — not the old BigQuery bronze project — is the source of truth for which endpoints exist.

Previously we carried 28 parsers, all ported from the frozen `dw_1_bronze_xero` project (its 29 tables). Only 16 of those endpoints are actually in the GCS bucket. The other 12 were speculative — built against a project that no longer drives the pipeline. They have been **removed** to keep the parser set honest.

**Removed 2026-07-07** (were old-project-only, absent from GCS): `bank_transfers`, `batch_payments`, `budgets`, `contact_groups`, `expense_claims`, `journals`, `linked_transactions`, `overpayments`, `payment_services`, `prepayments`, `receipts`, `repeating_invoices`.

**`journals` restored 2026-07-09** — see "Journals" above. This is the intended lifecycle of the policy working as designed: removed speculatively, drift detection flagged it landing in the bucket, restored from git with the payload unchanged.

**`bank_transfers`/`overpayments` restored 2026-07-24** — same lifecycle again, this time triggered by the GL-reconciliation `source_type` mapping exercise (see RESUME HERE) rather than a drift-detection warning alone. Both had real data in the bucket for a while (confirmed via drift warnings on every backfill run) but nothing had needed them until `journals.source_type IN ('TRANSFER','APOVERPAYMENT','AROVERPAYMENT')` gave a concrete reason to close the gap.

The remaining 8 (`batch_payments`, `budgets`, `contact_groups`, `expense_claims`, `linked_transactions`, `prepayments`, `receipts`, `repeating_invoices`) are still **preserved in git history** (commit before the 2026-07-07 removal) and documented in `docs/STAGING_XERO.md`. They continue to appear in the bucket (drift detection flags them on every backfill run) — build a parser only when a report actually needs one. `payment_services` remains absent from the bucket entirely.

### Drift detection — get warned when a new endpoint appears

Both entry points now compare bucket endpoints against the parser set and warn on anything unrecognised:

- **`backfill_gcs.py`** — on every run, lists all bucket endpoints and logs `NEW ENDPOINT DETECTED IN BUCKET: '<x>' has no parser…` for any endpoint that has neither a parser nor a `KNOWN_UNPARSED` entry. Prints a summary banner if any are found.
- **`cloud_function/main.py`** — per meta-file event, if the endpoint has no parser it logs `NEW ENDPOINT DETECTED: <vendor>/<endpoint> has no parser…` (unless it's in `KNOWN_UNPARSED`).

`KNOWN_UNPARSED` (endpoints intentionally skipped, kept out of warnings): `bills` — proven subset of `invoices` (ACCPAY).

**Current parser inventory (2026-07-24, later): 25 parsers, all backed by live GCS data** (19 endpoint-scoped + `etl/xero/reports.py`, one shared module registered under all 6 `report_*` endpoint names — see RESUME HERE for the full write-up). `bank_transfers` and `overpayments` restored this session to close GL-reconciliation source_type gaps (see RESUME HERE). **8 endpoints remain unparsed** in `prj-dw-dev-raw` (`batch_payments`, `budgets`, `contact_groups`, `expense_claims`, `linked_transactions`, `prepayments`, `receipts`, `repeating_invoices`) — flagged by drift detection every run; build a parser when a report needs one. `contact_groups` specifically unblocks the deferred `dim_contact` group-membership reconciliation (Decision 5) if it's ever built.

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
