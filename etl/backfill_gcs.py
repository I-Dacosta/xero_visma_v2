"""
GCS → staging backfill.

Reads all raw JSON files for each Xero endpoint from the GCS bucket and writes
parsed, typed rows into the staging_xero BigQuery dataset via the entity parsers.

This is the batch equivalent of the Cloud Function: instead of processing one
meta file per event, it walks every meta file for an endpoint and processes the
full history in one run.

Usage:
    python -m etl.backfill_gcs                 # all endpoints with parsers
    python -m etl.backfill_gcs accounts users   # specific endpoints only
    python -m etl.backfill_gcs --workers=12     # tune concurrency (default 8)

Reads from:  gs://prj-dw-dev-raw/raw/xero/...
Writes to:   prj-dw-dev.staging_xero.*

Parser policy (2026-07-07): parsers exist ONLY for endpoints that are actually
present in the GCS bucket. Speculative parsers ported from the old BigQuery
bronze project were removed. When a NEW endpoint appears in the bucket without a
parser, this script warns loudly (drift detection) — that is the signal to build
the parser then, guided by the real payload rather than the frozen old project.
Removed parsers remain in git history and their payloads are documented in
docs/STAGING_XERO.md, so rebuilding is a quick `git show` + backfill.

Concurrency model (2026-07-23): work is split into independent (tenant, endpoint)
jobs, run on a thread pool. Two prior single-endpoint, no-tenant-scope, serial
runs against this bucket (~54k blobs, 7 tenants) took over 2 hours without
finishing even one endpoint. Splitting by tenant lets each job use GCSReader's
tight per-tenant prefix instead of a full-bucket scan-and-filter, and running
jobs concurrently means wall-clock scales with the slowest job, not the sum of
all of them. Each thread gets its own GCSReader/BQWriter (bigquery/storage
clients are not guaranteed thread-safe) via thread-local storage, constructed
once per thread and reused across that thread's jobs.
"""

import sys
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage

from etl.common.gcs_reader import GCSReader
from etl.common.bq_writer import BQWriter

# Entity parsers — one per endpoint currently present in the GCS bucket.
import etl.xero.accounts            as accounts
import etl.xero.bank_transactions   as bank_transactions
import etl.xero.bank_transfers      as bank_transfers
import etl.xero.branding_themes     as branding_themes
import etl.xero.contacts            as contacts
import etl.xero.credit_notes        as credit_notes
import etl.xero.currencies          as currencies
import etl.xero.invoices            as invoices
import etl.xero.items               as items
import etl.xero.journals            as journals
import etl.xero.manual_journals     as manual_journals
import etl.xero.organisations       as organisations
import etl.xero.overpayments        as overpayments
import etl.xero.payments            as payments
import etl.xero.purchase_orders     as purchase_orders
import etl.xero.quotes              as quotes
import etl.xero.reports             as reports
import etl.xero.tax_rates           as tax_rates
import etl.xero.tracking_categories as tracking_categories
import etl.xero.users               as users

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BUCKET          = "prj-dw-dev-raw"
PROJECT         = "prj-dw-dev"
STAGING_DATASET = "staging_xero"
VENDOR          = "xero"
DEFAULT_WORKERS = 8

# Endpoint -> parser module. Every entry corresponds to an endpoint present in
# the GCS bucket. Keep this map in sync with the bucket via the drift check below.
PARSERS = {
    "accounts":             accounts,
    "bank_transactions":    bank_transactions,
    "bank_transfers":       bank_transfers,
    "branding_themes":      branding_themes,
    "contacts":             contacts,
    "credit_notes":         credit_notes,
    "currencies":           currencies,
    "invoices":             invoices,
    "items":                items,
    "journals":             journals,
    "manual_journals":      manual_journals,
    "organisations":        organisations,
    "overpayments":         overpayments,
    "payments":             payments,
    "purchase_orders":      purchase_orders,
    "quotes":               quotes,
    # Reports API — one shared module across all six report kinds (identical
    # payload shape; report kind is read from sync metadata, not the module).
    "report_balance_sheet":      reports,
    "report_bank_summary":       reports,
    "report_budget_summary":     reports,
    "report_executive_summary": reports,
    "report_profit_and_loss":    reports,
    "report_trial_balance":      reports,
    "tax_rates":            tax_rates,
    "tracking_categories":  tracking_categories,
    "users":                users,
}

# Bucket endpoints we knowingly do not parse (and why). Kept out of the drift
# warning so genuinely-new endpoints stand out.
KNOWN_UNPARSED = {
    "bills": "subset of invoices (ACCPAY) — already covered by staging_xero.invoices",
}

_print_lock = threading.Lock()
_thread_local = threading.local()


class _BatchReader:
    """Yields pre-collected GCS records regardless of the table name passed."""
    def __init__(self, records: list[dict], project: str):
        self.project = project
        self._records = records

    def iter_records(self, table: str, **kwargs):  # noqa: ARG002
        yield from self._records


def _scan_bucket() -> tuple[set[str], set[str]]:
    """
    Single pass over the bucket: return (endpoints, tenants) seen in blob paths.
    One full listing instead of two — listing is cheap; downloading is what's slow.
    """
    client = storage.Client(project=PROJECT)
    endpoints, tenants = set(), set()
    for b in client.list_blobs(BUCKET, prefix=f"raw/{VENDOR}/"):
        parts = b.name.split("/")
        if len(parts) > 4:
            tenants.add(parts[2])
            endpoints.add(parts[4])
    return endpoints, tenants


def check_for_new_endpoints(bucket_endpoints: set[str]) -> list[str]:
    """
    Drift detection: warn about any bucket endpoint that has neither a parser
    nor a known-unparsed reason. Returns the list of unrecognised endpoints.
    """
    unrecognised = sorted(
        e for e in bucket_endpoints
        if e not in PARSERS and e not in KNOWN_UNPARSED
    )
    for e in unrecognised:
        logger.warning(
            "NEW ENDPOINT DETECTED IN BUCKET: '%s' has no parser. "
            "Inspect a payload and build etl/xero/%s.py, then add it to PARSERS.",
            e, e,
        )
    for e in sorted(bucket_endpoints & set(KNOWN_UNPARSED)):
        logger.info("Skipping known-unparsed endpoint '%s' (%s)", e, KNOWN_UNPARSED[e])
    return unrecognised


def _reader_writer_for_this_thread() -> tuple[GCSReader, BQWriter]:
    """One GCSReader/BQWriter per thread, built once and reused across its jobs."""
    if not hasattr(_thread_local, "gcs"):
        _thread_local.gcs = GCSReader(bucket=BUCKET, project=PROJECT)
        _thread_local.writer = BQWriter(project=PROJECT, dataset=STAGING_DATASET)
    return _thread_local.gcs, _thread_local.writer


def backfill_tenant_endpoint(tenant: str, endpoint: str) -> dict:
    """One independent unit of work: one tenant, one endpoint, tight GCS prefix."""
    gcs, writer = _reader_writer_for_this_thread()
    module = PARSERS[endpoint]
    records = list(gcs.iter_records(VENDOR, endpoint, tenant_id=tenant))
    if not records:
        return {"records": 0}
    reader = _BatchReader(records, project=PROJECT)
    result = module.run(reader, writer)
    result["records"] = len(records)
    return result


def main(endpoints: list[str] | None, max_workers: int = DEFAULT_WORKERS) -> None:
    # Always run drift detection against the live bucket first (one full scan,
    # also yields the tenant list used to scope every job below).
    bucket_endpoints, bucket_tenants = _scan_bucket()
    new_endpoints = check_for_new_endpoints(bucket_endpoints)
    if new_endpoints:
        print(f"\n  ⚠️  {len(new_endpoints)} unparsed endpoint(s) in bucket: "
              f"{', '.join(new_endpoints)} — see warnings above.\n")

    requested = endpoints or list(PARSERS.keys())

    jobs: list[tuple[str, str]] = []
    for endpoint in requested:
        if endpoint not in PARSERS:
            reason = KNOWN_UNPARSED.get(endpoint, "no parser")
            print(f"  SKIP  {endpoint:<22} ({reason})")
            continue
        for tenant in sorted(bucket_tenants):
            jobs.append((tenant, endpoint))

    print(f"\n  Running {len(jobs)} (tenant, endpoint) jobs "
          f"({len(bucket_tenants)} tenants x {len(requested)} endpoints) "
          f"on {max_workers} workers…\n")

    passed = failed = 0
    total_start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_job = {
            pool.submit(backfill_tenant_endpoint, tenant, endpoint): (tenant, endpoint)
            for tenant, endpoint in jobs
        }
        for future in as_completed(future_to_job):
            tenant, endpoint = future_to_job[future]
            label = f"{endpoint}/{tenant[:8]}"
            try:
                result = future.result()
                with _print_lock:
                    print(f"  OK    {label:<32} {result}")
                passed += 1
            except Exception as e:
                with _print_lock:
                    print(f"  FAIL  {label:<32} {e}")
                failed += 1

    print(f"\n{passed} ok, {failed} failed — total {time.time() - total_start:.0f}s")


if __name__ == "__main__":
    args = sys.argv[1:]
    workers = DEFAULT_WORKERS
    endpoint_args = []
    for a in args:
        if a.startswith("--workers="):
            workers = int(a.split("=", 1)[1])
        else:
            endpoint_args.append(a)
    main(endpoint_args or None, max_workers=workers)
