"""
BigQuery writer — MERGE via temp table.

Pattern for every entity:
  1. Write parsed rows to a short-lived temp table
  2. MERGE temp into the target staging table on (tenant_id, record_id)
     - MATCHED     → UPDATE all fields
     - NOT MATCHED → INSERT
  3. Drop the temp table

This guarantees exactly one current row per (tenant_id, record_id) in staging.
If the process fails before the MERGE, the staging table is untouched.

Usage:
    from etl.common.bq_writer import BQWriter

    writer = BQWriter(project="prj-dw-dev", dataset="dw_2_staging_xero")
    writer.merge(
        table="bank_transactions",
        rows=[{"tenant_id": "...", "record_id": "...", "amount": 100.0, ...}],
    )
"""

import uuid
import logging
import threading
from datetime import datetime, date, timedelta, timezone
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BQWriter:
    # Per-target-table locks, shared across every BQWriter instance/thread in
    # this process. Concurrent backfills (2026-07-24) construct one BQWriter
    # per worker thread, so two threads can both be merging into the SAME
    # staging table at once (e.g. two tenants of the same endpoint) — without
    # this, BigQuery can reject one MERGE with "Could not serialize access to
    # table ... due to concurrent update" instead of queuing it. Serializing
    # merges per table (not per writer instance) fixes the race while leaving
    # merges into DIFFERENT tables fully concurrent.
    _table_locks_guard = threading.Lock()
    _table_locks: dict[str, threading.Lock] = {}

    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset
        self.client = bigquery.Client(project=project)

    def _lock_for(self, table: str) -> threading.Lock:
        key = f"{self.project}.{self.dataset}.{table}"
        with BQWriter._table_locks_guard:
            lock = BQWriter._table_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                BQWriter._table_locks[key] = lock
            return lock

    def _full_table(self, table: str) -> str:
        return f"`{self.project}.{self.dataset}.{table}`"

    def _full_temp(self, table: str, run_id: str) -> str:
        return f"`{self.project}.{self.dataset}._tmp_{table}_{run_id}`"

    def merge(
        self,
        table: str,
        rows: list[dict[str, Any]],
        key_columns: tuple[str, ...] = ("tenant_id", "record_id"),
    ) -> int:
        """
        Upsert rows into the target staging table via a temp table MERGE.

        Args:
            table:       Target staging table name (e.g. "bank_transactions").
            rows:        List of dicts; all dicts must have the same keys.
            key_columns: Columns that form the unique key. Defaults to
                         (tenant_id, record_id) which covers all Xero entities.

        Returns:
            Number of rows written to the temp table (before merge).

        Raises:
            ValueError: If rows is empty.
        """
        if not rows:
            raise ValueError(f"No rows to write to {table}")

        # Dedup the source batch by key_columns, keeping the latest synced_at.
        # A single merge() call can receive the same record many times — e.g. a
        # batch backfill that reads repeated full-snapshot ("master") sync files
        # from GCS yields every record once per snapshot. Without deduping here,
        # MERGE into an empty target treats all copies as NOT MATCHED and inserts
        # every one, producing N duplicate rows per key (observed: quotes /
        # purchase_orders at 34x). Collapsing to one row per key makes the MERGE
        # a true upsert and keeps the "exactly one current row per key" guarantee.
        rows = self._dedup_by_key(rows, key_columns)

        run_id = uuid.uuid4().hex[:12]
        tmp = self._full_temp(table, run_id)
        target = self._full_table(table)
        columns = list(rows[0].keys())

        # Serialize the write+merge sequence per target table across threads
        # (see _table_locks docstring above). The temp-table write and MERGE
        # for a DIFFERENT table proceed fully in parallel.
        with self._lock_for(table):
            try:
                # Fetched INSIDE the lock — deliberately. For a table that
                # doesn't exist yet, two tenant threads can both see "no
                # schema" if this is read before the lock: whichever thread
                # gets there first autodetects its own schema and creates the
                # table; the second thread's stale None then makes it also
                # autodetect its own (possibly differently-typed) temp table,
                # and its MERGE fails against the table the first thread just
                # created (found 2026-07-24 restoring bank_transfers/
                # overpayments — first-ever backfill of a new table, two
                # tenants' JSON values for the same field had different
                # native types, e.g. a numeric-looking reference as
                # int/float on one tenant vs string on another).
                existing_schema = self._get_schema(target)
                self._write_temp(tmp, rows, schema=existing_schema)
                self._ensure_target_exists(target, tmp)
                # If the target exists but is missing new columns, add them now
                # (schema evolution: new API fields). Types are taken from the
                # temp table schema which was created with autodetect.
                if existing_schema is not None:
                    self._evolve_target_schema(target, tmp, existing_schema)
                self._run_merge(target, tmp, columns, key_columns)
                logger.info("Merged %d row(s) into %s", len(rows), table)
                return len(rows)
            finally:
                self._drop_temp(tmp)

    @staticmethod
    def _dedup_by_key(
        rows: list[dict[str, Any]],
        key_columns: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        """
        Collapse rows to one per key_columns tuple, keeping the row with the
        greatest synced_at (falls back to last-seen when synced_at is absent,
        e.g. child/line tables). Preserves input order of the surviving rows.

        Rows missing any key column are treated as their own singleton keys and
        always kept (defensive — never silently drop data on a malformed key).
        """
        def _ts(row: dict[str, Any]) -> str:
            # Normalise synced_at to a sortable string (datetimes -> ISO, which
            # sorts chronologically); absent -> "" so it never wins over a real
            # timestamp and datetime-vs-None comparisons can't raise.
            v = row.get("synced_at")
            if hasattr(v, "isoformat"):
                return v.isoformat()
            return v or ""

        best: dict[tuple, dict[str, Any]] = {}
        order: list[tuple] = []
        for i, row in enumerate(rows):
            if all(c in row for c in key_columns):
                key = tuple(row[c] for c in key_columns)
            else:
                key = ("__nokey__", i)  # unique — never collapses
            prev = best.get(key)
            if prev is None:
                best[key] = row
                order.append(key)
            elif _ts(row) >= _ts(prev):
                best[key] = row
        return [best[k] for k in order]

    @staticmethod
    def _serialize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Convert datetime/date objects to ISO strings so load_table_from_json
        can serialise them. BQ autodetect maps these back to TIMESTAMP/DATE.
        """
        def _convert(v: Any) -> Any:
            if isinstance(v, datetime):
                return v.isoformat()
            if isinstance(v, date):
                return v.isoformat()
            return v

        return [{k: _convert(v) for k, v in row.items()} for row in rows]

    def _get_schema(self, target: str) -> list | None:
        """Return the existing staging table schema, or None if it doesn't exist yet."""
        try:
            return self.client.get_table(target.replace("`", "")).schema
        except Exception:
            return None

    def _write_temp(self, tmp: str, rows: list[dict[str, Any]],
                    schema: list | None = None) -> None:
        if schema:
            # Use the existing staging table schema — prevents type mismatches
            # when BQ would otherwise autodetect a different type (e.g. numeric
            # account codes inferred as INT64 instead of STRING).
            # However, if the data contains fields not yet in the schema (schema
            # evolution — new API fields), fall back to autodetect so new columns
            # are added rather than rejected.
            existing_field_names = {f.name for f in schema}
            data_field_names = set(rows[0].keys())
            new_fields = data_field_names - existing_field_names
            if new_fields:
                logger.info(
                    "New fields detected (schema evolution) — using autodetect: %s",
                    sorted(new_fields),
                )
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    autodetect=True,
                )
            else:
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    schema=schema,
                )
        else:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True,
            )
        table_ref = tmp.replace("`", "")
        job = self.client.load_table_from_json(
            self._serialize(rows), table_ref, job_config=job_config
        )
        job.result()
        logger.debug("Wrote %d row(s) to temp table %s", len(rows), tmp)
        self._set_temp_expiration(table_ref)

    def _set_temp_expiration(self, table_ref: str, ttl_hours: int = 1) -> None:
        """
        Safety net so an orphaned temp table cannot linger forever. _drop_temp()
        normally removes it within seconds, but if the process is killed (or the
        delete call itself transiently fails — caught there as non-fatal) the
        table was otherwise left with no expiration at all. Found 2026-07-24:
        4 temp tables from an interrupted run had expires=None and were never
        cleaned up. A failure here is logged and swallowed — worst case a temp
        table just outlives this TTL instead of being permanently orphaned.
        """
        try:
            table = self.client.get_table(table_ref)
            table.expires = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
            self.client.update_table(table, ["expires"])
        except Exception as e:
            logger.warning("Could not set expiration on temp table %s: %s", table_ref, e)

    def _evolve_target_schema(
        self, target: str, tmp: str, existing_schema: list
    ) -> None:
        """
        Add columns to the target that exist in the temp table but not in the
        target. Called only when schema evolution is detected (new API fields).
        Uses ALTER TABLE ADD COLUMN IF NOT EXISTS so it is idempotent.
        """
        try:
            tmp_table    = self.client.get_table(tmp.replace("`", ""))
            existing_names = {f.name for f in existing_schema}
            new_fields   = [f for f in tmp_table.schema if f.name not in existing_names]
            if not new_fields:
                return
            target_ref = target.replace("`", "")
            adds = ", ".join(
                f"ADD COLUMN IF NOT EXISTS {f.name} {f.field_type}"
                for f in new_fields
            )
            sql = f"ALTER TABLE `{target_ref}` {adds}"
            self.client.query(sql).result()
            logger.info(
                "Schema evolution: added %d column(s) to %s: %s",
                len(new_fields),
                target_ref,
                [f.name for f in new_fields],
            )
        except Exception as e:
            logger.warning("Schema evolution ALTER TABLE failed (non-fatal): %s", e)

    def _ensure_target_exists(self, target: str, tmp: str) -> None:
        """
        If the target staging table does not exist yet, create it from the
        temp table schema so we never have to manually write DDL.
        """
        table_ref = target.replace("`", "")
        try:
            self.client.get_table(table_ref)
        except Exception:
            # Create target with same schema as temp, but empty
            tmp_ref = tmp.replace("`", "")
            tmp_table = self.client.get_table(tmp_ref)
            new_table = bigquery.Table(table_ref, schema=tmp_table.schema)
            self.client.create_table(new_table)
            logger.info("Created staging table %s from temp schema", target)

    def _run_merge(
        self,
        target: str,
        tmp: str,
        columns: list[str],
        key_columns: tuple[str, ...],
    ) -> None:
        on_clause = " AND ".join(f"T.{c} = S.{c}" for c in key_columns)

        non_key = [c for c in columns if c not in key_columns]
        update_clause = ",\n      ".join(f"T.{c} = S.{c}" for c in non_key)

        all_cols = ", ".join(columns)
        all_vals = ", ".join(f"S.{c}" for c in columns)

        sql = f"""
MERGE {target} T
USING {tmp} S
ON {on_clause}
WHEN MATCHED THEN
  UPDATE SET
      {update_clause}
WHEN NOT MATCHED THEN
  INSERT ({all_cols})
  VALUES ({all_vals})
"""
        job = self.client.query(sql)
        job.result()
        logger.debug("MERGE complete into %s", target)

    def _drop_temp(self, tmp: str) -> None:
        try:
            table_ref = tmp.replace("`", "")
            self.client.delete_table(table_ref, not_found_ok=True)
            logger.debug("Dropped temp table %s", tmp)
        except Exception as e:
            # Non-fatal — temp tables are cheap and will expire anyway
            logger.warning("Could not drop temp table %s: %s", tmp, e)
