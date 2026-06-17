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
from datetime import datetime, date
from typing import Any

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BQWriter:
    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset
        self.client = bigquery.Client(project=project)

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

        run_id = uuid.uuid4().hex[:12]
        tmp = self._full_temp(table, run_id)
        target = self._full_table(table)
        columns = list(rows[0].keys())

        # Use existing table schema for the temp write when available,
        # preventing type mismatches (e.g. account_code INT64 vs STRING).
        existing_schema = self._get_schema(target)

        try:
            self._write_temp(tmp, rows, schema=existing_schema)
            self._ensure_target_exists(target, tmp)
            self._run_merge(target, tmp, columns, key_columns)
            logger.info("Merged %d row(s) into %s", len(rows), table)
            return len(rows)
        finally:
            self._drop_temp(tmp)

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
