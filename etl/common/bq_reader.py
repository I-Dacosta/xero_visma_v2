"""
BigQuery bronze reader — development stand-in for GCS.

During development the Python parsers read from the existing BQ bronze tables
(dw_1_bronze_xero.*) which have the identical JSON structure in their payload
column. Once the Rust sync service is updated to write to GCS, only this
module needs to swap for gcs_reader.py — all parser logic is unchanged.

Bronze table schema:
    tenant_id   STRING
    record_id   STRING
    payload     STRING  (JSON)
    first_seen_at  TIMESTAMP
    last_seen_at   TIMESTAMP
    last_run_id    STRING
    synced_at      TIMESTAMP

Usage:
    from etl.common.bq_reader import BQReader

    reader = BQReader(project="prj-dw-dev", dataset="dw_1_bronze_xero")

    for record in reader.iter_records("xero_bank_transactions"):
        tenant_id = record["tenant_id"]
        record_id = record["record_id"]
        payload   = record["payload"]   # dict (already parsed from JSON string)
        ...

    # Or fetch a single record by tenant + record id:
    record = reader.get_record("xero_bank_transactions",
                               tenant_id="9dc5d3f0-...",
                               record_id="a1c669bb-...")
"""

import json
import logging
from typing import Iterator

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BQReader:
    def __init__(self, project: str, dataset: str):
        self.project = project
        self.dataset = dataset
        self.client = bigquery.Client(project=project)

    def iter_records(
        self,
        table: str,
        tenant_id: str | None = None,
        limit: int | None = None,
    ) -> Iterator[dict]:
        """
        Yield records from a bronze table as dicts with parsed payload.

        Each yielded dict has:
            tenant_id    str
            record_id    str
            payload      dict  (JSON already parsed)
            first_seen_at, last_seen_at, synced_at  (datetime or None)

        Args:
            table:     Bronze table name, e.g. "xero_bank_transactions".
            tenant_id: Optional filter — only yield records for this tenant.
            limit:     Optional row cap (useful for development/testing).
        """
        where = f"WHERE tenant_id = '{tenant_id}'" if tenant_id else ""
        limit_clause = f"LIMIT {limit}" if limit else ""

        sql = f"""
            SELECT
                tenant_id,
                record_id,
                payload,
                first_seen_at,
                last_seen_at,
                synced_at
            FROM `{self.project}.{self.dataset}.{table}`
            {where}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY tenant_id, record_id
                ORDER BY last_seen_at DESC
            ) = 1
            {limit_clause}
        """

        logger.debug("Reading from %s.%s", self.dataset, table)
        rows = self.client.query(sql).result()

        for row in rows:
            yield {
                "tenant_id": row.tenant_id,
                "record_id": row.record_id,
                "payload": json.loads(row.payload),
                "first_seen_at": row.first_seen_at,
                "last_seen_at": row.last_seen_at,
                "synced_at": row.synced_at,
            }

    def get_record(
        self,
        table: str,
        tenant_id: str,
        record_id: str,
    ) -> dict | None:
        """
        Fetch a single record by its natural key. Returns None if not found.
        Useful for targeted testing of a specific payload.
        """
        sql = f"""
            SELECT
                tenant_id,
                record_id,
                payload,
                first_seen_at,
                last_seen_at,
                synced_at
            FROM `{self.project}.{self.dataset}.{table}`
            WHERE tenant_id = '{tenant_id}'
              AND record_id  = '{record_id}'
            LIMIT 1
        """
        rows = list(self.client.query(sql).result())
        if not rows:
            return None
        row = rows[0]
        return {
            "tenant_id": row.tenant_id,
            "record_id": row.record_id,
            "payload": json.loads(row.payload),
            "first_seen_at": row.first_seen_at,
            "last_seen_at": row.last_seen_at,
            "synced_at": row.synced_at,
        }

    def list_tables(self) -> list[str]:
        """Return all table names in the bronze dataset."""
        tables = self.client.list_tables(f"{self.project}.{self.dataset}")
        return sorted(t.table_id for t in tables)

    def count(self, table: str, tenant_id: str | None = None) -> int:
        """Row count for a bronze table, optionally filtered by tenant."""
        where = f"WHERE tenant_id = '{tenant_id}'" if tenant_id else ""
        sql = f"SELECT COUNT(*) AS n FROM `{self.project}.{self.dataset}.{table}` {where}"
        return list(self.client.query(sql).result())[0].n
