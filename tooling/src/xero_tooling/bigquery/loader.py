"""BigQuery MERGE loader for xero_service_v2.

Called as a subprocess by the Rust sync executor.

Why MERGE-based loading (lessons learned from visma_service v1 Bug E,
2026-05-19):

- `insert_rows_json` is a streaming insert. Rows enter a streaming buffer
  before being committed to managed storage. Subsequent MERGEs cannot
  reliably see buffered rows, so repeated syncs accumulate duplicates.
- An hourly sync that streams + never deduplicates grows bronze
  unboundedly (the v1 leak was ~12,000 dup rows/hour for GL alone).

Strategy used here (matches `visma_service_v2/tooling/.../bigquery/loader.py`):

1. Load records to a unique short-lived temp table via
   `load_table_from_json` (auto-detect schema, `WRITE_TRUNCATE`). Batch
   load — no streaming buffer.
2. Pre-deduplicate records in Python on the declared primary key. Drops
   identical repeats; raises on same-PK-different-payload to surface
   upstream bugs.
3. MERGE temp → target using `IS NOT DISTINCT FROM` for NULL-safe key
   matching (avoids the v1 Bug E shape where a NULL on either side
   silently breaks the join).
4. Always drop the temp table in `finally` so failed runs do not leave
   orphans behind.

Usage (CLI):
    python -m xero_tooling.bigquery.loader \
        --project     my-gcp-project        \
        --dataset     dw_1_bronze_xero      \
        --table       invoices              \
        --tenant      <xero_tenant_id>      \
        --primary-key invoiceID             \
        --input       /tmp/records.jsonl
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

COMPOSITE_PK_SEPARATOR = "\x1f"  # ASCII unit-separator — won't appear in real keys


def _get_path(record: dict[str, Any], path: str) -> Any:
    """Resolve a dotted JSON path inside a record (e.g. 'contact.contactID')."""
    current: Any = record
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def _stringify_pk_value(value: Any) -> str:
    if value is None:
        raise ValueError("primary key value cannot be null")
    if isinstance(value, str):
        if not value.strip():
            raise ValueError("primary key value cannot be empty")
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _record_pk(record: dict[str, Any], primary_keys: list[str]) -> str:
    return COMPOSITE_PK_SEPARATOR.join(
        _stringify_pk_value(_get_path(record, key)) for key in primary_keys
    )


def _canonical_json(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _parse_primary_key(primary_key: str) -> list[str]:
    keys = [part.strip() for part in primary_key.split(",") if part.strip()]
    if not keys:
        raise ValueError("primary_key must contain at least one field")
    return keys


def _dedupe_records_by_pk(
    records: list[dict[str, Any]],
    primary_keys: list[str],
) -> tuple[list[dict[str, Any]], int]:
    """Drop identical-payload repeats; raise on same-PK-different-payload.

    The MERGE later cannot tolerate multiple source rows sharing one target
    key — BigQuery raises 'multiple rows matched'. We pre-dedupe in Python
    so the failure mode surfaces here, not at the BQ MERGE call site where
    diagnosing it is harder.
    """
    out: list[dict[str, Any]] = []
    seen_payload_by_key: dict[str, str] = {}
    duplicates_dropped = 0

    for record in records:
        key = _record_pk(record, primary_keys)
        payload = _canonical_json(record)
        prior = seen_payload_by_key.get(key)
        if prior == payload:
            duplicates_dropped += 1
            continue
        if prior is not None:
            raise ValueError(f"duplicate primary key with different payload: {key!r}")
        seen_payload_by_key[key] = payload
        out.append(record)

    return out, duplicates_dropped


def _field_expr(alias: str, path: str) -> str:
    """Build a BigQuery field expression supporting nested paths.

    Top-level: `T`.`field`
    Nested:    `T`.`a`.`b` (BQ struct field access via dotted backticks)
    """
    return f"{alias}." + ".".join(f"`{part}`" for part in path.split("."))


def _validate_target_pk_schema(
    client: Any, target_id: str, primary_keys: list[str]
) -> None:
    """Verify each primary-key top-level column exists on the target table.

    Runs BEFORE any load to fail fast on PK/schema mismatches — saves a
    load-job round trip and avoids leaving an orphan temp table on a
    config error. NotFound on the target itself is allowed (caller may be
    creating the table); we only block when the table exists and the PK
    column is missing.
    """
    from google.api_core.exceptions import NotFound  # type: ignore[import]

    try:
        target_meta = client.get_table(target_id)
    except NotFound:
        # First write to a new table — autodetect will create the schema.
        return
    target_cols = {f.name for f in target_meta.schema}
    top_level = {pk.split(".", maxsplit=1)[0] for pk in primary_keys}
    missing = sorted(top_level - target_cols)
    if missing:
        raise ValueError(
            f"primary-key column(s) missing from target {target_id}: {missing}. "
            f"Available: {sorted(target_cols)[:20]}{'...' if len(target_cols) > 20 else ''}"
        )


def _warn_on_suspicious_dml(
    *,
    target_id: str,
    source_count: int,
    affected: int,
    inserted: int | None,
    updated: int | None,
) -> None:
    """Emit warnings on DML patterns that historically masked sync bugs.

    Specifically:
      * ``inserted == source_count, updated == 0, source_count > 0`` was the
        v1 Bug E signature — every source row fell through to INSERT because
        the MERGE ON-clause was NULL-unsafe. The IS NOT DISTINCT FROM fix
        eliminates that mechanism, but a recurrence (e.g. a column rename
        breaking the ON-clause) would show the same shape.
      * ``affected == 0, source_count > 0`` means MERGE silently did nothing.
        Usually fine on a no-op resync, but on a fresh-data sync it indicates
        the MERGE matched nothing AND inserted nothing — schema or join key
        mismatch.
    """
    if source_count == 0:
        return
    if inserted is not None and updated is not None:
        if inserted == source_count and updated == 0 and source_count > 0:
            logger.warning(
                "DML pattern alert: every source row INSERTed (none matched). "
                "Target=%s source=%d inserted=%d updated=0. "
                "Verify MERGE ON-clause and column names against target schema.",
                target_id, source_count, inserted,
            )
    if affected == 0:
        logger.warning(
            "MERGE affected zero rows for %d source rows on %s. "
            "If this is a fresh-data sync, investigate; on a no-op resync it's expected.",
            source_count, target_id,
        )


def merge(
    *,
    project: str,
    dataset: str,
    table: str,
    primary_key: str,
    tenant_id: str,
    records: list[dict[str, Any]],
    client: Any | None = None,
) -> int:
    """MERGE records into `project.dataset.table`.

    Returns the number of rows affected (inserted + updated) per
    `merge_job.num_dml_affected_rows`. Raises on BQ API errors.
    """
    try:
        from google.cloud import bigquery  # type: ignore[import]
    except ImportError:
        logger.error("google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery")
        raise

    if not records:
        return 0

    if client is None:
        client = bigquery.Client(project=project)

    primary_keys = _parse_primary_key(primary_key)

    for rec in records:
        rec["_tenant_id"] = tenant_id

    records, duplicates_dropped = _dedupe_records_by_pk(records, primary_keys)
    if duplicates_dropped:
        logger.info("Dropped %d identical duplicate record(s) before MERGE", duplicates_dropped)

    target_id = f"{project}.{dataset}.{table}"
    temp_id = f"{project}.{dataset}.{table}_tmp_{uuid.uuid4().hex[:8]}"

    # Fail fast on schema mismatch BEFORE any load happens. Catches typo'd
    # --primary-key arguments and renamed-column situations without paying
    # for a doomed load job + orphan temp-table cleanup.
    _validate_target_pk_schema(client, target_id, primary_keys)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    try:
        load_job = client.load_table_from_json(records, temp_id, job_config=job_config)
        load_job.result()

        temp_meta = client.get_table(temp_id)
        columns = [f.name for f in temp_meta.schema]

        pk_top_level = {pk.split(".", maxsplit=1)[0] for pk in primary_keys}
        missing_pk = sorted(pk for pk in pk_top_level if pk not in columns)
        if missing_pk:
            raise ValueError(
                "primary key field(s) missing from loaded records: " + ", ".join(missing_pk)
            )

        set_clause = ", ".join(
            f"T.`{c}` = S.`{c}`" for c in columns if c not in pk_top_level
        )
        insert_cols = ", ".join(f"`{c}`" for c in columns)
        insert_vals = ", ".join(f"S.`{c}`" for c in columns)
        on_clause = " AND ".join(
            f"{_field_expr('T', pk)} IS NOT DISTINCT FROM {_field_expr('S', pk)}"
            for pk in primary_keys
        )

        merge_sql = f"""
            MERGE `{target_id}` AS T
            USING `{temp_id}` AS S
            ON {on_clause}
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """

        merge_job = client.query(merge_sql)
        merge_job.result()

        affected = merge_job.num_dml_affected_rows or 0
        stats = getattr(merge_job, "dml_stats", None)
        inserted = getattr(stats, "inserted_row_count", None) if stats else None
        updated = getattr(stats, "updated_row_count", None) if stats else None
        logger.info(
            "MERGE complete: table=%s, source=%d, affected=%d, inserted=%s, updated=%s",
            target_id,
            len(records),
            affected,
            inserted if inserted is not None else "n/a",
            updated if updated is not None else "n/a",
        )
        _warn_on_suspicious_dml(
            target_id=target_id,
            source_count=len(records),
            affected=affected,
            inserted=inserted,
            updated=updated,
        )
        return affected
    finally:
        client.delete_table(temp_id, not_found_ok=True)


def load_jsonl(
    project: str,
    dataset: str,
    table: str,
    tenant_id: str,
    primary_key: str,
    records: list[dict[str, Any]],
) -> int:
    """Backward-compatible alias used by the existing CLI."""
    return merge(
        project=project,
        dataset=dataset,
        table=table,
        primary_key=primary_key,
        tenant_id=tenant_id,
        records=records,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="MERGE JSONL records into BigQuery.")
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--tenant", required=True)
    parser.add_argument(
        "--primary-key",
        required=True,
        help="Comma-separated primary key field(s); dotted paths allowed (e.g. 'contact.contactID')",
    )
    parser.add_argument("--input", required=True, help="Path to JSONL file")
    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        sys.exit(f"Input file not found: {path}")

    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    affected = merge(
        project=args.project,
        dataset=args.dataset,
        table=args.table,
        primary_key=args.primary_key,
        tenant_id=args.tenant,
        records=records,
    )
    print(f"OK affected={affected}")


if __name__ == "__main__":
    main()
