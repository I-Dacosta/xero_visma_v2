"""BigQuery loader for xero_service_v2.

Called as a subprocess by the Rust sync executor (Phase 2).

Usage (CLI):
    python -m xero_tooling.bigquery.loader \
        --project  my-gcp-project           \
        --dataset  xero_bronze              \
        --table    invoices                 \
        --tenant   <xero_tenant_id>         \
        --input    /tmp/records.jsonl
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def load_jsonl(
    project: str,
    dataset: str,
    table: str,
    tenant_id: str,
    records: list[dict[str, Any]],
) -> int:
    """Merge a list of records into BigQuery via MERGE statement.

    Returns the number of rows written.
    """
    try:
        from google.cloud import bigquery  # type: ignore[import]
    except ImportError:
        logger.error("google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery")
        raise

    client = bigquery.Client(project=project)
    full_table = f"{project}.{dataset}.{table}"

    # Stamp tenant_id + lineage onto every record before loading.
    for rec in records:
        rec["_tenant_id"] = tenant_id
        rec["_loaded_at"] = None  # BigQuery CURRENT_TIMESTAMP() fills this

    if not records:
        logger.info("No records to load — skipping")
        return 0

    errors = client.insert_rows_json(full_table, records)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")

    logger.info("Loaded %d rows into %s", len(records), full_table)
    return len(records)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Load JSONL records into BigQuery.")
    parser.add_argument("--project",  required=True)
    parser.add_argument("--dataset",  required=True)
    parser.add_argument("--table",    required=True)
    parser.add_argument("--tenant",   required=True)
    parser.add_argument("--input",    required=True, help="Path to JSONL file")
    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        sys.exit(f"Input file not found: {path}")

    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    loaded = load_jsonl(args.project, args.dataset, args.table, args.tenant, records)
    print(f"OK loaded={loaded}")


if __name__ == "__main__":
    main()
