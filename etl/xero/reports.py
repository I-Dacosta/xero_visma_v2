"""
Xero Reports API parser — shared across all report kinds.

Reads from:   dw_1_bronze_xero.xero_report_* (dev) / GCS raw JSON (prod)
Writes to:    staging_xero.report_snapshots  (header — one row per report pull)
              staging_xero.report_rows       (long-form — one row per cell)

One module, registered under all six report endpoint names in PARSERS
(backfill_gcs.py / cloud_function/main.py), since every Xero report kind
shares an identical wrapper shape: {"Reports": [{..., "Rows": [...]}]}.
Report kind is read from the sync metadata (meta["x-report"]), not hardcoded
per file — the same module handles report_balance_sheet, report_bank_summary,
report_budget_summary, report_executive_summary, report_profit_and_loss, and
report_trial_balance.

⚠️ These are SNAPSHOTS, not entities — every pull is a new, independent
observation (re-running the same report for the same date range is not an
"update" to a prior row). Unlike every other Xero parser in this package,
staging must never collapse snapshots via upsert on a natural entity id —
there isn't one; ReportID (e.g. "ProfitAndLoss") is a constant per report
KIND, not per run. Snapshot identity is synthesized from the meta sidecar
(x-run-id, tenant_id, report kind), which the GCS envelope now carries as
record["meta"] (see etl/common/gcs_reader.py). Both staging tables are
therefore MERGE-keyed on that synthesized id, not upserted by content.

Xero's Rows[] tree (verified against live payloads, 2026-07-24):
  Reports[] (usually length 1)
    Rows[]                       — RowType: Header | Section | Row | SummaryRow
      Header.Cells[]             — column labels, keyed by cell position
      Section.Title, .Rows[]     — nests the actual data rows
      Row/SummaryRow.Cells[]     — each cell: {Value, Attributes: [{Id, Value}]}
                                    Attributes link a cell to an entity, e.g.
                                    {"Id": "account", "Value": "<account-guid>"} —
                                    the join key for reconciling report totals
                                    against our own bottom-up GL/document facts.

Report params live ONLY in the meta sidecar (x-report-params, a query string:
"date=2026-07-09" for point-in-time reports, "fromDate=...&toDate=..." for
period reports) — the payload's own "Fields" is empty on every report kind
observed. Parsed generically via urllib.parse, not per-report special-casing.
"""

import json
import logging
from urllib.parse import parse_qs

from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.common.date_parser import parse_iso_date, parse_xero_datetime

logger = logging.getLogger(__name__)

BRONZE_TABLE       = "xero_reports"   # unused in GCS mode; kept for interface parity
SNAPSHOT_TABLE     = "report_snapshots"
ROW_TABLE          = "report_rows"


def _parse_report_params(params: str | None) -> dict:
    if not params:
        return {}
    parsed = parse_qs(params)
    return {k: v[0] for k, v in parsed.items() if v}


def _first_attribute(cell: dict) -> tuple[str | None, str | None]:
    attrs = cell.get("Attributes") or []
    if not attrs:
        return None, None
    return attrs[0].get("Id"), attrs[0].get("Value")


def _flatten_report_rows(rpt: dict, report_index: int):
    """
    Walk one Report object's Rows[] tree, yielding one flat dict per cell.

    Header row(s) are collected first (by cell position) to resolve each data
    cell's column_header; Section rows nest the real data Rows[]; anything
    else at the top level (RowType not Header/Section) is itself a data row —
    some report kinds (e.g. BankSummary) emit rows directly, unwrapped.
    """
    rows = rpt.get("Rows") or []

    header_labels: dict[int, str] = {}
    for row in rows:
        if row.get("RowType") == "Header":
            for cell_idx, cell in enumerate(row.get("Cells") or []):
                header_labels[cell_idx] = cell.get("Value")

    def emit_data_row(data_row: dict, section_index: int, section_title: str | None, row_index: int):
        cells = data_row.get("Cells") or []
        row_label = cells[0].get("Value") if cells else None
        row_type = data_row.get("RowType")
        for cell_idx, cell in enumerate(cells):
            attribute_id, attribute_value = _first_attribute(cell)
            attrs = cell.get("Attributes") or []
            yield {
                "report_index":   report_index,
                "section_index":  section_index,
                "section_title":  section_title,
                "row_index":      row_index,
                "row_type":       row_type,
                "cell_index":     cell_idx,
                "row_label":      row_label,
                "column_header":  header_labels.get(cell_idx),
                "cell_value":         cell.get("Value"),
                "attribute_id":       attribute_id,
                "attribute_value":    attribute_value,
                # Full Attributes[] as JSON — ~2% of cells carry more than one
                # (e.g. account + tracking-category on the same cell), which
                # attribute_id/attribute_value alone would lose.
                "attributes_json":    json.dumps(attrs) if attrs else None,
            }

    for section_index, row in enumerate(rows):
        row_type = row.get("RowType")
        if row_type == "Header":
            continue
        if row_type == "Section":
            section_title = row.get("Title")
            for row_index, inner in enumerate(row.get("Rows") or []):
                yield from emit_data_row(inner, section_index, section_title, row_index)
        else:
            yield from emit_data_row(row, section_index, None, -1)


def parse_header(record: dict) -> dict:
    p    = record["payload"]
    meta = record.get("meta") or {}
    params = _parse_report_params(meta.get("x-report-params"))

    report = meta.get("x-report") or meta.get("x-endpoint")
    run_id = meta.get("x-run-id", "")

    return {
        "tenant_id":        record["tenant_id"],
        # Synthesized snapshot identity — NOT the generic record_id (ReportID
        # is a per-kind constant, not per-run). One row per (tenant, report,
        # run); a re-run is a new snapshot, never an update to a prior one.
        "record_id":        f"{report}|{run_id}",
        "synced_at":        record["last_seen_at"],
        "first_seen_at":    record["first_seen_at"],

        "report":           report,
        "run_id":           run_id,
        "report_id":        p.get("ReportID"),
        "report_name":      p.get("ReportName"),
        "report_type":      p.get("ReportType"),
        "report_titles":    " | ".join(p.get("ReportTitles") or []) or None,
        "report_updated_utc": parse_xero_datetime(p.get("UpdatedDateUTC")),

        "report_date":      parse_iso_date(params.get("date")),
        "report_from":      parse_iso_date(params.get("fromDate")),
        "report_to":        parse_iso_date(params.get("toDate")),
    }


def parse_rows(record: dict) -> list[dict]:
    p         = record["payload"]
    meta      = record.get("meta") or {}
    tenant_id = record["tenant_id"]
    report    = meta.get("x-report") or meta.get("x-endpoint")
    run_id    = meta.get("x-run-id", "")
    record_id = f"{report}|{run_id}"

    result = []
    # `p` here is a single entry from Reports[] (GCSReader unpacks the array
    # for us), so there is exactly one report per record — report_index is
    # always 0 for GCS-sourced records. Kept as a field for forward
    # compatibility / parity with the old multi-report bronze design.
    for cell in _flatten_report_rows(p, report_index=0):
        result.append({
            "tenant_id":  tenant_id,
            "record_id":  record_id,
            "report":     report,
            **cell,
        })
    return result


def run(reader: BQReader, writer: BQWriter,
        tenant_id: str | None = None, limit: int | None = None) -> dict:
    headers, rows = [], []
    for record in reader.iter_records(BRONZE_TABLE, tenant_id=tenant_id, limit=limit):
        headers.append(parse_header(record))
        rows.extend(parse_rows(record))

    if headers:
        writer.merge(SNAPSHOT_TABLE, headers, key_columns=("tenant_id", "record_id"))
    if rows:
        writer.merge(ROW_TABLE, rows,
                     key_columns=("tenant_id", "record_id", "report_index",
                                   "section_index", "row_index", "cell_index"))
    logger.info("reports: %d snapshots, %d rows", len(headers), len(rows))
    return {"headers": len(headers), "rows": len(rows)}
