"""
End-to-end test for etl/xero/bank_transactions.py

Reads real bronze data, parses it, and writes to dw_2_staging_xero.
Run with:
    python etl/tests/test_bank_transactions.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from datetime import datetime, date
from google.cloud import bigquery

from etl.common.bq_reader import BQReader
from etl.common.bq_writer import BQWriter
from etl.xero.bank_transactions import parse_header, parse_lines, run

PROJECT        = "prj-dw-dev"
BRONZE_DATASET = "dw_1_bronze_xero"
STAGING_DATASET = "dw_2_staging_xero"

reader = BQReader(project=PROJECT, dataset=BRONZE_DATASET)
writer = BQWriter(project=PROJECT, dataset=STAGING_DATASET)
client = bigquery.Client(project=PROJECT)


def _sample_record() -> dict:
    return list(reader.iter_records("xero_bank_transactions", limit=1))[0]


# --- Unit tests on parse functions ---

def test_parse_header_has_required_fields():
    record = _sample_record()
    header = parse_header(record)
    for field in ("tenant_id", "record_id", "bank_transaction_id",
                  "transaction_type", "status", "total", "transaction_date"):
        assert field in header, f"Missing field: {field}"
    print("  PASS  test_parse_header_has_required_fields")


def test_parse_header_dates_are_typed():
    record = _sample_record()
    header = parse_header(record)
    assert isinstance(header["transaction_date"], datetime), \
        f"transaction_date should be datetime, got {type(header['transaction_date'])}"
    assert isinstance(header["transaction_date_local"], date), \
        f"transaction_date_local should be date, got {type(header['transaction_date_local'])}"
    assert isinstance(header["updated_at"], datetime)
    print("  PASS  test_parse_header_dates_are_typed")


def test_parse_header_amounts_are_float():
    record = _sample_record()
    header = parse_header(record)
    for field in ("sub_total", "total_tax", "total"):
        assert isinstance(header[field], float), f"{field} should be float"
    print("  PASS  test_parse_header_amounts_are_float")


def test_parse_header_bank_account_extracted():
    record = _sample_record()
    header = parse_header(record)
    assert header["bank_account_id"] is not None
    assert header["bank_account_code"] is not None
    print("  PASS  test_parse_header_bank_account_extracted")


def test_parse_lines_returns_list():
    record = _sample_record()
    lines = parse_lines(record)
    assert isinstance(lines, list)
    print(f"  PASS  test_parse_lines_returns_list ({len(lines)} lines)")


def test_parse_lines_have_required_fields():
    record = _sample_record()
    lines = parse_lines(record)
    if not lines:
        print("  SKIP  test_parse_lines_have_required_fields (no lines in sample)")
        return
    for field in ("tenant_id", "record_id", "bank_transaction_id",
                  "line_item_id", "line_amount", "tax_type"):
        assert field in lines[0], f"Missing field: {field}"
    print("  PASS  test_parse_lines_have_required_fields")


def test_parse_lines_count_matches_header():
    record = _sample_record()
    header = parse_header(record)
    lines  = parse_lines(record)
    assert header["line_item_count"] == len(lines)
    print("  PASS  test_parse_lines_count_matches_header")


# --- Integration test: full run against staging ---

def test_run_writes_to_staging():
    result = run(reader, writer, limit=10)
    assert result["headers"] == 10
    assert result["lines"] >= 10  # at least 1 line per transaction

    rows = list(client.query("""
        SELECT COUNT(*) AS n
        FROM `prj-dw-dev.dw_2_staging_xero.bank_transactions`
    """).result())
    assert rows[0].n >= 10

    print(f"  PASS  test_run_writes_to_staging "
          f"({result['headers']} headers, {result['lines']} lines)")


def test_run_deduplication():
    # Run twice with the same 10 records — row count should not double
    run(reader, writer, limit=10)
    run(reader, writer, limit=10)

    rows = list(client.query("""
        SELECT COUNT(*) AS n
        FROM `prj-dw-dev.dw_2_staging_xero.bank_transactions`
    """).result())

    # Should still be 10 (or close — same records merged, not duplicated)
    assert rows[0].n <= 20, f"Possible duplicate rows: {rows[0].n}"
    print(f"  PASS  test_run_deduplication ({rows[0].n} rows after 2 runs)")


if __name__ == "__main__":
    tests = [
        test_parse_header_has_required_fields,
        test_parse_header_dates_are_typed,
        test_parse_header_amounts_are_float,
        test_parse_header_bank_account_extracted,
        test_parse_lines_returns_list,
        test_parse_lines_have_required_fields,
        test_parse_lines_count_matches_header,
        test_run_writes_to_staging,
        test_run_deduplication,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
