"""
Tests for etl/common/bq_writer.py

These tests run against real BigQuery using a disposable test table in
dw_2_staging_xero. No mocking — the point is to verify the MERGE logic
actually works end-to-end.

Run with:
    python etl/tests/test_bq_writer.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from google.cloud import bigquery
from etl.common.bq_writer import BQWriter

PROJECT = "prj-dw-dev"
DATASET = "dw_2_staging_xero"
TEST_TABLE = "_test_bq_writer"

client = bigquery.Client(project=PROJECT)
writer = BQWriter(project=PROJECT, dataset=DATASET)


def _drop_test_table():
    client.delete_table(f"{PROJECT}.{DATASET}.{TEST_TABLE}", not_found_ok=True)


def _read_all() -> list[dict]:
    rows = client.query(
        f"SELECT * FROM `{PROJECT}.{DATASET}.{TEST_TABLE}` ORDER BY record_id"
    ).result()
    return [dict(r) for r in rows]


def test_insert_new_rows():
    _drop_test_table()
    rows = [
        {"tenant_id": "t1", "record_id": "r1", "amount": 100.0, "status": "AUTHORISED"},
        {"tenant_id": "t1", "record_id": "r2", "amount": 200.0, "status": "AUTHORISED"},
    ]
    writer.merge(TEST_TABLE, rows)
    result = _read_all()
    assert len(result) == 2
    assert result[0]["record_id"] == "r1"
    assert result[1]["amount"] == 200.0
    print("  PASS  test_insert_new_rows")


def test_update_existing_row():
    # Merge same record_id with different amount — should UPDATE, not duplicate
    updated = [{"tenant_id": "t1", "record_id": "r1", "amount": 999.0, "status": "PAID"}]
    writer.merge(TEST_TABLE, updated)
    result = _read_all()
    assert len(result) == 2, f"Expected 2 rows, got {len(result)}"
    r1 = next(r for r in result if r["record_id"] == "r1")
    assert r1["amount"] == 999.0
    assert r1["status"] == "PAID"
    print("  PASS  test_update_existing_row")


def test_insert_and_update_mixed():
    # r2 exists (update), r3 is new (insert)
    rows = [
        {"tenant_id": "t1", "record_id": "r2", "amount": 250.0, "status": "PAID"},
        {"tenant_id": "t1", "record_id": "r3", "amount": 300.0, "status": "AUTHORISED"},
    ]
    writer.merge(TEST_TABLE, rows)
    result = _read_all()
    assert len(result) == 3
    r2 = next(r for r in result if r["record_id"] == "r2")
    assert r2["amount"] == 250.0
    r3 = next(r for r in result if r["record_id"] == "r3")
    assert r3["amount"] == 300.0
    print("  PASS  test_insert_and_update_mixed")


def test_empty_rows_raises():
    try:
        writer.merge(TEST_TABLE, [])
        print("  FAIL  test_empty_rows_raises: expected ValueError")
    except ValueError:
        print("  PASS  test_empty_rows_raises")


def teardown():
    _drop_test_table()


if __name__ == "__main__":
    passed = failed = 0
    tests = [
        test_insert_new_rows,
        test_update_existing_row,
        test_insert_and_update_mixed,
        test_empty_rows_raises,
    ]
    try:
        for t in tests:
            try:
                t()
                passed += 1
            except Exception as e:
                print(f"  FAIL  {t.__name__}: {e}")
                failed += 1
    finally:
        teardown()
    print(f"\n{passed} passed, {failed} failed")
