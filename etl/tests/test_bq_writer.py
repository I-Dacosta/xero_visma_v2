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


def test_dedup_duplicate_keys_in_batch():
    # Regression: a single merge() batch containing the SAME key many times
    # (as the backfill produces from repeated full-snapshot sync files) must
    # collapse to one row per key, keeping the latest synced_at — NOT insert
    # every copy. This is the quotes/purchase_orders 34x bug.
    _drop_test_table()
    rows = [
        {"tenant_id": "t1", "record_id": "r1", "amount": 1.0, "synced_at": "2026-01-01"},
        {"tenant_id": "t1", "record_id": "r1", "amount": 3.0, "synced_at": "2026-01-03"},  # latest
        {"tenant_id": "t1", "record_id": "r1", "amount": 2.0, "synced_at": "2026-01-02"},
        {"tenant_id": "t1", "record_id": "r2", "amount": 5.0, "synced_at": "2026-01-01"},
    ]
    writer.merge(TEST_TABLE, rows)
    result = _read_all()
    assert len(result) == 2, f"Expected 2 rows (deduped), got {len(result)}"
    r1 = next(r for r in result if r["record_id"] == "r1")
    assert r1["amount"] == 3.0, f"Expected latest synced_at row (amount 3.0), got {r1['amount']}"
    print("  PASS  test_dedup_duplicate_keys_in_batch")


def test_dedup_by_key_unit():
    # Pure-Python unit test of the dedup helper (no BigQuery).
    # Header-style key with synced_at: keep latest.
    hdr = BQWriter._dedup_by_key(
        [
            {"tenant_id": "t", "record_id": "a", "v": 1, "synced_at": "2026-01-01"},
            {"tenant_id": "t", "record_id": "a", "v": 2, "synced_at": "2026-02-01"},
            {"tenant_id": "t", "record_id": "b", "v": 3, "synced_at": "2026-01-01"},
        ],
        ("tenant_id", "record_id"),
    )
    assert len(hdr) == 2
    assert next(r for r in hdr if r["record_id"] == "a")["v"] == 2
    # Line-style key with NO synced_at: identical dup copies collapse to one.
    lines = BQWriter._dedup_by_key(
        [
            {"tenant_id": "t", "record_id": "a", "line_item_id": "L1", "v": 9},
            {"tenant_id": "t", "record_id": "a", "line_item_id": "L1", "v": 9},
            {"tenant_id": "t", "record_id": "a", "line_item_id": "L2", "v": 8},
        ],
        ("tenant_id", "record_id", "line_item_id"),
    )
    assert len(lines) == 2, f"Expected 2 line rows, got {len(lines)}"
    print("  PASS  test_dedup_by_key_unit")


def teardown():
    _drop_test_table()


if __name__ == "__main__":
    passed = failed = 0
    tests = [
        test_insert_new_rows,
        test_update_existing_row,
        test_insert_and_update_mixed,
        test_empty_rows_raises,
        test_dedup_duplicate_keys_in_batch,
        test_dedup_by_key_unit,
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
