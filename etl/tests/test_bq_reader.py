"""
Tests for etl/common/bq_reader.py

Reads from the real dw_1_bronze_xero dataset. Tests are read-only — nothing
is written or deleted.

Run with:
    python etl/tests/test_bq_reader.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from etl.common.bq_reader import BQReader

PROJECT = "prj-dw-dev"
DATASET = "dw_1_bronze_xero"

reader = BQReader(project=PROJECT, dataset=DATASET)


def test_list_tables():
    tables = reader.list_tables()
    assert len(tables) > 0
    assert "xero_bank_transactions" in tables
    assert "xero_invoices" in tables
    print(f"  PASS  test_list_tables ({len(tables)} tables found)")


def test_iter_records_returns_dicts_with_parsed_payload():
    records = list(reader.iter_records("xero_bank_transactions", limit=3))
    assert len(records) > 0
    for r in records:
        assert "tenant_id" in r
        assert "record_id" in r
        assert isinstance(r["payload"], dict), "payload should be a parsed dict"
        assert "BankTransactionID" in r["payload"]
    print(f"  PASS  test_iter_records_returns_dicts_with_parsed_payload ({len(records)} records)")


def test_iter_records_tenant_filter():
    # Get any tenant_id from the table, then filter by it
    first = list(reader.iter_records("xero_bank_transactions", limit=1))[0]
    tenant_id = first["tenant_id"]
    records = list(reader.iter_records("xero_bank_transactions", tenant_id=tenant_id, limit=5))
    assert all(r["tenant_id"] == tenant_id for r in records)
    print(f"  PASS  test_iter_records_tenant_filter (tenant {tenant_id[:8]}...)")


def test_get_record_returns_correct_record():
    first = list(reader.iter_records("xero_bank_transactions", limit=1))[0]
    fetched = reader.get_record(
        "xero_bank_transactions",
        tenant_id=first["tenant_id"],
        record_id=first["record_id"],
    )
    assert fetched is not None
    assert fetched["record_id"] == first["record_id"]
    assert isinstance(fetched["payload"], dict)
    print("  PASS  test_get_record_returns_correct_record")


def test_get_record_returns_none_for_missing():
    result = reader.get_record(
        "xero_bank_transactions",
        tenant_id="00000000-0000-0000-0000-000000000000",
        record_id="00000000-0000-0000-0000-000000000000",
    )
    assert result is None
    print("  PASS  test_get_record_returns_none_for_missing")


def test_count():
    n = reader.count("xero_bank_transactions")
    assert n > 0
    print(f"  PASS  test_count ({n} rows in xero_bank_transactions)")


def test_count_with_tenant_filter():
    first = list(reader.iter_records("xero_bank_transactions", limit=1))[0]
    n = reader.count("xero_bank_transactions", tenant_id=first["tenant_id"])
    assert n > 0
    print(f"  PASS  test_count_with_tenant_filter ({n} rows for this tenant)")


if __name__ == "__main__":
    tests = [
        test_list_tables,
        test_iter_records_returns_dicts_with_parsed_payload,
        test_iter_records_tenant_filter,
        test_get_record_returns_correct_record,
        test_get_record_returns_none_for_missing,
        test_count,
        test_count_with_tenant_filter,
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
