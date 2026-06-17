"""Tests for etl/common/date_parser.py"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from datetime import datetime, date, timezone
from etl.common.date_parser import (
    parse_xero_datetime,
    parse_xero_date,
    parse_iso_date,
    parse_iso_datetime,
)


# --- parse_xero_datetime ---

def test_standard_with_offset():
    result = parse_xero_datetime("/Date(1773360000000+0000)/")
    assert result == datetime(2026, 3, 13, 0, 0, 0, tzinfo=timezone.utc)

def test_standard_with_nonzero_offset():
    # Offset is ignored — ms value is always UTC
    result = parse_xero_datetime("/Date(1773360000000+1000)/")
    assert result == datetime(2026, 3, 13, 0, 0, 0, tzinfo=timezone.utc)

def test_no_offset_quotes_format():
    # xero_quotes omits the ±offset entirely
    result = parse_xero_datetime("/Date(1774483200000)/")
    assert result is not None
    assert result.tzinfo == timezone.utc
    assert result == datetime(2026, 3, 26, 0, 0, 0, tzinfo=timezone.utc)

def test_none_input():
    assert parse_xero_datetime(None) is None

def test_empty_string():
    assert parse_xero_datetime("") is None

def test_non_date_string():
    assert parse_xero_datetime("ACTIVE") is None

def test_with_fractional_ms():
    # UpdatedDateUTC often includes sub-second precision in the ms value
    result = parse_xero_datetime("/Date(1773612484360+0000)/")
    assert result is not None
    assert result.tzinfo == timezone.utc


# --- parse_xero_date ---

def test_xero_date_returns_date_only():
    result = parse_xero_date("/Date(1773360000000+0000)/")
    assert result == date(2026, 3, 13)

def test_xero_date_none():
    assert parse_xero_date(None) is None


# --- parse_iso_date ---

def test_bare_date_string():
    # Repeating invoice Schedule.NextScheduledDateString
    assert parse_iso_date("2026-06-04") == date(2026, 6, 4)

def test_datetime_string_returns_date():
    # DateString companion fields include time — we only want the date
    assert parse_iso_date("2026-03-13T00:00:00") == date(2026, 3, 13)

def test_iso_date_none():
    assert parse_iso_date(None) is None

def test_iso_date_empty():
    assert parse_iso_date("") is None

def test_iso_date_malformed():
    assert parse_iso_date("not-a-date") is None


# --- parse_iso_datetime ---

def test_iso_datetime_full():
    result = parse_iso_datetime("2026-03-13T00:00:00")
    assert result == datetime(2026, 3, 13, 0, 0, 0)
    assert result.tzinfo is None  # naive — local org time, not UTC

def test_iso_datetime_none():
    assert parse_iso_datetime(None) is None

def test_iso_datetime_empty():
    assert parse_iso_datetime("") is None


if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
