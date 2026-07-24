"""
Tests for etl/common/gcs_reader.py

Reads real files from gs://prj-dw-dev-raw.
Run with:
    python etl/tests/test_gcs_reader.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from datetime import datetime, timezone
from google.cloud import storage

from etl.common.gcs_reader import GCSReader, _parse_synced_at
from etl.common.endpoint_config import ARRAY_KEYS, RECORD_ID_FIELDS

BUCKET  = "prj-dw-dev-raw"
PROJECT = "prj-dw-dev"

reader = GCSReader(bucket=BUCKET, project=PROJECT)


def _find_a_meta_file(endpoint: str | None = None) -> str | None:
    """Return the GCS path of the first .meta.json file found."""
    client = storage.Client(project=PROJECT)
    prefix = "raw/xero/"
    if endpoint:
        prefix = f"raw/xero/"  # list all then filter
    for blob in client.list_blobs(BUCKET, prefix=prefix, max_results=200):
        if blob.name.endswith(".meta.json"):
            if endpoint is None or f"/{endpoint}/" in blob.name:
                return blob.name
    return None


# --- Unit tests ---

def test_parse_synced_at_nanoseconds():
    # Xero timestamps have 9-digit sub-second precision
    result = _parse_synced_at("2026-06-18T13:40:56.194109096+00:00")
    assert isinstance(result, datetime)
    assert result.tzinfo == timezone.utc
    print("  PASS  test_parse_synced_at_nanoseconds")


def test_parse_synced_at_microseconds():
    result = _parse_synced_at("2026-06-18T13:40:56.194109+00:00")
    assert isinstance(result, datetime)
    print("  PASS  test_parse_synced_at_microseconds")


def test_endpoint_config_complete():
    assert set(ARRAY_KEYS.keys()) == set(RECORD_ID_FIELDS.keys()), \
        "ARRAY_KEYS and RECORD_ID_FIELDS must have the same endpoint keys"
    assert len(ARRAY_KEYS) >= 20
    print(f"  PASS  test_endpoint_config_complete ({len(ARRAY_KEYS)} endpoints)")


# --- Integration tests against real GCS bucket ---

def test_find_meta_file():
    path = _find_a_meta_file()
    assert path is not None, f"No .meta.json files found in gs://{BUCKET}/raw/xero/"
    assert path.endswith(".meta.json")
    print(f"  PASS  test_find_meta_file ({path[:60]}...)")
    return path


def test_iter_records_from_meta():
    meta_path = _find_a_meta_file()
    if not meta_path:
        print("  SKIP  test_iter_records_from_meta (no meta files in bucket)")
        return

    records = list(reader.iter_records_from_meta(meta_path))
    assert len(records) > 0, "Expected at least one record"

    r = records[0]
    assert "tenant_id"    in r and r["tenant_id"]
    assert "record_id"    in r and r["record_id"]
    assert "payload"      in r and isinstance(r["payload"], dict)
    assert "synced_at"    in r and isinstance(r["synced_at"], datetime)
    assert "last_seen_at" in r

    print(f"  PASS  test_iter_records_from_meta ({len(records)} records from {meta_path.split('/')[-1]})")


def test_record_id_is_entity_specific():
    """The record_id should be the entity-specific ID (e.g. AccountID), not a file-level key."""
    meta_path = _find_a_meta_file(endpoint="accounts")
    if not meta_path:
        print("  SKIP  test_record_id_is_entity_specific (no accounts meta file found)")
        return

    records = list(reader.iter_records_from_meta(meta_path))
    assert records, "No records returned"

    r = records[0]
    # For accounts, record_id should be the AccountID from the payload
    assert r["record_id"] == r["payload"].get("AccountID"), \
        f"record_id {r['record_id']} != AccountID {r['payload'].get('AccountID')}"
    print(f"  PASS  test_record_id_is_entity_specific (AccountID={r['record_id'][:8]}...)")


def test_payload_is_individual_record_not_wrapper():
    """payload should be a single account/invoice record, not the full API response wrapper."""
    meta_path = _find_a_meta_file()
    if not meta_path:
        print("  SKIP  test_payload_is_individual_record_not_wrapper")
        return

    records = list(reader.iter_records_from_meta(meta_path))
    assert records

    # Id and ProviderName are wrapper-only keys — they should not appear in individual records.
    # Status and DateTimeUTC are excluded from this check because they legitimately
    # appear in both the wrapper ("Status": "OK") and individual records ("Status": "ACTIVE").
    wrapper_only_keys = {"Id", "ProviderName"}
    payload_keys = set(records[0]["payload"].keys())
    assert not wrapper_only_keys.intersection(payload_keys), \
        f"Wrapper keys leaked into payload: {wrapper_only_keys.intersection(payload_keys)}"
    print("  PASS  test_payload_is_individual_record_not_wrapper")


if __name__ == "__main__":
    tests = [
        test_parse_synced_at_nanoseconds,
        test_parse_synced_at_microseconds,
        test_endpoint_config_complete,
        test_find_meta_file,
        test_iter_records_from_meta,
        test_record_id_is_entity_specific,
        test_payload_is_individual_record_not_wrapper,
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
