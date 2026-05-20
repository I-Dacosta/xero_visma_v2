"""Unit tests for the xero_tooling.bigquery.loader module."""
from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from xero_tooling.bigquery.loader import (
    _dedupe_records_by_pk,
    _field_expr,
    _get_path,
    _parse_primary_key,
    _record_pk,
    _stringify_pk_value,
    _validate_target_pk_schema,
    _warn_on_suspicious_dml,
    merge,
)


@pytest.mark.unit
def test_parse_primary_key_csv() -> None:
    assert _parse_primary_key("a, b , c") == ["a", "b", "c"]


@pytest.mark.unit
def test_parse_primary_key_rejects_empty() -> None:
    with pytest.raises(ValueError, match="primary_key must contain"):
        _parse_primary_key(", ,")


@pytest.mark.unit
def test_stringify_pk_value_rejects_null() -> None:
    with pytest.raises(ValueError, match="cannot be null"):
        _stringify_pk_value(None)


@pytest.mark.unit
def test_stringify_pk_value_rejects_empty_string() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        _stringify_pk_value("   ")


@pytest.mark.unit
def test_get_path_dotted() -> None:
    record = {"contact": {"contactID": "C-1"}}
    assert _get_path(record, "contact.contactID") == "C-1"


@pytest.mark.unit
def test_get_path_missing_returns_none() -> None:
    assert _get_path({"a": {}}, "a.b.c") is None


@pytest.mark.unit
def test_record_pk_composite() -> None:
    assert _record_pk({"a": "x", "b": "y"}, ["a", "b"]) == "x\x1fy"


@pytest.mark.unit
def test_field_expr_dotted() -> None:
    assert _field_expr("T", "contact.contactID") == "T.`contact`.`contactID`"


@pytest.mark.unit
def test_dedupe_drops_identical_repeats() -> None:
    records = [
        {"id": "a", "v": 1},
        {"id": "a", "v": 1},  # identical repeat
        {"id": "b", "v": 2},
    ]
    out, dropped = _dedupe_records_by_pk(records, ["id"])
    assert len(out) == 2
    assert dropped == 1


@pytest.mark.unit
def test_dedupe_raises_on_different_payload_same_pk() -> None:
    records = [{"id": "a", "v": 1}, {"id": "a", "v": 2}]
    with pytest.raises(ValueError, match="duplicate primary key"):
        _dedupe_records_by_pk(records, ["id"])


# ---- 2.3 schema-time PK validation ----


@pytest.mark.unit
def test_validate_target_pk_schema_passes_when_column_present() -> None:
    """Happy path: target table exists and has every PK column."""
    client = MagicMock()
    target_meta = MagicMock()
    target_meta.schema = [MagicMock(name="invoiceID"), MagicMock(name="amount")]
    target_meta.schema[0].name = "invoiceID"
    target_meta.schema[1].name = "amount"
    client.get_table.return_value = target_meta

    # Should not raise.
    _validate_target_pk_schema(client, "p.d.t", ["invoiceID"])


@pytest.mark.unit
def test_validate_target_pk_schema_raises_on_missing_column() -> None:
    """PK column missing from target schema → ValueError with context."""
    client = MagicMock()
    target_meta = MagicMock()
    col = MagicMock()
    col.name = "amount"
    target_meta.schema = [col]
    client.get_table.return_value = target_meta

    with pytest.raises(ValueError, match="primary-key column.*missing.*invoiceID"):
        _validate_target_pk_schema(client, "p.d.invoices", ["invoiceID"])


@pytest.mark.unit
def test_validate_target_pk_schema_silent_when_target_not_found() -> None:
    """Target doesn't exist yet — first write, autodetect will create it."""
    from google.api_core.exceptions import NotFound  # type: ignore[import]

    client = MagicMock()
    client.get_table.side_effect = NotFound("does not exist")

    # Should not raise.
    _validate_target_pk_schema(client, "p.d.t", ["invoiceID"])


@pytest.mark.unit
def test_validate_target_pk_schema_dotted_path() -> None:
    """For dotted PK paths, only the top-level segment must exist."""
    client = MagicMock()
    target_meta = MagicMock()
    col = MagicMock()
    col.name = "contact"
    target_meta.schema = [col]
    client.get_table.return_value = target_meta

    # Should not raise — "contact" exists at the top level even if "contactID"
    # is a nested field.
    _validate_target_pk_schema(client, "p.d.invoices", ["contact.contactID"])


# ---- 2.2 DML-stats alerting ----


@pytest.mark.unit
def test_warn_on_suspicious_dml_bug_e_signature(caplog: pytest.LogCaptureFixture) -> None:
    """inserted == source, updated == 0 is the v1 Bug E shape — warn loudly."""
    with caplog.at_level(logging.WARNING, logger="xero_tooling.bigquery.loader"):
        _warn_on_suspicious_dml(
            target_id="p.d.invoices",
            source_count=100,
            affected=100,
            inserted=100,
            updated=0,
        )
    assert any("DML pattern alert" in r.message for r in caplog.records)


@pytest.mark.unit
def test_warn_on_suspicious_dml_zero_affected(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="xero_tooling.bigquery.loader"):
        _warn_on_suspicious_dml(
            target_id="p.d.invoices",
            source_count=10,
            affected=0,
            inserted=0,
            updated=0,
        )
    assert any("affected zero rows" in r.message for r in caplog.records)


@pytest.mark.unit
def test_warn_on_suspicious_dml_clean_run_no_warning(caplog: pytest.LogCaptureFixture) -> None:
    """Normal mix of inserts + updates is expected — no warning."""
    with caplog.at_level(logging.WARNING, logger="xero_tooling.bigquery.loader"):
        _warn_on_suspicious_dml(
            target_id="p.d.invoices",
            source_count=100,
            affected=100,
            inserted=20,
            updated=80,
        )
    assert not any(r.levelname == "WARNING" for r in caplog.records)


@pytest.mark.unit
def test_warn_on_suspicious_dml_empty_batch_silent(caplog: pytest.LogCaptureFixture) -> None:
    """Zero source rows is not suspicious — caller already returns 0 early."""
    with caplog.at_level(logging.WARNING, logger="xero_tooling.bigquery.loader"):
        _warn_on_suspicious_dml(
            target_id="p.d.invoices",
            source_count=0,
            affected=0,
            inserted=None,
            updated=None,
        )
    assert not any(r.levelname == "WARNING" for r in caplog.records)


@pytest.mark.unit
def test_merge_empty_records_returns_zero() -> None:
    """Sanity check: empty input short-circuits, never touches BQ client."""
    result = merge(
        project="p", dataset="d", table="t",
        primary_key="id", tenant_id="t-1",
        records=[],
    )
    assert result == 0
