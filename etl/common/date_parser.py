"""
Xero date parsing utilities.

Xero encodes timestamps in three formats depending on the endpoint:

  1. /Date(1773360000000+0000)/   — milliseconds UTC + timezone offset (most entities)
  2. /Date(1774483200000)/        — milliseconds UTC, no offset (xero_quotes)
  3. "2026-06-04"                 — bare ISO date string, no time (repeating invoice schedule)

All functions return None on missing or malformed input rather than raising,
so a bad field in one record does not abort the entire parse.
"""

import re
from datetime import datetime, date, timezone
from typing import Optional

# Matches /Date(ms+0000)/ and /Date(ms)/ — offset is optional
_XERO_DATE_RE = re.compile(r"/Date\((\d+)(?:[+-]\d{4})?\)/")


def parse_xero_datetime(value: Optional[str]) -> Optional[datetime]:
    """
    Parse a Xero /Date(ms±offset)/ string to a timezone-aware UTC datetime.

    Handles both the standard form with offset and the offset-free form
    used by xero_quotes.

    Returns None if value is None, empty, or does not match the pattern.
    """
    if not value:
        return None
    m = _XERO_DATE_RE.search(value)
    if not m:
        return None
    ms = int(m.group(1))
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def parse_xero_date(value: Optional[str]) -> Optional[date]:
    """
    Parse a Xero /Date(ms±offset)/ string to a date (UTC calendar date).

    Useful when you only need the date portion (e.g. invoice_date, due_date)
    and want to avoid timezone-boundary issues by staying in UTC.
    """
    dt = parse_xero_datetime(value)
    return dt.date() if dt else None


def parse_iso_date(value: Optional[str]) -> Optional[date]:
    """
    Parse a bare ISO date string "YYYY-MM-DD" to a date object.

    Used for fields like Schedule.NextScheduledDateString in repeating invoices
    which do not include a time component.

    Also accepts "YYYY-MM-DDTHH:MM:SS" (DateString companions) and returns
    just the date portion.
    """
    if not value:
        return None
    try:
        # Handle both "2026-06-04" and "2026-06-04T00:00:00"
        return date.fromisoformat(value[:10])
    except (ValueError, TypeError):
        return None


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """
    Parse a "YYYY-MM-DDTHH:MM:SS" string (Xero DateString companion fields)
    to a naive datetime.

    These strings represent local time in the organisation's timezone.
    Returned as naive (no tzinfo) to signal that it is not UTC.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
