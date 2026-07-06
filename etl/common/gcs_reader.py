"""
GCS reader — production replacement for bq_reader.py.

Reads Xero API data from the GCS bucket written by the sync service.
Each API call produces two files:

  Data file:  raw/{vendor}/{tenant_id}/2.0/{endpoint}/{date}/{ts}_{run}_{page}.json
  Meta file:  raw/{vendor}/{tenant_id}/2.0/{endpoint}/{date}/{ts}_{run}_{page}.json.meta.json

The meta file carries context (tenant_id, endpoint, synced_at, run_id).
The data file is the raw Xero API response — a wrapper object containing a
PascalCase-keyed array of records (e.g. "Accounts": [...], "Invoices": [...]).

Usage (Cloud Function — single meta file event):
    from etl.common.gcs_reader import GCSReader

    reader = GCSReader(bucket="aquatiq-dw-dev-storage")
    for record in reader.iter_records_from_meta("raw/xero/.../accounts/2026-06-18/file.json.meta.json"):
        # record["tenant_id"], record["record_id"], record["payload"], record["synced_at"]
        ...

Usage (batch / replay — iterate all files for an endpoint):
    for record in reader.iter_records("xero", "accounts", date="2026-06-18"):
        ...

Interface contract:
    Each yielded dict has the same shape as BQReader.iter_records() so all
    entity parsers work without modification:
        tenant_id     str
        record_id     str   (entity-specific ID, e.g. AccountID)
        payload       dict  (the individual record from the array)
        first_seen_at datetime | None
        last_seen_at  datetime  (= synced_at from meta file)
        synced_at     datetime
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Iterator

from google.cloud import storage

from etl.common.endpoint_config import ARRAY_KEYS, RECORD_ID_FIELDS

logger = logging.getLogger(__name__)

# Matches: raw/{vendor}/{tenant_id}/2.0/{endpoint}/{date}/{filename}
_PATH_RE = re.compile(
    r"raw/(?P<vendor>[^/]+)"
    r"/(?P<tenant_id>[^/]+)"
    r"/\d+\.\d+"            # version e.g. 2.0
    r"/(?P<endpoint>[^/]+)"
    r"/(?P<date>[^/]+)"
    r"/(?P<filename>[^/]+)"
)


class GCSReader:
    def __init__(self, bucket: str, project: str = "prj-dw-dev"):
        self.bucket_name = bucket
        self.project = project
        self._client = storage.Client(project=project)
        self._bucket = self._client.bucket(bucket)

    # ------------------------------------------------------------------
    # Primary interface: process a single meta file (Cloud Function mode)
    # ------------------------------------------------------------------

    def iter_records_from_meta(self, meta_path: str) -> Iterator[dict]:
        """
        Given the GCS path of a .meta.json file, read both files and yield
        one record dict per item in the records array.

        Args:
            meta_path: GCS object path of the .meta.json file, e.g.
                       "raw/xero/19b25bd5.../2.0/accounts/2026-06-18/file.json.meta.json"
        """
        meta = self._read_meta(meta_path)
        data_path = meta_path.removesuffix(".meta.json")
        data = self._read_data(data_path)

        endpoint  = meta["x-endpoint"]
        tenant_id = meta["x-tenant-id"]
        synced_at = _parse_synced_at(meta["x-synced-at"])

        yield from self._extract_records(data, endpoint, tenant_id, synced_at)

    # ------------------------------------------------------------------
    # Batch / replay interface: iterate all files for an endpoint + date
    # ------------------------------------------------------------------

    def iter_records(
        self,
        vendor: str,
        endpoint: str,
        tenant_id: str | None = None,
        date: str | None = None,
    ) -> Iterator[dict]:
        """
        Iterate all records for a vendor/endpoint, optionally filtered by
        tenant_id and/or date (YYYY-MM-DD). Reads from .meta.json files only.

        Useful for batch replays and backfills from GCS.

        The endpoint segment sits after {tenant_id}/{version} in the path, so a
        clean list prefix is only possible when tenant_id is known. When it is
        not, we list the whole vendor tree and match endpoint (and optionally
        date) via the parsed path.
        """
        if tenant_id:
            # Tightest possible prefix: raw/{vendor}/{tenant_id}/2.0/{endpoint}/[{date}/]
            prefix = f"raw/{vendor}/{tenant_id}/2.0/{endpoint}/"
            if date:
                prefix += f"{date}/"
        else:
            # Cannot pin endpoint in the prefix without tenant_id — list the
            # vendor tree and filter each blob by parsed path.
            prefix = f"raw/{vendor}/"

        for blob in self._client.list_blobs(self.bucket_name, prefix=prefix):
            if not blob.name.endswith(".meta.json"):
                continue
            m = _PATH_RE.search(blob.name)
            if not m:
                continue
            if m.group("endpoint") != endpoint:
                continue
            if date and m.group("date") != date:
                continue
            try:
                yield from self.iter_records_from_meta(blob.name)
            except Exception as e:
                logger.error("Failed processing %s: %s", blob.name, e)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _read_meta(self, path: str) -> dict:
        blob = self._bucket.blob(path)
        return json.loads(blob.download_as_text())

    def _read_data(self, path: str) -> dict:
        blob = self._bucket.blob(path)
        return json.loads(blob.download_as_text())

    def _extract_records(
        self,
        data: dict,
        endpoint: str,
        tenant_id: str,
        synced_at: datetime,
    ) -> Iterator[dict]:
        array_key = ARRAY_KEYS.get(endpoint)
        if not array_key:
            logger.warning("No array key configured for endpoint: %s", endpoint)
            return

        id_field = RECORD_ID_FIELDS.get(endpoint)
        if not id_field:
            logger.warning("No record ID field configured for endpoint: %s", endpoint)
            return

        records = data.get(array_key)
        if not records:
            logger.debug("No records found under key '%s' for endpoint %s", array_key, endpoint)
            return

        for item in records:
            record_id = str(item.get(id_field, ""))
            if not record_id:
                logger.warning("Record missing %s in endpoint %s — skipping", id_field, endpoint)
                continue

            yield {
                "tenant_id":    tenant_id,
                "record_id":    record_id,
                "payload":      item,
                "first_seen_at": synced_at,  # best approximation; not tracked per-record in GCS
                "last_seen_at":  synced_at,
                "synced_at":     synced_at,
            }


def _parse_synced_at(value: str) -> datetime:
    """Parse x-synced-at ISO timestamp (with or without nanoseconds) to UTC datetime."""
    try:
        # Truncate sub-microsecond precision Python can't handle
        # e.g. "2026-06-18T13:40:56.194109096+00:00" -> "2026-06-18T13:40:56.194109+00:00"
        truncated = re.sub(r"(\.\d{6})\d+([\+\-Z])", r"\1\2", value)
        return datetime.fromisoformat(truncated).astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)
