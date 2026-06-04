#!/usr/bin/env python3
"""One-shot script to delete the stale duplicate currency rates file from GCP."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from sync_to_gcp import api_request, commit_workspace, get_access_token, push_workspace

STALE_PATH = "definitions/bronze/curency/historical_curency_rates.sqlx"

token = get_access_token()
print(f"Deleting: {STALE_PATH}")
api_request(token, "POST", ":removeFile", {"path": STALE_PATH})
print("File removed from workspace.")

commit_workspace(token, "Remove stale duplicate HistoryratesPowerBI declaration")
print("Committed.")

push_workspace(token)
print("Pushed. Done.")
