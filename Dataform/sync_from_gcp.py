#!/usr/bin/env python3
"""
Sync GCP Dataform workspace files to local directory.
Usage: python3 sync_from_gcp.py
"""

import base64
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

PROJECT = "prj-dw-dev"
LOCATION = "europe-north1"
REPOSITORY = "kim-test-repo"
WORKSPACE = "kim-rormark"
LOCAL_DIR = Path(__file__).parent

BASE_URL = (
    f"https://dataform.googleapis.com/v1beta1"
    f"/projects/{PROJECT}/locations/{LOCATION}"
    f"/repositories/{REPOSITORY}/workspaces/{WORKSPACE}"
)

SKIP_DIRS = {"node_modules", ".git"}


def get_access_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def api_get(token: str, path: str) -> dict:
    from urllib.parse import quote
    url = f"{BASE_URL}:readFile?path={quote(path)}"
    req = Request(url, headers={"Authorization": f"Bearer {token}"})
    with urlopen(req) as resp:
        return json.loads(resp.read())


def list_directory(token: str, path: str = "") -> dict:
    from urllib.parse import quote
    url = f"{BASE_URL}:queryDirectoryContents?path={quote(path)}"
    req = Request(url, headers={"Authorization": f"Bearer {token}"})
    with urlopen(req) as resp:
        return json.loads(resp.read())


def collect_files(token: str, dir_path: str = "") -> list[str]:
    """Recursively collect all file paths in workspace, skipping SKIP_DIRS."""
    result = list_directory(token, dir_path)
    files = []
    for entry in result.get("directoryEntries", []):
        if "file" in entry:
            file_path = f"{dir_path}/{entry['file']}" if dir_path else entry["file"]
            files.append(file_path)
        elif "directory" in entry:
            dirname = entry["directory"]
            if dirname in SKIP_DIRS:
                print(f"  ⏭  Skipping {dir_path}/{dirname}/")
                continue
            sub_path = f"{dir_path}/{dirname}" if dir_path else dirname
            files.extend(collect_files(token, sub_path))
    return files


def sync_file(token: str, remote_path: str) -> bool:
    """Download a single file and write it locally. Returns True if changed."""
    data = api_get(token, remote_path)
    content_b64 = data.get("fileContents", "")
    content_bytes = base64.b64decode(content_b64)

    local_path = LOCAL_DIR / remote_path
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if local_path.exists():
        existing = local_path.read_bytes()
        if existing == content_bytes:
            return False  # unchanged

    local_path.write_bytes(content_bytes)
    return True


def main():
    print(f"🔑 Getting GCP access token...")
    try:
        token = get_access_token()
    except subprocess.CalledProcessError as exc:
        print(f"❌ Failed to get access token: {exc.stderr}")
        sys.exit(1)

    print(f"📂 Listing workspace: {WORKSPACE}")
    print(f"   Project : {PROJECT}")
    print(f"   Location: {LOCATION}")
    print(f"   Repo    : {REPOSITORY}")
    print()

    print("🔍 Collecting file list (skipping node_modules)...")
    files = collect_files(token)
    print(f"   Found {len(files)} files\n")

    updated = 0
    unchanged = 0
    failed = 0

    for i, fpath in enumerate(files, 1):
        try:
            changed = sync_file(token, fpath)
            if changed:
                print(f"  ✅ [{i}/{len(files)}] Updated : {fpath}")
                updated += 1
            else:
                unchanged += 1
        except HTTPError as exc:
            print(f"  ❌ [{i}/{len(files)}] Failed  : {fpath} — HTTP {exc.code}")
            failed += 1
        except Exception as exc:
            print(f"  ❌ [{i}/{len(files)}] Error   : {fpath} — {exc}")
            failed += 1

    print()
    print("=" * 50)
    print(f"✅ Updated  : {updated}")
    print(f"⏭  Unchanged: {unchanged}")
    print(f"❌ Failed   : {failed}")
    print("=" * 50)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
