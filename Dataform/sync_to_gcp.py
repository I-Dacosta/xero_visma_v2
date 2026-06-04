#!/usr/bin/env python3
"""Sync local Dataform workspace files to a GCP Dataform workspace."""

from __future__ import annotations

import base64
import json
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

PROJECT = "prj-dw-dev"
LOCATION = "europe-north1"
REPOSITORY = "Datawarehouse"
WORKSPACE = "Dev"
LOCAL_DIR = Path(__file__).parent

BASE_URL = (
    f"https://dataform.googleapis.com/v1beta1"
    f"/projects/{PROJECT}/locations/{LOCATION}"
    f"/repositories/{REPOSITORY}/workspaces/{WORKSPACE}"
)

SKIP_DIRS = {
    ".git",
    "my_venv",
    "node_modules",
    "__pycache__",
}

SKIP_FILES = {
    ".DS_Store",
    ".df-credentials.json",
    ".df-credentials.json.bak_invalid_schema",
    "df-credentials-object.json",
}


def get_access_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def api_request(token: str, method: str, suffix: str, payload: dict | None = None) -> dict:
    body = None
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(f"{BASE_URL}{suffix}", data=body, headers=headers, method=method)
    with urlopen(req) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def api_read_file(token: str, relative_file: str) -> bytes:
    data = api_request(token, "GET", f":readFile?path={quote(relative_file)}")
    return base64.b64decode(data.get("fileContents", ""))


def collect_local_files() -> list[Path]:
    files: list[Path] = []
    for file_path in sorted(LOCAL_DIR.rglob("*")):
        if not file_path.is_file():
            continue

        relative_parts = file_path.relative_to(LOCAL_DIR).parts
        if any(part in SKIP_DIRS for part in relative_parts[:-1]):
            continue
        if file_path.name in SKIP_FILES:
            continue
        files.append(file_path)

    return files


def ensure_remote_directories(token: str, relative_file: str, created_directories: set[str]) -> None:
    relative_path = Path(relative_file)
    parent = relative_path.parent
    if str(parent) in ("", "."):
        return

    parts: list[str] = []
    for part in parent.parts:
        parts.append(part)
        directory = "/".join(parts)
        if directory in created_directories:
            continue

        try:
            api_request(token, "POST", ":makeDirectory", {"path": directory})
        except HTTPError as exc:
            if exc.code != 409:
                raise
        created_directories.add(directory)


def write_remote_file(token: str, relative_file: str, content_bytes: bytes) -> None:
    encoded_contents = base64.b64encode(content_bytes).decode("ascii")
    api_request(token, "POST", ":writeFile", {"path": relative_file, "contents": encoded_contents})


def sync_file(token: str, file_path: Path, created_directories: set[str]) -> str:
    relative_file = file_path.relative_to(LOCAL_DIR).as_posix()
    local_bytes = file_path.read_bytes()

    remote_bytes = b""
    try:
        remote_bytes = api_read_file(token, relative_file)
    except HTTPError as exc:
        if exc.code != 404:
            raise

    if remote_bytes == local_bytes:
        return "unchanged"

    ensure_remote_directories(token, relative_file, created_directories)
    write_remote_file(token, relative_file, local_bytes)
    return "uploaded"


def get_gcloud_account_email() -> str | None:
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "account"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        return None

    account = result.stdout.strip()
    return account or None


def commit_workspace(token: str, commit_message: str) -> bool:
    account_email = get_gcloud_account_email()
    if not account_email:
        return False

    payload = {
        "author": {
            "name": "Copilot Sync",
            "emailAddress": account_email,
        },
        "commitMessage": commit_message,
    }

    try:
        api_request(token, "POST", ":commit", payload)
        return True
    except HTTPError:
        return False


def push_workspace(token: str) -> bool:
    try:
        api_request(token, "POST", ":push", {})
        return True
    except HTTPError:
        return False


def main() -> int:
    commit_message = "Sync local Dataform workspace files"
    if len(sys.argv) > 1:
        commit_message = sys.argv[1]

    print(f"Syncing {LOCAL_DIR} to {PROJECT}/{LOCATION}/{REPOSITORY}/{WORKSPACE}")
    try:
        token = get_access_token()
    except subprocess.CalledProcessError as exc:
        print(f"Failed to get access token: {exc.stderr}")
        return 1

    created_directories: set[str] = set()
    uploaded = 0
    unchanged = 0

    for file_path in collect_local_files():
        try:
            outcome = sync_file(token, file_path, created_directories)
        except UnicodeDecodeError:
            print(f"Skipping non-text file: {file_path.relative_to(LOCAL_DIR).as_posix()}")
            continue
        except HTTPError as exc:
            print(f"Failed to upload {file_path.relative_to(LOCAL_DIR).as_posix()}: HTTP {exc.code}")
            return 1

        if outcome == "uploaded":
            uploaded += 1
            print(f"Uploaded  {file_path.relative_to(LOCAL_DIR).as_posix()}")
        else:
            unchanged += 1

    committed = commit_workspace(token, commit_message)
    pushed = push_workspace(token) if committed else False

    print()
    print(f"Uploaded : {uploaded}")
    print(f"Unchanged: {unchanged}")
    print(f"Committed: {committed}")
    print(f"Pushed   : {pushed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())