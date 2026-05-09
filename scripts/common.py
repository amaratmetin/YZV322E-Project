from __future__ import annotations

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


OPENAQ_BASE_URL = "https://api.openaq.org/v3"
TOKYO_BBOX = "139.55,35.50,139.95,35.85"


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def openaq_api_key() -> str:
    api_key = os.getenv("OPENAQ_API_KEY", "").strip()
    if not api_key or api_key == "replace_with_your_key":
        raise SystemExit("OPENAQ_API_KEY must be set in .env before fetching data.")
    return api_key


def openaq_get(path: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{OPENAQ_BASE_URL}{path}"
    headers = {"X-API-Key": openaq_api_key()}

    for attempt in range(5):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            if response.status_code not in {429, 500, 502, 503, 504}:
                response.raise_for_status()
                time.sleep(float(os.getenv("OPENAQ_REQUEST_DELAY_SECONDS", "1.1")))
                return response.json()
            if attempt == 4:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else 2**attempt
            time.sleep(delay)
        except requests.RequestException:
            if attempt == 4:
                raise
            time.sleep(2**attempt)

    raise RuntimeError(f"OpenAQ request failed after retries: {path}")


def latest_file(directory: Path, pattern: str) -> Path:
    files = sorted(directory.glob(pattern))
    if not files:
        raise SystemExit(f"No files found for {directory / pattern}")
    return files[-1]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def timestamped_path(directory: Path, prefix: str, suffix: str = ".json") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return directory / f"{prefix}_{timestamp}{suffix}"


def write_json(payload: dict[str, Any], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
