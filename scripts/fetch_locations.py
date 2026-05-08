from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


OPENAQ_LOCATIONS_URL = "https://api.openaq.org/v3/locations"
TOKYO_BBOX = "139.55,35.50,139.95,35.85"


def fetch_locations_page(api_key: str, bbox: str, limit: int, page: int) -> dict[str, Any]:
    query_string = urlencode({"bbox": bbox, "limit": limit, "page": page})
    request = Request(
        f"{OPENAQ_LOCATIONS_URL}?{query_string}",
        headers={"X-API-Key": api_key},
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_all_locations(api_key: str, bbox: str, limit: int) -> dict[str, Any]:
    page = 1
    all_results: list[dict[str, Any]] = []
    latest_payload: dict[str, Any] = {}

    while True:
        latest_payload = fetch_locations_page(api_key=api_key, bbox=bbox, limit=limit, page=page)
        page_results = latest_payload.get("results", [])
        all_results.extend(page_results)

        if len(page_results) < limit:
            break

        page += 1

    latest_payload["results"] = all_results
    latest_payload["pages_fetched"] = page
    return latest_payload


def write_raw_payload(payload: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = output_dir / f"locations_{timestamp}.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch raw Tokyo location data from OpenAQ.")
    parser.add_argument("--bbox", default=os.getenv("OPENAQ_BBOX", TOKYO_BBOX))
    parser.add_argument("--limit", type=int, default=int(os.getenv("OPENAQ_LOCATION_LIMIT", "100")))
    parser.add_argument("--output-dir", type=Path, default=Path("raw_data"))
    return parser.parse_args()


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def main() -> None:
    load_dotenv()
    args = parse_args()

    api_key = os.getenv("OPENAQ_API_KEY", "").strip()
    if not api_key or api_key == "replace_with_your_key":
        raise SystemExit("OPENAQ_API_KEY must be set in .env before fetching data.")

    payload = fetch_all_locations(api_key=api_key, bbox=args.bbox, limit=args.limit)
    output_path = write_raw_payload(payload, args.output_dir)
    results = payload.get("results", [])

    print(f"Fetched {len(results)} locations across {payload.get('pages_fetched')} pages")
    print(f"Wrote raw payload to {output_path}")


if __name__ == "__main__":
    main()
