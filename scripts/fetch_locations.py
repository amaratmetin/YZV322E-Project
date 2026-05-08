from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from common import TOKYO_BBOX, load_dotenv, openaq_api_key, openaq_get, timestamped_path, write_json


RAW_DIR = Path("raw_data")


def fetch_locations_page(api_key: str, bbox: str, limit: int, page: int) -> dict[str, Any]:
    return openaq_get("/locations", {"bbox": bbox, "limit": limit, "page": page})


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
    output_path = timestamped_path(output_dir, "locations")
    write_json(payload, output_path)
    return output_path


def main() -> None:
    load_dotenv()
    bbox = os.getenv("OPENAQ_BBOX", TOKYO_BBOX)
    limit = int(os.getenv("OPENAQ_LOCATION_LIMIT", "100"))

    payload = fetch_all_locations(api_key=openaq_api_key(), bbox=bbox, limit=limit)
    output_path = write_raw_payload(payload, RAW_DIR)
    results = payload.get("results", [])

    print(f"Fetched {len(results)} locations across {payload.get('pages_fetched')} pages")
    print(f"Wrote raw payload to {output_path}")


if __name__ == "__main__":
    main()
