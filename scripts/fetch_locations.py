from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from common import TOKYO_BBOX, load_dotenv, openaq_get, timestamped_path, write_json


RAW_DIR = Path("raw_data")
TOKYO_TZ = ZoneInfo("Asia/Tokyo")


def fetch_locations_page(bbox: str, limit: int, page: int) -> dict[str, Any]:
    return openaq_get("/locations", {"bbox": bbox, "limit": limit, "page": page})


def fetch_all_locations(bbox: str, limit: int) -> dict[str, Any]:
    page = 1
    all_results: list[dict[str, Any]] = []
    latest_payload: dict[str, Any] = {}

    while True:
        latest_payload = fetch_locations_page(bbox=bbox, limit=limit, page=page)
        page_results = latest_payload.get("results", [])
        all_results.extend(page_results)

        if len(page_results) < limit:
            break

        page += 1

    latest_payload["results"] = all_results
    latest_payload["pages_fetched"] = page
    return latest_payload


def parse_api_datetime(value: dict[str, Any] | None) -> datetime | None:
    if not value:
        return None
    raw = value.get("utc")
    if not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def date_window(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(start_date, time.min, tzinfo=TOKYO_TZ)
    end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=TOKYO_TZ)
    return start, end


def overlaps_window(location: dict[str, Any], start_date: date, end_date: date) -> bool:
    location_first = parse_api_datetime(location.get("datetimeFirst"))
    location_last = parse_api_datetime(location.get("datetimeLast"))
    if not location_first or not location_last:
        return False

    window_start, window_end = date_window(start_date, end_date)
    return location_first < window_end and location_last >= window_start


def filter_locations_by_coverage(payload: dict[str, Any], start_date: date, end_date: date) -> dict[str, Any]:
    all_locations = payload.get("results", [])
    filtered_locations = [
        location
        for location in all_locations
        if overlaps_window(location, start_date, end_date)
    ]

    payload["all_location_count"] = len(all_locations)
    payload["coverage_start_date"] = start_date.isoformat()
    payload["coverage_end_date"] = end_date.isoformat()
    payload["results"] = filtered_locations
    return payload


def write_raw_payload(payload: dict[str, Any], output_dir: Path) -> Path:
    output_path = timestamped_path(output_dir, "locations")
    write_json(payload, output_path)
    return output_path


def main() -> None:
    load_dotenv()
    bbox = os.getenv("OPENAQ_BBOX", TOKYO_BBOX)
    limit = int(os.getenv("OPENAQ_LOCATION_LIMIT", "1000"))
    coverage_start_date = date.fromisoformat(os.getenv("COVERAGE_START_DATE", "2026-04-01"))
    coverage_end_date = date.fromisoformat(os.getenv("COVERAGE_END_DATE", "2026-04-30"))

    payload = fetch_all_locations(bbox=bbox, limit=limit)
    payload = filter_locations_by_coverage(payload, coverage_start_date, coverage_end_date)
    output_path = write_raw_payload(payload, RAW_DIR)
    results = payload.get("results", [])

    print(f"Fetched {payload.get('all_location_count')} locations across {payload.get('pages_fetched')} pages")
    print(f"Kept {len(results)} locations with coverage between {coverage_start_date} and {coverage_end_date}")
    print(f"Focused sensors: {sum(len(location.get('sensors') or []) for location in results)}")
    print(f"Wrote raw payload to {output_path}")


if __name__ == "__main__":
    main()
