from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from common import latest_file, load_dotenv, read_json, timestamped_path, write_json


RAW_DIR = Path("raw_data")
ARCHIVE_DIR = RAW_DIR / "archive"
URL_ROOT = "https://openaq-data-archive.s3.amazonaws.com/records/csv.gz/locationid="


def parse_api_datetime(value: dict[str, Any] | None) -> datetime | None:
    if not value:
        return None
    raw = value.get("utc")
    if not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def location_covers_date(location: dict[str, Any], target_date: date) -> bool:
    datetime_first = parse_api_datetime(location.get("datetimeFirst"))
    datetime_last = parse_api_datetime(location.get("datetimeLast"))
    if not datetime_first or not datetime_last:
        return False

    return datetime_first.date() <= target_date <= datetime_last.date()


def archive_url(location_id: int, target_date: date) -> str:
    year = f"{target_date.year:04d}"
    month = f"{target_date.month:02d}"
    date_string = target_date.strftime("%Y%m%d")
    return (
        f"{URL_ROOT}{location_id}/year={year}/month={month}/"
        f"location-{location_id}-{date_string}.csv.gz"
    )


def archive_path(location_id: int, target_date: date) -> Path:
    year = f"{target_date.year:04d}"
    month = f"{target_date.month:02d}"
    date_string = target_date.strftime("%Y%m%d")
    return ARCHIVE_DIR / f"year={year}" / f"month={month}" / f"location-{location_id}-{date_string}.csv.gz"


def download_archive_file(session: requests.Session, url: str, output_path: Path) -> tuple[str, int]:
    for attempt in range(4):
        response = session.get(url, timeout=60)
        if response.status_code == 404:
            return "missing", 0
        if response.status_code in {500, 502, 503, 504} and attempt < 3:
            time.sleep(2**attempt)
            continue

        response.raise_for_status()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)
        return "downloaded", len(response.content)

    return "failed", 0


def download_location(location: dict[str, Any], target_date: date) -> dict[str, Any]:
    location_id = int(location["id"])
    url = archive_url(location_id, target_date)
    file_path = archive_path(location_id, target_date)

    with requests.Session() as session:
        status, size_bytes = download_archive_file(session, url, file_path)

    return {
        "location_id": location_id,
        "location_name": location.get("name"),
        "sensor_ids": [sensor.get("id") for sensor in location.get("sensors") or []],
        "url": url,
        "path": str(file_path) if status == "downloaded" else None,
        "status": status,
        "bytes": size_bytes,
    }


def main() -> None:
    started_at = time.monotonic()
    load_dotenv()
    target_date = date.fromisoformat(os.getenv("TARGET_DATE", "2026-04-01"))
    worker_count = int(os.getenv("S3_DOWNLOAD_WORKERS", "16"))

    locations_file = latest_file(RAW_DIR, "locations_*.json")
    locations_payload = read_json(locations_file)
    locations = locations_payload.get("results", [])

    output_payload: dict[str, Any] = {
        "source": "openaq_s3_archive_csv_gz",
        "source_locations_file": str(locations_file),
        "target_date": target_date.isoformat(),
        "url_root": URL_ROOT,
        "locations_considered": len(locations),
        "locations_skipped_outside_date": 0,
        "locations_downloaded": 0,
        "locations_missing_file": 0,
        "locations_failed": 0,
        "total_bytes": 0,
        "elapsed_seconds": 0,
        "results": [],
    }
    output_path = timestamped_path(RAW_DIR, "measurements")
    write_json(output_payload, output_path)

    active_locations = [
        location
        for location in locations
        if location_covers_date(location, target_date)
    ]
    output_payload["locations_skipped_outside_date"] = len(locations) - len(active_locations)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(download_location, location, target_date)
            for location in active_locations
        ]

        for index, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            status = result["status"]
            size_bytes = result["bytes"]

            if status == "downloaded":
                output_payload["locations_downloaded"] += 1
                output_payload["total_bytes"] += size_bytes
            elif status == "missing":
                output_payload["locations_missing_file"] += 1
            else:
                output_payload["locations_failed"] += 1

            output_payload["results"].append(result)
            output_payload["elapsed_seconds"] = round(time.monotonic() - started_at, 2)
            write_json(output_payload, output_path)

            if index == 1 or index % 25 == 0 or index == len(active_locations):
                print(
                    f"{target_date}: {index}/{len(active_locations)} locations checked, "
                    f"{output_payload['locations_downloaded']} downloaded, "
                    f"{output_payload['locations_missing_file']} missing",
                    flush=True,
                )

    output_payload["results"].sort(key=lambda row: row["location_id"])
    output_payload["elapsed_seconds"] = round(time.monotonic() - started_at, 2)
    write_json(output_payload, output_path)

    print(f"S3 workers: {worker_count}")
    print(f"Target date: {target_date}")
    print(f"Locations considered: {output_payload['locations_considered']}")
    print(f"Skipped outside date: {output_payload['locations_skipped_outside_date']}")
    print(f"Downloaded files: {output_payload['locations_downloaded']}")
    print(f"Missing files: {output_payload['locations_missing_file']}")
    print(f"Failed files: {output_payload['locations_failed']}")
    print(f"Downloaded bytes: {output_payload['total_bytes']}")
    print(f"Elapsed time: {output_payload['elapsed_seconds']}s")
    print(f"Wrote raw payload to {output_path}")


if __name__ == "__main__":
    main()
