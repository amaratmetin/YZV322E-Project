from __future__ import annotations

import argparse
import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from common import latest_file, load_dotenv, openaq_get, read_json, timestamped_path, write_json


TOKYO_TZ = ZoneInfo("Asia/Tokyo")


def fetch_sensor_measurements(sensor_id: int, start: datetime, end: datetime, limit: int) -> list[dict[str, Any]]:
    page = 1
    results: list[dict[str, Any]] = []

    while True:
        payload = openaq_get(
            f"/sensors/{sensor_id}/measurements",
            {
                "datetime_from": start.isoformat(),
                "datetime_to": end.isoformat(),
                "limit": limit,
                "page": page,
            },
        )
        page_results = payload.get("results", [])
        results.extend(page_results)

        if len(page_results) < limit:
            return results
        page += 1


def read_completed_dates(path: Path) -> set[date]:
    if not path.exists():
        return set()
    payload = read_json(path)
    return {date.fromisoformat(value) for value in payload.get("completed_dates", [])}


def write_completed_dates(path: Path, completed_dates: set[date]) -> None:
    write_json(
        {"completed_dates": [value.isoformat() for value in sorted(completed_dates)]},
        path,
    )


def target_dates(backfill_start: date, state_file: Path) -> list[date]:
    today = datetime.now(TOKYO_TZ).date()
    completed_dates = read_completed_dates(state_file)
    targets = [today]

    cursor = backfill_start
    while cursor < today:
        if cursor not in completed_dates:
            targets.append(cursor)
            break
        cursor += timedelta(days=1)

    return targets


def date_window(target_date: date) -> tuple[datetime, datetime]:
    start = datetime.combine(target_date, time.min, tzinfo=TOKYO_TZ)
    return start, start + timedelta(days=1)


def iter_sensors(sensors_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for location_group in sensors_payload.get("results", []):
        location_id = int(location_group["location_id"])
        for sensor in location_group.get("sensors", []):
            rows.append({"location_id": location_id, "sensor_id": int(sensor["id"])})
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch raw OpenAQ measurements for saved sensors.")
    parser.add_argument("--sensors-file", type=Path)
    parser.add_argument("--raw-dir", type=Path, default=Path("raw_data"))
    parser.add_argument("--state-file", type=Path, default=Path("raw_data/ingestion_state.json"))
    parser.add_argument(
        "--backfill-start-date",
        type=date.fromisoformat,
        default=date.fromisoformat(os.getenv("BACKFILL_START_DATE", "2026-05-01")),
    )
    parser.add_argument("--limit", type=int, default=int(os.getenv("OPENAQ_MEASUREMENT_LIMIT", "1000")))
    parser.add_argument("--max-sensors", type=int, help="Optional safety limit for test runs.")
    parser.add_argument("--mark-complete", action="store_true", help="Mark fetched non-today dates as completed.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    sensors_file = args.sensors_file or latest_file(args.raw_dir, "sensors_*.json")
    sensors_payload = read_json(sensors_file)
    sensors = iter_sensors(sensors_payload)
    if args.max_sensors:
        sensors = sensors[: args.max_sensors]

    dates = target_dates(args.backfill_start_date, args.state_file)
    measurement_groups: list[dict[str, Any]] = []
    total_measurements = 0

    for target_date in dates:
        start, end = date_window(target_date)
        for sensor in sensors:
            measurements = fetch_sensor_measurements(
                sensor_id=sensor["sensor_id"],
                start=start,
                end=end,
                limit=args.limit,
            )
            total_measurements += len(measurements)
            measurement_groups.append(
                {
                    "target_date": target_date.isoformat(),
                    "location_id": sensor["location_id"],
                    "sensor_id": sensor["sensor_id"],
                    "measurements": measurements,
                }
            )

    output_payload = {
        "source_sensors_file": str(sensors_file),
        "target_dates": [value.isoformat() for value in dates],
        "sensors_processed": len(sensors),
        "total_measurements": total_measurements,
        "results": measurement_groups,
    }
    output_path = timestamped_path(args.raw_dir, "measurements")
    write_json(output_payload, output_path)

    if args.mark_complete:
        completed_dates = read_completed_dates(args.state_file)
        today = datetime.now(TOKYO_TZ).date()
        completed_dates.update(value for value in dates if value != today)
        write_completed_dates(args.state_file, completed_dates)

    print(f"Target dates: {', '.join(output_payload['target_dates'])}")
    print(f"Processed {len(sensors)} sensors")
    print(f"Fetched {total_measurements} measurements")
    print(f"Wrote raw payload to {output_path}")


if __name__ == "__main__":
    main()
