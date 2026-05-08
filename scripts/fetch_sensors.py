from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

from common import latest_file, load_dotenv, openaq_get, read_json, timestamped_path, write_json


def fetch_location_sensors(location_id: int) -> list[dict[str, Any]]:
    payload = openaq_get(f"/locations/{location_id}/sensors", {})
    return payload.get("results", [])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch raw OpenAQ sensor data for saved locations.")
    parser.add_argument("--locations-file", type=Path)
    parser.add_argument("--raw-dir", type=Path, default=Path("raw_data"))
    parser.add_argument("--max-locations", type=int, help="Optional safety limit for test runs.")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    locations_file = args.locations_file or latest_file(args.raw_dir, "locations_*.json")
    locations_payload = read_json(locations_file)
    locations = locations_payload.get("results", [])
    if args.max_locations:
        locations = locations[: args.max_locations]

    sensor_groups: list[dict[str, Any]] = []
    total_sensors = 0
    for location in locations:
        location_id = int(location["id"])
        sensors = fetch_location_sensors(location_id)
        total_sensors += len(sensors)
        sensor_groups.append(
            {
                "location_id": location_id,
                "location_name": location.get("name"),
                "sensors": sensors,
            }
        )
        time.sleep(0.2)

    output_payload = {
        "source_locations_file": str(locations_file),
        "locations_processed": len(locations),
        "total_sensors": total_sensors,
        "results": sensor_groups,
    }
    output_path = timestamped_path(args.raw_dir, "sensors")
    write_json(output_payload, output_path)

    print(f"Processed {len(locations)} locations")
    print(f"Fetched {total_sensors} sensors")
    print(f"Wrote raw payload to {output_path}")


if __name__ == "__main__":
    main()
