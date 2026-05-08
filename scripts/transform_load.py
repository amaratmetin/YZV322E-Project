from __future__ import annotations

import argparse
import os
from datetime import date
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import psycopg2
from psycopg2.extras import execute_values

from common import latest_file, load_dotenv, read_json, timestamped_path, write_json


TOKYO_TZ = ZoneInfo("Asia/Tokyo")


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def coordinates(payload: dict[str, Any]) -> tuple[float | None, float | None]:
    coordinate_payload = payload.get("coordinates") or {}
    return coordinate_payload.get("latitude"), coordinate_payload.get("longitude")


def normalize_locations(locations_payload: dict[str, Any]) -> pl.DataFrame:
    fetched_at = datetime.now(UTC)
    rows: list[dict[str, Any]] = []

    for location in locations_payload.get("results", []):
        latitude, longitude = coordinates(location)
        country = location.get("country") or {}
        rows.append(
            {
                "location_id": location.get("id"),
                "name": location.get("name"),
                "locality": location.get("locality"),
                "country_code": country.get("code") if isinstance(country, dict) else None,
                "latitude": latitude,
                "longitude": longitude,
                "sensor_count": len(location.get("sensors") or []),
                "fetched_at": fetched_at,
            }
        )

    if not rows:
        return pl.DataFrame(
            schema={
                "location_id": pl.Int64,
                "name": pl.Utf8,
                "locality": pl.Utf8,
                "country_code": pl.Utf8,
                "latitude": pl.Float64,
                "longitude": pl.Float64,
                "sensor_count": pl.Int64,
                "fetched_at": pl.Datetime(time_zone="UTC"),
            }
        )

    return pl.DataFrame(rows)


def normalize_sensors(sensors_payload: dict[str, Any]) -> pl.DataFrame:
    fetched_at = datetime.now(UTC)
    rows: list[dict[str, Any]] = []

    for location_group in sensors_payload.get("results", []):
        location_id = int(location_group["location_id"])
        for sensor in location_group.get("sensors", []):
            parameter = sensor.get("parameter") or {}
            datetime_first = sensor.get("datetimeFirst") or {}
            datetime_last = sensor.get("datetimeLast") or {}
            rows.append(
                {
                    "sensor_id": sensor.get("id"),
                    "location_id": location_id,
                    "name": sensor.get("name"),
                    "parameter": parameter.get("name") if isinstance(parameter, dict) else None,
                    "units": parameter.get("units") if isinstance(parameter, dict) else None,
                    "datetime_first_utc": parse_datetime(datetime_first.get("utc")),
                    "datetime_last_utc": parse_datetime(datetime_last.get("utc")),
                    "fetched_at": fetched_at,
                }
            )

    if not rows:
        return pl.DataFrame(
            schema={
                "sensor_id": pl.Int64,
                "location_id": pl.Int64,
                "name": pl.Utf8,
                "parameter": pl.Utf8,
                "units": pl.Utf8,
                "datetime_first_utc": pl.Datetime(time_zone="UTC"),
                "datetime_last_utc": pl.Datetime(time_zone="UTC"),
                "fetched_at": pl.Datetime(time_zone="UTC"),
            }
        )

    return (
        pl.DataFrame(rows)
        .filter(pl.col("sensor_id").is_not_null() & pl.col("parameter").is_not_null())
        .unique(subset=["sensor_id"], keep="last")
    )


def normalize_measurements(measurements_payload: dict[str, Any]) -> pl.DataFrame:
    ingested_at = datetime.now(UTC)
    rows: list[dict[str, Any]] = []

    for sensor_group in measurements_payload.get("results", []):
        target_date = sensor_group["target_date"]
        location_id = int(sensor_group["location_id"])
        sensor_id = int(sensor_group["sensor_id"])

        for measurement in sensor_group.get("measurements", []):
            parameter = measurement.get("parameter") or {}
            period = measurement.get("period") or {}
            period_start = period.get("datetimeFrom") or {}
            period_end = period.get("datetimeTo") or {}
            latitude, longitude = coordinates(measurement)
            rows.append(
                {
                    "location_id": location_id,
                    "sensor_id": sensor_id,
                    "parameter": parameter.get("name") if isinstance(parameter, dict) else None,
                    "units": parameter.get("units") if isinstance(parameter, dict) else None,
                    "value": measurement.get("value"),
                    "measurement_date": target_date,
                    "period_start_utc": parse_datetime(period_start.get("utc")),
                    "period_end_utc": parse_datetime(period_end.get("utc")),
                    "period_start_local": parse_datetime(period_start.get("local")),
                    "period_end_local": parse_datetime(period_end.get("local")),
                    "period_label": period.get("label"),
                    "period_interval": period.get("interval"),
                    "latitude": latitude,
                    "longitude": longitude,
                    "ingested_at": ingested_at,
                }
            )

    if not rows:
        return pl.DataFrame(
            schema={
                "location_id": pl.Int64,
                "sensor_id": pl.Int64,
                "parameter": pl.Utf8,
                "units": pl.Utf8,
                "value": pl.Float64,
                "measurement_date": pl.Date,
                "period_start_utc": pl.Datetime(time_zone="UTC"),
                "period_end_utc": pl.Datetime(time_zone="UTC"),
                "period_start_local": pl.Datetime(time_zone="UTC"),
                "period_end_local": pl.Datetime(time_zone="UTC"),
                "period_label": pl.Utf8,
                "period_interval": pl.Utf8,
                "latitude": pl.Float64,
                "longitude": pl.Float64,
                "ingested_at": pl.Datetime(time_zone="UTC"),
            }
        )

    return (
        pl.DataFrame(rows)
        .filter(
            pl.col("parameter").is_not_null()
            & pl.col("value").is_not_null()
            & pl.col("period_start_utc").is_not_null()
            & pl.col("period_end_utc").is_not_null()
        )
        .with_columns(
            pl.col("measurement_date").str.to_date(),
            pl.col("value").cast(pl.Float64),
        )
        .unique(subset=["sensor_id", "period_start_utc", "period_end_utc"], keep="last")
    )


def daily_summaries(measurements: pl.DataFrame) -> pl.DataFrame:
    if measurements.is_empty():
        return pl.DataFrame(
            schema={
                "summary_date": pl.Date,
                "parameter": pl.Utf8,
                "measurement_count": pl.Int64,
                "avg_value": pl.Float64,
                "min_value": pl.Float64,
                "max_value": pl.Float64,
                "p50_value": pl.Float64,
            }
        )

    return (
        measurements.rename({"measurement_date": "summary_date"})
        .group_by("summary_date", "parameter")
        .agg(
            pl.len().alias("measurement_count"),
            pl.col("value").mean().alias("avg_value"),
            pl.col("value").min().alias("min_value"),
            pl.col("value").max().alias("max_value"),
            pl.col("value").median().alias("p50_value"),
        )
        .sort(["summary_date", "parameter"])
    )


def postgres_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "openaq"),
        user=os.getenv("POSTGRES_USER", "openaq"),
        password=os.getenv("POSTGRES_PASSWORD", "openaq"),
    )


def upsert_frame(connection: psycopg2.extensions.connection, table: str, columns: list[str], rows: list[dict[str, Any]], conflict: str) -> int:
    if not rows:
        return 0

    values = [[row.get(column) for column in columns] for row in rows]
    insert_columns = ", ".join(columns)
    update_columns = ", ".join(f"{column}=EXCLUDED.{column}" for column in columns if column not in conflict)
    query = f"""
        INSERT INTO {table} ({insert_columns})
        VALUES %s
        ON CONFLICT {conflict}
        DO UPDATE SET {update_columns}
    """
    with connection.cursor() as cursor:
        execute_values(cursor, query, values)
    return len(rows)


def insert_measurements(connection: psycopg2.extensions.connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0

    columns = [
        "location_id",
        "sensor_id",
        "parameter",
        "units",
        "value",
        "measurement_date",
        "period_start_utc",
        "period_end_utc",
        "period_start_local",
        "period_end_local",
        "period_label",
        "period_interval",
        "latitude",
        "longitude",
        "ingested_at",
    ]
    values = [[row.get(column) for column in columns] for row in rows]
    query = f"""
        INSERT INTO measurements ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (sensor_id, period_start_utc, period_end_utc)
        DO NOTHING
    """
    with connection.cursor() as cursor:
        execute_values(cursor, query, values)
    return len(rows)


def load_database(locations: pl.DataFrame, sensors: pl.DataFrame, measurements: pl.DataFrame, summaries: pl.DataFrame) -> dict[str, int]:
    with postgres_connection() as connection:
        loaded = {
            "locations": upsert_frame(
                connection,
                "locations",
                locations.columns,
                locations.to_dicts(),
                "(location_id)",
            ),
            "sensors": upsert_frame(
                connection,
                "sensors",
                sensors.columns,
                sensors.to_dicts(),
                "(sensor_id)",
            ),
            "measurements": insert_measurements(connection, measurements.to_dicts()),
            "daily_summaries": upsert_frame(
                connection,
                "daily_summaries",
                summaries.columns,
                summaries.to_dicts(),
                "(summary_date, parameter)",
            ),
        }
        connection.commit()
    return loaded


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


def mark_loaded_backfill_dates(measurements_payload: dict[str, Any], state_file: Path) -> None:
    today = datetime.now(TOKYO_TZ).date()
    target_dates = {
        date.fromisoformat(value)
        for value in measurements_payload.get("target_dates", [])
    }
    completed_dates = read_completed_dates(state_file)
    completed_dates.update(value for value in target_dates if value != today)
    write_completed_dates(state_file, completed_dates)


def write_clean_csv(frame: pl.DataFrame, output_dir: Path, prefix: str) -> Path:
    output_path = timestamped_path(output_dir, prefix, ".csv")
    frame.write_csv(output_path)
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform raw OpenAQ JSON and load cleaned rows into PostgreSQL.")
    parser.add_argument("--locations-file", type=Path)
    parser.add_argument("--sensors-file", type=Path)
    parser.add_argument("--measurements-file", type=Path)
    parser.add_argument("--raw-dir", type=Path, default=Path("raw_data"))
    parser.add_argument("--clean-dir", type=Path, default=Path("clean_data"))
    parser.add_argument("--state-file", type=Path, default=Path("raw_data/ingestion_state.json"))
    parser.add_argument("--mark-complete", action="store_true")
    parser.add_argument("--skip-load", action="store_true")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()

    locations_file = args.locations_file or latest_file(args.raw_dir, "locations_*.json")
    sensors_file = args.sensors_file or latest_file(args.raw_dir, "sensors_*.json")
    measurements_file = args.measurements_file or latest_file(args.raw_dir, "measurements_*.json")

    locations = normalize_locations(read_json(locations_file))
    sensors = normalize_sensors(read_json(sensors_file))
    measurements_payload = read_json(measurements_file)
    measurements = normalize_measurements(measurements_payload)
    summaries = daily_summaries(measurements)

    paths = {
        "locations_csv": write_clean_csv(locations, args.clean_dir, "locations"),
        "sensors_csv": write_clean_csv(sensors, args.clean_dir, "sensors"),
        "measurements_csv": write_clean_csv(measurements, args.clean_dir, "measurements"),
        "daily_summaries_csv": write_clean_csv(summaries, args.clean_dir, "daily_summaries"),
    }

    print(f"Cleaned locations: {locations.height}")
    print(f"Cleaned sensors: {sensors.height}")
    print(f"Cleaned measurements: {measurements.height}")
    print(f"Daily summaries: {summaries.height}")
    for label, path in paths.items():
        print(f"{label}: {path}")

    if args.skip_load:
        return

    loaded = load_database(locations, sensors, measurements, summaries)
    print(f"Loaded rows: {loaded}")

    if args.mark_complete:
        mark_loaded_backfill_dates(measurements_payload, args.state_file)
        print(f"Updated ingestion state at {args.state_file}")


if __name__ == "__main__":
    main()
