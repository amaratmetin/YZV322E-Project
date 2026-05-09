from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import psycopg2
from psycopg2.extras import execute_values

from common import latest_file, load_dotenv, read_json, timestamped_path


RAW_DIR = Path("raw_data")
CLEAN_DIR = Path("clean_data")


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


def normalize_sensors(locations_payload: dict[str, Any]) -> pl.DataFrame:
    fetched_at = datetime.now(UTC)
    rows: list[dict[str, Any]] = []

    for location in locations_payload.get("results", []):
        location_id = int(location["id"])
        location_first = location.get("datetimeFirst") or {}
        location_last = location.get("datetimeLast") or {}
        for sensor in location.get("sensors", []):
            parameter = sensor.get("parameter") or {}
            rows.append(
                {
                    "sensor_id": sensor.get("id"),
                    "location_id": location_id,
                    "name": sensor.get("name"),
                    "parameter": parameter.get("name") if isinstance(parameter, dict) else None,
                    "units": parameter.get("units") if isinstance(parameter, dict) else None,
                    "datetime_first_utc": parse_datetime(location_first.get("utc")),
                    "datetime_last_utc": parse_datetime(location_last.get("utc")),
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


def empty_measurements_frame() -> pl.DataFrame:
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


def read_archive_csv(path: Path, target_date: str, ingested_at: datetime) -> pl.DataFrame:
    frame = pl.read_csv(path)
    if frame.is_empty():
        return empty_measurements_frame()

    return (
        frame.rename({"sensors_id": "sensor_id", "lat": "latitude", "lon": "longitude"})
        .with_columns(
            pl.lit(target_date).str.to_date().alias("measurement_date"),
            pl.col("datetime")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .dt.convert_time_zone("UTC")
            .alias("period_start_utc"),
            pl.col("datetime")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%z", strict=False)
            .alias("period_start_local"),
            pl.col("value").cast(pl.Float64, strict=False),
            pl.lit(None).cast(pl.Utf8).alias("period_label"),
            pl.lit(None).cast(pl.Utf8).alias("period_interval"),
            pl.lit(ingested_at).alias("ingested_at"),
        )
        .with_columns(
            pl.col("period_start_utc").alias("period_end_utc"),
            pl.col("period_start_local").alias("period_end_local"),
        )
        .select(
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
        )
    )


def normalize_measurements(measurements_payload: dict[str, Any]) -> pl.DataFrame:
    ingested_at = datetime.now(UTC)
    target_date = measurements_payload["target_date"]
    frames = [
        read_archive_csv(Path(result["path"]), target_date, ingested_at)
        for result in measurements_payload.get("results", [])
        if result.get("status") == "downloaded" and result.get("path")
    ]

    if not frames:
        return empty_measurements_frame()

    return (
        pl.concat(frames, how="vertical_relaxed")
        .filter(
            pl.col("parameter").is_not_null()
            & pl.col("value").is_not_null()
            & pl.col("period_start_utc").is_not_null()
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


def write_clean_csv(frame: pl.DataFrame, output_dir: Path, prefix: str) -> Path:
    output_path = timestamped_path(output_dir, prefix, ".csv")
    frame.write_csv(output_path)
    return output_path


def main() -> None:
    load_dotenv()

    locations_file = latest_file(RAW_DIR, "locations_*.json")
    measurements_file = latest_file(RAW_DIR, "measurements_*.json")

    locations_payload = read_json(locations_file)
    locations = normalize_locations(locations_payload)
    sensors = normalize_sensors(locations_payload)
    measurements_payload = read_json(measurements_file)
    measurements = normalize_measurements(measurements_payload)
    summaries = daily_summaries(measurements)

    paths = {
        "locations_csv": write_clean_csv(locations, CLEAN_DIR, "locations"),
        "sensors_csv": write_clean_csv(sensors, CLEAN_DIR, "sensors"),
        "measurements_csv": write_clean_csv(measurements, CLEAN_DIR, "measurements"),
        "daily_summaries_csv": write_clean_csv(summaries, CLEAN_DIR, "daily_summaries"),
    }

    print(f"Cleaned locations: {locations.height}")
    print(f"Cleaned sensors: {sensors.height}")
    print(f"Cleaned measurements: {measurements.height}")
    print(f"Daily summaries: {summaries.height}")
    for label, path in paths.items():
        print(f"{label}: {path}")

    loaded = load_database(locations, sensors, measurements, summaries)
    print(f"Loaded rows: {loaded}")


if __name__ == "__main__":
    main()
