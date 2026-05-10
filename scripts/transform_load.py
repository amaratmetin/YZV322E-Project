from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl
import psycopg2
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import execute_values

from common import latest_file, load_dotenv, read_json, timestamped_path


RAW_DIR = Path("raw_data")
CLEAN_DIR = Path("clean_data")

MEASUREMENTS_INDEX_TEMPLATE = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "location_id": {"type": "long"},
            "location_name": {"type": "keyword"},
            "sensor_id": {"type": "long"},
            "parameter": {"type": "keyword"},
            "units": {"type": "keyword"},
            "value": {"type": "double"},
            "measurement_date": {"type": "date"},
            "measurement_time_utc": {"type": "date"},
            "latitude": {"type": "double"},
            "longitude": {"type": "double"},
            "location": {"type": "geo_point"},
            "ingested_at": {"type": "date"},
        }
    },
}

DAILY_SUMMARIES_INDEX_TEMPLATE = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "summary_date": {"type": "date"},
            "parameter": {"type": "keyword"},
            "measurement_count": {"type": "long"},
            "avg_value": {"type": "double"},
            "min_value": {"type": "double"},
            "max_value": {"type": "double"},
            "p50_value": {"type": "double"},
        }
    },
}


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
            "measurement_time_utc": pl.Datetime(time_zone="UTC"),
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
            .alias("measurement_time_utc"),
            pl.col("value").cast(pl.Float64, strict=False),
            pl.lit(ingested_at).alias("ingested_at"),
        )
        .with_columns(
            pl.col("measurement_time_utc")
        )
        .select(
            "location_id",
            "sensor_id",
            "parameter",
            "units",
            "value",
            "measurement_date",
            "measurement_time_utc",
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
            & pl.col("measurement_time_utc").is_not_null()
        )
        .unique(subset=["sensor_id", "measurement_time_utc"], keep="last")
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
        "measurement_time_utc",
        "latitude",
        "longitude",
        "ingested_at",
    ]
    values = [[row.get(column) for column in columns] for row in rows]
    query = f"""
        INSERT INTO measurements ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (sensor_id, measurement_time_utc)
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


def measurements_file_for_date(directory: Path, target_date: str) -> Path:
    matching_payloads: list[Path] = []
    for path in sorted(directory.glob("measurements_*.json")):
        try:
            payload = read_json(path)
        except (json.JSONDecodeError, OSError):
            continue
        if payload.get("target_date") == target_date:
            matching_payloads.append(path)
    if not matching_payloads:
        raise SystemExit(f"No measurements file found for target_date={target_date} in {directory}")

    date_named_matches = [path for path in matching_payloads if path.name.startswith(f"measurements_{target_date}_")]
    candidates = date_named_matches or matching_payloads
    return max(candidates, key=lambda path: path.stat().st_mtime)


def elasticsearch_client() -> Elasticsearch:
    return Elasticsearch(
        os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200"),
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )


def ensure_index(client: Elasticsearch, index: str, body: dict[str, Any]) -> None:
    if not client.indices.exists(index=index):
        client.indices.create(index=index, **body)


def jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def measurement_action(index: str, row: dict[str, Any], location_names: dict[int, str]) -> dict[str, Any]:
    doc = {key: jsonable(value) for key, value in row.items()}
    latitude = doc.get("latitude")
    longitude = doc.get("longitude")
    if latitude is not None and longitude is not None:
        doc["location"] = {"lat": latitude, "lon": longitude}
    location_id = row.get("location_id")
    if location_id is not None:
        name = location_names.get(int(location_id))
        if name:
            doc["location_name"] = name
    measurement_time = row.get("measurement_time_utc")
    measurement_key = measurement_time.isoformat() if isinstance(measurement_time, datetime) else measurement_time
    return {
        "_index": index,
        "_id": f"{row.get('sensor_id')}-{measurement_key}",
        "_source": doc,
    }


def summary_action(index: str, row: dict[str, Any]) -> dict[str, Any]:
    doc = {key: jsonable(value) for key, value in row.items()}
    summary_date = row.get("summary_date")
    summary_key = summary_date.isoformat() if hasattr(summary_date, "isoformat") else summary_date
    return {
        "_index": index,
        "_id": f"{summary_key}-{row.get('parameter')}",
        "_source": doc,
    }


def location_name_lookup(locations: pl.DataFrame) -> dict[int, str]:
    if locations.is_empty():
        return {}
    return {
        int(row["location_id"]): row["name"]
        for row in locations.select("location_id", "name").to_dicts()
        if row.get("location_id") is not None and row.get("name")
    }


def index_to_elasticsearch(locations: pl.DataFrame, measurements: pl.DataFrame, summaries: pl.DataFrame) -> dict[str, int]:
    measurements_index = os.getenv("ES_MEASUREMENTS_INDEX", "aq-measurements")
    summaries_index = os.getenv("ES_DAILY_SUMMARIES_INDEX", "aq-daily-summaries")

    client = elasticsearch_client()
    ensure_index(client, measurements_index, MEASUREMENTS_INDEX_TEMPLATE)
    ensure_index(client, summaries_index, DAILY_SUMMARIES_INDEX_TEMPLATE)

    location_names = location_name_lookup(locations)
    measurement_actions = [measurement_action(measurements_index, row, location_names) for row in measurements.to_dicts()]
    summary_actions = [summary_action(summaries_index, row) for row in summaries.to_dicts()]

    measurements_indexed = 0
    summaries_indexed = 0
    if measurement_actions:
        measurements_indexed, _ = helpers.bulk(client, measurement_actions, chunk_size=1000, raise_on_error=False)
    if summary_actions:
        summaries_indexed, _ = helpers.bulk(client, summary_actions, chunk_size=500, raise_on_error=False)

    client.indices.refresh(index=f"{measurements_index},{summaries_index}")
    client.close()
    return {measurements_index: measurements_indexed, summaries_index: summaries_indexed}


def main() -> None:
    load_dotenv()

    locations_file = latest_file(RAW_DIR, "locations_*.json")
    target_date = os.getenv("TARGET_DATE", "").strip()
    measurements_file = (
        measurements_file_for_date(RAW_DIR, target_date)
        if target_date
        else latest_file(RAW_DIR, "measurements_*.json")
    )

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

    indexed = index_to_elasticsearch(locations, measurements, summaries)
    print(f"Indexed rows: {indexed}")


if __name__ == "__main__":
    main()
