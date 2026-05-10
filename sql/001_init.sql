CREATE TABLE IF NOT EXISTS locations (
    location_id BIGINT PRIMARY KEY,
    name TEXT,
    locality TEXT,
    country_code TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    sensor_count INTEGER DEFAULT 0,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id BIGINT PRIMARY KEY,
    location_id BIGINT NOT NULL REFERENCES locations(location_id),
    name TEXT,
    parameter TEXT NOT NULL,
    units TEXT,
    datetime_first_utc TIMESTAMPTZ,
    datetime_last_utc TIMESTAMPTZ,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS measurements (
    id BIGSERIAL PRIMARY KEY,
    location_id BIGINT NOT NULL REFERENCES locations(location_id),
    sensor_id BIGINT NOT NULL REFERENCES sensors(sensor_id),
    parameter TEXT NOT NULL,
    units TEXT,
    value DOUBLE PRECISION NOT NULL,
    measurement_date DATE NOT NULL,
    measurement_time_utc TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sensor_id, measurement_time_utc)
);

CREATE TABLE IF NOT EXISTS daily_summaries (
    summary_date DATE NOT NULL,
    parameter TEXT NOT NULL,
    measurement_count INTEGER NOT NULL,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    p50_value DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (summary_date, parameter)
);