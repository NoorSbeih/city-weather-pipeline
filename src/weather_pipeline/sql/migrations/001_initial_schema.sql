-- Layers:
--   raw      : API responses exactly as received (JSONB) + quarantined rows. Append-only, replayable.
--   staging  : typed, validated, one row per (city, day). Upserted.
--   curated  : analytics-ready tables derived from staging with SQL. Rebuilt idempotently.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE raw.open_meteo_responses (
    response_id     BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL,
    city_id         TEXT        NOT NULL,
    start_date      DATE        NOT NULL,
    end_date        DATE        NOT NULL,
    request_params  JSONB       NOT NULL,
    payload         JSONB       NOT NULL,
    payload_sha256  TEXT        NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Re-fetching an identical payload bumps last_seen_at instead of storing a duplicate.
    UNIQUE (city_id, payload_sha256)
);

CREATE TABLE raw.rejected_daily_rows (
    rejection_id    BIGSERIAL PRIMARY KEY,
    response_id     BIGINT      NOT NULL REFERENCES raw.open_meteo_responses (response_id),
    city_id         TEXT        NOT NULL,
    date            TEXT,
    reason          TEXT        NOT NULL,
    record          JSONB       NOT NULL,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (response_id, date)
);

CREATE TABLE curated.dim_city (
    city_id         TEXT PRIMARY KEY,
    name            TEXT             NOT NULL,
    country_code    CHAR(2)          NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    timezone        TEXT             NOT NULL,
    updated_at      TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE TABLE staging.daily_weather (
    city_id             TEXT    NOT NULL REFERENCES curated.dim_city (city_id),
    date                DATE    NOT NULL,
    temperature_max_c   NUMERIC(5, 2),
    temperature_min_c   NUMERIC(5, 2),
    temperature_mean_c  NUMERIC(5, 2),
    precipitation_mm    NUMERIC(7, 2),
    wind_speed_max_kmh  NUMERIC(6, 2),
    source_response_id  BIGINT      NOT NULL REFERENCES raw.open_meteo_responses (response_id),
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, date)
);

CREATE TABLE curated.daily_city_stats (
    city_id                     TEXT NOT NULL REFERENCES curated.dim_city (city_id),
    date                        DATE NOT NULL,
    temperature_max_c           NUMERIC(5, 2),
    temperature_min_c           NUMERIC(5, 2),
    temperature_mean_c          NUMERIC(5, 2),
    temperature_range_c         NUMERIC(5, 2),
    temperature_mean_7d_avg_c   NUMERIC(5, 2),
    temperature_mean_30d_avg_c  NUMERIC(5, 2),
    temperature_anomaly_30d_c   NUMERIC(5, 2),
    temperature_mean_change_c   NUMERIC(5, 2),
    precipitation_mm            NUMERIC(7, 2),
    precipitation_7d_sum_mm     NUMERIC(8, 2),
    precipitation_30d_sum_mm    NUMERIC(8, 2),
    wind_speed_max_kmh          NUMERIC(6, 2),
    days_in_7d_window           SMALLINT NOT NULL,
    days_in_30d_window          SMALLINT NOT NULL,
    refreshed_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, date)
);

CREATE INDEX daily_city_stats_date_idx ON curated.daily_city_stats (date);
