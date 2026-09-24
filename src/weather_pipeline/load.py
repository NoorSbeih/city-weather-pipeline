"""Idempotent writes into the raw and staging layers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from weather_pipeline.cities import City
from weather_pipeline.extract import RawResponse
from weather_pipeline.validate import DailyWeather, Rejection

# Response fields that change on every call without the data changing.
_VOLATILE_PAYLOAD_KEYS = frozenset({"generationtime_ms"})

_STAGING_VALUE_COLUMNS = (
    "temperature_max_c",
    "temperature_min_c",
    "temperature_mean_c",
    "precipitation_mm",
    "wind_speed_max_kmh",
)


@dataclass(frozen=True)
class StagingLoadStats:
    inserted: int
    updated: int
    unchanged: int


def payload_fingerprint(payload: dict[str, Any]) -> str:
    stable = {k: v for k, v in payload.items() if k not in _VOLATILE_PAYLOAD_KEYS}
    canonical = json.dumps(stable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def upsert_cities(conn: psycopg.Connection, cities: Iterable[City]) -> None:
    conn.cursor().executemany(
        """
        INSERT INTO curated.dim_city (city_id, name, country_code, latitude, longitude, timezone)
        VALUES (%(city_id)s, %(name)s, %(country_code)s, %(latitude)s, %(longitude)s, %(timezone)s)
        ON CONFLICT (city_id) DO UPDATE SET
            name = EXCLUDED.name,
            country_code = EXCLUDED.country_code,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            timezone = EXCLUDED.timezone,
            updated_at = now()
        WHERE (dim_city.name, dim_city.country_code, dim_city.latitude,
               dim_city.longitude, dim_city.timezone)
              IS DISTINCT FROM
              (EXCLUDED.name, EXCLUDED.country_code, EXCLUDED.latitude,
               EXCLUDED.longitude, EXCLUDED.timezone)
        """,
        [c.__dict__ for c in cities],
    )


def get_watermark(conn: psycopg.Connection, city_id: str) -> date | None:
    """Latest day already loaded into staging for this city."""
    row = conn.execute(
        "SELECT max(date) FROM staging.daily_weather WHERE city_id = %s", (city_id,)
    ).fetchone()
    return row[0] if row else None


def insert_raw_response(conn: psycopg.Connection, response: RawResponse) -> int:
    """Store the payload verbatim; returns response_id (existing one if payload is unchanged)."""
    row = conn.execute(
        """
        INSERT INTO raw.open_meteo_responses
            (source, city_id, start_date, end_date, request_params, payload, payload_sha256)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, payload_sha256) DO UPDATE SET last_seen_at = now()
        RETURNING response_id
        """,
        (
            response.source,
            response.city_id,
            response.start_date,
            response.end_date,
            Jsonb(response.request_params),
            Jsonb(response.payload),
            payload_fingerprint(response.payload),
        ),
    ).fetchone()
    assert row is not None
    return row[0]


def insert_rejections(
    conn: psycopg.Connection, rejections: Iterable[Rejection], response_id: int
) -> None:
    conn.cursor().executemany(
        """
        INSERT INTO raw.rejected_daily_rows (response_id, city_id, date, reason, record)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (response_id, date) DO NOTHING
        """,
        [(response_id, r.city_id, r.date, r.reason, Jsonb(r.record)) for r in rejections],
    )


def upsert_daily_weather(
    conn: psycopg.Connection, rows: list[DailyWeather], response_id: int
) -> StagingLoadStats:
    """Bulk-load via COPY into a temp table, then MERGE-style upsert into staging.

    Rows whose values are unchanged are not rewritten, so re-running the same window is a no-op.
    """
    if not rows:
        return StagingLoadStats(inserted=0, updated=0, unchanged=0)

    cols = ("city_id", "date", *_STAGING_VALUE_COLUMNS)
    conn.execute(
        f"""
        CREATE TEMP TABLE _daily_weather_load AS
        SELECT {", ".join(cols)} FROM staging.daily_weather WITH NO DATA
        """
    )
    with conn.cursor().copy(f"COPY _daily_weather_load ({', '.join(cols)}) FROM STDIN") as copy:
        for r in rows:
            copy.write_row(tuple(getattr(r, c) for c in cols))

    set_clause = ",\n            ".join(f"{c} = EXCLUDED.{c}" for c in _STAGING_VALUE_COLUMNS)
    target_values = ", ".join(f"t.{c}" for c in _STAGING_VALUE_COLUMNS)
    new_values = ", ".join(f"EXCLUDED.{c}" for c in _STAGING_VALUE_COLUMNS)
    results = conn.execute(
        f"""
        INSERT INTO staging.daily_weather AS t ({", ".join(cols)}, source_response_id)
        SELECT {", ".join(cols)}, %s FROM _daily_weather_load
        ON CONFLICT (city_id, date) DO UPDATE SET
            {set_clause},
            source_response_id = EXCLUDED.source_response_id,
            updated_at = now()
        WHERE ({target_values}) IS DISTINCT FROM ({new_values})
        RETURNING (xmax = 0) AS inserted
        """,
        (response_id,),
    ).fetchall()
    conn.execute("DROP TABLE _daily_weather_load")

    inserted = sum(1 for (was_insert,) in results if was_insert)
    updated = len(results) - inserted
    return StagingLoadStats(
        inserted=inserted, updated=updated, unchanged=len(rows) - inserted - updated
    )
