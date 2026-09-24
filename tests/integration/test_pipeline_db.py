"""End-to-end checks against a real Postgres. Run with: pytest -m integration

Requires DATABASE_URL pointing at a disposable database: the tests drop and recreate the
pipeline schemas.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from decimal import Decimal

import psycopg
import pytest

from tests.conftest import make_response
from weather_pipeline import db, load, transform
from weather_pipeline.cities import CITIES
from weather_pipeline.validate import DailyWeather, parse_daily

pytestmark = pytest.mark.integration


@pytest.fixture
def conn():
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set")
    with psycopg.connect(url, autocommit=True) as c:
        c.execute(
            "DROP SCHEMA IF EXISTS raw, staging, curated CASCADE;"
            "DROP TABLE IF EXISTS public.schema_migrations;"
        )
    with db.connect(url) as c:
        db.apply_migrations(c)
        with c.transaction():
            load.upsert_cities(c, CITIES.values())
        yield c


def _load(conn, payload):
    response = make_response(payload)
    with conn.transaction():
        response_id = load.insert_raw_response(conn, response)
    result = parse_daily(response)
    with conn.transaction():
        load.insert_rejections(conn, result.rejected, response_id)
        stats = load.upsert_daily_weather(conn, result.rows, response_id)
    return response_id, stats


def _count(conn, table: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_migrations_are_idempotent(conn):
    assert db.apply_migrations(conn) == []


def test_load_is_idempotent(conn, madrid_payload):
    first_id, first = _load(conn, madrid_payload)
    assert (first.inserted, first.updated, first.unchanged) == (10, 0, 0)

    # Same data, different generationtime_ms: no new raw row, no staging writes.
    second_id, second = _load(conn, dict(madrid_payload, generationtime_ms=123.4))
    assert second_id == first_id
    assert (second.inserted, second.updated, second.unchanged) == (0, 0, 10)
    assert _count(conn, "raw.open_meteo_responses") == 1
    assert _count(conn, "staging.daily_weather") == 10


def test_upstream_revision_updates_only_changed_rows(conn, madrid_payload):
    _load(conn, madrid_payload)
    revised = dict(madrid_payload, daily=dict(madrid_payload["daily"]))
    revised["daily"]["temperature_2m_max"] = list(revised["daily"]["temperature_2m_max"])
    revised["daily"]["temperature_2m_max"][4] += 1.0

    new_id, stats = _load(conn, revised)

    assert (stats.inserted, stats.updated, stats.unchanged) == (0, 1, 9)
    assert _count(conn, "raw.open_meteo_responses") == 2
    source = conn.execute(
        "SELECT source_response_id FROM staging.daily_weather WHERE date = '2024-07-05'"
    ).fetchone()[0]
    assert source == new_id


def test_rejected_rows_are_quarantined_not_loaded(conn, madrid_payload):
    madrid_payload["daily"]["precipitation_sum"][0] = -1.0
    _load(conn, madrid_payload)
    assert _count(conn, "staging.daily_weather") == 9
    assert _count(conn, "raw.rejected_daily_rows") == 1


def test_watermark(conn, madrid_payload):
    assert load.get_watermark(conn, "madrid") is None
    _load(conn, madrid_payload)
    assert load.get_watermark(conn, "madrid") == date(2024, 7, 10)


def test_curated_rolling_windows_respect_calendar_gaps(conn):
    # Mean temps 10, 11, ..., for Jan 1-10, with Jan 5 missing.
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(10)]
    rows = [
        DailyWeather(
            city_id="madrid",
            date=d,
            temperature_max_c=10 + i + 5,
            temperature_min_c=10 + i - 5,
            temperature_mean_c=10 + i,
            precipitation_mm=1.0,
            wind_speed_max_kmh=20,
        )
        for i, d in enumerate(days)
        if d != date(2024, 1, 5)
    ]
    with conn.transaction():
        response_id = load.insert_raw_response(conn, make_response({"daily": {}}))
        load.upsert_daily_weather(conn, rows, response_id)
        written = transform.refresh_daily_city_stats(conn, ["madrid"])
    assert written == 9

    stats = {
        r[0]: r[1:]
        for r in conn.execute(
            """
            SELECT date, temperature_mean_7d_avg_c, days_in_7d_window,
                   precipitation_7d_sum_mm, temperature_mean_change_c, temperature_range_c
            FROM curated.daily_city_stats ORDER BY date
            """
        )
    }
    # Jan 7 window = Jan 1..7 minus Jan 5 -> means 10,11,12,13,15,16 -> avg 12.83
    assert stats[date(2024, 1, 7)][:3] == (Decimal("12.83"), 6, Decimal("6.00"))
    # Day-over-day change is null across the gap, populated otherwise.
    assert stats[date(2024, 1, 6)][3] is None
    assert stats[date(2024, 1, 7)][3] == Decimal("1.00")
    assert stats[date(2024, 1, 7)][4] == Decimal("10.00")

    with conn.transaction():
        assert transform.refresh_daily_city_stats(conn, ["madrid"]) == 0
