"""Runs the real Prefect flow against Postgres, with the HTTP layer mocked."""

from __future__ import annotations

import os
from datetime import date

import psycopg
import pytest
from prefect.testing.utilities import prefect_test_harness

from tests.conftest import make_response
from weather_pipeline.extract import OpenMeteoClient
from weather_pipeline.flows import ingest_daily_weather

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def prefect_harness():
    with prefect_test_harness():
        yield


@pytest.fixture
def database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set")
    with psycopg.connect(url, autocommit=True) as c:
        c.execute(
            "DROP SCHEMA IF EXISTS raw, staging, curated CASCADE;"
            "DROP TABLE IF EXISTS public.schema_migrations;"
        )
    return url


def test_flow_loads_all_layers_and_is_rerunnable(database_url, madrid_payload, monkeypatch):
    calls: list[tuple[date, date]] = []

    def fake_fetch(self, city, start, end):
        calls.append((start, end))
        return make_response(madrid_payload, city_id=city.city_id, start=start, end=end)

    monkeypatch.setattr(OpenMeteoClient, "fetch_daily", fake_fetch)

    window = {"city_ids": ["madrid"], "start_date": date(2024, 7, 1), "end_date": date(2024, 7, 10)}
    first = ingest_daily_weather(**window)
    second = ingest_daily_weather(**window)

    assert first[0]["inserted"] == 10
    assert (second[0]["inserted"], second[0]["updated"], second[0]["unchanged"]) == (0, 0, 10)
    assert len(calls) == 2

    with psycopg.connect(database_url) as c:
        counts = c.execute(
            """
            SELECT (SELECT count(*) FROM raw.open_meteo_responses),
                   (SELECT count(*) FROM staging.daily_weather),
                   (SELECT count(*) FROM curated.daily_city_stats)
            """
        ).fetchone()
    assert counts == (1, 10, 10)
