"""API route tests with the database layer mocked (no network, no Postgres)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from weather_pipeline.api import create_app


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    sample_cities = [
        {
            "city_id": "madrid",
            "name": "Madrid",
            "country_code": "ES",
            "latitude": 40.4,
            "longitude": -3.7,
            "timezone": "Europe/Madrid",
            "latest_date": "2024-07-10",
            "day_count": 10,
            "in_registry": True,
        }
    ]
    sample_metric = {
        "city_id": "madrid",
        "date": "2024-07-10",
        "temperature_max_c": 36.0,
        "temperature_min_c": 20.0,
        "temperature_mean_c": 28.0,
        "temperature_range_c": 16.0,
        "temperature_mean_7d_avg_c": 27.5,
        "temperature_mean_30d_avg_c": 26.0,
        "temperature_anomaly_30d_c": 2.0,
        "temperature_mean_change_c": 0.5,
        "precipitation_mm": 0.0,
        "precipitation_7d_sum_mm": 1.0,
        "precipitation_30d_sum_mm": 12.0,
        "wind_speed_max_kmh": 25.0,
        "days_in_7d_window": 7,
        "days_in_30d_window": 30,
    }

    @contextmanager
    def fake_conn() -> Iterator[Any]:
        yield MagicMock()

    monkeypatch.setattr("weather_pipeline.api.get_conn", fake_conn)
    monkeypatch.setattr(
        "weather_pipeline.queries.health", lambda conn: {"status": "ok", "database": True}
    )
    monkeypatch.setattr("weather_pipeline.queries.list_cities", lambda conn: list(sample_cities))
    monkeypatch.setattr(
        "weather_pipeline.queries.latest_metrics",
        lambda conn, city_id: sample_metric if city_id == "madrid" else None,
    )
    monkeypatch.setattr(
        "weather_pipeline.queries.timeseries",
        lambda conn, city_id, start=None, end=None, limit=90: (
            [sample_metric] if city_id == "madrid" else []
        ),
    )
    return TestClient(create_app())


def test_health(client: TestClient) -> None:
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"status": "ok", "database": True}


def test_list_cities_includes_registry(client: TestClient) -> None:
    res = client.get("/cities")
    assert res.status_code == 200
    ids = {c["city_id"] for c in res.json()}
    assert "madrid" in ids
    assert "tokyo" in ids  # registry city not yet in DB mock


def test_latest_and_timeseries(client: TestClient) -> None:
    latest = client.get("/cities/madrid/latest")
    assert latest.status_code == 200
    assert latest.json()["temperature_mean_c"] == 28.0

    series = client.get("/cities/madrid/timeseries?limit=30")
    assert series.status_code == 200
    assert len(series.json()) == 1


def test_unknown_city_404(client: TestClient) -> None:
    assert client.get("/cities/atlantis/latest").status_code == 404


def test_inverted_date_range_400(client: TestClient) -> None:
    res = client.get("/cities/madrid/timeseries?start=2024-07-10&end=2024-07-01")
    assert res.status_code == 400


def test_dashboard_served(client: TestClient) -> None:
    res = client.get("/")
    assert res.status_code == 200
    assert "City Weather Pipeline" in res.text
    css = client.get("/static/styles.css")
    assert css.status_code == 200
