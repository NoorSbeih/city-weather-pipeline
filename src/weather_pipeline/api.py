"""Thin read API over curated weather tables."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from weather_pipeline import queries
from weather_pipeline.cities import CITIES, get_city
from weather_pipeline.config import get_settings

STATIC_DIR = Path(__file__).resolve().parent / "static"


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    with psycopg.connect(get_settings().database_url) as conn:
        yield conn


class HealthResponse(BaseModel):
    status: str
    database: bool


class CitySummary(BaseModel):
    city_id: str
    name: str
    country_code: str
    latitude: float
    longitude: float
    timezone: str
    latest_date: str | None = None
    day_count: int = 0
    in_registry: bool = True


class DailyMetrics(BaseModel):
    city_id: str
    date: str
    temperature_max_c: float | None = None
    temperature_min_c: float | None = None
    temperature_mean_c: float | None = None
    temperature_range_c: float | None = None
    temperature_mean_7d_avg_c: float | None = None
    temperature_mean_30d_avg_c: float | None = None
    temperature_anomaly_30d_c: float | None = None
    temperature_mean_change_c: float | None = None
    precipitation_mm: float | None = None
    precipitation_7d_sum_mm: float | None = None
    precipitation_30d_sum_mm: float | None = None
    wind_speed_max_kmh: float | None = None
    days_in_7d_window: int
    days_in_30d_window: int


def create_app() -> FastAPI:
    app = FastAPI(
        title="City Weather Pipeline API",
        description="Read API over curated Open-Meteo daily stats.",
        version="0.1.0",
    )

    @app.get("/health", response_model=HealthResponse)
    def health() -> dict[str, Any]:
        try:
            with get_conn() as conn:
                return queries.health(conn)
        except psycopg.Error as exc:
            raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc

    @app.get("/cities", response_model=list[CitySummary])
    def cities() -> list[dict[str, Any]]:
        try:
            with get_conn() as conn:
                rows = queries.list_cities(conn)
        except psycopg.Error:
            # Empty DB / migrations not applied yet — still list registry cities.
            rows = []
        # Include registry cities that have not been ingested yet.
        present = {r["city_id"] for r in rows}
        for city_id, city in CITIES.items():
            if city_id not in present:
                rows.append(
                    {
                        "city_id": city.city_id,
                        "name": city.name,
                        "country_code": city.country_code,
                        "latitude": city.latitude,
                        "longitude": city.longitude,
                        "timezone": city.timezone,
                        "latest_date": None,
                        "day_count": 0,
                        "in_registry": True,
                    }
                )
        rows.sort(key=lambda r: r["name"])
        return rows
    @app.get("/cities/{city_id}/latest", response_model=DailyMetrics)
    def city_latest(city_id: str) -> dict[str, Any]:
        _require_known_city(city_id)
        with get_conn() as conn:
            row = queries.latest_metrics(conn, city_id)
        if row is None:
            raise HTTPException(status_code=404, detail=f"no metrics for city {city_id!r}")
        return row

    @app.get("/cities/{city_id}/timeseries", response_model=list[DailyMetrics])
    def city_timeseries(
        city_id: str,
        start: Annotated[date | None, Query(description="YYYY-MM-DD inclusive")] = None,
        end: Annotated[date | None, Query(description="YYYY-MM-DD inclusive")] = None,
        limit: Annotated[int, Query(ge=1, le=2000)] = 90,
    ) -> list[dict[str, Any]]:
        _require_known_city(city_id)
        if start is not None and end is not None and start > end:
            raise HTTPException(status_code=400, detail="start must be on or before end")
        with get_conn() as conn:
            return queries.timeseries(conn, city_id, start=start, end=end, limit=limit)

    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

        @app.get("/", include_in_schema=False)
        def dashboard() -> FileResponse:
            return FileResponse(STATIC_DIR / "index.html")

    return app


def _require_known_city(city_id: str) -> None:
    try:
        get_city(city_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ASGI entry for uvicorn: `uvicorn weather_pipeline.api:app`
app = create_app()
