"""Read-side SQL used by the FastAPI layer. All queries hit curated tables."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from weather_pipeline.cities import CITIES


def _dec(value: Decimal | float | int | None) -> float | None:
    if value is None:
        return None
    return float(value)


def health(conn: psycopg.Connection) -> dict[str, Any]:
    row = conn.execute("SELECT 1 AS ok").fetchone()
    assert row is not None
    return {"status": "ok", "database": True}


def list_cities(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Cities from dim_city, preferring registry metadata when present."""
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            """
            SELECT
                d.city_id,
                d.name,
                d.country_code,
                d.latitude,
                d.longitude,
                d.timezone,
                (SELECT max(s.date) FROM curated.daily_city_stats s WHERE s.city_id = d.city_id)
                    AS latest_date,
                (SELECT count(*) FROM curated.daily_city_stats s WHERE s.city_id = d.city_id)
                    AS day_count
            FROM curated.dim_city d
            ORDER BY d.name
            """
        ).fetchall()
    return [
        {
            "city_id": r["city_id"],
            "name": r["name"],
            "country_code": r["country_code"],
            "latitude": float(r["latitude"]),
            "longitude": float(r["longitude"]),
            "timezone": r["timezone"],
            "latest_date": r["latest_date"].isoformat() if r["latest_date"] else None,
            "day_count": int(r["day_count"]),
            "in_registry": r["city_id"] in CITIES,
        }
        for r in rows
    ]


def latest_metrics(conn: psycopg.Connection, city_id: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        row = cur.execute(
            """
            SELECT
                city_id, date,
                temperature_max_c, temperature_min_c, temperature_mean_c,
                temperature_range_c, temperature_mean_7d_avg_c, temperature_mean_30d_avg_c,
                temperature_anomaly_30d_c, temperature_mean_change_c,
                precipitation_mm, precipitation_7d_sum_mm, precipitation_30d_sum_mm,
                wind_speed_max_kmh, days_in_7d_window, days_in_30d_window
            FROM curated.daily_city_stats
            WHERE city_id = %s
            ORDER BY date DESC
            LIMIT 1
            """,
            (city_id,),
        ).fetchone()
    if row is None:
        return None
    return _metric_row(row)


def timeseries(
    conn: psycopg.Connection,
    city_id: str,
    *,
    start: date | None = None,
    end: date | None = None,
    limit: int = 90,
) -> list[dict[str, Any]]:
    clauses = ["city_id = %(city_id)s"]
    params: dict[str, Any] = {"city_id": city_id, "limit": limit}
    if start is not None:
        clauses.append("date >= %(start)s")
        params["start"] = start
    if end is not None:
        clauses.append("date <= %(end)s")
        params["end"] = end
    where = " AND ".join(clauses)
    with conn.cursor(row_factory=dict_row) as cur:
        rows = cur.execute(
            f"""
            SELECT
                city_id, date,
                temperature_max_c, temperature_min_c, temperature_mean_c,
                temperature_range_c, temperature_mean_7d_avg_c, temperature_mean_30d_avg_c,
                temperature_anomaly_30d_c, temperature_mean_change_c,
                precipitation_mm, precipitation_7d_sum_mm, precipitation_30d_sum_mm,
                wind_speed_max_kmh, days_in_7d_window, days_in_30d_window
            FROM curated.daily_city_stats
            WHERE {where}
            ORDER BY date DESC
            LIMIT %(limit)s
            """,
            params,
        ).fetchall()
    # Ascending for charts; query uses DESC + LIMIT so "last N days" is correct.
    return [_metric_row(r) for r in reversed(rows)]


def _metric_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "city_id": row["city_id"],
        "date": row["date"].isoformat() if isinstance(row["date"], date) else row["date"],
        "temperature_max_c": _dec(row["temperature_max_c"]),
        "temperature_min_c": _dec(row["temperature_min_c"]),
        "temperature_mean_c": _dec(row["temperature_mean_c"]),
        "temperature_range_c": _dec(row["temperature_range_c"]),
        "temperature_mean_7d_avg_c": _dec(row["temperature_mean_7d_avg_c"]),
        "temperature_mean_30d_avg_c": _dec(row["temperature_mean_30d_avg_c"]),
        "temperature_anomaly_30d_c": _dec(row["temperature_anomaly_30d_c"]),
        "temperature_mean_change_c": _dec(row["temperature_mean_change_c"]),
        "precipitation_mm": _dec(row["precipitation_mm"]),
        "precipitation_7d_sum_mm": _dec(row["precipitation_7d_sum_mm"]),
        "precipitation_30d_sum_mm": _dec(row["precipitation_30d_sum_mm"]),
        "wind_speed_max_kmh": _dec(row["wind_speed_max_kmh"]),
        "days_in_7d_window": int(row["days_in_7d_window"]),
        "days_in_30d_window": int(row["days_in_30d_window"]),
    }
