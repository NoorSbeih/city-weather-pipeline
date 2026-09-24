"""SQL transforms from staging into the curated layer."""

from __future__ import annotations

import psycopg

from weather_pipeline.db import read_sql


def refresh_daily_city_stats(conn: psycopg.Connection, city_ids: list[str]) -> int:
    """Recompute curated.daily_city_stats for the given cities. Returns rows written."""
    cur = conn.execute(read_sql("transforms", "daily_city_stats.sql"), {"city_ids": city_ids})
    return cur.rowcount
