"""Connections and a minimal forward-only SQL migration runner."""

from __future__ import annotations

import logging
from importlib import resources

import psycopg

logger = logging.getLogger(__name__)

_SQL_PACKAGE = "weather_pipeline.sql"


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url)


def read_sql(*parts: str) -> str:
    return resources.files(_SQL_PACKAGE).joinpath(*parts).read_text(encoding="utf-8")


def _migration_files() -> list[str]:
    folder = resources.files(_SQL_PACKAGE).joinpath("migrations")
    return sorted(f.name for f in folder.iterdir() if f.name.endswith(".sql"))


def apply_migrations(conn: psycopg.Connection) -> list[str]:
    """Apply pending migrations in filename order, each in its own transaction."""
    with conn.transaction():
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS public.schema_migrations (
                version    TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        # Serialise concurrent runners (e.g. two flow runs starting together).
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('weather_pipeline.migrations'))")
        applied = {row[0] for row in conn.execute("SELECT version FROM public.schema_migrations")}

        newly_applied = []
        for name in _migration_files():
            if name in applied:
                continue
            logger.info("Applying migration %s", name)
            conn.execute(read_sql("migrations", name))
            conn.execute("INSERT INTO public.schema_migrations (version) VALUES (%s)", (name,))
            newly_applied.append(name)
    return newly_applied
