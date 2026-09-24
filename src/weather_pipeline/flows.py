"""Prefect orchestration: extract -> land raw -> validate -> upsert staging -> refresh curated."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta

from prefect import flow, get_run_logger, task

from weather_pipeline import db, load, transform
from weather_pipeline.cities import CITIES, get_city
from weather_pipeline.config import Settings, get_settings
from weather_pipeline.extract import OpenMeteoClient, RawResponse
from weather_pipeline.validate import ValidationResult, parse_daily


@dataclass(frozen=True)
class CityRunSummary:
    city_id: str
    start_date: date | None
    end_date: date | None
    response_id: int | None = None
    rows_valid: int = 0
    rows_rejected: int = 0
    rows_skipped_empty: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    status: str = "loaded"


def plan_window(
    watermark: date | None, today: date, settings: Settings
) -> tuple[date, date] | None:
    """Pick the date range to fetch: full backfill for a new city, else watermark minus overlap."""
    end = today - timedelta(days=settings.archive_lag_days)
    if watermark is None:
        start = settings.backfill_start_date
    else:
        start = watermark - timedelta(days=settings.incremental_overlap_days)
    return None if start > end else (start, end)


@task
def prepare_database(city_ids: list[str]) -> list[str]:
    settings = get_settings()
    with db.connect(settings.database_url) as conn:
        applied = db.apply_migrations(conn)
        with conn.transaction():
            load.upsert_cities(conn, [get_city(c) for c in city_ids])
    return applied


@task
def plan_city_window(
    city_id: str, start_date: date | None, end_date: date | None
) -> tuple[date, date] | None:
    settings = get_settings()
    if start_date and end_date:
        return (start_date, end_date)
    with db.connect(settings.database_url) as conn:
        watermark = load.get_watermark(conn, city_id)
    window = plan_window(watermark, date.today(), settings)
    if window is None:
        return None
    return (start_date or window[0], end_date or window[1])


@task(retries=1, retry_delay_seconds=60)
def extract_city(city_id: str, start_date: date, end_date: date) -> RawResponse:
    settings = get_settings()
    with OpenMeteoClient(
        settings.open_meteo_base_url,
        timeout=settings.http_timeout_seconds,
        max_retries=settings.http_max_retries,
        backoff_seconds=settings.http_backoff_seconds,
    ) as client:
        return client.fetch_daily(get_city(city_id), start_date, end_date)


@task
def land_raw(response: RawResponse) -> int:
    """Persist the payload before validating it, so a schema break is still replayable."""
    with db.connect(get_settings().database_url) as conn, conn.transaction():
        return load.insert_raw_response(conn, response)


@task
def validate_response(response: RawResponse) -> ValidationResult:
    return parse_daily(response)


@task
def load_staging(result: ValidationResult, response_id: int) -> load.StagingLoadStats:
    with db.connect(get_settings().database_url) as conn, conn.transaction():
        load.insert_rejections(conn, result.rejected, response_id)
        return load.upsert_daily_weather(conn, result.rows, response_id)


@task
def refresh_curated(city_ids: list[str]) -> int:
    with db.connect(get_settings().database_url) as conn, conn.transaction():
        return transform.refresh_daily_city_stats(conn, city_ids)


@flow(name="ingest-daily-weather", log_prints=True)
def ingest_daily_weather(
    city_ids: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    logger = get_run_logger()
    city_ids = city_ids or sorted(CITIES)
    for c in city_ids:
        get_city(c)  # fail fast on typos before touching the DB or the API

    applied = prepare_database(city_ids)
    if applied:
        logger.info("Applied migrations: %s", applied)

    summaries: list[CityRunSummary] = []
    for city_id in city_ids:
        window = plan_city_window(city_id, start_date, end_date)
        if window is None:
            logger.info("%s is up to date; nothing to fetch", city_id)
            summaries.append(CityRunSummary(city_id, None, None, status="up_to_date"))
            continue

        start, end = window
        response = extract_city(city_id, start, end)
        response_id = land_raw(response)
        result = validate_response(response)
        stats = load_staging(result, response_id)

        if result.rejected:
            logger.warning(
                "%s: %d rows rejected (see raw.rejected_daily_rows, response_id=%d)",
                city_id,
                len(result.rejected),
                response_id,
            )
        summaries.append(
            CityRunSummary(
                city_id=city_id,
                start_date=start,
                end_date=end,
                response_id=response_id,
                rows_valid=len(result.rows),
                rows_rejected=len(result.rejected),
                rows_skipped_empty=result.skipped_empty,
                inserted=stats.inserted,
                updated=stats.updated,
                unchanged=stats.unchanged,
            )
        )

    curated_rows = refresh_curated(city_ids)
    logger.info("curated.daily_city_stats: %d rows written", curated_rows)

    for s in summaries:
        logger.info("%s", s)
    return [asdict(s) for s in summaries]


if __name__ == "__main__":
    ingest_daily_weather()
