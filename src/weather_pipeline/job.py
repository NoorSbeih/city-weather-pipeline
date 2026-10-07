"""One-shot ingest entrypoint for Cloud Run jobs (and `weather-pipeline run`).

Cloud Scheduler starts the job; this process runs the Prefect flow once and exits.
A non-zero exit marks the execution failed.
"""

from __future__ import annotations

import logging
from datetime import date

from weather_pipeline.logging_config import configure_logging, emit

logger = logging.getLogger(__name__)


def run_once(
    city_ids: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    from weather_pipeline.flows import ingest_daily_weather

    try:
        summaries = ingest_daily_weather(
            city_ids=city_ids, start_date=start_date, end_date=end_date
        )
    except Exception:
        logger.exception("ingest failed")
        emit("ERROR", "ingest failed", event="ingest_failed")
        raise

    inserted = sum(int(s.get("inserted") or 0) for s in summaries)
    updated = sum(int(s.get("updated") or 0) for s in summaries)
    unchanged = sum(int(s.get("unchanged") or 0) for s in summaries)
    rejected = sum(int(s.get("rows_rejected") or 0) for s in summaries)
    emit(
        "INFO",
        "ingest succeeded",
        event="ingest_succeeded",
        cities=len(summaries),
        inserted=inserted,
        updated=updated,
        unchanged=unchanged,
        rows_rejected=rejected,
        city_results=summaries,
    )
    return summaries


def main() -> None:
    configure_logging()
    try:
        run_once()
    except Exception:
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
