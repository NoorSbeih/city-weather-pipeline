from __future__ import annotations

import argparse
import logging
from datetime import date

from weather_pipeline.cities import CITIES

DEFAULT_CRON = "0 6 * * *"


def _cmd_init_db(_: argparse.Namespace) -> None:
    from weather_pipeline import db, load
    from weather_pipeline.config import get_settings

    with db.connect(get_settings().database_url) as conn:
        applied = db.apply_migrations(conn)
        with conn.transaction():
            load.upsert_cities(conn, CITIES.values())
    print(f"Migrations applied: {applied or 'none (already up to date)'}")


def _cmd_run(args: argparse.Namespace) -> None:
    from weather_pipeline.flows import ingest_daily_weather

    ingest_daily_weather(city_ids=args.city, start_date=args.start, end_date=args.end)


def _cmd_serve(args: argparse.Namespace) -> None:
    from weather_pipeline.flows import ingest_daily_weather

    ingest_daily_weather.serve(name="daily", cron=args.cron)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(prog="weather-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="apply migrations and seed dim_city").set_defaults(
        func=_cmd_init_db
    )

    run = sub.add_parser("run", help="run the ingest flow once")
    run.add_argument("--city", action="append", choices=sorted(CITIES), help="repeatable")
    run.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD (default: watermark)")
    run.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD (default: today - lag)")
    run.set_defaults(func=_cmd_run)

    serve = sub.add_parser("serve", help="long-running process that runs the flow on a cron")
    serve.add_argument("--cron", default=DEFAULT_CRON, help=f"default: '{DEFAULT_CRON}' (UTC)")
    serve.set_defaults(func=_cmd_serve)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
