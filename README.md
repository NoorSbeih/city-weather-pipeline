# city-weather-pipeline

A production-shaped batch data pipeline: daily historical weather for world cities is pulled from the
[Open-Meteo archive API](https://open-meteo.com/en/docs/historical-weather-api) on a schedule,
landed verbatim, validated, upserted into a typed staging layer, and transformed with SQL window
functions into an analytics table in PostgreSQL, all orchestrated by Prefect.

> Status: **v1 live** — 8 cities, Prefect-shaped ingest, FastAPI + dashboard, Render Postgres/API, daily GHA cron.
> Repo: https://github.com/NoorSbeih/city-weather-pipeline
> Live: https://city-weather-api-j05r.onrender.com (dashboard `/`, docs `/docs`)
> Free Render instances sleep after idle (~50s cold start).

[![Demo: city picker → chart + latest days](docs/demo.gif)](https://city-weather-api-j05r.onrender.com)

## Architecture

```mermaid
flowchart LR
    A[Open-Meteo<br/>archive API] -->|httpx + retries/backoff| B(extract)
    B --> C[(raw.open_meteo_responses<br/>JSONB, content-hashed)]
    C --> D(validate<br/>pydantic)
    D -->|bad rows| Q[(raw.rejected_daily_rows)]
    D -->|COPY + upsert| E[(staging.daily_weather<br/>1 row / city / day)]
    E -->|SQL: CTEs + window functions| F[(curated.daily_city_stats<br/>rolling 7d/30d, anomalies)]
    F --> G[FastAPI read API]
    G --> H[HTML dashboard]
    P{{Prefect flow<br/>cron schedule}} -.orchestrates.-> B
```

Cities in the registry: Madrid, Barcelona, London, Berlin, New York, Tokyo, São Paulo, Cairo.
| Layer | Table | Purpose |
|---|---|---|
| raw | `open_meteo_responses` | Exact API payloads (JSONB) + request params, deduplicated by content hash. Replayable. |
| raw | `rejected_daily_rows` | Quarantined rows that failed validation, with the reason. |
| staging | `daily_weather` | Typed, range-checked daily values. PK `(city_id, date)`. |
| curated | `dim_city` | City dimension. |
| curated | `daily_city_stats` | 7d/30d rolling averages and sums, 30d temperature anomaly, day-over-day change, window completeness. |

## What this demonstrates

- **Idempotent loads.** Every write is an upsert keyed on natural keys and guarded by
  `IS DISTINCT FROM`, so rerunning any window changes nothing. Identical payloads are detected by a
  SHA-256 of the payload, ignoring volatile fields like `generationtime_ms`.
- **Incremental, watermark-based extraction.** A new city is backfilled from `BACKFILL_START_DATE`.
  After that, each run fetches from `max(date) - INCREMENTAL_OVERLAP_DAYS`, which picks up upstream
  revisions without refetching history.
- **Raw, staging, and curated layers.** Payloads are landed *before* validation, so a schema change
  upstream is still debuggable and replayable.
- **Validation with two failure modes.** A structural break (missing variable, misaligned arrays)
  fails the run loudly. A bad individual row is quarantined and the rest still loads.
- **SQL transforms.** CTEs plus `RANGE`-framed window functions, so a missing day shrinks the window
  instead of silently pulling in an older day.
- **Resilience.** The HTTP client does exponential backoff on 429 and 5xx errors and on transport
  errors. On top of that, the Prefect task has its own retry for longer outages.
- **Bulk loading.** Rows are written with `COPY` into a temp table and then a single set-based upsert,
  with an inserted/updated/unchanged count on every run.
- **Engineering hygiene.** `src/` layout, forward-only SQL migrations guarded by an advisory lock,
  typed config via environment variables, and ruff. pytest runs unit tests with mocked HTTP, plus
  separately marked integration tests against a real Postgres. GitHub Actions CI runs both.

## Quickstart

Requirements: Python 3.11+, and either Docker or the pure-pip Postgres described below.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

**Start Postgres + API with Docker:**

```bash
docker compose up -d --build
# API: http://127.0.0.1:8000  ·  docs: http://127.0.0.1:8000/docs
# Still run the pipeline from the host (or a one-shot worker later):
weather-pipeline run
```

**Start Postgres, option A (Docker, DB only):**

```bash
docker compose up -d postgres
```

**Start Postgres, option B (no Docker):** keep this running in its own terminal, then put the
printed URL into `.env` as `DATABASE_URL`:

```bash
pip install pgserver
python scripts/local_pg.py
```

**Run the pipeline once:**

```bash
weather-pipeline run                                     # all cities, incremental
weather-pipeline run --city madrid --start 2024-07-01 --end 2024-07-31   # explicit window
```

The first run backfills from 2024-01-01 (about 1,000 days per city, one API call). Running it again
immediately only refetches the overlap window and reports `unchanged` rows.

**Serve the read API + dashboard:**

```bash
weather-pipeline api                  # http://127.0.0.1:8000
weather-pipeline api --port 8000 --reload
```

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Liveness + DB ping |
| GET | `/cities` | City list with latest date / day count |
| GET | `/cities/{id}/latest` | Most recent curated metrics |
| GET | `/cities/{id}/timeseries` | `?start=&end=&limit=` (default last 90 days) |
| GET | `/` | Minimal chart + table UI |
| GET | `/docs` | OpenAPI |

**Run on a schedule (Prefect deployment):**

```bash
# terminal 1: Prefect server (UI at http://127.0.0.1:4200)
prefect server start

# terminal 2
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
weather-pipeline serve                    # daily at 06:00 UTC
weather-pipeline serve --cron "*/15 * * * *"   # or anything else
prefect deployment run 'ingest-daily-weather/daily'   # trigger ad hoc
```

`serve` needs a running Prefect server to fire scheduled runs. Against the ephemeral in-process API
it only registers the deployment.

**Inspect the results:**

```sql
SELECT date, temperature_mean_c, temperature_mean_7d_avg_c,
       temperature_anomaly_30d_c, precipitation_30d_sum_mm
FROM curated.daily_city_stats
WHERE city_id = 'madrid'
ORDER BY date DESC
LIMIT 10;
```

## Deploy (Render + GitHub Actions)

1. **Push** this repo to GitHub (needs a token with the `workflow` scope so Actions files can upload).
2. **Blueprint:** open [Render → New → Blueprint](https://dashboard.render.com/select-repo?type=blueprint),
   select `NoorSbeih/city-weather-pipeline`, apply `render.yaml` (free Postgres + Docker API).
3. Copy the Postgres **Internal** or **External** connection string from Render.
4. In GitHub → Settings → Secrets → Actions, add `DATABASE_URL` = that connection string
   (use the **external** URL for Actions runners).
5. Run **Actions → Ingest → Run workflow** once to backfill. Cron then runs daily at 06:10 UTC.
6. Open the Render web service URL; put it in this README under Status.

Local Prefect `serve` remains the preferred local scheduler; Actions is the free cloud interim.

## Tests

```bash
ruff check . && ruff format --check .
pytest                      # unit tests, no network, no DB
pytest -m integration       # needs DATABASE_URL pointing at a DISPOSABLE database
```

The integration tests drop and recreate the pipeline schemas, so point them at a separate database
(for example `.../weather_test`), not the one you are exploring.

## Configuration

All settings are read from environment variables or `.env`. See `src/weather_pipeline/config.py`.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://weather:weather@localhost:5432/weather` | Target Postgres |
| `BACKFILL_START_DATE` | `2024-01-01` | First day loaded for a new city |
| `ARCHIVE_LAG_DAYS` | `5` | The archive trails real time; newer days are skipped |
| `INCREMENTAL_OVERLAP_DAYS` | `3` | Days refetched before the watermark on each run |
| `HTTP_MAX_RETRIES` | `4` | Retries on 429/5xx/transport errors (exponential backoff) |

## Project layout

```
src/weather_pipeline/
  cities.py        city registry (add cities here)
  config.py        typed settings
  extract.py       Open-Meteo client with retries
  validate.py      schema + row validation (pydantic)
  load.py          raw landing, COPY + upsert into staging
  transform.py     runs curated SQL
  queries.py       curated-layer SQL for the API
  api.py           FastAPI app
  flows.py         Prefect flow and tasks
  cli.py           `weather-pipeline` entry point
  static/          minimal dashboard (HTML/CSS/JS + Chart.js)
  sql/migrations/  forward-only schema migrations
  sql/transforms/  curated-layer SQL
tests/             unit tests (mocked HTTP/API) + tests/integration (real Postgres)
```

## Roadmap

- [x] Ingest → raw/staging/curated for one city, Prefect flow, tests, CI
- [x] 8 cities
- [x] FastAPI read API (`/health`, `/cities`, `/cities/{id}/latest`, `/cities/{id}/timeseries`)
- [x] Minimal UI (chart and table)
- [x] Deploy: Render Postgres + Docker API
- [x] First production backfill + GitHub Actions `DATABASE_URL` secret
- [x] Demo GIF here

## License

MIT
