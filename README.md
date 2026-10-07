# city-weather-pipeline

**Open-Meteo → Cloud Run job (Prefect flow) → Cloud SQL (raw/staging/curated) → Cloud Run API → dashboard, triggered by Cloud Scheduler.**

A production-shaped batch data pipeline: daily historical weather for world cities is pulled from the
[Open-Meteo archive API](https://open-meteo.com/en/docs/historical-weather-api) on a schedule,
landed verbatim, validated, upserted into a typed staging layer, and transformed with SQL window
functions into an analytics table in PostgreSQL, all orchestrated by Prefect. The schema is unchanged;
Google Cloud is the deployment target.

> Status: pipeline logic is unchanged. GCP deploy commands are below. The public demo is still Render until you create Cloud SQL.
> Repo: https://github.com/NoorSbeih/city-weather-pipeline
> Live (Render): https://city-weather-api-j05r.onrender.com (dashboard `/`, docs `/docs`)
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

## Deploy on Google Cloud

Same image for the API and the ingest job. Cloud Run mounts a Cloud SQL unix socket at
`/cloudsql/PROJECT:REGION:INSTANCE`. The app builds the connection string from
`CLOUD_SQL_CONNECTION_NAME`, `DB_USER`, `DB_PASSWORD`, and `DB_NAME` (see `connection_url` in
`config.py`). `DATABASE_URL` is still used for local Postgres and the Auth Proxy.

Schema, Prefect tasks, and the dashboard are unchanged. The job command is `weather-pipeline run`
(or `python -m weather_pipeline.job`): one Prefect flow execution, then the process exits.
Cloud Scheduler calls that job daily at 06:10 UTC, matching the previous cron.

Replace the placeholders, then run the blocks in order. These commands assume bash (Google Cloud Shell is the
straightforward place if `gcloud` is not installed locally). Region `europe-west1` is a cheap EU default;
pick one region and use it everywhere.

```bash
export PROJECT_ID="your-gcp-project"
export REGION="europe-west1"
export INSTANCE="city-weather-pg"
export REPO="weather"
export IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/city-weather-pipeline:latest"

gcloud config set project "$PROJECT_ID"
gcloud services enable \
  sqladmin.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com
```

### 1. Cloud SQL for PostgreSQL (smallest shared-core tier)

Zonal `db-f1-micro`, 10 GB HDD, no high availability. Public IP is on so Cloud Run can use the
built-in Cloud SQL connector **without** a VPC connector (a VPC connector is a separate always-on charge).
Do not add `0.0.0.0/0` to authorized networks; Cloud Run authenticates with IAM, not a public port.

```bash
gcloud sql instances create "$INSTANCE" \
  --database-version=POSTGRES_16 \
  --edition=enterprise \
  --tier=db-f1-micro \
  --region="$REGION" \
  --availability-type=zonal \
  --storage-size=10GB \
  --root-password="$(openssl rand -base64 24)"

gcloud sql databases create weather --instance="$INSTANCE"

# Password is only in Secret Manager, not in git.
export DB_PASSWORD="$(openssl rand -base64 24)"
gcloud sql users create weather \
  --instance="$INSTANCE" \
  --password="$DB_PASSWORD"

printf '%s' "$DB_PASSWORD" | gcloud secrets create db-password --data-file=-
```

Connection name (used as the socket directory):

```bash
export SQL_CONN="${PROJECT_ID}:${REGION}:${INSTANCE}"
echo "$SQL_CONN"
```

On a laptop, skip the socket and use the Auth Proxy instead. Leave `CLOUD_SQL_CONNECTION_NAME` unset
and point `DATABASE_URL` at `127.0.0.1`:

```bash
# https://cloud.google.com/sql/docs/postgres/sql-proxy
cloud-sql-proxy "$SQL_CONN"
# other terminal:
export DATABASE_URL="postgresql://weather:${DB_PASSWORD}@127.0.0.1:5432/weather"
weather-pipeline run
```

### 2. Build and push the image

```bash
gcloud artifacts repositories create "$REPO" \
  --repository-format=docker \
  --location="$REGION" \
  --description="city-weather-pipeline"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
gcloud builds submit --tag "$IMAGE"
```

### 3. Service account

```bash
gcloud iam service-accounts create weather-runner \
  --display-name="city-weather Cloud Run runtime"

export RUNNER="weather-runner@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${RUNNER}" \
  --role="roles/cloudsql.client"

gcloud secrets add-iam-policy-binding db-password \
  --member="serviceAccount:${RUNNER}" \
  --role="roles/secretmanager.secretAccessor"
```

### 4. Cloud Run API

Listens on `$PORT` (Cloud Run sets 8080). `--min-instances=0` so idle time is not billed as instances.

```bash
gcloud run deploy city-weather-api \
  --image="$IMAGE" \
  --region="$REGION" \
  --service-account="$RUNNER" \
  --allow-unauthenticated \
  --port=8080 \
  --min-instances=0 \
  --max-instances=2 \
  --memory=512Mi \
  --cpu=1 \
  --add-cloudsql-instances="$SQL_CONN" \
  --set-secrets="DB_PASSWORD=db-password:latest" \
  --set-env-vars="CLOUD_SQL_CONNECTION_NAME=${SQL_CONN},DB_USER=weather,DB_NAME=weather"
```

### 5. Cloud Run job (Prefect flow, one execution)

```bash
gcloud run jobs deploy city-weather-ingest \
  --image="$IMAGE" \
  --region="$REGION" \
  --service-account="$RUNNER" \
  --command="weather-pipeline" \
  --args="run" \
  --tasks=1 \
  --max-retries=1 \
  --task-timeout=20m \
  --memory=1Gi \
  --cpu=1 \
  --set-cloudsql-instances="$SQL_CONN" \
  --set-secrets="DB_PASSWORD=db-password:latest" \
  --set-env-vars="CLOUD_SQL_CONNECTION_NAME=${SQL_CONN},DB_USER=weather,DB_NAME=weather"

# First backfill (about 1,000 days x 8 cities). Later runs are incremental.
gcloud run jobs execute city-weather-ingest --region="$REGION" --wait
```

Each run logs one JSON line to stdout (`event=ingest_succeeded` or `ingest_failed`, plus inserted /
updated / unchanged / rejected counts). Cloud Logging stores that automatically.

### 6. Cloud Scheduler (daily 06:10 UTC)

```bash
gcloud iam service-accounts create weather-scheduler \
  --display-name="city-weather scheduler"

export SCHEDULER="weather-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud run jobs add-iam-policy-binding city-weather-ingest \
  --region="$REGION" \
  --member="serviceAccount:${SCHEDULER}" \
  --role="roles/run.invoker"

gcloud scheduler jobs create http city-weather-ingest-daily \
  --location="$REGION" \
  --schedule="10 6 * * *" \
  --time-zone="Etc/UTC" \
  --uri="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/city-weather-ingest:run" \
  --http-method=POST \
  --oauth-service-account-email="$SCHEDULER"
```

Trigger it once without waiting for 06:10:

```bash
gcloud scheduler jobs run city-weather-ingest-daily --location="$REGION"
```

### What keeps charging if you leave it up

| Resource | Idle cost | What to do after screenshots |
|---|---|---|
| **Cloud SQL `db-f1-micro`** | The real bill. Roughly **$8–15/month** in a typical region, plus ~10 GB disk, even with no traffic. Backups add a little more. | Stop or delete it. This is the one that matters. |
| Cloud SQL public IP | Small, and it goes away with the instance. | Deleted with the instance. |
| Cloud Run API (`min-instances=0`) | About **$0** while idle. You pay for requests. | Optional delete. |
| Cloud Run job | About **$0** until it runs. A daily ~2 minute run is cents per month. | Pause the scheduler so it stops running. |
| Cloud Scheduler | First 3 jobs per billing account are free. | Delete the job if you want it gone. |
| Artifact Registry | Free tier covers a small image; after that about $0.10/GB-month. | Delete the repo if you are done. |
| Secret Manager | Negligible for one secret. | Optional delete. |

Stopping Cloud SQL stops the VM charge. Disk storage can still bill until you delete the instance.

```bash
# Stop compute charges (storage may remain):
gcloud sql instances patch "$INSTANCE" --activation-policy=NEVER

# Stop the daily job:
gcloud scheduler jobs pause city-weather-ingest-daily --location="$REGION"

# Remove everything that can bill:
gcloud scheduler jobs delete city-weather-ingest-daily --location="$REGION" --quiet
gcloud run services delete city-weather-api --region="$REGION" --quiet
gcloud run jobs delete city-weather-ingest --region="$REGION" --quiet
gcloud sql instances delete "$INSTANCE" --quiet
gcloud artifacts repositories delete "$REPO" --location="$REGION" --quiet
```

Do this after you have screenshots. A forgotten `db-f1-micro` is the usual surprise invoice.

## Deploy (Render + GitHub Actions)

Earlier public demo. Prefer the Google Cloud section above for the GCP write-up. Render free
web services sleep when idle; the GitHub Actions cron is the scheduler for that deploy.

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
| `DATABASE_URL` | `postgresql://weather:weather@localhost:5432/weather` | Target Postgres when Cloud SQL socket vars are unset (local, Auth Proxy, Render) |
| `CLOUD_SQL_CONNECTION_NAME` | empty | `PROJECT:REGION:INSTANCE`. When set, connect via `/cloudsql/...` |
| `DB_USER` / `DB_PASSWORD` / `DB_NAME` | empty / empty / `weather` | Cloud SQL login. Required together with the connection name |
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
  job.py           one-shot ingest for Cloud Run jobs
  logging_config.py  JSON logs on stdout (Cloud Logging)
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
- [x] GCP config: Cloud SQL socket URL, Cloud Run image, Cloud Run job entrypoint, structured logs
- [ ] Provision Cloud SQL / Cloud Run in a billed project (commands above; not created from this repo automatically)

## License

MIT
