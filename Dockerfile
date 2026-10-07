FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8080

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN pip install --upgrade pip && pip install .

EXPOSE 8080

# Cloud Run sets PORT. The ingest job overrides this command with `weather-pipeline run`.
CMD uvicorn weather_pipeline.api:app --host 0.0.0.0 --port ${PORT:-8080}
