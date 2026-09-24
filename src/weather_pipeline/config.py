from datetime import date
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, read from environment variables or a local `.env` file."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://weather:weather@localhost:5432/weather"

    open_meteo_base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    http_timeout_seconds: float = 30.0
    http_max_retries: int = 4
    http_backoff_seconds: float = 1.0

    # First day loaded for a city that has no data yet.
    backfill_start_date: date = date(2024, 1, 1)
    # The ERA5-based archive publishes with a delay; days newer than this come back as nulls.
    archive_lag_days: int = 5
    # Incremental runs re-read this many days before the watermark to pick up upstream revisions.
    incremental_overlap_days: int = 3


@lru_cache
def get_settings() -> Settings:
    return Settings()
