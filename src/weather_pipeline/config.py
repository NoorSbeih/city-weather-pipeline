from datetime import date
from functools import lru_cache
from urllib.parse import quote, quote_plus

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, read from environment variables or a local `.env` file."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://weather:weather@localhost:5432/weather"

    # Set on Cloud Run together with --add-cloudsql-instances. When present, the
    # app connects through the Cloud SQL unix socket instead of DATABASE_URL.
    cloud_sql_connection_name: str = ""
    db_user: str = ""
    db_password: str = ""
    db_name: str = "weather"

    @property
    def connection_url(self) -> str:
        """Postgres URL for this process: Cloud SQL socket if configured, else DATABASE_URL."""
        name = self.cloud_sql_connection_name.strip()
        if not name:
            return self.database_url
        if not self.db_user or not self.db_password:
            raise ValueError(
                "CLOUD_SQL_CONNECTION_NAME is set but DB_USER and DB_PASSWORD are required"
            )
        user = quote_plus(self.db_user)
        password = quote_plus(self.db_password)
        database = quote_plus(self.db_name)
        socket_dir = quote(f"/cloudsql/{name}", safe="/:")
        return f"postgresql://{user}:{password}@/{database}?host={socket_dir}"

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
