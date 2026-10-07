from __future__ import annotations

import json
import logging

from weather_pipeline.config import Settings
from weather_pipeline.logging_config import JsonFormatter, emit


def test_tcp_database_url_is_unchanged_without_cloud_sql():
    settings = Settings(database_url="postgresql://weather:weather@localhost:5432/weather")
    assert settings.connection_url == "postgresql://weather:weather@localhost:5432/weather"


def test_cloud_sql_socket_url_quotes_password():
    settings = Settings(
        cloud_sql_connection_name="my-proj:europe-west1:city-weather-pg",
        db_user="weather",
        db_password="p@ss/word",
        db_name="weather",
    )
    assert settings.connection_url == (
        "postgresql://weather:p%40ss%2Fword@/weather"
        "?host=/cloudsql/my-proj:europe-west1:city-weather-pg"
    )


def test_json_formatter_uses_cloud_logging_severity(capsys):
    record = logging.LogRecord(
        name="weather_pipeline.job",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="ingest succeeded",
        args=(),
        exc_info=None,
    )
    record.event = "ingest_succeeded"
    record.inserted = 3
    line = JsonFormatter().format(record)
    payload = json.loads(line)
    assert payload["severity"] == "INFO"
    assert payload["message"] == "ingest succeeded"
    assert payload["event"] == "ingest_succeeded"
    assert payload["inserted"] == 3

    emit("ERROR", "ingest failed", event="ingest_failed", inserted=0)
    logged = json.loads(capsys.readouterr().out.strip())
    assert logged["severity"] == "ERROR"
    assert logged["event"] == "ingest_failed"
