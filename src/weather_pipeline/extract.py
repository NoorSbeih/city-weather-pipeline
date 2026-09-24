"""Open-Meteo historical (archive) API client."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from weather_pipeline.cities import City

logger = logging.getLogger(__name__)

SOURCE = "open-meteo-archive"

# Open-Meteo variable name -> staging column name.
DAILY_VARIABLES: dict[str, str] = {
    "temperature_2m_max": "temperature_max_c",
    "temperature_2m_min": "temperature_min_c",
    "temperature_2m_mean": "temperature_mean_c",
    "precipitation_sum": "precipitation_mm",
    "wind_speed_10m_max": "wind_speed_max_kmh",
}

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class OpenMeteoError(RuntimeError):
    """Non-retryable API failure, or retries exhausted."""


@dataclass(frozen=True)
class RawResponse:
    source: str
    city_id: str
    start_date: date
    end_date: date
    request_params: dict[str, Any]
    payload: dict[str, Any]


class OpenMeteoClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        max_retries: int = 4,
        backoff_seconds: float = 1.0,
        http_client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._base_url = base_url
        self._max_retries = max_retries
        self._backoff_seconds = backoff_seconds
        self._sleep = sleep
        self._http = http_client or httpx.Client(
            timeout=timeout, headers={"User-Agent": "city-weather-pipeline/0.1"}
        )

    def __enter__(self) -> OpenMeteoClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def fetch_daily(self, city: City, start_date: date, end_date: date) -> RawResponse:
        if start_date > end_date:
            raise ValueError(f"start_date {start_date} is after end_date {end_date}")
        params: dict[str, Any] = {
            "latitude": city.latitude,
            "longitude": city.longitude,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": ",".join(DAILY_VARIABLES),
            "timezone": city.timezone,
        }
        payload = self._get_json(params)
        return RawResponse(
            source=SOURCE,
            city_id=city.city_id,
            start_date=start_date,
            end_date=end_date,
            request_params=params,
            payload=payload,
        )

    def _get_json(self, params: dict[str, Any]) -> dict[str, Any]:
        attempt = 0
        while True:
            try:
                response = self._http.get(self._base_url, params=params)
            except httpx.TransportError as exc:
                failure = f"transport error: {exc!r}"
            else:
                if response.status_code == 200:
                    return response.json()
                if response.status_code not in RETRYABLE_STATUS:
                    raise OpenMeteoError(
                        f"Open-Meteo returned {response.status_code}: {_error_reason(response)}"
                    )
                failure = f"HTTP {response.status_code}"

            if attempt >= self._max_retries:
                raise OpenMeteoError(f"Giving up after {attempt + 1} attempts ({failure})")
            delay = self._backoff_seconds * 2**attempt
            logger.warning("Open-Meteo %s; retrying in %.1fs", failure, delay)
            self._sleep(delay)
            attempt += 1


def _error_reason(response: httpx.Response) -> str:
    try:
        return str(response.json().get("reason", response.text))
    except ValueError:
        return response.text[:200]
