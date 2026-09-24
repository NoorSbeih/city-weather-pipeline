from __future__ import annotations

from datetime import date

import httpx
import pytest

from weather_pipeline.cities import get_city
from weather_pipeline.extract import DAILY_VARIABLES, OpenMeteoClient, OpenMeteoError

BASE_URL = "https://archive.example/v1/archive"


def make_client(handler, sleeps: list[float] | None = None, max_retries: int = 3):
    recorded = sleeps if sleeps is not None else []
    return OpenMeteoClient(
        BASE_URL,
        max_retries=max_retries,
        backoff_seconds=1.0,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
        sleep=recorded.append,
    )


def test_fetch_daily_sends_expected_params(madrid_payload):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=madrid_payload)

    with make_client(handler) as client:
        resp = client.fetch_daily(get_city("madrid"), date(2024, 7, 1), date(2024, 7, 10))

    params = seen[0].url.params
    assert params["start_date"] == "2024-07-01"
    assert params["end_date"] == "2024-07-10"
    assert params["timezone"] == "Europe/Madrid"
    assert params["daily"].split(",") == list(DAILY_VARIABLES)
    assert resp.city_id == "madrid"
    assert resp.payload == madrid_payload


def test_retries_retryable_status_with_exponential_backoff(madrid_payload):
    statuses = iter([429, 503])
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(statuses, 200)
        return httpx.Response(status, json=madrid_payload if status == 200 else {})

    with make_client(handler, sleeps) as client:
        resp = client.fetch_daily(get_city("madrid"), date(2024, 7, 1), date(2024, 7, 10))

    assert resp.payload == madrid_payload
    assert sleeps == [1.0, 2.0]


def test_retries_transport_errors(madrid_payload):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectTimeout("boom", request=request)
        return httpx.Response(200, json=madrid_payload)

    with make_client(handler) as client:
        client.fetch_daily(get_city("madrid"), date(2024, 7, 1), date(2024, 7, 10))
    assert calls["n"] == 2


def test_gives_up_after_max_retries():
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502)

    with (
        make_client(handler, sleeps, max_retries=2) as client,
        pytest.raises(OpenMeteoError, match="after 3 attempts"),
    ):
        client.fetch_daily(get_city("madrid"), date(2024, 7, 1), date(2024, 7, 10))
    assert sleeps == [1.0, 2.0]


def test_client_error_is_not_retried_and_surfaces_reason():
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": True, "reason": "Parameter 'daily' invalid"})

    with (
        make_client(handler, sleeps) as client,
        pytest.raises(OpenMeteoError, match="'daily' invalid"),
    ):
        client.fetch_daily(get_city("madrid"), date(2024, 7, 1), date(2024, 7, 10))
    assert sleeps == []


def test_rejects_inverted_date_range():
    with make_client(lambda r: httpx.Response(200)) as client, pytest.raises(ValueError):
        client.fetch_daily(get_city("madrid"), date(2024, 7, 10), date(2024, 7, 1))
