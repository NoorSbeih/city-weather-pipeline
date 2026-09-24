from __future__ import annotations

from datetime import date

import pytest

from tests.conftest import make_response
from weather_pipeline.validate import SchemaError, parse_daily


def test_parses_real_fixture(madrid_payload):
    result = parse_daily(make_response(madrid_payload))

    assert len(result.rows) == 10
    assert result.rejected == []
    assert result.skipped_empty == 0
    first = result.rows[0]
    assert first.city_id == "madrid"
    assert first.date == date(2024, 7, 1)
    assert first.temperature_max_c == madrid_payload["daily"]["temperature_2m_max"][0]


def test_skips_days_where_archive_is_not_published_yet(madrid_payload):
    daily = madrid_payload["daily"]
    for var in daily:
        if var != "time":
            daily[var][-1] = None

    result = parse_daily(make_response(madrid_payload))

    assert len(result.rows) == 9
    assert result.skipped_empty == 1


def test_quarantines_out_of_range_rows_and_keeps_the_rest(madrid_payload):
    madrid_payload["daily"]["precipitation_sum"][2] = -4.0

    result = parse_daily(make_response(madrid_payload))

    assert len(result.rows) == 9
    assert len(result.rejected) == 1
    rejection = result.rejected[0]
    assert rejection.date == "2024-07-03"
    assert "precipitation_mm" in rejection.reason


def test_quarantines_min_above_max(madrid_payload):
    madrid_payload["daily"]["temperature_2m_min"][0] = 50.0

    result = parse_daily(make_response(madrid_payload))

    assert [r.date for r in result.rejected] == ["2024-07-01"]
    assert "temperature_min_c" in result.rejected[0].reason


def test_missing_variable_is_a_schema_error(madrid_payload):
    del madrid_payload["daily"]["wind_speed_10m_max"]
    with pytest.raises(SchemaError, match="wind_speed_10m_max"):
        parse_daily(make_response(madrid_payload))


def test_misaligned_arrays_are_a_schema_error(madrid_payload):
    madrid_payload["daily"]["temperature_2m_max"].pop()
    with pytest.raises(SchemaError, match="temperature_2m_max"):
        parse_daily(make_response(madrid_payload))


def test_missing_daily_block_is_a_schema_error(madrid_payload):
    del madrid_payload["daily"]
    with pytest.raises(SchemaError):
        parse_daily(make_response(madrid_payload))
