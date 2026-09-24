from __future__ import annotations

from datetime import date

from weather_pipeline.config import Settings
from weather_pipeline.flows import plan_window
from weather_pipeline.load import payload_fingerprint

SETTINGS = Settings(
    backfill_start_date=date(2024, 1, 1), archive_lag_days=5, incremental_overlap_days=3
)
TODAY = date(2026, 9, 24)


def test_new_city_backfills_from_configured_start():
    assert plan_window(None, TODAY, SETTINGS) == (date(2024, 1, 1), date(2026, 9, 19))


def test_incremental_run_rereads_overlap_before_watermark():
    assert plan_window(date(2026, 9, 10), TODAY, SETTINGS) == (date(2026, 9, 7), date(2026, 9, 19))


def test_overlap_window_is_still_fetched_when_caught_up():
    assert plan_window(date(2026, 9, 19), TODAY, SETTINGS) == (date(2026, 9, 16), date(2026, 9, 19))


def test_nothing_to_fetch_when_backfill_start_is_after_available_data():
    settings = SETTINGS.model_copy(update={"backfill_start_date": date(2026, 9, 22)})
    assert plan_window(None, TODAY, settings) is None


def test_fingerprint_ignores_volatile_fields_and_key_order(madrid_payload):
    a = dict(madrid_payload, generationtime_ms=0.1)
    b = dict(reversed(list(madrid_payload.items())), generationtime_ms=9.9)
    assert payload_fingerprint(a) == payload_fingerprint(b)


def test_fingerprint_changes_when_data_changes(madrid_payload):
    changed = dict(madrid_payload, daily=dict(madrid_payload["daily"]))
    changed["daily"]["temperature_2m_max"] = [0.0] * len(madrid_payload["daily"]["time"])
    assert payload_fingerprint(madrid_payload) != payload_fingerprint(changed)
