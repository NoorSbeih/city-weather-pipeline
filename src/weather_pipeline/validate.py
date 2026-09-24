"""Turn a raw Open-Meteo payload into typed, range-checked daily rows.

Structural problems (missing keys, misaligned arrays) raise `SchemaError` and fail the run:
that means upstream changed shape and loading anything would be wrong. Row-level problems
(an impossible value on one day) are quarantined as `Rejection`s and the rest still loads.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from weather_pipeline.extract import DAILY_VARIABLES, RawResponse


class SchemaError(ValueError):
    pass


class DailyWeather(BaseModel):
    model_config = ConfigDict(frozen=True)

    city_id: str
    date: date
    temperature_max_c: float | None = Field(default=None, ge=-90, le=60)
    temperature_min_c: float | None = Field(default=None, ge=-90, le=60)
    temperature_mean_c: float | None = Field(default=None, ge=-90, le=60)
    precipitation_mm: float | None = Field(default=None, ge=0, le=2000)
    wind_speed_max_kmh: float | None = Field(default=None, ge=0, le=500)

    @model_validator(mode="after")
    def _min_not_above_max(self) -> DailyWeather:
        lo, hi = self.temperature_min_c, self.temperature_max_c
        if lo is not None and hi is not None and lo > hi:
            raise ValueError(f"temperature_min_c {lo} > temperature_max_c {hi}")
        return self


@dataclass(frozen=True)
class Rejection:
    city_id: str
    date: str | None
    reason: str
    record: dict[str, Any]


@dataclass
class ValidationResult:
    rows: list[DailyWeather] = field(default_factory=list)
    rejected: list[Rejection] = field(default_factory=list)
    skipped_empty: int = 0


def parse_daily(response: RawResponse) -> ValidationResult:
    daily = response.payload.get("daily")
    if not isinstance(daily, dict) or "time" not in daily:
        raise SchemaError("payload has no 'daily.time' array")

    missing = [v for v in DAILY_VARIABLES if v not in daily]
    if missing:
        raise SchemaError(f"payload is missing daily variables: {missing}")

    times = daily["time"]
    for var in DAILY_VARIABLES:
        if len(daily[var]) != len(times):
            raise SchemaError(f"'{var}' has {len(daily[var])} values but 'time' has {len(times)}")

    result = ValidationResult()
    for i, day in enumerate(times):
        values = {column: daily[var][i] for var, column in DAILY_VARIABLES.items()}
        if all(v is None for v in values.values()):
            # Archive not yet published for this day; the next incremental run will pick it up.
            result.skipped_empty += 1
            continue
        record = {"city_id": response.city_id, "date": day, **values}
        try:
            result.rows.append(DailyWeather.model_validate(record))
        except ValidationError as exc:
            reason = "; ".join(
                f"{'.'.join(map(str, e['loc'])) or 'row'}: {e['msg']}" for e in exc.errors()
            )
            result.rejected.append(
                Rejection(city_id=response.city_id, date=day, reason=reason, record=record)
            )
    return result
