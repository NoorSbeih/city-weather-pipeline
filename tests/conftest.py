from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from weather_pipeline.extract import SOURCE, RawResponse

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def madrid_payload() -> dict[str, Any]:
    """Real Open-Meteo archive response for Madrid, 2024-07-01..2024-07-10."""
    return json.loads((FIXTURES / "open_meteo_madrid_2024-07-01_10.json").read_text())


def make_response(
    payload: dict[str, Any],
    city_id: str = "madrid",
    start: date = date(2024, 7, 1),
    end: date = date(2024, 7, 10),
) -> RawResponse:
    return RawResponse(
        source=SOURCE,
        city_id=city_id,
        start_date=start,
        end_date=end,
        request_params={"start_date": start.isoformat(), "end_date": end.isoformat()},
        payload=copy.deepcopy(payload),
    )
