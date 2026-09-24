from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    city_id: str
    name: str
    country_code: str
    latitude: float
    longitude: float
    timezone: str


CITIES: dict[str, City] = {
    c.city_id: c
    for c in [
        City("madrid", "Madrid", "ES", 40.4168, -3.7038, "Europe/Madrid"),
    ]
}


def get_city(city_id: str) -> City:
    try:
        return CITIES[city_id]
    except KeyError:
        known = ", ".join(sorted(CITIES))
        raise ValueError(f"Unknown city_id {city_id!r}. Known cities: {known}") from None
