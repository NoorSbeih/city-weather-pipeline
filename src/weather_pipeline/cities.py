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
        City("barcelona", "Barcelona", "ES", 41.3874, 2.1686, "Europe/Madrid"),
        City("london", "London", "GB", 51.5074, -0.1278, "Europe/London"),
        City("berlin", "Berlin", "DE", 52.5200, 13.4050, "Europe/Berlin"),
        City("new_york", "New York", "US", 40.7128, -74.0060, "America/New_York"),
        City("tokyo", "Tokyo", "JP", 35.6762, 139.6503, "Asia/Tokyo"),
        City("sao_paulo", "São Paulo", "BR", -23.5505, -46.6333, "America/Sao_Paulo"),
        City("cairo", "Cairo", "EG", 30.0444, 31.2357, "Africa/Cairo"),
    ]
}


def get_city(city_id: str) -> City:
    try:
        return CITIES[city_id]
    except KeyError:
        known = ", ".join(sorted(CITIES))
        raise ValueError(f"Unknown city_id {city_id!r}. Known cities: {known}") from None
