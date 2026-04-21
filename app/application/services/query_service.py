from __future__ import annotations

from datetime import date

from app.domain.entities.weather import WeatherObservation
from app.domain.repositories.weather_repository import WeatherRepository


class WeatherQueryService:
    def __init__(self, repository: WeatherRepository) -> None:
        self._repository = repository

    def execute(
        self,
        country: str,
        target_date: date,
        location_name: str | None = None,
    ) -> list[WeatherObservation]:
        return self._repository.find_by_country_and_date(
            country=country,
            target_date=target_date,
            location_name=location_name,
        )

