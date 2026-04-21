from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Protocol

from app.domain.entities.weather import WeatherObservation


@dataclass(slots=True, frozen=True)
class ImportResult:
    inserted: int
    skipped: int
    schema_stage: str


class WeatherRepository(Protocol):
    def import_observations(
        self, observations: Iterable[WeatherObservation]
    ) -> ImportResult:
        ...

    def find_by_country_and_date(
        self,
        country: str,
        target_date: date,
        location_name: str | None = None,
    ) -> list[WeatherObservation]:
        ...

    def schema_stage(self) -> str:
        ...

