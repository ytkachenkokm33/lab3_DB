from __future__ import annotations

from pathlib import Path

from app.domain.repositories.weather_repository import ImportResult, WeatherRepository
from app.infrastructure.csv_reader import CsvWeatherReader
from app.infrastructure.database.config import get_settings


class CsvImportService:
    def __init__(
        self,
        repository: WeatherRepository,
        reader: CsvWeatherReader,
    ) -> None:
        self._repository = repository
        self._reader = reader

    def execute(
        self,
        csv_path: str | None = None,
        limit: int | None = None,
    ) -> ImportResult:
        source_path = Path(csv_path) if csv_path else get_settings().csv_path
        observations = self._reader.read(source_path, limit=limit)
        return self._repository.import_observations(observations)

