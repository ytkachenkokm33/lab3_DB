from __future__ import annotations

import csv
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from app.domain.entities.weather import AirQualityReading, WeatherObservation, WindDirection
from app.domain.services.air_quality_policy import is_outdoor_safe


class CsvWeatherReader:
    def read(
        self,
        path: Path,
        limit: int | None = None,
    ) -> Iterator[WeatherObservation]:
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        with path.open("r", encoding="utf-8", newline="") as csv_file:
            reader = csv.DictReader(csv_file)

            for row_number, row in enumerate(reader, start=1):
                if limit is not None and row_number > limit:
                    break
                yield self._map_row(row)

    def _map_row(self, row: dict[str, str]) -> WeatherObservation:
        timestamp = datetime.strptime(row["last_updated"], "%Y-%m-%d %H:%M")
        sunrise = datetime.strptime(row["sunrise"], "%I:%M %p").time()

        air_quality = AirQualityReading(
            carbon_monoxide=float(row["air_quality_Carbon_Monoxide"]),
            ozone=float(row["air_quality_Ozone"]),
            nitrogen_dioxide=float(row["air_quality_Nitrogen_dioxide"]),
            sulphur_dioxide=float(row["air_quality_Sulphur_dioxide"]),
            pm25=float(row["air_quality_PM2.5"]),
            pm10=float(row["air_quality_PM10"]),
            us_epa_index=int(row["air_quality_us-epa-index"]),
            gb_defra_index=int(row["air_quality_gb-defra-index"]),
        )

        return WeatherObservation(
            country=row["country"].strip(),
            location_name=row["location_name"].strip(),
            timezone=row["timezone"].strip(),
            last_updated=timestamp.date(),
            last_updated_time=timestamp.time(),
            temperature_celsius=float(row["temperature_celsius"]),
            condition_text=row["condition_text"].strip(),
            wind_kph=float(row["wind_kph"]),
            wind_degree=int(row["wind_degree"]),
            wind_direction=WindDirection(row["wind_direction"]),
            sunrise=sunrise,
            humidity=int(row["humidity"]),
            air_quality=air_quality.with_safety(is_outdoor_safe(air_quality)),
        )
