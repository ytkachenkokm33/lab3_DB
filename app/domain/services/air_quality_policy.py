from __future__ import annotations

from app.domain.entities.weather import AirQualityReading


def is_outdoor_safe(reading: AirQualityReading) -> bool:
    return (
        reading.pm25 <= 35.0
        and reading.pm10 <= 50.0
        and reading.us_epa_index <= 3
        and reading.gb_defra_index <= 3
        and reading.carbon_monoxide <= 4400.0
        and reading.ozone <= 100.0
        and reading.nitrogen_dioxide <= 40.0
        and reading.sulphur_dioxide <= 20.0
    )
