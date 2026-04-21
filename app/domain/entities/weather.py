from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, time
from enum import Enum


class WindDirection(str, Enum):
    N = "N"
    NNE = "NNE"
    NE = "NE"
    ENE = "ENE"
    E = "E"
    ESE = "ESE"
    SE = "SE"
    SSE = "SSE"
    S = "S"
    SSW = "SSW"
    SW = "SW"
    WSW = "WSW"
    W = "W"
    WNW = "WNW"
    NW = "NW"
    NNW = "NNW"


@dataclass(slots=True, frozen=True)
class AirQualityReading:
    carbon_monoxide: float
    ozone: float
    nitrogen_dioxide: float
    sulphur_dioxide: float
    pm25: float
    pm10: float
    us_epa_index: int
    gb_defra_index: int
    is_outdoor_safe: bool | None = None

    def with_safety(self, value: bool) -> "AirQualityReading":
        return replace(self, is_outdoor_safe=value)


@dataclass(slots=True, frozen=True)
class WeatherObservation:
    country: str
    location_name: str
    timezone: str
    last_updated: date
    last_updated_time: time
    temperature_celsius: float
    condition_text: str
    wind_kph: float
    wind_degree: int
    wind_direction: WindDirection
    sunrise: time
    humidity: int
    air_quality: AirQualityReading

