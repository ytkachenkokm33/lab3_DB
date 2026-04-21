from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SQLITE_PATH = PROJECT_ROOT / "weather_lab.db"


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    csv_path: Path


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    database_url = os.getenv(
        "DATABASE_URL",
        f"sqlite:///{DEFAULT_SQLITE_PATH.as_posix()}",
    )
    csv_path = Path(
        os.getenv("WEATHER_CSV_PATH", str(PROJECT_ROOT / "GlobalWeatherRepository.csv"))
    )
    return Settings(database_url=database_url, csv_path=csv_path)

