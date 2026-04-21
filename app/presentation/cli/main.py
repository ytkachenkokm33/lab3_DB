from __future__ import annotations

import argparse
from datetime import datetime

from app.application.services.import_service import CsvImportService
from app.application.services.query_service import WeatherQueryService
from app.infrastructure.csv_reader import CsvWeatherReader
from app.infrastructure.database.session import create_engine_from_settings, create_session_factory
from app.infrastructure.repositories.weather_repository import SqlAlchemyWeatherRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Weather migration lab for variant 22 (air quality)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_parser = subparsers.add_parser("import-csv", help="Import rows from CSV.")
    import_parser.add_argument("--path", dest="path", help="Path to the CSV file.")
    import_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for quick smoke tests.",
    )

    find_parser = subparsers.add_parser(
        "find-weather",
        help="Find weather records by country and date.",
    )
    find_parser.add_argument("--country", required=True, help="Country name from the dataset.")
    find_parser.add_argument(
        "--date",
        required=True,
        help="Date in ISO format, for example 2024-05-16.",
    )
    find_parser.add_argument(
        "--location",
        default=None,
        help="Optional city/location name for more precise filtering.",
    )

    subparsers.add_parser("schema-stage", help="Print current database schema stage.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    engine = create_engine_from_settings()
    session_factory = create_session_factory(engine=engine)
    repository = SqlAlchemyWeatherRepository(engine=engine, session_factory=session_factory)
    reader = CsvWeatherReader()

    if args.command == "import-csv":
        service = CsvImportService(repository=repository, reader=reader)
        result = service.execute(csv_path=args.path, limit=args.limit)
        print(f"schema_stage={result.schema_stage}")
        print(f"inserted={result.inserted}")
        print(f"skipped={result.skipped}")
        return 0

    if args.command == "find-weather":
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        service = WeatherQueryService(repository=repository)
        observations = service.execute(
            country=args.country,
            target_date=target_date,
            location_name=args.location,
        )
        if not observations:
            print("No weather records found.")
            return 0

        for observation in observations:
            print(f"{observation.country} | {observation.location_name}")
            print(
                "  updated: "
                f"{observation.last_updated.isoformat()} "
                f"{observation.last_updated_time.isoformat(timespec='minutes')}"
            )
            print(f"  timezone: {observation.timezone}")
            print(
                "  weather: "
                f"{observation.temperature_celsius:.1f}C, "
                f"{observation.condition_text}, humidity={observation.humidity}%"
            )
            print(
                "  wind: "
                f"{observation.wind_kph:.1f} kph, "
                f"{observation.wind_degree} deg, {observation.wind_direction.value}"
            )
            print(f"  sunrise: {observation.sunrise.isoformat(timespec='minutes')}")
            print(
                "  air quality: "
                f"CO={observation.air_quality.carbon_monoxide:.1f}, "
                f"O3={observation.air_quality.ozone:.1f}, "
                f"NO2={observation.air_quality.nitrogen_dioxide:.1f}, "
                f"SO2={observation.air_quality.sulphur_dioxide:.1f}, "
                f"PM2.5={observation.air_quality.pm25:.1f}, "
                f"PM10={observation.air_quality.pm10:.1f}, "
                f"EPA={observation.air_quality.us_epa_index}, "
                f"DEFRA={observation.air_quality.gb_defra_index}"
            )
            print(
                "  should_go_outside: "
                f"{'yes' if observation.air_quality.is_outdoor_safe else 'no'}"
            )
        return 0

    if args.command == "schema-stage":
        print(repository.schema_stage())
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2
