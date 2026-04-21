from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from sqlalchemy import MetaData, Table, func, insert, inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, selectinload, sessionmaker

from app.domain.entities.weather import AirQualityReading, WeatherObservation, WindDirection
from app.domain.repositories.weather_repository import ImportResult, WeatherRepository
from app.domain.services.air_quality_policy import is_outdoor_safe
from app.infrastructure.database.models import AirQualityMetric, WeatherRecord


class SqlAlchemyWeatherRepository(WeatherRepository):
    def __init__(self, engine: Engine, session_factory: sessionmaker[Session]) -> None:
        self._engine = engine
        self._session_factory = session_factory

    def schema_stage(self) -> str:
        inspector = inspect(self._engine)
        if not inspector.has_table("weather_records"):
            return "empty"
        if inspector.has_table("air_quality_metrics"):
            return "normalized"
        return "legacy"

    def import_observations(
        self,
        observations: Iterable[WeatherObservation],
    ) -> ImportResult:
        stage = self.schema_stage()
        if stage == "empty":
            raise RuntimeError("Database schema is empty. Run Alembic migrations first.")
        if stage == "normalized":
            return self._import_normalized(observations)
        return self._import_legacy(observations)

    def find_by_country_and_date(
        self,
        country: str,
        target_date: date,
        location_name: str | None = None,
    ) -> list[WeatherObservation]:
        stage = self.schema_stage()
        if stage == "empty":
            raise RuntimeError("Database schema is empty. Run Alembic migrations first.")
        if stage == "normalized":
            return self._find_normalized(country, target_date, location_name)
        return self._find_legacy(country, target_date, location_name)

    def _import_normalized(
        self,
        observations: Iterable[WeatherObservation],
    ) -> ImportResult:
        inserted = 0
        skipped = 0
        pending = 0

        with self._session_factory() as session:
            existing_keys = {
                (country, location_name, last_updated, last_updated_time)
                for country, location_name, last_updated, last_updated_time in session.execute(
                    select(
                        WeatherRecord.country,
                        WeatherRecord.location_name,
                        WeatherRecord.last_updated,
                        WeatherRecord.last_updated_time,
                    )
                )
            }

            try:
                for observation in observations:
                    key = (
                        observation.country,
                        observation.location_name,
                        observation.last_updated,
                        observation.last_updated_time,
                    )
                    if key in existing_keys:
                        skipped += 1
                        continue

                    existing_keys.add(key)
                    session.add(
                        WeatherRecord(
                            country=observation.country,
                            location_name=observation.location_name,
                            timezone=observation.timezone,
                            last_updated=observation.last_updated,
                            last_updated_time=observation.last_updated_time,
                            temperature_celsius=observation.temperature_celsius,
                            condition_text=observation.condition_text,
                            wind_kph=observation.wind_kph,
                            wind_degree=observation.wind_degree,
                            wind_direction=observation.wind_direction,
                            sunrise=observation.sunrise,
                            humidity=observation.humidity,
                            air_quality=AirQualityMetric(
                                carbon_monoxide=observation.air_quality.carbon_monoxide,
                                ozone=observation.air_quality.ozone,
                                nitrogen_dioxide=observation.air_quality.nitrogen_dioxide,
                                sulphur_dioxide=observation.air_quality.sulphur_dioxide,
                                pm25=observation.air_quality.pm25,
                                pm10=observation.air_quality.pm10,
                                us_epa_index=observation.air_quality.us_epa_index,
                                gb_defra_index=observation.air_quality.gb_defra_index,
                                is_outdoor_safe=bool(observation.air_quality.is_outdoor_safe),
                            ),
                        )
                    )
                    inserted += 1
                    pending += 1

                    if pending >= 500:
                        session.commit()
                        session.expunge_all()
                        pending = 0

                session.commit()
            except Exception:
                session.rollback()
                raise

        return ImportResult(inserted=inserted, skipped=skipped, schema_stage="normalized")

    def _import_legacy(
        self,
        observations: Iterable[WeatherObservation],
    ) -> ImportResult:
        metadata = MetaData()
        weather_table = Table("weather_records", metadata, autoload_with=self._engine)
        inserted = 0
        skipped = 0
        payloads: list[dict[str, object]] = []

        with self._engine.begin() as connection:
            existing_keys = {
                (country, location_name, last_updated, last_updated_time)
                for country, location_name, last_updated, last_updated_time in connection.execute(
                    select(
                        weather_table.c.country,
                        weather_table.c.location_name,
                        weather_table.c.last_updated,
                        weather_table.c.last_updated_time,
                    )
                )
            }

            for observation in observations:
                key = (
                    observation.country,
                    observation.location_name,
                    observation.last_updated,
                    observation.last_updated_time,
                )
                if key in existing_keys:
                    skipped += 1
                    continue

                existing_keys.add(key)
                payloads.append(
                    {
                        "country": observation.country,
                        "location_name": observation.location_name,
                        "timezone": observation.timezone,
                        "last_updated": observation.last_updated,
                        "last_updated_time": observation.last_updated_time,
                        "temperature_celsius": observation.temperature_celsius,
                        "condition_text": observation.condition_text,
                        "wind_kph": observation.wind_kph,
                        "wind_degree": observation.wind_degree,
                        "wind_direction": observation.wind_direction.value,
                        "sunrise": observation.sunrise,
                        "humidity": observation.humidity,
                        "air_quality_carbon_monoxide": observation.air_quality.carbon_monoxide,
                        "air_quality_ozone": observation.air_quality.ozone,
                        "air_quality_nitrogen_dioxide": observation.air_quality.nitrogen_dioxide,
                        "air_quality_sulphur_dioxide": observation.air_quality.sulphur_dioxide,
                        "air_quality_pm25": observation.air_quality.pm25,
                        "air_quality_pm10": observation.air_quality.pm10,
                        "air_quality_us_epa_index": observation.air_quality.us_epa_index,
                        "air_quality_gb_defra_index": observation.air_quality.gb_defra_index,
                    }
                )
                inserted += 1

                if len(payloads) >= 1000:
                    connection.execute(insert(weather_table), payloads)
                    payloads.clear()

            if payloads:
                connection.execute(insert(weather_table), payloads)

        return ImportResult(inserted=inserted, skipped=skipped, schema_stage="legacy")

    def _find_normalized(
        self,
        country: str,
        target_date: date,
        location_name: str | None,
    ) -> list[WeatherObservation]:
        with self._session_factory() as session:
            statement = (
                select(WeatherRecord)
                .options(selectinload(WeatherRecord.air_quality))
                .where(func.lower(WeatherRecord.country) == country.lower())
                .where(WeatherRecord.last_updated == target_date)
                .order_by(WeatherRecord.location_name, WeatherRecord.last_updated_time)
            )
            if location_name:
                statement = statement.where(
                    func.lower(WeatherRecord.location_name) == location_name.lower()
                )

            records = session.scalars(statement).all()
            return [self._map_model(record) for record in records]

    def _find_legacy(
        self,
        country: str,
        target_date: date,
        location_name: str | None,
    ) -> list[WeatherObservation]:
        metadata = MetaData()
        weather_table = Table("weather_records", metadata, autoload_with=self._engine)
        statement = (
            select(weather_table)
            .where(func.lower(weather_table.c.country) == country.lower())
            .where(weather_table.c.last_updated == target_date)
            .order_by(weather_table.c.location_name, weather_table.c.last_updated_time)
        )
        if location_name:
            statement = statement.where(
                func.lower(weather_table.c.location_name) == location_name.lower()
            )

        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()

        observations: list[WeatherObservation] = []
        for row in rows:
            air_quality = AirQualityReading(
                carbon_monoxide=float(row["air_quality_carbon_monoxide"]),
                ozone=float(row["air_quality_ozone"]),
                nitrogen_dioxide=float(row["air_quality_nitrogen_dioxide"]),
                sulphur_dioxide=float(row["air_quality_sulphur_dioxide"]),
                pm25=float(row["air_quality_pm25"]),
                pm10=float(row["air_quality_pm10"]),
                us_epa_index=int(row["air_quality_us_epa_index"]),
                gb_defra_index=int(row["air_quality_gb_defra_index"]),
            )
            observations.append(
                WeatherObservation(
                    country=row["country"],
                    location_name=row["location_name"],
                    timezone=row["timezone"],
                    last_updated=row["last_updated"],
                    last_updated_time=row["last_updated_time"],
                    temperature_celsius=float(row["temperature_celsius"]),
                    condition_text=row["condition_text"],
                    wind_kph=float(row["wind_kph"]),
                    wind_degree=int(row["wind_degree"]),
                    wind_direction=WindDirection(row["wind_direction"]),
                    sunrise=row["sunrise"],
                    humidity=int(row["humidity"]),
                    air_quality=air_quality.with_safety(is_outdoor_safe(air_quality)),
                )
            )
        return observations

    def _map_model(self, record: WeatherRecord) -> WeatherObservation:
        air_quality = record.air_quality
        return WeatherObservation(
            country=record.country,
            location_name=record.location_name,
            timezone=record.timezone,
            last_updated=record.last_updated,
            last_updated_time=record.last_updated_time,
            temperature_celsius=record.temperature_celsius,
            condition_text=record.condition_text,
            wind_kph=record.wind_kph,
            wind_degree=record.wind_degree,
            wind_direction=record.wind_direction,
            sunrise=record.sunrise,
            humidity=record.humidity,
            air_quality=AirQualityReading(
                carbon_monoxide=air_quality.carbon_monoxide,
                ozone=air_quality.ozone,
                nitrogen_dioxide=air_quality.nitrogen_dioxide,
                sulphur_dioxide=air_quality.sulphur_dioxide,
                pm25=air_quality.pm25,
                pm10=air_quality.pm10,
                us_epa_index=air_quality.us_epa_index,
                gb_defra_index=air_quality.gb_defra_index,
                is_outdoor_safe=air_quality.is_outdoor_safe,
            ),
        )

