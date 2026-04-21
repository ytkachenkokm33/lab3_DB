from __future__ import annotations

from datetime import date, time

from sqlalchemy import Boolean, Date, Enum, Float, ForeignKey, Integer, String, Time, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.domain.entities.weather import WindDirection
from app.infrastructure.database.base import Base


class WeatherRecord(Base):
    __tablename__ = "weather_records"
    __table_args__ = (
        UniqueConstraint(
            "country",
            "location_name",
            "last_updated",
            "last_updated_time",
            name="uq_weather_record_identity",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    country: Mapped[str] = mapped_column(String(120), nullable=False)
    location_name: Mapped[str] = mapped_column(String(120), nullable=False)
    timezone: Mapped[str] = mapped_column(String(120), nullable=False)
    last_updated: Mapped[date] = mapped_column(Date, nullable=False)
    last_updated_time: Mapped[time] = mapped_column(Time, nullable=False)
    temperature_celsius: Mapped[float] = mapped_column(Float, nullable=False)
    condition_text: Mapped[str] = mapped_column(String(120), nullable=False)
    wind_kph: Mapped[float] = mapped_column(Float, nullable=False)
    wind_degree: Mapped[int] = mapped_column(Integer, nullable=False)
    wind_direction: Mapped[WindDirection] = mapped_column(
        Enum(WindDirection, name="wind_direction_enum"),
        nullable=False,
    )
    sunrise: Mapped[time] = mapped_column(Time, nullable=False)
    humidity: Mapped[int] = mapped_column(Integer, nullable=False)

    air_quality: Mapped["AirQualityMetric"] = relationship(
        back_populates="weather_record",
        cascade="all, delete-orphan",
        uselist=False,
    )


class AirQualityMetric(Base):
    __tablename__ = "air_quality_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    weather_record_id: Mapped[int] = mapped_column(
        ForeignKey("weather_records.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    carbon_monoxide: Mapped[float] = mapped_column(Float, nullable=False)
    ozone: Mapped[float] = mapped_column(Float, nullable=False)
    nitrogen_dioxide: Mapped[float] = mapped_column(Float, nullable=False)
    sulphur_dioxide: Mapped[float] = mapped_column(Float, nullable=False)
    pm25: Mapped[float] = mapped_column(Float, nullable=False)
    pm10: Mapped[float] = mapped_column(Float, nullable=False)
    us_epa_index: Mapped[int] = mapped_column(Integer, nullable=False)
    gb_defra_index: Mapped[int] = mapped_column(Integer, nullable=False)
    is_outdoor_safe: Mapped[bool] = mapped_column(Boolean, nullable=False)

    weather_record: Mapped[WeatherRecord] = relationship(back_populates="air_quality")
