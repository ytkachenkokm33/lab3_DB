from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_initial_weather_schema"
down_revision = None
branch_labels = None
depends_on = None


wind_direction_enum = sa.Enum(
    "N",
    "NNE",
    "NE",
    "ENE",
    "E",
    "ESE",
    "SE",
    "SSE",
    "S",
    "SSW",
    "SW",
    "WSW",
    "W",
    "WNW",
    "NW",
    "NNW",
    name="wind_direction_enum",
)


def upgrade() -> None:
    op.create_table(
        "weather_records",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("country", sa.String(length=120), nullable=False),
        sa.Column("location_name", sa.String(length=120), nullable=False),
        sa.Column("timezone", sa.String(length=120), nullable=False),
        sa.Column("last_updated", sa.Date(), nullable=False),
        sa.Column("last_updated_time", sa.Time(), nullable=False),
        sa.Column("temperature_celsius", sa.Float(), nullable=False),
        sa.Column("condition_text", sa.String(length=120), nullable=False),
        sa.Column("wind_kph", sa.Float(), nullable=False),
        sa.Column("wind_degree", sa.Integer(), nullable=False),
        sa.Column("wind_direction", wind_direction_enum, nullable=False),
        sa.Column("sunrise", sa.Time(), nullable=False),
        sa.Column("humidity", sa.Integer(), nullable=False),
        sa.Column("air_quality_carbon_monoxide", sa.Float(), nullable=False),
        sa.Column("air_quality_ozone", sa.Float(), nullable=False),
        sa.Column("air_quality_nitrogen_dioxide", sa.Float(), nullable=False),
        sa.Column("air_quality_sulphur_dioxide", sa.Float(), nullable=False),
        sa.Column("air_quality_pm25", sa.Float(), nullable=False),
        sa.Column("air_quality_pm10", sa.Float(), nullable=False),
        sa.Column("air_quality_us_epa_index", sa.Integer(), nullable=False),
        sa.Column("air_quality_gb_defra_index", sa.Integer(), nullable=False),
        sa.UniqueConstraint(
            "country",
            "location_name",
            "last_updated",
            "last_updated_time",
            name="uq_weather_record_identity",
        ),
    )
    op.create_index(
        "ix_weather_records_country_last_updated",
        "weather_records",
        ["country", "last_updated"],
    )


def downgrade() -> None:
    op.drop_index("ix_weather_records_country_last_updated", table_name="weather_records")
    op.drop_table("weather_records")
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        wind_direction_enum.drop(bind, checkfirst=True)

