from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0002_split_air_quality"
down_revision = "0001_initial_weather_schema"
branch_labels = None
depends_on = None


def _is_outdoor_safe(row: sa.RowMapping) -> bool:
    return (
        float(row["air_quality_pm25"]) <= 35.0
        and float(row["air_quality_pm10"]) <= 50.0
        and int(row["air_quality_us_epa_index"]) <= 3
        and int(row["air_quality_gb_defra_index"]) <= 3
        and float(row["air_quality_carbon_monoxide"]) <= 4400.0
        and float(row["air_quality_ozone"]) <= 100.0
        and float(row["air_quality_nitrogen_dioxide"]) <= 40.0
        and float(row["air_quality_sulphur_dioxide"]) <= 20.0
    )


def _flush_in_chunks(
    connection: sa.Connection,
    table: sa.Table,
    payloads: list[dict[str, object]],
) -> None:
    if payloads:
        connection.execute(sa.insert(table), payloads)
        payloads.clear()


def upgrade() -> None:
    op.create_table(
        "air_quality_metrics",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("weather_record_id", sa.Integer(), nullable=False),
        sa.Column("carbon_monoxide", sa.Float(), nullable=False),
        sa.Column("ozone", sa.Float(), nullable=False),
        sa.Column("nitrogen_dioxide", sa.Float(), nullable=False),
        sa.Column("sulphur_dioxide", sa.Float(), nullable=False),
        sa.Column("pm25", sa.Float(), nullable=False),
        sa.Column("pm10", sa.Float(), nullable=False),
        sa.Column("us_epa_index", sa.Integer(), nullable=False),
        sa.Column("gb_defra_index", sa.Integer(), nullable=False),
        sa.Column("is_outdoor_safe", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["weather_record_id"],
            ["weather_records.id"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("weather_record_id", name="uq_air_quality_metrics_weather_record_id"),
    )
    op.create_index(
        "ix_air_quality_metrics_weather_record_id",
        "air_quality_metrics",
        ["weather_record_id"],
        unique=True,
    )

    connection = op.get_bind()
    metadata = sa.MetaData()
    weather_records = sa.Table(
        "weather_records",
        metadata,
        sa.Column("id", sa.Integer()),
        sa.Column("air_quality_carbon_monoxide", sa.Float()),
        sa.Column("air_quality_ozone", sa.Float()),
        sa.Column("air_quality_nitrogen_dioxide", sa.Float()),
        sa.Column("air_quality_sulphur_dioxide", sa.Float()),
        sa.Column("air_quality_pm25", sa.Float()),
        sa.Column("air_quality_pm10", sa.Float()),
        sa.Column("air_quality_us_epa_index", sa.Integer()),
        sa.Column("air_quality_gb_defra_index", sa.Integer()),
    )
    air_quality_metrics = sa.Table(
        "air_quality_metrics",
        metadata,
        sa.Column("weather_record_id", sa.Integer()),
        sa.Column("carbon_monoxide", sa.Float()),
        sa.Column("ozone", sa.Float()),
        sa.Column("nitrogen_dioxide", sa.Float()),
        sa.Column("sulphur_dioxide", sa.Float()),
        sa.Column("pm25", sa.Float()),
        sa.Column("pm10", sa.Float()),
        sa.Column("us_epa_index", sa.Integer()),
        sa.Column("gb_defra_index", sa.Integer()),
        sa.Column("is_outdoor_safe", sa.Boolean()),
    )

    rows = connection.execute(sa.select(weather_records)).mappings()
    payloads: list[dict[str, object]] = []
    for row in rows:
        payloads.append(
            {
                "weather_record_id": row["id"],
                "carbon_monoxide": row["air_quality_carbon_monoxide"],
                "ozone": row["air_quality_ozone"],
                "nitrogen_dioxide": row["air_quality_nitrogen_dioxide"],
                "sulphur_dioxide": row["air_quality_sulphur_dioxide"],
                "pm25": row["air_quality_pm25"],
                "pm10": row["air_quality_pm10"],
                "us_epa_index": row["air_quality_us_epa_index"],
                "gb_defra_index": row["air_quality_gb_defra_index"],
                "is_outdoor_safe": _is_outdoor_safe(row),
            }
        )
        if len(payloads) >= 1000:
            _flush_in_chunks(connection, air_quality_metrics, payloads)
    _flush_in_chunks(connection, air_quality_metrics, payloads)

    with op.batch_alter_table("weather_records") as batch_op:
        batch_op.drop_column("air_quality_carbon_monoxide")
        batch_op.drop_column("air_quality_ozone")
        batch_op.drop_column("air_quality_nitrogen_dioxide")
        batch_op.drop_column("air_quality_sulphur_dioxide")
        batch_op.drop_column("air_quality_pm25")
        batch_op.drop_column("air_quality_pm10")
        batch_op.drop_column("air_quality_us_epa_index")
        batch_op.drop_column("air_quality_gb_defra_index")


def downgrade() -> None:
    with op.batch_alter_table("weather_records") as batch_op:
        batch_op.add_column(sa.Column("air_quality_carbon_monoxide", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("air_quality_ozone", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("air_quality_nitrogen_dioxide", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("air_quality_sulphur_dioxide", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("air_quality_pm25", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("air_quality_pm10", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("air_quality_us_epa_index", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("air_quality_gb_defra_index", sa.Integer(), nullable=True))

    connection = op.get_bind()
    metadata = sa.MetaData()
    weather_records = sa.Table(
        "weather_records",
        metadata,
        sa.Column("id", sa.Integer()),
        sa.Column("air_quality_carbon_monoxide", sa.Float()),
        sa.Column("air_quality_ozone", sa.Float()),
        sa.Column("air_quality_nitrogen_dioxide", sa.Float()),
        sa.Column("air_quality_sulphur_dioxide", sa.Float()),
        sa.Column("air_quality_pm25", sa.Float()),
        sa.Column("air_quality_pm10", sa.Float()),
        sa.Column("air_quality_us_epa_index", sa.Integer()),
        sa.Column("air_quality_gb_defra_index", sa.Integer()),
    )
    air_quality_metrics = sa.Table(
        "air_quality_metrics",
        metadata,
        sa.Column("weather_record_id", sa.Integer()),
        sa.Column("carbon_monoxide", sa.Float()),
        sa.Column("ozone", sa.Float()),
        sa.Column("nitrogen_dioxide", sa.Float()),
        sa.Column("sulphur_dioxide", sa.Float()),
        sa.Column("pm25", sa.Float()),
        sa.Column("pm10", sa.Float()),
        sa.Column("us_epa_index", sa.Integer()),
        sa.Column("gb_defra_index", sa.Integer()),
    )

    rows = connection.execute(sa.select(air_quality_metrics)).mappings()
    for row in rows:
        connection.execute(
            sa.update(weather_records)
            .where(weather_records.c.id == row["weather_record_id"])
            .values(
                air_quality_carbon_monoxide=row["carbon_monoxide"],
                air_quality_ozone=row["ozone"],
                air_quality_nitrogen_dioxide=row["nitrogen_dioxide"],
                air_quality_sulphur_dioxide=row["sulphur_dioxide"],
                air_quality_pm25=row["pm25"],
                air_quality_pm10=row["pm10"],
                air_quality_us_epa_index=row["us_epa_index"],
                air_quality_gb_defra_index=row["gb_defra_index"],
            )
        )

    with op.batch_alter_table("weather_records") as batch_op:
        batch_op.alter_column("air_quality_carbon_monoxide", nullable=False)
        batch_op.alter_column("air_quality_ozone", nullable=False)
        batch_op.alter_column("air_quality_nitrogen_dioxide", nullable=False)
        batch_op.alter_column("air_quality_sulphur_dioxide", nullable=False)
        batch_op.alter_column("air_quality_pm25", nullable=False)
        batch_op.alter_column("air_quality_pm10", nullable=False)
        batch_op.alter_column("air_quality_us_epa_index", nullable=False)
        batch_op.alter_column("air_quality_gb_defra_index", nullable=False)

    op.drop_index("ix_air_quality_metrics_weather_record_id", table_name="air_quality_metrics")
    op.drop_table("air_quality_metrics")
