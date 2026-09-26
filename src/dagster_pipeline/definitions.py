"""Dagster Definitions — central orchestration point."""
from dagster import Definitions

from dagster_pipeline.assets.weather import weather_assets
from dagster_pipeline.assets.wikipedia import wiki_assets
from dagster_pipeline.assets.postgres_loader import postgres_tables_load
from dagster_pipeline.resources.open_meteo import OpenMeteoResource
from dagster_pipeline.resources.wikimedia import WikimediaResource
from dagster_pipeline.resources.io_manager import filesystem_io_manager
from dagster_pipeline.resources.postgres import PostgresResource
from dagster_pipeline.checks.weather_checks import weather_asset_checks
from dagster_pipeline.checks.wiki_checks import wiki_asset_checks
from dagster_pipeline.schedules.weather_schedule import weather_daily_schedule
from dagster_pipeline.sensors.wiki_sensor import wikimedia_event_sensor

defs = Definitions(
    assets=[*weather_assets, *wiki_assets, postgres_tables_load],
    resources={
        "open_meteo": OpenMeteoResource(),
        "wikimedia": WikimediaResource(),
        "io": filesystem_io_manager,
        "postgres": PostgresResource(),
    },
    asset_checks=[*weather_asset_checks, *wiki_asset_checks],
    schedules=[weather_daily_schedule],
    sensors=[wikimedia_event_sensor],
)
