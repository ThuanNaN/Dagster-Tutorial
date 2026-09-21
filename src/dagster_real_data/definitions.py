"""Dagster Definitions — central orchestration point."""
from dagster import Definitions

from dagster_real_data.assets.weather import weather_assets
from dagster_real_data.assets.wikipedia import wiki_assets
from dagster_real_data.resources.open_meteo import OpenMeteoResource
from dagster_real_data.resources.wikimedia import WikimediaResource
from dagster_real_data.resources.io_manager import filesystem_io_manager
from dagster_real_data.checks.weather_checks import weather_asset_checks
from dagster_real_data.checks.wiki_checks import wiki_asset_checks
from dagster_real_data.schedules.weather_schedule import weather_daily_schedule
from dagster_real_data.sensors.wiki_sensor import wikimedia_event_sensor

defs = Definitions(
    assets=[*weather_assets, *wiki_assets],
    resources={
        "open_meteo": OpenMeteoResource(),
        "wikimedia": WikimediaResource(),
        "io": filesystem_io_manager,
    },
    asset_checks=[*weather_asset_checks, *wiki_asset_checks],
    schedules=[weather_daily_schedule],
    sensors=[wikimedia_event_sensor],
)