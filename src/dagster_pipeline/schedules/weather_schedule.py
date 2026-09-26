"""Daily weather schedule — runs at 23:00 UTC."""
from dagster import ScheduleDefinition, AssetKey

weather_daily_schedule = ScheduleDefinition(
    name="weather_daily_schedule",
    cron_schedule="0 23 * * *",
    target="weather_historical",
    description="Materialize daily weather partition at 23:00 UTC",
)
