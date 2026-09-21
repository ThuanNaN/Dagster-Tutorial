"""Open-Meteo resource for weather data retrieval."""
import logging

from dagster import ConfigurableResource, EnvVar

from dagster_real_data.utils.http import HttpClient, HttpClientConfig
from dagster_real_data.utils.time import format_date

logger = logging.getLogger(__name__)


class OpenMeteoResource(ConfigurableResource):
    base_url: str = EnvVar("OPEN_METEO_BASE_URL")
    archive_url: str = EnvVar("OPEN_METEO_ARCHIVE_URL")
    latitude: float = 21.0285
    longitude: float = 105.8542
    timezone: str = EnvVar("WEATHER_TIMEZONE")
    timeout: int = 30

    def _get_client(self) -> HttpClient:
        config = HttpClientConfig(base_url=self.base_url, timeout=self.timeout)
        return HttpClient(config)

    def _get_archive_client(self) -> HttpClient:
        config = HttpClientConfig(base_url=self.archive_url, timeout=self.timeout)
        return HttpClient(config)

    def get_forecast(
        self, forecast_days: int = 7, hourly_params: list | None = None, daily_params: list | None = None
    ) -> dict:
        client = self._get_client()
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timezone": self.timezone,
            "forecast_days": forecast_days,
            "hourly": ",".join(hourly_params or ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]),
            "daily": ",".join(daily_params or ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]),
        }
        logger.info("Fetching forecast for %s,%s", self.latitude, self.longitude)
        return client.get_json("v1/forecast", params)

    def get_historical(
        self, start_date: str, end_date: str, hourly_params: list | None = None, daily_params: list | None = None
    ) -> dict:
        client = self._get_archive_client()
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": self.timezone,
            "hourly": ",".join(hourly_params or ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]),
            "daily": ",".join(daily_params or ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]),
        }
        logger.info("Fetching historical data %s to %s", start_date, end_date)
        return client.get_json("v1/archive", params)

    def get_historical_forecast(self, start_date: str, end_date: str) -> dict:
        client = self._get_client()
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timezone": self.timezone,
            "start_date": start_date,
            "end_date": end_date,
        }
        logger.info("Fetching historical forecast %s to %s", start_date, end_date)
        return client.get_json("v1/forecast", params)
