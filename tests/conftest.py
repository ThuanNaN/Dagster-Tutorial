"""Shared test fixtures and utilities."""
import pytest
from unittest.mock import MagicMock
import pandas as pd


@pytest.fixture
def mock_open_meteo_resource():
    resource = MagicMock()
    resource.latitude = 21.0285
    resource.longitude = 105.8542
    resource.timezone = "Asia/Bangkok"
    resource.timeout = 30

    resource.get_historical.return_value = {
        "hourly": {
            "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
            "temperature_2m": [25.0, 26.0],
            "relative_humidity_2m": [60.0, 65.0],
            "precipitation": [0.0, 0.5],
            "wind_speed_10m": [5.0, 6.0],
        }
    }
    resource.get_forecast.return_value = {
        "hourly": {
            "time": ["2026-09-22T00:00"],
            "temperature_2m": [27.0],
            "relative_humidity_2m": [55.0],
            "precipitation": [0.0],
            "wind_speed_10m": [4.0],
        }
    }
    return resource


@pytest.fixture
def mock_wikimedia_resource():
    resource = MagicMock()
    resource.max_events = 100
    resource.stream_events.return_value = [
        {
            "event_id": "1",
            "timestamp": "2026-01-01T00:00:00Z",
            "wiki": "enwiki",
            "title": "Test Page",
            "user": "Alice",
            "bot": False,
            "type": "edit",
        },
        {
            "event_id": "2",
            "timestamp": "2026-01-01T00:30:00Z",
            "wiki": "enwiki",
            "title": "Test Page 2",
            "user": "Bob",
            "bot": False,
            "type": "edit",
        },
        {
            "event_id": "3",
            "timestamp": "2026-01-01T01:00:00Z",
            "wiki": "enwiki",
            "title": "Test Page 3",
            "user": "Alice",
            "bot": False,
            "type": "edit",
        },
    ]
    resource.get_cursor_state.return_value = {
        "last_processed_event_id": "3",
        "last_processed_timestamp": "2026-01-01T01:00:00Z",
    }
    return resource


@pytest.fixture
def sample_weather_dataframe():
    return pd.DataFrame({
        "timestamp": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "temperature_2m": [25.0, 26.0],
        "relative_humidity_2m": [60.0, 65.0],
        "precipitation": [0.0, 0.5],
        "wind_speed_10m": [5.0, 6.0],
    })
