"""Tests for resources."""
from unittest.mock import patch, MagicMock
import pytest
import tempfile
import json
from pathlib import Path

from dagster_real_data.resources.open_meteo import OpenMeteoResource
from dagster_real_data.resources.wikimedia import WikimediaResource
from dagster_real_data.resources.io_manager import FilesystemIOManager


def test_open_meteo_resource_creation():
    resource = OpenMeteoResource(
        base_url="https://api.open-meteo.com",
        archive_url="https://archive-api.open-meteo.com",
        latitude=21.0285,
        longitude=105.8542,
        timezone="Asia/Bangkok",
    )
    assert resource.latitude == 21.0285
    assert resource.timeout == 30


def test_open_meteo_resource_get_forecast():
    resource = OpenMeteoResource(
        base_url="https://api.open-meteo.com",
        archive_url="https://archive-api.open-meteo.com",
        latitude=21.0285,
        longitude=105.8542,
        timezone="Asia/Bangkok",
    )
    import requests
    with patch("requests.Session.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {"hourly": {"time": ["2026-01-01T00:00"]}}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response
        result = resource.get_forecast(forecast_days=1)
        assert "hourly" in result


def test_wikimedia_resource_creation():
    resource = WikimediaResource(
        stream_url="https://stream.wikimedia.org/v2/stream/recentchange",
    )
    assert resource.max_events == 100
    assert resource.timeout == 30


def test_wikimedia_resource_get_cursor_state():
    resource = WikimediaResource(stream_url="https://stream.wikimedia.org/v2/stream/recentchange")
    event = {
        "event_id": "12345",
        "timestamp": "2026-01-01T00:00:00Z",
        "wiki": "enwiki",
        "title": "Test Page",
        "user": "TestUser",
        "bot": False,
        "type": "edit",
    }
    cursor = resource.get_cursor_state(event)
    assert cursor["last_processed_event_id"] == "12345"
    assert cursor["last_processed_timestamp"] == "2026-01-01T00:00:00Z"


def test_filesystem_io_manager_write_json():
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = FilesystemIOManager(base_dir=tmpdir)
        context = MagicMock()
        context.asset_key.path = ["test_asset"]
        context.asset_partitions_decorated = "2026-01-01"

        data = [{"id": 1, "value": "test"}]
        manager.handle_output(context, data)

        output_path = Path(tmpdir) / "test_asset" / "2026-01-01" / "output.json"
        assert output_path.exists()
        with open(output_path) as f:
            loaded = json.load(f)
        assert loaded == data


def test_filesystem_io_manager_write_dataframe():
    import pandas as pd
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = FilesystemIOManager(base_dir=tmpdir)
        context = MagicMock()
        context.asset_key.path = ["weather_cleaned"]
        context.asset_partitions_decorated = "2026-01-01"

        df = pd.DataFrame({"timestamp": ["2026-01-01"], "temperature_2m": [25.5]})
        manager.handle_output(context, df)

        output_path = Path(tmpdir) / "weather_cleaned" / "2026-01-01" / "output.parquet"
        assert output_path.exists()
        loaded = pd.read_parquet(output_path)
        assert len(loaded) == 1


def test_filesystem_io_manager_load_input():
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = FilesystemIOManager(base_dir=tmpdir)

        asset_dir = Path(tmpdir) / "test_asset" / "2026-01-01"
        asset_dir.mkdir(parents=True, exist_ok=True)
        with open(asset_dir / "output.json", "w") as f:
            json.dump([{"id": 1}], f)

        context = MagicMock()
        context.upstream_asset_key.path = ["test_asset"]
        context.asset_partitions_decorated = "2026-01-01"

        result = manager.load_input(context)
        assert result == [{"id": 1}]
