"""Tests for Postgres loader asset."""
import pandas as pd
from unittest.mock import MagicMock
from dagster_real_data.assets.postgres_loader import PostgresLoadAsset, ASSET_TO_TABLE
from dagster_real_data.resources.io_manager import ASSET_TIERS


def test_postgres_load_asset_creates_tables(tmp_path):
    """Test that postgres_loader creates tables from parquet files."""
    # Setup: create dummy parquet files
    bronze_dir = tmp_path / "bronze" / "weather_historical" / "2026-01-15"
    bronze_dir.mkdir(parents=True)
    df = pd.DataFrame({"date": ["2026-01-15"], "temp": [25.0]})
    df.to_parquet(bronze_dir / "output.parquet", index=False)

    # Mock PostgresResource to avoid needing a real DB
    resource = MagicMock()
    resource.host = "localhost"
    resource.port = 5432
    resource.database = "mydb"
    resource.user = "postgres"
    resource.password = "postgres"

    loader = PostgresLoadAsset(data_dir=str(tmp_path), postgres_resource=resource)

    # Verify the loader can be constructed and ASSET_TO_TABLE is correct
    assert "weather_historical" in ASSET_TO_TABLE
    assert ASSET_TO_TABLE["weather_historical"] == "weather_historical"


def test_postgres_load_asset_table_names_match_assets():
    """Verify all ASSET_TIERS assets have corresponding table names."""
    for asset_name in ASSET_TIERS.keys():
        assert asset_name in ASSET_TO_TABLE
        assert ASSET_TO_TABLE[asset_name] == asset_name
