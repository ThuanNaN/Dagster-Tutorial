"""DuckDB loader asset — loads parquet data into DuckDB tables."""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import duckdb
from dagster import AssetExecutionContext, asset, AssetIn

from dagster_real_data.resources.duckdb import DuckDBResource
from dagster_real_data.resources.io_manager import ASSET_TIERS

logger = logging.getLogger(__name__)

# Map asset names to DuckDB table names
ASSET_TO_TABLE = {name: name for name in ASSET_TIERS.keys()}


class DuckDBLoadAsset:
    """Utility to load parquet files into DuckDB tables."""

    def __init__(self, data_dir: str, db_path: str):
        self.data_dir = Path(data_dir)
        self.db_path = Path(db_path)

    def load_to_duckdb(self) -> None:
        """Load all parquet files into DuckDB tables."""
        conn = duckdb.connect(str(self.db_path))

        for asset_name, tier in ASSET_TIERS.items():
            parquet_path = self.data_dir / tier / asset_name
            if not parquet_path.exists():
                logger.warning("No data directory for %s at %s", asset_name, parquet_path)
                continue

            # Find all partition parquet files
            for partition_dir in parquet_path.iterdir():
                if not partition_dir.is_dir():
                    continue
                parquet_file = partition_dir / "output.parquet"
                if not parquet_file.exists():
                    continue

                table_name = ASSET_TO_TABLE.get(asset_name, asset_name)
                df = pd.read_parquet(parquet_file)
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
                logger.info(
                    "Loaded %d rows from %s into DuckDB table %s",
                    len(df), parquet_file, table_name,
                )

        conn.close()


@asset(
    name="duckdb_tables",
    description="Loads all parquet data from Bronze/Silver/Gold tiers into DuckDB tables",
    ins={
        "weather_historical": AssetIn(key=["weather_historical"]),
        "weather_hourly": AssetIn(key=["weather_hourly"]),
        "weather_forecast": AssetIn(key=["weather_forecast"]),
        "wiki_events_raw": AssetIn(key=["wiki_events_raw"]),
    },
)
def duckdb_tables_load(
    context: AssetExecutionContext,
    weather_historical: Any,
    weather_hourly: Any,
    weather_forecast: Any,
    wiki_events_raw: Any,
) -> None:
    """Dagster asset that triggers DuckDB load from parquet files."""
    data_dir = context.resources.io.base_dir if hasattr(context.resources.io, 'base_dir') else "./data"
    loader = DuckDBLoadAsset(data_dir=data_dir, db_path=context.resources.duckdb.database)
    loader.load_to_duckdb()
