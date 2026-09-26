"""PostgreSQL loader asset — loads parquet data into PostgreSQL tables."""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import AssetExecutionContext, asset, AssetIn

from dagster_real_data.resources.postgres import PostgresResource
from dagster_real_data.resources.io_manager import ASSET_TIERS

logger = logging.getLogger(__name__)

# Map asset names to PostgreSQL table names
ASSET_TO_TABLE = {name: name for name in ASSET_TIERS.keys()}


class PostgresLoadAsset:
    """Utility to load parquet files into PostgreSQL tables."""

    def __init__(self, data_dir: str, postgres_resource: PostgresResource):
        self.data_dir = Path(data_dir)
        self.postgres = postgres_resource

    def load_to_postgres(self) -> None:
        """Load all parquet files into PostgreSQL tables."""
        engine = self.postgres.get_engine()
        try:
            for asset_name, tier in ASSET_TIERS.items():
                parquet_path = self.data_dir / tier / asset_name
                if not parquet_path.exists():
                    logger.warning("No data directory for %s at %s", asset_name, parquet_path)
                    continue

                for partition_dir in parquet_path.iterdir():
                    if not partition_dir.is_dir():
                        continue
                    parquet_file = partition_dir / "output.parquet"
                    if not parquet_file.exists():
                        continue

                    table_name = ASSET_TO_TABLE.get(asset_name, asset_name)
                    df = pd.read_parquet(parquet_file)
                    df.to_sql(table_name, engine, if_exists="replace", index=False)
                    logger.info(
                        "Loaded %d rows from %s into PostgreSQL table %s",
                        len(df), parquet_file, table_name,
                    )
        finally:
            engine.dispose()


@asset(
    name="postgres_tables",
    description="Loads all parquet data from Bronze/Silver/Gold tiers into PostgreSQL tables",
    ins={
        "weather_historical": AssetIn(key=["weather_historical"]),
        "weather_hourly": AssetIn(key=["weather_hourly"]),
        "weather_forecast": AssetIn(key=["weather_forecast"]),
        "wiki_events_raw": AssetIn(key=["wiki_events_raw"]),
    },
)
def postgres_tables_load(
    context: AssetExecutionContext,
    weather_historical: Any,
    weather_hourly: Any,
    weather_forecast: Any,
    wiki_events_raw: Any,
) -> None:
    """Dagster asset that triggers PostgreSQL load from parquet files."""
    data_dir = context.resources.io.base_dir if hasattr(context.resources.io, 'base_dir') else "./data"
    loader = PostgresLoadAsset(data_dir=data_dir, postgres_resource=context.resources.postgres)
    loader.load_to_postgres()
