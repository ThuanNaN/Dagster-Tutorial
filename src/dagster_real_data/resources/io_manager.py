"""Filesystem IOManager for persisting assets as Bronze/Silver/Gold lakehouse tiers.

Tier mapping:
- Bronze: raw API data (weather_historical, weather_hourly, weather_forecast, wiki_events_raw)
- Silver: cleaned/validated data (weather_cleaned, wiki_events_cleaned)
- Gold: aggregated/business-ready data (weather_daily, forecast_accuracy, wiki_events_by_hour, wiki_daily_analytics)
"""
import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import IOManager, IOManagerDefinition, OutputContext, InputContext

logger = logging.getLogger(__name__)

# Asset → tier mapping
ASSET_TIERS = {
    # Bronze: raw
    "weather_historical": "bronze",
    "weather_hourly": "bronze",
    "weather_forecast": "bronze",
    "wiki_events_raw": "bronze",
    # Silver: cleaned
    "weather_cleaned": "silver",
    "wiki_events_cleaned": "silver",
    # Gold: aggregated
    "weather_daily": "gold",
    "forecast_accuracy": "gold",
    "wiki_events_by_hour": "gold",
    "wiki_daily_analytics": "gold",
}


def _get_data_dir(context: OutputContext | InputContext) -> Path:
    data_dir = os.environ.get("DATA_DIR", "./data")
    return Path(data_dir)


def _get_tier(asset_name: str) -> str:
    return ASSET_TIERS.get(asset_name, "bronze")


class FilesystemIOManager(IOManager):
    """IOManager that persists outputs into Bronze/Silver/Gold tier directories."""

    def __init__(self, base_dir: str | None = None):
        self.base_dir = Path(base_dir or os.environ.get("DATA_DIR", "./data"))

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        asset_name = context.asset_key.path[-1]
        tier = _get_tier(asset_name)
        partition = context.asset_partitions_decorated or ""
        partition_str = str(partition) if partition else "latest"
        output_dir = self.base_dir / tier / asset_name / partition_str
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / "output.json"
        if isinstance(obj, pd.DataFrame):
            file_path = output_dir / "output.parquet"
            obj.to_parquet(file_path, index=False)
            logger.info("Wrote %d rows to [%s] parquet: %s", len(obj), tier, file_path)
        elif isinstance(obj, list):
            with open(file_path, "w") as f:
                json.dump(obj, f, indent=2, default=str)
            logger.info("Wrote %d items to [%s] JSON: %s", len(obj), tier, file_path)
        else:
            with open(file_path, "w") as f:
                json.dump({"data": obj}, f, indent=2, default=str)
            logger.info("Wrote output to [%s] JSON: %s", tier, file_path)

    def load_input(self, context: InputContext) -> Any:
        upstream_key = context.upstream_asset_key
        asset_name = upstream_key.path[-1] if upstream_key else context.asset_key.path[-1]
        tier = _get_tier(asset_name)
        partition = context.asset_partitions_decorated or ""
        partition_str = str(partition) if partition else "latest"
        input_dir = self.base_dir / tier / asset_name / partition_str

        parquet_path = input_dir / "output.parquet"
        json_path = input_dir / "output.json"

        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            logger.info("Loaded %d rows from [%s] parquet: %s", len(df), tier, parquet_path)
            return df
        elif json_path.exists():
            with open(json_path) as f:
                data = json.load(f)
            logger.info("Loaded data from [%s] JSON: %s", tier, json_path)
            return data
        else:
            raise FileNotFoundError(f"No input data found at {input_dir}")


filesystem_io_manager = IOManagerDefinition(
    resource_fn=lambda: FilesystemIOManager(),
)
