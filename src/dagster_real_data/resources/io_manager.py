"""Filesystem IOManager for persisting assets as JSON/Parquet."""
import json
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import IOManager, IOManagerDefinition, OutputContext, InputContext

logger = logging.getLogger(__name__)


def _get_data_dir(context: OutputContext | InputContext) -> Path:
    data_dir = os.environ.get("DATA_DIR", "./data")
    return Path(data_dir)


class FilesystemIOManager(IOManager):
    """IOManager that persists outputs to filesystem as JSON or Parquet."""

    def __init__(self, base_dir: str | None = None):
        self.base_dir = Path(base_dir or os.environ.get("DATA_DIR", "./data"))

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        asset_name = context.asset_key.path[-1]
        partition = context.asset_partitions_decorated or ""
        partition_str = str(partition) if partition else "latest"
        output_dir = self.base_dir / asset_name / partition_str
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / "output.json"
        if isinstance(obj, pd.DataFrame):
            file_path = output_dir / "output.parquet"
            obj.to_parquet(file_path, index=False)
            logger.info("Wrote %d rows to parquet: %s", len(obj), file_path)
        elif isinstance(obj, list):
            with open(file_path, "w") as f:
                json.dump(obj, f, indent=2, default=str)
            logger.info("Wrote %d items to JSON: %s", len(obj), file_path)
        else:
            with open(file_path, "w") as f:
                json.dump({"data": obj}, f, indent=2, default=str)
            logger.info("Wrote output to JSON: %s", file_path)

    def load_input(self, context: InputContext) -> Any:
        upstream_key = context.upstream_asset_key
        asset_name = upstream_key.path[-1] if upstream_key else context.asset_key.path[-1]
        partition = context.asset_partitions_decorated or ""
        partition_str = str(partition) if partition else "latest"
        input_dir = self.base_dir / asset_name / partition_str

        parquet_path = input_dir / "output.parquet"
        json_path = input_dir / "output.json"

        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            logger.info("Loaded %d rows from parquet: %s", len(df), parquet_path)
            return df
        elif json_path.exists():
            with open(json_path) as f:
                data = json.load(f)
            logger.info("Loaded data from JSON: %s", json_path)
            return data
        else:
            raise FileNotFoundError(f"No input data found at {input_dir}")


filesystem_io_manager = IOManagerDefinition(
    resource_fn=lambda: FilesystemIOManager(),
)
