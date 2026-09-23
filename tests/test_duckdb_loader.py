"""Tests for DuckDB loader asset."""
import pandas as pd
import duckdb
from dagster_real_data.assets.duckdb_loader import DuckDBLoadAsset


def test_duckdb_load_asset_creates_tables(tmp_path):
    """Test that duckdb_loader creates DuckDB tables from parquet files."""
    # Setup: create dummy parquet files
    bronze_dir = tmp_path / "bronze" / "weather_historical" / "2026-01-15"
    bronze_dir.mkdir(parents=True)
    df = pd.DataFrame({"date": ["2026-01-15"], "temp": [25.0]})
    df.to_parquet(bronze_dir / "output.parquet", index=False)

    # Run loader
    loader = DuckDBLoadAsset(data_dir=str(tmp_path), db_path=str(tmp_path / "test.duckdb"))
    loader.load_to_duckdb()

    # Verify tables exist in DuckDB
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    tables = conn.execute("SHOW TABLES").fetchall()
    assert len(tables) > 0


def test_duckdb_load_asset_row_count_matches_parquet(tmp_path):
    """Verify DuckDB table row count matches parquet file row count."""
    # Setup parquet with known row count
    bronze_dir = tmp_path / "bronze" / "weather_historical" / "2026-01-15"
    bronze_dir.mkdir(parents=True)
    df = pd.DataFrame({"a": range(100)})
    df.to_parquet(bronze_dir / "output.parquet", index=False)

    loader = DuckDBLoadAsset(data_dir=str(tmp_path), db_path=str(tmp_path / "test.duckdb"))
    loader.load_to_duckdb()

    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    count = conn.execute("SELECT COUNT(*) FROM weather_historical").fetchone()[0]
    assert count == 100
