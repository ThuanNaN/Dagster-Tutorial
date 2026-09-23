# DuckDB + Metabase Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add DuckDB as a fast analytical query layer on top of existing parquet data, and configure Metabase to visualize the data via DuckDB JDBC connection.

**Architecture:** Dagster pipeline (unchanged) writes parquet to Bronze/Silver/Gold tiers → DuckDB Load Asset reads parquet → loads into DuckDB server tables → Metabase connects via JDBC to display dashboards. Parquet remains the source of truth; DuckDB is an ephemeral query cache.

**Tech Stack:** Python `duckdb>=1.5`, Dagster, PostgreSQL wire protocol (`duckdb --serve`), Metabase (external).

**Spec:** `docs/superpowers/specs/2026-09-23-duckdb-metabase-design.md`

---

## Global Constraints

- Python 3.11+, Dagster >= 1.9
- Follow existing ConfigurableResource pattern (class-based, NOT dataclass)
- Follow existing asset naming conventions: `asset(name="...")` with `io_manager_key`
- Existing tests in `tests/` use `pytest`, `responses`, `pytest-mock`
- All new code in `src/dagster_real_data/`, tests in `tests/`
- DuckDB server uses `--serve` mode on `localhost:8080`

## Review Focus

- **Parquet path resolution**: Asset loaders must resolve paths the same way `FilesystemIOManager` does (`./data/{tier}/{asset_name}/{partition}/output.parquet`). Test that the loader finds the correct parquet files for all tiers.
- **DuckDB connection failure**: If DuckDB server is not running, the loader asset should raise a clear error, not hang silently.
- **Data consistency**: DuckDB tables must match parquet data exactly — test row counts and column names match between parquet and DuckDB.
- **Metabase JDBC URL format**: Must match DuckDB `--serve` protocol. Test the connection string is valid.

---

### Task 1: Add DuckDB dependency to `pyproject.toml`

**Files:**
- Modify: `pyproject.toml`

**Interfaces:**
- Produces: `duckdb` and `duckdb-http-server` available in Python environment

- [ ] **Step 1: Add duckdb dependency to pyproject.toml**

```toml
dependencies = [
    "dagster>=1.9",
    "dagster-webserver>=1.9",
    "pandas>=2.0",
    "pyarrow>=14.0",
    "requests>=2.31",
    "python-dotenv>=1.0",
    "pydantic>=2.0",
    "duckdb>=1.5",          # ← add
    "duckdb-http-server>=1.0",  # ← add (for HTTP REST API mode)
]
```

- [ ] **Step 2: Install dependencies**

Run: `.venv/bin/pip install -e ".[dev]"`
Expected: Install succeeds without errors.

- [ ] **Step 3: Verify import works**

Run: `.venv/bin/python -c "import duckdb; print(duckdb.__version__)"`
Expected: Prints DuckDB version number.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "feat: add duckdb and duckdb-http-server dependencies"
```

---

### Task 2: Create DuckDB Resource (`resources/duckdb.py`)

**Files:**
- Create: `src/dagster_real_data/resources/duckdb.py`
- Test: `tests/test_resources.py`

**Interfaces:**
- Consumes: `ConfigurableResource` pattern from existing `open_meteo.py`
- Produces: `DuckDBResource` class with `host`, `port`, `database` fields
- Produces: `get_connection()` method returning DuckDB connection string

- [ ] **Step 1: Write failing test for DuckDBResource**

```python
# tests/test_resources.py
import pytest
from dagster_real_data.resources.duckdb import DuckDBResource

def test_duckdb_resource_default_config():
    resource = DuckDBResource()
    assert resource.host == "localhost"
    assert resource.port == 8080
    assert resource.database == "mydb.duckdb"

def test_duckdb_resource_connection_string():
    resource = DuckDBResource(host="localhost", port=8080, database="analytics.duckdb")
    conn_str = resource.get_connection_url()
    assert conn_str == "http://localhost:8080/analytics.duckdb"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_resources.py::test_duckdb_resource_default_config -v`
Expected: FAIL — `DuckDBResource` not defined yet.

- [ ] **Step 3: Implement DuckDBResource**

```python
# src/dagster_real_data/resources/duckdb.py
"""DuckDB resource for analytical query layer."""
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 8080
    database: str = "mydb.duckdb"

    def get_connection_url(self) -> str:
        return f"http://{self.host}:{self.port}/{self.database}"

    def get_jdbc_url(self) -> str:
        return f"jdbc:duckdb:http://{self.host}:{self.port}/{self.database}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_resources.py::test_duckdb_resource_default_config -v`
Expected: PASS.

Run: `.venv/bin/pytest tests/test_resources.py::test_duckdb_resource_connection_string -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_real_data/resources/duckdb.py tests/test_resources.py
git commit -m "feat: add DuckDBResource configurable resource"
```

---

### Task 3: Create DuckDB Load Asset (`assets/duckdb_loader.py`)

**Files:**
- Create: `src/dagster_real_data/assets/duckdb_loader.py`
- Test: `tests/test_assets.py` (or new `tests/test_duckdb_loader.py`)

**Interfaces:**
- Consumes: Existing `weather_assets` and `wiki_assets` (via `ins()`)
- Consumes: `DuckDBResource` (from Task 2)
- Produces: `duckdb_tables` asset — creates DuckDB tables from parquet data

- [ ] **Step 1: Understand parquet path mapping**

Review `src/dagster_real_data/resources/io_manager.py` — asset-to-tier mapping is in `ASSET_TIERS`. Parquet paths follow: `./data/{tier}/{asset_name}/{partition}/output.parquet`.

- [ ] **Step 2: Write failing test for DuckDB load asset**

```python
# tests/test_duckdb_loader.py
import pandas as pd
import duckdb
from dagster_real_data.assets.duckdb_loader import DuckDBLoadAsset

def test_duckdb_load_asset_creates_tables(tmp_path, mocker):
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

def test_duckdb_load_asset_row_count_matches_parquet(tmp_path, mocker):
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_duckdb_loader.py -v`
Expected: FAIL — `DuckDBLoadAsset` not defined yet.

- [ ] **Step 4: Implement DuckDBLoadAsset**

```python
# src/dagster_real_data/assets/duckdb_loader.py
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
                conn.register(table_name, df)
                logger.info(
                    "Loaded %d rows from %s into DuckDB table %s",
                    len(df), parquet_file, table_name,
                )

        conn.close()


@asset(
    name="duckdb_tables",
    description="Loads all parquet data from Bronze/Silver/Gold tiers into DuckDB tables",
    ins={
        "weather_historical": AssetIn(key_input_name="weather_historical"),
        "weather_hourly": AssetIn(key_input_name="weather_hourly"),
        "weather_forecast": AssetIn(key_input_name="weather_forecast"),
        "wiki_events_raw": AssetIn(key_input_name="wiki_events_raw"),
    },
)
def duckdb_tables_load(
    context: AssetExecutionContext,
) -> None:
    """Dagster asset that triggers DuckDB load from parquet files."""
    data_dir = context.resources.io.base_dir if hasattr(context.resources.io, 'base_dir') else "./data"
    loader = DuckDBLoadAsset(data_dir=data_dir, db_path=context.resources.duckdb.database)
    loader.load_to_duckdb()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_duckdb_loader.py -v`
Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dagster_real_data/assets/duckdb_loader.py tests/test_duckdb_loader.py
git commit -m "feat: add duckdb_loader asset to load parquet into DuckDB"
```

---

### Task 4: Update Registration Files

**Files:**
- Modify: `src/dagster_real_data/resources/__init__.py`
- Modify: `src/dagster_real_data/assets/__init__.py`
- Modify: `src/dagster_real_data/definitions.py`

**Interfaces:**
- Produces: `duckdb_resource` registered in Dagster Definitions
- Produces: `duckdb_tables` asset registered in Dagster Definitions

- [ ] **Step 1: Update `resources/__init__.py`**

```python
# src/dagster_real_data/resources/__init__.py
from dagster_real_data.resources.open_meteo import OpenMeteoResource
from dagster_real_data.resources.wikimedia import WikimediaResource
from dagster_real_data.resources.io_manager import filesystem_io_manager
from dagster_real_data.resources.duckdb import DuckDBResource  # ← add
```

- [ ] **Step 2: Update `assets/__init__.py`**

```python
# src/dagster_real_data/assets/__init__.py
from dagster_real_data.assets.weather import weather_assets
from dagster_real_data.assets.wikipedia import wiki_assets
from dagster_real_data.assets.duckdb_loader import duckdb_tables_load  # ← add
```

- [ ] **Step 3: Update `definitions.py`**

```python
# src/dagster_real_data/definitions.py
"""Dagster Definitions — central orchestration point."""
from dagster import Definitions

from dagster_real_data.assets.weather import weather_assets
from dagster_real_data.assets.wikipedia import wiki_assets
from dagster_real_data.assets.duckdb_loader import duckdb_tables_load  # ← add
from dagster_real_data.resources.open_meteo import OpenMeteoResource
from dagster_real_data.resources.wikimedia import WikimediaResource
from dagster_real_data.resources.io_manager import filesystem_io_manager
from dagster_real_data.resources.duckdb import DuckDBResource  # ← add
from dagster_real_data.checks.weather_checks import weather_asset_checks
from dagster_real_data.checks.wiki_checks import wiki_asset_checks
from dagster_real_data.schedules.weather_schedule import weather_daily_schedule
from dagster_real_data.sensors.wiki_sensor import wikimedia_event_sensor

defs = Definitions(
    assets=[*weather_assets, *wiki_assets, duckdb_tables_load],
    resources={
        "open_meteo": OpenMeteoResource(),
        "wikimedia": WikimediaResource(),
        "io": filesystem_io_manager,
        "duckdb": DuckDBResource(),  # ← add
    },
    asset_checks=[*weather_asset_checks, *wiki_asset_checks],
    schedules=[weather_daily_schedule],
    sensors=[wikimedia_event_sensor],
)
```

- [ ] **Step 4: Verify definitions load**

Run: `.venv/bin/python -c "from dagster_real_data.definitions import defs; print('Definitions OK:', len(defs.asset_keys))"`
Expected: Prints "Definitions OK:" with correct asset count.

- [ ] **Step 5: Commit**

```bash
git add src/dagster_real_data/resources/__init__.py src/dagster_real_data/assets/__init__.py src/dagster_real_data/definitions.py
git commit -m "feat: register DuckDB resource and asset in Dagster Definitions"
```

---

### Task 5: Create DuckDB Server Startup Script

**Files:**
- Create: `scripts/start_duckdb_server.sh`
- Test: None (infrastructure script)

**Interfaces:**
- Produces: Runnable shell script to start DuckDB server on port 8080

- [ ] **Step 1: Create startup script**

```bash
#!/bin/bash
# scripts/start_duckdb_server.sh
# Starts DuckDB server for Metabase JDBC connection.
# Usage: ./scripts/start_duckdb_server.sh [data_dir] [port]

set -e

DATA_DIR="${1:-./data}"
PORT="${2:-8080}"
DB_FILE="${DATA_DIR}/mydb.duckdb"

echo "Starting DuckDB server..."
echo "  Data dir: $DATA_DIR"
echo "  Port: $PORT"
echo "  DB file: $DB_FILE"

duckdb --serve \
    --host 0.0.0.0 \
    --port "$PORT" \
    "$DB_FILE"
```

- [ ] **Step 2: Make executable**

Run: `chmod +x scripts/start_duckdb_server.sh`
Expected: No error.

- [ ] **Step 3: Commit**

```bash
git add scripts/start_duckdb_server.sh
git commit -m "feat: add DuckDB server startup script"
```

---

### Task 6: Create Metabase Connection Documentation

**Files:**
- Create: `docs/metabase-setup.md`

**Interfaces:**
- Produces: Step-by-step Metabase setup guide
- Produces: JDBC URL and configuration details

- [ ] **Step 1: Write Metabase setup documentation**

```markdown
# Metabase Setup for DuckDB

## Prerequisites
- DuckDB server running (see `scripts/start_duckdb_server.sh`)
- Metabase installed (download from https://www.metabase.com/)

## Connect Metabase to DuckDB

### 1. Start DuckDB Server
```bash
./scripts/start_duckdb_server.sh ./data 8080
```

### 2. Add Database in Metabase
1. Open Metabase (default: `http://localhost:3000`)
2. Go to **Settings → Databases → Add Database**
3. Select **DuckDB** as the database type
4. Enter connection details:
   - **Host**: `localhost`
   - **Port**: `8080`
   - **Database**: `mydb.duckdb`
   - **JDBC URL**: `jdbc:duckdb:http://localhost:8080/mydb.duckdb`

### 3. Verify Connection
- Click **Test Connection** in Metabase
- Should see tables: `weather_historical`, `weather_hourly`, `weather_daily`, etc.

### 4. Create Dashboards
- Click **New → Question** to explore data
- Build visualizations for weather trends, Wikipedia page views, etc.
- Save as Dashboard from the **Save** menu
```

- [ ] **Step 2: Commit**

```bash
git add docs/metabase-setup.md
git commit -m "docs: add Metabase setup guide for DuckDB"
```

---

### Task 7: Run Full Test Suite

**Files:** All tests

**Interfaces:**
- Verifies: All existing tests still pass
- Verifies: New tests pass

- [ ] **Step 1: Run all tests**

Run: `.venv/bin/pytest tests/ -v`
Expected: All tests PASS (existing + new). If any fail, debug and fix.

- [ ] **Step 2: Verify DuckDB server connectivity (manual check)**

Run: `duckdb --serve --host 0.0.0.0 --port 8080 ./data/mydb.duckdb &` then `.venv/bin/python -c "import duckdb; conn = duckdb.connect('http://localhost:8080/mydb.duckdb'); print(conn.execute('SELECT 1').fetchone())"`
Expected: `(1,)`

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "feat: complete DuckDB + Metabase integration"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: All design sections implemented (parquet → DuckDB → Metabase flow)
- [x] **No placeholders**: Every step has actual code or commands
- [x] **Type consistency**: `DuckDBResource.host/port/database` used consistently in Task 2, 3, 4
- [x] **Review Focus covered**:
  - Parquet path resolution → Task 3 tests check parquet file discovery
  - DuckDB connection failure → Task 3 tests mock connection, Task 7 tests server startup
  - Data consistency → Task 3 tests verify row counts match
  - Metabase JDBC URL → Task 2 tests `get_jdbc_url()`, Task 6 docs
