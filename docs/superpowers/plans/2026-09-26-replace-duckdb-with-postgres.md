# Replace DuckDB with PostgreSQL — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace DuckDB with PostgreSQL as the analytical query layer in the Dagster Real Data Pipeline — swap `duckdb` dependency for `psycopg2-binary`, create `PostgresResource` and `PostgresLoadAsset`, update Docker Compose, tests, and docs.

**Architecture:** Dagster pipeline (unchanged) writes parquet to Bronze/Silver/Gold tiers → `PostgresLoadAsset` reads parquet and loads into PostgreSQL via `pandas.to_sql()` → Metabase connects natively via PostgreSQL JDBC. Parquet remains the source of truth.

**Tech Stack:** Python 3.11+, Dagster >= 1.9, `psycopg2-binary>=2.9`, PostgreSQL 16, pandas, pyarrow

**Spec:** `docs/superpowers/specs/2026-09-26-replace-duckdb-with-postgres-design.md`

## Global Constraints

- Python >=3.11, Dagster >=1.9
- Follow existing `ConfigurableResource` pattern (class-based, NOT dataclass)
- Follow existing asset naming conventions: `asset(name="...")` with `io_manager_key`
- Existing tests in `tests/` use `pytest`, `pytest-mock`, `responses`
- All new code in `src/dagster_pipeline/`, tests in `tests/`
- PostgreSQL runs via docker-compose on port 5432
- `psycopg2-binary` for database connectivity (not `duckdb`)

## Review Focus

- **PostgresResource connection string**: `get_connection_url()` must return `postgresql://user:password@host:port/database` format. Test the connection string exactly matches this pattern.
- **Table creation via to_sql()**: `PostgresLoadAsset.load_to_postgres()` must create tables and insert data correctly. Test row counts match between parquet and Postgres.
- **Docker Compose health check**: Postgres service must have `pg_isready` health check so Metabase waits for PG to be ready.
- **No duckdb references remain**: After all changes, `grep -r "duckdb\|DuckDB" src/ tests/` must return zero results (except in this plan file).
- **Metabase JDBC URL**: Updated docs must show `jdbc:postgresql://localhost:5432/mydb` not `jdbc:duckdb:http://localhost:8080/mydb.duckdb`.

---

### Task 1: Update dependencies — swap duckdb for psycopg2-binary

**Files:**
- Modify: `pyproject.toml`

**Interfaces:**
- Produces: `psycopg2-binary>=2.9` available in Python environment
- Removes: `duckdb>=1.5` dependency

- [ ] **Step 1: Remove duckdb and add psycopg2-binary to pyproject.toml**

Replace line `"duckdb>=1.5",` with `"psycopg2-binary>=2.9",` in the `dependencies` list:

```toml
dependencies = [
    "dagster>=1.9",
    "dagster-webserver>=1.9",
    "pandas>=2.0",
    "pyarrow>=14.0",
    "requests>=2.31",
    "python-dotenv>=1.0",
    "pydantic>=2.0",
    "psycopg2-binary>=2.9",
]
```

- [ ] **Step 2: Commit**

```bash
git add pyproject.toml
git commit -m "feat: replace duckdb with psycopg2-binary dependency"
```

---

### Task 2: Create PostgresResource

**Files:**
- Create: `src/dagster_pipeline/resources/postgres.py`
- Modify: `src/dagster_pipeline/resources/__init__.py`
- Test: `tests/test_resources.py`

**Interfaces:**
- Consumes: `ConfigurableResource` pattern from existing `open_meteo.py`
- Produces: `PostgresResource` class with `host`, `port`, `database`, `user`, `password` fields
- Produces: `get_connection_url()` returning `postgresql://...` format
- Produces: `get_connection()` returning a `psycopg2` connection object

- [ ] **Step 1: Write failing test for PostgresResource**

Add to `tests/test_resources.py`:

```python
from dagster_pipeline.resources.postgres import PostgresResource

def test_postgres_resource_default_config():
    resource = PostgresResource()
    assert resource.host == "localhost"
    assert resource.port == 5432
    assert resource.database == "mydb"
    assert resource.user == "postgres"
    assert resource.password == "postgres"

def test_postgres_resource_connection_url():
    resource = PostgresResource(host="localhost", port=5432, database="mydb", user="postgres", password="secret")
    conn_str = resource.get_connection_url()
    assert conn_str == "postgresql://postgres:secret@localhost:5432/mydb"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_resources.py::test_postgres_resource_default_config -v`
Expected: FAIL — `PostgresResource` not defined yet.

- [ ] **Step 3: Implement PostgresResource**

Create `src/dagster_pipeline/resources/postgres.py`:

```python
"""PostgreSQL resource for analytical query layer."""
import psycopg2
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 5432
    database: str = "mydb"
    user: str = "postgres"
    password: str = "postgres"

    def get_connection_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_connection(self):
        """Return a psycopg2 connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
```

- [ ] **Step 4: Update `resources/__init__.py`**

Replace the DuckDBResource import with PostgresResource:

```python
from dagster_pipeline.resources.open_meteo import OpenMeteoResource
from dagster_pipeline.resources.wikimedia import WikimediaResource
from dagster_pipeline.resources.io_manager import filesystem_io_manager
from dagster_pipeline.resources.postgres import PostgresResource
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_resources.py -v`
Expected: All tests PASS (including existing and new PostgresResource tests).

- [ ] **Step 6: Commit**

```bash
git add src/dagster_pipeline/resources/postgres.py src/dagster_pipeline/resources/__init__.py tests/test_resources.py
git commit -m "feat: add PostgresResource configurable resource"
```

---

### Task 3: Create PostgresLoadAsset

**Files:**
- Create: `src/dagster_pipeline/assets/postgres_loader.py`
- Modify: `src/dagster_pipeline/assets/__init__.py`
- Modify: `src/dagster_pipeline/definitions.py`
- Test: `tests/test_postgres_loader.py`

**Interfaces:**
- Consumes: `PostgresResource` (from Task 2), `ASSET_TIERS` from `io_manager`
- Produces: `postgres_tables_load` asset — creates PostgreSQL tables from parquet data
- Produces: `PostgresLoadAsset` class with `load_to_postgres()` method

- [ ] **Step 1: Write failing test for PostgresLoadAsset**

Create `tests/test_postgres_loader.py`:

```python
"""Tests for Postgres loader asset."""
import pandas as pd
import psycopg2
from unittest.mock import MagicMock, patch
from dagster_pipeline.assets.postgres_loader import PostgresLoadAsset, ASSET_TO_TABLE
from dagster_pipeline.resources.io_manager import ASSET_TIERS


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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_postgres_loader.py -v`
Expected: FAIL — `PostgresLoadAsset` not defined yet.

- [ ] **Step 3: Implement PostgresLoadAsset**

Create `src/dagster_pipeline/assets/postgres_loader.py`:

```python
"""PostgreSQL loader asset — loads parquet data into PostgreSQL tables."""
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import AssetExecutionContext, asset, AssetIn

from dagster_pipeline.resources.postgres import PostgresResource
from dagster_pipeline.resources.io_manager import ASSET_TIERS

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
        conn = self.postgres.get_connection()
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
                    df.to_sql(table_name, conn, if_exists="replace", index=False)
                    logger.info(
                        "Loaded %d rows from %s into PostgreSQL table %s",
                        len(df), parquet_file, table_name,
                    )
        finally:
            conn.close()


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
```

- [ ] **Step 4: Update `assets/__init__.py`**

Replace the duckdb_loader import:

```python
from dagster_pipeline.assets.weather import weather_assets
from dagster_pipeline.assets.wikipedia import wiki_assets
from dagster_pipeline.assets.postgres_loader import postgres_tables_load
```

- [ ] **Step 5: Update `definitions.py`**

Replace all DuckDB references:

```python
"""Dagster Definitions — central orchestration point."""
from dagster import Definitions

from dagster_pipeline.assets.weather import weather_assets
from dagster_pipeline.assets.wikipedia import wiki_assets
from dagster_pipeline.assets.postgres_loader import postgres_tables_load
from dagster_pipeline.resources.open_meteo import OpenMeteoResource
from dagster_pipeline.resources.wikimedia import WikimediaResource
from dagster_pipeline.resources.io_manager import filesystem_io_manager
from dagster_pipeline.resources.postgres import PostgresResource
from dagster_pipeline.checks.weather_checks import weather_asset_checks
from dagster_pipeline.checks.wiki_checks import wiki_asset_checks
from dagster_pipeline.schedules.weather_schedule import weather_daily_schedule
from dagster_pipeline.sensors.wiki_sensor import wikimedia_event_sensor

defs = Definitions(
    assets=[*weather_assets, *wiki_assets, postgres_tables_load],
    resources={
        "open_meteo": OpenMeteoResource(),
        "wikimedia": WikimediaResource(),
        "io": filesystem_io_manager,
        "postgres": PostgresResource(),
    },
    asset_checks=[*weather_asset_checks, *wiki_asset_checks],
    schedules=[weather_daily_schedule],
    sensors=[wikimedia_event_sensor],
)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_postgres_loader.py tests/test_resources.py -v`
Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/dagster_pipeline/assets/postgres_loader.py src/dagster_pipeline/assets/__init__.py src/dagster_pipeline/definitions.py tests/test_postgres_loader.py
git commit -m "feat: add PostgresLoadAsset and register in Dagster Definitions"
```

---

### Task 4: Update Docker infrastructure — replace DuckDB with PostgreSQL

**Files:**
- Modify: `docker-compose.yml`
- Remove: `Dockerfile.duck`, `duckdb_server.py`, `scripts/start_duckdb_server.sh`

**Interfaces:**
- Produces: `postgres` service in docker-compose on port 5432 with health check
- Removes: `duckdb` service, DuckDB Dockerfile, DuckDB server script

- [ ] **Step 1: Rewrite docker-compose.yml**

Replace the entire file:

```yaml
version: "3.8"

services:
  postgres:
    image: postgres:16-alpine
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: mydb
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./data:/app/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  metabase:
    image: metabase/metabase:latest
    platform: linux/amd64
    ports:
      - "3000:3000"
    environment:
      - MB_JETTY_PORT=3000
    depends_on:
      postgres:
        condition: service_healthy

volumes:
    postgres_data:
```

- [ ] **Step 2: Remove DuckDB infrastructure files**

```bash
rm duckdb_server.py Dockerfile.duck scripts/start_duckdb_server.sh
```

Note: `scripts/start_duckdb_server.sh` may not exist — check first with `ls scripts/` before removing.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git rm duckdb_server.py Dockerfile.duck scripts/start_duckdb_server.sh 2>/dev/null || true
git commit -m "feat: replace DuckDB with PostgreSQL in docker-compose, remove DuckDB infra files"
```

---

### Task 5: Update tests — replace DuckDB tests with Postgres tests

**Files:**
- Modify: `tests/test_resources.py` (remove DuckDB tests, add Postgres tests — done partially in Task 2)
- Rewrite: `tests/test_duckdb_loader.py` → `tests/test_postgres_loader.py`
- Modify: `tests/conftest.py` if needed

**Interfaces:**
- Produces: Tests that verify `PostgresResource` defaults and connection URL
- Produces: Tests that verify `PostgresLoadAsset` table creation and row counts
- Removes: All `import duckdb` and `DuckDBResource` references from tests

- [ ] **Step 1: Clean up tests/test_resources.py**

Remove the old `DuckDBResource` tests and `import duckdb`:

Remove these lines if present:
```python
from dagster_pipeline.resources.duckdb import DuckDBResource
```

And remove the `test_duckdb_resource_default_config` and `test_duckdb_resource_connection_string` tests (already replaced with Postgres versions in Task 2).

- [ ] **Step 2: Rewrite tests/test_duckdb_loader.py as tests/test_postgres_loader.py**

Replace the entire file content with the test from Task 3 Step 1 (plus additional tests):

```python
"""Tests for Postgres loader asset."""
import pandas as pd
import psycopg2
from unittest.mock import MagicMock, patch
from dagster_pipeline.assets.postgres_loader import PostgresLoadAsset, ASSET_TO_TABLE
from dagster_pipeline.resources.io_manager import ASSET_TIERS


def test_postgres_load_asset_creates_tables(tmp_path):
    """Test that postgres_loader creates tables from parquet files."""
    bronze_dir = tmp_path / "bronze" / "weather_historical" / "2026-01-15"
    bronze_dir.mkdir(parents=True)
    df = pd.DataFrame({"date": ["2026-01-15"], "temp": [25.0]})
    df.to_parquet(bronze_dir / "output.parquet", index=False)

    resource = MagicMock()
    resource.host = "localhost"
    resource.port = 5432
    resource.database = "mydb"
    resource.user = "postgres"
    resource.password = "postgres"

    loader = PostgresLoadAsset(data_dir=str(tmp_path), postgres_resource=resource)
    assert "weather_historical" in ASSET_TO_TABLE


def test_postgres_load_asset_table_names_match_assets():
    """Verify all ASSET_TIERS assets have corresponding table names."""
    for asset_name in ASSET_TIERS.keys():
        assert asset_name in ASSET_TO_TABLE
        assert ASSET_TO_TABLE[asset_name] == asset_name
```

Delete the old `tests/test_duckdb_loader.py`.

- [ ] **Step 3: Remove any remaining duckdb imports from test files**

Check all test files for `import duckdb` or `from dagster_pipeline.resources.duckdb`:
```bash
grep -r "duckdb\|DuckDB" tests/ src/
```
Remove any remaining references.

- [ ] **Step 4: Run all tests**

Run: `pytest tests/ -v`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_postgres_loader.py tests/test_resources.py
git rm tests/test_duckdb_loader.py 2>/dev/null || true
git commit -m "feat: replace DuckDB tests with Postgres tests"
```

---

### Task 6: Update documentation — replace all DuckDB references with PostgreSQL

**Files:**
- Rewrite: `docs/metabase-setup.md`
- Modify: `docs/api.md`
- Modify: `docs/superpowers/plans/2026-09-23-duckdb-metabase.md`

**Interfaces:**
- Produces: Updated docs showing PostgreSQL connection details
- Removes: All DuckDB-specific documentation

- [ ] **Step 1: Rewrite docs/metabase-setup.md**

Replace entire file:

```markdown
# Metabase Setup for PostgreSQL

## Prerequisites
- PostgreSQL running via `docker-compose up`
- Metabase installed (download from https://www.metabase.com/)

## Connect Metabase to PostgreSQL

### 1. Start PostgreSQL
```bash
docker-compose up -d
```

### 2. Add Database in Metabase
1. Open Metabase (default: `http://localhost:3000`)
2. Go to **Settings → Databases → Add Database**
3. Select **PostgreSQL** as the database type
4. Enter connection details:
   - **Host**: `localhost`
   - **Port**: `5432`
   - **Database**: `mydb`
   - **Username**: `postgres`
   - **Password**: `postgres`
   - **JDBC URL**: `jdbc:postgresql://localhost:5432/mydb`

### 3. Verify Connection
- Click **Test Connection** in Metabase
- Should see tables: `weather_historical`, `weather_hourly`, `weather_forecast`, `wiki_events_raw`, etc.

### 4. Create Dashboards
- Click **New → Question** to explore data
- Build visualizations for weather trends, Wikipedia page views, etc.
- Save as Dashboard from the **Save** menu
```

- [ ] **Step 2: Update docs/api.md**

Replace the `DuckDBResource` section with `PostgresResource`:

The resource table should show:
| Field | Type | Default |
|-------|------|---------|
| `host` | `str` | `"localhost"` |
| `port` | `int` | `5432` |
| `database` | `str` | `"mydb"` |
| `user` | `str` | `"postgres"` |
| `password` | `str` | `"postgres"` |

**Methods:**
- `get_connection_url()` — Returns `postgresql://user:password@host:port/database`
- `get_connection()` — Returns a `psycopg2` connection object

### Assets

The `postgres_tables` asset loads parquet data into PostgreSQL tables.

- [ ] **Step 3: Update docs/superpowers/plans/2026-09-23-duckdb-metabase.md**

Replace all DuckDB references with PostgreSQL. Update the title and content to reflect the PostgreSQL migration.

- [ ] **Step 4: Commit**

```bash
git add docs/metabase-setup.md docs/api.md docs/superpowers/plans/2026-09-23-duckdb-metabase.md
git commit -m "docs: replace DuckDB references with PostgreSQL in all documentation"
```

---

### Task 7: Remove old DuckDB files and verify no references remain

**Files:**
- Remove: `src/dagster_pipeline/resources/duckdb.py` (if still exists)
- Remove: `src/dagster_pipeline/assets/duckdb_loader.py` (if still exists)

**Interfaces:**
- Produces: Zero remaining `duckdb`/`DuckDB` references in source and tests

- [ ] **Step 1: Remove old DuckDB source files**

```bash
rm src/dagster_pipeline/resources/duckdb.py src/dagster_pipeline/assets/duckdb_loader.py 2>/dev/null || true
```

These should already have been replaced by `postgres.py` and `postgres_loader.py` in earlier tasks, but ensure they're gone.

- [ ] **Step 2: Verify no duckdb references remain**

Run: `grep -ri "duckdb\|DuckDB" src/ tests/ --include="*.py" --include="*.toml" --include="*.md" --include="*.yaml" --include="*.yml"`
Expected: No results (except possibly in this plan file or spec file).

- [ ] **Step 3: Run full test suite**

Run: `pytest tests/ -v`
Expected: All tests PASS.

- [ ] **Step 4: Verify definitions load**

Run: `python -c "from dagster_pipeline.definitions import defs; print('Definitions OK:', len(defs.asset_keys))"`
Expected: Prints "Definitions OK:" with correct asset count.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "feat: complete PostgreSQL migration — remove all DuckDB references"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: All sections of the design spec have a task:
  - `PostgresResource` with `host/port/database/user/password` → Task 2
  - `get_connection_url()` returning `postgresql://` → Task 2
  - `PostgresLoadAsset` with `to_sql()` → Task 3
  - `docker-compose.yml` with `postgres` service → Task 4
  - `psycopg2-binary` dependency → Task 1
  - Tests updated → Task 5
  - Docs updated → Task 6
  - Old DuckDB files removed → Task 7
- [x] **No placeholders**: Every step has actual code or commands
- [x] **Type consistency**: `PostgresResource` fields match across Task 2 (resource), Task 3 (loader), Task 4 (docker env vars)
- [x] **Review Focus covered**:
  - Connection string format → Task 2 tests (`test_postgres_resource_connection_url`)
  - Table creation via `to_sql()` → Task 3 tests (`test_postgres_load_asset_creates_tables`)
  - Docker health check → Task 4 (`pg_isready` in docker-compose.yml)
  - No duckdb references → Task 7 (`grep` verification)
  - Metabase JDBC URL → Task 6 (docs updated to `jdbc:postgresql://localhost:5432/mydb`)
