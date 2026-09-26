# Replace DuckDB with PostgreSQL — Design Specification

**Date**: 2026-09-26
**Status**: Approved

---

## 1. Goal

Replace DuckDB as the analytical query layer in the Dagster Real Data Pipeline with PostgreSQL. All data currently loaded into DuckDB tables should instead be loaded into PostgreSQL tables. Metabase connects to PostgreSQL natively via its JDBC driver.

**Why**: PostgreSQL is a mature, production-grade RDBMS with broader tooling support and a native Metabase connector, making it a more sustainable choice for the analytical query layer.

**What stays unchanged**: The parquet-first architecture (Bronze/Silver/Gold tiers on filesystem), the Dagster asset graph, the Open-Meteo and Wikimedia API integrations, the IOManager, asset checks, schedules, and sensors.

---

## 2. Architecture

```
┌───────────────────────┐
│        Dagster        │
└───────────┬───────────┘
            │
   ┌────────┴───────────┐     ┌────────────────────┐
   │    Open-Meteo      │     │     Wikimedia      │
   │    REST APIs       │     │   EventStreams     │
   └─────────┬─────────┘     └──────────┬─────────┘
             │                           │
             ▼                           ▼
    Parquet files (filesystem)          │
    (Bronze/Silver/Gold tiers)          │
             │                           │
             ▼                           │
    ┌──────────────────┐                │
    │  PostgreSQL      │◄───────────────┘
    │  (query layer)   │
    └────────┬─────────┘
             │
             ▼
        Metabase
```

**Data flow**: APIs → parquet (filesystem) → **PostgreSQL** → Metabase

PostgreSQL replaces DuckDB entirely as the analytical query layer. Parquet remains the source of truth.

---

## 3. Technical Decisions

### 3.1 Connection Library: `psycopg2-binary`

- Direct PostgreSQL connectivity via `psycopg2` (synchronous, lightweight)
- No ORM needed — the pipeline uses pandas DataFrames and SQL directly
- `psycopg2-binary` for development; `psycopg2` for production

### 3.2 Data Loading: `pandas.DataFrame.to_sql()`

- Replaces `duckdb.connect()` + `CREATE TABLE AS SELECT * FROM df`
- `df.to_sql(table_name, con, if_exists="replace", index=False)` handles table creation and bulk insert automatically
- Uses `execute_values` under the hood for efficient batch inserts

### 3.3 PostgresResource

Replaces `DuckDBResource` with a new `PostgresResource`:

```python
class PostgresResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 5432
    database: str = "mydb"
    user: str = "postgres"
    password: str = "postgres"

    def get_connection_url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
```

- Drop `get_jdbc_url()` — Metabase connects natively to PostgreSQL
- Connection string uses standard `postgresql://` protocol

### 3.4 Docker Compose: PostgreSQL Service

Replaces the `duckdb` service with a `postgres` service:
- Image: `postgres:16-alpine`
- Port: `5432:5432`
- Volume: persistent data storage
- Environment: `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- Health check: `pg_isready`

### 3.5 Metabase Connection

Metabase connects directly to PostgreSQL via native JDBC:
- JDBC URL: `jdbc:postgresql://localhost:5432/mydb`
- Driver: PostgreSQL (built into Metabase)
- No middleware server needed (unlike DuckDB's HTTP server)

### 3.6 Removed Infrastructure

The following are removed entirely:
- `duckdb_server.py` — no longer needed (Metabase talks directly to PG)
- `Dockerfile.duck` — replaced by Dockerfile for Postgres
- `scripts/start_duckdb_server.sh` — no longer needed
- `duckdb-http-server` dependency — no longer needed

---

## 4. File Changes

### 4.1 Source Code

| File | Action | Details |
|------|--------|---------|
| `src/dagster_pipeline/resources/duckdb.py` | **Rename** → `postgres.py` | `DuckDBResource` → `PostgresResource`; add `user`, `password` fields; replace `get_jdbc_url()` with `get_connection_url()` using `postgresql://` |
| `src/dagster_pipeline/assets/duckdb_loader.py` | **Rewrite** → `postgres_loader.py` | Replace `import duckdb` with `import psycopg2`; replace `conn.execute("CREATE TABLE AS SELECT")` with `df.to_sql()`; rename `DuckDBLoadAsset` → `PostgresLoadAsset`; rename `duckdb_tables_load` → `postgres_tables_load` |
| `src/dagster_pipeline/resources/__init__.py` | **Update** | Swap `DuckDBResource` import for `PostgresResource` |
| `src/dagster_pipeline/assets/__init__.py` | **Update** | Swap `duckdb_loader` import for `postgres_loader` |
| `src/dagster_pipeline/definitions.py` | **Update** | Replace `DuckDBResource()` with `PostgresResource()`, swap imports |
| `pyproject.toml` | **Update** | Replace `duckdb>=1.5` with `psycopg2-binary>=2.9` |

### 4.2 Docker & Infrastructure

| File | Action | Details |
|------|--------|---------|
| `docker-compose.yml` | **Rewrite** | Replace `duckdb` service with `postgres` service; add `pg_isready` health check; mount persistent volume |
| `Dockerfile.duck` | **Remove** | Replaced by `Dockerfile` (or Postgres uses official image directly) |
| `duckdb_server.py` | **Remove** | No longer needed |
| `scripts/start_duckdb_server.sh` | **Remove** | No longer needed |

### 4.3 Tests

| File | Action | Details |
|------|--------|---------|
| `tests/test_duckdb_loader.py` | **Rewrite** → `tests/test_postgres_loader.py` | Replace `duckdb` import with `psycopg2`; test Postgres table creation and row counts |
| `tests/test_resources.py` | **Update** | Replace `DuckDBResource` tests with `PostgresResource` tests |

### 4.4 Documentation

| File | Action | Details |
|------|--------|---------|
| `docs/metabase-setup.md` | **Rewrite** | Update all DuckDB references to PostgreSQL; update JDBC URL format |
| `docs/api.md` | **Update** | Replace `DuckDBResource` docs with `PostgresResource` docs |
| `docs/superpowers/plans/2026-09-23-duckdb-metabase.md` | **Update** | Replace DuckDB references with PostgreSQL |

### 4.5 New Files

| File | Details |
|------|---------|
| `Dockerfile` | PostgreSQL server setup (or Postgres uses official image in docker-compose directly) |

---

## 5. PostgresResource Specification

```python
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

The `get_connection()` method is added to support the loader asset, which needs a live connection to execute `to_sql()`.

---

## 6. PostgresLoadAsset Specification

The loader asset reads parquet files from the filesystem and loads them into PostgreSQL tables using `pandas.DataFrame.to_sql()`:

```python
class PostgresLoadAsset:
    def __init__(self, data_dir: str, postgres_resource: PostgresResource):
        self.data_dir = Path(data_dir)
        self.postgres = postgres_resource

    def load_to_postgres(self) -> None:
        conn = self.postgres.get_connection()
        try:
            for asset_name, tier in ASSET_TIERS.items():
                parquet_path = self.data_dir / tier / asset_name
                for partition_dir in parquet_path.iterdir():
                    if not partition_dir.is_dir():
                        continue
                    parquet_file = partition_dir / "output.parquet"
                    if not parquet_file.exists():
                        continue
                    table_name = ASSET_TO_TABLE.get(asset_name, asset_name)
                    df = pd.read_parquet(parquet_file)
                    df.to_sql(table_name, conn, if_exists="replace", index=False)
        finally:
            conn.close()
```

Key differences from DuckDB:
- `to_sql()` handles table creation automatically (no `CREATE TABLE AS SELECT`)
- `if_exists="replace"` drops and recreates tables on each load
- `psycopg2` connection instead of `duckdb.connect()`

---

## 7. docker-compose.yml Specification

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

No custom Dockerfile needed — Postgres uses the official image directly.

---

## 8. Metabase Setup (Updated)

Metabase connects to PostgreSQL natively:

1. Add Database → Select **PostgreSQL**
2. Connection details:
   - **Host**: `localhost`
   - **Port**: `5432`
   - **Database**: `mydb`
   - **Username**: `postgres`
   - **Password**: `postgres`
3. JDBC URL: `jdbc:postgresql://localhost:5432/mydb`
4. Verify tables: `weather_historical`, `weather_hourly`, `weather_forecast`, `wiki_events_raw`, etc.

---

## 9. Error Handling

- **Connection failures**: `psycopg2.OperationalError` raised with descriptive message if Postgres is unreachable
- **Table creation**: `to_sql()` with `if_exists="replace"` handles existing tables gracefully
- **Parquet file not found**: Same behavior as before — log warning and skip
- **Health check**: `pg_isready` ensures Metabase only starts after Postgres is ready

---

## 10. Testing Strategy

### 10.1 Unit Tests

- `test_postgres_resource_creation`: Verify default host, port, database, user, password
- `test_postgres_resource_connection_url`: Verify `postgresql://` connection string format
- `test_postgres_resource_get_connection`: Verify `psycopg2.connect()` is called with correct params (mocked)

### 10.2 Loader Tests

- `test_postgres_load_asset_creates_tables`: Create dummy parquet files, load into Postgres, verify tables exist
- `test_postgres_load_asset_row_count_matches_parquet`: Verify row counts match between parquet and Postgres tables
- Use `pytest` with `responses`/`pytest-mock` for mocking (same as before)
- Tests connect to a real or mock Postgres instance

### 10.3 Integration

- All existing tests for weather assets, wiki assets, checks remain unchanged (they don't touch the DB layer)
- Resource tests updated to test `PostgresResource` instead of `DuckDBResource`

---

## 11. Migration Path

1. Create `postgres.py` resource (new file, not in-place rename)
2. Create `postgres_loader.py` (new file)
3. Update `__init__.py` files and `definitions.py`
4. Update `pyproject.toml` dependencies
5. Rewrite `docker-compose.yml`, remove `Dockerfile.duck`, `duckdb_server.py`
6. Rewrite tests
7. Update docs
8. Remove old `duckdb.py`, `duckdb_loader.py` files

This is a full replacement, not an incremental migration. The old DuckDB files are deleted.

---

## 12. Verification Checklist

- [ ] `PostgresResource` has correct defaults (`host="localhost"`, `port=5432`, `database="mydb"`, `user="postgres"`, `password="postgres"`)
- [ ] `get_connection_url()` returns `postgresql://...` format
- [ ] `PostgresLoadAsset` loads parquet data into Postgres tables correctly
- [ ] `to_sql()` creates tables and inserts data without errors
- [ ] `docker-compose up` starts Postgres and Metabase successfully
- [ ] `pg_isready` health check passes
- [ ] Metabase can connect to Postgres and see all tables
- [ ] All existing tests pass (weather, wiki, checks)
- [ ] New Postgres-specific tests pass
- [ ] `duckdb` dependency removed from `pyproject.toml`
- [ ] No references to `duckdb` remain in source code
- [ ] Docs updated with PostgreSQL connection details
- [ ] Old DuckDB files (`duckdb_server.py`, `Dockerfile.duck`, `start_duckdb_server.sh`) removed
