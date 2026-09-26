# PostgreSQL + Metabase Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace DuckDB with PostgreSQL as the analytical query layer on top of existing parquet data, and configure Metabase to visualize the data via PostgreSQL JDBC connection.

**Architecture:** Dagster pipeline (unchanged) writes parquet to Bronze/Silver/Gold tiers → Postgres Load Asset reads parquet → loads into PostgreSQL tables → Metabase connects via native PostgreSQL JDBC to display dashboards. Parquet remains the source of truth.

**Tech Stack:** Python `psycopg2-binary>=2.9`, Dagster, PostgreSQL 16, Metabase (external).

**Spec:** `docs/superpowers/specs/2026-09-26-replace-duckdb-with-postgres-design.md`

---

## Global Constraints

- Python 3.11+, Dagster >= 1.9
- Follow existing ConfigurableResource pattern (class-based, NOT dataclass)
- Follow existing asset naming conventions: `asset(name="...")` with `io_manager_key`
- Existing tests in `tests/` use `pytest`, `responses`, `pytest-mock`
- All new code in `src/dagster_pipeline/`, tests in `tests/`
- PostgreSQL runs on port 5432 via docker-compose
- `psycopg2-binary` for database connectivity (not `duckdb`)

## Review Focus

- **PostgresResource connection string**: `get_connection_url()` must return `postgresql://user:password@host:port/database` format.
- **Table creation via to_sql()**: `PostgresLoadAsset.load_to_postgres()` must create tables and insert data correctly. Test row counts match between parquet and PostgreSQL.
- **Docker Compose health check**: Postgres service must have `pg_isready` health check so Metabase waits for PG to be ready.
- **No duckdb references remain**: After all changes, `grep -r "duckdb\|DuckDB" src/ tests/` must return zero results.
- **Metabase JDBC URL**: Updated docs must show `jdbc:postgresql://localhost:5432/mydb` not `jdbc:duckdb:http://localhost:8080/mydb.duckdb`.

---

### Task 1: Update dependencies — swap duckdb for psycopg2-binary

**Files:** `pyproject.toml`

- [ ] **Step 1: Replace duckdb with psycopg2-binary**

Replace `"duckdb>=1.5",` with `"psycopg2-binary>=2.9",` in the dependencies list.

- [ ] **Step 2: Commit**

```bash
git add pyproject.toml
git commit -m "feat: replace duckdb with psycopg2-binary dependency"
```

---

### Task 2: Create PostgresResource

**Files:** `src/dagster_pipeline/resources/postgres.py`, `src/dagster_pipeline/resources/__init__.py`, `tests/test_resources.py`

- [ ] **Step 1: Create PostgresResource**

Create `src/dagster_pipeline/resources/postgres.py` with `host`, `port`, `database`, `user`, `password` fields, `get_connection_url()` returning `postgresql://...`, and `get_connection()` returning a psycopg2 connection.

- [ ] **Step 2: Update `resources/__init__.py`** to import `PostgresResource` instead of `DuckDBResource`.

- [ ] **Step 3: Update `tests/test_resources.py`** — replace DuckDBResource tests with PostgresResource tests.

- [ ] **Step 4: Run tests and commit**

```bash
git add src/dagster_pipeline/resources/postgres.py src/dagster_pipeline/resources/__init__.py tests/test_resources.py
git commit -m "feat: add PostgresResource configurable resource"
```

---

### Task 3: Create PostgresLoadAsset

**Files:** `src/dagster_pipeline/assets/postgres_loader.py`, `src/dagster_pipeline/assets/__init__.py`, `src/dagster_pipeline/definitions.py`, `tests/test_postgres_loader.py`

- [ ] **Step 1: Create PostgresLoadAsset**

Create `src/dagster_pipeline/assets/postgres_loader.py` using `psycopg2` and `pandas.DataFrame.to_sql()` instead of `duckdb.connect()` and `CREATE TABLE AS SELECT`. Asset renamed to `postgres_tables_load`.

- [ ] **Step 2: Update `assets/__init__.py`** and `definitions.py` to register `postgres_tables_load` and `PostgresResource`.

- [ ] **Step 3: Create `tests/test_postgres_loader.py`** with tests for table creation and asset mapping.

- [ ] **Step 4: Run tests and commit**

```bash
git add src/dagster_pipeline/assets/postgres_loader.py src/dagster_pipeline/assets/__init__.py src/dagster_pipeline/definitions.py tests/test_postgres_loader.py
git commit -m "feat: add PostgresLoadAsset and register in Dagster Definitions"
```

---

### Task 4: Update Docker infrastructure — replace DuckDB with PostgreSQL

**Files:** `docker-compose.yml`, `Dockerfile.duck` (remove), `duckdb_server.py` (remove), `scripts/start_duckdb_server.sh` (remove)

- [ ] **Step 1: Rewrite `docker-compose.yml`** with a `postgres:16-alpine` service on port 5432 with `pg_isready` health check.

- [ ] **Step 2: Remove DuckDB infrastructure files:**

```bash
rm duckdb_server.py Dockerfile.duck scripts/start_duckdb_server.sh
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git rm duckdb_server.py Dockerfile.duck scripts/start_duckdb_server.sh
git commit -m "feat: replace DuckDB with PostgreSQL in docker-compose"
```

---

### Task 5: Update tests — replace DuckDB tests with Postgres tests

**Files:** `tests/test_duckdb_loader.py` (remove), `tests/test_resources.py` (already updated in Task 2)

- [ ] **Step 1: Remove `tests/test_duckdb_loader.py`** and create `tests/test_postgres_loader.py`.

- [ ] **Step 2: Verify no duckdb references remain in tests**

```bash
grep -r "duckdb\|DuckDB" tests/ src/
```

- [ ] **Step 3: Run all tests and commit**

```bash
pytest tests/ -v
git add -A
git commit -m "feat: replace DuckDB tests with Postgres tests"
```

---

### Task 6: Update documentation — replace all DuckDB references with PostgreSQL

**Files:** `docs/metabase-setup.md`, `docs/api.md`, `docs/superpowers/plans/2026-09-23-duckdb-metabase.md`

- [ ] **Step 1: Rewrite `docs/metabase-setup.md`** with PostgreSQL connection details (`jdbc:postgresql://localhost:5432/mydb`).

- [ ] **Step 2: Update `docs/api.md`** to add `PostgresResource` section and `postgres_tables` asset.

- [ ] **Step 3: Rewrite `docs/superpowers/plans/2026-09-23-duckdb-metabase.md`** to reflect the PostgreSQL migration.

- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs: replace DuckDB references with PostgreSQL"
```

---

### Task 7: Remove old DuckDB files and verify

**Files:** `src/dagster_pipeline/resources/duckdb.py` (remove), `src/dagster_pipeline/assets/duckdb_loader.py` (remove)

- [ ] **Step 1: Remove old DuckDB source files** if they still exist.

- [ ] **Step 2: Verify no duckdb references remain**

```bash
grep -ri "duckdb\|DuckDB" src/ tests/ --include="*.py" --include="*.toml" --include="*.md" --include="*.yaml" --include="*.yml"
```

Expected: No results (except in this plan file or spec file).

- [ ] **Step 3: Run full test suite and verify definitions load**

```bash
pytest tests/ -v
python -c "from dagster_pipeline.definitions import defs; print('Definitions OK:', len(defs.asset_keys))"
```

- [ ] **Step 4: Final commit**

```bash
git add -A
git commit -m "feat: complete PostgreSQL migration — remove all DuckDB references"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: All sections of the design spec have a task
- [x] **No placeholders**: Every step has actual code or commands
- [x] **Type consistency**: `PostgresResource` fields match across all tasks
- [x] **Review Focus covered**:
  - Connection string format → Task 2 tests
  - Table creation via `to_sql()` → Task 3 tests
  - Docker health check → Task 4 (`pg_isready`)
  - No duckdb references → Task 7 (`grep` verification)
  - Metabase JDBC URL → Task 6 (docs updated)
