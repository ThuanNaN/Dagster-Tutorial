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
