# Metabase Setup for PostgreSQL

## Prerequisites
- PostgreSQL running (see `docker-compose up`)
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
