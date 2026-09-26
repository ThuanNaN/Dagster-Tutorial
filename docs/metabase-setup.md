# Metabase Setup for PostgreSQL

## Prerequisites
- PostgreSQL running (see `docker-compose up`)
- Metabase running (see `docker-compose up`)

## Connect Metabase to PostgreSQL

> **Important:** Metabase runs inside a Docker container. Use `postgres` as the host (the Docker service name), not `localhost`.

### 1. Start both services
```bash
docker-compose up -d
```

### 2. Add Database in Metabase
1. Open Metabase (default: `http://localhost:3000`)
2. Go to **Settings → Databases → Add Database**
3. Select **PostgreSQL** as the database type
4. Enter connection details:
   - **Host**: `postgres` *(Docker service name, NOT localhost)*
   - **Port**: `5432`
   - **Database**: `mydb`
   - **Username**: `postgres`
   - **Password**: `postgres`
   - **JDBC URL**: `jdbc:postgresql://postgres:5432/mydb`

### 3. Verify Connection
- Click **Test Connection** in Metabase
- Should see tables: `weather_historical`, `weather_hourly`, `weather_forecast`, `wiki_events_raw`, etc.

### 4. Create Dashboards
- Click **New → Question** to explore data
- Build visualizations for weather trends, Wikipedia page views, etc.
- Save as Dashboard from the **Save** menu
