# Dagster Real Data Pipeline — Design Specification

**Date**: 2026-09-22
**Status**: Approved

---

## 1. Goal

Build a **Dagster Data Engineering Demo Project** using real public data sources:

- **Open-Meteo** — batch/time-series/weather forecast APIs
- **Wikimedia EventStreams** — streaming/event-driven SSE pipeline

The project must demonstrate: Assets, Asset dependency/lineage, Resources, EnvVar configuration, IOManager, Partitions, Historical data, Time-series, Forecast, Backfill, Schedule, Freshness, Asset checks, Sensors, Streaming, Incremental processing, and Testing.

Final goal: `dagster dev` opens the UI with the full asset graph visible.

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
   ┌─────────┼───────────┐               │
   │         │           │               │
   ▼         ▼           ▼               ▼
Historical  Forecast   Hourly      Streaming events
   │         │           │               │
   ▼         ▼           ▼               ▼
weather_  weather_   weather_     wiki_events_raw
historical forecast  timeseries             │
             │                           ▼
             └───────────┐     wiki_events_cleaned
                         │               │
                         ▼               ▼
                  weather_cleaned    wiki_events_by_hour
                         │               │
                         ▼               ▼
                  weather_daily     wiki_daily_analytics
                         │
                         ▼
                forecast_accuracy
```

---

## 3. External APIs

### 3.1 Open-Meteo

- **Forecast**: `https://api.open-meteo.com/v1/forecast`
- **Historical**: `https://archive-api.open-meteo.com/v1/archive`
- **Historical Forecast**: `https://historical-forecast-api.open-meteo.com/v1/forecast`

Default location: latitude=21.0285, longitude=105.8542, timezone=Asia/Bangkok

### 3.2 Wikimedia

- **Endpoint**: `https://stream.wikimedia.org/v2/stream/recentchange`
- **Protocol**: Server-Sent Events (SSE)
- **No API key required**

---

## 4. Repository Structure

```
dagster-real-data-pipeline/
├── pyproject.toml
├── README.md
├── .env.example
├── .gitignore
├── src/
│   └── dagster_real_data/
│       ├── __init__.py
│       ├── definitions.py
│       ├── assets/
│       │   ├── __init__.py
│       │   ├── weather.py
│       │   └── wikipedia.py
│       ├── resources/
│       │   ├── __init__.py
│       │   ├── open_meteo.py
│       │   ├── wikimedia.py
│       │   └── io_manager.py
│       ├── checks/
│       │   ├── __init__.py
│       │   ├── weather_checks.py
│       │   └── wiki_checks.py
│       ├── schedules/
│       │   ├── __init__.py
│       │   └── weather_schedule.py
│       ├── sensors/
│       │   ├── __init__.py
│       │   ├── weather_sensor.py
│       │   └── wiki_sensor.py
│       └── utils/
│           ├── __init__.py
│           ├── http.py
│           ├── time.py
│           └── validation.py
├── tests/
│   ├── test_weather_assets.py
│   ├── test_wiki_assets.py
│   ├── test_resources.py
│   └── test_checks.py
├── data/
│   ├── raw/
│   ├── cleaned/
│   └── analytics/
└── docs/
    ├── architecture.md
    └── api.md
```

---

## 5. Data Storage

- **Raw**: JSON
- **Cleaned**: Parquet
- **Analytics**: Parquet
- Filesystem-based, no external database

---

## 6. Configuration

All configuration via `EnvVar(...)`. No hard-coded secrets.

Key env vars: `OPEN_METEO_BASE_URL`, `OPEN_METEO_ARCHIVE_URL`, `WEATHER_LATITUDE`, `WEATHER_LONGITUDE`, `WEATHER_TIMEZONE`, `WIKIMEDIA_STREAM_URL`, `DATA_DIR`, `HTTP_TIMEOUT_SECONDS`, `WIKIMEDIA_MAX_EVENTS_PER_RUN`.

---

## 7. Core Components

### 7.1 Resources

**OpenMeteoResource** (`ConfigurableResource`):
- Fields: `base_url`, `archive_url`, `latitude`, `longitude`, `timezone`, `timeout`
- Methods: `get_forecast()`, `get_historical()`, `get_historical_forecast()`
- HTTP client with timeout, retry (exponential backoff), status validation, logging

**WikimediaResource** (`ConfigurableResource`):
- Fields: `stream_url`, `timeout`, `max_events`
- Methods: `stream_events()`
- SSE parsing, bounded event window, cursor-based incremental processing, graceful shutdown

### 7.2 IOManager

Filesystem IOManager:
- Asset output → filesystem (JSON/Parquet)
- Filesystem → asset input
- Partitions determine file paths

### 7.3 Weather Assets

| Asset | Partitions | Input | Output |
|-------|-----------|-------|--------|
| `weather_historical` | Daily | partition_date | `data/raw/weather/historical/YYYY-MM-DD.json` |
| `weather_hourly` | Daily | Open-Meteo Forecast API | hourly observations |
| `weather_forecast` | Daily | Open-Meteo Forecast API | future predictions |
| `weather_cleaned` | Daily | historical + hourly | `cleaned/weather/YYYY-MM-DD.parquet` |
| `weather_daily` | Daily | weather_cleaned | daily aggregates |
| `forecast_accuracy` | Daily | forecast + historical | MAE/RMSE/bias metrics |

### 7.4 Wiki Assets

| Asset | Partitions | Input | Output |
|-------|-----------|-------|--------|
| `wiki_events_raw` | Daily | Wikimedia SSE | `data/raw/wiki/YYYY-MM-DD/*.json` |
| `wiki_events_cleaned` | Daily | wiki_events_raw | cleaned parquet |
| `wiki_events_by_hour` | Daily | wiki_events_cleaned | hourly aggregates |
| `wiki_daily_analytics` | Daily | wiki_events_by_hour | daily analytics |

### 7.5 Asset Checks

**Weather checks**: temperature_not_null, humidity_valid, precipitation_non_negative, minimum_row_count
**Wiki checks**: event_id_unique, timestamp_not_null, wiki_not_null, minimum_event_count

Bad upstream data → check failure → downstream not materialized as valid.

### 7.6 Schedule

- `weather_daily_schedule` at 23:00 UTC
- Materializes weather partition for each day

### 7.7 Sensor

- `wikimedia_event_sensor`
- Cursor: `last_processed_event_id`, `last_processed_timestamp`
- New events detected → trigger materialization
- No infinite loops; bounded processing

---

## 8. Error Handling

- HTTP 500, 429, timeout, connection reset, malformed JSON handled
- Exponential backoff retry: 1s → 2s → 4s, max retries capped
- Never retry infinitely
- Descriptive exceptions for unrecoverable errors

---

## 9. Testing

- No test requires internet (mock external APIs)
- Unit tests: API parsing, schema validation, cleaning, deduplication, aggregation, asset checks, partition behavior
- Integration test: materialize `[weather_historical, weather_cleaned, weather_daily]` with test resources and in-memory IOManager

---

## 10. Implementation Phases

| Phase | Deliverable | Acceptance |
|-------|------------|------------|
| 1 | Bootstrap (pyproject, src, definitions, .env, README) | `dagster dev` runs |
| 2 | Open-Meteo Resource | Manual API call works |
| 3 | Historical Asset | One partition → real API → persisted data |
| 4 | Time-series (hourly, cleaned, daily) | Lineage visible in Dagster |
| 5 | Forecast + forecast_accuracy | Forecast + actual → metrics |
| 6 | Asset checks + freshness | Bad data → check failure |
| 7 | Schedule + Backfill | Multiple partitions materialize |
| 8 | Wikimedia (Resource, SSE, assets) | Real events ingested |
| 9 | Sensor + cursor + incremental | New event → materialization |
| 10 | Testing | `pytest` passes without internet |
| 11 | Documentation | README, architecture.md, api.md complete |

---

## 11. Definition of Done

- [ ] `dagster dev` starts
- [ ] Dagster UI loads
- [ ] Asset graph visible
- [ ] Open-Meteo real API works
- [ ] Historical, hourly, forecast, daily, forecast_accuracy assets work
- [ ] DailyPartitionsDefinition works
- [ ] Backfill works
- [ ] IOManager works
- [ ] ConfigurableResource works
- [ ] EnvVar configuration works
- [ ] Freshness configured
- [ ] Asset checks configured
- [ ] Wikimedia SSE works
- [ ] Wiki cleaning and aggregation works
- [ ] Sensor works with cursor
- [ ] Retry and error handling work
- [ ] Tests pass without internet
- [ ] README contains setup instructions
- [ ] `.env.example` exists
- [ ] No secrets committed
