# API Reference

## Resources

### OpenMeteoResource

`ConfigurableResource` configured via environment variables.

| Field | Env Var | Type | Default |
|-------|---------|------|---------|
| `base_url` | `OPEN_METEO_BASE_URL` | `str` | `https://api.open-meteo.com` |
| `archive_url` | `OPEN_METEO_ARCHIVE_URL` | `str` | `https://archive-api.open-meteo.com` |
| `latitude` | `WEATHER_LATITUDE` | `float` | `21.0285` |
| `longitude` | `WEATHER_LONGITUDE` | `float` | `105.8542` |
| `timezone` | `WEATHER_TIMEZONE` | `str` | `Asia/Bangkok` |
| `timeout` | — | `int` | `30` |

**Methods:**
- `get_forecast(forecast_days, hourly_params, daily_params)` — Fetch forecast data
- `get_historical(start_date, end_date, hourly_params, daily_params)` — Fetch historical data
- `get_historical_forecast(start_date, end_date)` — Fetch historical forecast

### WikimediaResource

`ConfigurableResource` configured via environment variables.

| Field | Env Var | Type | Default |
|-------|---------|------|---------|
| `stream_url` | `WIKIMEDIA_STREAM_URL` | `str` | `https://stream.wikimedia.org/v2/stream/recentchange` |
| `timeout` | — | `int` | `30` |
| `max_events` | — | `int` | `100` |

**Methods:**
- `stream_events(since_event_id, max_events)` — Stream events from Wikimedia SSE
- `get_cursor_state(event)` — Extract cursor state from an event

## Assets

### Weather Assets (DailyPartitionsDefinition, start_date="2026-01-01")

| Asset | Input | Output | Description |
|-------|-------|--------|-------------|
| `weather_historical` | partition_date | `list[dict]` | Historical weather from Archive API |
| `weather_hourly` | Open-Meteo Forecast | `pd.DataFrame` | Hourly time-series observations |
| `weather_forecast` | Open-Meteo Forecast | `pd.DataFrame` | Future predictions (7-day) |
| `weather_cleaned` | historical + hourly | `pd.DataFrame` | Validated & cleaned data |
| `weather_daily` | weather_cleaned | `pd.DataFrame` | Daily aggregates |
| `forecast_accuracy` | forecast + historical | `pd.DataFrame` | MAE/RMSE/bias metrics |

### Wiki Assets (DailyPartitionsDefinition, start_date="2026-01-01")

| Asset | Input | Output | Description |
|-------|-------|--------|-------------|
| `wiki_events_raw` | Wikimedia SSE | `list[dict]` | Raw events |
| `wiki_events_cleaned` | wiki_events_raw | `pd.DataFrame` | Cleaned & deduplicated |
| `wiki_events_by_hour` | wiki_events_cleaned | `pd.DataFrame` | Hourly aggregations |
| `wiki_daily_analytics` | wiki_events_by_hour | `pd.DataFrame` | Daily summaries |

## Asset Checks

### Weather Checks
- `temperature_not_null` — ERROR: No null temperature values
- `humidity_valid` — WARNING: Humidity within 0-100%
- `precipitation_non_negative` — ERROR: Precipitation >= 0
- `minimum_row_count` — WARNING: At least 1 row

### Wiki Checks
- `event_id_unique` — ERROR: No duplicate event IDs
- `timestamp_not_null` — ERROR: No null timestamps
- `wiki_not_null` — WARNING: No null wiki values
- `minimum_event_count` — WARNING: At least 1 event

## Schedule

- **`weather_daily_schedule`** — Cron `0 23 * * *`, targets `weather_historical`

## Sensor

- **`wikimedia_event_sensor`** — Monitors Wikimedia stream, cursor-based incremental, `minimum_interval_seconds=60`, default status STOPPED
