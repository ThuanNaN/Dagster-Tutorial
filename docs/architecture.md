# Architecture

## Overview

This project demonstrates a complete Dagster data pipeline using real public APIs. It combines batch processing (Open-Meteo weather data), time-series analysis, forecasting, and streaming event ingestion (Wikimedia EventStreams).

## Components

### Weather Pipeline
- **weather_historical**: Batch historical weather data from Open-Meteo Archive API (DailyPartitionsDefinition)
- **weather_hourly**: Hourly time-series observations from Forecast API
- **weather_forecast**: Future weather predictions (7-day forecast)
- **weather_cleaned**: Validated and cleaned data with quality rules
- **weather_daily**: Aggregated daily statistics (min/max/avg temperature, humidity, precipitation, wind)
- **forecast_accuracy**: Comparison of forecast vs actuals with MAE, RMSE, and bias metrics

### Wiki Pipeline
- **wiki_events_raw**: Raw events from Wikimedia EventStreams (SSE)
- **wiki_events_cleaned**: Cleaned and deduplicated events
- **wiki_events_by_hour**: Hourly aggregations with unique user counts
- **wiki_daily_analytics**: Daily summaries

### Shared Infrastructure
- **OpenMeteoResource**: ConfigurableResource with HTTP client, timeout, retry
- **WikimediaResource**: ConfigurableResource with SSE parsing, cursor tracking
- **FilesystemIOManager**: Persists assets as JSON (raw) and Parquet (cleaned/analytics)
- **Asset Checks**: Data quality validation on weather_cleaned and wiki_events_cleaned
- **Schedule**: Daily materialization at 23:00 UTC
- **Sensor**: Wikimedia event-driven triggers with cursor-based incremental processing

## Data Flow

```
Open-Meteo ──HTTP──> weather_historical ──┐
Open-Meteo ──HTTP──> weather_hourly ──────┤
Open-Meteo ──HTTP──> weather_forecast ────┤
                                           ▼
                                     weather_cleaned
                                           │
                                           ▼
                                     weather_daily ──┐
                                                     │
                                        forecast_accuracy ◄──┘
                                                                │
Wikimedia SSE ──> wiki_events_raw ──> wiki_events_cleaned ──> wiki_events_by_hour ──> wiki_daily_analytics
```

## I/O

All data persisted to filesystem under `DATA_DIR` (default `./data`):
- Raw: JSON (`data/raw/...`)
- Cleaned: Parquet (`data/cleaned/...`)
- Analytics: Parquet (`data/analytics/...`)

## Freshness

- **Weather**: Maximum lag 24h (daily batch)
- **Wiki**: Maximum lag 15m (near-real-time streaming)
