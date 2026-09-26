"""Weather assets — historical, hourly, forecast, cleaned, daily."""
import logging
from typing import Any

import pandas as pd
from dagster import AssetExecutionContext, asset, DailyPartitionsDefinition, AssetIn

from dagster_pipeline.resources.open_meteo import OpenMeteoResource
from dagster_pipeline.utils.time import format_date, parse_date, date_range

logger = logging.getLogger(__name__)

DEFAULT_PARTITION_START = "2026-01-01"

weather_partitions = DailyPartitionsDefinition(start_date=DEFAULT_PARTITION_START)


@asset(
    name="weather_historical",
    partitions_def=weather_partitions,
    io_manager_key="io",
    description="Historical weather data from Open-Meteo Archive API",
)
def weather_historical(context: AssetExecutionContext, open_meteo: OpenMeteoResource) -> list[dict[str, Any]]:
    partition_date = context.partition_key
    start_date = partition_date
    end_date = partition_date

    logger.info("Fetching historical weather for partition=%s", partition_date)
    data = open_meteo.get_historical(start_date, end_date)

    hourly = data.get("hourly", {})
    timestamps = hourly.get("time", [])
    records = []
    for i, ts in enumerate(timestamps):
        record: dict[str, Any] = {"timestamp": ts, "partition_date": partition_date}
        for key, values in hourly.items():
            if key != "time" and i < len(values):
                record[key] = values[i]
        records.append(record)

    logger.info("Fetched %d records for partition=%s", len(records), partition_date)
    return records


@asset(
    name="weather_hourly",
    partitions_def=weather_partitions,
    io_manager_key="io",
    description="Hourly time-series weather data from Open-Meteo Forecast API",
)
def weather_hourly(context: AssetExecutionContext, open_meteo: OpenMeteoResource) -> pd.DataFrame:
    partition_date = context.partition_key

    logger.info("Fetching hourly forecast for partition=%s", partition_date)
    forecast_data = open_meteo.get_forecast(forecast_days=1)

    hourly = forecast_data.get("hourly", {})
    timestamps = hourly.get("time", [])
    records = []
    for i, ts in enumerate(timestamps):
        record: dict[str, Any] = {"timestamp": ts}
        for key, values in hourly.items():
            if key != "time" and i < len(values):
                record[key] = values[i]
        records.append(record)

    df = pd.DataFrame(records)
    logger.info("Created %d hourly records for partition=%s", len(df), partition_date)
    return df


@asset(
    name="weather_forecast",
    partitions_def=weather_partitions,
    io_manager_key="io",
    description="Future weather forecast from Open-Meteo Forecast API",
)
def weather_forecast(context: AssetExecutionContext, open_meteo: OpenMeteoResource) -> pd.DataFrame:
    logger.info("Fetching weather forecast for partition=%s", context.partition_key)
    forecast_data = open_meteo.get_forecast(forecast_days=7, past_days=7)

    hourly = forecast_data.get("hourly", {})
    timestamps = hourly.get("time", [])
    records = []
    for i, ts in enumerate(timestamps):
        record: dict = {"forecast_timestamp": ts}
        for key, values in hourly.items():
            if key != "time" and i < len(values):
                record[key] = values[i]
        records.append(record)

    df = pd.DataFrame(records)
    logger.info("Created %d forecast records for partition=%s", len(df), context.partition_key)
    return df


@asset(
    name="weather_cleaned",
    partitions_def=weather_partitions,
    io_manager_key="io",
    description="Cleaned weather data with validation rules",
    ins={
        "historical": AssetIn(key=["weather_historical"]),
        "hourly": AssetIn(key=["weather_hourly"]),
    },
)
def weather_cleaned(
    context: AssetExecutionContext,
    historical: list[dict],
    hourly: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Cleaning weather data for partition=%s", context.partition_key)

    hist_df = pd.DataFrame(historical)
    dfs = [d for d in [hist_df, hourly] if not d.empty]
    combined = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    # Cleaning rules
    combined = combined.dropna(subset=["timestamp", "temperature_2m"])
    combined = combined[combined["temperature_2m"].between(-50, 60)]
    combined = combined[combined["relative_humidity_2m"].between(0, 100)]
    combined = combined[combined["wind_speed_10m"] >= 0]
    combined = combined[combined["precipitation"] >= 0]

    logger.info("Cleaned %d rows for partition=%s", len(combined), context.partition_key)
    return combined


@asset(
    name="weather_daily",
    partitions_def=weather_partitions,
    io_manager_key="io",
    description="Daily weather analytics aggregations",
    ins={"cleaned": AssetIn(key=["weather_cleaned"])},
)
def weather_daily(context: AssetExecutionContext, cleaned: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing daily analytics for partition=%s", context.partition_key)

    daily = cleaned.groupby(cleaned["timestamp"].str[:10]).agg(
        temperature_min=("temperature_2m", "min"),
        temperature_max=("temperature_2m", "max"),
        temperature_avg=("temperature_2m", "mean"),
        humidity_avg=("relative_humidity_2m", "mean"),
        precipitation_total=("precipitation", "sum"),
        wind_speed_avg=("wind_speed_10m", "mean"),
    ).reset_index()

    logger.info("Computed daily analytics: %d rows", len(daily))
    return daily


@asset(
    name="forecast_accuracy",
    partitions_def=weather_partitions,
    io_manager_key="io",
    description="Compare forecast vs historical actuals with MAE, RMSE, and bias metrics",
    ins={
        "forecast": AssetIn(key=["weather_forecast"]),
        "historical": AssetIn(key=["weather_historical"]),
    },
)
def forecast_accuracy(
    context: AssetExecutionContext,
    forecast: pd.DataFrame,
    historical: list[dict],
) -> pd.DataFrame:
    logger.info("Computing forecast accuracy for partition=%s", context.partition_key)

    hist_df = pd.DataFrame(historical)
    hist_df["timestamp"] = hist_df["timestamp"].astype(str)

    forecast_df = forecast.copy()
    forecast_df = forecast_df.rename(columns={"forecast_timestamp": "timestamp"})
    forecast_df["timestamp"] = forecast_df["timestamp"].astype(str)

    merged = forecast_df.merge(
        hist_df[["timestamp", "temperature_2m"]],
        on="timestamp",
        how="inner",
        suffixes=("_forecast", "_actual"),
    )

    if len(merged) == 0:
        logger.warning("No overlapping timestamps for forecast accuracy")
        return pd.DataFrame()

    merged["absolute_error"] = (merged["temperature_2m_forecast"] - merged["temperature_2m_actual"]).abs()
    merged["squared_error"] = (merged["temperature_2m_forecast"] - merged["temperature_2m_actual"]) ** 2

    mae = merged["absolute_error"].mean()
    rmse = (merged["squared_error"].mean()) ** 0.5
    bias = (merged["temperature_2m_forecast"] - merged["temperature_2m_actual"]).mean()

    result = pd.DataFrame([{
        "partition": context.partition_key,
        "mae": mae,
        "rmse": rmse,
        "bias": bias,
        "sample_count": len(merged),
    }])

    logger.info("Forecast accuracy: MAE=%.2f, RMSE=%.2f, bias=%.2f", mae, rmse, bias)
    return result


# Collection for Definitions
weather_assets = [
    weather_historical,
    weather_hourly,
    weather_forecast,
    weather_cleaned,
    weather_daily,
    forecast_accuracy,
]
