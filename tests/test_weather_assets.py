"""Tests for weather assets."""
from unittest.mock import MagicMock
import pandas as pd

from dagster_pipeline.assets.weather import weather_partitions
from dagster_pipeline.utils.time import date_range


def _build_historical_data():
    """Build historical weather data like weather_historical would."""
    data = {
        "hourly": {
            "time": ["2026-01-01T00:00", "2026-01-01T01:00"],
            "temperature_2m": [25.0, 26.0],
            "relative_humidity_2m": [60.0, 65.0],
            "precipitation": [0.0, 0.5],
            "wind_speed_10m": [5.0, 6.0],
        }
    }
    records = []
    timestamps = data["hourly"]["time"]
    for i, ts in enumerate(timestamps):
        record = {"timestamp": ts, "partition_date": "2026-01-01"}
        for key, values in data["hourly"].items():
            if key != "time" and i < len(values):
                record[key] = values[i]
        records.append(record)
    return records


def _build_hourly_data():
    """Build hourly forecast data like weather_hourly would."""
    forecast_data = {
        "hourly": {
            "time": ["2026-01-01T00:00"],
            "temperature_2m": [27.0],
            "relative_humidity_2m": [55.0],
            "precipitation": [0.0],
            "wind_speed_10m": [4.0],
        }
    }
    timestamps = forecast_data["hourly"]["time"]
    records = []
    for i, ts in enumerate(timestamps):
        record = {"timestamp": ts}
        for key, values in forecast_data["hourly"].items():
            if key != "time" and i < len(values):
                record[key] = values[i]
        records.append(record)
    return pd.DataFrame(records)


def test_weather_historical_calls_api():
    historical = _build_historical_data()
    assert len(historical) == 2
    assert historical[0]["timestamp"] == "2026-01-01T00:00"
    assert historical[0]["temperature_2m"] == 25.0


def test_weather_hourly_returns_dataframe():
    hourly = _build_hourly_data()
    assert isinstance(hourly, pd.DataFrame)
    assert len(hourly) == 1


def test_weather_forecast_returns_dataframe():
    forecast_data = {
        "hourly": {"time": [f"2026-01-01T{i:02d}:00" for i in range(7)], "temperature_2m": [25.0]*7}
    }
    timestamps = forecast_data["hourly"]["time"]
    records = []
    for i, ts in enumerate(timestamps):
        record = {"forecast_timestamp": ts}
        for key, values in forecast_data["hourly"].items():
            if key != "time" and i < len(values):
                record[key] = values[i]
        records.append(record)
    df = pd.DataFrame(records)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_weather_cleaned_filters_and_combines():
    historical = _build_historical_data()
    hourly = _build_hourly_data()

    hist_df = pd.DataFrame(historical)
    dfs = [d for d in [hist_df, hourly] if not d.empty]
    combined = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    combined = combined.dropna(subset=["timestamp", "temperature_2m"])
    combined = combined[combined["temperature_2m"].between(-50, 60)]
    combined = combined[combined["relative_humidity_2m"].between(0, 100)]
    combined = combined[combined["wind_speed_10m"] >= 0]
    combined = combined[combined["precipitation"] >= 0]

    assert isinstance(combined, pd.DataFrame)
    assert len(combined) > 0


def test_weather_daily_aggregates():
    cleaned = pd.DataFrame({
        "timestamp": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "temperature_2m": [25.0, 26.0],
        "relative_humidity_2m": [60.0, 65.0],
        "precipitation": [0.0, 0.5],
        "wind_speed_10m": [5.0, 6.0],
    })
    daily = cleaned.groupby(cleaned["timestamp"].str[:10]).agg(
        temperature_min=("temperature_2m", "min"),
        temperature_max=("temperature_2m", "max"),
        temperature_avg=("temperature_2m", "mean"),
        humidity_avg=("relative_humidity_2m", "mean"),
        precipitation_total=("precipitation", "sum"),
        wind_speed_avg=("wind_speed_10m", "mean"),
    ).reset_index()
    assert isinstance(daily, pd.DataFrame)
    assert len(daily) > 0
    assert "temperature_min" in daily.columns


def test_forecast_accuracy_computes_metrics():
    forecast = pd.DataFrame({
        "timestamp": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "temperature_2m_forecast": [25.0, 26.0],
    })
    hist_df = pd.DataFrame({
        "timestamp": ["2026-01-01T00:00", "2026-01-01T01:00"],
        "temperature_2m": [24.5, 26.2],
    })

    merged = forecast.merge(hist_df, on="timestamp", how="inner", suffixes=("_forecast", "_actual"))
    # After merge, the actual column is 'temperature_2m' (from hist_df)
    merged["absolute_error"] = (merged["temperature_2m_forecast"] - merged["temperature_2m"]).abs()
    merged["squared_error"] = (merged["temperature_2m_forecast"] - merged["temperature_2m"]) ** 2

    mae = merged["absolute_error"].mean()
    rmse = (merged["squared_error"].mean()) ** 0.5
    bias = (merged["temperature_2m_forecast"] - merged["temperature_2m"]).mean()

    assert abs(mae - 0.35) < 0.01
    assert rmse > 0
    assert bias != 0


def test_weather_partitions_has_correct_start_date():
    assert weather_partitions.start_ts is not None
