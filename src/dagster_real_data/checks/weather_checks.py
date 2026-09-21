"""Asset checks for weather data quality."""
import logging

import pandas as pd
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity

logger = logging.getLogger(__name__)


def _temperature_not_null_logic(weather_cleaned: pd.DataFrame) -> bool:
    if "temperature_2m" in weather_cleaned.columns:
        return weather_cleaned["temperature_2m"].isnull().sum() == 0
    return True


def _humidity_valid_logic(weather_cleaned: pd.DataFrame) -> bool:
    if "relative_humidity_2m" in weather_cleaned.columns:
        invalid = ((weather_cleaned["relative_humidity_2m"] < 0) | (weather_cleaned["relative_humidity_2m"] > 100)).sum()
        return invalid == 0
    return True


def _precipitation_non_negative_logic(weather_cleaned: pd.DataFrame) -> bool:
    if "precipitation" in weather_cleaned.columns:
        return (weather_cleaned["precipitation"] < 0).sum() == 0
    return True


def _minimum_row_count_logic(weather_cleaned: pd.DataFrame) -> bool:
    return len(weather_cleaned) >= 1


@asset_check(asset="weather_cleaned", name="temperature_not_null")
def temperature_not_null(weather_cleaned: pd.DataFrame) -> AssetCheckResult:
    passed = _temperature_not_null_logic(weather_cleaned)
    if not passed:
        null_count = weather_cleaned["temperature_2m"].isnull().sum() if "temperature_2m" in weather_cleaned.columns else 0
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR, description=f"Found {null_count} null temperature values")
    return AssetCheckResult(passed=True, description="All temperature values present")


@asset_check(asset="weather_cleaned", name="humidity_valid")
def humidity_valid(weather_cleaned: pd.DataFrame) -> AssetCheckResult:
    passed = _humidity_valid_logic(weather_cleaned)
    if not passed:
        invalid = ((weather_cleaned["relative_humidity_2m"] < 0) | (weather_cleaned["relative_humidity_2m"] > 100)).sum()
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARNING, description=f"Found {invalid} invalid humidity values")
    return AssetCheckResult(passed=True, description="All humidity values valid")


@asset_check(asset="weather_cleaned", name="precipitation_non_negative")
def precipitation_non_negative(weather_cleaned: pd.DataFrame) -> AssetCheckResult:
    passed = _precipitation_non_negative_logic(weather_cleaned)
    if not passed:
        negative = (weather_cleaned["precipitation"] < 0).sum()
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR, description=f"Found {negative} negative precipitation values")
    return AssetCheckResult(passed=True, description="All precipitation values non-negative")


@asset_check(asset="weather_cleaned", name="minimum_row_count")
def minimum_row_count(weather_cleaned: pd.DataFrame) -> AssetCheckResult:
    passed = _minimum_row_count_logic(weather_cleaned)
    if not passed:
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARNING, description=f"No rows found")
    return AssetCheckResult(passed=True, description=f"Found {len(weather_cleaned)} rows")


weather_asset_checks = [temperature_not_null, humidity_valid, precipitation_non_negative, minimum_row_count]
