"""Asset checks for Wiki data quality."""
import pandas as pd
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity


def _event_id_unique_logic(wiki_events_cleaned: pd.DataFrame) -> bool:
    if "event_id" in wiki_events_cleaned.columns:
        return wiki_events_cleaned["event_id"].duplicated().sum() == 0
    return True


def _timestamp_not_null_logic(wiki_events_cleaned: pd.DataFrame) -> bool:
    if "timestamp" in wiki_events_cleaned.columns:
        return wiki_events_cleaned["timestamp"].isnull().sum() == 0
    return True


def _wiki_not_null_logic(wiki_events_cleaned: pd.DataFrame) -> bool:
    if "wiki" in wiki_events_cleaned.columns:
        return wiki_events_cleaned["wiki"].isnull().sum() == 0
    return True


def _minimum_event_count_logic(wiki_events_cleaned: pd.DataFrame) -> bool:
    return len(wiki_events_cleaned) >= 1


@asset_check(asset="wiki_events_cleaned", name="event_id_unique")
def event_id_unique(wiki_events_cleaned: pd.DataFrame) -> AssetCheckResult:
    passed = _event_id_unique_logic(wiki_events_cleaned)
    if not passed:
        dup_count = wiki_events_cleaned["event_id"].duplicated().sum() if "event_id" in wiki_events_cleaned.columns else 0
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR, description=f"Found {dup_count} duplicate event_ids")
    return AssetCheckResult(passed=True, description="All event IDs unique")


@asset_check(asset="wiki_events_cleaned", name="timestamp_not_null")
def timestamp_not_null(wiki_events_cleaned: pd.DataFrame) -> AssetCheckResult:
    passed = _timestamp_not_null_logic(wiki_events_cleaned)
    if not passed:
        null_count = wiki_events_cleaned["timestamp"].isnull().sum() if "timestamp" in wiki_events_cleaned.columns else 0
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.ERROR, description=f"Found {null_count} null timestamps")
    return AssetCheckResult(passed=True, description="All timestamps present")


@asset_check(asset="wiki_events_cleaned", name="wiki_not_null")
def wiki_not_null(wiki_events_cleaned: pd.DataFrame) -> AssetCheckResult:
    passed = _wiki_not_null_logic(wiki_events_cleaned)
    if not passed:
        null_count = wiki_events_cleaned["wiki"].isnull().sum() if "wiki" in wiki_events_cleaned.columns else 0
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARNING, description=f"Found {null_count} null wiki values")
    return AssetCheckResult(passed=True, description="All wiki values present")


@asset_check(asset="wiki_events_cleaned", name="minimum_event_count")
def minimum_event_count(wiki_events_cleaned: pd.DataFrame) -> AssetCheckResult:
    passed = _minimum_event_count_logic(wiki_events_cleaned)
    if not passed:
        return AssetCheckResult(passed=False, severity=AssetCheckSeverity.WARNING, description=f"No events found")
    return AssetCheckResult(passed=True, description=f"Found {len(wiki_events_cleaned)} events")


wiki_asset_checks = [event_id_unique, timestamp_not_null, wiki_not_null, minimum_event_count]
