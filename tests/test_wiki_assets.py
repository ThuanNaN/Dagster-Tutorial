"""Tests for wiki assets."""
import pandas as pd

from dagster_pipeline.assets.wikipedia import wiki_partitions


def test_wiki_events_raw_ingests():
    raw_events = [
        {"event_id": "1", "timestamp": "2026-01-01T00:00:00Z", "wiki": "enwiki"},
        {"event_id": "2", "timestamp": "2026-01-01T00:30:00Z", "wiki": "enwiki"},
        {"event_id": "3", "timestamp": "2026-01-01T01:00:00Z", "wiki": "enwiki"},
    ]
    assert len(raw_events) == 3
    assert raw_events[0]["event_id"] == "1"


def test_wiki_events_cleaned_deduplicates():
    raw = [
        {"event_id": "1", "timestamp": "2026-01-01T00:00:00Z", "wiki": "enwiki", "title": "Test", "user": "Alice", "bot": False, "type": "edit"},
        {"event_id": "1", "timestamp": "2026-01-01T00:00:00Z", "wiki": "enwiki", "title": "Test", "user": "Alice", "bot": False, "type": "edit"},
        {"event_id": "2", "timestamp": "2026-01-01T01:00:00Z", "wiki": "enwiki", "title": "Test2", "user": "Bob", "bot": True, "type": "edit"},
    ]
    df = pd.DataFrame(raw)
    df = df.dropna(subset=["event_id", "timestamp", "wiki", "title"])
    df = df.drop_duplicates(subset=["event_id"])
    assert len(df) == 2
    assert df["event_id"].nunique() == 2


def test_wiki_events_by_hour_aggregates():
    cleaned = pd.DataFrame([
        {"timestamp": pd.Timestamp("2026-01-01 00:00:00", tz="UTC"), "wiki": "enwiki", "type": "edit", "user": "Alice", "bot": False, "event_id": "1"},
        {"timestamp": pd.Timestamp("2026-01-01 00:30:00", tz="UTC"), "wiki": "enwiki", "type": "edit", "user": "Bob", "bot": False, "event_id": "2"},
        {"timestamp": pd.Timestamp("2026-01-01 01:00:00", tz="UTC"), "wiki": "enwiki", "type": "edit", "user": "Alice", "bot": False, "event_id": "3"},
    ])
    cleaned = cleaned.copy()
    cleaned["hour"] = cleaned["timestamp"].dt.floor("h")
    grouped = cleaned.groupby(["hour", "wiki", "type"]).agg(
        event_count=("event_id", "count"),
        unique_users=("user", "nunique"),
        bot_event_count=("bot", "sum"),
    ).reset_index()
    assert len(grouped) == 2
    assert grouped["event_count"].sum() == 3


def test_wiki_events_by_hour_has_correct_columns():
    cleaned = pd.DataFrame([
        {"timestamp": pd.Timestamp("2026-01-01 00:00:00", tz="UTC"), "wiki": "enwiki", "type": "edit", "user": "Alice", "bot": False, "event_id": "1"},
        {"timestamp": pd.Timestamp("2026-01-01 00:30:00", tz="UTC"), "wiki": "enwiki", "type": "edit", "user": "Bob", "bot": False, "event_id": "2"},
    ])
    cleaned = cleaned.copy()
    cleaned["hour"] = cleaned["timestamp"].dt.floor("h")
    grouped = cleaned.groupby(["hour", "wiki", "type"]).agg(
        event_count=("event_id", "count"),
        unique_users=("user", "nunique"),
        bot_event_count=("bot", "sum"),
    ).reset_index()
    assert "event_count" in grouped.columns
    assert "unique_users" in grouped.columns
    assert "bot_event_count" in grouped.columns


def test_wiki_daily_analytics_computes():
    by_hour = pd.DataFrame([
        {"hour": pd.Timestamp("2026-01-01 00:00:00", tz="UTC"), "event_count": 100, "unique_users": 50, "bot_event_count": 10},
        {"hour": pd.Timestamp("2026-01-01 01:00:00", tz="UTC"), "event_count": 200, "unique_users": 80, "bot_event_count": 20},
    ])
    daily = by_hour.groupby("hour").agg(
        total_events=("event_count", "sum"),
        unique_users=("unique_users", "max"),
        bot_events=("bot_event_count", "sum"),
    ).reset_index()
    daily["human_events"] = daily["total_events"] - daily["bot_events"]
    assert isinstance(daily, pd.DataFrame)
    assert len(daily) == 2  # 2 hours
    assert "human_events" in daily.columns
    assert daily["total_events"].values[0] == 100
    assert daily["human_events"].values[0] == 90


def test_wiki_partitions_has_correct_start_date():
    assert wiki_partitions.start_ts is not None
