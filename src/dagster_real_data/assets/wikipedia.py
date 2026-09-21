"""Wiki assets — raw events, cleaned, by_hour, daily_analytics."""
import logging

import pandas as pd
from dagster import AssetExecutionContext, asset, DailyPartitionsDefinition, AssetIn

from dagster_real_data.resources.wikimedia import WikimediaResource

logger = logging.getLogger(__name__)

wiki_partitions = DailyPartitionsDefinition(start_date="2026-01-01")


@asset(
    name="wiki_events_raw",
    partitions_def=wiki_partitions,
    io_manager_key="io",
    description="Raw Wikimedia EventStream events",
)
def wiki_events_raw(context: AssetExecutionContext, wikimedia: WikimediaResource) -> list[dict]:
    partition_date = context.partition_key
    logger.info("Ingesting Wikimedia events for partition=%s", partition_date)

    events = list(wikimedia.stream_events(max_events=wikimedia.max_events))
    logger.info("Ingested %d raw events for %s", len(events), partition_date)
    return events


@asset(
    name="wiki_events_cleaned",
    partitions_def=wiki_partitions,
    io_manager_key="io",
    description="Cleaned and deduplicated Wiki events",
    ins={"raw": AssetIn(key_prefix=["wiki_events_raw"])},
)
def wiki_events_cleaned(context: AssetExecutionContext, raw: list[dict]) -> pd.DataFrame:
    logger.info("Cleaning wiki events for partition=%s", context.partition_key)

    df = pd.DataFrame(raw)

    # Validation rules
    df = df.dropna(subset=["event_id", "timestamp", "wiki", "title"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["bot"] = df["bot"].astype(bool)
    df["user"] = df["user"].astype(str)

    # Deduplicate by event_id
    df = df.drop_duplicates(subset=["event_id"])

    logger.info("Cleaned %d wiki events (deduplicated)", len(df))
    return df


@asset(
    name="wiki_events_by_hour",
    partitions_def=wiki_partitions,
    io_manager_key="io",
    description="Wiki events aggregated by hour",
    ins={"cleaned": AssetIn(key_prefix=["wiki_events_cleaned"])},
)
def wiki_events_by_hour(context: AssetExecutionContext, cleaned: pd.DataFrame) -> pd.DataFrame:
    logger.info("Aggregating wiki events by hour for partition=%s", context.partition_key)

    cleaned = cleaned.copy()
    cleaned["hour"] = cleaned["timestamp"].dt.floor("H")
    grouped = cleaned.groupby(["hour", "wiki", "type"]).agg(
        event_count=("event_id", "count"),
        unique_users=("user", "nunique"),
        bot_event_count=("bot", "sum"),
    ).reset_index()

    logger.info("Created %d hourly aggregations", len(grouped))
    return grouped


@asset(
    name="wiki_daily_analytics",
    partitions_def=wiki_partitions,
    io_manager_key="io",
    description="Daily Wiki analytics summaries",
    ins={"by_hour": AssetIn(key_prefix=["wiki_events_by_hour"])},
)
def wiki_daily_analytics(context: AssetExecutionContext, by_hour: pd.DataFrame) -> pd.DataFrame:
    logger.info("Computing daily wiki analytics for partition=%s", context.partition_key)

    daily = by_hour.groupby("hour").agg(
        total_events=("event_count", "sum"),
        unique_users=("unique_users", "max"),
        bot_events=("bot_event_count", "sum"),
    ).reset_index()

    daily["human_events"] = daily["total_events"] - daily["bot_events"]

    logger.info("Computed daily analytics: %d rows", len(daily))
    return daily


# Collection for Definitions
wiki_assets = [
    wiki_events_raw,
    wiki_events_cleaned,
    wiki_events_by_hour,
    wiki_daily_analytics,
]
