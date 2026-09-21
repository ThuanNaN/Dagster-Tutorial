"""Tests for asset checks."""
import pandas as pd
from dagster_real_data.checks.weather_checks import (
    _temperature_not_null_logic,
    _humidity_valid_logic,
    _precipitation_non_negative_logic,
    _minimum_row_count_logic,
    temperature_not_null,
    humidity_valid,
    precipitation_non_negative,
    minimum_row_count,
)
from dagster_real_data.checks.wiki_checks import (
    _event_id_unique_logic,
    _timestamp_not_null_logic,
    _wiki_not_null_logic,
    _minimum_event_count_logic,
    event_id_unique,
    timestamp_not_null,
    wiki_not_null,
    minimum_event_count,
)


def test_temperature_not_null_pass():
    df = pd.DataFrame({"temperature_2m": [25.0, 26.0, 27.0]})
    assert _temperature_not_null_logic(df)


def test_temperature_not_null_fail():
    df = pd.DataFrame({"temperature_2m": [25.0, None, 27.0]})
    assert not _temperature_not_null_logic(df)


def test_humidity_valid_pass():
    df = pd.DataFrame({"relative_humidity_2m": [50.0, 60.0, 70.0]})
    assert _humidity_valid_logic(df)


def test_humidity_valid_fail():
    df = pd.DataFrame({"relative_humidity_2m": [50.0, 150.0, -10.0]})
    assert not _humidity_valid_logic(df)


def test_precipitation_non_negative_pass():
    df = pd.DataFrame({"precipitation": [0.0, 1.0, 2.0]})
    assert _precipitation_non_negative_logic(df)


def test_precipitation_non_negative_fail():
    df = pd.DataFrame({"precipitation": [0.0, -1.0, 2.0]})
    assert not _precipitation_non_negative_logic(df)


def test_minimum_row_count_pass():
    df = pd.DataFrame({"temperature_2m": [25.0]})
    assert _minimum_row_count_logic(df)


def test_minimum_row_count_fail():
    df = pd.DataFrame({"temperature_2m": []})
    assert not _minimum_row_count_logic(df)


def test_event_id_unique_pass():
    df = pd.DataFrame({"event_id": ["1", "2", "3"], "timestamp": ["2026-01-01"] * 3})
    assert _event_id_unique_logic(df)


def test_event_id_unique_fail():
    df = pd.DataFrame({"event_id": ["1", "1", "2"], "timestamp": ["2026-01-01"] * 3})
    assert not _event_id_unique_logic(df)


def test_timestamp_not_null_pass():
    df = pd.DataFrame({"timestamp": ["2026-01-01", "2026-01-02"]})
    assert _timestamp_not_null_logic(df)


def test_timestamp_not_null_fail():
    df = pd.DataFrame({"timestamp": ["2026-01-01", None]})
    assert not _timestamp_not_null_logic(df)


def test_wiki_not_null_pass():
    df = pd.DataFrame({"wiki": ["enwiki", "dewiki"]})
    assert _wiki_not_null_logic(df)


def test_wiki_not_null_fail():
    df = pd.DataFrame({"wiki": ["enwiki", None]})
    assert not _wiki_not_null_logic(df)


def test_minimum_event_count_pass():
    df = pd.DataFrame({"event_id": ["1", "2"]})
    assert _minimum_event_count_logic(df)


def test_minimum_event_count_fail():
    df = pd.DataFrame({"event_id": []})
    assert not _minimum_event_count_logic(df)


