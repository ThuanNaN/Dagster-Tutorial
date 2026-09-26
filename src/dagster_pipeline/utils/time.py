"""Time and date utilities."""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


def now_utc() -> datetime:
    return datetime.now(tz=ZoneInfo("UTC"))


def format_date(d: date) -> str:
    return d.isoformat()


def parse_date(s: str) -> date:
    return date.fromisoformat(s)


def date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]
