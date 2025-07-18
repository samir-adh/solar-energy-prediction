from datetime import datetime, tzinfo
import zoneinfo


def date_to_int(date: str) -> int:
    """Convert a date from ISO format to Unix time"""
    dt = datetime.fromisoformat(date).replace(tzinfo=zoneinfo.ZoneInfo('Europe/Paris'))
    timestamp = int(dt.timestamp())
    return timestamp


def int_to_date(date: int) -> str:
    """Convert a Unix time to a date in ISO format
    For example, 1700000000 -> '2023-11-14'
    """
    dt = datetime.fromtimestamp(date, tz=zoneinfo.ZoneInfo('Europe/Paris'))
    return dt.strftime("%Y-%m-%d")


def format_datetime(dt: datetime | int | float) -> str:
    if isinstance(dt, int) or isinstance(dt, float):
        dt = datetime.fromtimestamp(dt, tz=zoneinfo.ZoneInfo('Europe/Paris'))
    formatted = dt.replace(tzinfo=zoneinfo.ZoneInfo('Europe/Paris')).strftime("%Y-%m-%dT%H:%M:%S%z")
    formatted =formatted[:-2] + ':' + formatted[-2:]
    return formatted
