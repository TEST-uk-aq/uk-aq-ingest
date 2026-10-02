from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo


UK_LOCAL_TIMEZONE = ZoneInfo("Europe/London")


def uk_local_wall_clock_to_utc(source_wall: datetime) -> datetime:
    if source_wall.tzinfo is not None:
        raise ValueError("UK local source wall-clock datetime must be naive")
    return source_wall.replace(tzinfo=UK_LOCAL_TIMEZONE).astimezone(timezone.utc)


def ricardo_wall_clock_epoch_ms_to_utc(timestamp_ms: float) -> datetime:
    source_wall = datetime.fromtimestamp(
        timestamp_ms / 1000.0, tz=timezone.utc
    ).replace(tzinfo=None)
    return uk_local_wall_clock_to_utc(source_wall)
