"""Normal ingest ownership follows Prune's whole UTC-day retention boundary."""

import os
from datetime import datetime, timedelta, timezone


def retention_days() -> int:
    raw = os.getenv("INGESTDB_RETENTION_DAYS", "")
    if not raw.isdecimal() or int(raw) < 2:
        raise ValueError("INGESTDB_RETENTION_DAYS must be configured as an integer of at least 2")
    return int(raw)


def retention_start(now: datetime, days: int) -> datetime:
    utc_now = now.astimezone(timezone.utc)
    return utc_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)


def eligible_timestamp(value: datetime | None, now: datetime, days: int) -> bool:
    if value is None or value.tzinfo is None:
        return False
    observed = value.astimezone(timezone.utc)
    return retention_start(now, days) <= observed <= now.astimezone(timezone.utc)
