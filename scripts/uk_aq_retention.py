"""Normal-ingest retention/ownership safeguards; historical maintenance is separate."""
from datetime import datetime, timedelta, timezone
import os
import re
import requests

_days = None
_ownership_floor = None

def retention_days():
    raw = os.getenv("INGESTDB_RETENTION_DAYS", "")
    if not re.fullmatch(r"[1-9][0-9]*", raw) or int(raw) < 2:
        raise ValueError("INGESTDB_RETENTION_DAYS must be configured as an integer of at least 2")
    return int(raw)

def retention_start(now, days):
    utc_now = now.astimezone(timezone.utc)
    return utc_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)

def eligible_timestamp(value, now, days):
    if value is None or value.tzinfo is None:
        return False
    observed = value.astimezone(timezone.utc)
    return retention_start(now, days) <= observed <= now.astimezone(timezone.utc)

def load_retention(connector_code):
    global _days, _ownership_floor
    raw = os.environ.get("INGESTDB_RETENTION_DAYS", "")
    if not re.fullmatch(r"[0-9]+", raw) or int(raw) < 2:
        raise ValueError("INGESTDB_RETENTION_DAYS is required and must be an integer >= 2")
    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SB_SECRET_KEY")
    if not url or not key:
        raise ValueError("Retention ownership requires SUPABASE_URL and SB_SECRET_KEY")
    response = requests.post(url.rstrip("/") + "/rest/v1/rpc/uk_aq_rpc_ingest_retention_boundaries",
        headers={"apikey": key, "Authorization": "Bearer " + key,
                 "Content-Profile": "uk_aq_public"},
        json={"p_retention_days": int(raw)}, timeout=20)
    response.raise_for_status()
    row = next((r for r in response.json() if r["connector_code"] == connector_code), None)
    if row is None or not row.get("not_before"):
        raise ValueError(f"Missing retention ownership for {connector_code}")
    _days, _ownership_floor = int(raw), datetime.fromisoformat(row["not_before"].replace("Z", "+00:00"))

def retention_floor(now=None):
    if _days is None or _ownership_floor is None:
        raise RuntimeError("Retention ownership not loaded")
    now = now or datetime.now(timezone.utc)
    return max(_ownership_floor, now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=_days))

def eligible_observation(value, now=None):
    if value is None or value == "":
        return False
    try:
        at = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if at.tzinfo is None:
            return False
    except (ValueError, TypeError, OverflowError):
        return False
    now = now or datetime.now(timezone.utc)
    return retention_floor(now) <= at <= now

def bounded_start(value, now=None):
    if value is None or value.tzinfo is None:
        raise ValueError("Invalid acquisition start timestamp")
    return max(value, retention_floor(now))

def assert_eligible_rows(rows):
    if any(not eligible_observation(row.get("observed_at")) for row in rows):
        raise RuntimeError("Observation batch crossed retention/ownership boundary; retry eligible rows")
