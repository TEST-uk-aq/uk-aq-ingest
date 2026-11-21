"""Read the official ERG catalogue and hourly concentration feed."""

from __future__ import annotations

import math
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests

BASE_URL = "https://api.erg.ic.ac.uk/AirQuality"
CONNECTOR_CODE = "erg_laqn"
NETWORK_CODE = "laqn"
SITE_URL = BASE_URL + "/Information/MonitoringSites/GroupName=London/Json"
SPECIES_URL = BASE_URL + "/Information/MonitoringSiteSpecies/GroupName=London/Json"
GROUPS_URL = BASE_URL + "/Information/Groups/Json"
SITE_CODE = re.compile(r"[A-Z0-9][A-Z0-9_.-]{0,63}")
# ERG mass concentration channels. Other catalogue entries remain in raw evidence.
SPECIES = {"PM25": "pm25", "PM10": "pm10", "NO2": "no2"}
UOM = "ug/m3"
HOUR = timedelta(hours=1)


class SourceError(RuntimeError):
    pass


class SourceUnavailable(SourceError):
    """An access restriction or rate limit that should stop the current sweep."""


def text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def source_code(value: Any) -> str:
    code = text(value).upper()
    if not SITE_CODE.fullmatch(code):
        raise SourceError(f"Invalid ERG source code: {value!r}")
    return code


def object_rows(value: Any, label: str, *, allow_empty: bool = False) -> list[dict]:
    if isinstance(value, dict):
        value = [value]
    if value in (None, "") and allow_empty:
        return []
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise SourceError(f"Unexpected ERG {label} payload")
    if not value and not allow_empty:
        raise SourceError(f"Empty ERG {label} payload")
    return value


def gmt_time(value: Any) -> datetime | None:
    if not text(value):
        return None
    try:
        stamp = datetime.fromisoformat(text(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SourceError(f"Invalid ERG GMT timestamp: {value!r}") from exc
    if stamp.utcoffset() not in (None, timedelta(0)):
        raise SourceError(f"Non-GMT ERG timestamp: {value!r}")
    return stamp.replace(tzinfo=timezone.utc)


class ErgClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "UK-AQ-ERG-LAQN/1.0"})

    def get(self, url: str) -> dict:
        for attempt in range(2):
            try:
                # Fixed provider routes and no redirects.
                response = self.session.get(url, timeout=(5, 15), allow_redirects=False)
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt:
                    raise SourceError(f"ERG connection failed: {type(exc).__name__}") from exc
                time.sleep(2)
                continue
            if response.status_code == 429:
                raise SourceUnavailable(
                    f"ERG HTTP 429; Retry-After={response.headers.get('Retry-After', 'unspecified')}"
                )
            if response.status_code in (401, 403):
                raise SourceUnavailable(f"ERG HTTP {response.status_code}")
            if response.status_code in (502, 503, 504) and not attempt:
                header = response.headers.get("Retry-After")
                delay = 2
                if header:
                    try:
                        delay = int(header) if header.isdecimal() else (
                            parsedate_to_datetime(header) - datetime.now(timezone.utc)
                        ).total_seconds()
                    except (TypeError, ValueError):
                        raise SourceUnavailable("Unrecognised ERG Retry-After header") from None
                if delay > 5:
                    raise SourceUnavailable(f"ERG HTTP {response.status_code}; Retry-After={header}")
                time.sleep(max(0, delay))
                continue
            if response.status_code != 200:
                raise SourceError(f"ERG HTTP {response.status_code}")
            try:
                payload = response.json()
            except ValueError as exc:
                raise SourceError("ERG response is not JSON") from exc
            if not isinstance(payload, dict):
                raise SourceError("ERG response must be a JSON object")
            return payload
        raise AssertionError("Unreachable ERG request retry state")


def hourly_url(site: str, species: str, start: datetime, end: datetime, hour_label: str) -> str:
    if hour_label not in {"beginning", "ending"}:
        raise ValueError("Set hour_label from verified ERG timestamp evidence")
    shift = HOUR if hour_label == "beginning" else timedelta(0)
    start = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    return (
        f"{BASE_URL}/Data/SiteSpecies/SiteCode={source_code(site)}"
        f"/SpeciesCode={source_code(species)}/StartDate={(start - shift).date()}"
        f"/EndDate={(end - shift).date() + timedelta(days=1)}/Json"
    )


def hourly_points(
    payload: dict,
    site: str,
    species: str,
    start: datetime,
    end: datetime,
    hour_label: str,
    quality_field: str | None = None,
    invalid_values: frozenset[float] = frozenset(),
) -> tuple[list[tuple[datetime, float, str | None]], dict[str, int]]:
    raw = payload.get("RawAQData")
    if (not isinstance(raw, dict) or raw.get("@SiteCode") != site
            or raw.get("@SpeciesCode") != species):
        raise SourceError("ERG hourly response identity differs from the request")
    if hour_label not in {"beginning", "ending"}:
        raise ValueError("Set hour_label from verified ERG timestamp evidence")
    shift = HOUR if hour_label == "beginning" else timedelta(0)
    points: dict[datetime, tuple[float, str | None]] = {}
    stats = {"hours_blank": 0, "hours_invalid": 0, "hours_unknown_quality": 0}
    for row in object_rows(raw.get("Data"), "hourly Data", allow_empty=True):
        at = gmt_time(row.get("@MeasurementDateGMT"))
        if not at or at.minute or at.second or at.microsecond:
            raise SourceError("ERG concentration row has no exact GMT hour label")
        at += shift
        if not start <= at <= end:
            continue
        raw_value = row.get("@Value")
        if not text(raw_value):
            stats["hours_blank"] += 1
            continue
        try:
            value = float(raw_value) if not isinstance(raw_value, bool) else math.nan
        except (TypeError, ValueError, OverflowError):
            value = math.nan
        flag = text(row.get(quality_field)).upper() if quality_field else ""
        if not math.isfinite(value) or value in invalid_values or flag not in {"", "P", "R"}:
            stats["hours_invalid"] += 1
            continue
        stats["hours_unknown_quality"] += not flag
        point = (value, flag or None)
        if at in points and points[at] != point:
            raise SourceError("ERG hourly response contains conflicting rows for one source hour")
        points[at] = point
    return [(at, value, status) for at, (value, status) in sorted(points.items())], stats
