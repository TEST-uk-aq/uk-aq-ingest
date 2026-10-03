#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.official_networks.graph_parser import (
    GraphDataError,
    parse_embedded_graph_data,
)
from scripts.official_networks.ni_parser import acquire_ni_observations
from scripts.official_networks.official_network_database import OfficialNetworkDatabase
from scripts.official_networks.profiles import (
    PROPERTY_TO_SPEC,
    OfficialNetworkProfile,
    get_profile,
)
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client


SERVICE_REF = "default"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_RETRIES = 3
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


def response_rows(response: Any) -> List[Dict[str, Any]]:
    data = response.data if hasattr(response, "data") else response.get("data")
    return [dict(row) for row in (data or [])]


def parse_iso(value: object) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalized_name(value: object) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def iter_strings(value: object) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from iter_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_strings(item)


def extract_collection(payload: object, *keys: str) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, dict)]
        if isinstance(value, dict):
            nested = extract_collection(value, *keys)
            if nested:
                return nested
    return []


def source_pollutant_from_text(value: object) -> Optional[str]:
    strings = [normalized_name(item) for item in iter_strings(value)]
    joined = " | ".join(strings)

    checks = (
        ("nox_as_no2", ("nitrogen oxides", "nox as no2", "noxasno2")),
        ("no2", ("nitrogen dioxide",)),
        ("no", ("nitrogen monoxide", "nitric oxide")),
        ("pm25", ("pm2 5", "pm25", "particulate matter less than 2 5")),
        ("pm10", ("pm10", "particulate matter less than 10")),
        ("pm1", ("pm1", "particulate matter less than 1")),
        ("o3", ("ozone",)),
        ("so2", ("sulphur dioxide", "sulfur dioxide")),
        ("co", ("carbon monoxide",)),
        ("bc", ("black carbon",)),
    )
    for code, needles in checks:
        if any(needle in joined for needle in needles):
            return code

    exact = {item.replace(" ", "") for item in strings}
    aliases = {
        "no2": "no2",
        "no": "no",
        "nox": "nox_as_no2",
        "o3": "o3",
        "so2": "so2",
        "co": "co",
        "bc": "bc",
        "pm10": "pm10",
        "pm25": "pm25",
        "pm2.5": "pm25",
        "pm1": "pm1",
    }
    for alias, code in aliases.items():
        if alias.replace(" ", "") in exact:
            return code
    return None


class HttpClient:
    def __init__(self, timeout: int = DEFAULT_TIMEOUT_SECONDS, retries: int = DEFAULT_RETRIES) -> None:
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "UK-AQ-official-network-ingest/1.0"})

    def get_text(self, url: str) -> str:
        last_error: Optional[BaseException] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                if response.status_code in RETRYABLE_STATUSES and attempt < self.retries:
                    time.sleep(min(8, 2 ** (attempt - 1)))
                    continue
                response.raise_for_status()
                return response.text
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise
                time.sleep(min(8, 2 ** (attempt - 1)))
        raise RuntimeError(f"HTTP request failed: {last_error}")

    def get_json(self, url: str, params: Optional[Mapping[str, object]] = None) -> object:
        last_error: Optional[BaseException] = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                if response.status_code in RETRYABLE_STATUSES and attempt < self.retries:
                    time.sleep(min(8, 2 ** (attempt - 1)))
                    continue
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise
                time.sleep(min(8, 2 ** (attempt - 1)))
        raise RuntimeError(f"JSON request failed: {last_error}")


class SosRestClient:
    def __init__(self, base_url: str, http: HttpClient) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http

    def get(self, path: str, params: Optional[Mapping[str, object]] = None) -> object:
        return self.http.get_json(f"{self.base_url}/{path.lstrip('/')}", params=params)

    def timeseries_catalogue(self) -> List[Dict[str, Any]]:
        payload = self.get("timeseries", {"expanded": "true"})
        return extract_collection(payload, "timeseries", "data")

    def data(self, series_id: object, start: datetime, end: datetime) -> object:
        return self.get(
            f"timeseries/{series_id}/getData",
            {
                "timespan": f"{utc_iso(start)}/{utc_iso(end)}",
                "format": "tvp",
            },
        )


def parse_source_timestamp(value: object) -> Optional[datetime]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        numeric = float(value)
        seconds = numeric / 1000.0 if abs(numeric) >= 100_000_000_000 else numeric
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    return parse_iso(value)


def extract_tvp_points(payload: object) -> List[Tuple[datetime, float]]:
    candidates: List[object] = []
    if isinstance(payload, dict):
        for key in ("values", "data", "value"):
            value = payload.get(key)
            if isinstance(value, list):
                candidates.extend(value)
        if not candidates:
            for value in payload.values():
                if isinstance(value, (dict, list)):
                    nested = extract_tvp_points(value)
                    if nested:
                        return nested
    elif isinstance(payload, list):
        candidates.extend(payload)

    points: List[Tuple[datetime, float]] = []
    for item in candidates:
        timestamp: object = None
        raw_value: object = None
        if isinstance(item, list) and len(item) >= 2:
            timestamp, raw_value = item[0], item[1]
        elif isinstance(item, dict):
            timestamp = (
                item.get("timestamp")
                or item.get("time")
                or item.get("phenomenonTime")
                or item.get("date")
            )
            raw_value = item.get("value")
        else:
            continue
        observed = parse_source_timestamp(timestamp)
        if observed is None or raw_value is None or isinstance(raw_value, bool):
            continue
        try:
            numeric = float(raw_value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(numeric):
            continue
        points.append((observed, numeric))

    points.sort(key=lambda item: item[0])
    deduped: Dict[str, Tuple[datetime, float]] = {}
    for observed, numeric in points:
        deduped[utc_iso(observed)] = (observed, numeric)
    return [deduped[key] for key in sorted(deduped)]


class PubSubPublisher:
    def __init__(self) -> None:
        self.project_id = (
            os.getenv("GCP_PROJECT_ID")
            or os.getenv("GOOGLE_CLOUD_PROJECT")
            or ""
        ).strip()
        self.observs_topic = (
            os.getenv("GCP_OBSERVS_PUBSUB_TOPIC")
            or "uk-aq-observs-observations"
        ).strip()
        self.batch_size = int(os.getenv("OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE") or "500")

    def _topic_path(self, topic: str) -> str:
        if topic.startswith("projects/"):
            return topic
        if not self.project_id:
            return ""
        return f"projects/{self.project_id}/topics/{topic}"

    @staticmethod
    def _token() -> str:
        response = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/"
            "service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"},
            timeout=10,
        )
        response.raise_for_status()
        token = response.json().get("access_token")
        if not token:
            raise RuntimeError("Metadata token response missing access_token")
        return str(token)

    def publish_observations(self, rows: Sequence[Dict[str, Any]]) -> int:
        path = self._topic_path(self.observs_topic)
        if not rows or not path:
            return 0
        token = self._token()
        count = 0
        for offset in range(0, len(rows), self.batch_size):
            messages = []
            for row in rows[offset : offset + self.batch_size]:
                attrs = {
                    key: str(row[key])
                    for key in ("connector_id", "timeseries_id", "observed_at")
                    if row.get(key) is not None
                }
                messages.append(
                    {
                        "data": base64.b64encode(
                            json.dumps(row, separators=(",", ":")).encode()
                        ).decode(),
                        "attributes": attrs,
                    }
                )
            response = requests.post(
                f"https://pubsub.googleapis.com/v1/{path}:publish",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"messages": messages},
                timeout=30,
            )
            if not response.ok:
                raise RuntimeError(
                    f"Pub/Sub publish failed for {path}: "
                    f"HTTP {response.status_code} {response.text[:500]}"
                )
            result = response.json()
            count += len(result.get("messageIds") or messages)
        return count


class ObservsWriter:
    def __init__(self, main_client: Any) -> None:
        requested = (os.getenv("OBSERVS_WRITE_MODE") or "").strip().lower()
        self.mode = requested if requested in {
            "direct", "outbox_only", "pubsub_only"
        } else "outbox_only"
        self.main_public = main_client.schema(
            os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public"
        )
        self.publisher = PubSubPublisher() if self.mode == "pubsub_only" else None
        self.direct = None
        if self.mode == "direct":
            url = (os.getenv("OBS_AQIDB_SUPABASE_URL") or "").strip()
            key = (os.getenv("OBS_AQIDB_SECRET_KEY") or "").strip()
            if not url or not key:
                raise RuntimeError(
                    "OBSERVS_WRITE_MODE=direct requires OBS_AQIDB_SUPABASE_URL "
                    "and OBS_AQIDB_SECRET_KEY"
                )
            self.direct = create_supabase_client(url, key).schema(
                os.getenv("OBS_AQIDB_RPC_SCHEMA") or "uk_aq_public"
            )

    def write(self, rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
        payload = [
            {
                "connector_id": row["connector_id"],
                "timeseries_id": row["timeseries_id"],
                "observed_at": row["observed_at"],
                "value": row["value"],
                "status": row.get("status"),
            }
            for row in rows
        ]
        if not payload:
            return {"written": 0, "enqueued": 0, "pubsub_observs": 0}
        if self.mode == "pubsub_only":
            assert self.publisher is not None
            return {
                "written": 0,
                "enqueued": 0,
                "pubsub_observs": self.publisher.publish_observations(payload),
            }
        if self.mode == "direct":
            assert self.direct is not None
            for offset in range(0, len(payload), 500):
                chunk = payload[offset : offset + 500]
                self.direct.rpc(
                    "uk_aq_rpc_observs_observations_compact_upsert_v1",
                    {
                        "timeseries_ids": [row["timeseries_id"] for row in chunk],
                        "observed_ats": [row["observed_at"] for row in chunk],
                        "values": [row["value"] for row in chunk],
                    },
                ).execute()
            return {"written": len(payload), "enqueued": 0, "pubsub_observs": 0}
        for offset in range(0, len(payload), 500):
            chunk = payload[offset : offset + 500]
            self.main_public.rpc(
                "uk_aq_rpc_observs_outbox_enqueue",
                {"entries": [{"payload": chunk}]},
            ).execute()
        return {"written": 0, "enqueued": len(payload), "pubsub_observs": 0}


class IngestDatabase:
    def __init__(self, profile: OfficialNetworkProfile) -> None:
        self.profile = profile
        requested_transport = os.getenv("OFFICIAL_NETWORK_INGESTDB_WRITE_TRANSPORT")
        self.ingestdb_write_transport = (
            "postgrest" if requested_transport is None else requested_transport.strip()
        )
        if self.ingestdb_write_transport not in {"postgrest", "database"}:
            raise RuntimeError(
                "OFFICIAL_NETWORK_INGESTDB_WRITE_TRANSPORT must be exactly "
                "postgrest or database."
            )
        self.observation_database = (
            OfficialNetworkDatabase.from_environment()
            if self.ingestdb_write_transport == "database"
            else None
        )
        self.client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core
        self.public = self.client.schema(
            os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public"
        )

    def connector(self) -> Dict[str, Any]:
        rows = response_rows(
            self.core.table("connectors")
            .select(
                "id,connector_code,default_network_id,poll_enabled,"
                "poll_window_hours,config"
            )
            .eq("connector_code", self.profile.connector_code)
            .limit(1)
            .execute()
        )
        if not rows:
            raise RuntimeError(f"Connector not found: {self.profile.connector_code}")
        row = rows[0]
        if int(row["id"]) != self.profile.connector_id:
            raise RuntimeError(
                f"{self.profile.connector_code} connector id does not match contract"
            )
        if int(row["default_network_id"]) != self.profile.network_id:
            raise RuntimeError(
                f"{self.profile.connector_code} default network does not match contract"
            )
        return row

    def stations(self, site_code: Optional[str] = None) -> List[Dict[str, Any]]:
        query = (
            self.core.table("stations")
            .select("id,station_ref,station_name,label,removed_at")
            .eq("connector_id", self.profile.connector_id)
        )
        if site_code:
            query = query.eq("station_ref", site_code.upper())
        rows = response_rows(query.execute())
        return [row for row in rows if row.get("removed_at") is None]

    def timeseries(self, station_ids: Sequence[int]) -> List[Dict[str, Any]]:
        if not station_ids:
            return []
        rows: List[Dict[str, Any]] = []
        for offset in range(0, len(station_ids), 100):
            response = (
                self.core.table("timeseries")
                .select(
                    "id,timeseries_ref,station_id,uom,ended_at,"
                    "observed_property_id,extras"
                )
                .eq("connector_id", self.profile.connector_id)
                .in_("station_id", list(station_ids[offset : offset + 100]))
                .execute()
            )
            rows.extend(response_rows(response))
        return [row for row in rows if row.get("ended_at") is None]

    def property_codes(self, property_ids: Sequence[int]) -> Dict[int, str]:
        ids = sorted(set(int(value) for value in property_ids if value is not None))
        if not ids:
            return {}
        response = (
            self.core.table("observed_properties")
            .select("id,code")
            .in_("id", ids)
            .execute()
        )
        return {int(row["id"]): str(row["code"]) for row in response_rows(response)}

    def write_observations(
        self, rows: Sequence[Dict[str, Any]], acquisition_method: str
    ) -> int:
        changed = 0
        for offset in range(0, len(rows), 500):
            chunk = list(rows[offset : offset + 500])
            arguments: Dict[str, Any] = {
                "timeseries_ids": [row["timeseries_id"] for row in chunk],
                "observed_ats": [row["observed_at"] for row in chunk],
                "values": [row["value"] for row in chunk],
                "acquisition_method": acquisition_method,
            }
            statuses = [row.get("status") for row in chunk]
            if any(status is not None for status in statuses):
                arguments["statuses"] = statuses
            if self.observation_database is not None:
                changed += self.observation_database.upsert_compact_observations_v2(
                    arguments
                )
                continue
            response = self.public.rpc(
                "uk_aq_rpc_observations_compact_upsert_v2",
                arguments,
            ).execute()
            values = response_rows(response)
            if values and values[0].get("observations_upserted") is not None:
                changed += int(values[0]["observations_upserted"])
        return changed

    def update_last_values(self, rows: Sequence[Dict[str, Any]]) -> int:
        latest: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            current = latest.get(int(row["timeseries_id"]))
            if current is None or str(row["observed_at"]) > str(current["observed_at"]):
                latest[int(row["timeseries_id"])] = row
        if not latest:
            return 0
        ordered = list(latest.values())
        changed = 0
        for offset in range(0, len(ordered), 500):
            chunk = ordered[offset : offset + 500]
            response = self.public.rpc(
                "uk_aq_rpc_timeseries_last_values_compact_update_v1",
                {
                    "timeseries_ids": [row["timeseries_id"] for row in chunk],
                    "last_values": [row["value"] for row in chunk],
                    "last_value_ats": [row["observed_at"] for row in chunk],
                },
            ).execute()
            values = response_rows(response)
            if values and values[0].get("timeseries_updated") is not None:
                changed += int(values[0]["timeseries_updated"])
        return changed


def html_observations(
    profile: OfficialNetworkProfile,
    http: HttpClient,
    stations: Sequence[Dict[str, Any]],
    timeseries_by_station: Mapping[int, Mapping[str, Dict[str, Any]]],
    start: datetime,
    end: datetime,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    observations: List[Dict[str, Any]] = []
    stats = {
        "stations_attempted": 0,
        "stations_no_recent_graph": 0,
        "stations_failed": 0,
        "series_polled": 0,
    }
    warnings: List[str] = []

    for station in stations:
        station_id = int(station["id"])
        site_code = str(station["station_ref"]).upper()
        destination = timeseries_by_station.get(station_id) or {}
        if not destination:
            continue
        stats["stations_attempted"] += 1
        try:
            html = http.get_text(profile.site_url(site_code))
            payload = parse_embedded_graph_data(html)
        except (requests.RequestException, GraphDataError) as exc:
            stats["stations_failed"] += 1
            warnings.append(f"{site_code}: {type(exc).__name__}: {exc}")
            continue
        if payload.no_recent_graph_data:
            stats["stations_no_recent_graph"] += 1
            continue
        for series in payload.series:
            target = destination.get(series.pollutant_code)
            if target is None:
                continue
            stats["series_polled"] += 1
            for point in series.points:
                observed = parse_iso(point.observed_at)
                if observed is None or observed < start or observed > end:
                    continue
                observations.append(
                    {
                        "connector_id": profile.connector_id,
                        "timeseries_id": int(target["id"]),
                        "observed_at": utc_iso(observed),
                        "value": point.value,
                        "status": None,
                    }
                )
    return observations, stats, warnings


def sos_catalogue_mapping(
    catalogue: Sequence[Dict[str, Any]],
    stations: Sequence[Dict[str, Any]],
    timeseries_by_station: Mapping[int, Mapping[str, Dict[str, Any]]],
) -> Tuple[Dict[int, object], List[str]]:
    station_keys: Dict[int, Tuple[str, str]] = {}
    for station in stations:
        station_keys[int(station["id"])] = (
            normalized_name(station.get("station_name") or station.get("label")),
            normalized_name(station.get("station_ref")),
        )

    candidates: Dict[int, List[object]] = defaultdict(list)
    for source in catalogue:
        source_id = source.get("id")
        if source_id is None:
            continue
        strings = [normalized_name(item) for item in iter_strings(source)]
        strings_no_space = {item.replace(" ", "") for item in strings if item}
        pollutant_code = source_pollutant_from_text(
            source.get("phenomenon") or source.get("label") or source
        )
        if pollutant_code is None:
            continue
        for station_id, (station_name, station_ref) in station_keys.items():
            if pollutant_code not in (timeseries_by_station.get(station_id) or {}):
                continue
            name_match = station_name and station_name in strings
            ref_match = station_ref and station_ref.replace(" ", "") in strings_no_space
            if name_match or ref_match:
                candidates[int(timeseries_by_station[station_id][pollutant_code]["id"])].append(
                    source_id
                )

    resolved: Dict[int, object] = {}
    warnings: List[str] = []
    for timeseries_id, source_ids in candidates.items():
        unique = list(dict.fromkeys(source_ids))
        if len(unique) == 1:
            resolved[timeseries_id] = unique[0]
        else:
            warnings.append(
                f"timeseries {timeseries_id}: ambiguous SOS series ids {unique[:5]}"
            )
    return resolved, warnings


def sos_observations(
    profile: OfficialNetworkProfile,
    client: SosRestClient,
    stations: Sequence[Dict[str, Any]],
    timeseries_by_station: Mapping[int, Mapping[str, Dict[str, Any]]],
    start: datetime,
    end: datetime,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    catalogue = client.timeseries_catalogue()
    if not catalogue:
        raise RuntimeError("SOS timeseries catalogue is empty")
    mapping, warnings = sos_catalogue_mapping(
        catalogue, stations, timeseries_by_station
    )
    observations: List[Dict[str, Any]] = []
    failed = 0
    for timeseries_id, source_id in mapping.items():
        try:
            payload = client.data(source_id, start, end)
            points = extract_tvp_points(payload)
        except Exception as exc:
            failed += 1
            warnings.append(
                f"timeseries {timeseries_id}: SOS data fetch failed: {exc}"
            )
            continue
        for observed, value in points:
            if observed < start or observed > end:
                continue
            observations.append(
                {
                    "connector_id": profile.connector_id,
                    "timeseries_id": timeseries_id,
                    "observed_at": utc_iso(observed),
                    "value": value,
                    "status": None,
                }
            )
    return observations, {
        "stations_attempted": len(stations),
        "stations_no_recent_graph": 0,
        "stations_failed": 0,
        "series_polled": len(mapping),
        "series_failed": failed,
    }, warnings


def dedupe_observations(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for row in rows:
        deduped[(int(row["timeseries_id"]), str(row["observed_at"]))] = row
    return sorted(
        deduped.values(),
        key=lambda row: (int(row["timeseries_id"]), str(row["observed_at"])),
    )


def run_ingest(
    connector_code: str,
    *,
    site_code: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    max_stations: Optional[int] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    profile = get_profile(connector_code)
    connector_code = profile.connector_code
    db = IngestDatabase(profile)
    connector = db.connector()
    config = connector.get("config") if isinstance(connector.get("config"), dict) else {}

    now = datetime.now(timezone.utc)
    end = parse_iso(end_time) if end_time else now
    if end is None:
        raise ValueError("Invalid --end-time")
    start = parse_iso(start_time) if start_time else end - timedelta(
        hours=int(connector.get("poll_window_hours") or 6)
    )
    if start is None:
        raise ValueError("Invalid --start-time")
    if start >= end:
        raise ValueError("start_time must be before end_time")

    stations = db.stations(site_code)
    if max_stations is not None:
        stations = stations[: max(0, max_stations)]
    if not stations:
        return {
            "ok": True,
            "connector_code": connector_code,
            "run_status": "succeeded",
            "run_message": "no_active_stations",
            "stations_updated": 0,
            "observations_upserted": 0,
            "timeseries_updated": 0,
            "series_polled": 0,
            "last_observed_at": None,
        }

    timeseries = db.timeseries([int(row["id"]) for row in stations])
    property_codes = db.property_codes(
        [int(row["observed_property_id"]) for row in timeseries if row.get("observed_property_id")]
    )
    by_station: Dict[int, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    for row in timeseries:
        property_id = row.get("observed_property_id")
        if property_id is None:
            continue
        property_code = property_codes.get(int(property_id))
        if property_code in PROPERTY_TO_SPEC:
            by_station[int(row["station_id"])][property_code] = row

    http = HttpClient()
    acquisition_method: str
    warnings: List[str] = []

    if connector_code == "ni":
        observations, stats, source_warnings = acquire_ni_observations(
            profile,
            http,
            stations,
            by_station,
            start,
            end,
            source_pollutant_from_text,
        )
        acquisition_method = "site_graph_html"
        warnings.extend(source_warnings)
    else:
        sos_enabled = bool(
            config.get("sos_probe_enabled", profile.sos_probe_enabled)
        )
        graph_supported = bool(
            config.get(
                "site_graph_html_supported", profile.site_graph_html_supported
            )
        )

        if sos_enabled:
            try:
                observations, stats, source_warnings = sos_observations(
                    profile,
                    SosRestClient(
                        str(config.get("sos_base_url") or profile.sos_base_url),
                        http,
                    ),
                    stations,
                    by_station,
                    start,
                    end,
                )
                acquisition_method = "sos"
                warnings.extend(source_warnings)
                if not observations and graph_supported:
                    raise RuntimeError("SOS returned no eligible observations")
            except Exception as exc:
                if not graph_supported:
                    raise
                warnings.append(
                    f"SOS primary unavailable: {type(exc).__name__}: {exc}"
                )
                observations, stats, source_warnings = html_observations(
                    profile, http, stations, by_station, start, end
                )
                acquisition_method = "site_graph_html"
                warnings.extend(source_warnings)
        elif graph_supported:
            observations, stats, source_warnings = html_observations(
                profile, http, stations, by_station, start, end
            )
            acquisition_method = "site_graph_html"
            warnings.extend(source_warnings)
        else:
            raise RuntimeError(
                f"{connector_code} has no enabled current observation acquisition route"
            )

    observations = dedupe_observations(observations)
    if stats.get("stations_attempted", 0) and (
        stats.get("stations_failed", 0) >= stats.get("stations_attempted", 0)
    ) and not observations:
        run_status = "failed"
        ok = False
    elif (
        stats.get("stations_failed", 0)
        or stats.get("series_failed", 0)
        or stats.get("source_failures", 0)
    ):
        run_status = "partial"
        ok = True
    else:
        run_status = "succeeded"
        ok = True

    observations_upserted = 0
    timeseries_updated = 0
    secondary = {"written": 0, "enqueued": 0, "pubsub_observs": 0}
    if not dry_run and observations:
        observations_upserted = db.write_observations(
            observations, acquisition_method
        )
        timeseries_updated = db.update_last_values(observations)
        secondary = ObservsWriter(db.client).write(observations)

    last_observed_at = (
        max(str(row["observed_at"]) for row in observations)
        if observations else None
    )
    if connector_code == "ni":
        message = (
            f"{acquisition_method}: {len(observations)} observations, "
            f"{stats.get('series_polled', 0)} series, "
            f"{stats.get('stations_no_usable_observations', 0)} stations "
            "with no usable observations, "
            f"{stats.get('stations_failed', 0)} station failures, "
            f"{stats.get('source_failures', 0)} source failures"
        )
    else:
        message = (
            f"{acquisition_method}: {len(observations)} observations, "
            f"{stats.get('series_polled', 0)} series, "
            f"{stats.get('stations_failed', 0)} station failures"
        )
    return {
        "ok": ok,
        "connector_id": profile.connector_id,
        "connector_code": connector_code,
        "network_id": profile.network_id,
        "run_status": run_status,
        "run_message": message,
        "acquisition_method": acquisition_method,
        "stations_updated": 0,
        "stations_attempted": stats.get("stations_attempted", 0),
        "stations_no_recent_graph": stats.get("stations_no_recent_graph", 0),
        **(
            {
                "stations_no_usable_observations": stats.get(
                    "stations_no_usable_observations", 0
                )
            }
            if connector_code == "ni"
            else {}
        ),
        "stations_failed": stats.get("stations_failed", 0),
        "observations_selected": len(observations),
        "observations_upserted": observations_upserted,
        "timeseries_updated": timeseries_updated,
        "series_polled": stats.get("series_polled", 0),
        "series_failed": stats.get("series_failed", 0),
        "source_failures": stats.get("source_failures", 0),
        "graph_period_days": stats.get("graph_period_days"),
        "last_observed_at": last_observed_at,
        "secondary": secondary,
        "warnings": warnings[:50],
        "window_start": utc_iso(start),
        "window_end": utc_iso(end),
        "dry_run": dry_run,
    }


def main(default_connector_code: Optional[str] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--connector-code",
        choices=("waqn", "saqn", "ni"),
        default=default_connector_code,
        required=default_connector_code is None,
    )
    parser.add_argument("--site-code")
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--max-stations", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    summary = run_ingest(
        args.connector_code,
        site_code=args.site_code,
        start_time=args.start_time,
        end_time=args.end_time,
        max_stations=args.max_stations,
        dry_run=args.dry_run,
    )
    print(
        "RUN_SUMMARY_JSON "
        + json.dumps(summary, separators=(",", ":"), sort_keys=True),
        flush=True,
    )
    return 0 if summary.get("run_status") != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
