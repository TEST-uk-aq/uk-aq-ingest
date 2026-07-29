#!/usr/bin/env python3
"""
Fetch Breathe London Communities sensors and optionally upsert to Supabase.

Examples:
  python3 scripts/blondon_communities/blondon_communities_list_stations.py
  python3 scripts/blondon_communities/blondon_communities_list_stations.py --format csv --output uk_blondon_communities_stations.csv
  python3 scripts/blondon_communities/blondon_communities_list_stations.py --to-supabase
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from supabase import Client

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingest_helpers import station_coords
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client
from scripts.uk_aq_phenomena_rpc import upsert_phenomena_via_rpc

load_dotenv()

LOG = logging.getLogger("blondon_communities_stations")
DEFAULT_LOG_LEVEL = os.getenv("BLONDON_COMMUNITIES_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))

BLONDON_COMMUNITIES_BASE_URL = (
    os.getenv("BLONDON_COMMUNITIES_BASE_URL") or "https://api.breathelondon-communities.org/api"
).rstrip("/")
_CONFIGURED_CONNECTOR_CODE = (
    os.getenv("BLONDON_COMMUNITIES_CONNECTOR_CODE")
    or ""
).strip()
CONNECTOR_CODE_ERROR = (
    "Use connector_code=blondon_communities for Breathe London Communities. "
    "network_code/service_ref may remain breathelondon."
)
if _CONFIGURED_CONNECTOR_CODE and _CONFIGURED_CONNECTOR_CODE != "blondon_communities":
    raise RuntimeError(CONNECTOR_CODE_ERROR)
BLONDON_COMMUNITIES_CONNECTOR_CODE = "blondon_communities"
BLONDON_COMMUNITIES_SERVICE_REF = os.getenv("BLONDON_COMMUNITIES_SERVICE_REF") or "breathelondon"
BLONDON_COMMUNITIES_SERVICE_LABEL = (
    os.getenv("BLONDON_COMMUNITIES_SERVICE_LABEL") or "Breathe London Communities"
)
BLONDON_COMMUNITIES_USER_AGENT = os.getenv("BLONDON_COMMUNITIES_USER_AGENT", "uk-air-quality-networks")


UK_BBOX = {
    "west": -11.0,
    "south": 49.0,
    "east": 2.0,
    "north": 61.0,
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def load_api_key(explicit_key: Optional[str] = None) -> str:
    if explicit_key:
        return explicit_key
    env_key = os.getenv("BLONDON_COMMUNITIES_API_KEY")
    if env_key:
        return env_key.strip()
    raise RuntimeError("BLONDON_COMMUNITIES_API_KEY is required.")


def _normalize_list_sensors(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        payload = payload[0]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _station_metadata_attributes(station: Dict[str, Any]) -> Dict[str, Any]:
    attributes: Dict[str, Any] = {}
    for key, target in (
        ("Enabled", "enabled"),
        ("SiteActive", "site_active"),
        ("OrganisationName", "organisation_name"),
        ("SponsorName", "sponsor_name"),
        ("DeviceCode", "device_code"),
        ("SiteDescription", "site_description"),
        ("SitePhotoURL", "site_photo_url"),
        ("BatteryStatus", "battery_status"),
        ("BatteryPercentage", "battery_percentage"),
        ("SignalStrength", "signal_strength"),
        ("SensorsHealthStatus", "sensors_health_status"),
        ("OverallStatus", "overall_status"),
        ("PowerTag", "power_tag"),
        ("OtherTags", "other_tags"),
        ("Indoor", "indoor"),
        ("HeadHeight", "head_height"),
        ("ToRoad", "to_road"),
    ):
        value = station.get(key)
        if value is None:
            continue
        attributes[target] = value
    return attributes


def normalize_station_payload(
    station: Dict[str, Any], connector_id: int
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    site_code = _clean_str(station.get("SiteCode"))
    site_name = _clean_str(station.get("SiteName"))
    lon = station.get("Longitude")
    lat = station.get("Latitude")
    station_stub = {"properties": {"longitude": lon, "latitude": lat}}
    lon_val, lat_val = station_coords(station_stub, bbox=UK_BBOX)

    row = {
        "station_ref": site_code,
        "service_ref": BLONDON_COMMUNITIES_SERVICE_REF,
        "label": site_name or site_code or "Breathe London Station",
        "station_name": site_name,
        "station_type": _clean_str(station.get("SiteClassification")),
        "station_exposure": _clean_str(station.get("SiteLocationType")),
        "region": _clean_str(station.get("SiteGroup")),
        "geometry": (
            f"SRID=4326;POINT({lon_val} {lat_val})"
            if lon_val is not None and lat_val is not None
            else None
        ),
        "first_seen_at": _clean_str(station.get("StartDate")),
        "last_seen_at": _clean_str(station.get("LastCommunication")),
        "removed_at": _clean_str(station.get("EndDate")),
        "connector_id": connector_id,
    }
    return row, _station_metadata_attributes(station)


class BreatheLondonClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = BLONDON_COMMUNITIES_BASE_URL,
        timeout: int = 60,
        retries: int = 3,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": BLONDON_COMMUNITIES_USER_AGENT})

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        query = {"key": self.api_key}
        if params:
            query.update(params)
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, params=query, timeout=self.timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    self._sleep(attempt)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                LOG.warning("Request failed (attempt %s/%s): %s", attempt, self.retries, exc)
                if attempt == self.retries:
                    raise
                self._sleep(attempt)
        return []

    def _sleep(self, attempt: int) -> None:
        time.sleep(min(30, 2**attempt))

    def list_sensors(self) -> List[Dict[str, Any]]:
        payload = self.get("/ListSensors")
        sensors = _normalize_list_sensors(payload)
        LOG.info("Fetched %s sensors from Breathe London.", len(sensors))
        return sensors

    def get_clarity_data(
        self, site_code: str, species: str, start_time: datetime, end_time: datetime
    ) -> Any:
        start_str = quote(start_time.strftime("%a %d %b %Y %H:%M:%S GMT"))
        end_str = quote(end_time.strftime("%a %d %b %Y %H:%M:%S GMT"))
        path = f"/getClarityData/{site_code}/{species}/{start_str}/{end_str}/Hourly"
        return self.get(path)


class SupabaseWriter:
    def __init__(self) -> None:
        self.client: Client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core
        self.raw = schemas.raw
        self.public = self.client.schema(os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public")

    def upsert_connector(self) -> int:
        existing = (
            self.core.table("connectors")
            .select("id,poll_enabled")
            .eq("connector_code", BLONDON_COMMUNITIES_CONNECTOR_CODE)
            .limit(1)
            .execute()
        )
        existing_rows = existing.data if hasattr(existing, "data") else existing.get("data")
        existing_row = (
            existing_rows[0]
            if isinstance(existing_rows, list) and existing_rows
            else existing_rows
            if isinstance(existing_rows, dict)
            else None
        )
        poll_enabled = bool(existing_row.get("poll_enabled")) if isinstance(existing_row, dict) else False
        payload = {
            "connector_code": BLONDON_COMMUNITIES_CONNECTOR_CODE,
            "label": BLONDON_COMMUNITIES_SERVICE_LABEL,
            "display_name": BLONDON_COMMUNITIES_SERVICE_LABEL,
            "service_url": BLONDON_COMMUNITIES_BASE_URL,
            "stations_bbox_supported": False,
            "timeseries_station_filter_supported": False,
            "poll_enabled": poll_enabled,
        }
        self.core.table("connectors").upsert(payload, on_conflict="connector_code").execute()
        row = (
            self.core.table("connectors")
            .select("id")
            .eq("connector_code", BLONDON_COMMUNITIES_CONNECTOR_CODE)
            .single()
            .execute()
        )
        data = row.data if hasattr(row, "data") else row.get("data")
        if not data:
            raise RuntimeError("Failed to resolve connector id for Breathe London.")
        return int(data["id"])

    def fetch_connector_id(self) -> Optional[int]:
        resp = (
            self.core.table("connectors")
            .select("id")
            .eq("connector_code", BLONDON_COMMUNITIES_CONNECTOR_CODE)
            .limit(1)
            .execute()
        )
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        if not rows:
            return None
        row = rows[0] if isinstance(rows, list) else rows
        if not isinstance(row, dict):
            return None
        try:
            return int(row.get("id"))
        except (TypeError, ValueError):
            return None

    def update_connector_last_polled(self, connector_id: int) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        self.core.table("connectors").update(
            {"last_polled_at": timestamp}
        ).eq("id", connector_id).execute()

    def upsert_stations(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = [row for row in rows if row.get("station_ref")]
        if not payload:
            return 0
        self.core.table("stations").upsert(
            payload,
            on_conflict="connector_id,service_ref,station_ref",
        ).execute()
        return len(payload)

    def fetch_station_ids_by_ref(
        self,
        connector_id: int,
        service_ref: str,
        station_refs: Iterable[str],
    ) -> Dict[str, int]:
        refs = [str(ref) for ref in station_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        for chunk in chunked(refs, 200):
            resp = (
                self.core.table("stations")
                .select("id,station_ref")
                .eq("connector_id", connector_id)
                .eq("service_ref", str(service_ref))
                .in_("station_ref", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                mapping[str(row["station_ref"])] = int(row["id"])
        return mapping

    def fetch_stations(
        self,
        connector_id: int,
        service_ref: str,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        page_size = 1000
        offset = 0
        max_rows = max(0, int(limit)) if limit is not None else None
        while True:
            if max_rows is not None and len(rows) >= max_rows:
                break
            remaining = max_rows - len(rows) if max_rows is not None else page_size
            page_limit = min(page_size, remaining) if max_rows is not None else page_size
            if page_limit <= 0:
                break
            resp = (
                self.core.table("stations")
                .select("id,station_ref,station_name,label")
                .eq("connector_id", connector_id)
                .eq("service_ref", str(service_ref))
                .order("station_ref")
                .range(offset, offset + page_limit - 1)
                .execute()
            )
            batch = resp.data if hasattr(resp, "data") else resp.get("data")
            if not batch:
                break
            rows.extend(batch)
            offset += page_limit
            if len(batch) < page_limit:
                break
        return rows

    def fetch_recent_stations(
        self,
        connector_id: int,
        service_ref: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        def _fetch_rows(include_service_ref: bool) -> List[Dict[str, Any]]:
            query = (
                self.core.table("timeseries")
                .select("station_id,last_value_at")
                .eq("connector_id", connector_id)
                .filter("station_id", "not.is", "null")
                .filter("last_value_at", "not.is", "null")
                .order("last_value_at", desc=True)
                .limit(max(50, limit * 5))
            )
            if include_service_ref:
                query = query.eq("service_ref", str(service_ref))
            resp = query.execute()
            return resp.data if hasattr(resp, "data") else resp.get("data") or []

        station_ids: List[int] = []
        seen: set[int] = set()
        for include_service_ref in (True, False):
            rows = _fetch_rows(include_service_ref)
            LOG.info(
                "Recent station query include_service_ref=%s rows=%s",
                include_service_ref,
                len(rows),
            )
            for row in rows or []:
                station_id = row.get("station_id")
                last_value_at = row.get("last_value_at")
                if not station_id or not last_value_at:
                    continue
                try:
                    station_id = int(station_id)
                except (TypeError, ValueError):
                    continue
                if station_id in seen:
                    continue
                seen.add(station_id)
                station_ids.append(station_id)
                if len(station_ids) >= limit:
                    break
            if len(station_ids) >= limit:
                break
        if not station_ids:
            resp = (
                self.core.table("observations")
                .select("timeseries_id,observed_at")
                .order("observed_at", desc=True)
                .limit(max(200, limit * 20))
                .execute()
            )
            obs_rows = resp.data if hasattr(resp, "data") else resp.get("data") or []
            timeseries_ids = []
            seen_ts = set()
            for row in obs_rows:
                ts_id = row.get("timeseries_id")
                if ts_id is None:
                    continue
                try:
                    ts_id = int(ts_id)
                except (TypeError, ValueError):
                    continue
                if ts_id in seen_ts:
                    continue
                seen_ts.add(ts_id)
                timeseries_ids.append(ts_id)
                if len(timeseries_ids) >= limit * 10:
                    break
            if not timeseries_ids:
                return []
            ts_resp = (
                self.core.table("timeseries")
                .select("id,station_id")
                .eq("connector_id", connector_id)
                .in_("id", timeseries_ids)
                .execute()
            )
            ts_rows = ts_resp.data if hasattr(ts_resp, "data") else ts_resp.get("data") or []
            ts_station = {
                int(row["id"]): int(row["station_id"])
                for row in ts_rows
                if row.get("id") is not None and row.get("station_id") is not None
            }
            for ts_id in timeseries_ids:
                station_id = ts_station.get(ts_id)
                if station_id is None or station_id in seen:
                    continue
                seen.add(station_id)
                station_ids.append(station_id)
                if len(station_ids) >= limit:
                    break
        if not station_ids:
            return []
        station_resp = (
            self.core.table("stations")
            .select("id,station_ref,station_name,label")
            .in_("id", station_ids)
            .execute()
        )
        station_rows = station_resp.data if hasattr(station_resp, "data") else station_resp.get("data")
        station_by_id = {int(row["id"]): row for row in station_rows or [] if row.get("id") is not None}
        return [station_by_id[station_id] for station_id in station_ids if station_id in station_by_id]

    def fetch_station_metadata(self, station_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
        if not station_ids:
            return {}
        metadata: Dict[int, Dict[str, Any]] = {}
        for chunk in chunked([str(val) for val in station_ids], 200):
            resp = (
                self.core.table("station_metadata")
                .select("station_id,attributes")
                .in_("station_id", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                try:
                    station_id = int(row.get("station_id"))
                except (TypeError, ValueError):
                    continue
                attributes = row.get("attributes") or {}
                if isinstance(attributes, dict):
                    metadata[station_id] = attributes
        return metadata

    def upsert_station_metadata(self, attributes_by_station: Dict[int, Dict[str, Any]]) -> int:
        if not attributes_by_station:
            return 0
        existing = self.fetch_station_metadata(list(attributes_by_station.keys()))
        rows = []
        timestamp = utcnow().isoformat()
        for station_id, attributes in attributes_by_station.items():
            merged = dict(existing.get(station_id, {}))
            merged.update(attributes)
            if not merged:
                continue
            rows.append(
                {"station_id": station_id, "attributes": merged, "updated_at": timestamp}
            )
        if rows:
            self.core.table("station_metadata").upsert(rows, on_conflict="station_id").execute()
        return len(rows)

    def upsert_phenomena(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        return len(upsert_phenomena_via_rpc(self.public, payload))

    def fetch_phenomena_ids(
        self, connector_id: int, source_labels: Iterable[str]
    ) -> Dict[str, int]:
        refs = [str(ref) for ref in source_labels if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        for chunk in chunked(refs, 200):
            resp = (
                self.core.table("phenomena")
                .select("id,source_label")
                .eq("connector_id", connector_id)
                .in_("source_label", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                mapping[str(row["source_label"])] = int(row["id"])
        return mapping

    def upsert_timeseries(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        self.core.table("timeseries").upsert(
            payload, on_conflict="connector_id,service_ref,timeseries_ref"
        ).execute()
        return len(payload)

    def fetch_timeseries_ids(
        self, connector_id: int, service_ref: str, timeseries_refs: Iterable[str]
    ) -> Dict[str, int]:
        refs = [str(ref) for ref in timeseries_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        for chunk in chunked(refs, 200):
            resp = (
                self.core.table("timeseries")
                .select("id,timeseries_ref")
                .eq("connector_id", connector_id)
                .eq("service_ref", str(service_ref))
                .in_("timeseries_ref", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                mapping[str(row["timeseries_ref"])] = int(row["id"])
        return mapping

    def upsert_observations(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        self.core.table("observations").upsert(
            payload, on_conflict="timeseries_id,observed_at"
        ).execute()
        return len(payload)

    def fetch_checkpoints(
        self, station_ids: Sequence[int], species: Sequence[str]
    ) -> Dict[Tuple[int, str], Dict[str, Any]]:
        if not station_ids or not species:
            return {}
        checkpoints: Dict[Tuple[int, str], Dict[str, Any]] = {}
        for chunk in chunked([str(val) for val in station_ids], 200):
            resp = (
                self.raw.table("blondon_communities_timeseries_checkpoints")
                .select(
                    "station_id,species,timeseries_id,last_observed_at,last_polled_at,last_error"
                )
                .in_("station_id", list(chunk))
                .in_("species", list(species))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                try:
                    station_id = int(row.get("station_id"))
                except (TypeError, ValueError):
                    continue
                key = (station_id, str(row.get("species")))
                checkpoints[key] = row
        return checkpoints

    def upsert_checkpoints(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        self.raw.table("blondon_communities_timeseries_checkpoints").upsert(
            payload, on_conflict="station_id,species"
        ).execute()
        return len(payload)

    def update_timeseries_last_values(
        self, rows: Iterable[Dict[str, Any]]
    ) -> int:
        payload = [row for row in rows if row.get("id")]
        if not payload:
            return 0
        updated = 0
        for row in payload:
            update_payload = {}
            if "last_value" in row:
                update_payload["last_value"] = row["last_value"]
            if "last_value_at" in row:
                update_payload["last_value_at"] = row["last_value_at"]
            if not update_payload:
                continue
            self.core.table("timeseries").update(update_payload).eq("id", row["id"]).execute()
            updated += 1
        return updated


def chunked(values: List[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = 200
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def write_csv(output: str, stations: Iterable[Dict[str, Any]]) -> None:
    fieldnames = [
        "station_ref",
        "label",
        "station_name",
        "station_type",
        "station_exposure",
        "region",
        "longitude",
        "latitude",
        "first_seen_at",
        "last_seen_at",
        "removed_at",
    ]
    with open(output, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for station in stations:
            writer.writerow(station)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Breathe London sensors.")
    parser.add_argument(
        "--output",
        default="blondon_communities_stations.json",
        help="Output file path (default: blondon_communities_stations.json).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (json or csv).",
    )
    parser.add_argument(
        "--raw-output",
        help="Write raw sensor payloads to this file (JSON only).",
    )
    parser.add_argument(
        "--api-key",
        help="API key override (otherwise uses BLONDON_COMMUNITIES_API_KEY).",
    )
    parser.add_argument(
        "--to-supabase",
        action="store_true",
        help="Upsert stations into Supabase (requires SUPABASE_URL and SB_SECRET_KEY).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = load_api_key(args.api_key)
    client = BreatheLondonClient(api_key)
    sensors = client.list_sensors()

    if args.raw_output:
        with open(args.raw_output, "w", encoding="utf-8") as handle:
            json.dump(sensors, handle, indent=2)

    if not sensors:
        LOG.warning("No sensors returned from Breathe London.")

    if args.to_supabase:
        writer = SupabaseWriter()
        connector_id = writer.upsert_connector()
        station_rows = []
        metadata_by_ref: Dict[str, Dict[str, Any]] = {}
        for sensor in sensors:
            row, metadata = normalize_station_payload(sensor, connector_id)
            if not row.get("station_ref"):
                continue
            station_rows.append(row)
            if metadata:
                metadata_by_ref[str(row["station_ref"])] = metadata

        upserted = writer.upsert_stations(station_rows)
        LOG.info("Upserted %s stations.", upserted)
        if metadata_by_ref:
            id_map = writer.fetch_station_ids_by_ref(
                connector_id, BLONDON_COMMUNITIES_SERVICE_REF, metadata_by_ref.keys()
            )
            attributes_by_station = {
                id_map[ref]: attrs
                for ref, attrs in metadata_by_ref.items()
                if ref in id_map
            }
            if attributes_by_station:
                updated = writer.upsert_station_metadata(attributes_by_station)
                LOG.info("Upserted %s station_metadata rows.", updated)

    if args.format == "json":
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(sensors, handle, indent=2)
    else:
        csv_rows = []
        for sensor in sensors:
            csv_rows.append(
                {
                    "station_ref": sensor.get("SiteCode"),
                    "label": sensor.get("SiteName"),
                    "station_name": sensor.get("SiteName"),
                    "station_type": sensor.get("SiteClassification"),
                    "station_exposure": sensor.get("SiteLocationType"),
                    "region": sensor.get("SiteGroup"),
                    "longitude": sensor.get("Longitude"),
                    "latitude": sensor.get("Latitude"),
                    "first_seen_at": sensor.get("StartDate"),
                    "last_seen_at": sensor.get("LastCommunication"),
                    "removed_at": sensor.get("EndDate"),
                }
            )
        write_csv(args.output, csv_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
