#!/usr/bin/env python3
"""
Fetch Sensor.Community stations and filter to the UK bounding box.

Examples:
  python3 scripts/sensorcommunity/sensorcommunity_list_stations.py
  python3 scripts/sensorcommunity/sensorcommunity_list_stations.py --format csv --output uk_sensorcommunity_stations.csv
  python3 scripts/sensorcommunity/sensorcommunity_list_stations.py --to-supabase
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

warnings.filterwarnings(
    "ignore",
    message="urllib3 v2 only supports OpenSSL 1.1.1\\+",
    category=Warning,
    module="urllib3",
)

import requests
from dotenv import load_dotenv
from supabase import Client

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingest_helpers import station_coords, station_in_bbox_or_missing_coords
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client

load_dotenv()

LOG = logging.getLogger("sensorcommunity_stations")
DEFAULT_LOG_LEVEL = os.getenv("SCOMM_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))

SCOMM_BASE_URL = (os.getenv("SCOMM_BASE_URL") or "https://data.sensor.community").rstrip("/")
SCOMM_CONNECTOR_CODE = (
    os.getenv("SCOMM_CONNECTOR_CODE")
    or os.getenv("SCOMM_CONNECTOR_REF")
    or os.getenv("SCOMM_SERVICE_REF")
    or "sensorcommunity"
)
SCOMM_SERVICE_REF = os.getenv("SCOMM_SERVICE_REF") or SCOMM_CONNECTOR_CODE
SCOMM_SERVICE_LABEL = (
    os.getenv("SCOMM_SERVICE_LABEL")
    or os.getenv("SCOMM_CONNECTOR_LABEL")
    or "Sensor.Community"
)
SCOMM_COUNTRY = os.getenv("SCOMM_COUNTRY", "GB")
SCOMM_USER_AGENT = os.getenv(
    "SCOMM_USER_AGENT",
    "uk-air-quality-networks",
)

UK_BBOX = {
    "west": -11.0,
    "south": 49.0,
    "east": 2.0,
    "north": 61.0,
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SensorCommunityClient:
    def __init__(self, base_url: str = SCOMM_BASE_URL, timeout: int = 60, retries: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": SCOMM_USER_AGENT})

    def get(self, path: str) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
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

    def stations(self) -> List[Dict[str, Any]]:
        payload = self.get(f"/airrohr/v1/filter/country={SCOMM_COUNTRY}")
        if isinstance(payload, list):
            LOG.info("Fetched %s station payloads from Sensor.Community.", len(payload))
            return payload
        return []


class SupabaseWriter:
    def __init__(self) -> None:
        self.client: Client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core

    def upsert_connector(self) -> Tuple[int, bool]:
        existing = (
            self.core.table("connectors")
            .select("id,poll_enabled,overwrite_station_name")
            .eq("connector_code", SCOMM_CONNECTOR_CODE)
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
            "connector_code": SCOMM_CONNECTOR_CODE,
            "label": SCOMM_SERVICE_LABEL,
            "display_name": SCOMM_SERVICE_LABEL,
            "service_url": SCOMM_BASE_URL,
            "overwrite_station_name": False,
            "stations_bbox_supported": False,
            "timeseries_station_filter_supported": False,
            "poll_enabled": poll_enabled,
        }
        self.core.table("connectors").upsert(payload, on_conflict="connector_code").execute()
        row = (
            self.core.table("connectors")
            .select("id,overwrite_station_name")
            .eq("connector_code", SCOMM_CONNECTOR_CODE)
            .single()
            .execute()
        )
        data = row.data if hasattr(row, "data") else row.get("data")
        if not data:
            raise RuntimeError("Failed to resolve connector id for Sensor.Community.")
        overwrite_station_name = data.get("overwrite_station_name")
        return int(data["id"]), bool(overwrite_station_name)

    def fetch_station_names(
        self, connector_id: int, service_ref: str, station_refs: Iterable[str]
    ) -> Dict[str, Optional[str]]:
        refs = [str(ref) for ref in station_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, Optional[str]] = {}
        for chunk in chunked(refs, 200):
            resp = (
                self.core.table("stations")
                .select("station_ref,station_name")
                .eq("connector_id", connector_id)
                .eq("service_ref", str(service_ref))
                .in_("station_ref", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                mapping[str(row.get("station_ref"))] = row.get("station_name")
        return mapping

    def upsert_stations(
        self,
        stations: Iterable[Dict[str, Any]],
        connector_id: int,
        service_ref: str,
        overwrite_station_name: bool,
    ) -> int:
        rows_by_ref: Dict[str, Dict[str, Any]] = {}
        for station in stations:
            payload = _normalize_station_payload(station)
            station_ref = payload.get("station_ref")
            if not station_ref:
                continue
            lon = payload.get("longitude")
            lat = payload.get("latitude")
            station_ref_value = str(station_ref)
            station_name = payload.get("station_name")
            if isinstance(station_name, str) and not station_name.strip():
                station_name = None
            candidate = {
                "station_ref": station_ref_value,
                "service_ref": str(service_ref),
                "label": payload.get("label") or f"Sensor.Community {station_ref_value}",
                "station_name": station_name,
                "station_type": payload.get("station_type"),
                "station_exposure": payload.get("station_exposure"),
                "geometry": (
                    f"SRID=4326;POINT({lon} {lat})"
                    if lon is not None and lat is not None
                    else None
                ),
                "connector_id": connector_id,
                "last_seen_at": utcnow().isoformat(),
                "removed_at": None,
            }
            existing = rows_by_ref.get(station_ref_value)
            if existing is None:
                rows_by_ref[station_ref_value] = candidate
            else:
                rows_by_ref[station_ref_value] = _merge_station_row(existing, candidate)
        rows = list(rows_by_ref.values())
        if rows and not overwrite_station_name:
            existing_names = self.fetch_station_names(
                connector_id,
                service_ref,
                [row.get("station_ref") for row in rows if row.get("station_ref")],
            )
            for row in rows:
                station_ref_value = row.get("station_ref")
                existing_name = existing_names.get(str(station_ref_value))
                if isinstance(existing_name, str) and not existing_name.strip():
                    existing_name = None
                # Preserve previously curated names when overwrite_station_name is false.
                if existing_name is not None:
                    row["station_name"] = existing_name
        if rows:
            self.core.table("stations").upsert(
                rows, on_conflict="connector_id,service_ref,station_ref"
            ).execute()
        return len(rows)


def _normalize_station_payload(station: Dict[str, Any]) -> Dict[str, Any]:
    location = station.get("location") if isinstance(station.get("location"), dict) else {}
    sensor = station.get("sensor") if isinstance(station.get("sensor"), dict) else {}
    sensor_type = station.get("sensor_type") if isinstance(station.get("sensor_type"), dict) else {}
    lat = location.get("latitude")
    lon = location.get("longitude")
    station_stub = {
        "properties": {
            "latitude": lat,
            "longitude": lon,
        }
    }
    lon_val, lat_val = station_coords(station_stub, bbox=UK_BBOX)
    station_ref = sensor.get("id") or station.get("sensor_id") or station.get("id")
    label = location.get("name") or station.get("location_name")
    station_name = label
    station_type = sensor_type.get("name") or sensor_type.get("id")
    station_exposure = _station_exposure(location)
    return {
        "station_ref": str(station_ref) if station_ref is not None else None,
        "label": label,
        "station_name": station_name,
        "station_type": station_type,
        "station_exposure": station_exposure,
        "longitude": lon_val,
        "latitude": lat_val,
    }


def _station_exposure(location: Dict[str, Any]) -> Optional[str]:
    indoor = location.get("indoor")
    if indoor is None:
        return None
    if isinstance(indoor, bool):
        return "indoor" if indoor else "outdoor"
    if isinstance(indoor, (int, float)):
        if indoor == 1:
            return "indoor"
        if indoor == 0:
            return "outdoor"
        return None
    if isinstance(indoor, str):
        value = indoor.strip().lower()
        if value in {"1", "true", "yes", "y"}:
            return "indoor"
        if value in {"0", "false", "no", "n"}:
            return "outdoor"
    return None


def _merge_station_row(existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing)
    for key, value in candidate.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        merged[key] = value
    return merged


def _write_json(output: str, payload: Dict[str, Any]) -> None:
    with open(output, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _write_csv(output: str, stations: Iterable[Dict[str, Any]]) -> None:
    rows = [_normalize_station_payload(station) for station in stations]
    fieldnames = [
        "station_ref",
        "label",
        "station_name",
        "station_type",
        "station_exposure",
        "longitude",
        "latitude",
    ]
    with open(output, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Sensor.Community stations for the UK.")
    parser.add_argument(
        "--output",
        default="sensorcommunity_stations.json",
        help="Output file path (default: sensorcommunity_stations.json).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (json or csv).",
    )
    parser.add_argument(
        "--raw-output",
        help="Write raw station payloads to this file (JSON only).",
    )
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Skip the UK bounding box filter and save all stations.",
    )
    parser.add_argument(
        "--to-supabase",
        action="store_true",
        help="Upsert stations into Supabase (requires SUPABASE_URL and SB_SECRET_KEY).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_at = utcnow()
    client = SensorCommunityClient()
    stations = client.stations()
    if not stations:
        LOG.warning("No stations returned from Sensor.Community.")

    filtered = (
        stations
        if args.no_filter
        else [s for s in stations if station_in_bbox_or_missing_coords(_station_stub(s), UK_BBOX)]
    )
    missing_coords = sum(
        1
        for station in filtered
        if station_coords(_station_stub(station), bbox=UK_BBOX) == (None, None)
    )
    LOG.info(
        "Stations total=%s, uk_filtered=%s (missing coords=%s)",
        len(stations),
        len(filtered),
        missing_coords,
    )

    if args.to_supabase:
        writer = SupabaseWriter()
        connector_id, overwrite_station_name = writer.upsert_connector()
        inserted = writer.upsert_stations(
            filtered,
            connector_id,
            SCOMM_SERVICE_REF,
            overwrite_station_name,
        )
        LOG.info("Upserted %s stations into Supabase.", inserted)

    if args.format == "csv":
        _write_csv(args.output, filtered)
    else:
        raw_payload = None
        if args.raw_output:
            raw_payload = {
                "source": SCOMM_BASE_URL,
                "fetched_at": run_at.isoformat(),
                "bbox": None if args.no_filter else UK_BBOX,
                "count": len(filtered),
                "stations": filtered,
            }
            _write_json(args.raw_output, raw_payload)
        payload = {
            "source": SCOMM_BASE_URL,
            "fetched_at": run_at.isoformat(),
            "bbox": None if args.no_filter else UK_BBOX,
            "count": len(filtered),
            "connector_code": SCOMM_CONNECTOR_CODE,
            "service_ref": SCOMM_SERVICE_REF,
            "stations": [_normalize_station_payload(station) for station in filtered],
        }
        _write_json(args.output, payload)
    LOG.info("Wrote %s", args.output)


def _station_stub(station: Dict[str, Any]) -> Dict[str, Any]:
    location = station.get("location") if isinstance(station.get("location"), dict) else {}
    return {
        "properties": {
            "longitude": location.get("longitude"),
            "latitude": location.get("latitude"),
        }
    }


def chunked(values: List[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = 200
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


if __name__ == "__main__":
    main()
