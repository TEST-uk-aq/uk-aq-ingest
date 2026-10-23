#!/usr/bin/env python3
"""
Fetch Breathe London Nodes station metadata and optionally upsert to Supabase.

Examples:
  python3 scripts/blondon_nodes/blondon_nodes_list_stations.py --dry-run
  python3 scripts/blondon_nodes/blondon_nodes_list_stations.py --input-json network_info/blondon_nodes/list_sensors_sample.json --dry-run
  python3 scripts/blondon_nodes/blondon_nodes_list_stations.py --to-supabase
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional local convenience
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingest_helpers import station_coords

load_dotenv()

LOG = logging.getLogger("blondon_nodes_stations")
DEFAULT_LOG_LEVEL = os.getenv("BLONDON_NODES_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))

BLONDON_NODES_BASE_URL = (
    os.getenv("BLONDON_NODES_BASE_URL") or "https://breathe-london-7x54d7qf.ew.gateway.dev"
).rstrip("/")
_CONFIGURED_CONNECTOR_CODE = (os.getenv("BLONDON_NODES_CONNECTOR_CODE") or "").strip()
CONNECTOR_CODE_ERROR = (
    "Use connector_code=blondon_nodes for Breathe London Nodes. "
    "network_code/service_ref may remain breathelondon. Do not use connector_code=breathelondon."
)
if _CONFIGURED_CONNECTOR_CODE and _CONFIGURED_CONNECTOR_CODE != "blondon_nodes":
    raise RuntimeError(CONNECTOR_CODE_ERROR)
BLONDON_NODES_CONNECTOR_CODE = "blondon_nodes"
BLONDON_NODES_SERVICE_REF = os.getenv("BLONDON_NODES_SERVICE_REF") or "breathelondon"
BLONDON_NODES_SERVICE_LABEL = os.getenv("BLONDON_NODES_SERVICE_LABEL") or "Breathe London"
BLONDON_NETWORK_CODE = "breathelondon"
STATION_INITIAL_METADATA_TABLE = "station_initial_metadata"
assert STATION_INITIAL_METADATA_TABLE, "station_initial_metadata table name must not be blank"


def validate_station_initial_metadata_table_name(table_name: str = STATION_INITIAL_METADATA_TABLE) -> str:
    if table_name != "station_initial_metadata":
        raise RuntimeError(
            "Breathe London Nodes initial metadata must query "
            "uk_aq_core.station_initial_metadata; got blank/invalid table name "
            f"{table_name!r}."
        )
    return table_name


UK_BBOX = {"west": -11.0, "south": 49.0, "east": 2.0, "north": 61.0}

SOURCE_HISTORY_KEYS = (
    "SiteCode",
    "SiteName",
    "DeviceCode",
    "InstallationCode",
    "StartDate",
    "EndDate",
    "Latitude",
    "Longitude",
    "SensorContract",
    "SponsorName",
    "PowerTag",
)


def parse_source_datetime(value: Any) -> Optional[datetime]:
    text = clean_str(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def source_history_entry(station: Dict[str, Any]) -> Dict[str, Any]:
    return {key: station.get(key) for key in SOURCE_HISTORY_KEYS if key in station}


def dedupe_nodes_sensors_by_site_code(
    sensors: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    grouped: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
    missing_site_code: List[Tuple[int, Dict[str, Any]]] = []
    for idx, station in enumerate(sensors):
        site_code = clean_str(station.get("SiteCode"))
        if site_code:
            grouped.setdefault(site_code, []).append((idx, station))
        else:
            missing_site_code.append((idx, station))

    duplicate_site_codes = sorted(site_code for site_code, rows in grouped.items() if len(rows) > 1)
    duplicate_current_site_codes = sorted(
        site_code
        for site_code, rows in grouped.items()
        if sum(1 for _idx, row in rows if clean_str(row.get("EndDate")) is None) > 1
    )

    canonical_index_rows: List[Tuple[int, Dict[str, Any]]] = []
    for site_code in sorted(grouped):
        rows = grouped[site_code]
        current_rows = [(idx, row) for idx, row in rows if clean_str(row.get("EndDate")) is None]
        if current_rows:
            candidate_rows = current_rows
            date_key = "StartDate"
        else:
            candidate_rows = rows
            date_key = "EndDate"

        parsed_rows = [
            (parse_source_datetime(row.get(date_key)), idx, row)
            for idx, row in candidate_rows
        ]
        parseable_rows = [(dt, idx, row) for dt, idx, row in parsed_rows if dt is not None]
        unparseable_rows = [(idx, row) for dt, idx, row in parsed_rows if dt is None]
        if unparseable_rows and len(rows) > 1:
            LOG.warning(
                "Missing or unparseable %s values for duplicate Nodes SiteCode=%s; rows=%s",
                date_key,
                site_code,
                [source_history_entry(row) for idx, row in unparseable_rows],
            )
        if parseable_rows:
            _dt, canonical_idx, canonical_row = max(parseable_rows, key=lambda item: (item[0], -item[1]))
        else:
            canonical_idx, canonical_row = min(candidate_rows, key=lambda item: item[0])
            if len(rows) > 1:
                LOG.warning(
                    "Unable to parse %s values for duplicate Nodes SiteCode=%s; using first deterministic row. rows=%s",
                    date_key,
                    site_code,
                    [source_history_entry(row) for _idx, row in rows],
                )

        if len(rows) > 1:
            canonical_copy = dict(canonical_row)
            canonical_copy["source_history"] = [
                source_history_entry(row)
                for idx, row in rows
                if idx != canonical_idx
            ]
            canonical_row = canonical_copy
        canonical_index_rows.append((canonical_idx, canonical_row))

    canonical_index_rows.extend(missing_site_code)
    canonical_index_rows.sort(key=lambda item: item[0])
    canonical_rows = [row for _idx, row in canonical_index_rows]

    stats = {
        "raw_nodes_sensors": len(sensors),
        "unique_site_codes": len(grouped),
        "duplicate_site_codes": len(duplicate_site_codes),
        "canonical_rows": len(canonical_rows),
        "duplicate_current_site_codes_count": len(duplicate_current_site_codes),
        "duplicate_current_site_codes": duplicate_current_site_codes,
    }
    LOG.info(
        "raw_nodes_sensors=%s unique_site_codes=%s duplicate_site_codes=%s canonical_rows=%s",
        stats["raw_nodes_sensors"],
        stats["unique_site_codes"],
        stats["duplicate_site_codes"],
        stats["canonical_rows"],
    )
    LOG.info(
        "duplicate_current_site_codes_count=%s duplicate_current_site_codes=%s",
        stats["duplicate_current_site_codes_count"],
        ",".join(duplicate_current_site_codes) if duplicate_current_site_codes else "none",
    )
    return canonical_rows, stats


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def clean_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_api_key(explicit_key: Optional[str] = None) -> str:
    api_key = (explicit_key or os.getenv("BLONDON_NODES_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("BLONDON_NODES_API_KEY is required.")
    return api_key


def normalize_list_sensors(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ("sensors", "Sensors", "sites", "Sites", "data", "Data", "results", "Results"):
            value = payload.get(key)
            if isinstance(value, list):
                payload = value
                break
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        payload = payload[0]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def initial_metadata_attributes(station: Dict[str, Any]) -> Dict[str, Any]:
    attributes: Dict[str, Any] = {}
    for key in (
        "InstallationCode",
        "Facility",
        "SponsorName",
        "PowerTag",
        "SensorContract",
        "DeviceCode",
        "SiteCode",
        "SiteName",
        "Borough",
    ):
        if key in station and station.get(key) is not None:
            attributes[key] = station.get(key)
    source_history = station.get("source_history")
    if isinstance(source_history, list) and source_history:
        attributes["source_history"] = source_history
    return attributes


def normalize_station_payload(
    station: Dict[str, Any], connector_id: int, network_id: Optional[int]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    site_code = clean_str(station.get("SiteCode"))
    site_name = clean_str(station.get("SiteName"))
    lat = clean_float(station.get("Latitude"))
    lon = clean_float(station.get("Longitude"))
    station_stub = {"properties": {"longitude": lon, "latitude": lat}}
    lon_val, lat_val = station_coords(station_stub, bbox=UK_BBOX)

    row = {
        "connector_id": connector_id,
        "service_ref": BLONDON_NODES_SERVICE_REF,
        "station_ref": site_code,
        "label": site_name or site_code or "Breathe London Nodes station",
        "station_name": site_name,
        "station_device_ref": clean_str(station.get("DeviceCode")),
        "station_type": clean_str(station.get("SiteClassification")),
        "station_exposure": clean_str(station.get("SiteLocationType")),
        "region": clean_str(station.get("Borough")),
        "latitude": lat_val,
        "longitude": lon_val,
        "geometry": (
            f"SRID=4326;POINT({lon_val} {lat_val})"
            if lon_val is not None and lat_val is not None
            else None
        ),
        "sensor_height_m": clean_float(station.get("SensorHeightAboveGround")),
        "distance_to_road_m": clean_float(station.get("DistanceToKerb")),
        "description": clean_str(station.get("SiteDescription")),
        "photo_url": clean_str(station.get("SitePhotoURL")),
        "first_seen_at": clean_str(station.get("StartDate")),
        "removed_at": clean_str(station.get("EndDate")),
    }
    if network_id is not None:
        row["network_id"] = network_id
    return row, initial_metadata_attributes(station)


class BreatheLondonNodesClient:
    def __init__(self, api_key: str, base_url: str = BLONDON_NODES_BASE_URL, timeout: int = 60, retries: int = 3) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        import requests

        self.requests = requests
        self.session = requests.Session()
        self.session.headers.update({"X-API-KEY": api_key})

    def list_sensors(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/ListSensors"
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(30, 2**attempt))
                    continue
                resp.raise_for_status()
                sensors = normalize_list_sensors(resp.json())
                LOG.info("Fetched %s sensors from Breathe London Nodes.", len(sensors))
                return sensors
            except self.requests.RequestException as exc:
                LOG.warning("Request failed (attempt %s/%s): %s", attempt, self.retries, exc)
                if attempt == self.retries:
                    raise
                time.sleep(min(30, 2**attempt))
        return []


class SupabaseWriter:
    def __init__(self) -> None:
        from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client

        self.client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core

    def fetch_connector_id(self) -> int:
        resp = self.core.table("connectors").select("id").eq("connector_code", BLONDON_NODES_CONNECTOR_CODE).limit(1).execute()
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        if not rows:
            raise RuntimeError("Connector not found: blondon_nodes")
        return int(rows[0]["id"])

    def fetch_network_id(self) -> Optional[int]:
        resp = self.core.table("networks").select("id").eq("network_code", BLONDON_NETWORK_CODE).limit(1).execute()
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        if not rows:
            LOG.warning("Network not found: %s", BLONDON_NETWORK_CODE)
            return None
        return int(rows[0]["id"])

    def upsert_stations(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = [row for row in rows if row.get("station_ref")]
        if not payload:
            return 0
        self.core.table("stations").upsert(payload, on_conflict="connector_id,station_ref").execute()
        return len(payload)

    def fetch_station_ids_by_ref(self, connector_id: int, station_refs: Iterable[str]) -> Dict[str, int]:
        refs = [str(ref) for ref in station_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        for idx in range(0, len(refs), 200):
            chunk = refs[idx : idx + 200]
            resp = (
                self.core.table("stations")
                .select("id,station_ref")
                .eq("connector_id", connector_id)
                .eq("service_ref", BLONDON_NODES_SERVICE_REF)
                .in_("station_ref", chunk)
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                mapping[str(row["station_ref"])] = int(row["id"])
        return mapping

    @staticmethod
    def response_rows(resp: Any, table_name: str) -> List[Dict[str, Any]]:
        if hasattr(resp, "data"):
            rows = resp.data
        elif isinstance(resp, dict):
            rows = resp.get("data")
        else:
            raise RuntimeError(
                f"Expected {table_name} response to expose data rows; "
                f"got response type {type(resp).__name__}."
            )
        if rows is None:
            return []
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise RuntimeError(
                f"Expected {table_name} response data to be a list of dict rows; "
                f"got {type(rows).__name__}."
            )
        return rows

    def station_initial_metadata_table(self) -> Any:
        return self.core.table(validate_station_initial_metadata_table_name())

    def insert_initial_metadata_once(self, attributes_by_station: Dict[int, Dict[str, Any]]) -> int:
        if not attributes_by_station:
            return 0
        station_ids = list(attributes_by_station.keys())
        existing: set[int] = set()
        for idx in range(0, len(station_ids), 200):
            chunk = station_ids[idx : idx + 200]
            resp = (
                self.station_initial_metadata_table()
                .select("station_id")
                .in_("station_id", chunk)
                .execute()
            )
            rows = self.response_rows(resp, STATION_INITIAL_METADATA_TABLE)
            existing.update(int(row["station_id"]) for row in rows if row.get("station_id") is not None)
        now = utcnow().isoformat()
        rows = [
            {"station_id": station_id, "attributes": attrs, "captured_at": now, "created_at": now}
            for station_id, attrs in attributes_by_station.items()
            if station_id not in existing and attrs
        ]
        if rows:
            self.station_initial_metadata_table().insert(rows).execute()
        return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Breathe London Nodes stations.")
    parser.add_argument("--to-supabase", action="store_true", help="Upsert mapped station rows into Supabase.")
    parser.add_argument("--dry-run", action="store_true", help="Print count and first mapped rows without writing.")
    parser.add_argument("--input-json", help="Read a saved ListSensors JSON payload instead of calling the API.")
    parser.add_argument("--api-key", help="Breathe London Nodes API key override.")
    parser.add_argument("--base-url", default=BLONDON_NODES_BASE_URL, help="Nodes API base URL override.")
    parser.add_argument("--limit", type=int, help="Limit rows for diagnostics.")
    parser.add_argument("--sample-size", type=int, default=5, help="Rows to print in dry-run mode.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.input_json:
        with open(args.input_json, "r", encoding="utf-8") as handle:
            sensors = normalize_list_sensors(json.load(handle))
    else:
        client = BreatheLondonNodesClient(load_api_key(args.api_key), base_url=args.base_url.rstrip("/"))
        sensors = client.list_sensors()
    if args.limit is not None:
        sensors = sensors[: max(0, args.limit)]

    sensors, dedupe_stats = dedupe_nodes_sensors_by_site_code(sensors)

    connector_id = 0
    network_id: Optional[int] = None
    writer: Optional[SupabaseWriter] = None
    if args.to_supabase:
        writer = SupabaseWriter()
        connector_id = writer.fetch_connector_id()
        network_id = writer.fetch_network_id()

    mapped = [normalize_station_payload(station, connector_id, network_id) for station in sensors]
    station_rows = [row for row, _attrs in mapped]
    metadata_by_ref = {
        str(row["station_ref"]): attrs
        for row, attrs in mapped
        if row.get("station_ref") and attrs
    }

    if args.dry_run or not args.to_supabase:
        print(f"Fetched {dedupe_stats['raw_nodes_sensors']} Breathe London Nodes station row(s).")
        print(f"raw_nodes_sensors={dedupe_stats['raw_nodes_sensors']}")
        print(f"unique_site_codes={dedupe_stats['unique_site_codes']}")
        print(f"duplicate_site_codes={dedupe_stats['duplicate_site_codes']}")
        print(f"canonical_rows={dedupe_stats['canonical_rows']}")
        print(f"duplicate_current_site_codes_count={dedupe_stats['duplicate_current_site_codes_count']}")
        print(
            "duplicate_current_site_codes="
            + (
                ",".join(dedupe_stats["duplicate_current_site_codes"])
                if dedupe_stats["duplicate_current_site_codes"]
                else "none"
            )
        )
        print(f"Mapped {len(station_rows)} station row(s).")
        for row in station_rows[: max(0, args.sample_size)]:
            print(json.dumps(row, indent=2, sort_keys=True, default=str))
        if not args.to_supabase:
            return 0

    if writer is None:
        raise RuntimeError("writer was not initialized")
    upserted = writer.upsert_stations(station_rows)
    station_ids = writer.fetch_station_ids_by_ref(connector_id, metadata_by_ref.keys())
    attrs_by_station = {
        station_ids[station_ref]: attrs
        for station_ref, attrs in metadata_by_ref.items()
        if station_ref in station_ids
    }
    inserted_metadata = writer.insert_initial_metadata_once(attrs_by_station)
    print(
        "Breathe London Nodes stations synced: "
        f"stations={upserted}, initial_metadata_inserted={inserted_metadata}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
