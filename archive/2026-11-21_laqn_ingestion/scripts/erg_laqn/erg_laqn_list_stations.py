#!/usr/bin/env python3
"""
Fetch LAQN monitoring sites from the ERG AirQuality API.

Examples:
  python3 scripts/erg_laqn/erg_laqn_list_stations.py
  python3 scripts/erg_laqn/erg_laqn_list_stations.py --format csv --output laqn_stations.csv
  python3 scripts/erg_laqn/erg_laqn_list_stations.py --to-supabase
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

import requests
from dotenv import load_dotenv
from supabase import Client

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingest_helpers import station_in_bbox_or_missing_coords
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client

load_dotenv()

LOG = logging.getLogger("erg_laqn_stations")
DEFAULT_LOG_LEVEL = os.getenv("LAQN_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))

LAQN_BASE_URL = (os.getenv("LAQN_BASE_URL") or "https://api.erg.ic.ac.uk/AirQuality").rstrip("/")
LAQN_CONNECTOR_CODE = os.getenv("LAQN_CONNECTOR_CODE") or "erg_laqn"
LAQN_SERVICE_REF = os.getenv("LAQN_SERVICE_REF") or LAQN_CONNECTOR_CODE
LAQN_CONNECTOR_LABEL = (
    os.getenv("LAQN_CONNECTOR_LABEL")
    or os.getenv("LAQN_SERVICE_LABEL")
    or "ERG London Air"
)
LAQN_CONNECTOR_DISPLAY_NAME = (
    os.getenv("LAQN_CONNECTOR_DISPLAY_NAME") or "London Air LAQN"
)
LAQN_SERVICE_LABEL = LAQN_CONNECTOR_LABEL
LAQN_USER_AGENT = os.getenv("LAQN_USER_AGENT", "uk-air-quality-networks")
LAQN_MONITORING_SITES_PATHS = os.getenv("LAQN_MONITORING_SITES_PATHS")
LAQN_DEFAULT_GROUP = os.getenv("LAQN_DEFAULT_GROUP") or "London"
LAQN_TIMESERIES_SPECIES = os.getenv("LAQN_TIMESERIES_SPECIES") or "NO2,PM10,PM25,O3"

SPECIES_CONFIG: Dict[str, Dict[str, str]] = {
    "NO2": {"label": "NO2", "uom": "ug/m3"},
    "PM10": {"label": "PM10", "uom": "ug/m3"},
    "PM25": {"label": "PM2.5", "uom": "ug/m3"},
    "O3": {"label": "O3", "uom": "ug/m3"},
    "SO2": {"label": "SO2", "uom": "ug/m3"},
    "CO": {"label": "CO", "uom": "mg/m3"},
}

UK_BBOX = {
    "west": -11.0,
    "south": 49.0,
    "east": 2.0,
    "north": 61.0,
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text


def _normalize_key(key: Any) -> str:
    text = str(key)
    if text.startswith("@"):
        text = text[1:]
    return text.lower()


def _lowered_keys(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {_normalize_key(key): value for key, value in payload.items()}


def _pick_value(payload: Dict[str, Any], keys: Sequence[str]) -> Optional[Any]:
    if not payload:
        return None
    lowered = _lowered_keys(payload)
    for key in keys:
        if key in payload:
            return payload.get(key)
        at_key = f"@{key}"
        if at_key in payload:
            return payload.get(at_key)
        lowered_key = _normalize_key(key)
        if lowered_key in lowered:
            return lowered.get(lowered_key)
    return None


def _station_coords(station: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    lon = _coerce_float(_pick_value(station, ["Longitude", "Lon", "Lng", "Easting"]))
    lat = _coerce_float(_pick_value(station, ["Latitude", "Lat", "Northing"]))
    return lon, lat


def _parse_species_list(value: Optional[str]) -> List[str]:
    if not value:
        return []
    parts = [item.strip().upper() for item in value.split(",") if item.strip()]
    return [item for item in parts if item]


def _normalize_station_payload(
    station: Dict[str, Any], connector_id: int
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    station_ref = _clean_text(_pick_value(station, ["SiteCode", "SiteID", "SiteId", "Site"]))
    label = _clean_text(_pick_value(station, ["SiteName", "Label", "Name"]))
    station_name = label or station_ref
    lon, lat = _station_coords(station)
    station_type = _clean_text(_pick_value(station, ["SiteType", "SiteClassification", "Type"]))
    station_exposure = _clean_text(
        _pick_value(station, ["LocationType", "SiteLocation", "SiteLocationType"])
    )
    region = _clean_text(
        _pick_value(station, ["LocalAuthority", "Borough", "Region", "LocalAuthorityName"])
    )
    first_seen_at = _parse_date(
        _pick_value(station, ["StartDate", "SiteStartDate", "SiteSetupDate"])
    )
    last_seen_at = _parse_date(
        _pick_value(station, ["LastUpdated", "LastCommunication", "LastSeen"])
    )
    removed_at = _parse_date(
        _pick_value(station, ["EndDate", "SiteEndDate", "DateClosed"])
    )

    row = {
        "station_ref": station_ref,
        "service_ref": LAQN_SERVICE_REF,
        "label": label or station_ref or "LAQN Station",
        "station_name": station_name,
        "station_type": station_type,
        "station_exposure": station_exposure,
        "region": region,
        "geometry": (
            f"SRID=4326;POINT({lon} {lat})"
            if lon is not None and lat is not None
            else None
        ),
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
        "removed_at": removed_at,
        "connector_id": connector_id,
    }

    attributes: Dict[str, Any] = {}
    for source_key, target_key in (
        ("SiteType", "site_type"),
        ("SiteClassification", "site_classification"),
        ("SiteLocationType", "site_location_type"),
        ("SiteStatus", "site_status"),
        ("LocalAuthority", "local_authority"),
        ("Borough", "borough"),
        ("Operator", "operator"),
        ("Network", "network"),
    ):
        value = _pick_value(station, [source_key])
        if value is not None:
            attributes[target_key] = value

    return row, attributes


def _extract_station_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("Sites", "sites", "MonitoringSites", "monitoringSites", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
            if isinstance(value, dict):
                sites = value.get("Site") or value.get("site")
                if isinstance(sites, list):
                    return [row for row in sites if isinstance(row, dict)]
                if isinstance(sites, dict):
                    return [sites]
        sites = payload.get("Site") or payload.get("site")
        if isinstance(sites, list):
            return [row for row in sites if isinstance(row, dict)]
        if isinstance(sites, dict):
            return [sites]
    return []


def _parse_paths(config_value: Optional[str], defaults: Sequence[str]) -> List[str]:
    if config_value:
        return [item.strip() for item in config_value.split(",") if item.strip()]
    return list(defaults)


class LaqnClient:
    def __init__(self, base_url: str = LAQN_BASE_URL, timeout: int = 60, retries: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": LAQN_USER_AGENT, "Accept": "application/json"}
        )
        self.monitoring_sites_paths = _parse_paths(
            LAQN_MONITORING_SITES_PATHS,
            (
                "GetMonitoringSitesJson",
                "MonitoringSites/Json",
                "MonitoringSitesJson",
                "GetMonitoringSites",
                "MonitoringSites",
            ),
        )

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code in (403, 404):
                    raise requests.HTTPError(
                        f"HTTP {resp.status_code} for {url}", response=resp
                    )
                if resp.status_code in (429, 500, 502, 503, 504):
                    self._sleep(attempt)
                    continue
                resp.raise_for_status()
                try:
                    return resp.json()
                except ValueError as exc:
                    snippet = " ".join((resp.text or "").split())
                    snippet = snippet[:200]
                    message = f"Non-JSON response for {url} (status {resp.status_code})"
                    if snippet:
                        message = f"{message}: {snippet}"
                    raise requests.RequestException(message) from exc
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (403, 404):
                    raise
                LOG.warning("Request failed (attempt %s/%s): %s", attempt, self.retries, exc)
                if attempt == self.retries:
                    raise
                self._sleep(attempt)
            except requests.RequestException as exc:
                LOG.warning("Request failed (attempt %s/%s): %s", attempt, self.retries, exc)
                if attempt == self.retries:
                    raise
                self._sleep(attempt)
        return []

    def _sleep(self, attempt: int) -> None:
        time.sleep(min(30, 2**attempt))

    def monitoring_sites(self, group: Optional[str] = None) -> List[Dict[str, Any]]:
        group = group or LAQN_DEFAULT_GROUP
        params = {"GroupName": group} if group else None
        path_candidates: List[Tuple[str, Optional[Dict[str, Any]]]] = [
            (f"Information/MonitoringSites/GroupName={group}/Json", None),
            (f"Information/MonitoringSites/GroupName={group}", None),
        ]
        for path in self.monitoring_sites_paths:
            path_candidates.append((path, params))
        last_error: Optional[Exception] = None
        for path, path_params in path_candidates:
            try:
                payload = self.get(path, params=path_params)
            except requests.HTTPError as exc:
                last_error = exc
                LOG.warning("Monitoring site fetch failed for %s: %s", path, exc)
                continue
            except requests.RequestException as exc:
                last_error = exc
                LOG.warning("Monitoring site fetch failed for %s: %s", path, exc)
                continue
            stations = _extract_station_list(payload)
            if stations:
                LOG.info("Fetched %s monitoring sites via %s", len(stations), path)
                return stations
        if last_error:
            raise last_error
        return []


def chunked(values: List[Any], size: int) -> Iterable[List[Any]]:
    if size <= 0:
        size = 200
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


class SupabaseWriter:
    def __init__(self) -> None:
        self.client: Client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core

    def upsert_connector(self) -> int:
        existing = (
            self.core.table("connectors")
            .select("id,poll_enabled")
            .eq("connector_code", LAQN_CONNECTOR_CODE)
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
            "connector_code": LAQN_CONNECTOR_CODE,
            "label": LAQN_CONNECTOR_LABEL,
            "display_name": LAQN_CONNECTOR_DISPLAY_NAME,
            "service_url": LAQN_BASE_URL,
            "stations_bbox_supported": False,
            "timeseries_station_filter_supported": False,
            "poll_enabled": poll_enabled,
        }
        self.core.table("connectors").upsert(payload, on_conflict="connector_code").execute()
        row = (
            self.core.table("connectors")
            .select("id")
            .eq("connector_code", LAQN_CONNECTOR_CODE)
            .single()
            .execute()
        )
        data = row.data if hasattr(row, "data") else row.get("data")
        if not data:
            raise RuntimeError("Failed to resolve connector id for LAQN.")
        return int(data["id"])

    def upsert_stations(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = [row for row in rows if row.get("station_ref")]
        if not payload:
            return 0
        self.core.table("stations").upsert(
            payload, on_conflict="connector_id,service_ref,station_ref"
        ).execute()
        return len(payload)

    def upsert_timeseries(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = [row for row in rows if row.get("timeseries_ref")]
        if not payload:
            return 0
        self.core.table("timeseries").upsert(
            payload, on_conflict="connector_id,service_ref,timeseries_ref"
        ).execute()
        return len(payload)

    def fetch_station_ids_by_ref(
        self, connector_id: int, service_ref: str, station_refs: Iterable[str]
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


def _filter_by_bbox(stations: List[Dict[str, Any]], skip_bbox: bool) -> List[Dict[str, Any]]:
    if skip_bbox:
        return stations
    filtered = []
    for station in stations:
        lon, lat = _station_coords(station)
        station_stub = {"properties": {"longitude": lon, "latitude": lat}}
        if station_in_bbox_or_missing_coords(station_stub, UK_BBOX):
            filtered.append(station)
    return filtered


def _write_csv(output: str, rows: Iterable[Dict[str, Any]]) -> None:
    fieldnames = [
        "station_ref",
        "label",
        "station_name",
        "station_type",
        "station_exposure",
        "region",
        "longitude",
        "latitude",
        "service_ref",
        "first_seen_at",
        "last_seen_at",
        "removed_at",
    ]
    with open(output, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch LAQN monitoring sites from ERG API.")
    parser.add_argument(
        "--output",
        default="erg_laqn_stations.json",
        help="Output file path (default: erg_laqn_stations.json).",
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
        "--group",
        default=LAQN_DEFAULT_GROUP,
        help=(
            "GroupName filter (default: London). "
            "LAQN monitoring sites are listed under the London group."
        ),
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
    parser.add_argument(
        "--skip-station-metadata",
        action="store_true",
        help="Skip station_metadata upserts when writing to Supabase.",
    )
    parser.add_argument(
        "--skip-timeseries",
        action="store_true",
        help="Skip seeding timeseries rows for each station/species.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = LaqnClient()
    stations = client.monitoring_sites(group=args.group)
    if not stations:
        LOG.warning("No stations returned from LAQN monitoring sites API.")

    filtered = _filter_by_bbox(stations, args.no_filter)
    LOG.info("LAQN stations=%s (from total=%s)", len(filtered), len(stations))

    if args.raw_output:
        with open(args.raw_output, "w", encoding="utf-8") as handle:
            json.dump(stations, handle, indent=2)

    if args.to_supabase:
        writer = SupabaseWriter()
        connector_id = writer.upsert_connector()
        station_rows = []
        metadata_by_ref: Dict[str, Dict[str, Any]] = {}
        for station in filtered:
            row, metadata = _normalize_station_payload(station, connector_id)
            if not row.get("station_ref"):
                continue
            station_rows.append(row)
            if metadata:
                metadata_by_ref[str(row["station_ref"])] = metadata

        upserted = writer.upsert_stations(station_rows)
        LOG.info("Upserted %s stations.", upserted)
        if metadata_by_ref and not args.skip_station_metadata:
            id_map = writer.fetch_station_ids_by_ref(
                connector_id, LAQN_SERVICE_REF, metadata_by_ref.keys()
            )
            attributes_by_station = {
                id_map[ref]: attrs
                for ref, attrs in metadata_by_ref.items()
                if ref in id_map
            }
            if attributes_by_station:
                updated = writer.upsert_station_metadata(attributes_by_station)
                LOG.info("Upserted %s station_metadata rows.", updated)

        if station_rows and not args.skip_timeseries:
            species_list = _parse_species_list(LAQN_TIMESERIES_SPECIES)
            if not species_list:
                LOG.warning("No LAQN_TIMESERIES_SPECIES provided; skipping timeseries seed.")
            else:
                station_ids = writer.fetch_station_ids_by_ref(
                    connector_id,
                    LAQN_SERVICE_REF,
                    [row.get("station_ref") for row in station_rows if row.get("station_ref")],
                )
                timeseries_rows: List[Dict[str, Any]] = []
                for row in station_rows:
                    station_ref = row.get("station_ref")
                    if not station_ref:
                        continue
                    station_id = station_ids.get(str(station_ref))
                    if not station_id:
                        continue
                    station_name = row.get("station_name") or row.get("label") or station_ref
                    for species in species_list:
                        config = SPECIES_CONFIG.get(species, {"label": species, "uom": "ug/m3"})
                        timeseries_rows.append(
                            {
                                "timeseries_ref": f"{station_ref}:{species}",
                                "label": f"{station_name} {config['label']}",
                                "uom": config.get("uom"),
                                "station_id": station_id,
                                "service_ref": LAQN_SERVICE_REF,
                                "connector_id": connector_id,
                                "extras": {"site_code": station_ref, "species": species},
                            }
                        )
                upserted_ts = writer.upsert_timeseries(timeseries_rows)
                LOG.info("Upserted %s timeseries rows.", upserted_ts)

    if args.format == "json":
        payload = {
            "generated_at": utcnow().isoformat(),
            "station_count": len(filtered),
            "stations": [
                {
                    **_normalize_station_payload(station, connector_id=0)[0],
                    "connector_id": None,
                }
                for station in filtered
            ],
        }
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    else:
        csv_rows = []
        for station in filtered:
            row, _ = _normalize_station_payload(station, connector_id=0)
            lon, lat = _station_coords(station)
            csv_rows.append(
                {
                    "station_ref": row.get("station_ref"),
                    "label": row.get("label"),
                    "station_name": row.get("station_name"),
                    "station_type": row.get("station_type"),
                    "station_exposure": row.get("station_exposure"),
                    "region": row.get("region"),
                    "longitude": lon,
                    "latitude": lat,
                    "service_ref": LAQN_SERVICE_REF,
                    "first_seen_at": row.get("first_seen_at"),
                    "last_seen_at": row.get("last_seen_at"),
                    "removed_at": row.get("removed_at"),
                }
            )
        _write_csv(args.output, csv_rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
