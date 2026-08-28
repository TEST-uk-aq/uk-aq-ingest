#!/usr/bin/env python3
"""
Fetch UK-AIR SOS stations and filter to the UK bounding box.

Examples:
  python3 scripts/sos/sos_list_stations.py
  python3 scripts/sos/sos_list_stations.py --format csv --output uk_stations.csv
  python3 scripts/sos/sos_list_stations.py --no-filter
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

warnings.filterwarnings(
    "ignore",
    message="urllib3 v2 only supports OpenSSL 1.1.1\\+",
    category=Warning,
    module="urllib3",
)
import requests
from dotenv import load_dotenv
from supabase import Client

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingest_helpers import station_coords, station_in_bbox_or_missing_coords
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client
from scripts.uk_aq_phenomena_rpc import upsert_phenomena_via_rpc
load_dotenv()

LOG = logging.getLogger("uk_aq_stations")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SOS_BASE_URL = (
    os.getenv("SOS_BASE_URL")
    or os.getenv("UK_AIR_BASE_URL")
    or os.getenv("UKAIR_BASE_URL")
    or "https://uk-air.defra.gov.uk/sos-ukair/api/v1"
).rstrip("/")
SOS_SERVICE_LABEL = (
    os.getenv("SOS_SERVICE_LABEL")
    or os.getenv("UK_AIR_SERVICE_LABEL")
    or "SOS"
)
SOS_CONNECTOR_CODE = "sos"
PLACEHOLDER_STATION_REFS = {"9999999999"}

UK_BBOX = {
    "west": -11.0,
    "south": 49.0,
    "east": 2.0,
    "north": 61.0,
}

_STATION_LABEL_POLLUTANT_HINTS = (
    "sulphur",
    "sulfur",
    "nitrogen",
    "ozone",
    "particulate",
    "pm10",
    "pm25",
    "pm2",
    "carbon",
    "benzene",
    "toluene",
    "monoxide",
    "dioxide",
    "oxide",
    "lead",
    "so2",
    "no2",
    "no",
    "co",
)

_DASH_PATTERN = re.compile(r"[\u2010\u2011\u2012\u2013\u2014\u2212]")


def _normalize_station_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _normalize_dashes(value: str) -> str:
    return _DASH_PATTERN.sub("-", value)


def _extract_station_descriptor_from_label(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    text = _normalize_dashes(label.strip())
    if not text:
        return None
    match = re.match(r"^https?://\S+\s+\d+\s+-\s+(.*)$", text)
    if not match:
        match = re.match(r"^\S+\s+\d+\s+-\s+(.*)$", text)
    if not match:
        match = re.match(r"^\d+\s+-\s+(.*)$", text)
    if match:
        text = match.group(1)
    if "," in text:
        text = text.split(",", 1)[0]
    text = text.strip()
    return text or None


def _looks_like_pollutant_suffix(value: str) -> bool:
    normalized = _normalize_station_label(value)
    if any(hint in normalized for hint in _STATION_LABEL_POLLUTANT_HINTS):
        return True
    lowered = value.lower()
    return any(token in lowered for token in ("(air)", "micro", "aerosol"))


def _extract_station_name_from_label(label: Optional[str]) -> Optional[str]:
    text = _extract_station_descriptor_from_label(label)
    if not text:
        return None
    if " - " in text:
        candidate = text.split(" - ", 1)[0].strip()
        if candidate:
            return candidate
    if "-" in text:
        left, right = text.rsplit("-", 1)
        if _looks_like_pollutant_suffix(right):
            candidate = left.strip()
            if candidate:
                return candidate
    return text


def _derive_station_name(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    cleaned = _extract_station_name_from_label(label)
    if cleaned:
        return cleaned
    trimmed = label.strip()
    return trimmed or None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _station_type_from_payload(station: Dict[str, Any]) -> Optional[str]:
    props = station.get("properties", {}) if isinstance(station.get("properties"), dict) else {}
    return props.get("stationType") or station.get("stationType")


def _station_metadata_attributes(station: Dict[str, Any]) -> Dict[str, Any]:
    props = station.get("properties", {}) if isinstance(station.get("properties"), dict) else {}
    attributes: Dict[str, Any] = {}
    operator = props.get("operator") or station.get("operator")
    if operator:
        attributes["operator"] = operator
    status = props.get("status") or station.get("status")
    if status:
        attributes["status"] = status
    return attributes


def _resolve_station_ref(station: Dict[str, Any]) -> Optional[str]:
    props = station.get("properties", {}) if isinstance(station.get("properties"), dict) else {}
    ref = station.get("id") or props.get("id")
    if ref is None:
        return None
    return str(ref)


def _is_placeholder_station_ref(station_ref: Optional[str]) -> bool:
    if station_ref is None:
        return False
    return str(station_ref) in PLACEHOLDER_STATION_REFS


def _resolve_service_ref(
    station: Dict[str, Any],
    station_ref: Optional[str],
    station_service_ref_map: Optional[Dict[str, str]] = None,
    default_service_ref: Optional[str] = None,
) -> Optional[str]:
    props = station.get("properties", {}) if isinstance(station.get("properties"), dict) else {}
    raw_service = station.get("service") or props.get("service")
    service_ref = None
    if isinstance(raw_service, dict):
        service_ref = raw_service.get("id")
    elif raw_service is not None:
        service_ref = str(raw_service)
    if station_service_ref_map and station_ref:
        mapped = station_service_ref_map.get(str(station_ref))
        if mapped:
            service_ref = mapped
    if not service_ref and default_service_ref:
        service_ref = default_service_ref
    if service_ref is None:
        return None
    return str(service_ref)


def _index_station_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        station_ref = row.get("station_ref")
        if station_ref is None:
            continue
        grouped.setdefault(str(station_ref), []).append(row)
    return grouped


def _select_station_row(
    station_ref: str,
    service_ref: Optional[str],
    station_rows: Dict[str, List[Dict[str, Any]]],
) -> Optional[Dict[str, Any]]:
    candidates = station_rows.get(str(station_ref), [])
    if service_ref is not None:
        for row in candidates:
            if str(row.get("service_ref")) == str(service_ref):
                return row
    if len(candidates) == 1:
        return candidates[0]
    return None


class UkAirClient:
    def __init__(self, base_url: str = SOS_BASE_URL, timeout: int = 60, retries: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_label = self._request_label(path, params)
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException as exc:
                failure = f"{type(exc).__name__} {request_label}: {exc}"
                if self._retry_failed(attempt, failure):
                    continue
                raise

            if resp.status_code in (429, 500, 502, 503, 504):
                failure = f"HTTP {resp.status_code} {request_label}"
                if self._retry_failed(attempt, failure):
                    continue
                resp.raise_for_status()

            try:
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                failure = f"{type(exc).__name__} {request_label}: {exc}"
                if self._retry_failed(attempt, failure):
                    continue
                raise
        raise RuntimeError(
            f"UK-AIR SOS request ended without a response after {self.retries} attempts: "
            f"{request_label}"
        )

    def _retry_delay(self, attempt: int) -> int:
        return min(30, 2**attempt)

    def _retry_failed(self, attempt: int, failure: str) -> bool:
        if attempt == self.retries:
            LOG.error(
                "Request failed (attempt %s/%s): %s; retries exhausted.",
                attempt,
                self.retries,
                failure,
            )
            return False
        delay = self._retry_delay(attempt)
        LOG.warning(
            "Request failed (attempt %s/%s): %s; retrying in %ss.",
            attempt,
            self.retries,
            failure,
            delay,
        )
        self._sleep(attempt)
        return True

    def _sleep(self, attempt: int) -> None:
        time.sleep(self._retry_delay(attempt))

    def _request_label(self, path: str, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return path
        station_list = params.get("station") if isinstance(params, dict) else None
        if isinstance(station_list, list):
            return f"{path} (stations={len(station_list)})"
        return f"{path} (params={','.join(sorted(params.keys()))})"

    def stations(self) -> List[Dict[str, Any]]:
        params_options: List[Optional[Dict[str, Any]]] = [{"expanded": "true"}, None]
        last_error: Optional[Exception] = None
        for params in params_options:
            try:
                data = self.get("/stations", params=params)
                stations = _extract_list(data, ("stations", "data"))
                if stations:
                    LOG.info("Fetched %s stations using params=%s", len(stations), params or {})
                return stations
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 400:
                    LOG.warning("Stations query failed (400) with params=%s; trying fallback.", params)
                    last_error = exc
                    continue
                raise
        if last_error is not None:
            raise last_error
        return []

    def services(self) -> List[Dict[str, Any]]:
        data = self.get("/services")
        return _extract_list(data, ("services", "data"))

    def timeseries(
        self, station_ids: Sequence[str], service_ref: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if not station_ids:
            return []
        params: Dict[str, Any] = {"expanded": "true", "station": list(station_ids)}
        if service_ref:
            params["service"] = service_ref
        try:
            data = self.get("/timeseries", params=params)
            return _extract_list(data, ("timeseries", "data"))
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 400 and service_ref:
                params.pop("service", None)
                data = self.get("/timeseries", params=params)
                return _extract_list(data, ("timeseries", "data"))
            raise


class SupabaseWriter:
    def __init__(self) -> None:
        self.client: Client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core
        self.raw = schemas.raw
        self.public = self.client.schema(os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public")

    def upsert_connectors(self, services: Iterable[Dict[str, Any]]) -> Optional[int]:
        existing_connector_id = self.get_connector_id()
        services_list = [svc for svc in services if isinstance(svc, dict)]
        primary = _select_primary_service(services_list)
        if primary is None or primary.get("id") is None:
            if existing_connector_id is not None:
                LOG.warning(
                    "UK-AIR SOS services payload missing a usable primary service; "
                    "reusing existing connector id=%s.",
                    existing_connector_id,
                )
            return existing_connector_id
        existing = (
            self.core.table("connectors")
            .select("id,poll_enabled")
            .eq("connector_code", SOS_CONNECTOR_CODE)
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
        payload = [
            {
                "connector_code": SOS_CONNECTOR_CODE,
                "label": _normalize_service_label(primary.get("label") or primary.get("name")),
                "display_name": _normalize_service_label(
                    primary.get("label") or primary.get("name")
                ),
                "service_url": primary.get("serviceUrl") or primary.get("url") or SOS_BASE_URL,
                "poll_enabled": poll_enabled,
            }
        ]
        self.core.table("connectors").upsert(payload, on_conflict="connector_code").execute()
        return self.get_connector_id() or existing_connector_id

    def get_connector_id(self) -> Optional[int]:
        resp = (
            self.core.table("connectors")
            .select("id")
            .eq("connector_code", SOS_CONNECTOR_CODE)
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

    def fetch_station_rows(
        self,
        connector_id: int,
        station_refs: Sequence[str],
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not station_refs:
            return rows
        for chunk in _chunked(list(station_refs), 200):
            resp = (
                self.core.table("stations")
                .select("id,station_ref,service_ref,station_type")
                .eq("connector_id", connector_id)
                .in_("station_ref", list(chunk))
                .execute()
            )
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            rows.extend(data or [])
        return rows

    def flag_placeholder_stations(self, connector_id: int, station_refs: Sequence[str]) -> int:
        rows = self.fetch_station_rows(connector_id, station_refs)
        if not rows:
            return 0
        attributes_by_station = {}
        for row in rows:
            station_id = row.get("id")
            if station_id is None:
                continue
            attributes_by_station[int(station_id)] = {
                "is_placeholder": True,
                "exclude_from_ui": True,
                "placeholder_source": "sos",
            }
        return self.upsert_station_metadata(attributes_by_station)

    def fetch_station_rows_by_id(self, station_ids: Sequence[int]) -> Dict[int, str]:
        if not station_ids:
            return {}
        mapping: Dict[int, str] = {}
        for chunk in _chunked(list(station_ids), 200):
            resp = (
                self.core.table("stations")
                .select("id,station_ref")
                .in_("id", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                try:
                    station_id = int(row.get("id"))
                except (TypeError, ValueError):
                    continue
                station_ref = row.get("station_ref")
                if station_ref is not None:
                    mapping[station_id] = str(station_ref)
        return mapping

    def fetch_timeseries_rows(
        self,
        connector_id: int,
        timeseries_refs: Sequence[str],
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not timeseries_refs:
            return rows
        for chunk in _chunked(list(timeseries_refs), 200):
            resp = (
                self.core.table("timeseries")
                .select("id,timeseries_ref,station_id,service_ref")
                .eq("connector_id", connector_id)
                .in_("timeseries_ref", list(chunk))
                .execute()
            )
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            rows.extend(data or [])
        return rows

    def fetch_timeseries_rows_by_station_ids(
        self,
        connector_id: int,
        station_ids: Sequence[int],
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not station_ids:
            return rows
        for chunk in _chunked(list(station_ids), 200):
            resp = (
                self.core.table("timeseries")
                .select("id,station_id,last_value,last_value_at")
                .eq("connector_id", connector_id)
                .in_("station_id", list(chunk))
                .execute()
            )
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            rows.extend(data or [])
        return rows

    def fetch_latest_site_register_snapshot(self) -> Optional[str]:
        resp = (
            self.raw.table("sos_site_register")
            .select("snapshot_at")
            .order("snapshot_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        if not rows:
            return None
        row = rows[0] if isinstance(rows, list) else rows
        snapshot_at = row.get("snapshot_at") if isinstance(row, dict) else None
        return snapshot_at

    def fetch_site_register_rows(
        self,
        snapshot_at: str,
        page_size: int = 1000,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not snapshot_at:
            return rows
        offset = 0
        while True:
            resp = (
                self.raw.table("sos_site_register")
                .select("uk_air_ref,site_name,latitude,longitude,networks,snapshot_at")
                .eq("snapshot_at", snapshot_at)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            data = resp.data if hasattr(resp, "data") else resp.get("data")
            batch = data or []
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return rows

    def fetch_sos_networks(self) -> Dict[str, Dict[str, Any]]:
        resp = (
            self.core.table("sos_networks")
            .select("network_ref,network_code,network_display_name")
            .execute()
        )
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        networks: Dict[str, Dict[str, Any]] = {}
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            ref = row.get("network_ref")
            if ref:
                networks[str(ref)] = row
        return networks

    def fetch_sos_station_uk_air_refs(
        self, station_ids: Sequence[int]
    ) -> Dict[int, Dict[str, Any]]:
        if not station_ids:
            return {}
        refs: Dict[int, Dict[str, Any]] = {}
        for chunk in _chunked(list(station_ids), 200):
            resp = (
                self.raw.table("sos_station_uk_air_refs")
                .select("station_id,uk_air_ref,match_method,match_distance_m,source_snapshot_at")
                .in_("station_id", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                try:
                    station_id = int(row.get("station_id"))
                except (TypeError, ValueError):
                    continue
                refs[station_id] = row
        return refs

    def fetch_station_metadata(self, station_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
        if not station_ids:
            return {}
        metadata: Dict[int, Dict[str, Any]] = {}
        for chunk in _chunked(list(station_ids), 200):
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
                {
                    "station_id": station_id,
                    "attributes": merged,
                    "updated_at": timestamp,
                }
            )
        if rows:
            self.core.table("station_metadata").upsert(rows, on_conflict="station_id").execute()
        return len(rows)

    def upsert_station_types(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        self.core.table("stations").upsert(rows, on_conflict="id").execute()
        return len(rows)

    def upsert_reference_table(
        self,
        table: str,
        ref_key: str,
        items: Iterable[Dict[str, Any]],
        connector_id: int,
        default_service_ref: Optional[str] = None,
    ) -> int:
        rows = []
        for item in items:
            ref = item.get("id") or item.get(ref_key)
            label = _item_label(item)
            service_ref = _item_service_id(item) or default_service_ref
            if not ref or not label:
                continue
            if not service_ref:
                continue
            rows.append(
                {
                    ref_key: str(ref),
                    "label": label,
                    "service_ref": str(service_ref),
                    "connector_id": connector_id,
                }
            )
        if rows:
            self.core.table(table).upsert(
                rows,
                on_conflict=f"connector_id,service_ref,{ref_key}",
            ).execute()
        return len(rows)

    def upsert_phenomena(self, items: Iterable[Dict[str, Any]], connector_id: int) -> int:
        payload_by_uri: Dict[str, Dict[str, Any]] = {}
        for item in items:
            uri = item.get("source_label") or item.get("eionet_uri") or item.get("id")
            label = _item_label(item)
            if not uri or not label:
                continue
            uri_value = str(uri)
            notation = item.get("notation")
            row = payload_by_uri.get(uri_value)
            if row is None:
                payload_by_uri[uri_value] = {
                    "source_label": uri_value,
                    "label": label,
                    "notation": notation,
                    "connector_id": connector_id,
                }
                continue
            if label and (not row.get("label") or row.get("label") == uri_value):
                row["label"] = label
            if notation and not row.get("notation"):
                row["notation"] = notation
        rows = list(payload_by_uri.values())
        return len(upsert_phenomena_via_rpc(self.public, rows))

    def upsert_stations(
        self,
        stations: Iterable[Dict[str, Any]],
        connector_id: int,
        seen_at: datetime,
        station_service_ref_map: Optional[Dict[str, str]] = None,
        default_service_ref: Optional[str] = None,
    ) -> int:
        seen_at_value = seen_at.isoformat()
        rows = []
        skipped_missing_ref: List[Dict[str, Any]] = []
        skipped_missing_service: List[Dict[str, Any]] = []
        skipped_limit = 10
        for station in stations:
            props = station.get("properties", {}) if isinstance(station.get("properties"), dict) else {}
            station_ref = _resolve_station_ref(station)
            if not station_ref:
                if len(skipped_missing_ref) < skipped_limit:
                    skipped_missing_ref.append(
                        {
                            "id": station.get("id") or props.get("id"),
                            "label": station.get("label") or props.get("label") or station.get("name"),
                            "service": station.get("service") or props.get("service"),
                        }
                    )
                continue
            lon, lat = station_coords(station, bbox=UK_BBOX)
            service_ref = _resolve_service_ref(
                station,
                station_ref,
                station_service_ref_map,
                default_service_ref,
            )
            if not service_ref:
                if len(skipped_missing_service) < skipped_limit:
                    skipped_missing_service.append(
                        {
                            "id": station_ref,
                            "label": station.get("label") or props.get("label") or station.get("name"),
                            "service": raw_service,
                        }
                    )
                continue
            label = station.get("label") or props.get("label") or station.get("name")
            station_name = _derive_station_name(label)
            row = {
                "station_ref": str(station_ref),
                "service_ref": str(service_ref),
                "label": label,
                "station_type": _station_type_from_payload(station),
                "region": props.get("region") or station.get("region"),
                "geometry": f"SRID=4326;POINT({lon} {lat})" if lon is not None and lat is not None else None,
                "connector_id": connector_id,
                "last_seen_at": seen_at_value,
                "removed_at": None,
            }
            if station_name:
                row["station_name"] = station_name
            rows.append(row)
        if rows:
            self.core.table("stations").upsert(
                rows,
                on_conflict="connector_id,service_ref,station_ref",
            ).execute()
        if skipped_missing_ref:
            LOG.warning(
                "Skipped %s stations missing station_ref. Examples=%s",
                len(skipped_missing_ref),
                json.dumps(skipped_missing_ref, ensure_ascii=True),
            )
        if skipped_missing_service:
            LOG.warning(
                "Skipped %s stations missing service_ref. Examples=%s",
                len(skipped_missing_service),
                json.dumps(skipped_missing_service, ensure_ascii=True),
            )
        return len(rows)

    def backfill_station_names(self, connector_ids: Sequence[int]) -> int:
        if not connector_ids:
            return 0
        resp = (
            self.core.table("stations")
            .select("id,station_ref,label,service_ref,connector_id")
            .in_("connector_id", list(connector_ids))
            .is_("station_name", "null")
            .execute()
        )
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        updates = []
        for row in rows or []:
            label = row.get("label")
            station_name = _derive_station_name(label)
            if station_name:
                updates.append(
                    {
                        "id": row.get("id"),
                        "station_ref": row.get("station_ref"),
                        "service_ref": row.get("service_ref"),
                        "label": label,
                        "connector_id": row.get("connector_id"),
                        "station_name": station_name,
                    }
                )
        if updates:
            self.core.table("stations").upsert(updates, on_conflict="id").execute()
        return len(updates)

    def mark_removed(self, seen_at: datetime, connector_ids: Sequence[int]) -> None:
        if not connector_ids:
            return
        seen_at_value = seen_at.isoformat()
        self.core.table("stations").update({"removed_at": seen_at_value}).in_(
            "connector_id", list(connector_ids)
        ).is_("removed_at", "null").lt("last_seen_at", seen_at_value).execute()


def _extract_list(payload: Any, keys: Sequence[str]) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            items = payload.get(key)
            if isinstance(items, list):
                return items
    return []


def _normalize_service_label(label: Optional[str]) -> Optional[str]:
    if label is None:
        return SOS_SERVICE_LABEL
    trimmed = label.strip()
    if not trimmed:
        return SOS_SERVICE_LABEL
    if trimmed.lower().startswith("my timeseries service"):
        return SOS_SERVICE_LABEL
    return trimmed


def _select_primary_service(services: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for svc in services:
        if str(svc.get("id")) == "1":
            return svc
    for svc in services:
        label = str(svc.get("label") or svc.get("name") or "").lower()
        if "uk" in label and "air" in label:
            return svc
    return services[0] if services else None


def _item_label(item: Dict[str, Any]) -> Optional[str]:
    return (
        item.get("label")
        or item.get("name")
        or item.get("title")
        or item.get("notation")
        or item.get("source_label")
        or item.get("eionet_uri")
    )


def _item_service_id(item: Dict[str, Any]) -> Optional[str]:
    service = item.get("service")
    if isinstance(service, dict):
        return service.get("id")
    if service is not None:
        return str(service)
    return None


def _collect_reference(store: Dict[str, Dict[str, Any]], item: Any) -> None:
    if not isinstance(item, dict):
        return
    item_id = item.get("id")
    label = _item_label(item)
    if not item_id or not label:
        return
    store[item_id] = item


def _chunked(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    if size <= 0:
        size = 50
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _station_service_map_from_timeseries(
    client: UkAirClient,
    station_ids: Sequence[str],
    service_refs: Sequence[str],
    batch_size: int,
) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    if not station_ids:
        return mapping
    service_list = list(service_refs) if service_refs else [None]
    for chunk in _chunked(list(station_ids), batch_size):
        for service_ref in service_list:
            series = client.timeseries(chunk, service_ref=service_ref)
            for ts in series:
                station = ts.get("station")
                if isinstance(station, dict):
                    station_id = station.get("id")
                else:
                    station_id = station
                service = ts.get("service")
                if isinstance(service, dict):
                    svc_id = service.get("id")
                else:
                    svc_id = service
                if not station_id or not svc_id:
                    continue
                station_key = str(station_id)
                svc_key = str(svc_id)
                if station_key in mapping and mapping[station_key] != svc_key:
                    LOG.warning(
                        "Station %s maps to multiple services (%s, %s)",
                        station_key,
                        mapping[station_key],
                        svc_key,
                    )
                    continue
                mapping[station_key] = svc_key
    return mapping


def _normalize_station(
    station: Dict[str, Any],
    service_ref_map: Optional[Dict[str, str]] = None,
    default_service_ref: Optional[str] = None,
    timeseries_summary: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    props = station.get("properties", {}) if isinstance(station.get("properties"), dict) else {}
    lon, lat = station_coords(station, bbox=UK_BBOX)
    timeseries = props.get("timeseries") if isinstance(props.get("timeseries"), list) else []
    timeseries_ids = []
    for entry in timeseries:
        if isinstance(entry, dict):
            ts_id = entry.get("id")
            if ts_id:
                timeseries_ids.append(ts_id)
        elif entry is not None:
            timeseries_ids.append(str(entry))
    station_ref = _resolve_station_ref(station)
    service_ref = _resolve_service_ref(station, station_ref, service_ref_map, default_service_ref)
    label = station.get("label") or props.get("label") or station.get("name")
    station_name = _derive_station_name(label)
    payload = {
        "station_ref": station_ref,
        "label": label,
        "station_name": station_name,
        "station_type": _station_type_from_payload(station),
        "region": props.get("region") or station.get("region"),
        "longitude": lon,
        "latitude": lat,
        "service_ref": service_ref,
        "timeseries_refs": timeseries_ids or None,
    }
    if timeseries_summary and station_ref:
        summary = timeseries_summary.get(str(station_ref))
        if summary:
            payload.update(summary)
    return payload


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        cleaned = value.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except (TypeError, ValueError):
        return None


def build_timeseries_summary(
    writer: "SupabaseWriter",
    connector_id: int,
    station_refs: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    summary: Dict[str, Dict[str, Any]] = {}
    if not station_refs:
        return summary
    station_rows = writer.fetch_station_rows(connector_id, station_refs)
    station_id_to_ref: Dict[int, str] = {}
    for row in station_rows:
        try:
            station_id = int(row.get("id"))
        except (TypeError, ValueError):
            continue
        station_ref = row.get("station_ref")
        if station_ref is None:
            continue
        station_id_to_ref[station_id] = str(station_ref)
        summary.setdefault(
            str(station_ref),
            {"timeseries_count": 0, "last_value": None, "last_value_at": None},
        )
    if not station_id_to_ref:
        return summary
    timeseries_rows = writer.fetch_timeseries_rows_by_station_ids(
        connector_id,
        list(station_id_to_ref.keys()),
    )
    for row in timeseries_rows:
        try:
            station_id = int(row.get("station_id"))
        except (TypeError, ValueError):
            continue
        station_ref = station_id_to_ref.get(station_id)
        if not station_ref:
            continue
        entry = summary.setdefault(
            station_ref,
            {"timeseries_count": 0, "last_value": None, "last_value_at": None},
        )
        entry["timeseries_count"] += 1
        last_value_at = row.get("last_value_at")
        if last_value_at is None:
            continue
        candidate_dt = _parse_iso_datetime(str(last_value_at))
        current_dt = _parse_iso_datetime(
            str(entry["last_value_at"]) if entry.get("last_value_at") else None
        )
        if current_dt is None or (candidate_dt and candidate_dt > current_dt):
            entry["last_value_at"] = str(last_value_at)
            entry["last_value"] = row.get("last_value")
    return summary


def _extract_timeseries_refs(station: Dict[str, Any]) -> List[str]:
    props = station.get("properties", {}) if isinstance(station.get("properties"), dict) else {}
    raw = props.get("timeseries")
    timeseries_refs: List[str] = []
    if isinstance(raw, list):
        for entry in raw:
            if isinstance(entry, dict):
                ts_id = entry.get("id")
                if ts_id is not None:
                    timeseries_refs.append(str(ts_id))
            elif entry is not None:
                timeseries_refs.append(str(entry))
    elif isinstance(raw, dict):
        for key, value in raw.items():
            if isinstance(value, dict) and value.get("id") is not None:
                timeseries_refs.append(str(value.get("id")))
            elif key is not None:
                timeseries_refs.append(str(key))
    return timeseries_refs


def _select_timeseries_row(
    rows: List[Dict[str, Any]],
    service_ref: Optional[str],
) -> Optional[Dict[str, Any]]:
    if service_ref is not None:
        for row in rows:
            if str(row.get("service_ref")) == str(service_ref):
                return row
    if len(rows) == 1:
        return rows[0]
    return None


def _write_timeseries_link_check(output: str, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "station_ref",
        "station_label",
        "service_ref",
        "timeseries_ref",
        "expected_station_id",
        "actual_station_id",
        "actual_station_ref",
        "actual_service_ref",
        "issue",
    ]
    with open(output, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def check_timeseries_links(
    stations: List[Dict[str, Any]],
    writer: "SupabaseWriter",
    connector_id: int,
    station_service_ref_map: Optional[Dict[str, str]],
    default_service_ref: Optional[str],
    output: str,
) -> int:
    expected_rows: List[Dict[str, Any]] = []
    for station in stations:
        station_ref = _resolve_station_ref(station)
        if _is_placeholder_station_ref(station_ref):
            continue
        if not station_ref:
            continue
        service_ref = _resolve_service_ref(
            station,
            station_ref,
            station_service_ref_map,
            default_service_ref,
        )
        label = station.get("label") or (station.get("properties") or {}).get("label") or station.get("name")
        timeseries_refs = _extract_timeseries_refs(station)
        for ts_ref in timeseries_refs:
            expected_rows.append(
                {
                    "station_ref": station_ref,
                    "station_label": label,
                    "service_ref": service_ref,
                    "timeseries_ref": ts_ref,
                }
            )

    if not expected_rows:
        LOG.warning("No timeseries refs found in station payload; check skipped.")
        _write_timeseries_link_check(output, [])
        return 0

    station_refs = sorted({row["station_ref"] for row in expected_rows})
    station_rows = writer.fetch_station_rows(connector_id, station_refs)
    station_index = _index_station_rows(station_rows)
    expected_station_ids: Dict[str, Optional[int]] = {}
    for row in expected_rows:
        station_ref = row["station_ref"]
        service_ref = row.get("service_ref")
        station_row = _select_station_row(station_ref, service_ref, station_index)
        expected_station_ids[station_ref] = (
            int(station_row.get("id")) if station_row and station_row.get("id") is not None else None
        )

    timeseries_refs = sorted({row["timeseries_ref"] for row in expected_rows})
    timeseries_rows = writer.fetch_timeseries_rows(connector_id, timeseries_refs)
    timeseries_by_ref: Dict[str, List[Dict[str, Any]]] = {}
    for row in timeseries_rows:
        ts_ref = row.get("timeseries_ref")
        if ts_ref is None:
            continue
        timeseries_by_ref.setdefault(str(ts_ref), []).append(row)

    station_ids = sorted(
        {int(row["station_id"]) for row in timeseries_rows if row.get("station_id") is not None}
    )
    station_by_id = writer.fetch_station_rows_by_id(station_ids)

    issues: List[Dict[str, Any]] = []
    for row in expected_rows:
        station_ref = row["station_ref"]
        service_ref = row.get("service_ref")
        ts_ref = row["timeseries_ref"]
        expected_station_id = expected_station_ids.get(station_ref)
        if expected_station_id is None:
            issues.append(
                {
                    **row,
                    "expected_station_id": None,
                    "actual_station_id": None,
                    "actual_station_ref": None,
                    "actual_service_ref": None,
                    "issue": "missing_station_row",
                }
            )
            continue

        ts_candidates = timeseries_by_ref.get(str(ts_ref), [])
        if not ts_candidates:
            issues.append(
                {
                    **row,
                    "expected_station_id": expected_station_id,
                    "actual_station_id": None,
                    "actual_station_ref": None,
                    "actual_service_ref": None,
                    "issue": "missing_timeseries",
                }
            )
            continue

        selected = _select_timeseries_row(ts_candidates, service_ref)
        if not selected:
            issues.append(
                {
                    **row,
                    "expected_station_id": expected_station_id,
                    "actual_station_id": None,
                    "actual_station_ref": None,
                    "actual_service_ref": None,
                    "issue": "ambiguous_timeseries",
                }
            )
            continue

        actual_station_id = selected.get("station_id")
        actual_station_ref = None
        if actual_station_id is not None:
            actual_station_ref = station_by_id.get(int(actual_station_id))

        if actual_station_id is None:
            issues.append(
                {
                    **row,
                    "expected_station_id": expected_station_id,
                    "actual_station_id": None,
                    "actual_station_ref": actual_station_ref,
                    "actual_service_ref": selected.get("service_ref"),
                    "issue": "missing_timeseries_station_id",
                }
            )
            continue

        if str(actual_station_id) != str(expected_station_id):
            issues.append(
                {
                    **row,
                    "expected_station_id": expected_station_id,
                    "actual_station_id": actual_station_id,
                    "actual_station_ref": actual_station_ref,
                    "actual_service_ref": selected.get("service_ref"),
                    "issue": "station_ref_mismatch",
                }
            )

    _write_timeseries_link_check(output, issues)
    LOG.info("Timeseries link check complete: %s issues written to %s", len(issues), output)
    return len(issues)


def _write_json(output: str, payload: Dict[str, Any]) -> None:
    with open(output, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _write_csv(
    output: str,
    stations: Iterable[Dict[str, Any]],
    service_ref_map: Optional[Dict[str, str]] = None,
    default_service_ref: Optional[str] = None,
) -> None:
    rows = [
        _normalize_station(
            station, service_ref_map=service_ref_map, default_service_ref=default_service_ref
        )
        for station in stations
    ]
    fieldnames = [
        "station_ref",
        "label",
        "station_name",
        "station_type",
        "region",
        "longitude",
        "latitude",
        "service_ref",
        "timeseries_refs",
    ]
    with open(output, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            if isinstance(row.get("timeseries_refs"), list):
                row["timeseries_refs"] = ",".join(str(val) for val in row["timeseries_refs"])
            writer.writerow(row)


def apply_station_enrichment(
    writer: SupabaseWriter,
    stations: Sequence[Dict[str, Any]],
    connector_id: int,
    station_service_ref_map: Optional[Dict[str, str]] = None,
    default_service_ref: Optional[str] = None,
    update_station_type: bool = True,
    skip_metadata: bool = False,
) -> Dict[str, int]:
    station_refs = sorted(
        {
            ref
            for ref in (
                _resolve_station_ref(station) for station in stations if isinstance(station, dict)
            )
            if ref
        }
    )
    station_rows = writer.fetch_station_rows(connector_id, station_refs)
    station_rows_map = _index_station_rows(station_rows)
    missing_station = 0
    ambiguous_station = 0
    metadata_updates: Dict[int, Dict[str, Any]] = {}
    station_type_updates: List[Dict[str, Any]] = []

    for station in stations:
        if not isinstance(station, dict):
            continue
        station_ref = _resolve_station_ref(station)
        if not station_ref:
            continue
        service_ref = _resolve_service_ref(
            station,
            station_ref,
            station_service_ref_map,
            default_service_ref,
        )
        row = _select_station_row(station_ref, service_ref, station_rows_map)
        if row is None:
            if station_ref in station_rows_map and service_ref is None:
                ambiguous_station += 1
            else:
                missing_station += 1
            continue
        try:
            station_id = int(row.get("id"))
        except (TypeError, ValueError):
            continue
        station_type = _station_type_from_payload(station) or row.get("station_type")
        if update_station_type:
            if not row.get("station_type") and station_type:
                station_type_updates.append({"id": station_id, "station_type": station_type})
        if not skip_metadata:
            attributes = _station_metadata_attributes(station)
            if attributes:
                metadata_updates[station_id] = attributes
    if station_type_updates:
        writer.upsert_station_types(station_type_updates)
    if not skip_metadata and metadata_updates:
        writer.upsert_station_metadata(metadata_updates)
    return {
        "station_rows": len(station_rows),
        "station_refs": len(station_refs),
        "missing_station": missing_station,
        "ambiguous_station": ambiguous_station,
        "station_type_updates": len(station_type_updates),
        "metadata_updates": len(metadata_updates),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch UK-AIR SOS stations for the UK.")
    parser.add_argument(
        "--output",
        default="sos_stations.json",
        help="Output file path (default: sos_stations.json).",
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
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip phenomena/procedures/offerings upserts when writing to Supabase.",
    )
    parser.add_argument(
        "--skip-station-metadata",
        action="store_true",
        help="Skip station_metadata upserts when writing to Supabase.",
    )
    parser.add_argument(
        "--skip-station-type-backfill",
        action="store_true",
        help="Skip station_type updates when writing to Supabase.",
    )
    parser.add_argument(
        "--metadata-batch-size",
        type=int,
        default=50,
        help="Batch size for timeseries metadata requests (default: 50).",
    )
    parser.add_argument(
        "--service-ref-from-timeseries",
        "--service-id-from-timeseries",
        action="store_true",
        help="Resolve service_ref using timeseries metadata instead of defaulting to a single service.",
    )
    parser.add_argument(
        "--check-timeseries-links",
        action="store_true",
        help="Compare payload station_ref + timeseries_ref links against Supabase timeseries rows.",
    )
    parser.add_argument(
        "--check-output",
        default="sos_timeseries_link_check.csv",
        help="CSV output path for --check-timeseries-links results.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_at = utcnow()
    client = UkAirClient()
    services = client.services()
    if not services:
        raise RuntimeError("No services returned from UK-AIR SOS.")
    primary_service = _select_primary_service(services)
    default_service_ref = None
    if primary_service and primary_service.get("id") is not None:
        default_service_ref = str(primary_service.get("id"))
    stations = client.stations()
    if not stations:
        LOG.warning("No stations returned from UK-AIR SOS.")

    filtered = (
        stations
        if args.no_filter
        else [s for s in stations if station_in_bbox_or_missing_coords(s, UK_BBOX)]
    )
    placeholder_stations = [
        station for station in filtered if _is_placeholder_station_ref(_resolve_station_ref(station))
    ]
    if placeholder_stations:
        LOG.warning(
            "Skipping %s placeholder station(s) with refs=%s",
            len(placeholder_stations),
            ", ".join(sorted({str(_resolve_station_ref(station)) for station in placeholder_stations})),
        )
        filtered = [
            station
            for station in filtered
            if not _is_placeholder_station_ref(_resolve_station_ref(station))
        ]
    missing_coords = sum(
        1
        for station in filtered
        if station_coords(station, bbox=UK_BBOX) == (None, None)
    )
    LOG.info(
        "Stations total=%s, uk_filtered=%s (missing coords=%s)",
        len(stations),
        len(filtered),
        missing_coords,
    )

    station_service_ref_map: Dict[str, str] = {}
    if args.service_ref_from_timeseries:
        station_ids = [
            s.get("id") or (s.get("properties") or {}).get("id")
            for s in filtered
            if s.get("id") or (s.get("properties") or {}).get("id")
        ]
        service_refs = [svc.get("id") for svc in services if svc.get("id")]
        station_service_ref_map = _station_service_map_from_timeseries(
            client, station_ids, service_refs, args.metadata_batch_size
        )
        LOG.info("Resolved service ref from timeseries for %s stations.", len(station_service_ref_map))

    writer: Optional[SupabaseWriter] = None
    if args.to_supabase:
        writer = SupabaseWriter()
        connector_id = writer.upsert_connectors(services)
        if connector_id is None:
            raise RuntimeError("Failed to resolve connector id for UK-AIR SOS.")
        if placeholder_stations:
            placeholder_refs = sorted(
                {
                    str(_resolve_station_ref(station))
                    for station in placeholder_stations
                    if _resolve_station_ref(station)
                }
            )
            if placeholder_refs:
                flagged = writer.flag_placeholder_stations(connector_id, placeholder_refs)
                if flagged:
                    LOG.info("Flagged %s placeholder station_metadata rows.", flagged)
        if not filtered:
            LOG.warning(
                "No non-placeholder UK-AIR SOS stations available; skipping station upsert, "
                "metadata enrichment, and mark_removed safeguards for this run."
            )
        else:
            inserted = writer.upsert_stations(
                filtered,
                connector_id,
                run_at,
                station_service_ref_map=station_service_ref_map,
                default_service_ref=default_service_ref,
            )
            LOG.info("Upserted %s stations into Supabase.", inserted)
            backfilled = writer.backfill_station_names([connector_id])
            if backfilled:
                LOG.info("Backfilled station_name for %s stations.", backfilled)
            else:
                LOG.info("No station_name backfill needed.")
            enrichment = apply_station_enrichment(
                writer,
                filtered,
                connector_id,
                station_service_ref_map=station_service_ref_map,
                default_service_ref=default_service_ref,
                update_station_type=not args.skip_station_type_backfill,
                skip_metadata=args.skip_station_metadata,
            )
            if not args.skip_station_type_backfill:
                LOG.info("Backfilled station_type for %s stations.", enrichment["station_type_updates"])
            if not args.skip_station_metadata:
                LOG.info("Upserted station_metadata for %s stations.", enrichment["metadata_updates"])
            if enrichment["missing_station"]:
                LOG.warning(
                    "Station enrichment skipped %s stations missing in DB.",
                    enrichment["missing_station"],
                )
            if enrichment["ambiguous_station"]:
                LOG.warning(
                    "Station enrichment skipped %s stations with ambiguous service_ref.",
                    enrichment["ambiguous_station"],
                )
            writer.mark_removed(run_at, [connector_id])

            if not args.skip_metadata:
                station_ids = [
                    s.get("id") or (s.get("properties") or {}).get("id")
                    for s in filtered
                    if s.get("id") or (s.get("properties") or {}).get("id")
                ]
                phenomena: Dict[str, Dict[str, Any]] = {}
                procedures: Dict[str, Dict[str, Any]] = {}
                offerings: Dict[str, Dict[str, Any]] = {}
                for chunk in _chunked(station_ids, args.metadata_batch_size):
                    series = client.timeseries(chunk, service_ref=default_service_ref)
                    for ts in series:
                        _collect_reference(phenomena, ts.get("phenomenon"))
                        _collect_reference(procedures, ts.get("procedure"))
                        _collect_reference(offerings, ts.get("offering"))
                if phenomena:
                    LOG.info("Upserting phenomena: %s", len(phenomena))
                    writer.upsert_phenomena(phenomena.values(), connector_id)
                if procedures:
                    LOG.info("Upserting procedures: %s", len(procedures))
                    writer.upsert_reference_table(
                        "procedures",
                        "procedure_ref",
                        procedures.values(),
                        connector_id,
                        default_service_ref,
                    )
                if offerings:
                    LOG.info("Upserting offerings: %s", len(offerings))
                    writer.upsert_reference_table(
                        "offerings",
                        "offering_ref",
                        offerings.values(),
                        connector_id,
                        default_service_ref,
                    )

    if args.check_timeseries_links:
        if writer is None:
            writer = SupabaseWriter()
        connector_id = writer.get_connector_id()
        if connector_id is None:
            raise RuntimeError(
                "Missing connector id for UK-AIR SOS. Run with --to-supabase at least once."
            )
        check_timeseries_links(
            filtered,
            writer,
            connector_id,
            station_service_ref_map,
            default_service_ref,
            args.check_output,
        )

    timeseries_summary: Dict[str, Dict[str, Any]] = {}
    if writer is not None:
        connector_id = writer.get_connector_id()
        if connector_id is not None:
            station_refs = sorted(
                {
                    ref
                    for ref in (_resolve_station_ref(station) for station in filtered)
                    if ref
                }
            )
            timeseries_summary = build_timeseries_summary(
                writer,
                connector_id,
                station_refs,
            )

    if args.format == "csv":
        _write_csv(
            args.output,
            filtered,
            service_ref_map=station_service_ref_map,
            default_service_ref=default_service_ref,
        )
    else:
        raw_payload = None
        if args.raw_output:
            raw_payload = {
                "source": SOS_BASE_URL,
                "fetched_at": utcnow().isoformat(),
                "bbox": None if args.no_filter else UK_BBOX,
                "count": len(filtered),
                "stations": filtered,
            }
            _write_json(args.raw_output, raw_payload)
        payload = {
            "source": SOS_BASE_URL,
            "fetched_at": utcnow().isoformat(),
            "bbox": None if args.no_filter else UK_BBOX,
            "count": len(filtered),
            "service_ref": default_service_ref,
            "stations": [
                _normalize_station(
                    station,
                    service_ref_map=station_service_ref_map,
                    default_service_ref=default_service_ref,
                    timeseries_summary=timeseries_summary,
                )
                for station in filtered
            ],
        }
        _write_json(args.output, payload)
    LOG.info("Wrote %s", args.output)


if __name__ == "__main__":
    main()
