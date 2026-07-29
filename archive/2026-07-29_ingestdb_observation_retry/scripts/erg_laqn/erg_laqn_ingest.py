#!/usr/bin/env python3
"""
Ingest LAQN observations from the ERG AirQuality API.

Examples:
  python3 scripts/erg_laqn/erg_laqn_ingest.py --species NO2,PM10
  python3 scripts/erg_laqn/erg_laqn_ingest.py --days 3 --limit 5 --dry-run
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from supabase import Client

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.erg_laqn.erg_laqn_list_stations import (
    LAQN_BASE_URL,
    LAQN_CONNECTOR_CODE,
    LAQN_SERVICE_REF,
    LAQN_USER_AGENT,
    LaqnClient,
    _normalize_station_payload,
)
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client
from scripts.uk_aq_phenomena_rpc import upsert_phenomena_via_rpc

load_dotenv()

LOG = logging.getLogger("erg_laqn_ingest")
DEFAULT_LOG_LEVEL = os.getenv("LAQN_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))

DEFAULT_DAYS = 7
DEFAULT_SLEEP_SECONDS = 0.2
DEFAULT_BATCH_SIZE = 500
DEFAULT_SKIP_ZERO_RECENT_HOURS = 1.0

LAQN_RAW_DATA_URL_TEMPLATE = os.getenv("LAQN_RAW_DATA_URL_TEMPLATE")

SPECIES_CONFIG = {
    "NO2": {"label": "NO2", "uom": "ug/m3", "pollutant_label": "no2"},
    "PM10": {"label": "PM10", "uom": "ug/m3", "pollutant_label": "pm10"},
    "PM25": {"label": "PM2.5", "uom": "ug/m3", "pollutant_label": "pm2.5"},
    "O3": {"label": "O3", "uom": "ug/m3", "pollutant_label": "o3"},
    "SO2": {"label": "SO2", "uom": "ug/m3", "pollutant_label": "so2"},
    "CO": {"label": "CO", "uom": "mg/m3", "pollutant_label": "co"},
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utc_day_start(value: datetime) -> datetime:
    value_utc = value.astimezone(timezone.utc)
    return datetime(value_utc.year, value_utc.month, value_utc.day, tzinfo=timezone.utc)


def _build_erg_date_range(
    now: datetime,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    days: int,
) -> Tuple[datetime, datetime]:
    today_start = _utc_day_start(now)
    default_end = today_start + timedelta(days=1)
    end_date = end_date.astimezone(timezone.utc) if end_date else default_end
    start_date = (
        start_date.astimezone(timezone.utc)
        if start_date
        else end_date - timedelta(days=max(days, 1))
    )
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    return start_date, end_date


def _clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d",
    ):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _normalize_species(value: str) -> Optional[str]:
    if not value:
        return None
    cleaned = value.strip().upper().replace(".", "").replace(" ", "")
    if cleaned in {"PM2_5", "PM2-5", "PM25"}:
        return "PM25"
    return cleaned


def _parse_species_list(value: str) -> List[str]:
    items = [item.strip() for item in value.split(",") if item.strip()]
    normalized = []
    for item in items:
        key = _normalize_species(item)
        if key:
            normalized.append(key)
    return normalized


def _build_timeseries_ref(station_ref: str, species: str) -> str:
    return f"{station_ref}:{species}"


def _extract_observations(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        nested = payload
        for key in ("RawAQData", "rawAQData"):
            value = payload.get(key)
            if isinstance(value, dict):
                nested = value
                break
        for key in ("RawData", "rawData", "Data", "data", "Measurements", "measurements"):
            value = nested.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _parse_observations(
    payload: Any,
    timeseries_id: int,
    recent_zero_cutoff: Optional[datetime],
) -> Tuple[List[Dict[str, Any]], Optional[datetime], Optional[float]]:
    rows: List[Dict[str, Any]] = []
    last_observed: Optional[datetime] = None
    last_value: Optional[float] = None
    for entry in _extract_observations(payload):
        observed_at = _parse_datetime(
            entry.get("DateTime")
            or entry.get("DateTimeGMT")
            or entry.get("Date")
            or entry.get("MeasurementDate")
            or entry.get("@MeasurementDateGMT")
            or entry.get("@MeasurementDate")
            or entry.get("@DateTimeGMT")
            or entry.get("@DateTime")
        )
        value = _coerce_float(
            entry.get("Value")
            or entry.get("ScaledValue")
            or entry.get("RawValue")
            or entry.get("@Value")
        )
        if observed_at is None or value is None:
            continue
        if recent_zero_cutoff and value == 0 and observed_at >= recent_zero_cutoff:
            continue
        rows.append(
            {
                "timeseries_id": timeseries_id,
                "observed_at": observed_at.isoformat(),
                "value": value,
            }
        )
        if last_observed is None or observed_at > last_observed:
            last_observed = observed_at
            last_value = value
    return rows, last_observed, last_value


def _chunked(values: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    if size <= 0:
        size = DEFAULT_BATCH_SIZE
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _chunked_values(values: List[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = 200
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


class SupabaseWriter:
    def __init__(self) -> None:
        self.client: Client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core
        self.public = self.client.schema(os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public")

    def upsert_connector(self) -> int:
        row = (
            self.core.table("connectors")
            .select("id")
            .eq("connector_code", LAQN_CONNECTOR_CODE)
            .single()
            .execute()
        )
        data = row.data if hasattr(row, "data") else row.get("data")
        if not data:
            raise RuntimeError("Connector not found for LAQN. Run the list_stations job first.")
        return int(data["id"])

    def upsert_stations(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = [row for row in rows if row.get("station_ref")]
        if not payload:
            return 0
        self.core.table("stations").upsert(
            payload, on_conflict="connector_id,service_ref,station_ref"
        ).execute()
        return len(payload)

    def fetch_station_ids_by_ref(
        self, connector_id: int, service_ref: str, station_refs: Iterable[str]
    ) -> Dict[str, int]:
        refs = [str(ref) for ref in station_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        for chunk in _chunked_values(refs, 200):
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

    def upsert_phenomena(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        for row in payload:
            raw_code = str(row.get("pollutant_label") or "").strip().lower()
            observed_property_code = "pm25" if raw_code == "pm2.5" else raw_code
            row.update(
                {
                    "source_uom": next(
                        (
                            config.get("uom")
                            for config in SPECIES_CONFIG.values()
                            if config.get("pollutant_label") == row.get("pollutant_label")
                        ),
                        None,
                    ),
                    "mapping_kind": "raw_observed_property",
                    "observed_property_code": observed_property_code,
                    "is_aqi_eligible": observed_property_code in ("pm25", "pm10", "no2"),
                }
            )
        return len(
            upsert_phenomena_via_rpc(
                self.public,
                payload,
                allow_mapping_upsert=True,
            )
        )

    def fetch_phenomena_ids(
        self, connector_id: int, source_labels: Iterable[str]
    ) -> Dict[str, int]:
        refs = [str(ref) for ref in source_labels if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        for chunk in _chunked_values(refs, 200):
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
        for chunk in _chunked_values(refs, 200):
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

    def update_timeseries_last_values(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        updated = 0
        for row in payload:
            timeseries_id = row.get("id")
            if timeseries_id is None:
                continue
            self.core.table("timeseries").update(
                {
                    "last_value": row.get("last_value"),
                    "last_value_at": row.get("last_value_at"),
                }
            ).eq("id", int(timeseries_id)).execute()
            updated += 1
        return updated


class LaqnIngestClient:
    def __init__(self, base_url: str = LAQN_BASE_URL, timeout: int = 60, retries: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": LAQN_USER_AGENT})

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
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

    def raw_data(
        self,
        site_code: str,
        species: str,
        start: datetime,
        end: datetime,
        index_days: int,
    ) -> Any:
        if LAQN_RAW_DATA_URL_TEMPLATE:
            url = LAQN_RAW_DATA_URL_TEMPLATE.format(
                site_code=site_code,
                species=species,
                start=start.date().isoformat(),
                end=end.date().isoformat(),
                index_days=index_days,
            )
            return self.get(url)
        if index_days > 0:
            LOG.warning("Index-days requests are unsupported for LAQN raw data; using date range.")
        start_date = start.date().isoformat()
        end_date = end.date().isoformat()
        templates = [
            (
                (
                    f"{self.base_url}/Data/SiteSpecies/SiteCode={site_code}"
                    f"/SpeciesCode={species}/StartDate={start_date}/EndDate={end_date}/Json"
                ),
                None,
            ),
            (
                (
                    f"{self.base_url}/Data/Site/SiteCode={site_code}"
                    f"/StartDate={start_date}/EndDate={end_date}/Json"
                ),
                None,
            ),
        ]
        last_error: Optional[Exception] = None
        for url, params in templates:
            try:
                return self.get(url, params=params)
            except requests.RequestException as exc:
                last_error = exc
                LOG.warning("Raw data fetch failed for %s (%s): %s", site_code, url, exc)
                continue
        if last_error:
            raise last_error
        return []


def _collect_station_rows(
    stations: List[Dict[str, Any]], connector_id: int
) -> List[Dict[str, Any]]:
    rows = []
    for station in stations:
        row, _ = _normalize_station_record(station, connector_id)
        if row.get("station_ref"):
            rows.append(row)
    return rows


def _station_ref_from_payload(station: Dict[str, Any]) -> Optional[str]:
    return _clean_text(
        station.get("station_ref")
        or station.get("SiteCode")
        or station.get("sitecode")
    )


def _normalize_station_record(
    station: Dict[str, Any], connector_id: int
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    if station.get("station_ref"):
        row = dict(station)
        row["connector_id"] = connector_id
        row.setdefault("service_ref", LAQN_SERVICE_REF)
        row.setdefault("label", row.get("station_name") or row.get("station_ref"))
        row.setdefault("station_name", row.get("label") or row.get("station_ref"))
        return row, {}
    return _normalize_station_payload(station, connector_id)


def _select_stations(
    stations: List[Dict[str, Any]],
    site_codes: Optional[List[str]],
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    filtered = stations
    if site_codes:
        allowed = {code.strip().upper() for code in site_codes if code.strip()}
        filtered = [
            station
            for station in stations
            if (
                _station_ref_from_payload(station) or ""
            ).upper()
            in allowed
        ]
    if limit is not None and limit > 0:
        filtered = filtered[:limit]
    return filtered


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest LAQN observations from ERG API.")
    parser.add_argument(
        "--species",
        default="NO2,PM10,PM25,O3",
        help="Comma-separated species list (default: NO2,PM10,PM25,O3).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="Days of history to fetch (default: 7).",
    )
    parser.add_argument(
        "--start-date",
        help="Override start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        help="Override end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--index-days",
        type=int,
        default=0,
        help="Use GetRawDataSiteSpeciesIndexDaysJSON with this index days value.",
    )
    parser.add_argument(
        "--site-codes",
        help="Comma-separated site codes to ingest (optional).",
    )
    parser.add_argument(
        "--stations-json",
        help="Optional JSON snapshot of LAQN stations to use instead of the live API.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of stations to ingest (optional).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="Sleep between API calls (default: 0.2).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Supabase writes and only log fetched data.",
    )
    parser.add_argument(
        "--skip-stations",
        action="store_true",
        help="Skip station upserts (assumes stations already exist).",
    )
    parser.add_argument(
        "--output-observations",
        help="Optional JSON file to write observations payloads.",
    )
    parser.add_argument(
        "--output-raw-responses",
        help="Optional JSON file to write raw API responses for each station/species.",
    )
    return parser.parse_args()


def _parse_date_arg(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    parsed = _parse_datetime(value)
    return parsed


def _load_stations_snapshot(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, dict):
        stations = payload.get("stations")
        if isinstance(stations, list):
            return [row for row in stations if isinstance(row, dict)]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    raise ValueError(f"Unsupported stations snapshot format: {path}")


def main() -> int:
    args = parse_args()
    species_list = _parse_species_list(args.species)
    if not species_list:
        raise RuntimeError("No species specified.")

    now = utcnow()
    end_date = _parse_date_arg(args.end_date)
    start_date = _parse_date_arg(args.start_date)
    start_date, end_date = _build_erg_date_range(now, start_date, end_date, args.days)
    utc_today_start = _utc_day_start(now)
    recent_zero_cutoff = now - timedelta(hours=DEFAULT_SKIP_ZERO_RECENT_HOURS)

    if args.stations_json:
        stations = _load_stations_snapshot(args.stations_json)
        LOG.info("Loaded %s stations from %s", len(stations), args.stations_json)
    else:
        stations_client = LaqnClient()
        stations = stations_client.monitoring_sites()
    if not stations:
        LOG.warning("No LAQN stations returned from monitoring sites API.")
        return 0

    site_codes = [code.strip() for code in args.site_codes.split(",") if code.strip()] if args.site_codes else None
    selected = _select_stations(stations, site_codes, args.limit)
    if not selected:
        LOG.warning("No stations selected for ingest.")
        return 0

    ingest_client = LaqnIngestClient()
    observations_output = [] if args.output_observations else None
    raw_output = [] if args.output_raw_responses else None
    observation_total = 0
    timeseries_updates: List[Dict[str, Any]] = []

    if args.dry_run:
        station_rows = _collect_station_rows(selected, connector_id=0)
        for row in station_rows:
            station_ref = str(row["station_ref"])
            for species in species_list:
                try:
                    payload = ingest_client.raw_data(
                        station_ref,
                        species,
                        start=start_date,
                        end=end_date,
                        index_days=max(args.index_days, 0),
                    )
                except requests.RequestException as exc:
                    LOG.warning(
                        "Raw data fetch failed for %s (%s): %s", station_ref, species, exc
                    )
                    if raw_output is not None:
                        raw_output.append(
                            {
                                "station_ref": station_ref,
                                "species": species,
                                "error": str(exc),
                            }
                        )
                    continue
                if raw_output is not None:
                    raw_output.append(
                        {"station_ref": station_ref, "species": species, "payload": payload}
                    )
                rows, last_observed, _ = _parse_observations(
                    payload, 0, recent_zero_cutoff
                )
                if last_observed and last_observed < utc_today_start:
                    LOG.warning(
                        "ERG LAQN observations missing today. station_ref=%s species=%s start_date=%s end_date=%s last_observed_at=%s",
                        station_ref,
                        species,
                        start_date.date().isoformat(),
                        end_date.date().isoformat(),
                        last_observed.isoformat(),
                    )
                if observations_output is not None:
                    observations_output.append(
                        {
                            "station_ref": station_ref,
                            "species": species,
                            "observations": rows,
                        }
                    )
                observation_total += len(rows)
                time.sleep(max(args.sleep_seconds, 0))

        if observations_output is not None:
            with open(args.output_observations, "w", encoding="utf-8") as handle:
                json.dump(observations_output, handle, indent=2)
        if raw_output is not None:
            with open(args.output_raw_responses, "w", encoding="utf-8") as handle:
                json.dump(raw_output, handle, indent=2)

        LOG.info("Dry-run fetched %s observations.", observation_total)
        return 0

    writer = SupabaseWriter()
    connector_id = writer.upsert_connector()

    station_rows = _collect_station_rows(selected, connector_id)
    if not args.skip_stations:
        station_count = writer.upsert_stations(station_rows)
        LOG.info("Upserted %s stations.", station_count)
    elif args.skip_stations:
        LOG.info("Skipping station upserts.")

    station_id_map = writer.fetch_station_ids_by_ref(
        connector_id, LAQN_SERVICE_REF, [row["station_ref"] for row in station_rows]
    )
    if not station_id_map:
        LOG.warning("No station ids resolved for LAQN.")
        return 0

    phenomena_rows = []
    for species in species_list:
        config = SPECIES_CONFIG.get(
            species, {"label": species, "uom": None, "pollutant_label": species.lower()}
        )
        phenomena_rows.append(
            {
                "connector_id": connector_id,
                "label": config["label"],
                "source_label": f"laqn:{species}",
                "notation": species,
                "pollutant_label": config["pollutant_label"],
            }
        )
    writer.upsert_phenomena(phenomena_rows)
    phenomenon_ids = writer.fetch_phenomena_ids(
        connector_id, [row["source_label"] for row in phenomena_rows]
    )

    timeseries_rows = []
    for row in station_rows:
        station_ref = str(row["station_ref"])
        station_id = station_id_map.get(station_ref)
        if station_id is None:
            continue
        station_name = row.get("station_name") or row.get("label") or station_ref
        for species in species_list:
            config = SPECIES_CONFIG.get(species, {"label": species, "uom": None})
            timeseries_rows.append(
                {
                    "timeseries_ref": _build_timeseries_ref(station_ref, species),
                    "label": f"{station_name} {config['label']}",
                    "uom": config.get("uom"),
                    "station_id": station_id,
                    "service_ref": LAQN_SERVICE_REF,
                    "connector_id": connector_id,
                    "phenomenon_id": phenomenon_ids.get(f"laqn:{species}"),
                    "extras": {"site_code": station_ref, "species": species},
                }
            )
    writer.upsert_timeseries(timeseries_rows)
    timeseries_id_map = writer.fetch_timeseries_ids(
        connector_id, LAQN_SERVICE_REF, [row["timeseries_ref"] for row in timeseries_rows]
    )

    for row in station_rows:
        station_ref = str(row["station_ref"])
        for species in species_list:
            timeseries_ref = _build_timeseries_ref(station_ref, species)
            timeseries_id = timeseries_id_map.get(timeseries_ref)
            if timeseries_id is None:
                continue
            try:
                payload = ingest_client.raw_data(
                    station_ref,
                    species,
                    start=start_date,
                    end=end_date,
                    index_days=max(args.index_days, 0),
                )
            except requests.RequestException as exc:
                LOG.warning(
                    "Raw data fetch failed for %s (%s): %s", station_ref, species, exc
                )
                if raw_output is not None:
                    raw_output.append(
                        {"station_ref": station_ref, "species": species, "error": str(exc)}
                    )
                continue
            if raw_output is not None:
                raw_output.append(
                    {"station_ref": station_ref, "species": species, "payload": payload}
                )
            rows, last_observed, last_value = _parse_observations(
                payload, timeseries_id, recent_zero_cutoff
            )
            if last_observed and last_observed < utc_today_start:
                LOG.warning(
                    "ERG LAQN observations missing today. station_ref=%s species=%s start_date=%s end_date=%s last_observed_at=%s",
                    station_ref,
                    species,
                    start_date.date().isoformat(),
                    end_date.date().isoformat(),
                    last_observed.isoformat(),
                )
            if observations_output is not None:
                observations_output.append(
                    {
                        "station_ref": station_ref,
                        "species": species,
                        "observations": rows,
                    }
                )
            if rows and not args.dry_run:
                for chunk in _chunked(rows, DEFAULT_BATCH_SIZE):
                    observation_total += writer.upsert_observations(chunk)
            if last_observed and last_value is not None and not args.dry_run:
                timeseries_updates.append(
                    {
                        "id": timeseries_id,
                        "last_value": last_value,
                        "last_value_at": last_observed.isoformat(),
                    }
                )
            time.sleep(max(args.sleep_seconds, 0))

    if timeseries_updates and not args.dry_run:
        writer.update_timeseries_last_values(timeseries_updates)

    if observations_output is not None:
        with open(args.output_observations, "w", encoding="utf-8") as handle:
            json.dump(observations_output, handle, indent=2)
    if raw_output is not None:
        with open(args.output_raw_responses, "w", encoding="utf-8") as handle:
            json.dump(raw_output, handle, indent=2)

    LOG.info("Ingested %s observations.", observation_total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
