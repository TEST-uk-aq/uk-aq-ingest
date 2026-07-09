#!/usr/bin/env python3
"""
Backfill station/feature mappings for timeseries rows missing station_id.

Requires:
- SUPABASE_URL
- SB_SECRET_KEY
"""

from __future__ import annotations

import argparse
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.sos.sos_ingest import (
    SOS_BASE_URL,
    UK_BBOX,
    SupabaseWriter,
    UkAirClient,
    _extract_feature_payload,
    _extract_ref_id,
    _extract_station_ref,
    _extract_station_descriptor_from_label,
    _extract_station_ref_from_label,
    _extract_station_name_from_label,
    _dropbox_log_target_path,
    _dropbox_refresh_access_token,
    _dropbox_upload_file,
    _load_dropbox_config,
    _normalize_station_label,
    utcnow,
)

load_dotenv()


class CreatedStationsLog:
    def __init__(self, config) -> None:
        self.config = config
        self.temp_dir: Optional[tempfile.TemporaryDirectory] = None
        self.path: Optional[Path] = None
        self.handle = None
        self.count = 0

    def _ensure_open(self) -> None:
        if self.handle is not None:
            return
        stamp = utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"uk_aq_backfill_created_stations_{stamp}.log"
        if self.config:
            self.temp_dir = tempfile.TemporaryDirectory(prefix="uk_aq_backfill_created_")
            path = Path(self.temp_dir.name) / filename
        else:
            path = Path.cwd() / filename
        self.path = path
        self.handle = path.open("w", encoding="utf-8")

    def record(self, payload: Dict[str, Any]) -> None:
        self._ensure_open()
        assert self.handle is not None
        self.handle.write(f"{payload}\n")
        self.handle.flush()
        self.count += 1

    def finalize(self) -> None:
        if self.handle is None or self.path is None:
            return
        self.handle.close()
        if self.config:
            try:
                access_token = _dropbox_refresh_access_token(self.config)
                dropbox_path = _dropbox_log_target_path(self.config.folder, self.path.name)
                _dropbox_upload_file(access_token, self.path, dropbox_path)
                print(f"Uploaded created-stations log to Dropbox: {dropbox_path}")
            except Exception as exc:
                print(f"Dropbox log upload failed: {exc}", file=sys.stderr)
        else:
            print(f"Dropbox credentials missing; created-stations log saved at {self.path}", file=sys.stderr)
        if self.temp_dir is not None:
            self.temp_dir.cleanup()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill timeseries station/feature mappings from SOS.",
    )
    parser.add_argument("--connector-id", type=int, help="Filter to a connector id.")
    parser.add_argument("--connector-code", help="Filter to a connector code.")
    parser.add_argument("--service-ref", help="Filter to a service ref.")
    parser.add_argument("--batch-size", type=int, default=200, help="Rows per fetch batch.")
    parser.add_argument("--limit", type=int, help="Maximum timeseries to process.")
    parser.add_argument("--sleep-seconds", type=float, default=0.2, help="Pause between API calls.")
    return parser.parse_args()


def fetch_connectors(writer: SupabaseWriter) -> Dict[int, Dict[str, Any]]:
    resp = writer.core.table("connectors").select("id,connector_code,service_url,label").execute()
    rows = resp.data if hasattr(resp, "data") else resp.get("data")
    return {int(row["id"]): row for row in (rows or [])}


def fetch_missing_timeseries(
    writer: SupabaseWriter,
    connector_id: Optional[int],
    service_ref: Optional[str],
    batch_size: int,
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        query = (
            writer.core.table("timeseries")
            .select("timeseries_ref,connector_id,service_ref,label")
            .is_("station_id", None)
            .order("id", desc=False)
            .range(offset, offset + batch_size - 1)
        )
        if connector_id is not None:
            query = query.eq("connector_id", connector_id)
        if service_ref is not None:
            query = query.eq("service_ref", str(service_ref))
        resp = query.execute()
        batch = resp.data if hasattr(resp, "data") else resp.get("data")
        if not batch:
            break
        rows.extend(batch)
        offset += batch_size
        if limit is not None and len(rows) >= limit:
            return rows[:limit]
    return rows


def fetch_station_label_map(
    writer: SupabaseWriter, connector_id: int, service_ref: str
) -> Dict[str, List[int]]:
    label_map: Dict[str, List[int]] = {}
    offset = 0
    batch_size = 1000
    while True:
        resp = (
            writer.core.table("stations")
            .select("id,label")
            .eq("connector_id", connector_id)
            .eq("service_ref", str(service_ref))
            .order("id", desc=False)
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        if not rows:
            break
        for row in rows:
            label = row.get("label")
            if not label:
                continue
            label_text = str(label)
            key_full = _normalize_station_label(label_text)
            if key_full:
                label_map.setdefault(key_full, []).append(int(row["id"]))
            base_name = _extract_station_name_from_label(label_text)
            if base_name:
                key_base = _normalize_station_label(base_name)
                if key_base and key_base != key_full:
                    label_map.setdefault(key_base, []).append(int(row["id"]))
        offset += batch_size
    return label_map


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_geometry_coords(value: Any) -> Optional[Tuple[float, float]]:
    if value is None:
        return None
    if isinstance(value, dict):
        coords = value.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            lon = _coerce_float(coords[0])
            lat = _coerce_float(coords[1])
            if lon is not None and lat is not None:
                return lon, lat
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        lon = _coerce_float(value[0])
        lat = _coerce_float(value[1])
        if lon is not None and lat is not None:
            return lon, lat
    if isinstance(value, str):
        match = re.search(r"POINT\\s*\\(\\s*(-?\\d+(?:\\.\\d+)?)\\s+(-?\\d+(?:\\.\\d+)?)\\s*\\)", value)
        if match:
            lon = _coerce_float(match.group(1))
            lat = _coerce_float(match.group(2))
            if lon is not None and lat is not None:
                return lon, lat
    return None


def _coords_match(a: Tuple[float, float], b: Tuple[float, float], tol: float = 1e-6) -> bool:
    return abs(a[0] - b[0]) <= tol and abs(a[1] - b[1]) <= tol


def _geometry_key(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if re.fullmatch(r"[0-9A-Fa-f]+", text):
            return text.lower()
        match = re.search(r"POINT\\s*\\(\\s*(-?\\d+(?:\\.\\d+)?)\\s+(-?\\d+(?:\\.\\d+)?)\\s*\\)", text)
        if match:
            lon = _coerce_float(match.group(1))
            lat = _coerce_float(match.group(2))
            if lon is not None and lat is not None:
                return f"point:{lon:.6f},{lat:.6f}"
        return None
    if isinstance(value, dict):
        coords = value.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            lon = _coerce_float(coords[0])
            lat = _coerce_float(coords[1])
            if lon is not None and lat is not None:
                return f"point:{lon:.6f},{lat:.6f}"
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        lon = _coerce_float(value[0])
        lat = _coerce_float(value[1])
        if lon is not None and lat is not None:
            return f"point:{lon:.6f},{lat:.6f}"
    return None


def _geometry_wkt(lon: float, lat: float) -> str:
    return f"SRID=4326;POINT({lon} {lat})"


def _resolve_uniform_value(values: Iterable[Optional[str]]) -> Optional[str]:
    unique = {value for value in values if value not in (None, "")}
    if len(unique) == 1:
        return next(iter(unique))
    return None


def fetch_station_geometry_index(
    writer: SupabaseWriter, connector_id: int, service_ref: str
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[int, str]]:
    index: Dict[str, List[Dict[str, Any]]] = {}
    geometry_by_id: Dict[int, str] = {}
    offset = 0
    batch_size = 1000
    while True:
        resp = (
            writer.core.table("stations")
            .select("id,label,geometry,station_type,region")
            .eq("connector_id", connector_id)
            .eq("service_ref", str(service_ref))
            .order("id", desc=False)
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        if not rows:
            break
        for row in rows:
            label = row.get("label")
            if not label:
                continue
            base_name = _extract_station_name_from_label(str(label)) or str(label)
            key = _normalize_station_label(base_name)
            if not key:
                continue
            index.setdefault(key, []).append(row)
            key = _geometry_key(row.get("geometry"))
            if key is not None:
                geometry_by_id[int(row["id"])] = key
        offset += batch_size
    return index, geometry_by_id


def infer_station_seed(
    station_index: Dict[str, List[Dict[str, Any]]], station_name: str
) -> Optional[Dict[str, Any]]:
    key = _normalize_station_label(station_name)
    if not key:
        return None
    rows = station_index.get(key)
    if not rows:
        return None
    geom_values = [row.get("geometry") for row in rows if row.get("geometry") is not None]
    geom_keys = [_geometry_key(value) for value in geom_values]
    geom_keys = [key for key in geom_keys if key is not None]
    if not geom_keys:
        return None
    first_key = geom_keys[0]
    if not all(first_key == key for key in geom_keys[1:]):
        return None
    geometry_value = geom_values[0]
    station_type = _resolve_uniform_value(row.get("station_type") for row in rows)
    region = _resolve_uniform_value(row.get("region") for row in rows)
    return {
        "geometry": geometry_value,
        "station_type": station_type,
        "region": region,
    }


def choose_station_id_by_geometry(
    matches: List[int],
    geometry_by_id: Dict[int, str],
) -> Optional[int]:
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    keys = [geometry_by_id.get(match) for match in matches]
    if any(key is None for key in keys):
        return None
    first = keys[0]
    if all(first == key for key in keys[1:]):
        return min(matches)
    return None


def collect_categories(stations: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    categories: List[Dict[str, Any]] = []
    for stn in stations:
        props = stn.get("properties") if isinstance(stn.get("properties"), dict) else {}
        category = props.get("category") if isinstance(props.get("category"), dict) else None
        if category:
            categories.append(category)
    return categories


def main() -> int:
    args = parse_args()
    created_log = CreatedStationsLog(_load_dropbox_config(None))
    if args.connector_id is not None and args.connector_code:
        print("Use either --connector-id or --connector-code, not both.", file=sys.stderr)
        return 1

    writer = SupabaseWriter()
    connectors = fetch_connectors(writer)
    if not connectors:
        print("No connectors found in database.", file=sys.stderr)
        return 1

    target_connector_id = args.connector_id
    if args.connector_code:
        matches = [
            cid
            for cid, row in connectors.items()
            if row.get("connector_code") == args.connector_code
        ]
        if not matches:
            print(f"Connector code not found: {args.connector_code}", file=sys.stderr)
            return 1
        target_connector_id = matches[0]

    missing = fetch_missing_timeseries(
        writer, target_connector_id, args.service_ref, args.batch_size, args.limit
    )
    if not missing:
        print("No timeseries rows missing station_id.")
        return 0

    by_service: Dict[Tuple[int, str], List[str]] = {}
    label_by_ref: Dict[Tuple[int, str, str], str] = {}
    for row in missing:
        ts_ref = row.get("timeseries_ref")
        connector_id = row.get("connector_id")
        service_ref = row.get("service_ref")
        if ts_ref is None or connector_id is None or not service_ref:
            continue
        connector_id_int = int(connector_id)
        service_ref_str = str(service_ref)
        by_service.setdefault((connector_id_int, service_ref_str), []).append(str(ts_ref))
        label = row.get("label")
        if label:
            label_by_ref[(connector_id_int, service_ref_str, str(ts_ref))] = str(label)

    for (connector_id, service_ref), ts_refs in by_service.items():
        connector = connectors.get(connector_id) or {}
        base_url = (connector.get("service_url") or SOS_BASE_URL).rstrip("/")

        client = UkAirClient(base_url=base_url)
        print(
            "Fetching timeseries details for connector %s service_ref %s (%s rows)."
            % (connector_id, service_ref, len(ts_refs))
        )

        details_by_ref: Dict[str, Dict[str, Any]] = {}
        feature_payloads: List[Dict[str, Any]] = []
        for idx, ts_ref in enumerate(ts_refs, start=1):
            detail = client.timeseries_detail(ts_ref)
            if not detail:
                continue
            station_ref = _extract_station_ref(detail)
            feature_payload = _extract_feature_payload(detail)
            feature_ref = _extract_ref_id(feature_payload) if feature_payload else None
            label = detail.get("label") if isinstance(detail, dict) else None
            if not label:
                label = label_by_ref.get((connector_id, service_ref, ts_ref))
            if station_ref is None and label:
                station_ref = _extract_station_ref_from_label(label)
            details_by_ref[ts_ref] = {
                "station_ref": station_ref,
                "feature_ref": feature_ref,
                "label": label,
            }
            if feature_payload:
                feature_payloads.append(feature_payload)
            if args.sleep_seconds:
                time.sleep(max(0.0, args.sleep_seconds))
            if idx % 50 == 0:
                print(".", end="", flush=True)
        if len(ts_refs) >= 50:
            print()

        station_refs = {
            detail["station_ref"]
            for detail in details_by_ref.values()
            if detail.get("station_ref")
        }
        station_id_map = writer.get_station_id_map(connector_id, service_ref, list(station_refs))
        missing_station_refs = [ref for ref in station_refs if ref not in station_id_map]
        if missing_station_refs:
            print(f"Refreshing stations for service_ref {service_ref}.")
            stations = client.stations(service_ref, bbox=None, region=None)
            if stations:
                category_map = writer.upsert_reference_table(
                    "categories",
                    "category_ref",
                    collect_categories(stations),
                    connector_id,
                )
                writer.upsert_stations(
                    stations,
                    connector_id,
                    service_ref,
                    category_map,
                    bbox=UK_BBOX,
                )
                station_id_map = writer.get_station_id_map(
                    connector_id, service_ref, list(station_refs)
                )
                missing_station_refs = [ref for ref in station_refs if ref not in station_id_map]
        if missing_station_refs:
            print(f"Fetching {len(missing_station_refs)} stations by id for service {service_ref}.")
            fetched: List[Dict[str, Any]] = []
            for idx, ref in enumerate(sorted(missing_station_refs), start=1):
                detail = client.station_detail(str(ref))
                if detail:
                    fetched.append(detail)
                if args.sleep_seconds:
                    time.sleep(max(0.0, args.sleep_seconds))
                if idx % 50 == 0:
                    print(".", end="", flush=True)
            if len(missing_station_refs) >= 50:
                print()
            if fetched:
                category_map = writer.upsert_reference_table(
                    "categories",
                    "category_ref",
                    collect_categories(fetched),
                    connector_id,
                )
                writer.upsert_stations(
                    fetched,
                    connector_id,
                    service_ref,
                    category_map,
                    bbox=UK_BBOX,
                )
                station_id_map = writer.get_station_id_map(
                    connector_id, service_ref, list(station_refs)
                )
        station_index, station_geometry_by_id = fetch_station_geometry_index(
            writer,
            connector_id,
            service_ref,
        )
        created_refs: Set[str] = set()
        created_rows: List[Dict[str, Any]] = []
        for detail in details_by_ref.values():
            station_ref = detail.get("station_ref")
            if not station_ref:
                continue
            station_ref_str = str(station_ref)
            if station_ref_str in station_id_map or station_ref_str in created_refs:
                continue
            label = detail.get("label")
            station_name = _extract_station_name_from_label(label)
            if not station_name:
                continue
            station_label = _extract_station_descriptor_from_label(label) or station_name
            seed = infer_station_seed(station_index, station_name)
            if not seed:
                continue
            row: Dict[str, Any] = {
                "connector_id": connector_id,
                "service_ref": service_ref,
                "station_ref": station_ref_str,
                "label": station_label,
                "station_name": station_name,
                "geometry": seed["geometry"],
            }
            if seed.get("station_type"):
                row["station_type"] = seed["station_type"]
            if seed.get("region"):
                row["region"] = seed["region"]
            created_rows.append(row)
            created_refs.add(station_ref_str)
        if created_rows:
            writer.core.table("stations").upsert(
                created_rows,
                on_conflict="connector_id,service_ref,station_ref",
                returning="minimal",
            ).execute()
            print(
                "Created %s station row(s) from timeseries labels (service_ref %s)."
                % (len(created_rows), service_ref)
            )
            for row in created_rows:
                created_log.record(
                    {
                        "connector_id": connector_id,
                        "service_ref": service_ref,
                        "station_ref": row["station_ref"],
                        "label": row["label"],
                        "station_name": row.get("station_name"),
                        "geometry": row.get("geometry"),
                        "station_type": row.get("station_type"),
                        "region": row.get("region"),
                        "source": "timeseries_label_inferred",
                        "created_at": utcnow().isoformat(),
                    }
                )
                print(f"Created station {row['station_ref']} ({row['label']}).")
            station_id_map = writer.get_station_id_map(
                connector_id, service_ref, list(station_refs)
            )

        feature_id_map: Dict[str, int] = {}
        if feature_payloads:
            feature_id_map = writer.upsert_reference_table(
                "features",
                "feature_ref",
                feature_payloads,
                connector_id,
                service_ref,
            )

        station_label_map = fetch_station_label_map(writer, connector_id, service_ref)
        updates: List[Dict[str, Any]] = []
        skipped = 0
        matched_by_label = 0
        missing_samples: List[Dict[str, Optional[str]]] = []
        for ts_ref, detail in details_by_ref.items():
            station_ref = detail.get("station_ref")
            station_id = None
            if station_ref:
                station_id = station_id_map.get(str(station_ref))
            if not station_id and not station_ref:
                label = detail.get("label")
                descriptor = _extract_station_descriptor_from_label(label)
                if descriptor:
                    key = _normalize_station_label(descriptor)
                    ids = station_label_map.get(key) or []
                    chosen = choose_station_id_by_geometry(ids, station_geometry_by_id)
                    if chosen is not None:
                        station_id = chosen
                        matched_by_label += 1
                if not station_id:
                    station_name = _extract_station_name_from_label(label)
                    if station_name:
                        key = _normalize_station_label(station_name)
                        ids = station_label_map.get(key) or []
                        chosen = choose_station_id_by_geometry(ids, station_geometry_by_id)
                        if chosen is not None:
                            station_id = chosen
                            matched_by_label += 1
            if not station_id:
                skipped += 1
                if len(missing_samples) < 10:
                    missing_samples.append(
                        {
                            "timeseries_ref": ts_ref,
                            "station_ref": str(station_ref) if station_ref is not None else None,
                            "label": detail.get("label"),
                        }
                    )
                continue
            label = detail.get("label")
            if not label:
                skipped += 1
                if len(missing_samples) < 10:
                    missing_samples.append(
                        {
                            "timeseries_ref": ts_ref,
                            "station_ref": str(station_ref) if station_ref is not None else None,
                            "label": None,
                        }
                    )
                continue
            row: Dict[str, Any] = {
                "connector_id": connector_id,
                "service_ref": service_ref,
                "timeseries_ref": ts_ref,
                "label": label,
                "station_id": station_id,
            }
            feature_ref = detail.get("feature_ref")
            if feature_ref and str(feature_ref) in feature_id_map:
                row["feature_id"] = feature_id_map[str(feature_ref)]
            updates.append(row)

        if updates:
            writer.core.table("timeseries").upsert(
                updates,
                on_conflict="connector_id,service_ref,timeseries_ref",
                returning="minimal",
            ).execute()
            print(
                "Updated %s timeseries rows for service_ref %s (label matches %s, skipped %s)."
                % (len(updates), service_ref, matched_by_label, skipped)
            )
        else:
            print(f"No updates applied for service_ref {service_ref} (skipped {skipped}).")
        if missing_samples:
            print("First 10 timeseries still missing station mapping:")
            for sample in missing_samples:
                print(
                    "- timeseries_ref=%s station_ref=%s label=%s"
                    % (
                        sample["timeseries_ref"],
                        sample["station_ref"],
                        sample["label"],
                    )
                )

    created_log.finalize()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
