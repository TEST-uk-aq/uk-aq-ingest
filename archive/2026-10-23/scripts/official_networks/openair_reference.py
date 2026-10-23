#!/usr/bin/env python3
from __future__ import annotations

import argparse
import math
import os
import sys
import tempfile
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.official_networks.profiles import (
    PollutantSpec,
    get_profile,
    pollutant_spec_for_openair,
)
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client


SERVICE_REF = "default"


def _rows(response: Any) -> List[Dict[str, Any]]:
    data = response.data if hasattr(response, "data") else response.get("data")
    return [dict(row) for row in (data or [])]


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if bool(value != value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


def _float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _date_value(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _text(value)
    if not text or text.lower() in {"ongoing", "current", "present", "na", "nan", "nat"}:
        return None
    text = text[:10]
    for parser in (date.fromisoformat,):
        try:
            return parser(text)
        except ValueError:
            pass
    return None


def _is_active(row: Dict[str, Any], today: date) -> bool:
    start = _date_value(row.get("start_date"))
    raw_end = _text(row.get("end_date"))
    end = _date_value(row.get("end_date"))
    if start and start > today:
        return False
    if raw_end and raw_end.lower() in {"ongoing", "current", "present"}:
        return True
    return end is None or end >= today


def _iso_midnight(value: Optional[date]) -> Optional[str]:
    if value is None:
        return None
    return datetime(
        value.year, value.month, value.day, tzinfo=timezone.utc
    ).isoformat()


def _download_metadata(url: str) -> str:
    response = requests.get(
        url,
        timeout=60,
        headers={"User-Agent": "UK-AQ-official-network-reference/1.0"},
    )
    response.raise_for_status()
    handle = tempfile.NamedTemporaryFile(suffix=".RData", delete=False)
    try:
        handle.write(response.content)
        handle.close()
        return handle.name
    except Exception:
        handle.close()
        try:
            os.unlink(handle.name)
        except OSError:
            pass
        raise


def _load_frame(path: str):
    try:
        import pyreadr
    except ImportError as exc:
        raise RuntimeError(
            "pyreadr is required for official-network OpenAir metadata refresh"
        ) from exc

    result = pyreadr.read_r(path)
    frames = [
        frame for frame in result.values()
        if hasattr(frame, "columns") and hasattr(frame, "to_dict")
    ]
    if not frames:
        raise RuntimeError("OpenAir RData contained no data frame")
    return max(frames, key=lambda frame: len(frame))


def load_openair_rows(metadata_url: str) -> List[Dict[str, Any]]:
    path = _download_metadata(metadata_url)
    try:
        frame = _load_frame(path)
        return [
            {str(key): value for key, value in row.items()}
            for row in frame.to_dict(orient="records")
        ]
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


class ReferenceWriter:
    def __init__(self, connector_code: str) -> None:
        self.profile = get_profile(connector_code)
        self.client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core

    def verify_identity(self) -> Tuple[int, int]:
        connector_rows = _rows(
            self.core.table("connectors")
            .select("id,default_network_id")
            .eq("connector_code", self.profile.connector_code)
            .limit(1)
            .execute()
        )
        network_rows = _rows(
            self.core.table("networks")
            .select("id")
            .eq("network_code", self.profile.network_code)
            .limit(1)
            .execute()
        )
        if not connector_rows or not network_rows:
            raise RuntimeError(
                f"Missing connector/network reference for {self.profile.connector_code}"
            )
        connector_id = int(connector_rows[0]["id"])
        network_id = int(network_rows[0]["id"])
        if connector_id != self.profile.connector_id:
            raise RuntimeError(
                f"{self.profile.connector_code} connector id {connector_id} "
                f"does not match contracted id {self.profile.connector_id}"
            )
        if network_id != self.profile.network_id:
            raise RuntimeError(
                f"{self.profile.connector_code} network id {network_id} "
                f"does not match contracted id {self.profile.network_id}"
            )
        if int(connector_rows[0]["default_network_id"]) != network_id:
            raise RuntimeError(
                f"{self.profile.connector_code} default_network_id is inconsistent"
            )
        return connector_id, network_id

    def observed_properties(self, codes: Iterable[str]) -> Dict[str, Dict[str, Any]]:
        wanted = sorted(set(codes))
        response = (
            self.core.table("observed_properties")
            .select("id,code,canonical_uom")
            .in_("code", wanted)
            .execute()
        )
        rows = {str(row["code"]): row for row in _rows(response)}
        missing = sorted(set(wanted) - set(rows))
        if missing:
            raise RuntimeError(
                "Missing canonical observed properties: " + ",".join(missing)
            )
        return rows

    def upsert_stations(self, rows: List[Dict[str, Any]]) -> Dict[str, int]:
        if rows:
            for offset in range(0, len(rows), 100):
                self.core.table("stations").upsert(
                    rows[offset : offset + 100],
                    on_conflict="connector_id,station_ref",
                ).execute()
        connector_id = self.profile.connector_id
        refs = [str(row["station_ref"]) for row in rows]
        mapping: Dict[str, int] = {}
        for offset in range(0, len(refs), 200):
            chunk = refs[offset : offset + 200]
            if not chunk:
                continue
            response = (
                self.core.table("stations")
                .select("id,station_ref")
                .eq("connector_id", connector_id)
                .in_("station_ref", chunk)
                .execute()
            )
            for row in _rows(response):
                mapping[str(row["station_ref"])] = int(row["id"])
        return mapping

    def upsert_phenomena(
        self,
        connector_id: int,
        specs: Iterable[PollutantSpec],
        observed: Dict[str, Dict[str, Any]],
    ) -> Dict[str, int]:
        payload = []
        for spec in sorted(
            {item.source_series: item for item in specs}.values(),
            key=lambda item: item.source_series,
        ):
            property_row = observed[spec.observed_property_code]
            payload.append(
                {
                    "connector_id": connector_id,
                    "label": spec.source_series,
                    "source_label": spec.source_series,
                    "notation": spec.openair_variable,
                    "pollutant_label": spec.source_series,
                    "observed_property_id": int(property_row["id"]),
                }
            )
        if payload:
            self.core.table("phenomena").upsert(
                payload, on_conflict="connector_id,source_label"
            ).execute()
        response = (
            self.core.table("phenomena")
            .select("id,source_label")
            .eq("connector_id", connector_id)
            .in_("source_label", [row["source_label"] for row in payload])
            .execute()
        ) if payload else None
        return {
            str(row["source_label"]): int(row["id"])
            for row in (_rows(response) if response is not None else [])
        }

    def upsert_timeseries(self, rows: List[Dict[str, Any]]) -> int:
        for offset in range(0, len(rows), 100):
            self.core.table("timeseries").upsert(
                rows[offset : offset + 100],
                on_conflict="connector_id,timeseries_ref",
            ).execute()
        return len(rows)


def build_reference_rows(
    source_rows: List[Dict[str, Any]],
    connector_id: int,
    network_id: int,
) -> Tuple[List[Dict[str, Any]], Dict[Tuple[str, str], Dict[str, Any]], List[PollutantSpec]]:
    today = datetime.now(timezone.utc).date()
    by_site: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in source_rows:
        site_code = _text(row.get("site_id"))
        if not site_code:
            site_code = _text(row.get("code"))
        if site_code:
            by_site[site_code.upper()].append(row)

    station_rows: List[Dict[str, Any]] = []
    series_groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
    used_specs: List[PollutantSpec] = []

    for site_code, rows in sorted(by_site.items()):
        site_name = next(
            (
                value
                for value in (
                    (_text(row.get("site_name")) or _text(row.get("site")))
                    for row in rows
                )
                if value
            ),
            site_code,
        )
        latitude = next(
            (value for value in (_float(row.get("latitude")) for row in rows) if value is not None),
            None,
        )
        longitude = next(
            (value for value in (_float(row.get("longitude")) for row in rows) if value is not None),
            None,
        )
        site_type = next(
            (
                value
                for value in (
                    (_text(row.get("location_type")) or _text(row.get("site_type")))
                    for row in rows
                )
                if value
            ),
            None,
        )
        starts = [
            value for value in (_date_value(row.get("start_date")) for row in rows)
            if value is not None
        ]
        active = any(_is_active(row, today) for row in rows)
        ends = [
            value for value in (_date_value(row.get("end_date")) for row in rows)
            if value is not None
        ]
        station_row: Dict[str, Any] = {
            "connector_id": connector_id,
            "network_id": network_id,
            "service_ref": SERVICE_REF,
            "station_ref": site_code,
            "label": site_name,
            "station_name": site_name,
            "station_type": site_type,
            "latitude": latitude,
            "longitude": longitude,
            "geometry": (
                f"SRID=4326;POINT({longitude} {latitude})"
                if latitude is not None and longitude is not None
                else None
            ),
            "removed_at": None if active else _iso_midnight(max(ends) if ends else None),
            "priority": 10,
        }
        if starts:
            station_row["first_seen_at"] = _iso_midnight(min(starts))
        if active:
            station_row["last_seen_at"] = datetime.now(timezone.utc).isoformat()
        station_rows.append(station_row)

        for row in rows:
            source_variable = _text(row.get("parameter"))
            if not source_variable:
                source_variable = _text(row.get("variable"))
            spec = pollutant_spec_for_openair(source_variable)
            if spec is None:
                continue
            used_specs.append(spec)
            key = (site_code, spec.observed_property_code)
            candidate = series_groups.setdefault(
                key,
                {
                    "site_code": site_code,
                    "site_name": site_name,
                    "spec": spec,
                    "starts": [],
                    "ends": [],
                    "active": False,
                },
            )
            start = _date_value(row.get("start_date"))
            end = _date_value(row.get("end_date"))
            if start:
                candidate["starts"].append(start)
            if end:
                candidate["ends"].append(end)
            if _is_active(row, today):
                candidate["active"] = True

    return station_rows, series_groups, used_specs


def refresh(connector_code: str, dry_run: bool = False) -> Dict[str, Any]:
    profile = get_profile(connector_code)
    source_rows = load_openair_rows(profile.openair_metadata_url)
    writer = ReferenceWriter(connector_code)
    connector_id, network_id = writer.verify_identity()
    station_rows, series_groups, used_specs = build_reference_rows(
        source_rows, connector_id, network_id
    )
    supported_codes = sorted({spec.observed_property_code for spec in used_specs})
    observed = writer.observed_properties(supported_codes) if supported_codes else {}

    if dry_run:
        return {
            "connector_code": connector_code,
            "source_rows": len(source_rows),
            "stations": len(station_rows),
            "timeseries": len(series_groups),
            "pollutants": supported_codes,
            "dry_run": True,
        }

    station_ids = writer.upsert_stations(station_rows)
    missing_stations = sorted(
        {row["station_ref"] for row in station_rows} - set(station_ids)
    )
    if missing_stations:
        raise RuntimeError(
            "Failed to resolve station ids: " + ",".join(missing_stations[:20])
        )

    phenomenon_ids = writer.upsert_phenomena(connector_id, used_specs, observed)
    timeseries_rows: List[Dict[str, Any]] = []
    for (site_code, pollutant_code), group in sorted(series_groups.items()):
        spec: PollutantSpec = group["spec"]
        phenomenon_id = phenomenon_ids.get(spec.source_series)
        if phenomenon_id is None:
            raise RuntimeError(
                f"Missing phenomenon id for {connector_code}/{spec.source_series}"
            )
        property_row = observed[pollutant_code]
        first_date = min(group["starts"]) if group["starts"] else None
        end_date = max(group["ends"]) if group["ends"] else None
        active = bool(group["active"])
        timeseries_rows.append(
            {
                "connector_id": connector_id,
                "service_ref": SERVICE_REF,
                "timeseries_ref": f"{site_code}:{pollutant_code}",
                "label": f"{group['site_name']} - {spec.source_series}",
                "uom": spec.uom,
                "station_id": station_ids[site_code],
                "phenomenon_id": phenomenon_id,
                "observed_property_id": int(property_row["id"]),
                "first_value_at": _iso_midnight(first_date),
                "ended_at": None if active else _iso_midnight(end_date),
                "last_catalog_seen_at": datetime.now(timezone.utc).isoformat(),
                "catalog_missing_runs": 0,
                "extras": {
                    "site_code": site_code,
                    "source_series": spec.source_series,
                    "openair_variable": spec.openair_variable,
                    "reference_source": "openair_metadata",
                },
                "metadata": {
                    "reference_source": "openair_metadata",
                    "openair_metadata_url": profile.openair_metadata_url,
                },
            }
        )

    writer.upsert_timeseries(timeseries_rows)
    return {
        "connector_code": connector_code,
        "source_rows": len(source_rows),
        "stations": len(station_rows),
        "active_stations": sum(1 for row in station_rows if row.get("removed_at") is None),
        "timeseries": len(timeseries_rows),
        "active_timeseries": sum(1 for row in timeseries_rows if row.get("ended_at") is None),
        "pollutants": supported_codes,
        "dry_run": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh WAQN/SAQN/NI station and timeseries references from OpenAir metadata."
    )
    parser.add_argument("--connector-code", required=True, choices=("waqn", "saqn", "ni"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-if-unconfigured", action="store_true")
    args = parser.parse_args()
    try:
        summary = refresh(args.connector_code, dry_run=args.dry_run)
    except RuntimeError as exc:
        if args.skip_if_unconfigured and str(exc).startswith(
            "Missing connector/network reference"
        ):
            summary = {
                "connector_code": args.connector_code,
                "skipped": True,
                "reason": "connector_or_network_not_configured",
            }
        else:
            raise
    print("REFERENCE_SUMMARY_JSON " + __import__("json").dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
