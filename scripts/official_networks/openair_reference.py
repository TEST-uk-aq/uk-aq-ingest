#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import tempfile
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.official_networks.profiles import (
    PollutantSpec,
    PROPERTY_TO_SPEC,
    get_profile,
    pollutant_spec_for_openair,
)
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client
from scripts.stations_daily.reference_source import (
    SOURCE_ATTEMPTS,
    SOURCE_RETRY_DELAYS,
    SourceResponseError,
    SourceUnavailable,
    acquire_reference_bytes,
)


SERVICE_REF = "default"
class RetainedReferenceSourceUnavailable(SourceUnavailable):
    """Only raised after the independent read-only safety gate succeeds."""


class VerificationFailure(str, Enum):
    """Fixed diagnostic vocabulary; never include database or exception text."""

    WRITE_BOUNDARY = "write_boundary"
    IDENTITY = "identity"
    STATION_IDENTITY = "station_identity"
    STATION_RETIREMENT = "station_retirement"
    OWNERSHIP = "ownership"
    PHENOMENON_IDENTITY = "phenomenon_identity"
    PROPERTY_MAPPING = "property_mapping"
    CANONICAL_UNIT = "canonical_unit"
    TIMESERIES_IDENTITY = "timeseries_identity"
    TIMESERIES_UNIT = "timeseries_unit"
    PROVENANCE = "provenance"
    SOURCE_TIMESTAMP = "source_timestamp"
    REQUIRED_REFERENCES = "required_references"
    ACTIVE_STATION_COVERAGE = "active_station_coverage"
    DATABASE_READ = "database_read"
    UNEXPECTED_ERROR = "unexpected_verification_error"


class RetainedReferenceError(RuntimeError):
    """A fatal verification denial carrying only a bounded, safe reason code."""

    def __init__(self, reason: VerificationFailure) -> None:
        self.reason = reason
        super().__init__(reason.value)


class UnconfiguredConnector(RuntimeError):
    """A genuinely absent connector, rather than damaged configured authority."""


def _rows(response: Any) -> List[Dict[str, Any]]:
    data = response.data if hasattr(response, "data") else response.get("data")
    return [dict(row) for row in (data or [])]


def _reference_rows(query: Any) -> List[Dict[str, Any]]:
    try:
        return _rows(query.execute())
    except Exception:
        # Never expose database exceptions or classify them as source outages.
        raise RetainedReferenceError(VerificationFailure.DATABASE_READ) from None


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


def _download_metadata(url: str, *, canonical_origin: Optional[str] = None) -> str:
    body = acquire_reference_bytes(
        requests, url, headers={"User-Agent": "UK-AQ-official-network-reference/1.0"},
        canonical_origin=canonical_origin, official_maintenance=True,
    )
    handle = tempfile.NamedTemporaryFile(suffix=".RData", delete=False)
    try:
        handle.write(body)
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

    # Only this exact reader exception at this boundary can qualify.
    try:
        result = pyreadr.read_r(path)
    except pyreadr.custom_errors.LibrdataError:
        raise SourceUnavailable("unreadable_rdata") from None
    frames = [
        frame for frame in result.values()
        if hasattr(frame, "columns") and hasattr(frame, "to_dict")
    ]
    if not frames:
        raise RuntimeError("OpenAir RData contained no data frame")
    return max(frames, key=lambda frame: len(frame))


def load_openair_rows(
    metadata_url: str, *, canonical_origin: Optional[str] = None
) -> List[Dict[str, Any]]:
    for attempt in range(1, SOURCE_ATTEMPTS + 1):
        path = None
        try:
            path = _download_metadata(metadata_url, canonical_origin=canonical_origin)
            frame = _load_frame(path)
        except SourceUnavailable as exc:
            print(f"OpenAir source attempt={attempt}/{SOURCE_ATTEMPTS} failure_class={exc.failure_class}", flush=True)
            if attempt == SOURCE_ATTEMPTS:
                raise SourceUnavailable(exc.failure_class, attempt) from None
            time.sleep(SOURCE_RETRY_DELAYS[attempt - 1])
            continue
        finally:
            if path is not None:
                try:
                    os.unlink(path)
                except OSError:
                    # Best-effort cleanup must not mask the source/reader error.
                    pass
        # Parsed-frame/schema/application failures are outside the retry boundary.
        columns = set(frame.columns)
        if (
            not columns.intersection(("site_id", "code"))
            or not columns.intersection(("parameter", "variable")) or len(frame) == 0
        ):
            raise RuntimeError("OpenAir metadata is empty or missing mandatory source fields")
        return [
            {str(key): value for key, value in row.items()}
            for row in frame.to_dict(orient="records")
        ]
    raise AssertionError("unreachable")


class ReferenceWriter:
    def __init__(self, connector_code: str) -> None:
        self.profile = get_profile(connector_code)
        self.client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core
        self.write_attempted = False

    def verify_identity(self) -> Tuple[int, int]:
        connector_rows = _reference_rows(
            self.core.table("connectors")
            .select("id,connector_code,default_network_id")
            .or_(
                f"connector_code.eq.{self.profile.connector_code},"
                f"id.eq.{self.profile.connector_id},"
                f"default_network_id.eq.{self.profile.network_id}"
            )
        )
        network_rows = _reference_rows(
            self.core.table("networks")
            .select("id,network_code")
            .or_(f"network_code.eq.{self.profile.network_code},id.eq.{self.profile.network_id}")
        )
        if network_rows and (
            len(network_rows) != 1
            or network_rows[0].get("network_code") != self.profile.network_code
            or int(network_rows[0]["id"]) != self.profile.network_id
        ):
            raise RuntimeError("Contradictory official-network identity")
        if not connector_rows:
            raise UnconfiguredConnector("Official-network connector is not configured")
        if (
            len(connector_rows) != 1
            or connector_rows[0].get("connector_code") != self.profile.connector_code
            or not network_rows
        ):
            raise RuntimeError(
                "Missing/contradictory configured official-network authority"
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

    def _retained_rows(self, table: str, columns: str, ownership: str) -> List[Dict[str, Any]]:
        # Explicit pagination avoids treating a capped PostgREST response as complete.
        result: List[Dict[str, Any]] = []
        while True:
            page = _reference_rows(
                self.core.table(table).select(columns).or_(ownership)
                .order("id").range(len(result), len(result) + 499)
            )
            result.extend(page)
            if not page:
                return result

    def verify_retained_reference_state(self) -> Dict[str, int]:
        """Read existing authority only; never bootstrap, repair or refresh rows."""
        if self.write_attempted:
            raise RetainedReferenceError(VerificationFailure.WRITE_BOUNDARY)
        try:
            connector_id, network_id = self.verify_identity()
        except RetainedReferenceError:
            raise
        except (RuntimeError, ValueError, TypeError, KeyError):
            raise RetainedReferenceError(VerificationFailure.IDENTITY) from None
        station_rows = self._retained_rows(
            "stations", "id,connector_id,network_id,service_ref,station_ref,label,station_name,removed_at",
            f"connector_id.eq.{connector_id},network_id.eq.{network_id}",
        )
        stations: Dict[int, str] = {}
        active_stations = set()
        now = datetime.now(timezone.utc)
        for row in station_rows:
            ref = row.get("station_ref")
            if (
                int(row.get("connector_id") or 0) != connector_id
                or int(row.get("network_id") or 0) != network_id
                or row.get("service_ref") != SERVICE_REF
                or not isinstance(ref, str) or not ref or ref != ref.strip().upper()
                or re.search(r"[\s:\x00-\x1f\x7f]", ref) or len(ref) > 128
                or not _text(row.get("label")) or not _text(row.get("station_name"))
                or ref in stations.values() or int(row["id"]) in stations
            ):
                raise RetainedReferenceError(VerificationFailure.STATION_IDENTITY)
            station_id = int(row["id"])
            # Missing status is ambiguous; only an explicit NULL denotes active.
            if "removed_at" not in row:
                raise RetainedReferenceError(VerificationFailure.STATION_RETIREMENT)
            if row["removed_at"] is None:
                active_stations.add(station_id)
            else:
                try:
                    removed_at = datetime.fromisoformat(
                        str(row["removed_at"]).replace("Z", "+00:00")
                    )
                except (ValueError, TypeError):
                    raise RetainedReferenceError(VerificationFailure.STATION_RETIREMENT) from None
                if removed_at.tzinfo is None or removed_at >= now:
                    raise RetainedReferenceError(VerificationFailure.STATION_RETIREMENT)
            stations[station_id] = ref
        if not stations:
            raise RetainedReferenceError(VerificationFailure.REQUIRED_REFERENCES)
        station_ids = list(stations)
        for offset in range(0, len(station_ids), 200):
            foreign = _reference_rows(
                self.core.table("timeseries").select("id")
                .in_("station_id", station_ids[offset:offset + 200])
                .neq("connector_id", connector_id).limit(1)
            )
            if foreign:
                raise RetainedReferenceError(VerificationFailure.OWNERSHIP)

        phenomena = self._retained_rows(
            "phenomena", "id,connector_id,label,source_label,notation,pollutant_label,observed_property_id",
            f"connector_id.eq.{connector_id}",
        )
        specs_by_source = {spec.source_series: spec for spec in PROPERTY_TO_SPEC.values()}
        phenomena_by_id: Dict[int, Dict[str, Any]] = {}
        sources = set()
        for row in phenomena:
            source = row.get("source_label")
            spec = specs_by_source.get(source)
            if (
                spec is None or source in sources or int(row["id"]) in phenomena_by_id
                or int(row.get("connector_id") or 0) != connector_id
                or row.get("notation") != spec.openair_variable
                or row.get("label") != spec.source_series
                or row.get("pollutant_label") != spec.source_series
            ):
                raise RetainedReferenceError(VerificationFailure.PHENOMENON_IDENTITY)
            sources.add(source)
            phenomena_by_id[int(row["id"])] = row
        if not phenomena:
            raise RetainedReferenceError(VerificationFailure.REQUIRED_REFERENCES)
        required_codes = {specs_by_source[row["source_label"]].observed_property_code for row in phenomena}
        property_rows = _reference_rows(
            self.core.table("observed_properties").select("id,code,canonical_uom")
            .in_("code", sorted(required_codes))
        )
        properties = {row["code"]: row for row in property_rows}
        if len(properties) != len(property_rows) or set(properties) != required_codes:
            raise RetainedReferenceError(VerificationFailure.PROPERTY_MAPPING)
        for row in phenomena:
            spec = specs_by_source[row["source_label"]]
            property_row = properties[spec.observed_property_code]
            # Global units may be unspecified; the approved source unit still
            # governs every timeseries below. A defined contradiction is fatal.
            canonical_uom = property_row["canonical_uom"]
            if canonical_uom is not None and canonical_uom != spec.uom:
                raise RetainedReferenceError(VerificationFailure.CANONICAL_UNIT)
            if int(row.get("observed_property_id") or 0) != int(property_row["id"]):
                raise RetainedReferenceError(VerificationFailure.PROPERTY_MAPPING)

        timeseries = self._retained_rows(
            "timeseries", "id,connector_id,service_ref,timeseries_ref,station_id,phenomenon_id,"
            "observed_property_id,uom,extras,metadata,last_catalog_seen_at",
            f"connector_id.eq.{connector_id}",
        )
        refs = set()
        series_ids = set()
        used_phenomena = set()
        used_stations = set()
        for row in timeseries:
            station_ref = stations.get(int(row.get("station_id") or 0))
            phenomenon_id = int(row.get("phenomenon_id") or 0)
            phenomenon = phenomena_by_id.get(phenomenon_id)
            if station_ref is None or phenomenon is None:
                raise RetainedReferenceError(VerificationFailure.OWNERSHIP)
            spec = specs_by_source[phenomenon["source_label"]]
            expected_ref = f"{station_ref}:{spec.observed_property_code}"
            extras, metadata = row.get("extras"), row.get("metadata")
            if (
                int(row.get("connector_id") or 0) != connector_id
                or row.get("service_ref") != SERVICE_REF or row.get("timeseries_ref") != expected_ref
                or expected_ref in refs or int(row["id"]) in series_ids
                or int(row.get("observed_property_id") or 0) != int(properties[spec.observed_property_code]["id"])
            ):
                raise RetainedReferenceError(VerificationFailure.TIMESERIES_IDENTITY)
            if row.get("uom") != spec.uom:
                raise RetainedReferenceError(VerificationFailure.TIMESERIES_UNIT)
            if (
                not isinstance(extras, dict) or not isinstance(metadata, dict)
                or any(extras.get(key) != value for key, value in {
                    "site_code": station_ref, "source_series": spec.source_series,
                    "openair_variable": spec.openair_variable, "reference_source": "openair_metadata",
                }.items())
                or metadata.get("reference_source") != "openair_metadata"
                or metadata.get("openair_metadata_url") != self.profile.openair_metadata_url
            ):
                raise RetainedReferenceError(VerificationFailure.PROVENANCE)
            try:
                snapshot = datetime.fromisoformat(
                    str(row.get("last_catalog_seen_at")).replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                raise RetainedReferenceError(VerificationFailure.SOURCE_TIMESTAMP) from None
            if snapshot.tzinfo is None:
                raise RetainedReferenceError(VerificationFailure.SOURCE_TIMESTAMP)
            refs.add(expected_ref)
            series_ids.add(int(row["id"]))
            used_phenomena.add(phenomenon_id)
            used_stations.add(int(row["station_id"]))
        if not active_stations.issubset(used_stations):
            raise RetainedReferenceError(VerificationFailure.ACTIVE_STATION_COVERAGE)
        if not timeseries or used_phenomena != set(phenomena_by_id):
            raise RetainedReferenceError(VerificationFailure.REQUIRED_REFERENCES)
        return {"stations": len(stations), "phenomena": len(phenomena), "timeseries": len(timeseries)}

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
                self.write_attempted = True
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
            self.write_attempted = True
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
            self.write_attempted = True
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


def refresh(
    connector_code: str, dry_run: bool = False, *, isolate_source_failure: bool = False
) -> Dict[str, Any]:
    profile = get_profile(connector_code)
    # This catch ends before writer/mapping work. No later error, including an
    # uncertain write outcome, can reach the source-isolation path.
    try:
        source_rows = load_openair_rows(
            profile.openair_metadata_url,
            canonical_origin=profile.openair_canonical_origin,
        )
    except SourceUnavailable as exc:
        if not isolate_source_failure:
            raise
        try:
            retained = ReferenceWriter(connector_code).verify_retained_reference_state()
        except RetainedReferenceError:
            raise
        except Exception:
            # A database/identity/integrity failure is always fatal, never a source
            # classification. Do not expose DB messages, URLs or payloads.
            raise RetainedReferenceError(VerificationFailure.UNEXPECTED_ERROR) from None
        print("RETAINED_REFERENCE_VERIFIED_JSON " + json.dumps({
            "connector_code": connector_code, **retained,
        }, sort_keys=True), flush=True)
        raise RetainedReferenceSourceUnavailable(exc.failure_class, exc.attempts) from None
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


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Refresh WAQN/SAQN/NI station and timeseries references from OpenAir metadata."
    )
    parser.add_argument("--connector-code", required=True, choices=("waqn", "saqn", "ni"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-if-unconfigured", action="store_true")
    parser.add_argument(
        "--daily-stations-isolate-source-failure", action="store_true",
        help="Allow exit 75 only after a qualified pre-write source failure and verified retained references.",
    )
    args = parser.parse_args(argv)
    if args.dry_run and args.daily_stations_isolate_source_failure:
        parser.error("Daily Stations source isolation cannot be combined with --dry-run")
    try:
        summary = refresh(
            args.connector_code, dry_run=args.dry_run,
            isolate_source_failure=args.daily_stations_isolate_source_failure,
        )
    except RetainedReferenceSourceUnavailable as exc:
        reason = (
            "OpenAir metadata source could not be read by the RData reader; existing verified reference rows were retained."
            if exc.failure_class == "unreadable_rdata" else
            "OpenAir reference source refresh could not complete; existing verified reference rows were retained."
        )
        print("REFERENCE_SUMMARY_JSON " + json.dumps({
            "connector_code": args.connector_code, "ok": False, "reason": reason,
            "failure_class": exc.failure_class, "attempts": exc.attempts,
            "reference_state_retained": True,
        }, sort_keys=True), flush=True)
        if os.getenv("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
                handle.write(f"failure_class={exc.failure_class}\n")
                handle.write("retained_references_verified=true\n")
        return 75
    except RetainedReferenceError as exc:
        print("RETAINED_REFERENCE_VERIFICATION_FAILED_JSON " + json.dumps({
            "connector_code": args.connector_code, "reason_code": exc.reason.value,
        }, sort_keys=True), file=sys.stderr, flush=True)
        print(f"OpenAir retained-reference verification failed ({exc}); isolation denied.", file=sys.stderr)
        return 2
    except UnconfiguredConnector:
        if args.skip_if_unconfigured:
            summary = {
                "connector_code": args.connector_code,
                "skipped": True,
                "reason": "connector_or_network_not_configured",
            }
        else:
            print("OpenAir connector is not configured.", file=sys.stderr)
            return 1
    except SourceUnavailable as exc:
        print(f"OpenAir source failure_class={exc.failure_class}; isolation denied at this boundary.", file=sys.stderr)
        return 1
    except SourceResponseError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        # Fatal reporting only: this does not grant retries, retention or exit 75.
        print(f"OpenAir reference refresh failed ({type(exc).__name__}).", file=sys.stderr)
        return 1
    print("REFERENCE_SUMMARY_JSON " + json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
