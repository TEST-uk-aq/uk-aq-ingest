#!/usr/bin/env python3
"""Resolve source stations onto canonical UK-AIR physical monitoring sites.

The latest valid AURN rows in ``uk_aq_raw.sos_site_register`` are the
authority. SOS membership reuses the existing station bridge; devolved
official-network membership uses exact site codes or deliberately conservative
geographic evidence. Ambiguous rows remain unmatched.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.official_networks.profiles import get_profile
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client


LOG = logging.getLogger("canonical_site_resolver")
AURN_NETWORK = "Automatic Urban and Rural Monitoring Network (AURN)"
REGIONAL_CONNECTOR_CODES = ("waqn", "saqn", "ni")
MAX_DIFFERENT_CODE_DISTANCE_M = 50.0
COORDINATE_TOLERANCE = 1e-9
SCHEMA_MIGRATION = "20261003_001_ingest_canonical_physical_site_identity.sql"


def _rows(response: Any) -> List[Dict[str, Any]]:
    data = response.data if hasattr(response, "data") else response.get("data")
    return [dict(row) for row in (data or [])]


def _text(value: Any) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _upper(value: Any) -> Optional[str]:
    value = _text(value)
    return value.upper() if value else None


def _float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _date_value(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _valid_uk_air_ref(value: Any) -> Optional[str]:
    value = _upper(value)
    return value if value and re.fullmatch(r"UKA[0-9]{5}", value) else None


def coordinates_identical(
    station: Mapping[str, Any], site: Mapping[str, Any]
) -> bool:
    station_lat = _float(station.get("latitude"))
    station_lon = _float(station.get("longitude"))
    site_lat = _float(site.get("latitude"))
    site_lon = _float(site.get("longitude"))
    if None in (station_lat, station_lon, site_lat, site_lon):
        return False
    return (
        abs(station_lat - site_lat) <= COORDINATE_TOLERANCE
        and abs(station_lon - site_lon) <= COORDINATE_TOLERANCE
    )


def _distance_m(station: Mapping[str, Any], site: Mapping[str, Any]) -> Optional[float]:
    station_lat = _float(station.get("latitude"))
    station_lon = _float(station.get("longitude"))
    site_lat = _float(site.get("latitude"))
    site_lon = _float(site.get("longitude"))
    if None in (station_lat, station_lon, site_lat, site_lon):
        return None
    lat1 = math.radians(station_lat)
    lat2 = math.radians(site_lat)
    delta_lat = lat2 - lat1
    delta_lon = math.radians(site_lon - station_lon)
    haversine = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    return 6_371_008.8 * 2 * math.atan2(
        math.sqrt(haversine), math.sqrt(max(0.0, 1 - haversine))
    )


@dataclass(frozen=True)
class MatchDecision:
    status: str
    uk_air_ref: Optional[str] = None
    method: Optional[str] = None
    distance_m: Optional[float] = None
    evidence: Dict[str, Any] = field(default_factory=dict)


OfficialEvidence = Callable[
    [Mapping[str, Any], Mapping[str, Any]], Mapping[str, Any]
]


def choose_regional_match(
    station: Mapping[str, Any],
    aurn_sites: Sequence[Mapping[str, Any]],
    official_evidence: Optional[OfficialEvidence] = None,
) -> MatchDecision:
    """Return one conservative canonical-site decision for a regional station."""
    if _text(station.get("removed_at")):
        return MatchDecision(
            status="skipped_removed",
            evidence={"reason": "regional_station_already_removed"},
        )
    station_ref = _upper(station.get("station_ref"))
    exact = [site for site in aurn_sites if _upper(site.get("site_ref")) == station_ref]
    if len(exact) == 1:
        uk_air_ref = _valid_uk_air_ref(exact[0].get("uk_air_ref"))
        if uk_air_ref:
            return MatchDecision(
                status="matched",
                uk_air_ref=uk_air_ref,
                method="regional_exact_site_ref",
                distance_m=_distance_m(station, exact[0]),
                evidence={"kind": "exact_site_ref", "site_ref": station_ref},
            )
    if len(exact) > 1:
        return MatchDecision(
            status="ambiguous",
            evidence={"reason": "multiple_exact_site_ref_candidates", "count": len(exact)},
        )

    nearby = []
    for site in aurn_sites:
        if site.get("_current_at_snapshot") is False:
            continue
        distance = _distance_m(station, site)
        if distance is not None and distance <= MAX_DIFFERENT_CODE_DISTANCE_M:
            nearby.append((site, distance))
    nearby.sort(key=lambda item: (item[1], _valid_uk_air_ref(item[0].get("uk_air_ref")) or ""))
    if len(nearby) > 1:
        return MatchDecision(
            status="ambiguous",
            evidence={"reason": "multiple_candidates_within_50m", "count": len(nearby)},
        )
    if not nearby:
        return MatchDecision(
            status="unmatched", evidence={"reason": "no_candidate_within_50m"}
        )

    site, distance = nearby[0]
    uk_air_ref = _valid_uk_air_ref(site.get("uk_air_ref"))
    if not uk_air_ref:
        return MatchDecision(
            status="unmatched", evidence={"reason": "candidate_has_invalid_uk_air_ref"}
        )
    if coordinates_identical(station, site):
        return MatchDecision(
            status="matched",
            uk_air_ref=uk_air_ref,
            method="regional_unique_50m_identical_coordinates",
            distance_m=distance,
            evidence={"kind": "identical_published_coordinates"},
        )
    evidence = dict(official_evidence(station, site)) if official_evidence else {}
    if evidence.get("accepted") is True:
        evidence.pop("accepted", None)
        return MatchDecision(
            status="matched",
            uk_air_ref=uk_air_ref,
            method="regional_unique_50m_official_evidence",
            distance_m=distance,
            evidence=evidence,
        )
    evidence.pop("accepted", None)
    return MatchDecision(
        status="unmatched",
        distance_m=distance,
        evidence={"reason": "stronger_official_evidence_missing", **evidence},
    )


class _VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        if tag.lower() in {"script", "style", "noscript"}:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript"} and self._ignored_depth:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._ignored_depth:
            self.parts.append(data)


def fetch_regional_page_evidence(
    connector_code: str,
    station: Mapping[str, Any],
    site: Mapping[str, Any],
) -> Dict[str, Any]:
    station_ref = _upper(station.get("station_ref"))
    if not station_ref:
        return {"accepted": False, "reason": "station_ref_missing"}
    url = get_profile(connector_code).site_url(station_ref)
    try:
        response = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": "UK-AQ-canonical-site-resolver/1.0"},
        )
        response.raise_for_status()
    except requests.RequestException as error:
        LOG.warning("Official evidence request failed for %s: %s", url, error)
        return {"accepted": False, "reason": "official_page_request_failed", "source_url": url}

    parser = _VisibleTextParser()
    parser.feed(response.text)
    visible_text = re.sub(r"\s+", " ", " ".join(parser.parts)).strip().lower()
    candidate_phrases = [
        _text(site.get("uk_air_ref")),
        _text(site.get("site_ref")),
        _text(site.get("site_name")),
    ]
    relation_markers = (r"aurn\s+site", r"aurn\s+counterpart", r"counterpart\s+aurn")
    has_explicit_relation = False
    for candidate in candidate_phrases:
        if not candidate:
            continue
        escaped_candidate = re.escape(candidate.lower())
        if any(
            re.search(rf"{marker}.{{0,240}}{escaped_candidate}", visible_text)
            or re.search(rf"{escaped_candidate}.{{0,240}}{marker}", visible_text)
            for marker in relation_markers
        ):
            has_explicit_relation = True
            break
    return {
        "accepted": has_explicit_relation,
        "kind": "regional_page_aurn_counterpart",
        "source_url": url,
        "station_ref": station_ref,
        "candidate_site_ref": _upper(site.get("site_ref")),
        "candidate_uk_air_ref": _valid_uk_air_ref(site.get("uk_air_ref")),
    }


def _chunks(values: Sequence[int], size: int = 200) -> Iterable[Sequence[int]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _all_rows(query_factory: Callable[[], Any], page_size: int = 1000) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    offset = 0
    while True:
        page = _rows(
            query_factory().range(offset, offset + page_size - 1).execute()
        )
        output.extend(page)
        if len(page) < page_size:
            return output
        offset += page_size


def merge_member_evidence(
    existing_identity: Mapping[str, Any],
    members: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    evidence_by_station: Dict[int, Dict[str, Any]] = {}
    previous_evidence = existing_identity.get("member_evidence")
    if isinstance(previous_evidence, list):
        for item in previous_evidence:
            if not isinstance(item, dict):
                continue
            try:
                station_id = int(item["station_id"])
            except (KeyError, TypeError, ValueError):
                continue
            evidence_by_station[station_id] = dict(item)
    for item in members:
        try:
            station_id = int(item["station_id"])
        except (KeyError, TypeError, ValueError):
            continue
        evidence_by_station[station_id] = dict(item)
    return [
        evidence_by_station[station_id]
        for station_id in sorted(evidence_by_station)
    ]


def schema_prerequisite_message(error: BaseException) -> Optional[str]:
    """Translate missing canonical identity objects into an operator action."""
    text = str(error).lower()
    has_schema_identifier = any(
        identifier in text for identifier in ("station_matches", "uk_air_ref", "match_id")
    )
    has_missing_object_signal = any(
        signal in text
        for signal in (
            "pgrst204",
            "42p01",
            "42703",
            "could not find",
            "does not exist",
            "undefined column",
            "undefined table",
        )
    )
    if not (has_schema_identifier and has_missing_object_signal):
        return None
    return (
        "Canonical physical-site schema is not ready. Apply ingest migration "
        f"{SCHEMA_MIGRATION} to TEST, refresh the PostgREST schema cache if needed, "
        "then rerun Daily Stations."
    )


class CanonicalSiteResolver:
    def __init__(self, *, dry_run: bool = False) -> None:
        self.schemas = SupabaseSchemas.from_client(create_supabase_client())
        self.core = self.schemas.core
        self.raw = self.schemas.raw
        self.dry_run = dry_run

    def _latest_aurn_sites(self) -> tuple[Optional[str], List[Dict[str, Any]]]:
        latest = _rows(
            self.raw.table("sos_site_register")
            .select("snapshot_at")
            .order("snapshot_at", desc=True)
            .limit(1)
            .execute()
        )
        if not latest:
            return None, []
        snapshot_at = _text(latest[0].get("snapshot_at"))
        rows = _all_rows(
            lambda: self.raw.table("sos_site_register")
                .select(
                    "id,uk_air_ref,site_ref,site_name,latitude,longitude,networks,"
                    "start_date,end_date,snapshot_at,source_url"
                )
                .eq("snapshot_at", snapshot_at)
                .order("id")
        )
        valid = []
        snapshot_date = _date_value(snapshot_at)
        for row in rows:
            networks = row.get("networks")
            if not isinstance(networks, list) or AURN_NETWORK not in networks:
                continue
            uk_air_ref = _valid_uk_air_ref(row.get("uk_air_ref"))
            site_ref = _upper(row.get("site_ref"))
            site_name = _text(row.get("site_name"))
            latitude = _float(row.get("latitude"))
            longitude = _float(row.get("longitude"))
            if (
                not uk_air_ref
                or not site_ref
                or not site_name
                or latitude is None
                or longitude is None
                or not -90 <= latitude <= 90
                or not -180 <= longitude <= 180
            ):
                continue
            row["uk_air_ref"] = uk_air_ref
            row["site_ref"] = site_ref
            row["site_name"] = site_name
            row["latitude"] = latitude
            row["longitude"] = longitude
            start_date = _date_value(row.get("start_date"))
            end_date = _date_value(row.get("end_date"))
            row["_current_at_snapshot"] = bool(
                snapshot_date
                and (start_date is None or start_date <= snapshot_date)
                and (end_date is None or end_date >= snapshot_date)
            )
            valid.append(row)
        return snapshot_at, valid

    def _connector_ids(self) -> Dict[int, str]:
        rows = _rows(
            self.core.table("connectors")
            .select("id,connector_code")
            .in_("connector_code", ["sos", *REGIONAL_CONNECTOR_CODES])
            .execute()
        )
        return {
            int(row["id"]): str(row["connector_code"]).lower()
            for row in rows
            if row.get("id") is not None and row.get("connector_code")
        }

    def _stations(self, connector_ids: Sequence[int]) -> List[Dict[str, Any]]:
        if not connector_ids:
            return []
        return _all_rows(
            lambda: self.core.table("stations")
                .select(
                    "id,connector_id,station_ref,station_name,label,latitude,longitude,"
                    "match_id,removed_at"
                )
                .in_("connector_id", list(connector_ids))
                .order("id")
        )

    def _sos_memberships(self) -> List[Dict[str, Any]]:
        return _all_rows(
            lambda: self.raw.table("sos_station_uk_air_refs")
                .select("station_id,uk_air_ref,match_method,match_distance_m,source_snapshot_at")
                .order("station_id")
        )

    def _station_matches(self) -> List[Dict[str, Any]]:
        return _all_rows(
            lambda: self.core.table("station_matches")
                .select("id,uk_air_ref,metadata")
                .order("id")
        )

    def _ensure_match(
        self,
        site: Mapping[str, Any],
        snapshot_at: Optional[str],
        members: Sequence[Mapping[str, Any]],
        current_member_station_ids: Sequence[int],
        existing_by_ref: Dict[str, Dict[str, Any]],
    ) -> Optional[int]:
        uk_air_ref = str(site["uk_air_ref"])
        existing = existing_by_ref.get(uk_air_ref)
        existing_metadata = existing.get("metadata") if existing else None
        metadata = dict(existing_metadata) if isinstance(existing_metadata, dict) else {}
        existing_identity = metadata.get("canonical_site_identity")
        identity = dict(existing_identity) if isinstance(existing_identity, dict) else {}
        metadata["canonical_site_identity"] = {
            **identity,
            "authority": "uk_aq_raw.sos_site_register",
            "source_snapshot_at": snapshot_at,
            "source_url": site.get("source_url"),
            "member_evidence": merge_member_evidence(identity, members),
            "current_member_station_ids": sorted(set(current_member_station_ids)),
        }
        latitude = _float(site.get("latitude"))
        longitude = _float(site.get("longitude"))
        payload = {
            "uk_air_ref": uk_air_ref,
            "match_name": _text(site.get("site_name")),
            "latitude": latitude,
            "longitude": longitude,
            "geometry": (
                f"SRID=4326;POINT({longitude} {latitude})"
                if latitude is not None and longitude is not None
                else None
            ),
            "match_method": "uk_air_canonical_reference",
            "match_confidence": 1,
            "metadata": metadata,
        }
        if self.dry_run:
            return int(existing["id"]) if existing else None
        if existing:
            result = _rows(
                self.core.table("station_matches")
                .update(payload)
                .eq("id", existing["id"])
                .execute()
            )
        else:
            try:
                result = _rows(self.core.table("station_matches").insert(payload).execute())
            except Exception:
                # A concurrent idempotent run may have won the partial-unique race.
                result = _rows(
                    self.core.table("station_matches")
                    .select("id,uk_air_ref,metadata")
                    .eq("uk_air_ref", uk_air_ref)
                    .limit(1)
                    .execute()
                )
                if not result:
                    raise
        if not result:
            result = _rows(
                self.core.table("station_matches")
                .select("id,uk_air_ref,metadata")
                .eq("uk_air_ref", uk_air_ref)
                .limit(1)
                .execute()
            )
        if not result:
            raise RuntimeError(f"Could not resolve station_match id for {uk_air_ref}")
        existing_by_ref[uk_air_ref] = result[0]
        return int(result[0]["id"])

    def run(self) -> Dict[str, Any]:
        snapshot_at, aurn_sites = self._latest_aurn_sites()
        if not snapshot_at or not aurn_sites:
            raise RuntimeError("Latest SOS site-register snapshot has no valid AURN rows")
        sites_by_ref = {str(site["uk_air_ref"]): site for site in aurn_sites}
        connector_by_id = self._connector_ids()
        stations = self._stations(sorted(connector_by_id))
        stations_by_id = {int(row["id"]): row for row in stations}

        evidence_by_ref: Dict[str, List[Dict[str, Any]]] = {}
        target_ref_by_station: Dict[int, str] = {}
        diagnostics: List[Dict[str, Any]] = []

        for bridge in self._sos_memberships():
            station_id = int(bridge["station_id"])
            uk_air_ref = _valid_uk_air_ref(bridge.get("uk_air_ref"))
            if station_id not in stations_by_id or not uk_air_ref or uk_air_ref not in sites_by_ref:
                diagnostics.append({
                    "station_id": station_id,
                    "connector_code": "sos",
                    "status": "unmatched",
                    "reason": "bridge_reference_not_in_latest_aurn_register",
                    "uk_air_ref": uk_air_ref,
                })
                continue
            evidence = {
                "station_id": station_id,
                "connector_code": "sos",
                "method": bridge.get("match_method") or "sos_station_uk_air_refs",
                "distance_m": bridge.get("match_distance_m"),
                "source_snapshot_at": bridge.get("source_snapshot_at"),
            }
            target_ref_by_station[station_id] = uk_air_ref
            evidence_by_ref.setdefault(uk_air_ref, []).append(evidence)

        for station in stations:
            connector_code = connector_by_id.get(int(station["connector_id"]))
            if connector_code not in REGIONAL_CONNECTOR_CODES:
                continue
            decision = choose_regional_match(
                station,
                aurn_sites,
                official_evidence=lambda source, candidate, code=connector_code: (
                    fetch_regional_page_evidence(code, source, candidate)
                ),
            )
            station_id = int(station["id"])
            diagnostics.append({
                "station_id": station_id,
                "station_ref": station.get("station_ref"),
                "connector_code": connector_code,
                "status": decision.status,
                "uk_air_ref": decision.uk_air_ref,
                "method": decision.method,
                "distance_m": decision.distance_m,
                "evidence": decision.evidence,
            })
            if decision.status != "matched" or not decision.uk_air_ref:
                continue
            target_ref_by_station[station_id] = decision.uk_air_ref
            evidence_by_ref.setdefault(decision.uk_air_ref, []).append({
                "station_id": station_id,
                "connector_code": connector_code,
                "station_ref": station.get("station_ref"),
                "method": decision.method,
                "distance_m": decision.distance_m,
                "evidence": decision.evidence,
            })

        existing_matches = self._station_matches()
        existing_by_ref = {
            str(row["uk_air_ref"]): row
            for row in existing_matches
            if _valid_uk_air_ref(row.get("uk_air_ref"))
        }
        match_ref_by_id = {
            int(row["id"]): _valid_uk_air_ref(row.get("uk_air_ref"))
            for row in existing_matches
        }
        protected_conflicts = []
        for station_id, candidate_ref in list(target_ref_by_station.items()):
            current_match_id = stations_by_id[station_id].get("match_id")
            if current_match_id is None:
                continue
            current_ref = match_ref_by_id.get(int(current_match_id))
            if not current_ref or current_ref == candidate_ref:
                continue
            protected_conflicts.append({
                "station_id": station_id,
                "current_match_id": int(current_match_id),
                "current_uk_air_ref": current_ref,
                "candidate_uk_air_ref": candidate_ref,
            })
            target_ref_by_station.pop(station_id, None)
            retained_evidence = [
                item
                for item in evidence_by_ref.get(candidate_ref, [])
                if int(item.get("station_id") or 0) != station_id
            ]
            if retained_evidence:
                evidence_by_ref[candidate_ref] = retained_evidence
            else:
                evidence_by_ref.pop(candidate_ref, None)

        match_id_by_ref: Dict[str, Optional[int]] = {}
        current_member_ids_by_ref: Dict[str, set[int]] = {}
        for station_id, station in stations_by_id.items():
            if _text(station.get("removed_at")):
                continue
            current_match_id = station.get("match_id")
            if current_match_id is None:
                continue
            current_ref = match_ref_by_id.get(int(current_match_id))
            if current_ref:
                current_member_ids_by_ref.setdefault(current_ref, set()).add(station_id)
        for station_id, target_ref in target_ref_by_station.items():
            if _text(stations_by_id[station_id].get("removed_at")):
                continue
            current_member_ids_by_ref.setdefault(target_ref, set()).add(station_id)
        for uk_air_ref in sorted(evidence_by_ref):
            site = sites_by_ref.get(uk_air_ref)
            if not site:
                continue
            match_id_by_ref[uk_air_ref] = self._ensure_match(
                site,
                snapshot_at,
                sorted(evidence_by_ref[uk_air_ref], key=lambda row: int(row["station_id"])),
                sorted(current_member_ids_by_ref.get(uk_air_ref, set())),
                existing_by_ref,
            )

        assignments: Dict[int, int] = {}
        proposed_assignment_count = 0
        for station_id, uk_air_ref in sorted(target_ref_by_station.items()):
            station = stations_by_id[station_id]
            current_match_id = station.get("match_id")
            if current_match_id is not None:
                current_ref = match_ref_by_id.get(int(current_match_id))
                if current_ref == uk_air_ref:
                    continue
            proposed_assignment_count += 1
            target_match_id = match_id_by_ref.get(uk_air_ref)
            if target_match_id is None:
                continue
            if current_match_id is not None:
                if int(current_match_id) == target_match_id:
                    continue
            assignments[station_id] = target_match_id

        if not self.dry_run:
            assignments_by_match: Dict[int, List[int]] = {}
            for station_id, match_id in assignments.items():
                assignments_by_match.setdefault(match_id, []).append(station_id)
            for match_id, station_ids in sorted(assignments_by_match.items()):
                for station_id_chunk in _chunks(station_ids):
                    self.core.table("stations").update({"match_id": match_id}).in_(
                        "id", list(station_id_chunk)
                    ).execute()

        status_counts: Dict[str, int] = {}
        for item in diagnostics:
            status = str(item["status"])
            status_counts[status] = status_counts.get(status, 0) + 1
        return {
            "dry_run": self.dry_run,
            "source_snapshot_at": snapshot_at,
            "aurn_sites": len(aurn_sites),
            "canonical_matches_considered": len(evidence_by_ref),
            "station_assignments": proposed_assignment_count,
            "station_assignments_written": 0 if self.dry_run else len(assignments),
            "protected_reassignment_conflicts": protected_conflicts,
            "resolution_status_counts": status_counts,
            "diagnostics": diagnostics,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="resolve and report without writing station matches or memberships",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        result = CanonicalSiteResolver(dry_run=args.dry_run).run()
    except Exception as error:
        prerequisite = schema_prerequisite_message(error)
        if prerequisite:
            raise RuntimeError(prerequisite) from error
        raise
    print(json.dumps(result, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
