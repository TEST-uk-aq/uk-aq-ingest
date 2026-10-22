"""Opt-in Daily Stations source boundary and read-only Breathe London safety gate.

This module never calls reference/phenomena RPCs or repairs retained rows.
Normal observation ingestion and its existing clients do not use this path.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, List, Optional

from scripts.blondon_nodes.blondon_nodes_reference_data import (
    SPECIES_CONFIG as NODES_SPECIES,
)
from scripts.stations_daily.reference_source import (
    SOURCE_ATTEMPTS,
    SOURCE_RETRY_DELAYS,
    SourceUnavailable,
    acquire_reference_bytes,
)
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client

COMMUNITIES_SPECIES = {
    "IPM25": {
        "source_label": "breathelondon:pm2.5",
        "observed_property_code": "pm25",
        "uom": "ug/m3",
        "mapping_kind": "raw_observed_property",
        "is_aqi_eligible": True,
    },
    "INO2": {
        "source_label": "breathelondon:no2",
        "observed_property_code": "no2",
        "uom": "ug/m3",
        "mapping_kind": "raw_observed_property",
        "is_aqi_eligible": True,
    },
}


def load_station_catalogue(
    session: Any,
    url: str,
    connector_code: str,
    *,
    params: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Retry acquisition only; JSON and catalogue schema failures stay fatal."""
    for attempt in range(1, SOURCE_ATTEMPTS + 1):
        try:
            body = acquire_reference_bytes(
                session, url, params=params, expected_format="json"
            )
        except SourceUnavailable as exc:
            print(
                f"{connector_code} source attempt={attempt}/{SOURCE_ATTEMPTS} failure_class={exc.failure_class}",
                flush=True,
            )
            if attempt == SOURCE_ATTEMPTS:
                raise SourceUnavailable(exc.failure_class, attempt) from None
            time.sleep(SOURCE_RETRY_DELAYS[attempt - 1])
            continue
        payload = json.loads(body)
        if connector_code == "blondon_nodes" and isinstance(payload, dict):
            candidates = [
                payload[key]
                for key in (
                    "sensors",
                    "Sensors",
                    "sites",
                    "Sites",
                    "data",
                    "Data",
                    "results",
                    "Results",
                )
                if isinstance(payload.get(key), list)
            ]
            if len(candidates) != 1:
                raise RuntimeError("Ambiguous Nodes station catalogue schema")
            payload = candidates[0]
        if (
            isinstance(payload, list)
            and len(payload) == 1
            and isinstance(payload[0], list)
        ):
            payload = payload[0]
        if (
            not isinstance(payload, list)
            or not payload
            or any(
                not isinstance(row, dict)
                or not isinstance(row.get("SiteCode"), str)
                or not row["SiteCode"].strip()
                for row in payload
            )
        ):
            raise RuntimeError(
                "Empty or unsupported Breathe London station catalogue schema"
            )
        return payload
    raise AssertionError("unreachable")


def _rows(response: Any) -> List[Dict[str, Any]]:
    data = response.data if hasattr(response, "data") else response.get("data")
    if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
        raise RuntimeError("Invalid retained-reference database response")
    return data


def _read_all(
    core: Any, table: str, columns: str, **filters: Any
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    while True:
        query = core.table(table).select(columns)
        for key, value in filters.items():
            query = query.eq(key, value)
        page = _rows(query.order("id").range(len(rows), len(rows) + 499).execute())
        if not page:
            return rows
        rows.extend(page)


def verify_retained_references(
    connector_code: str, service_ref: str, core: Any = None
) -> Dict[str, int]:
    """Establish the entire active-station set from persisted IDs, read-only."""
    if (
        connector_code not in ("blondon_communities", "blondon_nodes")
        or service_ref != "breathelondon"
    ):
        raise RuntimeError("Invalid Breathe London isolation identity/configuration")
    if core is None:
        core = SupabaseSchemas.from_client(create_supabase_client()).core
    connectors = _read_all(
        core,
        "connectors",
        "id,connector_code,default_network_id",
        connector_code=connector_code,
    )
    networks = _read_all(
        core, "networks", "id,network_code", network_code="breathelondon"
    )
    if len(connectors) != 1 or len(networks) != 1:
        raise RuntimeError(
            "Missing/ambiguous retained Breathe London connector/network"
        )
    connector_id, network_id = int(connectors[0]["id"]), int(networks[0]["id"])
    if int(connectors[0].get("default_network_id") or 0) != network_id:
        raise RuntimeError("Invalid retained Breathe London network ownership")
    station_rows = _read_all(
        core,
        "stations",
        "id,connector_id,network_id,station_ref,service_ref,removed_at",
        connector_id=connector_id,
    )
    stations = {}
    refs = set()
    for row in station_rows:
        ref = row.get("station_ref")
        if (
            row.get("service_ref") != service_ref
            or int(row.get("network_id") or 0) != network_id
            or not isinstance(ref, str)
            or not ref
            or ref != ref.strip()
            or len(ref) > 128
            or re.search(r"[\s:\x00-\x1f\x7f]", ref)
            or ref in refs
            or int(row["id"]) in stations
        ):
            raise RuntimeError("Unsafe retained Breathe London station identity")
        refs.add(ref)
        stations[int(row["id"])] = row
    active = {
        key: row for key, row in stations.items() if row.get("removed_at") is None
    }
    if not active:
        raise RuntimeError("Uninitialised retained Breathe London active-station set")
    specs = NODES_SPECIES if connector_code == "blondon_nodes" else COMMUNITIES_SPECIES
    phenomena = _read_all(
        core,
        "phenomena",
        "id,connector_id,source_label,observed_property_id",
        connector_id=connector_id,
    )
    mappings = _read_all(
        core,
        "observed_property_mappings",
        "id,connector_id,source_label,observed_property_id,observed_property_code,source_uom,mapping_kind,is_aqi_eligible,is_active",
        connector_id=connector_id,
    )
    properties = _rows(
        core.table("observed_properties")
        .select("id,code,domain,canonical_uom")
        .in_("code", [spec["observed_property_code"] for spec in specs.values()])
        .execute()
    )
    by_code = {row["code"]: row for row in properties}
    if len(by_code) != len(specs) or len(properties) != len(specs):
        raise RuntimeError(
            "Missing/ambiguous retained Breathe London canonical properties"
        )
    phenomenon_ids = {}
    for species, spec in specs.items():
        matches = [
            row for row in phenomena if row.get("source_label") == spec["source_label"]
        ]
        mapping = [
            row for row in mappings if row.get("source_label") == spec["source_label"]
        ]
        property_row = by_code[spec["observed_property_code"]]
        domain = "index" if spec["mapping_kind"] == "derived_index" else "aq"
        if (
            len(matches) != 1
            or len(mapping) != 1
            or mapping[0].get("is_active") is not True
            or int(matches[0].get("observed_property_id") or 0)
            != int(property_row["id"])
            or int(mapping[0].get("observed_property_id") or 0)
            != int(property_row["id"])
            or any(
                mapping[0].get(field) != spec[field]
                for field in (
                    "observed_property_code",
                    "mapping_kind",
                    "is_aqi_eligible",
                )
            )
            or mapping[0].get("source_uom") != spec["uom"]
            or property_row.get("domain") != domain
            or property_row.get("canonical_uom")
            != ("DAQI" if domain == "index" else "ug/m3")
        ):
            raise RuntimeError(
                "Unsafe retained Breathe London phenomenon/property mapping"
            )
        phenomenon_ids[species] = int(matches[0]["id"])
    timeseries = _read_all(
        core,
        "timeseries",
        "id,connector_id,station_id,service_ref,timeseries_ref,phenomenon_id,observed_property_id,uom,extras",
        connector_id=connector_id,
    )
    expected = {
        f"{station['station_ref']}:{species}"
        for station in active.values()
        for species in specs
    }
    found, ids, stored_refs = set(), set(), set()
    for row in timeseries:
        station_id = int(row.get("station_id") or 0)
        station = stations.get(station_id)
        ref = row.get("timeseries_ref")
        if (
            station is None
            or not isinstance(ref, str)
            or ref in stored_refs
            or int(row["id"]) in ids
        ):
            raise RuntimeError(
                "Unsafe retained Breathe London timeseries ownership/identity"
            )
        species = ref.rsplit(":", 1)[-1]
        spec = specs.get(species)
        extras = row.get("extras")
        if (
            spec is None
            or ref != f"{station['station_ref']}:{species}"
            or row.get("service_ref") != service_ref
            or int(row.get("phenomenon_id") or 0) != phenomenon_ids[species]
            or int(row.get("observed_property_id") or 0)
            != int(by_code[spec["observed_property_code"]]["id"])
            or row.get("uom") != spec["uom"]
            or not isinstance(extras, dict)
            or extras.get("site_code") != station["station_ref"]
            or extras.get("species") != species
            or (
                connector_code == "blondon_nodes"
                and (
                    extras.get("measurement_kind") != spec["kind"]
                    or extras.get("api_units") != spec["uom"]
                )
            )
        ):
            raise RuntimeError(
                "Unsafe retained Breathe London deterministic timeseries mapping"
            )
        stored_refs.add(ref)
        ids.add(int(row["id"]))
        if station_id in active:
            found.add(ref)
    if found != expected:
        raise RuntimeError(
            "Incomplete retained Breathe London deterministic reference set"
        )
    for offset in range(0, len(stations), 200):
        foreign = _rows(
            core.table("timeseries")
            .select("id")
            .in_("station_id", list(stations)[offset : offset + 200])
            .neq("connector_id", connector_id)
            .limit(1)
            .execute()
        )
        if foreign:
            raise RuntimeError("Cross-connector Breathe London timeseries ownership")
    return {
        "active_stations": len(active),
        "required_timeseries": len(expected),
        "verified_timeseries": len(found),
    }


def retained_source_failure_exit(
    connector_code: str, service_ref: str, source_error: SourceUnavailable
) -> int:
    """Only called from the station-source catch, before importer writes exist."""
    try:
        verified = verify_retained_references(connector_code, service_ref)
    except Exception as exc:
        print(
            f"{connector_code} retained-reference verification failed ({type(exc).__name__}); isolation denied.",
            flush=True,
        )
        return 2
    print(
        "REFERENCE_SUMMARY_JSON "
        + json.dumps(
            {
                "connector_code": connector_code,
                "ok": False,
                "reference_state_retained": True,
                "failure_class": source_error.failure_class,
                "attempts": source_error.attempts,
                "reason": "Station source refresh could not complete; the complete verified retained reference set was preserved.",
                **verified,
            },
            sort_keys=True,
        ),
        flush=True,
    )
    if os.getenv("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
            handle.write(
                f"failure_class={source_error.failure_class}\nretained_references_verified=true\n"
            )
    return 75
