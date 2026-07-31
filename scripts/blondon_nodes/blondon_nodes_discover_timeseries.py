#!/usr/bin/env python3
"""Discover deterministic Breathe London Nodes timeseries reference rows."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.blondon_nodes.blondon_nodes_reference_data import (
    DEFAULT_SPECIES,
    build_nodes_timeseries_rows,
    upsert_nodes_phenomena,
)
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client


LOG = logging.getLogger("blondon_nodes_discover_timeseries")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CONNECTOR_CODE = "blondon_nodes"
SERVICE_REF = "breathelondon"
PAGE_SIZE = 500
UPSERT_BATCH_SIZE = 500
TIMESERIES_FIELDS = (
    "id",
    "connector_id",
    "station_id",
    "timeseries_ref",
    "label",
    "uom",
    "service_ref",
    "phenomenon_id",
    "observed_property_id",
    "extras",
)


def response_rows(response: Any) -> List[Dict[str, Any]]:
    data = response.data if hasattr(response, "data") else response.get("data")
    return [dict(row) for row in (data or [])]


def chunked(values: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def resolve_connector_id(core: Any) -> int:
    rows = response_rows(
        core.table("connectors")
        .select("id,connector_code")
        .eq("connector_code", CONNECTOR_CODE)
        .limit(2)
        .execute()
    )
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected exactly one connector with connector_code={CONNECTOR_CODE}; "
            f"found {len(rows)}"
        )
    return int(rows[0]["id"])


def fetch_active_stations(core: Any, connector_id: int) -> List[Dict[str, Any]]:
    stations: List[Dict[str, Any]] = []
    offset = 0
    while True:
        batch = response_rows(
            core.table("stations")
            .select("id,station_ref,station_name,label")
            .eq("connector_id", connector_id)
            .eq("service_ref", SERVICE_REF)
            .filter("removed_at", "is", "null")
            .order("id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )
        stations.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return stations


def fetch_required_timeseries(
    core: Any,
    connector_id: int,
    timeseries_refs: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    found: Dict[str, Dict[str, Any]] = {}
    for refs in chunked(timeseries_refs, 200):
        rows = response_rows(
            core.table("timeseries")
            .select(",".join(TIMESERIES_FIELDS))
            .eq("connector_id", connector_id)
            .in_("timeseries_ref", list(refs))
            .execute()
        )
        for row in rows:
            timeseries_ref = str(row.get("timeseries_ref") or "")
            if timeseries_ref in found:
                raise RuntimeError(
                    f"Duplicate stored Nodes timeseries identity: {timeseries_ref}"
                )
            found[timeseries_ref] = row
    return found


def timeseries_row_matches(
    existing: Mapping[str, Any], expected: Mapping[str, Any]
) -> bool:
    return all(existing.get(field) == value for field, value in expected.items())


def discover_nodes_timeseries(client: Any) -> Dict[str, Any]:
    schemas = SupabaseSchemas.from_client(client)
    core = schemas.core
    public = client.schema(os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public")

    connector_id = resolve_connector_id(core)
    stations = fetch_active_stations(core, connector_id)
    phenomenon_ids, observed_property_ids = upsert_nodes_phenomena(
        public,
        connector_id,
        DEFAULT_SPECIES,
    )
    expected_rows = build_nodes_timeseries_rows(
        stations,
        connector_id=connector_id,
        phenomenon_ids=phenomenon_ids,
        observed_property_ids=observed_property_ids,
        service_ref=SERVICE_REF,
        species=DEFAULT_SPECIES,
    )
    expected_by_ref = {
        str(row["timeseries_ref"]): row for row in expected_rows
    }
    if len(expected_by_ref) != len(expected_rows):
        raise RuntimeError("Duplicate generated Nodes timeseries identities detected")
    expected_refs = sorted(expected_by_ref)

    existing = fetch_required_timeseries(core, connector_id, expected_refs)
    rows_to_upsert = [
        row
        for timeseries_ref, row in expected_by_ref.items()
        if timeseries_ref not in existing
        or not timeseries_row_matches(existing[timeseries_ref], row)
    ]
    for rows in chunked(rows_to_upsert, UPSERT_BATCH_SIZE):
        core.table("timeseries").upsert(
            list(rows), on_conflict="connector_id,timeseries_ref"
        ).execute()

    final = fetch_required_timeseries(core, connector_id, expected_refs)
    missing_refs = sorted(set(expected_refs) - set(final))
    mismatched_refs = sorted(
        timeseries_ref
        for timeseries_ref, expected_row in expected_by_ref.items()
        if timeseries_ref in final
        and not timeseries_row_matches(final[timeseries_ref], expected_row)
    )
    changed_id_refs = sorted(
        timeseries_ref
        for timeseries_ref, existing_row in existing.items()
        if timeseries_ref in final
        and int(final[timeseries_ref]["id"]) != int(existing_row["id"])
    )
    ok = not missing_refs and not mismatched_refs and not changed_id_refs
    return {
        "connector_id": connector_id,
        "active_station_count": len(stations),
        "expected_active_timeseries_count": len(expected_rows),
        "pre_existing_required_timeseries_count": len(existing),
        "upserted_or_repaired_count": len(rows_to_upsert),
        "final_required_timeseries_count": len(final),
        "missing_required_timeseries_count": len(missing_refs),
        "mismatched_required_timeseries_count": len(mismatched_refs),
        "changed_existing_timeseries_id_count": len(changed_id_refs),
        "ok": ok,
    }


def emit_summary(summary: Mapping[str, Any]) -> None:
    print(
        "DISCOVERY_SUMMARY_JSON "
        + json.dumps(dict(summary), separators=(",", ":"), sort_keys=True),
        flush=True,
    )


def main() -> int:
    try:
        summary = discover_nodes_timeseries(create_supabase_client())
    except Exception as exc:
        LOG.error("Breathe London Nodes timeseries discovery failed: %s", exc)
        emit_summary(
            {
                "connector_id": None,
                "active_station_count": None,
                "expected_active_timeseries_count": None,
                "pre_existing_required_timeseries_count": None,
                "upserted_or_repaired_count": None,
                "final_required_timeseries_count": None,
                "missing_required_timeseries_count": None,
                "ok": False,
                "error": str(exc),
            }
        )
        return 1
    emit_summary(summary)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
