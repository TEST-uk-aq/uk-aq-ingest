"""Read-only retained OpenAQ authority on the station importer's database target."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List

from scripts.stations_daily.reference_source import SourceUnavailable


def _unit(value: Any) -> str:
    # Equivalence of existing source/canonical spellings; no rows are rewritten.
    text = (
        str(value or "").strip().replace("µ", "u").replace("μ", "u").replace("³", "3")
    )
    return {"ug.m-3": "ug/m3", "mg.m-3": "mg/m3"}.get(text, text)


def verify_snapshot(data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    connectors, networks = data["connectors"], data["networks"]
    if len(connectors) != 1 or len(networks) != 1:
        raise RuntimeError("Missing/ambiguous retained OpenAQ authority")
    connector_id, network_id = int(connectors[0]["id"]), int(networks[0]["id"])
    if (
        connectors[0].get("connector_code") != "openaq"
        or networks[0].get("network_code") != "openaq"
        or int(connectors[0].get("default_network_id") or 0) != network_id
    ):
        raise RuntimeError("Invalid retained OpenAQ connector/network identity")
    stations, station_refs = {}, set()
    for row in data["stations"]:
        ref = row.get("station_ref")
        if (
            int(row.get("connector_id") or 0) != connector_id
            or row.get("service_ref") != "openaq"
            or int(row.get("network_id") or 0) != network_id
            or not isinstance(ref, str)
            or not re.fullmatch(r"[1-9][0-9]*", ref)
            or ref in station_refs
            or int(row["id"]) in stations
        ):
            raise RuntimeError("Unsafe retained OpenAQ station identity")
        stations[int(row["id"])] = row
        station_refs.add(ref)
    if not stations:
        raise RuntimeError("Uninitialised retained OpenAQ station set")
    properties = {int(row["id"]): row for row in data["properties"]}
    mappings = {}
    for row in data["mappings"]:
        source = row.get("source_label")
        if not isinstance(source, str) or source in mappings:
            raise RuntimeError("Ambiguous retained OpenAQ source mapping")
        mappings[source] = row
    phenomena, source_labels = {}, set()
    for row in data["phenomena"]:
        source = row.get("source_label")
        mapping = mappings.get(source)
        property_row = properties.get(int(row.get("observed_property_id") or 0))
        if (
            not isinstance(source, str)
            or not re.fullmatch(r"openaq:[a-zA-Z0-9_.-]+", source)
            or source in source_labels
            or int(row["id"]) in phenomena
            or not mapping
            or mapping.get("is_active") is not True
            or int(mapping.get("connector_id") or 0) != connector_id
            or int(row.get("connector_id") or 0) != connector_id
            or not property_row
            or mapping.get("mapping_kind")
            not in ("raw_observed_property", "meteorological", "derived_index")
            or int(mapping.get("observed_property_id") or 0) != int(property_row["id"])
            or mapping.get("observed_property_code") != property_row.get("code")
            or not _unit(property_row.get("canonical_uom"))
            or property_row.get("domain")
            != {
                "raw_observed_property": "aq",
                "meteorological": "met",
                "derived_index": "index",
            }[mapping["mapping_kind"]]
        ):
            raise RuntimeError("Unsafe retained OpenAQ phenomenon/property mapping")
        phenomena[int(row["id"])] = (row, mapping, property_row)
        source_labels.add(source)
    if not phenomena:
        raise RuntimeError("Uninitialised retained OpenAQ phenomena")
    refs, ids, used_stations, used_phenomena = set(), set(), set(), set()
    for row in data["timeseries"]:
        phenomenon_id = int(row.get("phenomenon_id") or 0)
        station_id = int(row.get("station_id") or 0)
        ref = row.get("timeseries_ref")
        if (
            int(row.get("connector_id") or 0) != connector_id
            or station_id not in stations
            or row.get("service_ref") != "openaq"
            or not isinstance(ref, str)
            or not re.fullmatch(r"[1-9][0-9]*", ref)
            or ref in refs
            or int(row["id"]) in ids
            or phenomenon_id not in phenomena
        ):
            raise RuntimeError("Unsafe retained OpenAQ timeseries identity/ownership")
        _, mapping, property_row = phenomena[phenomenon_id]
        if (
            int(row.get("observed_property_id") or 0) != int(property_row["id"])
            or not _unit(row.get("uom"))
            or _unit(row.get("uom"))
            != _unit(mapping.get("source_uom") or property_row.get("canonical_uom"))
        ):
            raise RuntimeError("Unsafe retained OpenAQ timeseries property/units")
        refs.add(ref)
        ids.add(int(row["id"]))
        used_stations.add(station_id)
        used_phenomena.add(phenomenon_id)
    if not refs or used_stations != set(stations) or used_phenomena != set(phenomena):
        raise RuntimeError("Incomplete retained OpenAQ reference relationships")
    return {
        "stations": len(stations),
        "phenomena": len(phenomena),
        "timeseries": len(refs),
    }


def verify_retained_references(connection: Any) -> Dict[str, int]:
    from psycopg2.extras import RealDictCursor

    # Reuse the exact reference writer target. No Supabase URL/DSN substitution,
    # repair RPC, token reservation or reference write occurs in this transaction.
    with connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")

        def read(query: str, params: Any = ()) -> List[Dict[str, Any]]:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

        data = {
            "connectors": read(
                "SELECT id,connector_code,default_network_id FROM uk_aq_core.connectors WHERE connector_code = %s",
                ("openaq",),
            ),
            "networks": read(
                "SELECT id,network_code FROM uk_aq_core.networks WHERE network_code = %s",
                ("openaq",),
            ),
        }
        if len(data["connectors"]) != 1:
            raise RuntimeError("Missing/ambiguous retained OpenAQ connector")
        connector_id = int(data["connectors"][0]["id"])
        data["stations"] = read(
            "SELECT id,connector_id,network_id,service_ref,station_ref FROM uk_aq_core.stations WHERE connector_id = %s",
            (connector_id,),
        )
        data["phenomena"] = read(
            "SELECT id,connector_id,source_label,observed_property_id FROM uk_aq_core.phenomena WHERE connector_id = %s",
            (connector_id,),
        )
        data["mappings"] = read(
            "SELECT connector_id,source_label,observed_property_id,observed_property_code,source_uom,mapping_kind,is_active FROM uk_aq_core.observed_property_mappings WHERE connector_id = %s",
            (connector_id,),
        )
        data["properties"] = read(
            "SELECT id,code,domain,canonical_uom FROM uk_aq_core.observed_properties WHERE id = ANY(%s)",
            (
                [
                    int(row["observed_property_id"])
                    for row in data["phenomena"]
                    if row.get("observed_property_id")
                ],
            ),
        )
        data["timeseries"] = read(
            "SELECT id,connector_id,service_ref,station_id,timeseries_ref,phenomenon_id,observed_property_id,uom FROM uk_aq_core.timeseries WHERE connector_id = %s OR station_id = ANY(%s)",
            (connector_id, [int(row["id"]) for row in data["stations"]]),
        )
        return verify_snapshot(data)


def retained_source_failure_exit(
    connection: Any, source_error: SourceUnavailable
) -> int:
    try:
        counts = verify_retained_references(connection)
    except Exception as exc:
        print(
            f"OpenAQ retained-reference verification failed ({type(exc).__name__}); isolation denied.",
            flush=True,
        )
        return 2
    print(
        "REFERENCE_SUMMARY_JSON "
        + json.dumps(
            {
                "connector_code": "openaq",
                "ok": False,
                "reference_state_retained": True,
                "failure_class": source_error.failure_class,
                "attempts": source_error.attempts,
                "reason": "OpenAQ catalogue source refresh could not complete; existing verified reference rows were retained.",
                **counts,
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
