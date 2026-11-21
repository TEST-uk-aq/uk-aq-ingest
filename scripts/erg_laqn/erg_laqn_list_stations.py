#!/usr/bin/env python3
"""Refresh LAQN references inside Daily Stations, via the fixed-egress service."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.erg_laqn.catalogue import build_catalogue, concentration_series, coordinate, iso
from scripts.erg_laqn.source import (
    CONNECTOR_CODE, NETWORK_CODE, SITE_URL, SPECIES, SPECIES_URL, UOM, ErgClient, SourceError, text,
)
from scripts.official_networks.canonical_site_resolver import _all_rows, _rows
from scripts.uk_aq_phenomena_rpc import upsert_phenomena_via_rpc
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client


def load_connector(schemas: SupabaseSchemas) -> dict | None:
    rows = _rows(schemas.core.table("connectors")
                 .select("id,default_network_id,scheduler_backend,config")
                 .eq("connector_code", CONNECTOR_CODE).execute())
    if not rows:
        return None
    row = rows[0]
    networks = _rows(schemas.core.table("networks").select("id")
                     .eq("network_code", NETWORK_CODE).execute())
    if (not networks or row["default_network_id"] != networks[0]["id"]
            or row["scheduler_backend"] != "google_cloud_run"):
        raise RuntimeError("LAQN connector and network ownership disagree")
    return row


def write_batches(schema, table: str, rows: list[dict], key: str, *, ignore=False) -> None:
    for index in range(0, len(rows), 50):
        schema.table(table).upsert(rows[index:index + 50], on_conflict=key,
                                   ignore_duplicates=ignore, returning="minimal").execute()


def refresh(schemas: SupabaseSchemas, connector: dict) -> dict:
    now = datetime.now(timezone.utc)
    config = connector["config"]
    api = ErgClient()
    with api.session:
        catalogue = build_catalogue(api.get(SITE_URL), api.get(SPECIES_URL),
                                    config.get("membership_evidence"))
    sites = [site for site in catalogue.values() if site["laqn"]]
    if not sites:
        raise SourceError("No positively affiliated LAQN sites in ERG catalogue")
    core, raw = schemas.core, schemas.raw
    connector_id = connector["id"]
    prior = {
        row["station_ref"]: row for row in _all_rows(
            lambda: core.table("stations").select("id,station_ref,service_ref")
            .eq("connector_id", connector_id).order("id")
        ) if row["service_ref"] == CONNECTOR_CODE
    }

    properties = {
        row["code"]: row for row in _rows(core.table("observed_properties")
            .select("id,code,canonical_uom").in_("code", list(SPECIES.values())).execute())
    }
    if set(properties) != set(SPECIES.values()) or any(
        row["canonical_uom"] != UOM for row in properties.values()
    ):
        raise RuntimeError("LAQN canonical property or mass units are missing")

    station_rows, register_rows, metadata, series_by_ref = [], [], {}, {}
    for site in catalogue.values():
        detail, ref = site["detail"], site["ref"]
        name = text(detail.get("@SiteName"))
        lat = coordinate(detail.get("@Latitude"), 90)
        lon = coordinate(detail.get("@Longitude"), 180)
        opened, closed = site["opened"], site["closed"]
        if opened and closed and closed < opened:
            raise SourceError(f"Contradictory ERG site lifecycle for {ref}")
        removed_at = iso(closed) if closed and closed <= now else None
        source = {
            "erg_laqn": {
                **site["sources"], "networks": site["networks"],
                "membership": site["membership_evidence"],
                "captured_at": now.isoformat(), "species_url": SPECIES_URL,
            },
        }
        register_rows.append({
            "site_ref": ref, "label": name or None, "station_name": name or None,
            "station_type": text(detail.get("@SiteType")) or None,
            "local_authority": text(detail.get("@LocalAuthorityName")) or None,
            "network_label": ", ".join(site["networks"]) or None,
            "networks": site["networks"], "latitude": lat, "longitude": lon,
            "first_seen_at": iso(opened), "last_seen_at": now.isoformat(),
            "removed_at": removed_at, "site_url": text(detail.get("@SiteLink")) or None,
            "source_url": SITE_URL, "snapshot_at": now.isoformat(), "raw_payload": source,
        })
        if not site["laqn"]:
            continue
        if not name or not opened or not site["closure_known"]:
            raise SourceError(f"LAQN site {ref} is missing name or lifecycle evidence")
        # An inconsistent open reference response cannot clear prior state or
        # admit a partially discovered new station.
        if not removed_at and (not site["in_both_catalogues"] or not site["series"]):
            raise SourceError(f"Open LAQN site {ref} is missing species catalogue evidence")
        station_rows.append({
            "connector_id": connector_id, "network_id": connector["default_network_id"],
            "service_ref": CONNECTOR_CODE, "station_ref": ref, "label": name,
            "station_name": name, "station_type": text(detail.get("@SiteType")) or None,
            "first_seen_at": iso(opened), "last_seen_at": now.isoformat(),
            "removed_at": removed_at,
            **({"geometry": f"SRID=4326;POINT({lon} {lat})", "latitude": lat, "longitude": lon}
               if lat is not None and lon is not None else {}),
        })
        metadata[ref] = source
        series_by_ref[ref] = concentration_series(site, now)

    codes = {code for channels in series_by_ref.values() for code, row in channels.items()
             if not row["ambiguous"]}
    if not codes:
        raise SourceError("LAQN response contains no supported unambiguous concentration series")
    registered = _all_rows(lambda: core.table("timeseries")
        .select("station_id,timeseries_ref,extras").eq("connector_id", connector_id)
        .eq("service_ref", CONNECTOR_CODE).order("id"))
    existing_series = {row["timeseries_ref"]: row for row in registered}
    # Keep absent channels intact, but report them so a partial catalogue stays visible.
    retained_sites = sorted(prior.keys() - metadata.keys())
    seen_series = {f"{ref}:{code}" for ref, series in series_by_ref.items() for code in series}
    retained_series = sorted(existing_series.keys() - seen_series)
    if not any(row["pollable"] for series in series_by_ref.values() for row in series.values()):
        raise SourceError("LAQN source catalogue contains no currently pollable concentration channel")

    phenomenon_rows = [{
        "connector_id": connector_id, "label": code, "source_label": code, "notation": code,
        "pollutant_label": code, "source_uom": UOM, "mapping_kind": "raw_observed_property",
        "observed_property_code": SPECIES[code], "is_aqi_eligible": True,
    } for code in sorted(codes)]
    public = schemas.client.schema(os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public")
    phenomena = upsert_phenomena_via_rpc(public, phenomenon_rows, allow_mapping_upsert=True)

    # Different station payload shapes preserve old coordinates when the source omits them.
    for with_geometry in (True, False):
        write_batches(core, "stations", [row for row in station_rows if ("geometry" in row) == with_geometry],
                      "connector_id,service_ref,station_ref")
    station_ids = {
        row["station_ref"]: row["id"] for row in _all_rows(
            lambda: core.table("stations").select("id,station_ref")
            .eq("connector_id", connector_id).eq("service_ref", CONNECTOR_CODE).order("id")
        )
    }
    if metadata.keys() - station_ids.keys():
        raise RuntimeError("LAQN station upserts did not resolve every source station")
    metadata_rows = [{"station_id": station_ids[ref], "attributes": attributes}
                     for ref, attributes in metadata.items()]
    current_metadata = {}
    for offset in range(0, len(metadata_rows), 150):
        ids = [row["station_id"] for row in metadata_rows[offset:offset + 150]]
        current_metadata.update({
            row["station_id"]: row["attributes"] for row in _rows(
                core.table("station_metadata").select("station_id,attributes")
                .in_("station_id", ids).execute()
            )
        })
    write_batches(core, "station_initial_metadata", metadata_rows, "station_id", ignore=True)
    write_batches(core, "station_metadata", [
        {**row, "attributes": {**(current_metadata.get(row["station_id"]) or {}), **row["attributes"]},
         "updated_at": now.isoformat()} for row in metadata_rows
    ], "station_id")
    rows = []
    for ref, channels in series_by_ref.items():
        for code, channel in channels.items():
            series_ref = f"{ref}:{code}"
            existing = existing_series.get(series_ref, {})
            extra = {**(existing.get("extras") or {}),
                     "erg_laqn": {"site_code": ref, "species_code": code, **channel}}
            if channel["ambiguous"]:
                if existing:
                    core.table("timeseries").update({"extras": extra}).eq("connector_id", connector_id) \
                        .eq("service_ref", CONNECTOR_CODE).eq("timeseries_ref", series_ref).execute()
                continue
            rows.append({
                "connector_id": connector_id, "service_ref": CONNECTOR_CODE,
                "station_id": station_ids[ref], "timeseries_ref": series_ref,
                "label": f"{ref} {code}", "uom": UOM, "phenomenon_id": phenomena[code]["phenomenon_id"],
                "observed_property_id": properties[SPECIES[code]]["id"],
                "last_catalog_seen_at": now.isoformat(), "ended_at": channel["ended_at"],
                "extras": extra,
            })
    write_batches(core, "timeseries", rows, "connector_id,service_ref,timeseries_ref")
    write_batches(raw, "laqn_site_register", register_rows, "site_ref,snapshot_at")
    return {
        "ok": True, "snapshot_at": now.isoformat(),
        "group_sites": len(catalogue), "laqn_sites": len(sites), "timeseries": len(rows),
        "unclassified_sites": sorted(ref for ref, site in catalogue.items() if not site["networks"]),
        "ambiguous_series": sorted(f"{ref}:{code}" for ref, series in series_by_ref.items()
                                   for code, row in series.items() if row["ambiguous"]),
        "retained_sites_absent_or_unclassified": retained_sites,
        "retained_series_absent": retained_series,
    }


def dispatch_reference() -> dict:
    project = os.environ.get("GCP_PROJECT_NUMBER", "")
    region = os.environ.get("GCP_REGION", "")
    if not re.fullmatch(r"[0-9]+", project) or not re.fullmatch(r"[a-z]+-[a-z]+[0-9]+", region):
        raise ValueError("LAQN reference dispatch requires GCP_PROJECT_NUMBER and GCP_REGION")
    host = f"uk-aq-erg-laqn-ingest-{project}.{region}.run.app"
    secret = os.environ["UK_AQ_EDGE_UPSTREAM_SECRET"]
    response = requests.post(
        f"https://{host}/reference",
        headers={"x-uk-aq-upstream-auth": secret}, timeout=(10, 420), allow_redirects=False,
    )
    if response.status_code != 200:
        raise RuntimeError(f"LAQN reference service failed (HTTP {response.status_code}): {response.text[:1200]}")
    result = response.json()
    if result.get("ok") is not True:
        raise RuntimeError(f"LAQN reference refresh failed: {result}")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dispatch", action="store_true", help="Dispatch from Daily Stations")
    mode.add_argument("--refresh", action="store_true", help="Run on the approved ERG egress")
    args = parser.parse_args()
    schemas = SupabaseSchemas.from_client(create_supabase_client())
    connector = load_connector(schemas)
    if not connector or (connector.get("config") or {}).get("reference_enabled") is not True:
        result = {"ok": True, "skipped": "LAQN reference refresh disabled"}
    elif args.dispatch:
        result = dispatch_reference()
    else:
        result = refresh(schemas, connector)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
