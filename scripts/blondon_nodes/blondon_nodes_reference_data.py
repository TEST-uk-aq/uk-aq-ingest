#!/usr/bin/env python3
"""Shared deterministic reference data for Breathe London Nodes."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple

from scripts.uk_aq_phenomena_rpc import upsert_phenomena_via_rpc


DEFAULT_SPECIES = ("PM25", "NO2", "PM25Index", "NO2Index")

SPECIES_CONFIG: Dict[str, Dict[str, Any]] = {
    "PM25": {
        "label": "PM2.5",
        "uom": "ug.m-3",
        "source_label": "breathelondon_nodes:pm2.5",
        "notation": "PM2.5",
        "pollutant_label": "pm2.5",
        "kind": "pollutant",
        "mapping_kind": "raw_observed_property",
        "observed_property_code": "pm25",
        "is_aqi_eligible": True,
    },
    "NO2": {
        "label": "NO2",
        "uom": "ug.m-3",
        "source_label": "breathelondon_nodes:no2",
        "notation": "NO2",
        "pollutant_label": "no2",
        "kind": "pollutant",
        "mapping_kind": "raw_observed_property",
        "observed_property_code": "no2",
        "is_aqi_eligible": True,
    },
    "PM25Index": {
        "label": "PM2.5 DAQI",
        "uom": "DAQI",
        "source_label": "breathelondon_nodes:pm2.5:daqi",
        "notation": "PM2.5 DAQI",
        "pollutant_label": "daqi_pm25",
        "kind": "daqi_index",
        "mapping_kind": "derived_index",
        "observed_property_code": "pm25index",
        "is_aqi_eligible": False,
    },
    "NO2Index": {
        "label": "NO2 DAQI",
        "uom": "DAQI",
        "source_label": "breathelondon_nodes:no2:daqi",
        "notation": "NO2 DAQI",
        "pollutant_label": "daqi_no2",
        "kind": "daqi_index",
        "mapping_kind": "derived_index",
        "observed_property_code": "no2index",
        "is_aqi_eligible": False,
    },
}


def build_nodes_phenomena_rows(
    connector_id: int,
    species: Sequence[str] = DEFAULT_SPECIES,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for species_name in species:
        config = SPECIES_CONFIG.get(species_name)
        if config is None:
            raise RuntimeError(f"Unsupported Breathe London Nodes species: {species_name}")
        rows.append(
            {
                "connector_id": connector_id,
                "label": config["label"],
                "source_label": config["source_label"],
                "notation": config["notation"],
                "pollutant_label": config["pollutant_label"],
                "source_uom": config["uom"],
                "mapping_kind": config["mapping_kind"],
                "observed_property_code": config["observed_property_code"],
                "is_aqi_eligible": config["is_aqi_eligible"],
            }
        )
    return rows


def validate_nodes_phenomena_results(
    input_rows: Iterable[Dict[str, Any]],
    diagnostics_by_source_label: Mapping[str, Mapping[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    phenomenon_ids: Dict[str, int] = {}
    observed_property_ids: Dict[str, int] = {}
    for input_row in input_rows:
        source_label = str(input_row["source_label"])
        diagnostic = diagnostics_by_source_label.get(source_label)
        if diagnostic is None or diagnostic.get("phenomenon_id") is None:
            raise RuntimeError(
                f"Central phenomena RPC missing phenomenon_id for {source_label}"
            )
        if diagnostic.get("observed_property_id") is None:
            raise RuntimeError(
                "Central phenomena RPC missing canonical observed_property_id for "
                f"{source_label}"
            )
        if diagnostic.get("mapping_warning"):
            raise RuntimeError(
                "Central phenomena RPC returned mapping warning for "
                f"{source_label}: {diagnostic['mapping_warning']}"
            )
        if diagnostic.get("mapping_kind") != input_row["mapping_kind"]:
            raise RuntimeError(
                f"Central phenomena RPC mapping kind mismatch for {source_label}"
            )
        if (
            diagnostic.get("observed_property_code")
            != input_row["observed_property_code"]
        ):
            raise RuntimeError(
                f"Central phenomena RPC observed-property mismatch for {source_label}"
            )
        if diagnostic.get("is_aqi_eligible") is not input_row["is_aqi_eligible"]:
            raise RuntimeError(
                f"Central phenomena RPC AQI eligibility mismatch for {source_label}"
            )
        phenomenon_ids[source_label] = int(diagnostic["phenomenon_id"])
        observed_property_ids[source_label] = int(
            diagnostic["observed_property_id"]
        )
    return phenomenon_ids, observed_property_ids


def upsert_nodes_phenomena(
    public_client: Any,
    connector_id: int,
    species: Sequence[str] = DEFAULT_SPECIES,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    rows = build_nodes_phenomena_rows(connector_id, species)
    diagnostics = upsert_phenomena_via_rpc(public_client, rows)
    return validate_nodes_phenomena_results(rows, diagnostics)


def nodes_timeseries_ref(station_ref: str, species: str) -> str:
    clean_station_ref = str(station_ref or "").strip()
    if not clean_station_ref:
        raise RuntimeError("Breathe London Nodes station_ref is required")
    if species not in SPECIES_CONFIG:
        raise RuntimeError(f"Unsupported Breathe London Nodes species: {species}")
    return f"{clean_station_ref}:{species}"


def build_nodes_timeseries_rows(
    stations: Iterable[Mapping[str, Any]],
    *,
    connector_id: int,
    phenomenon_ids: Mapping[str, int],
    observed_property_ids: Mapping[str, int],
    service_ref: str,
    species: Sequence[str] = DEFAULT_SPECIES,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    identities: set[str] = set()
    for station in stations:
        station_ref = str(station.get("station_ref") or "").strip()
        station_name = (
            station.get("station_name")
            or station.get("label")
            or station_ref
        )
        try:
            station_id = int(station["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Invalid Breathe London Nodes station id for {station_ref or '<missing>'}"
            ) from exc
        for species_name in species:
            config = SPECIES_CONFIG.get(species_name)
            if config is None:
                raise RuntimeError(
                    f"Unsupported Breathe London Nodes species: {species_name}"
                )
            timeseries_ref = nodes_timeseries_ref(station_ref, species_name)
            if timeseries_ref in identities:
                raise RuntimeError(
                    f"Duplicate generated Nodes timeseries identity: {timeseries_ref}"
                )
            identities.add(timeseries_ref)
            source_label = str(config["source_label"])
            rows.append(
                {
                    "timeseries_ref": timeseries_ref,
                    "label": f"{station_name} {config['label']}",
                    "uom": config["uom"],
                    "station_id": station_id,
                    "service_ref": service_ref,
                    "connector_id": connector_id,
                    "phenomenon_id": phenomenon_ids.get(source_label),
                    "observed_property_id": observed_property_ids.get(source_label),
                    "extras": {
                        "site_code": station_ref,
                        "species": species_name,
                        "measurement_kind": config["kind"],
                        "api_units": config["uom"],
                    },
                }
            )
    return rows
