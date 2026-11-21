"""Run a bounded ERG sweep using the shared official-network observation writer."""

from __future__ import annotations

import math
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from scripts.erg_laqn.source import (
    CONNECTOR_CODE, NETWORK_CODE, SPECIES, UOM, ErgClient, SourceError,
    SourceUnavailable, gmt_time, hourly_points, hourly_url,
)
from scripts.official_networks.canonical_site_resolver import _all_rows, _rows
from scripts.uk_aq_retention import bounded_start, eligible_observation
from scripts.uk_aq_supabase import SupabaseSchemas

MAX_WINDOW_HOURS = 72
METHOD = "erg_api"


def run_laqn(db, connector: dict, writer_factory, *, start, end, site_code,
             max_stations, dry_run) -> dict:
    config = connector.get("config") or {}
    hour_label = config.get("hour_label")
    if hour_label not in {"beginning", "ending"}:
        raise ValueError("Configure hour_label from verified ERG UTC interval evidence")
    quality_field = config.get("quality_field")
    if quality_field is not None and (
        not isinstance(quality_field, str) or not quality_field.startswith("@")
    ):
        raise ValueError("quality_field must be the verified ERG JSON attribute")
    sentinels = config.get("invalid_values", [])
    if not isinstance(sentinels, list) or any(
        type(value) not in (int, float) or value == 0 or not math.isfinite(value)
        for value in sentinels
    ):
        raise ValueError("invalid_values must list verified, finite, non-zero ERG sentinels")
    invalid_values = frozenset(sentinels)
    now = datetime.now(timezone.utc)
    end = min(end or now, now)
    window_hours = int(connector.get("poll_window_hours") or 48)
    if not 1 <= window_hours <= MAX_WINDOW_HOURS:
        raise ValueError(f"LAQN poll_window_hours must be 1–{MAX_WINDOW_HOURS}")
    start = bounded_start(start or end - timedelta(hours=window_hours), now)
    if start >= end or end - start > timedelta(hours=MAX_WINDOW_HOURS):
        raise ValueError("LAQN replay must be within the live ownership boundary and 72 hours")

    schemas = SupabaseSchemas.from_client(db.client)
    networks = _rows(schemas.core.table("networks").select("id,ingest_enabled")
                     .eq("network_code", NETWORK_CODE).execute())
    if not networks or connector["default_network_id"] != networks[0]["id"]:
        raise RuntimeError("LAQN connector and network registration disagree")
    if not dry_run and (connector["poll_enabled"] is not True
                        or networks[0]["ingest_enabled"] is not True):
        raise RuntimeError("LAQN observation ingestion is disabled")

    connector_id = connector["id"]
    query = (schemas.core.table("stations").select("id,station_ref,network_id")
             .eq("connector_id", connector_id).eq("service_ref", CONNECTOR_CODE)
             .is_("removed_at", "null"))
    if site_code:
        query = query.eq("station_ref", site_code.upper())
    stations = _all_rows(lambda: query.order("id"), page_size=500)
    if not stations or any(s["network_id"] != connector["default_network_id"] for s in stations):
        raise RuntimeError("Missing or inconsistent active LAQN station references")
    channels = defaultdict(list)
    all_channels = _all_rows(lambda: schemas.core.table("timeseries")
        .select("id,timeseries_ref,station_id,uom,extras,observed_property_id")
        .eq("connector_id", connector_id).eq("service_ref", CONNECTOR_CODE)
        .is_("ended_at", "null").order("id"))
    properties = db.property_codes([row["observed_property_id"] for row in all_channels])
    station_refs = {row["id"]: row["station_ref"] for row in stations}
    for row in all_channels:
        station = station_refs.get(row["station_id"])
        source = (row.get("extras") or {}).get("erg_laqn") or {}
        if not station or source.get("pollable") is not True:
            continue
        species = source.get("species_code")
        if (species not in SPECIES or source.get("site_code") != station
                or row["timeseries_ref"] != f"{station}:{species}" or row["uom"] != UOM
                or properties.get(row["observed_property_id"]) != SPECIES[species]):
            raise RuntimeError(f"Insufficient LAQN timeseries evidence for {row['timeseries_ref']}")
        channels[row["station_id"]].append((row["id"], species, gmt_time(source["started"])))
    stations = [station for station in stations if channels.get(station["id"])]
    if not stations:
        raise RuntimeError("No eligible LAQN timeseries; complete and mirror reference discovery")

    checkpoint_table = "erg_laqn_station_checkpoints"
    checkpoints = {}
    station_ids = [row["id"] for row in stations]
    for offset in range(0, len(station_ids), 200):
        checkpoints.update({
            row["station_id"]: row for row in _rows(schemas.raw.table(checkpoint_table)
                .select("station_id,last_attempt_at,last_delivered_at")
                .in_("station_id", station_ids[offset:offset + 200]).execute())
        })
    if max_stations is not None and max_stations < 1:
        raise ValueError("max_stations must be positive")
    stations.sort(key=lambda row: (
        gmt_time(checkpoints.get(row["id"], {}).get("last_attempt_at"))
        or datetime.min.replace(tzinfo=timezone.utc), row["id"],
    ))
    total_stations = len(stations)
    stations = stations[:max_stations]
    counters = dict.fromkeys((
        "stations_attempted", "stations_failed", "stations_without_usable_values",
        "stations_without_delivery_checkpoint", "series_polled", "series_failed",
        "source_failures", "observations_selected", "observations_upserted",
        "timeseries_updated", "hours_blank", "hours_invalid", "hours_unknown_quality",
    ), 0)
    secondary = dict.fromkeys(("written", "enqueued", "pubsub_observs"), 0)
    warnings = []
    newest = None
    blocked = False
    # Leave time for database calls and worker bookkeeping before the 900s supervisor cutoff.
    deadline = time.monotonic() + 750
    writer = writer_factory(db.client) if not dry_run else None
    api = ErgClient()
    with api.session:
        for station in stations:
            if blocked or time.monotonic() >= deadline:
                break
            station_id, ref = station["id"], station["station_ref"]
            attempt_at = datetime.now(timezone.utc).isoformat()
            if not dry_run:
                schemas.raw.table(checkpoint_table).upsert({
                    "station_id": station_id, "last_attempt_at": attempt_at, "updated_at": attempt_at,
                }, on_conflict="station_id").execute()
            counters["stations_attempted"] += 1
            rows = []
            station_failed = False
            complete = True
            for series_id, species, series_start in channels[station_id]:
                if time.monotonic() >= deadline:
                    station_failed = True
                    warnings.append(f"{ref}: sweep budget exhausted within station")
                    break
                series_from = max(start, series_start)
                if series_from >= end:
                    complete = False
                    continue
                try:
                    payload = api.get(hourly_url(ref, species, series_from, end, hour_label))
                    points, stats = hourly_points(
                        payload, ref, species, series_from, end, hour_label, quality_field, invalid_values,
                    )
                except SourceError as exc:
                    counters["series_failed"] += 1
                    counters["source_failures"] += 1
                    station_failed = True
                    warnings.append(f"{ref}:{species}: {exc}")
                    if isinstance(exc, SourceUnavailable):
                        blocked = True
                        break
                    continue
                counters["series_polled"] += 1
                for key, value in stats.items():
                    counters[key] += value
                if stats["hours_invalid"]:
                    station_failed = True
                    warnings.append(f"{ref}:{species}: {stats['hours_invalid']} invalid source hours")
                complete = complete and bool(points)
                rows.extend({
                    "connector_id": connector_id, "timeseries_id": series_id,
                    "observed_at": at.isoformat(), "value": value, "status": status,
                } for at, value, status in points if eligible_observation(at))
            counters["observations_selected"] += len(rows)
            if not rows:
                counters["stations_without_usable_values"] += 1
            try:
                last_delivered = max((row["observed_at"] for row in rows), default=None)
                if not dry_run and rows:
                    counters["observations_upserted"] += db.write_observations(rows, METHOD)
                    counters["timeseries_updated"] += db.update_last_values(rows)
                    delivered = writer.write(rows)
                    for key, value in delivered.items():
                        secondary[key] += value
                    if sum(delivered.values()) != len(rows):
                        raise RuntimeError("LAQN secondary observation delivery count differs")
                if not rows or not complete or station_failed:
                    counters["stations_without_delivery_checkpoint"] += 1
                if not dry_run and last_delivered:
                    previous = checkpoints.get(station_id, {}).get("last_delivered_at")
                    patch = {"updated_at": datetime.now(timezone.utc).isoformat()}
                    if not previous or gmt_time(last_delivered) > gmt_time(previous):
                        patch["last_delivered_at"] = last_delivered
                    if complete and not station_failed:
                        patch["last_polled_at"] = attempt_at
                    schemas.raw.table(checkpoint_table).update(patch).eq("station_id", station_id) \
                        .eq("last_attempt_at", attempt_at).execute()
                if last_delivered:
                    newest = max(newest or last_delivered, last_delivered)
            except Exception as exc:
                station_failed = True
                warnings.append(f"{ref}: write/delivery/checkpoint failed ({type(exc).__name__}): {exc}")
            counters["stations_failed"] += station_failed

    deferred = total_stations - counters["stations_attempted"]
    if counters["stations_attempted"] == counters["stations_failed"] and not newest:
        status = "failed"
    elif deferred or counters["stations_failed"] or counters["stations_without_delivery_checkpoint"]:
        status = "partial"
    else:
        status = "succeeded"
    return {
        "ok": status != "failed", "connector_id": connector_id,
        "connector_code": CONNECTOR_CODE, "network_code": NETWORK_CODE,
        "network_id": connector["default_network_id"], "run_status": status,
        "run_message": (
            f"{METHOD}: {counters['observations_selected']} observations, "
            f"{counters['stations_failed']} failed, {deferred} deferred, "
            f"{counters['stations_without_usable_values']} stations without usable values"
        ),
        "acquisition_method": METHOD, "stations_total": total_stations,
        "stations_deferred": deferred, "stations_updated": 0,
        "window_start": start.isoformat(), "window_end": end.isoformat(),
        "dry_run": dry_run, "last_observed_at": newest,
        "secondary": secondary, "warnings": warnings[:50], **counters,
    }
