#!/usr/bin/env python3
"""Capture read-only ERG evidence and compare it with a read-only UK AQ TEST export."""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.erg_laqn.catalogue import aurn_identity_evidence, build_catalogue, coordinate, iso
from scripts.erg_laqn.source import (
    CONNECTOR_CODE, GROUPS_URL, SITE_URL, SPECIES, SPECIES_URL, ErgClient, gmt_time,
    hourly_points, hourly_url, source_code,
)

AURN = "Automatic Urban and Rural Monitoring Network (AURN)"
TEST_EXPORT = """
begin transaction isolation level repeatable read read only;
set local time zone 'UTC';
select jsonb_build_object(
  'captured_at', now(),
  'connectors', (select coalesce(jsonb_agg(to_jsonb(c)), '[]') from (
    select id, connector_code, default_network_id from uk_aq_core.connectors order by id
  ) c),
  'networks', (select coalesce(jsonb_agg(to_jsonb(n)), '[]') from (
    select id, network_code from uk_aq_core.networks order by id
  ) n),
  'stations', (select coalesce(jsonb_agg(to_jsonb(s)), '[]') from (
    select id, connector_id, network_id, service_ref, station_ref, station_name,
           match_id, latitude, longitude, first_seen_at, removed_at
    from uk_aq_core.stations order by id
  ) s),
  'timeseries', (select coalesce(jsonb_agg(to_jsonb(t)), '[]') from (
    select t.id, t.station_id, t.timeseries_ref, t.first_value_at, t.last_value_at,
           t.ended_at, op.code as property_code, t.uom
    from uk_aq_core.timeseries t left join uk_aq_core.observed_properties op
      on op.id = t.observed_property_id
    order by t.id
  ) t),
  'matches', (select coalesce(jsonb_agg(to_jsonb(m)), '[]') from (
    select id, uk_air_ref, metadata->'canonical_site_identity' as evidence
    from uk_aq_core.station_matches order by id
  ) m),
  'sos_bridges', (select coalesce(jsonb_agg(to_jsonb(b)), '[]') from (
    select station_id, uk_air_ref, match_method, source_snapshot_at
    from uk_aq_raw.sos_station_uk_air_refs order by station_id
  ) b),
  'defra_sites', (select coalesce(jsonb_agg(to_jsonb(d)), '[]') from (
    select site_ref, uk_air_ref, start_date, end_date, snapshot_at, networks
    from uk_aq_raw.sos_site_register
    where snapshot_at = (select max(snapshot_at) from uk_aq_raw.sos_site_register)
    order by site_ref, uk_air_ref
  ) d)
);
commit;
"""


def read_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_json(payload: dict, path: Path) -> None:
    # Preserve the evidence from earlier captures if the operator reuses a path.
    with path.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def capture(args) -> dict:
    if len(args.site) > 3:
        raise ValueError("Capture allows up to three sites per run")
    if args.site and (not args.start or not args.end or args.end <= args.start
                      or args.end - args.start > timedelta(days=7)):
        raise ValueError("Supply an increasing UTC date range of up to seven days")
    api = ErgClient()
    with api.session:
        bundle = {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "urls": {"groups": GROUPS_URL, "sites": SITE_URL, "species": SPECIES_URL},
            "groups": api.get(GROUPS_URL), "sites": api.get(SITE_URL),
            "species": api.get(SPECIES_URL), "samples": [],
        }
        site_catalogue = build_catalogue(bundle["sites"], bundle["species"], None)
        for site in sorted({source_code(ref) for ref in args.site}):
            if site not in site_catalogue:
                raise ValueError(f"{site} is absent from the ERG catalogue")
            start = datetime.combine(args.start, datetime.min.time(), tzinfo=timezone.utc)
            # Input end is exclusive in publisher-label dates.
            end = datetime.combine(args.end, datetime.min.time(), tzinfo=timezone.utc) - timedelta(hours=1)
            for species in sorted(SPECIES.keys() & site_catalogue[site]["series"].keys()):
                url = hourly_url(site, species, start, end, "ending")
                bundle["samples"].append({
                    "site": site, "species": species, "url": url, "payload": api.get(url),
                })
    return bundle


def nearby(site: dict, candidate: dict) -> bool:
    if None in (site["latitude"], site["longitude"], candidate["latitude"], candidate["longitude"]):
        return False
    lat, lon = map(math.radians, (site["latitude"], site["longitude"]))
    other_lat, other_lon = map(math.radians, (candidate["latitude"], candidate["longitude"]))
    metres = 2 * 6371000 * math.asin(math.sqrt(
        math.sin((other_lat - lat) / 2) ** 2
        + math.cos(lat) * math.cos(other_lat) * math.sin((other_lon - lon) / 2) ** 2
    ))
    return metres <= 50


def sample_reporting(bundle, args, cutoff, as_of):
    reporting = {}
    if not args.hour_label:
        return reporting
    for sample in bundle.get("samples", []):
        ref, code = sample["site"], sample["species"]
        if code not in SPECIES:
            continue
        points, _ = hourly_points(sample["payload"], ref, code, cutoff, as_of,
                                     args.hour_label, args.quality_field)
        if points:
            reporting[(ref, code)] = max(iso(points[-1][0]), reporting.get((ref, code), ""))
    return reporting


def compare(bundle: dict, test: dict, args) -> dict:
    as_of = gmt_time(bundle.get("captured_at"))
    test_captured_at = gmt_time(test.get("captured_at"))
    if not as_of or not test_captured_at or args.recent_days < 1:
        raise ValueError("Dated ERG and TEST exports and a positive recent-days are required")
    start = datetime.combine(args.history_start, datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(args.history_end, datetime.min.time(), tzinfo=timezone.utc)
    if start >= end:
        raise ValueError("history-end is exclusive and must follow history-start")
    cutoff = as_of - timedelta(days=args.recent_days)
    evidence = read_json(args.membership) if args.membership else None
    catalogue = build_catalogue(bundle["sites"], bundle["species"], evidence)
    reporting = sample_reporting(bundle, args, cutoff, as_of)
    reviewed = read_json(args.identity_review) if args.identity_review else {}
    connectors = {r["id"]: r["connector_code"] for r in test["connectors"]}
    networks = {r["id"]: r["network_code"] for r in test["networks"]}
    matches = {r["id"]: r for r in test["matches"]}
    defra = [r for r in test["defra_sites"] if AURN in r["networks"]]
    test_series = defaultdict(list)
    for row in test["timeseries"]:
        test_series[row["station_id"]].append(row)
    bridges = {r["station_id"]: r for r in test["sos_bridges"]}
    stations_by_id, laqn_existing = {}, {}
    for row in test["stations"]:
        item = {**row, "connector_code": connectors[row["connector_id"]],
                "network_code": networks[row["network_id"]]}
        if item["connector_code"] == CONNECTOR_CODE:
            if item["service_ref"] == CONNECTOR_CODE:
                laqn_existing[item["station_ref"]] = item
        else:
            stations_by_id[row["id"]] = item

    laqn_sites = {
        ref: {**site, "latitude": coordinate(site["detail"].get("@Latitude"), 90),
              "longitude": coordinate(site["detail"].get("@Longitude"), 180)}
        for ref, site in catalogue.items() if site["laqn"]
    }
    rows = []
    for ref, site in laqn_sites.items():
        detail = site["detail"]
        explicit = aurn_identity_evidence(ref, site["networks"], site["opened"], site["closed"], defra)
        existing = laqn_existing.get(ref, {})
        match = matches.get(existing.get("match_id"))
        confirmed = {}
        colocated = []
        for candidate in stations_by_id.values():
            station_id = candidate["id"]
            bridge = bridges.get(station_id, {})
            if (explicit and candidate["network_code"] == "gov_uk_aurn"
                    and bridge.get("uk_air_ref") == explicit["uk_air_ref"]):
                confirmed[station_id] = {**candidate, "evidence": {**explicit, "sos_bridge": bridge}}
            elif match and candidate.get("match_id") == match["id"]:
                confirmed[station_id] = {**candidate, "evidence": match}
            elif nearby(site, candidate):
                colocated.append(candidate)
        review = reviewed.get(ref, {})
        if review and not all(review.get(key) for key in ("source_url", "checked_on", "reason")):
            raise ValueError(f"Identity review for {ref} requires a dated source and reason")
        for station_id in review.get("same_station_ids", []):
            if station_id in stations_by_id:
                confirmed[station_id] = {**stations_by_id[station_id], "evidence": review}
        independent = set(review.get("independent_station_ids", []))
        laqn_colocated = sorted(
            other_ref for other_ref, other in laqn_sites.items()
            if other_ref != ref and nearby(site, other)
        )
        independent_laqn = sorted(set(review.get("independent_laqn_site_codes", []))
                                  & set(laqn_colocated))
        if independent & confirmed.keys():
            raise ValueError(f"Identity review contradicts canonical evidence for {ref}")
        uncertain = [row for row in colocated if row["id"] not in confirmed and row["id"] not in independent]
        identity = "uncertain"
        if any(row["network_code"] == "gov_uk_aurn" for row in confirmed.values()):
            identity = "represented_aurn"
        elif confirmed:
            identity = "represented_other"
        elif review.get("additional_station") is True and not uncertain:
            identity = "confirmed_additional"

        pollutants = {}
        for code, property_code in SPECIES.items():
            if code not in site["series"]:
                continue
            series_evidence = site["series"][code]
            overlapping = [
                row for row in series_evidence
                if (gmt_time(row.get("@DateMeasurementStarted")) or site["opened"] or end) < end
                and (gmt_time(row.get("@DateMeasurementFinished")) or site["closed"] or end) >= start
                and (not site["closed"] or site["closed"] >= start)
            ]
            registered = (len(series_evidence) == 1 and site["closure_known"]
                          and (not site["closed"] or site["closed"] > as_of)
                          and (gmt_time(series_evidence[0].get("@DateMeasurementStarted"))
                               or site["opened"] or as_of + timedelta(seconds=1)) <= as_of
                          and (gmt_time(series_evidence[0].get("@DateMeasurementFinished"))
                               or as_of + timedelta(seconds=1)) > as_of)
            comparable = [
                row for station_id in confirmed for row in test_series[station_id]
                if (not confirmed[station_id]["removed_at"]
                    or gmt_time(confirmed[station_id]["removed_at"]) > as_of)
                and row["property_code"] == property_code and row["uom"] == "ug/m3"
                and (not row["ended_at"] or gmt_time(row["ended_at"]) > as_of)
            ]
            latest = reporting.get((ref, code))
            if not latest and existing:
                latest_at = max((
                    gmt_time(row["last_value_at"]) for row in test_series[existing["id"]]
                    if row["property_code"] == property_code and row["last_value_at"]
                    and cutoff <= gmt_time(row["last_value_at"]) <= as_of
                ), default=None)
                latest = iso(latest_at)
            pollutants[property_code] = {
                "source_species": code, "source_records": series_evidence,
                "registered_current": registered, "recent_positive_evidence_at": latest,
                "matched_test_series": comparable,
                "additional_registered": bool(registered and not comparable
                                              and identity != "uncertain"),
                "additional_recent_reporting": bool(latest and not comparable
                                                     and identity != "uncertain"),
                "historical_candidate": bool(overlapping),
            }
        rows.append({
            "site_ref": ref, "name": detail.get("@SiteName"), "networks": site["networks"],
            "opened_at": iso(site["opened"]), "closed_at": iso(site["closed"]),
            "current": (site["closure_known"] and site["opened"] is not None and site["opened"] <= as_of
                        and (not site["closed"] or site["closed"] > as_of)),
            "closure_known": site["closure_known"], "identity": identity,
            "represented_aurn": any(row["network_code"] == "gov_uk_aurn" for row in confirmed.values()),
            "represented_other": any(row["network_code"] != "gov_uk_aurn" for row in confirmed.values()),
            "recent_reporting_evidenced": any(p["recent_positive_evidence_at"] for p in pollutants.values()),
            "represented": list(confirmed.values()), "possible_colocations": uncertain,
            "independent_colocated": [row for row in colocated if row["id"] in independent],
            "nearby_laqn_source_codes": laqn_colocated,
            "independent_colocated_laqn_codes": independent_laqn,
            "review_evidence": review or None, "source_catalogue": site["sources"],
            "membership_evidence": site["membership_evidence"], "pollutants": pollutants,
        })
    current = [row for row in rows if row["current"]]
    return {
        "erg_captured_at": iso(as_of), "test_captured_at": iso(test_captured_at),
        "recent_from": iso(cutoff), "historical_range": [iso(start), iso(end)],
        "source_urls": bundle.get("urls"), "london_group_site_count": len(catalogue),
        "unclassified_site_codes": sorted(ref for ref, site in catalogue.items() if not site["networks"]),
        "known_laqn_site_count": len(rows), "known_current_laqn_site_count": len(current),
        "current_station_identity_counts": dict(Counter(row["identity"] for row in current)),
        "current_stations_with_aurn": sum(row["represented_aurn"] for row in current),
        "current_stations_with_other_connectors": sum(row["represented_other"] for row in current),
        "current_stations_with_independent_colocations": sum(bool(row["independent_colocated"] or row["independent_colocated_laqn_codes"]) for row in current),
        "current_stations_with_recent_reporting_evidence": sum(row["recent_reporting_evidenced"] for row in current),
        "current_stations_without_recent_reporting_evidence": sum(not row["recent_reporting_evidenced"] for row in current),
        "historical_candidate_stations": sum(any(p["historical_candidate"] for p in row["pollutants"].values()) for row in rows),
        "pollutant_counts": {
            code: dict(Counter(
                flag for row in rows for flag in ("registered_current", "additional_registered",
                    "additional_recent_reporting", "recent_positive_evidence_at", "historical_candidate")
                if row["pollutants"].get(code, {}).get(flag)
            )) for code in SPECIES.values()
        },
        "rows": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    fetch = commands.add_parser("capture")
    fetch.add_argument("--site", action="append", default=[])
    fetch.add_argument("--start", type=date.fromisoformat)
    fetch.add_argument("--end", type=date.fromisoformat)
    export = commands.add_parser("export-test", help="Use psql environment/PGSERVICE with read-only TEST credentials")
    report = commands.add_parser("compare")
    report.add_argument("--erg", type=Path, required=True)
    report.add_argument("--test", type=Path, required=True)
    report.add_argument("--membership", type=Path)
    report.add_argument("--identity-review", type=Path)
    report.add_argument("--hour-label", choices=("beginning", "ending"))
    report.add_argument("--quality-field")
    report.add_argument("--recent-days", type=int, default=7)
    report.add_argument("--history-start", type=date.fromisoformat, required=True)
    report.add_argument("--history-end", type=date.fromisoformat, required=True)
    for command in (fetch, export, report):
        command.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    if args.command == "capture":
        payload = capture(args)
    elif args.command == "export-test":
        result = subprocess.run(
            ["psql", "-XqAt", "-v", "ON_ERROR_STOP=1"], input=TEST_EXPORT,
            check=True, capture_output=True, text=True, timeout=90,
        )
        payload = json.loads(result.stdout)
    else:
        payload = compare(read_json(args.erg), read_json(args.test), args)
    write_json(payload, args.output)


if __name__ == "__main__":
    main()
