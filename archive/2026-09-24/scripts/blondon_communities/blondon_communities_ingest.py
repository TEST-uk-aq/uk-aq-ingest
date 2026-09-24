#!/usr/bin/env python3
"""
Ingest Breathe London Communities observations with staged checkpoints.

Examples:
  python3 scripts/blondon_communities/blondon_communities_ingest.py
  python3 scripts/blondon_communities/blondon_communities_ingest.py --initial-days 30 --window-hours 12
  python3 scripts/blondon_communities/blondon_communities_ingest.py --limit 5 --dry-run
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.blondon_communities.blondon_communities_list_stations import (
    BLONDON_COMMUNITIES_SERVICE_REF,
    BreatheLondonClient,
    SupabaseWriter,
    load_api_key,
    normalize_station_payload,
)

load_dotenv()

LOG = logging.getLogger("blondon_communities_ingest")
DEFAULT_LOG_LEVEL = os.getenv("BLONDON_COMMUNITIES_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))

DEFAULT_INITIAL_DAYS = 7
DEFAULT_WINDOW_HOURS = 24
DEFAULT_SLEEP_SECONDS = 0.2
DEFAULT_BATCH_SIZE = 500

SPECIES_CONFIG = {
    "IPM25": {
        "label": "PM2.5",
        "uom": "ug/m3",
        "source_label": "breathelondon:pm2.5",
        "notation": "PM2.5",
        "pollutant_label": "pm2.5",
    },
    "INO2": {
        "label": "NO2",
        "uom": "ug/m3",
        "source_label": "breathelondon:no2",
        "notation": "NO2",
        "pollutant_label": "no2",
    },
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _floor_to_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_species_list(value: str) -> List[str]:
    items = [item.strip().upper() for item in value.split(",") if item.strip()]
    return [item for item in items if item in SPECIES_CONFIG]


def _parse_start_date(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    try:
        if "T" not in text and " " not in text:
            parsed = datetime.fromisoformat(text)
            return parsed.replace(tzinfo=timezone.utc)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed
    except ValueError:
        return None


def _build_timeseries_ref(station_ref: str, species: str) -> str:
    return f"{station_ref}:{species}"


def _extract_observations(
    payload: Any, timeseries_id: int
) -> Tuple[List[Dict[str, Any]], Optional[datetime], Optional[float]]:
    rows: List[Dict[str, Any]] = []
    last_observed = None
    last_value = None
    if isinstance(payload, list) and payload and isinstance(payload[0], list):
        payload = payload[0]
    if not isinstance(payload, list):
        return rows, last_observed, last_value
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        observed_at = _parse_iso_datetime(entry.get("DateTime"))
        value = _coerce_float(entry.get("ScaledValue"))
        if observed_at is None or value is None:
            continue
        rows.append(
            {
                "timeseries_id": timeseries_id,
                "observed_at": observed_at.isoformat(),
                "value": value,
            }
        )
        if last_observed is None or observed_at > last_observed:
            last_observed = observed_at
            last_value = value
    return rows, last_observed, last_value


def _chunked(values: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    if size <= 0:
        size = DEFAULT_BATCH_SIZE
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def _write_json(output: Optional[str], payload: Any) -> None:
    if not output:
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest Breathe London Communities observations with checkpoints."
    )
    parser.add_argument(
        "--api-key",
        help="API key override (otherwise uses BLONDON_COMMUNITIES_API_KEY).",
    )
    parser.add_argument(
        "--species",
        default="IPM25,INO2",
        help="Comma-separated species list (default: IPM25,INO2).",
    )
    parser.add_argument(
        "--initial-days",
        type=int,
        default=DEFAULT_INITIAL_DAYS,
        help="Days of history to fetch when no checkpoint exists (default: 7).",
    )
    parser.add_argument(
        "--start-date",
        help="ISO date/time override for first fetch when no checkpoint exists.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=DEFAULT_WINDOW_HOURS,
        help="Hours per API request window (default: 24).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="Delay between API calls (default: 0.2).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Observation upsert batch size (default: 500).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of stations processed (for testing).",
    )
    parser.add_argument(
        "--skip-stations",
        action="store_true",
        help="Skip station upserts (assumes stations already exist).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write to Supabase.",
    )
    parser.add_argument(
        "--output-timeseries",
        help="Write timeseries rows to this JSON file (best with --limit).",
    )
    parser.add_argument(
        "--output-observations",
        help="Write observation rows to this JSON file (best with --limit).",
    )
    parser.add_argument(
        "--output-checkpoints",
        help="Write checkpoint rows to this JSON file (best with --limit).",
    )
    parser.add_argument(
        "--ignore-checkpoints",
        action="store_true",
        help="Ignore checkpoint last_observed_at when fetching observations.",
    )
    parser.add_argument(
        "--recent-stations",
        action="store_true",
        help="When combined with --skip-stations, pick stations with recent observations.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    species_list = _parse_species_list(args.species)
    if not species_list:
        raise SystemExit("No valid species specified.")

    api_key = load_api_key(args.api_key)
    client = BreatheLondonClient(api_key)
    writer = SupabaseWriter()
    connector_id = writer.fetch_connector_id()
    if connector_id is None:
        raise SystemExit("Connector not found for Breathe London. Run the list_stations job first.")

    station_rows: List[Dict[str, Any]] = []
    metadata_by_ref: Dict[str, Dict[str, Any]] = {}

    if args.skip_stations:
        if args.recent_stations:
            station_limit = args.limit or 5
            station_rows = writer.fetch_recent_stations(
                connector_id,
                BLONDON_COMMUNITIES_SERVICE_REF,
                station_limit,
            )
            LOG.info(
                "Loaded %s recent stations from Supabase (skip-stations).",
                len(station_rows),
            )
        else:
            station_rows = writer.fetch_stations(
                connector_id,
                BLONDON_COMMUNITIES_SERVICE_REF,
                args.limit,
            )
            LOG.info(
                "Loaded %s stations from Supabase (skip-stations).",
                len(station_rows),
            )
        if not station_rows:
            LOG.warning("No stations returned from Supabase for Breathe London.")
            return 0
    else:
        sensors = client.list_sensors()
        if args.limit is not None:
            sensors = sensors[: args.limit]

        if not sensors:
            LOG.warning("No sensors returned from Breathe London.")
            return 0

        for sensor in sensors:
            row, metadata = normalize_station_payload(sensor, connector_id)
            if not row.get("station_ref"):
                continue
            station_rows.append(row)
            if metadata:
                metadata_by_ref[str(row["station_ref"])] = metadata

        if not args.dry_run:
            upserted = writer.upsert_stations(station_rows)
            LOG.info("Upserted %s stations.", upserted)
            if metadata_by_ref:
                id_map = writer.fetch_station_ids_by_ref(
                    connector_id, BLONDON_COMMUNITIES_SERVICE_REF, metadata_by_ref.keys()
                )
                attributes_by_station = {
                    id_map[ref]: attrs
                    for ref, attrs in metadata_by_ref.items()
                    if ref in id_map
                }
                if attributes_by_station:
                    updated = writer.upsert_station_metadata(attributes_by_station)
                    LOG.info("Upserted %s station_metadata rows.", updated)
        else:
            LOG.info("Dry run: skipping station upserts.")

    if args.skip_stations:
        station_id_map: Dict[str, int] = {}
        for row in station_rows:
            ref = row.get("station_ref")
            station_id = row.get("id")
            if not ref or station_id is None:
                continue
            try:
                station_id_map[str(ref)] = int(station_id)
            except (TypeError, ValueError):
                continue
    else:
        station_id_map = writer.fetch_station_ids_by_ref(
            connector_id, BLONDON_COMMUNITIES_SERVICE_REF, [row["station_ref"] for row in station_rows]
        )
    if not station_id_map:
        LOG.warning("No station ids resolved for Breathe London.")
        return 0

    phenomena_rows = []
    for species in species_list:
        config = SPECIES_CONFIG[species]
        phenomena_rows.append(
            {
                "connector_id": connector_id,
                "label": config["label"],
                "source_label": config["source_label"],
                "notation": config["notation"],
                "pollutant_label": config["pollutant_label"],
            }
        )
    if not args.dry_run:
        writer.upsert_phenomena(phenomena_rows)
    phenomenon_ids = writer.fetch_phenomena_ids(
        connector_id, [row["source_label"] for row in phenomena_rows]
    )

    timeseries_rows = []
    for row in station_rows:
        station_ref = str(row["station_ref"])
        station_id = station_id_map.get(station_ref)
        if station_id is None:
            continue
        station_name = row.get("station_name") or row.get("label") or station_ref
        for species in species_list:
            config = SPECIES_CONFIG[species]
            timeseries_rows.append(
                {
                    "timeseries_ref": _build_timeseries_ref(station_ref, species),
                    "label": f"{station_name} {config['label']}",
                    "uom": config["uom"],
                    "station_id": station_id,
                    "service_ref": BLONDON_COMMUNITIES_SERVICE_REF,
                    "connector_id": connector_id,
                    "phenomenon_id": phenomenon_ids.get(config["source_label"]),
                    "extras": {"site_code": station_ref, "species": species},
                }
            )
    if not args.dry_run:
        writer.upsert_timeseries(timeseries_rows)
    timeseries_output = [] if args.output_timeseries else None
    if timeseries_output is not None:
        timeseries_output.extend(timeseries_rows)
    timeseries_id_map = writer.fetch_timeseries_ids(
        connector_id, BLONDON_COMMUNITIES_SERVICE_REF, [row["timeseries_ref"] for row in timeseries_rows]
    )
    station_ref_by_id = {station_id: ref for ref, station_id in station_id_map.items()}

    station_ids = list({station_id_map[ref] for ref in station_id_map})
    checkpoints = writer.fetch_checkpoints(station_ids, species_list)
    checkpoints_output = [] if args.output_checkpoints else None
    if checkpoints_output is not None:
        timeseries_ref_by_id = {val: key for key, val in timeseries_id_map.items()}
        for key, row in checkpoints.items():
            station_id, _species = key
            entry = dict(row)
            entry["station_ref"] = station_ref_by_id.get(station_id)
            timeseries_id = entry.get("timeseries_id")
            try:
                timeseries_id = int(timeseries_id) if timeseries_id is not None else None
            except (TypeError, ValueError):
                timeseries_id = None
            entry["timeseries_ref"] = timeseries_ref_by_id.get(timeseries_id)
            checkpoints_output.append(entry)

    now = _floor_to_hour(utcnow())
    initial_start = _parse_start_date(args.start_date)
    if initial_start:
        initial_start = _floor_to_hour(initial_start)

    observation_total = 0
    observations_output = [] if args.output_observations else None
    checkpoint_rows = []
    timeseries_updates = []

    for row in station_rows:
        station_ref = str(row["station_ref"])
        station_id = station_id_map.get(station_ref)
        if station_id is None:
            continue
        for species in species_list:
            timeseries_ref = _build_timeseries_ref(station_ref, species)
            timeseries_id = timeseries_id_map.get(timeseries_ref)
            if timeseries_id is None:
                continue

            key = (station_id, species)
            checkpoint = checkpoints.get(key, {})
            last_observed = None if args.ignore_checkpoints else _parse_iso_datetime(
                checkpoint.get("last_observed_at")
            )
            last_error = None

            if last_observed:
                start_time = last_observed
            elif initial_start:
                start_time = initial_start
            else:
                start_time = now - timedelta(days=max(args.initial_days, 1))
            start_time = _floor_to_hour(start_time)
            if start_time >= now:
                continue

            window = timedelta(hours=max(args.window_hours, 1))
            cursor = start_time
            last_value = None

            while cursor < now:
                end_time = min(cursor + window, now)
                try:
                    payload = client.get_clarity_data(station_ref, species, cursor, end_time)
                except Exception as exc:
                    LOG.warning(
                        "Failed fetch for %s %s (%s to %s): %s",
                        station_ref,
                        species,
                        cursor.isoformat(),
                        end_time.isoformat(),
                        exc,
                    )
                    last_error = str(exc)
                    break

                obs_rows, obs_last, obs_value = _extract_observations(payload, timeseries_id)
                if observations_output is not None and obs_rows:
                    for obs_row in obs_rows:
                        observations_output.append(
                            {
                                **obs_row,
                                "timeseries_ref": timeseries_ref,
                                "station_ref": station_ref,
                                "species": species,
                            }
                        )
                if obs_rows and not args.dry_run:
                    for chunk in _chunked(obs_rows, args.batch_size):
                        writer.upsert_observations(chunk)
                    observation_total += len(obs_rows)
                if obs_last and (last_observed is None or obs_last > last_observed):
                    last_observed = obs_last
                if obs_last:
                    last_value = obs_value

                cursor = end_time
                if args.sleep_seconds:
                    time.sleep(args.sleep_seconds)

            if last_observed and last_value is not None:
                timeseries_updates.append(
                    {"id": timeseries_id, "last_value": last_value, "last_value_at": last_observed.isoformat()}
                )

            checkpoint_rows.append(
                {
                    "station_id": station_id,
                    "species": species,
                    "timeseries_id": timeseries_id,
                    "last_observed_at": last_observed.isoformat() if last_observed else None,
                    "last_polled_at": utcnow().isoformat(),
                    "last_error": last_error,
                    "updated_at": utcnow().isoformat(),
                }
            )

    if not args.dry_run:
        if timeseries_updates:
            writer.update_timeseries_last_values(timeseries_updates)
        if checkpoint_rows:
            writer.upsert_checkpoints(checkpoint_rows)
        try:
            writer.update_connector_last_polled(connector_id)
        except Exception as exc:
            LOG.warning("Failed to update connectors.last_polled_at: %s", exc)
    else:
        LOG.info("Dry run: skipping observation, timeseries, and checkpoint writes.")

    LOG.info(
        "Ingest complete: stations=%s species=%s observations=%s checkpoints=%s",
        len(station_id_map),
        len(species_list),
        observation_total,
        len(checkpoint_rows),
    )
    _write_json(args.output_timeseries, timeseries_output or [])
    _write_json(args.output_observations, observations_output or [])
    _write_json(args.output_checkpoints, checkpoints_output or [])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
