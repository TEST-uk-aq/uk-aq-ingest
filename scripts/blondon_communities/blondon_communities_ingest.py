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
import math
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
    SupabaseWriter as ReferenceWriter,
    load_api_key,
    normalize_station_payload,
)
from scripts.uk_aq_retention import load_retention, eligible_observation, bounded_start
from scripts.uk_aq_ingestdb_observation_writer import write_observations, build_compact_observation_rpc_args
from scripts.uk_aq_phenomena_rpc import upsert_phenomena_via_rpc
from scripts.uk_aq_retention import eligible_timestamp, retention_days, retention_start

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
DEFAULT_WINDOW_HOURS = 6
DEFAULT_SLEEP_SECONDS = 0.2
DEFAULT_BATCH_SIZE = 500

SPECIES_CONFIG = {
    "IPM25": {
        "label": "PM2.5",
        "uom": "ug/m3",
        "source_label": "breathelondon:pm2.5",
        "notation": "PM2.5",
        "pollutant_label": "pm2.5",
        "observed_property_code": "pm25",
        "mapping_kind": "raw_observed_property",
        "is_aqi_eligible": True,
    },
    "INO2": {
        "label": "NO2",
        "uom": "ug/m3",
        "source_label": "breathelondon:no2",
        "notation": "NO2",
        "pollutant_label": "no2",
        "observed_property_code": "no2",
        "mapping_kind": "raw_observed_property",
        "is_aqi_eligible": True,
    },
}


class SupabaseWriter(ReferenceWriter):
    def upsert_observations(self, rows):
        def commit(batch):
            if any(not eligible_observation(row["observed_at"]) for row in batch):
                raise RuntimeError("Observation batch crossed retention boundary; retry eligible rows")
            self.public.rpc("uk_aq_rpc_observations_compact_upsert_v1",
                            build_compact_observation_rpc_args(batch)).execute()
        return write_observations(rows, chunk_size=max(1, len(rows)),
            connector_code="blondon_communities", write_chunk=commit,
            config={"minimum_attempt_runtime_ms": 120_000, "shutdown_buffer_ms": 30_000},
            should_stop=lambda: time.monotonic() >= self.deadline - 45,
            remaining_runtime_ms=lambda: max(0, (self.deadline - time.monotonic()) * 1000))["committed_rows"]

    def update_timeseries_last_values(self, rows):
        rows = [r for r in rows if eligible_observation(r["last_value_at"])]
        if not rows:
            return 0
        self.public.rpc("uk_aq_rpc_timeseries_last_values_compact_update_v1", {
            "timeseries_ids": [r["id"] for r in rows],
            "last_values": [r["last_value"] for r in rows],
            "last_value_ats": [r["last_value_at"] for r in rows],
        }).execute()
        return len(rows)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _floor_to_hour(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except ValueError:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _parse_species_list(value: str) -> List[str]:
    items = [item.strip().upper() for item in value.split(",") if item.strip()]
    return [item for item in items if item in SPECIES_CONFIG]


def _parse_start_date(value: Optional[str]) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        if "T" not in text and " " not in text:
            parsed = datetime.fromisoformat(text)
            return parsed.replace(tzinfo=timezone.utc)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _build_timeseries_ref(station_ref: str, species: str) -> str:
    return f"{station_ref}:{species}"


def _build_phenomena_rows(
    connector_id: int, species_list: Iterable[str]
) -> List[Dict[str, Any]]:
    rows = []
    for species in species_list:
        config = SPECIES_CONFIG[species]
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


def _response_rows(response: Any) -> List[Dict[str, Any]]:
    data = response.data if hasattr(response, "data") else response.get("data")
    return [dict(row) for row in (data or [])]


def _validate_canonical_mappings(
    input_rows: Iterable[Dict[str, Any]],
    diagnostics: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    phenomenon_ids: Dict[str, int] = {}
    observed_property_ids: Dict[str, int] = {}
    for input_row in input_rows:
        source_label = str(input_row["source_label"])
        diagnostic = diagnostics.get(source_label)
        if not diagnostic or diagnostic.get("phenomenon_id") is None:
            raise RuntimeError(
                f"Canonical mapping missing phenomenon_id for {source_label}"
            )
        if diagnostic.get("observed_property_id") is None:
            raise RuntimeError(
                "Canonical mapping missing observed_property_id for "
                f"{source_label}"
            )
        if diagnostic.get("mapping_warning"):
            raise RuntimeError(
                f"Canonical mapping warning for {source_label}: "
                f"{diagnostic['mapping_warning']}"
            )
        if (
            diagnostic.get("observed_property_code")
            != input_row["observed_property_code"]
        ):
            raise RuntimeError(
                f"Canonical observed-property mismatch for {source_label}"
            )
        if diagnostic.get("mapping_kind") != input_row["mapping_kind"]:
            raise RuntimeError(f"Canonical mapping-kind mismatch for {source_label}")
        if (
            diagnostic.get("is_aqi_eligible")
            is not input_row["is_aqi_eligible"]
        ):
            raise RuntimeError(
                f"Canonical AQI-eligibility mismatch for {source_label}"
            )
        phenomenon_ids[source_label] = int(diagnostic["phenomenon_id"])
        observed_property_ids[source_label] = int(
            diagnostic["observed_property_id"]
        )
    return phenomenon_ids, observed_property_ids


def _read_canonical_mappings(
    writer: SupabaseWriter,
    connector_id: int,
    phenomena_rows: Iterable[Dict[str, Any]],
) -> Tuple[Dict[str, int], Dict[str, int]]:
    input_rows = list(phenomena_rows)
    source_labels = [str(row["source_label"]) for row in input_rows]
    phenomena_response = (
        writer.core.table("phenomena")
        .select("id,source_label,observed_property_id")
        .eq("connector_id", connector_id)
        .in_("source_label", source_labels)
        .execute()
    )
    mappings_response = (
        writer.core.table("observed_property_mappings")
        .select(
            "source_label,observed_property_id,observed_property_code,"
            "mapping_kind,is_aqi_eligible,is_active"
        )
        .eq("connector_id", connector_id)
        .in_("source_label", source_labels)
        .execute()
    )
    phenomena = {
        str(row["source_label"]): dict(row)
        for row in _response_rows(phenomena_response)
    }
    mappings = {
        str(row["source_label"]): dict(row)
        for row in _response_rows(mappings_response)
    }
    diagnostics: Dict[str, Dict[str, Any]] = {}
    for source_label in source_labels:
        phenomenon = phenomena.get(source_label, {})
        mapping = mappings.get(source_label, {})
        mapping_warning = None
        if not mapping:
            mapping_warning = "missing_mapping"
        elif mapping.get("is_active") is not True:
            mapping_warning = "inactive_mapping"
        elif phenomenon.get("observed_property_id") != mapping.get(
            "observed_property_id"
        ):
            mapping_warning = "phenomenon_mapping_mismatch"
        diagnostics[source_label] = {
            "phenomenon_id": phenomenon.get("id"),
            "observed_property_id": phenomenon.get("observed_property_id"),
            "observed_property_code": mapping.get("observed_property_code"),
            "mapping_kind": mapping.get("mapping_kind"),
            "is_aqi_eligible": mapping.get("is_aqi_eligible"),
            "mapping_warning": mapping_warning,
        }
    return _validate_canonical_mappings(input_rows, diagnostics)


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
    if len(payload) > 10000:
        raise RuntimeError("Communities response work budget exceeded")
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        observed_at = _parse_iso_datetime(entry.get("DateTime"))
        value = _coerce_float(entry.get("ScaledValue"))
        if observed_at is None or not eligible_observation(observed_at) or value is None:
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
        help="Bootstrap hint; ordinary acquisition is bounded to the most recent 48 hours.",
    )
    parser.add_argument(
        "--start-date",
        help="Manual retained start; one slice of at most 24 hours per species.",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=DEFAULT_WINDOW_HOURS,
        help="Recent window hours (bounded to 48 hours; manual slices to 24 hours).",
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
    if args.window_hours <= 0 or args.batch_size <= 0 or args.initial_days <= 0:
        raise ValueError("Window, batch size and initial days must be positive")
    if args.limit is not None and args.limit <= 0:
        raise ValueError("Limit must be a positive integer")
    load_retention("blondon_communities")
    max_runtime_seconds = min(600, max(30, int(os.getenv("BLONDON_COMMUNITIES_MAX_RUNTIME_SECONDS", "600"))))
    deadline = time.monotonic() + max_runtime_seconds
    acquisition_deadline = deadline - 90
    runtime_budget_hit = False
    failures = 0
    species_list = _parse_species_list(args.species)
    if not species_list:
        raise SystemExit("No valid species specified.")

    api_key = load_api_key(args.api_key)
    client = BreatheLondonClient(api_key, timeout=15, raise_on_retry_exhaustion=True)
    writer = SupabaseWriter()
    writer.deadline = deadline
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

    phenomena_rows = _build_phenomena_rows(connector_id, species_list)
    if args.dry_run:
        phenomenon_ids, observed_property_ids = _read_canonical_mappings(
            writer,
            connector_id,
            phenomena_rows,
        )
    else:
        diagnostics = upsert_phenomena_via_rpc(writer.public, phenomena_rows)
        phenomenon_ids, observed_property_ids = _validate_canonical_mappings(
            phenomena_rows,
            diagnostics,
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
                    "observed_property_id": observed_property_ids.get(config["source_label"]),
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
    earliest = retention_start(utcnow(), days)
    initial_start = _parse_start_date(args.start_date)
    if args.start_date and initial_start is None:
        raise ValueError("Invalid --start-date")
    observation_total = 0
    observations_output = [] if args.output_observations else None
    checkpoint_rows = []
    for row in station_rows:
        if runtime_budget_hit:
            break
        station_ref = str(row["station_ref"])
        station_id = station_id_map.get(station_ref)
        if station_id is None:
            continue
        for species in species_list:
            timeseries_ref = _build_timeseries_ref(station_ref, species)
            timeseries_id = timeseries_id_map.get(timeseries_ref)
            if timeseries_id is None:
                continue
            checkpoint = {} if args.ignore_checkpoints else checkpoints.get((station_id, species), {})
            prior = _parse_iso_datetime(checkpoint.get("last_observed_at"))
            polled = _parse_iso_datetime(checkpoint.get("last_polled_at"))
            stale = bool(checkpoint.get("last_error")) or prior is None or prior < now - timedelta(hours=6)
            interrupted = (now - polled).total_seconds() / 3600 + 1 if polled else 24
            hours = min(48, max(24 if stale else 6, min(args.window_hours, 24), interrupted))
            start_time = bounded_start(initial_start or now - timedelta(hours=hours))
            end_time = min(now, start_time + timedelta(hours=24)) if initial_start else now
            if start_time >= end_time:
                continue
            if time.monotonic() >= acquisition_deadline:
                LOG.warning("Stopping incomplete Communities work at the runtime budget")
                runtime_budget_hit = True
                failures += 1
                break
            progress = {"station_id": station_id, "species": species, "timeseries_id": timeseries_id,
                        "last_polled_at": utcnow().isoformat(), "updated_at": utcnow().isoformat(), "last_error": "attempt_incomplete"}
            if not args.dry_run:
                writer.upsert_checkpoints([progress])
            last_error = None
            latest = None
            try:
                start_time = bounded_start(start_time)
                if start_time >= end_time:
                    raise RuntimeError("Requested recovery slice left retention before acquisition")
                payload = client.get_clarity_data(station_ref, species, start_time, end_time)
                if isinstance(payload, list) and len(payload) > 10000:
                    raise RuntimeError("Communities response work budget exceeded")
                obs_rows, _, _ = _extract_observations(payload, timeseries_id)
                if len(obs_rows) > 10000:
                    raise RuntimeError("Communities response work budget exceeded")
                obs_rows = [r for r in obs_rows if eligible_observation(r["observed_at"])]
                if observations_output is not None:
                    observations_output.extend(obs_rows)
                for batch in _chunked(obs_rows, min(500, max(1, args.batch_size))):
                    if not args.dry_run:
                        observation_total += writer.upsert_observations(batch)
                    candidate = max(batch, key=lambda r: r["observed_at"])
                    if latest is None or candidate["observed_at"] >= latest["observed_at"]:
                        latest = candidate
                    if not args.dry_run:
                        writer.upsert_checkpoints([{**progress, **({"last_observed_at": latest["observed_at"]} if eligible_observation(latest["observed_at"]) else {})}])
                if latest and not args.dry_run:
                    writer.update_timeseries_last_values([{"id": timeseries_id, "last_value": latest["value"],
                                                          "last_value_at": latest["observed_at"]}])
            except Exception as exc:
                last_error = str(exc)
                failures += 1
                LOG.exception("Species ingest failed: %s %s", station_ref, species)
            progress["last_error"] = last_error
            if latest and eligible_observation(latest["observed_at"]):
                progress["last_observed_at"] = latest["observed_at"]
            checkpoint_rows.append(progress)
            if not args.dry_run:
                writer.upsert_checkpoints([progress])
            if args.sleep_seconds and time.monotonic() < acquisition_deadline:
                time.sleep(min(1, max(0, args.sleep_seconds)))

    if not args.dry_run:
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
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
