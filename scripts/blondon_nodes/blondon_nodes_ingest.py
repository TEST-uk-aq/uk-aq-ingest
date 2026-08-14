#!/usr/bin/env python3
"""Ingest Breathe London Nodes /SensorData observations.

Examples:
  python3 scripts/blondon_nodes/blondon_nodes_ingest.py --dry-run --max-stations 1 --max-api-calls 4
  python3 scripts/blondon_nodes/blondon_nodes_ingest.py --site-code BL0001 --species PM25 --start-time 2026-06-27T10:00:00Z --end-time 2026-06-28T10:00:00Z --dry-run
"""

import argparse
import base64
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client
from scripts.blondon_nodes.blondon_nodes_reference_data import (
    DEFAULT_SPECIES,
    SPECIES_CONFIG,
    build_nodes_timeseries_rows,
    upsert_nodes_phenomena,
)
from scripts.uk_aq_ingestdb_observation_writer import (
    DEFAULT_POSTGREST_ATTEMPT_RUNTIME_MS,
    IngestDbObservationWriteError,
    build_compact_observation_rpc_args,
    empty_stats,
    merge_stats,
    serialized_json_utf8_bytes,
    write_observations,
)
load_dotenv()

LOG = logging.getLogger("blondon_nodes_ingest")
DEFAULT_LOG_LEVEL = os.getenv("BLONDON_NODES_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))

CONNECTOR_CODE = "blondon_nodes"
SERVICE_REF = os.getenv("BLONDON_NODES_SERVICE_REF", "breathelondon")
BASE_URL = os.getenv("BLONDON_NODES_BASE_URL", "https://breathe-london-7x54d7qf.ew.gateway.dev").rstrip("/")
DEFAULT_BATCH_SIZE = 500
DEFAULT_OVERLAP_MINUTES = 10
DEFAULT_SLEEP_SECONDS = 0.1

def chunked(values: Sequence[Any], size: int) -> List[Sequence[Any]]:
    return [values[index:index + size] for index in range(0, len(values), size)]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def floor_to_minute(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0)


def coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_species(value: Optional[str]) -> List[str]:
    if not value:
        return list(DEFAULT_SPECIES)
    wanted = []
    allowed = {item.upper(): item for item in DEFAULT_SPECIES}
    for raw in value.split(","):
        key = raw.strip().upper()
        if key in allowed:
            wanted.append(allowed[key])
    return wanted


class BreatheLondonNodesClient:
    def __init__(self, api_key: str, base_url: str = BASE_URL, timeout: int = 60, retries: int = 3) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"X-API-KEY": api_key, "Content-Type": "application/json", "Accept": "application/json", "User-Agent": "uk-air-quality-networks"})

    def sensor_data(self, site_code: str, species: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        params = {"SiteCode": site_code, "Species": species, "startTime": iso_z(start_time), "endTime": iso_z(end_time)}
        url = f"{self.base_url}/SensorData"
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(30, 2 ** attempt)); continue
                resp.raise_for_status()
                payload = resp.json()
                if payload is None:
                    return []
                if not isinstance(payload, list):
                    raise RuntimeError(f"Unexpected /SensorData payload type: {type(payload).__name__}")
                return [row for row in payload if isinstance(row, dict)]
            except requests.RequestException as exc:
                LOG.warning("Nodes request failed (attempt %s/%s): %s", attempt, self.retries, exc)
                if attempt == self.retries:
                    raise
                time.sleep(min(30, 2 ** attempt))
        return []


class PubSubPublisher:
    def __init__(self) -> None:
        self.project_id = (os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or "").strip()
        self.observs_topic = (os.getenv("GCP_OBSERVS_PUBSUB_TOPIC") or "uk-aq-observs-observations").strip()
        self.batch_size = int(os.getenv("OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE") or "500")

    def _topic_path(self, topic: str) -> str:
        if topic.startswith("projects/"):
            return topic
        if not self.project_id:
            return ""
        return f"projects/{self.project_id}/topics/{topic}"

    def _token(self) -> str:
        resp = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token", headers={"Metadata-Flavor": "Google"}, timeout=10)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if not token:
            raise RuntimeError("Metadata token response missing access_token")
        return str(token)

    def publish(self, topic: str, rows: Sequence[Dict[str, Any]], attr_keys: Sequence[str]) -> int:
        path = self._topic_path(topic)
        if not rows or not path:
            return 0
        token = self._token()
        count = 0
        for rows_chunk in [rows[i:i+self.batch_size] for i in range(0, len(rows), self.batch_size)]:
            messages = []
            for row in rows_chunk:
                attrs = {key: str(row[key]) for key in attr_keys if row.get(key) is not None}
                messages.append({"data": base64.b64encode(json.dumps(row, separators=(",", ":")).encode()).decode(), "attributes": attrs})
            resp = requests.post(f"https://pubsub.googleapis.com/v1/{path}:publish", headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, json={"messages": messages}, timeout=30)
            if not resp.ok:
                raise RuntimeError(f"Pub/Sub publish failed for {path}: HTTP {resp.status_code} {resp.text[:500]}")
            payload = resp.json()
            count += len(payload.get("messageIds") or messages)
        return count

    def publish_observations(self, rows: Sequence[Dict[str, Any]]) -> int:
        return self.publish(self.observs_topic, rows, ("connector_id", "timeseries_id", "observed_at"))

class ObservsWriter:
    def __init__(self, main_client: Client) -> None:
        requested_mode = (os.getenv("OBSERVS_WRITE_MODE") or "").strip().lower()
        self.mode = requested_mode if requested_mode in {"direct", "outbox_only", "pubsub_only"} else "outbox_only"
        self.main_public = main_client.schema(os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public")
        self.publisher = PubSubPublisher() if self.mode == "pubsub_only" else None
        self.direct = None
        if self.mode == "direct":
            url = (os.getenv("OBS_AQIDB_SUPABASE_URL") or "").strip()
            key = (os.getenv("OBS_AQIDB_SECRET_KEY") or "").strip()
            if not url or not key:
                raise RuntimeError(
                    "OBSERVS_WRITE_MODE=direct requires OBS_AQIDB_SUPABASE_URL and OBS_AQIDB_SECRET_KEY"
                )
            self.direct = create_client(url, key).schema(
                os.getenv("OBS_AQIDB_RPC_SCHEMA") or "uk_aq_public"
            )

    @staticmethod
    def _rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {
                "connector_id": row["connector_id"],
                "timeseries_id": row["timeseries_id"],
                "observed_at": row["observed_at"],
                "value": row["value"],
                "status": row["status"],
            }
            for row in rows
        ]

    def write(self, rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
        payload = self._rows(rows)
        if not payload:
            return {"written": 0, "enqueued": 0, "pubsub_observs": 0}
        if self.mode == "pubsub_only":
            assert self.publisher is not None
            return {
                "written": 0,
                "enqueued": 0,
                "pubsub_observs": self.publisher.publish_observations(payload),
            }
        if self.mode == "direct":
            assert self.direct is not None
            self.direct.rpc(
                "uk_aq_rpc_observs_observations_compact_upsert_v1",
                {
                    "timeseries_ids": [row["timeseries_id"] for row in payload],
                    "observed_ats": [row["observed_at"] for row in payload],
                    "values": [row["value"] for row in payload],
                },
            ).execute()
            return {
                "written": len(payload),
                "enqueued": 0,
                "pubsub_observs": 0,
            }
        self.main_public.rpc(
            "uk_aq_rpc_observs_outbox_enqueue",
            {"entries": [{"payload": payload}]},
        ).execute()
        return {
            "written": 0,
            "enqueued": len(payload),
            "pubsub_observs": 0,
        }


class SupabaseWriter:
    def __init__(self) -> None:
        self.client: Client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core
        self.raw = schemas.raw
        self.public = self.client.schema(
            os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public"
        )
        self.observation_write_stats = empty_stats()

    def fetch_connector(self) -> Dict[str, Any]:
        resp = self.core.table("connectors").select("id,poll_enabled,poll_window_hours,poll_interval_minutes,poll_timeseries_batch_size").eq("connector_code", CONNECTOR_CODE).limit(1).execute()
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        if not rows:
            raise RuntimeError("Connector not found for blondon_nodes. Run the station import first.")
        return dict(rows[0])

    def fetch_active_stations(self, connector_id: int, site_code: Optional[str]) -> List[Dict[str, Any]]:
        query = self.core.table("stations").select("id,station_ref,station_name,label").eq("connector_id", connector_id).eq("service_ref", SERVICE_REF).filter("removed_at", "is", "null").order("station_ref")
        if site_code:
            query = query.eq("station_ref", site_code)
        resp = query.execute()
        return resp.data if hasattr(resp, "data") else resp.get("data") or []

    def fetch_station_checkpoints(self, station_ids: Sequence[int]) -> Dict[int, Dict[str, Any]]:
        out: Dict[int, Dict[str, Any]] = {}
        for ids in chunked([str(value) for value in station_ids], 200):
            resp = self.raw.table("blondon_nodes_station_checkpoints").select(
                "station_id,next_due_at,last_observed_at,ingest_lag_samples,"
                "last_polled_at,last_error,species_last_observed_at,species_last_error"
            ).in_("station_id", ids).execute()
            for row in resp.data or []:
                out[int(row["station_id"])] = dict(row)
        return out

    def upsert_phenomena(
        self, connector_id: int, species: Sequence[str]
    ) -> Tuple[Dict[str, int], Dict[str, int]]:
        return upsert_nodes_phenomena(self.public, connector_id, species)

    def upsert_timeseries(self, rows: Sequence[Dict[str, Any]]) -> Dict[str, int]:
        if not rows:
            return {}
        refs = [r["timeseries_ref"] for r in rows]
        out: Dict[str, int] = {}
        for refs_chunk in chunked(refs, 200):
            resp = self.core.table("timeseries").select("id,timeseries_ref").eq("connector_id", rows[0]["connector_id"]).in_("timeseries_ref", refs_chunk).execute()
            for r in resp.data or []:
                out[str(r["timeseries_ref"])] = int(r["id"])
        missing_rows = [row for row in rows if row["timeseries_ref"] not in out]
        if missing_rows:
            self.core.table("timeseries").upsert(
                missing_rows, on_conflict="connector_id,timeseries_ref"
            ).execute()
            missing_refs = [row["timeseries_ref"] for row in missing_rows]
            for refs_chunk in chunked(missing_refs, 200):
                resp = self.core.table("timeseries").select(
                    "id,timeseries_ref"
                ).eq("connector_id", rows[0]["connector_id"]).in_(
                    "timeseries_ref", refs_chunk
                ).execute()
                for result_row in resp.data or []:
                    out[str(result_row["timeseries_ref"])] = int(result_row["id"])
        unresolved = [ref for ref in refs if ref not in out]
        if unresolved:
            raise RuntimeError(
                f"Missing Nodes timeseries identities after self-repair: {unresolved[:10]}"
            )
        return out

    def upsert_observations(self, rows: Sequence[Dict[str, Any]]) -> int:
        payload = [
            {
                "connector_id": row["connector_id"],
                "timeseries_id": row["timeseries_id"],
                "observed_at": row["observed_at"],
                "value": row["value"],
                "status": row["status"],
            }
            for row in rows
        ]

        def write_chunk(chunk: Sequence[Dict[str, Any]]) -> None:
            self.public.rpc(
                "uk_aq_rpc_observations_compact_upsert_v1",
                build_compact_observation_rpc_args(chunk),
            ).execute()

        try:
            stats = write_observations(
                payload,
                chunk_size=max(1, len(payload)),
                connector_code=CONNECTOR_CODE,
                write_chunk=write_chunk,
                logger=LOG,
                config={
                    "minimum_attempt_runtime_ms": DEFAULT_POSTGREST_ATTEMPT_RUNTIME_MS,
                },
                request_body_bytes=lambda chunk: serialized_json_utf8_bytes(
                    build_compact_observation_rpc_args(chunk)
                ),
            )
        except IngestDbObservationWriteError as exc:
            merge_stats(self.observation_write_stats, exc.stats)
            raise
        merge_stats(self.observation_write_stats, stats)
        return int(stats["committed_rows"])

    def update_timeseries_value_bounds(self, rows: Sequence[Dict[str, Any]]) -> int:
        grouped: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(int(row["timeseries_id"]), []).append(dict(row))
        if not grouped:
            return 0

        existing: Dict[int, Dict[str, Any]] = {}
        for ids in chunked([str(value) for value in grouped], 200):
            resp = self.core.table("timeseries").select(
                "id,first_value_at,last_value_at,last_value"
            ).in_("id", ids).execute()
            for row in resp.data or []:
                existing[int(row["id"])] = dict(row)

        updated_ids: set[int] = set()
        latest_rows: List[Dict[str, Any]] = []
        for timeseries_id, timeseries_rows in grouped.items():
            ordered = sorted(
                timeseries_rows,
                key=lambda row: parse_iso(row.get("observed_at"))
                or datetime.min.replace(tzinfo=timezone.utc),
            )
            earliest = ordered[0]
            latest = ordered[-1]
            earliest_at = parse_iso(earliest.get("observed_at"))
            latest_at = parse_iso(latest.get("observed_at"))
            current = existing.get(timeseries_id, {})
            current_first = parse_iso(current.get("first_value_at"))
            current_last = parse_iso(current.get("last_value_at"))
            if earliest_at and (current_first is None or earliest_at < current_first):
                self.core.table("timeseries").update(
                    {"first_value_at": earliest_at.isoformat()}
                ).eq("id", timeseries_id).execute()
                updated_ids.add(timeseries_id)
            if latest_at and (current_last is None or latest_at > current_last):
                latest_rows.append({
                    "id": timeseries_id,
                    "last_value_at": latest_at.isoformat(),
                    "last_value": latest["value"],
                })
            elif (
                latest_at
                and current_last
                and latest_at == current_last
                and current.get("last_value") != latest["value"]
            ):
                latest_rows.append({
                    "id": timeseries_id,
                    "last_value_at": latest_at.isoformat(),
                    "last_value": latest["value"],
                })
        if latest_rows:
            latest_args = {
                "timeseries_ids": [row["id"] for row in latest_rows],
                "last_values": [row["last_value"] for row in latest_rows],
                "last_value_ats": [row["last_value_at"] for row in latest_rows],
            }
            LOG.info(
                "postgrest_request_metric %s",
                json.dumps(
                    {
                        "metric": "uk_aq_endpoint_egress",
                        "endpoint": "postgrest:rpc/uk_aq_rpc_timeseries_last_values_compact_update_v1",
                        "caller": "uk_aq_blondon_nodes_cloud_run",
                        "request_count": 1,
                        "request_body_bytes": serialized_json_utf8_bytes(latest_args),
                    },
                    separators=(",", ":"),
                ),
            )
            self.public.rpc(
                "uk_aq_rpc_timeseries_last_values_compact_update_v1",
                latest_args,
            ).execute()
            updated_ids.update(row["id"] for row in latest_rows)
        return len(updated_ids)

    def upsert_station_checkpoints(self, rows: Sequence[Dict[str, Any]]) -> None:
        if rows:
            self.raw.table("blondon_nodes_station_checkpoints").upsert(
                list(rows), on_conflict="station_id"
            ).execute()


def write_secondary_rows(
    observs_writer: Optional[ObservsWriter],
    rows_chunks: Sequence[Sequence[Dict[str, Any]]],
    station_ref: str,
    species: str,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {
        "written": 0,
        "enqueued": 0,
        "pubsub_observs": 0,
        "errors": [],
    }
    if observs_writer is None:
        return stats
    for rows_chunk in rows_chunks:
        try:
            chunk_stats = observs_writer.write(rows_chunk)
            stats["written"] += chunk_stats["written"]
            stats["enqueued"] += chunk_stats["enqueued"]
            stats["pubsub_observs"] += chunk_stats["pubsub_observs"]
        except Exception as exc:
            message = f"{station_ref}/{species}: {exc}"
            stats["errors"].append(message)
            LOG.warning(
                "Secondary Observs write failed for %s %s: %s",
                station_ref,
                species,
                exc,
            )
    return stats


def deduplicate_source_rows(
    rows: Sequence[Dict[str, Any]],
    station_ref: str,
    species: str,
) -> Tuple[List[Dict[str, Any]], int, int]:
    grouped: Dict[Tuple[int, str], List[Tuple[int, Dict[str, Any]]]] = {}
    for source_index, row in enumerate(rows):
        identity = (int(row["timeseries_id"]), str(row["observed_at"]))
        grouped.setdefault(identity, []).append((source_index, row))

    winners: List[Tuple[int, Dict[str, Any]]] = []
    duplicate_rows_deduplicated = 0
    conflicting_duplicate_timestamps = 0
    for (_timeseries_id, observed_at), candidates in grouped.items():
        winner_index, winner = candidates[-1]
        winners.append((winner_index, winner))
        duplicate_rows_deduplicated += len(candidates) - 1
        if len(candidates) < 2:
            continue

        first = candidates[0][1]
        value_differed = any(
            candidate[1]["value"] != first["value"] for candidate in candidates[1:]
        )
        status_differed = any(
            candidate[1]["status"] != first["status"] for candidate in candidates[1:]
        )
        if value_differed or status_differed:
            conflicting_duplicate_timestamps += 1
            LOG.warning(
                "Nodes source conflicting duplicate timestamp "
                "station_ref=%s species=%s observed_at=%s "
                "value_differed=%s status_differed=%s candidate_count=%s",
                station_ref[:100],
                species[:50],
                observed_at[:40],
                value_differed,
                status_differed,
                len(candidates),
            )

    winners.sort(key=lambda item: item[0])
    return (
        [winner for _source_index, winner in winners],
        duplicate_rows_deduplicated,
        conflicting_duplicate_timestamps,
    )


def build_rows(
    payload: Sequence[Dict[str, Any]],
    timeseries_id: int,
    connector_id: int,
    station_id: int,
    station_ref: str,
    species: str,
) -> Tuple[List[Dict[str, Any]], int, Optional[str], Optional[float], int, int]:
    candidate_rows = []
    nulls = 0
    for entry in payload:
        value = coerce_float(entry.get("ScaledValue"))
        if value is None:
            nulls += 1; continue
        observed = parse_iso(entry.get("DateTime"))
        if observed is None:
            continue
        meta = {k: entry.get(k) for k in ("Units", "RatificationStatus", "Source", "Duration", "SensorContract") if entry.get(k) is not None}
        status = entry.get("RatificationStatus")
        if status is not None:
            status = str(status).strip() or None
        candidate_rows.append({"connector_id": connector_id, "station_id": station_id, "timeseries_id": timeseries_id, "observed_at": observed.isoformat(), "value": value, "status": status, "metadata": meta, "species": species})

    rows, duplicate_rows_deduplicated, conflicting_duplicate_timestamps = (
        deduplicate_source_rows(candidate_rows, station_ref, species)
    )
    last_at: Optional[datetime] = None
    last_value: Optional[float] = None
    for row in rows:
        observed = parse_iso(row["observed_at"])
        if observed is None:
            continue
        if last_at is None or observed > last_at:
            last_at = observed
            last_value = row["value"]
    return (
        rows,
        nulls,
        (last_at.isoformat() if last_at else None),
        last_value,
        duplicate_rows_deduplicated,
        conflicting_duplicate_timestamps,
    )


def select_due_stations(
    stations: Sequence[Dict[str, Any]],
    checkpoints: Dict[int, Dict[str, Any]],
    now: datetime,
    bypass_due: bool,
    limit: Optional[int],
) -> List[Dict[str, Any]]:
    due = []
    for station in stations:
        checkpoint = checkpoints.get(int(station["id"]), {})
        next_due = parse_iso(checkpoint.get("next_due_at"))
        if bypass_due or next_due is None or next_due <= now:
            due.append(station)

    def sort_key(station: Dict[str, Any]) -> Tuple[Any, ...]:
        checkpoint = checkpoints.get(int(station["id"]), {})
        last_polled = parse_iso(checkpoint.get("last_polled_at"))
        next_due = parse_iso(checkpoint.get("next_due_at"))
        return (
            last_polled is not None,
            last_polled or datetime.min.replace(tzinfo=timezone.utc),
            next_due is not None,
            next_due or datetime.min.replace(tzinfo=timezone.utc),
            str(station.get("station_ref") or ""),
        )

    due.sort(key=sort_key)
    return due[:limit] if limit is not None else due


def json_object(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def lag_samples(value: Any) -> List[int]:
    if not isinstance(value, list):
        return []
    samples = []
    for item in value:
        if isinstance(item, bool):
            continue
        try:
            sample = int(item)
        except (TypeError, ValueError):
            continue
        if sample >= 0:
            samples.append(sample)
    return samples[-10:]


def emit_run_summary(summary: Dict[str, Any]) -> None:
    print(
        "RUN_SUMMARY_JSON "
        + json.dumps(summary, separators=(",", ":"), sort_keys=True),
        flush=True,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest Breathe London Nodes observations.")
    p.add_argument("--api-key", help="API key override; defaults to BLONDON_NODES_API_KEY.")
    p.add_argument("--start-time", help="Manual UTC start time; normal runs use checkpoints and connector poll_window_hours.")
    p.add_argument("--end-time", help="Manual UTC end time; default now UTC.")
    p.add_argument("--site-code", help="Limit to one SiteCode/station_ref.")
    p.add_argument("--species", help="Comma-separated species (default: PM25,NO2,PM25Index,NO2Index).")
    p.add_argument("--dry-run", action="store_true", help="Fetch/build rows without Supabase writes or Pub/Sub publishes.")
    p.add_argument("--max-stations", type=int, help="Safety limit for station count.")
    p.add_argument("--max-api-calls", type=int, help="Safety limit for /SensorData calls.")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    p.add_argument("--overlap-minutes", type=int, default=DEFAULT_OVERLAP_MINUTES)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    species = parse_species(args.species)
    if not species:
        raise SystemExit("No valid species selected.")
    api_key = (args.api_key or os.getenv("BLONDON_NODES_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("BLONDON_NODES_API_KEY is required; no sensible default exists for an API credential.")
    writer = SupabaseWriter()
    connector = writer.fetch_connector()
    connector_id = int(connector["id"])
    if connector.get("poll_enabled") is False and not (args.start_time or args.site_code):
        LOG.info("Connector blondon_nodes poll_enabled=false; exiting normal scheduled run.")
        emit_run_summary(
            {
                "ok": True,
                "connector_id": connector_id,
                "connector_code": CONNECTOR_CODE,
                "run_status": "skipped",
                "run_message": "poll_disabled",
                "last_observed_at": None,
                "stations_selected": 0,
                "stations_processed": 0,
                "stations_updated": 0,
                "series_polled": 0,
                "timeseries_updated": 0,
                "observations_upserted": 0,
                "cross_database_transaction": False,
                "observations_rows_input": 0,
                "observations_rows_prepared": 0,
                "observs_rows_prepared": 0,
                "observs_written": 0,
                "observs_enqueued": 0,
                "observs_receipts_upserted": 0,
                "pubsub_observs": 0,
                "secondary_errors": [],
                "secondary_error_count": 0,
                "secondary_error_message": None,
                "observs_error_count": 0,
                "observs_error_message": None,
                "null_values_skipped": 0,
                "source_duplicate_rows_deduplicated": 0,
                "source_conflicting_duplicate_timestamps": 0,
                "empty_series": 0,
                "checkpoints": 0,
                "partial": False,
                "stopped_reason": "poll_disabled",
                "dry_run": args.dry_run,
            }
        )
        return 0
    all_stations = writer.fetch_active_stations(connector_id, args.site_code)
    checkpoints = writer.fetch_station_checkpoints(
        [int(station["id"]) for station in all_stations]
    )
    selection_time = utcnow()
    stations = select_due_stations(
        all_stations,
        checkpoints,
        selection_time,
        bypass_due=bool(args.site_code or args.start_time),
        limit=args.max_stations,
    )
    LOG.info(
        "Selected %s of %s active blondon_nodes stations.",
        len(stations),
        len(all_stations),
    )
    if not stations:
        emit_run_summary(
            {
                "ok": True,
                "connector_id": connector_id,
                "connector_code": CONNECTOR_CODE,
                "run_status": "skipped",
                "run_message": "no_due_stations",
                "last_observed_at": None,
                "stations_selected": 0,
                "stations_processed": 0,
                "stations_updated": 0,
                "series_polled": 0,
                "timeseries_updated": 0,
                "observations_upserted": 0,
                "cross_database_transaction": False,
                "observations_rows_input": 0,
                "observations_rows_prepared": 0,
                "observs_rows_prepared": 0,
                "observs_written": 0,
                "observs_enqueued": 0,
                "observs_receipts_upserted": 0,
                "pubsub_observs": 0,
                "secondary_errors": [],
                "secondary_error_count": 0,
                "secondary_error_message": None,
                "observs_error_count": 0,
                "observs_error_message": None,
                "null_values_skipped": 0,
                "source_duplicate_rows_deduplicated": 0,
                "source_conflicting_duplicate_timestamps": 0,
                "empty_series": 0,
                "checkpoints": 0,
                "partial": False,
                "stopped_reason": "no_due_stations",
                "dry_run": args.dry_run,
            }
        )
        return 0
    if not args.dry_run:
        phenomenon_ids, observed_property_ids = writer.upsert_phenomena(
            connector_id, species
        )
    else:
        phenomenon_ids, observed_property_ids = {}, {}
    ts_rows = build_nodes_timeseries_rows(
        stations,
        connector_id=connector_id,
        phenomenon_ids=phenomenon_ids,
        observed_property_ids=observed_property_ids,
        service_ref=SERVICE_REF,
        species=species,
    )
    ts_ids = writer.upsert_timeseries(ts_rows) if not args.dry_run else {r["timeseries_ref"]: -i-1 for i, r in enumerate(ts_rows)}
    client = BreatheLondonNodesClient(api_key)
    secondary_errors: List[str] = []
    secondary_error_count = 0
    try:
        observs_writer: Optional[ObservsWriter] = ObservsWriter(writer.client)
    except Exception as exc:
        observs_writer = None
        secondary_error_count += 1
        secondary_errors.append(f"observs_writer_init: {exc}")
        LOG.warning("Secondary Observs writer initialization failed: %s", exc)
    end_time = floor_to_minute(parse_iso(args.end_time) or utcnow())
    poll_hours = float(connector.get("poll_window_hours") or 6)
    default_start = floor_to_minute(utcnow()) - timedelta(
        hours=max(poll_hours, 0.1)
    )
    explicit_start = parse_iso(args.start_time)
    api_calls = observations_input = observations_upserted = 0
    null_values_skipped = empty_series = pub_obs = 0
    source_duplicate_rows_deduplicated = 0
    source_conflicting_duplicate_timestamps = 0
    observs_rows_prepared = observs_written = observs_enqueued = 0
    stations_processed = 0
    stopped_reason: Optional[str] = None
    had_species_errors = False
    checkpoint_rows: List[Dict[str, Any]] = []
    written_observation_rows: List[Dict[str, Any]] = []
    for st in stations:
        if args.max_api_calls is not None and api_calls >= args.max_api_calls:
            LOG.warning("Stopping at --max-api-calls=%s", args.max_api_calls)
            stopped_reason = "max_api_calls"
            break
        station_ref = str(st["station_ref"]); station_id = int(st["id"])
        checkpoint = checkpoints.get(station_id, {})
        species_last_observed = json_object(checkpoint.get("species_last_observed_at"))
        species_last_error = json_object(checkpoint.get("species_last_error"))
        station_errors: Dict[str, str] = {}
        successful_last_observed: List[datetime] = []
        station_incomplete = False
        station_polled_at = utcnow()
        for sp in species:
            if args.max_api_calls is not None and api_calls >= args.max_api_calls:
                LOG.warning("Stopping at --max-api-calls=%s", args.max_api_calls)
                station_incomplete = True
                stopped_reason = "max_api_calls"
                break
            ts_ref = f"{station_ref}:{sp}"; ts_id = ts_ids.get(ts_ref)
            species_checkpoint = parse_iso(species_last_observed.get(sp))
            station_checkpoint = parse_iso(checkpoint.get("last_observed_at"))
            checkpoint_start = species_checkpoint or station_checkpoint
            start_time = explicit_start or max(
                (
                    checkpoint_start - timedelta(minutes=max(args.overlap_minutes, 0))
                    if checkpoint_start
                    else default_start
                ),
                default_start,
            )
            start_time = floor_to_minute(start_time)
            last_at = None
            last_value = None
            try:
                payload = client.sensor_data(station_ref, sp, start_time, end_time); api_calls += 1
                if not payload:
                    empty_series += 1
                (
                    rows,
                    nulls,
                    last_at,
                    last_value,
                    duplicate_rows_deduplicated,
                    conflicting_duplicate_timestamps,
                ) = build_rows(
                    payload,
                    int(ts_id),
                    connector_id,
                    station_id,
                    station_ref,
                    sp,
                )
                null_values_skipped += nulls
                source_duplicate_rows_deduplicated += duplicate_rows_deduplicated
                source_conflicting_duplicate_timestamps += conflicting_duplicate_timestamps
                observations_input += len(rows)
                if rows and not args.dry_run:
                    rows_chunks = [
                        rows[index:index + args.batch_size]
                        for index in range(0, len(rows), args.batch_size)
                    ]
                    for rows_chunk in rows_chunks:
                        writer.upsert_observations(rows_chunk)
                        observations_upserted += len(rows_chunk)
                        written_observation_rows.extend(rows_chunk)
                    if last_at and last_value is not None:
                        species_last_observed[sp] = last_at
                        parsed_last_at = parse_iso(last_at)
                        if parsed_last_at:
                            successful_last_observed.append(parsed_last_at)
                    observs_rows_prepared += len(rows)
                    observs_stats = write_secondary_rows(
                        observs_writer,
                        rows_chunks,
                        station_ref,
                        sp,
                    )
                    observs_written += observs_stats["written"]
                    observs_enqueued += observs_stats["enqueued"]
                    pub_obs += observs_stats["pubsub_observs"]
                    chunk_errors = observs_stats["errors"]
                    secondary_error_count += len(chunk_errors)
                    remaining_error_slots = max(0, 50 - len(secondary_errors))
                    secondary_errors.extend(
                        chunk_errors[:remaining_error_slots]
                    )
                species_last_error.pop(sp, None)
            except Exception as exc:
                error = str(exc)
                had_species_errors = True
                station_errors[sp] = error
                species_last_error[sp] = error
                LOG.warning("Failed %s %s: %s", station_ref, sp, exc)
                if isinstance(exc, IngestDbObservationWriteError):
                    raise
            if args.sleep_seconds:
                time.sleep(args.sleep_seconds)

        previous_last_observed = parse_iso(checkpoint.get("last_observed_at"))
        latest_success = max(successful_last_observed) if successful_last_observed else None
        station_last_observed = max(
            [value for value in (previous_last_observed, latest_success) if value is not None],
            default=None,
        )
        samples = lag_samples(checkpoint.get("ingest_lag_samples"))
        if latest_success is not None:
            samples.append(max(0, round((station_polled_at - latest_success).total_seconds())))
            samples = samples[-10:]

        if station_errors or station_incomplete:
            next_due_at = station_polled_at + timedelta(minutes=5)
        elif len(samples) < 10 or station_last_observed is None:
            next_due_at = station_polled_at + timedelta(minutes=5)
        else:
            next_due_at = station_last_observed + timedelta(
                seconds=3600 + min(samples)
            )

        checkpoint_rows.append(
            {
                "station_id": station_id,
                "next_due_at": next_due_at.isoformat(),
                "last_observed_at": (
                    station_last_observed.isoformat()
                    if station_last_observed is not None
                    else None
                ),
                "ingest_lag_samples": samples,
                "last_polled_at": station_polled_at.isoformat(),
                "last_error": (
                    "; ".join(f"{sp}: {error}" for sp, error in station_errors.items())
                    if station_errors
                    else None
                ),
                "species_last_observed_at": species_last_observed,
                "species_last_error": species_last_error,
                "updated_at": station_polled_at.isoformat(),
            }
        )
        stations_processed += 1
    timeseries_updated = 0
    if not args.dry_run:
        timeseries_updated = writer.update_timeseries_value_bounds(
            written_observation_rows
        )
        writer.upsert_station_checkpoints(checkpoint_rows)
    last_observed_at = None
    if written_observation_rows:
        last_observed_at = max(
            str(row["observed_at"]) for row in written_observation_rows
        )
    partial = had_species_errors or stopped_reason is not None
    if partial:
        run_status = "partial"
        run_message = stopped_reason or "species_errors"
    elif args.dry_run:
        run_status = "dry_run"
        run_message = "ok"
    elif secondary_error_count:
        run_status = "succeeded"
        run_message = "secondary_errors"
    else:
        run_status = "succeeded"
        run_message = "ok"
    secondary_error_message = (
        "; ".join(secondary_errors)[:4000] if secondary_errors else None
    )
    summary = {
        "ok": True,
        "connector_id": connector_id,
        "connector_code": CONNECTOR_CODE,
        "run_status": run_status,
        "run_message": run_message,
        "last_observed_at": last_observed_at,
        "stations_selected": len(stations),
        "stations_processed": stations_processed,
        "stations_updated": len(checkpoint_rows) if not args.dry_run else 0,
        "series_polled": api_calls,
        "timeseries_updated": timeseries_updated,
        "observations_upserted": writer.observation_write_stats["committed_rows"],
        "ingestdb_observation_write": writer.observation_write_stats,
        "cross_database_transaction": False,
        "observations_rows_input": observations_input,
        "observations_rows_prepared": observations_input,
        "observs_rows_prepared": observs_rows_prepared,
        "observs_written": observs_written,
        "observs_enqueued": observs_enqueued,
        "observs_receipts_upserted": 0,
        "pubsub_observs": pub_obs,
        "secondary_errors": secondary_errors,
        "secondary_error_count": secondary_error_count,
        "secondary_error_message": secondary_error_message,
        "observs_error_count": secondary_error_count,
        "observs_error_message": secondary_error_message,
        "null_values_skipped": null_values_skipped,
        "source_duplicate_rows_deduplicated": source_duplicate_rows_deduplicated,
        "source_conflicting_duplicate_timestamps": source_conflicting_duplicate_timestamps,
        "empty_series": empty_series,
        "checkpoints": len(checkpoint_rows) if not args.dry_run else 0,
        "partial": partial,
        "stopped_reason": stopped_reason,
        "dry_run": args.dry_run,
    }
    LOG.info(
        "Nodes ingest complete stations=%s species=%s api_calls=%s observations=%s "
        "null_values_skipped=%s empty_series=%s checkpoints=%s "
        "pubsub_observs=%s secondary_errors=%s dry_run=%s",
        len(stations), len(species), api_calls,
        writer.observation_write_stats["committed_rows"],
        null_values_skipped, empty_series, len(checkpoint_rows),
        pub_obs, secondary_error_count, args.dry_run,
    )
    emit_run_summary(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
