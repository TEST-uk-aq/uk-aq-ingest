#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.uk_aq_supabase import SchemaClient, create_supabase_client
from run_service import validated_cli_args

CONNECTOR_CODE = "blondon_nodes"
CORE_SCHEMA = os.getenv("UK_AQ_CORE_SCHEMA") or "uk_aq_core"
PUBLIC_SCHEMA = os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public"
DEFAULT_INTERVAL_MINUTES = 15
IN_FLIGHT_TIMEOUT_MINUTES = int(
    os.getenv("BLONDON_NODES_IN_FLIGHT_TIMEOUT_MINUTES") or "14"
)
CLAIM_TIMEOUT_MINUTES = int(
    os.getenv("BLONDON_NODES_CLAIM_TIMEOUT_MINUTES") or "14"
)


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def response_rows(response: Any) -> list[Dict[str, Any]]:
    data = response.data if hasattr(response, "data") else response.get("data")
    return [dict(row) for row in (data or [])]


class RunTracker:
    def __init__(self) -> None:
        self.client = create_supabase_client()
        self.core = SchemaClient(self.client, CORE_SCHEMA)
        self.public = SchemaClient(self.client, PUBLIC_SCHEMA)

    def load_connector(self) -> Optional[Dict[str, Any]]:
        response = self.core.table("connectors").select(
            "id,connector_code,poll_enabled,poll_interval_minutes,"
            "scheduler_backend,last_polled_at,last_run_start,last_run_end,"
            "last_run_status,last_run_message"
        ).eq("connector_code", CONNECTOR_CODE).limit(1).execute()
        rows = response_rows(response)
        return rows[0] if rows else None

    def claim(self, run_started_at: str) -> Optional[Dict[str, Any]]:
        response = self.public.rpc(
            "uk_aq_rpc_dispatch_claim",
            {
                "p_connector_code": CONNECTOR_CODE,
                "p_run_started_at": run_started_at,
                "p_timeout_minutes": CLAIM_TIMEOUT_MINUTES,
            },
        ).execute()
        rows = response_rows(response)
        return rows[0] if rows else None

    def complete_connector(
        self,
        connector_id: int,
        run_started_at: str,
        run_ended_at: str,
        run_status: str,
        run_message: str,
    ) -> None:
        patch = {
            "last_run_start": run_started_at,
            "last_run_end": run_ended_at,
            "last_run_status": run_status,
            "last_run_message": run_message[:1000],
        }
        if run_status in {"success", "succeeded"}:
            patch["last_polled_at"] = run_started_at
        self.core.table("connectors").update(patch).eq(
            "id", connector_id
        ).execute()

    def insert_run(
        self,
        connector_id: int,
        run_started_at: str,
        run_ended_at: str,
        run_status: str,
        run_message: str,
        response_status: int,
        summary: Dict[str, Any],
    ) -> None:
        self.core.table("uk_aq_ingest_runs").insert(
            {
                "connector_id": connector_id,
                "connector_code": CONNECTOR_CODE,
                "run_started_at": run_started_at,
                "run_ended_at": run_ended_at,
                "run_status": run_status,
                "run_message": run_message[:1000],
                "last_observed_at": summary.get("last_observed_at"),
                "stations_updated": summary.get("stations_updated"),
                "observations_upserted": summary.get("observations_upserted"),
                "timeseries_updated": summary.get("timeseries_updated"),
                "series_polled": summary.get("series_polled"),
                "response_status": response_status,
                "response_payload": summary,
            }
        ).execute()


def evaluate_due(
    connector: Optional[Dict[str, Any]],
    now: datetime,
    trigger_mode: str,
) -> tuple[bool, str]:
    if connector is None:
        return False, "connector_not_found"
    if connector.get("poll_enabled") is not True:
        return False, "poll_disabled"
    if (connector.get("scheduler_backend") or "supabase_function") != "google_cloud_run":
        return False, "scheduler_backend_not_cloud_run"
    if trigger_mode == "manual":
        return True, "manual"
    interval = int(
        connector.get("poll_interval_minutes") or DEFAULT_INTERVAL_MINUTES
    )
    started = parse_timestamp(connector.get("last_run_start"))
    ended = parse_timestamp(connector.get("last_run_end"))
    if started and not ended:
        guard_minutes = max(interval, IN_FLIGHT_TIMEOUT_MINUTES)
        if 0 <= (now - started).total_seconds() < guard_minutes * 60:
            return False, "in_flight"
    anchor = started or parse_timestamp(connector.get("last_polled_at"))
    if anchor and (now - anchor).total_seconds() < interval * 60:
        return False, "not_due"
    return True, "due" if anchor else "first_run"


def parse_ingest_summary(stdout: str) -> Optional[Dict[str, Any]]:
    for line in reversed(stdout.splitlines()):
        if line.startswith("RUN_SUMMARY_JSON "):
            try:
                value = json.loads(line[len("RUN_SUMMARY_JSON "):])
            except json.JSONDecodeError:
                return None
            return value if isinstance(value, dict) else None
    return None


def emit_job_summary(summary: Dict[str, Any]) -> None:
    print(
        "JOB_SUMMARY_JSON "
        + json.dumps(summary, separators=(",", ":"), sort_keys=True),
        flush=True,
    )


def record_timeout(timeout_seconds: int) -> Dict[str, Any]:
    tracker = RunTracker()
    connector = tracker.load_connector()
    if connector is None:
        return {"ok": False, "error": "connector_not_found"}
    connector_id = int(connector["id"])
    run_started_at = str(connector.get("last_run_start") or utcnow_iso())
    if connector.get("last_run_end"):
        return {"ok": False, "error": "no_in_flight_run"}
    run_ended_at = utcnow_iso()
    revision = (os.getenv("K_REVISION") or "unknown").strip()
    message = (
        f"cloud_run child_timeout after {timeout_seconds}s "
        f"on revision {revision}"
    )
    summary = {
        "ok": False,
        "connector_id": connector_id,
        "connector_code": CONNECTOR_CODE,
        "run_status": "failed",
        "run_message": message,
        "timed_out": True,
        "timeout_seconds": timeout_seconds,
    }
    tracker.complete_connector(
        connector_id, run_started_at, run_ended_at, "failed", message
    )
    tracker.insert_run(
        connector_id,
        run_started_at,
        run_ended_at,
        "failed",
        message,
        504,
        summary,
    )
    return summary


def run(payload: Dict[str, Any]) -> int:
    trigger_mode = str(payload.pop("trigger_mode", "scheduled")).strip().lower()
    tracker = RunTracker()
    now = datetime.now(timezone.utc)
    connector = tracker.load_connector()
    due, reason = evaluate_due(connector, now, trigger_mode)
    if not due:
        emit_job_summary(
            {
                "ok": True,
                "connector_code": CONNECTOR_CODE,
                "run_status": "skipped",
                "run_message": reason,
                "trigger_mode": trigger_mode,
            }
        )
        return 0

    run_started_at = now.isoformat()
    claim = tracker.claim(run_started_at)
    if not claim or claim.get("claimed") is not True:
        emit_job_summary(
            {
                "ok": True,
                "connector_code": CONNECTOR_CODE,
                "run_status": "skipped",
                "run_message": "claim_not_acquired",
                "trigger_mode": trigger_mode,
                "claim": claim,
            }
        )
        return 0

    connector_id = int(claim.get("connector_id") or connector["id"])
    args = validated_cli_args(payload)
    try:
        process = subprocess.run(args, text=True, capture_output=True)
    except Exception as exc:
        run_ended_at = utcnow_iso()
        run_status = "failed"
        run_message = f"blondon_nodes_ingest launch failed: {exc}"
        summary = {
            "ok": False,
            "connector_id": connector_id,
            "connector_code": CONNECTOR_CODE,
            "run_status": run_status,
            "run_message": run_message,
        }
        tracker.complete_connector(
            connector_id,
            run_started_at,
            run_ended_at,
            run_status,
            run_message,
        )
        tracker.insert_run(
            connector_id,
            run_started_at,
            run_ended_at,
            run_status,
            run_message,
            500,
            summary,
        )
        emit_job_summary(summary)
        return 1
    if process.stdout:
        print(process.stdout, end="")
    if process.stderr:
        print(process.stderr, end="", file=sys.stderr)
    summary = parse_ingest_summary(process.stdout) or {}
    run_ended_at = utcnow_iso()
    summary_status = str(summary.get("run_status") or "").lower()
    if (
        process.returncode == 0
        and summary
        and summary.get("ok") is not False
        and summary_status not in {"failed", "error"}
    ):
        run_status = str(summary.get("run_status") or "succeeded")
        run_message = str(summary.get("run_message") or "ok")
        response_status = 200
    else:
        run_status = "failed"
        run_message = (
            str(summary.get("run_message") or "").strip()
            or f"blondon_nodes_ingest exited {process.returncode}"
        )
        response_status = 500
        summary.update(
            {
                "ok": False,
                "connector_id": connector_id,
                "connector_code": CONNECTOR_CODE,
                "run_status": run_status,
                "run_message": run_message,
                "returncode": process.returncode,
                "stderr": process.stderr[-4000:],
            }
        )

    tracker.complete_connector(
        connector_id,
        run_started_at,
        run_ended_at,
        run_status,
        run_message,
    )
    tracker.insert_run(
        connector_id,
        run_started_at,
        run_ended_at,
        run_status,
        run_message,
        response_status,
        summary,
    )
    emit_job_summary(summary)
    return 0 if response_status < 400 else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--record-timeout", type=int)
    args = parser.parse_args()
    if args.record_timeout is not None:
        emit_job_summary(record_timeout(max(1, args.record_timeout)))
        return 0
    raw = sys.stdin.read() or "{}"
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("job payload must be a JSON object")
    return run(dict(payload))


if __name__ == "__main__":
    raise SystemExit(main())
