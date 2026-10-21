"""Bounded, source-native recent-hour recovery, independent of polling state."""

from __future__ import annotations

import json
import math
import os
import time
import uuid
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx
from postgrest import SyncPostgrestClient

from scripts.blondon_nodes.blondon_nodes_database import BlondonNodesDatabase
from scripts.uk_aq_service_egress_metrics import configured_service_egress_metrics

CONFIG_ENV = "BLONDON_NODES_RECOVERY_CONFIG"
# The approved Prune minimum is two UTC days. The existing recovery SQL helper
# subtracts (p_retention_days - 1), so pass 3 to retain the full 48-hour window.
MIN_PRUNE_RETENTION_DAYS = 2
RECOVERY_SQL_RETENTION_ARGUMENT_DAYS = MIN_PRUNE_RETENTION_DAYS + 1


def recovery_config() -> dict[str, Any] | None:
    text = (os.getenv(CONFIG_ENV) or "").strip()
    if not text:
        return None
    config = json.loads(text)
    if not isinstance(config, dict) or type(config.get("enabled")) is not bool:
        raise ValueError("Recovery config requires a boolean enabled field")
    if not config["enabled"]:
        return None
    if "not_before_utc" in config:
        raise ValueError(
            "Recovery not_before_utc is internal; remove it from "
            "BLONDON_NODES_RECOVERY_CONFIG"
        )
    limits = {
        "horizon_hours": (1, 48), "lag_hours": (2, 24),
        "scan_limit": (1, 16), "request_limit": (1, 16),
        "request_span_hours": (1, 6),
        "runtime_seconds": (75, 90), "cooldown_minutes": (60, 1440),
        "max_attempts_per_day": (1, 24),
    }
    for key, (low, high) in limits.items():
        if type(config.get(key)) is not int or not low <= config[key] <= high:
            raise ValueError(f"Recovery {key} must be an integer in {low}..{high}")
    if config["lag_hours"] >= config["horizon_hours"]:
        raise ValueError("Recovery lag must be smaller than the horizon")
    if os.getenv("BLONDON_NODES_INGESTDB_WRITE_TRANSPORT") != "database":
        raise ValueError("Recovery requires the configured pooled database transport")
    return config


def effective_sql_config(
    config: dict[str, Any], invocation_started_at: datetime,
) -> dict[str, Any]:
    """Supply the existing SQL helper's compatibility field, not R2 authority."""
    midnight = invocation_started_at.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    floor = midnight - timedelta(days=MIN_PRUNE_RETENTION_DAYS)
    return {
        **config,
        "not_before_utc": floor.isoformat().replace("+00:00", "Z"),
    }


def bounded_public(stack: ExitStack, url: str, key: str, schema: str) -> Any:
    if not url or not key:
        raise ValueError("Recovery database API configuration is missing")
    collector = configured_service_egress_metrics()
    client = (
        collector.create_httpx_client(url) if collector is not None and collector.enabled
        else httpx.Client()
    )
    client.timeout = httpx.Timeout(10)
    stack.enter_context(client)
    return SyncPostgrestClient(
        f"{url.rstrip('/')}/rest/v1", schema=schema,
        headers={"apiKey": key, "Authorization": f"Bearer {key}"}, http_client=client,
    )


def source_instant(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        # Recovery requires an unambiguous source instant. Normal parsing is
        # unchanged; offset-free source timestamps remain unconfirmed here.
        return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None
    except (TypeError, ValueError):
        return None


def run_recent_recovery(
    *, writer: Any, api_key: str, raw_capture: Any, client_factory: Callable,
    build_rows: Callable, observs_factory: Callable, run_started: float,
    skip_reason: str | None = None,
) -> dict[str, Any]:
    invocation_started_at = datetime.now(timezone.utc)
    report: dict[str, Any] = {
        "status": "disabled", "examined_timeseries": 0,
        "candidate_missing_hours": 0, "source_requests": 0,
        "source_null_hours": 0, "source_absent_hours": 0,
        "source_failed_requests": 0, "source_timestamp_unconfirmed": 0,
        "authentic_recoveries": 0, "existing_values_preserved": 0,
        "ownership_deferred": 0, "unresolved_gaps": 0,
        "retry_deferred_timeseries": 0, "remaining_due_timeseries": 0,
        "secondary_error_count": 0, "secondary_rows_delivered": 0,
        "secondary_rows_deferred": 0,
        "source_duplicate_rows_deduplicated": 0,
        "source_conflicting_duplicate_timestamps": 0,
        "write_outcome_unknown": False, "limits_reached": [],
    }
    phase = "configuration"
    try:
        config = recovery_config()
        if config is None:
            return report
        if skip_reason:
            report.update(status="skipped", reason=skip_reason)
            return report
        sql_config = effective_sql_config(config, invocation_started_at)
        remaining = run_started + int(os.getenv("BLONDON_NODES_MAX_RUNTIME_SECONDS") or "780") - 45
        deadline = min(time.monotonic() + config["runtime_seconds"], remaining)
        if deadline - time.monotonic() < 75:
            report.update(status="deferred", reason="normal_run_budget")
            report["limits_reached"].append("runtime")
            return report
        token = str(uuid.uuid4())
        with ExitStack() as stack:
            public = bounded_public(
                stack, os.getenv("SUPABASE_URL") or "",
                os.getenv("SB_SECRET_KEY") or os.getenv("SUPABASE_KEY") or "",
                os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public",
            )
            database = writer.observation_database or BlondonNodesDatabase.from_environment()
            stack.callback(database.close)
            phase = "scan"
            scan = public.rpc("uk_aq_rpc_blondon_nodes_recovery_scan_v1", {
                "p_config": sql_config,
                "p_retention_days": RECOVERY_SQL_RETENTION_ARGUMENT_DAYS,
                "p_claim_token": token,
            }).execute().data
            if not isinstance(scan, dict):
                raise ValueError("Unexpected recovery scan result")
            tasks = scan.pop("tasks", [])
            report.update(scan)
            report["unresolved_gaps"] = report["candidate_missing_hours"]
            if scan["status"] == "deferred":
                return report
            if report["remaining_due_timeseries"]:
                report["limits_reached"].append("scan_limit")
            if not tasks:
                report["status"] = "partial" if report["remaining_due_timeseries"] else "complete_selected_scan"
                return report
            raw_capture.record_context({
                "connector_code": "blondon_nodes", "recovery": True,
                "window_start": scan["window_start"], "window_end": scan["window_end"],
            })
            source = client_factory(
                api_key, raw_capture=raw_capture, retries=1, timeout=(3, 7),
                strict_single_attempt=True,
            )
            stack.callback(source.session.close)
            prepared: list[dict[str, Any]] = []
            phase = "source"
            # Each series receives at most one contiguous missing-hour request
            # per scan. The durable attempt index rotates among its groups.
            for task in tasks:
                if report["source_requests"] >= config["request_limit"]:
                    report["limits_reached"].append("request_limit")
                    break
                # Reserve pooled connect/query + existing secondary delivery.
                if deadline - time.monotonic() < 70:
                    report["limits_reached"].append("runtime")
                    break
                missing = task["missing_hours"]
                groups: list[list[datetime]] = []
                for text in missing:
                    at = source_instant(text)
                    if at is None:
                        raise ValueError("Invalid claimed recovery timestamp")
                    if (not groups or len(groups[-1]) >= config["request_span_hours"]
                            or at != groups[-1][-1] + timedelta(hours=1)):
                        groups.append([])
                    groups[-1].append(at)
                group = groups[(task["attempt_index"] - 1) % len(groups)]
                wanted = set(group)
                report["source_requests"] += 1
                try:
                    payload = source.sensor_data(
                        task["station_ref"], task["species"], group[0], group[-1] + timedelta(hours=1)
                    )
                except Exception:
                    report["source_failed_requests"] += 1
                    continue
                eligible = []
                seen: set[datetime] = set()
                source_null: set[datetime] = set()
                for entry in payload:
                    at = source_instant(entry.get("DateTime"))
                    if at is None:
                        report["source_timestamp_unconfirmed"] += 1
                        continue
                    if at not in wanted:
                        continue
                    seen.add(at)
                    if entry.get("ScaledValue") is None:
                        source_null.add(at)
                        continue
                    if isinstance(entry.get("ScaledValue"), bool):
                        continue
                    try:
                        numeric = float(entry["ScaledValue"])
                    except (TypeError, ValueError, OverflowError):
                        continue
                    if math.isfinite(numeric):
                        eligible.append(entry)
                rows, _, _, _, duplicates, conflicts = build_rows(
                    eligible, task["timeseries_id"], task["connector_id"],
                    task["station_id"], task["station_ref"], task["species"],
                )
                report["source_duplicate_rows_deduplicated"] += duplicates
                report["source_conflicting_duplicate_timestamps"] += conflicts
                valid = {source_instant(row["observed_at"]) for row in rows}
                report["source_null_hours"] += len(source_null - valid)
                report["source_absent_hours"] += len(wanted - seen)
                prepared.extend(rows)
            if prepared:
                phase = "write"
                # One bounded call, no retry after an ambiguous commit. The
                # server returns only inserted/null-replaced rows for delivery.
                result = database.recover_observations(
                    prepared, sql_config, RECOVERY_SQL_RETENTION_ARGUMENT_DAYS, token
                )
                committed = result["rows"]
                report["authentic_recoveries"] = len(committed)
                report["existing_values_preserved"] = result["existing_values_preserved"]
                report["ownership_deferred"] = result["ownership_deferred"]
                report["unresolved_gaps"] -= len(committed) + result["existing_values_preserved"]
                if committed:
                    phase = "secondary"
                    try:
                        secondary = observs_factory(writer.client)
                        secondary.main_public = public
                        if secondary.mode == "direct":
                            secondary.direct = bounded_public(
                                stack, os.getenv("OBS_AQIDB_SUPABASE_URL") or "",
                                os.getenv("OBS_AQIDB_SECRET_KEY") or "",
                                os.getenv("OBS_AQIDB_RPC_SCHEMA") or "uk_aq_public",
                            )
                        # Preserve the established payload/transport. Never send
                        # a stale source row that lost an IngestDB conflict.
                        by_id = {row["timeseries_id"]: row["connector_id"] for row in prepared}
                        for row in committed:
                            row["connector_id"] = by_id[row["timeseries_id"]]
                        size = (
                            secondary.publisher.batch_size if secondary.mode == "pubsub_only"
                            else len(committed)
                        )
                        if size < 1:
                            raise ValueError("Invalid secondary batch size")
                        for index in range(0, len(committed), size):
                            allowance = 40 if secondary.mode == "pubsub_only" else 10
                            if deadline - time.monotonic() < allowance:
                                report["secondary_rows_deferred"] = len(committed) - index
                                report["limits_reached"].append("secondary_runtime")
                                break
                            stats = secondary.write(committed[index:index + size])
                            report["secondary_rows_delivered"] += sum(stats.values())
                    except Exception as exc:
                        report["secondary_error_count"] = 1
                        report["secondary_error_type"] = type(exc).__name__
            report["status"] = "partial" if (
                report["unresolved_gaps"] or report["remaining_due_timeseries"]
                or report["limits_reached"] or report["secondary_error_count"]
            ) else "complete_selected_scan"
            return report
    except Exception as exc:
        report.update(status="failed", phase=phase, error_type=type(exc).__name__)
        # Never include raw exceptions: transport errors can contain secret URLs.
        if phase == "configuration":
            report["reason"] = "invalid_or_missing_recovery_configuration"
            report["configuration_error"] = str(exc)[:200]
        code = str(getattr(exc, "sqlstate", None) or getattr(exc, "code", "") or "")
        if code.isalnum() and len(code) <= 10:
            report["error_code"] = code
        if phase == "write":
            # SQL input/reference/permission errors cannot have committed;
            # connection loss or an unclassified result remains uncertain.
            state = str(getattr(exc, "sqlstate", "") or "")
            report["write_outcome_unknown"] = not state.startswith(("22", "23", "42"))
        return report
