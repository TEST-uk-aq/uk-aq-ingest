import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import httpx


SOURCE_TYPE = "supabase_postgrest"
BYPASS_HEADER = "x-ukaq-egress-bypass"
DEFAULT_METRICS_SCHEMA = "uk_aq_public"
DEFAULT_METRICS_RPC = "uk_aq_rpc_service_egress_metrics_batch_upsert"


def _parse_bool(raw: Optional[str], fallback: bool = False) -> bool:
    if raw is None:
        return fallback
    normalized = str(raw).strip().lower()
    if not normalized:
        return fallback
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return fallback


def _utc_minute(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(
        second=0, microsecond=0
    ).isoformat().replace("+00:00", "Z")


def _project_ref(url: str) -> str:
    try:
        hostname = (urlparse(url).hostname or "").lower()
    except ValueError:
        return ""
    suffix = ".supabase.co"
    if not hostname.endswith(suffix):
        return ""
    return hostname[: -len(suffix)].split(".")[-1]


def _normalized_origin(url: str) -> str:
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"


def _source_name(url: str) -> str:
    origin = _normalized_origin(url)
    observs_origin = _normalized_origin(
        os.getenv("OBS_AQIDB_SUPABASE_URL") or ""
    )
    ingest_origin = _normalized_origin(os.getenv("SUPABASE_URL") or "")
    if origin and observs_origin and origin == observs_origin:
        return "obs_aqidb"
    if origin and ingest_origin and origin == ingest_origin:
        return "ingestdb"
    return "supabase"


def _route_and_query(method: str, path: str) -> tuple[str, str]:
    normalized_path = path.split("?", 1)[0]
    marker = "/rest/v1/"
    if marker not in normalized_path:
        return "postgrest/unknown", "postgrest_request"
    target = normalized_path.split(marker, 1)[1].strip("/")
    if target.startswith("rpc/"):
        route_name = target
        rpc_name = target.split("/", 1)[1]
        query_names = {
            "uk_aq_rpc_dispatch_claim": "dispatch_claim",
            "uk_aq_rpc_phenomena_ids": "lookup_phenomena_ids",
            "uk_aq_rpc_phenomena_upsert": "upsert_phenomena",
            "uk_aq_rpc_observations_compact_upsert_v1":
                "compact_observation_upsert",
            "uk_aq_rpc_observations_compact_upsert_v2":
                "compact_observation_upsert",
            "uk_aq_rpc_observs_observations_compact_upsert_v1":
                "compact_observation_upsert",
            "uk_aq_rpc_observs_outbox_enqueue": "enqueue_observs",
            "uk_aq_rpc_timeseries_last_values_compact_update_v1":
                "update_timeseries_last_values",
        }
        return route_name, query_names.get(rpc_name, "rpc_call")

    route_name = f"table/{target or 'unknown'}"
    query_names = {
        ("GET", "connectors"): "load_connector",
        ("PATCH", "connectors"): "update_connector_run",
        ("POST", "uk_aq_ingest_runs"): "insert_ingest_run",
        ("GET", "stations"): "load_stations",
        ("POST", "stations"): "upsert_stations",
        ("GET", "timeseries"): "lookup_timeseries_refs",
        ("POST", "timeseries"): "upsert_timeseries",
        ("PATCH", "timeseries"): "update_timeseries",
        ("GET", "blondon_nodes_station_checkpoints"):
            "load_station_checkpoints",
        ("POST", "blondon_nodes_station_checkpoints"):
            "upsert_station_checkpoints",
    }
    return route_name, query_names.get(
        (method.upper(), target), f"{method.lower()}_table"
    )


class ServiceEgressMetricsCollector:
    def __init__(self, service_name: str) -> None:
        self.service_name = str(service_name).strip()
        self.enabled = _parse_bool(
            os.getenv("UK_AQ_SERVICE_EGRESS_METRICS_ENABLED"), False
        )
        self.env_name = (os.getenv("UKAQ_ENV_NAME") or "TEST").strip()
        self._aggregates: Dict[str, Dict[str, Any]] = {}

    def _warn(self, message: str, **details: Any) -> None:
        try:
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "service_name": self.service_name,
                "message": message,
                **details,
            }
            print(
                json.dumps(payload, separators=(",", ":")),
                file=sys.stderr,
                flush=True,
            )
        except Exception:
            pass

    def create_httpx_client(self, supabase_url: str) -> httpx.Client:
        started_at_ns_key = "ukaq_service_egress_started_at_ns"

        def on_request(request: httpx.Request) -> None:
            request.extensions[started_at_ns_key] = time.monotonic_ns()

        def on_response(response: httpx.Response) -> None:
            try:
                response.read()
                started_at_ns = response.request.extensions.get(
                    started_at_ns_key
                )
                duration_ms = 0
                if isinstance(started_at_ns, int):
                    duration_ms = max(
                        0, (time.monotonic_ns() - started_at_ns) // 1_000_000
                    )
                route_name, query_name = _route_and_query(
                    response.request.method, response.request.url.path
                )
                self.record(
                    completed_at=datetime.now(timezone.utc),
                    duration_ms=duration_ms,
                    http_status=response.status_code,
                    project_ref=_project_ref(supabase_url),
                    query_name=query_name,
                    response_bytes=len(response.content),
                    route_name=route_name,
                    source_name=_source_name(supabase_url),
                )
            except Exception as exc:
                self._warn(
                    "service_egress_metrics_record_warning",
                    error=type(exc).__name__,
                )

        return httpx.Client(
            timeout=120,
            follow_redirects=True,
            http2=True,
            event_hooks={"request": [on_request], "response": [on_response]},
        )

    def record(
        self,
        *,
        completed_at: datetime,
        duration_ms: int,
        http_status: int,
        project_ref: str,
        query_name: str,
        response_bytes: int,
        route_name: str,
        source_name: str,
    ) -> None:
        if not self.enabled:
            return
        status = "ok" if 200 <= int(http_status) < 300 else "error"
        identity = {
            "bucket_minute": _utc_minute(completed_at),
            "env_name": self.env_name,
            "project_ref": project_ref,
            "service_name": self.service_name,
            "source_name": source_name,
            "route_name": route_name,
            "query_name": query_name,
            "window_label": "",
            "status": status,
        }
        key = json.dumps(list(identity.values()), separators=(",", ":"))
        aggregate = self._aggregates.get(key)
        if aggregate is None:
            aggregate = {
                **identity,
                "source_type": SOURCE_TYPE,
                "request_count": 0,
                "response_rows": 0,
                "response_bytes_est": 0,
                "upstream_bytes_est": 0,
                "duration_ms": 0,
                "error_count": 0,
                "http_statuses": set(),
                "http_status_classes": set(),
            }
            self._aggregates[key] = aggregate
        aggregate["request_count"] += 1
        aggregate["response_bytes_est"] += max(0, int(response_bytes))
        aggregate["duration_ms"] += max(0, int(duration_ms))
        aggregate["error_count"] += 1 if status == "error" else 0
        aggregate["http_statuses"].add(int(http_status))
        aggregate["http_status_classes"].add(f"{int(http_status) // 100}xx")

    def rows(self) -> list[Dict[str, Any]]:
        rows: list[Dict[str, Any]] = []
        for aggregate in self._aggregates.values():
            row = {
                key: value
                for key, value in aggregate.items()
                if key not in {"http_statuses", "http_status_classes"}
            }
            notes: Dict[str, Any] = {"measurement_method": "body_bytes"}
            if len(aggregate["http_statuses"]) == 1:
                notes["http_status"] = next(iter(aggregate["http_statuses"]))
            if len(aggregate["http_status_classes"]) == 1:
                notes["http_status_class"] = next(
                    iter(aggregate["http_status_classes"])
                )
            row["notes"] = notes
            rows.append(row)
        return rows

    def flush(self) -> None:
        pending_rows = self.rows()
        if not self.enabled or not pending_rows:
            return
        metrics_url = (
            os.getenv("UK_AQ_SERVICE_EGRESS_METRICS_SUPABASE_URL")
            or os.getenv("OBS_AQIDB_SUPABASE_URL")
            or ""
        ).strip()
        metrics_key = (
            os.getenv("UK_AQ_SERVICE_EGRESS_METRICS_SB_SECRET_KEY") or ""
        ).strip()
        schema = (
            os.getenv("UK_AQ_SERVICE_EGRESS_METRICS_SCHEMA")
            or DEFAULT_METRICS_SCHEMA
        ).strip()
        rpc_name = (
            os.getenv("UK_AQ_SERVICE_EGRESS_METRICS_RPC")
            or DEFAULT_METRICS_RPC
        ).strip()
        if not metrics_url or not metrics_key or not schema or not rpc_name:
            self._warn(
                "service_egress_metrics_flush_warning",
                reason="missing_metrics_configuration",
                aggregate_rows=len(pending_rows),
            )
            return
        headers = {
            "apikey": metrics_key,
            "Accept": "application/json",
            "Accept-Profile": schema,
            "Content-Type": "application/json",
            "Content-Profile": schema,
            BYPASS_HEADER: "1",
        }
        if metrics_key.startswith("eyJ") and metrics_key.count(".") == 2:
            headers["Authorization"] = f"Bearer {metrics_key}"
        try:
            url = (
                f"{metrics_url.rstrip('/')}/rest/v1/rpc/"
                f"{rpc_name}"
            )
            with httpx.Client(timeout=10, follow_redirects=True) as client:
                response = client.post(
                    url, headers=headers, json={"p_rows": pending_rows}
                )
                response.read()
            if not 200 <= response.status_code < 300:
                self._warn(
                    "service_egress_metrics_flush_warning",
                    reason="metrics_rpc_failed",
                    http_status=response.status_code,
                    aggregate_rows=len(pending_rows),
                )
                return
            self._aggregates.clear()
        except Exception as exc:
            self._warn(
                "service_egress_metrics_flush_warning",
                reason="metrics_rpc_error",
                error=type(exc).__name__,
                aggregate_rows=len(pending_rows),
            )


_collector: Optional[ServiceEgressMetricsCollector] = None


def configure_service_egress_metrics(
    service_name: str,
) -> ServiceEgressMetricsCollector:
    global _collector
    if _collector is None:
        _collector = ServiceEgressMetricsCollector(service_name)
    return _collector


def configured_service_egress_metrics(
) -> Optional[ServiceEgressMetricsCollector]:
    return _collector


def flush_service_egress_metrics() -> None:
    if _collector is not None:
        _collector.flush()
