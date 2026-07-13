#!/usr/bin/env python3
import json
import hmac
import os
import re
import signal
import subprocess
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

PORT = int(os.getenv("PORT", "8000"))
SCRIPT = os.getenv("BLONDON_NODES_INGEST_SCRIPT_PATH", "/app/scripts/blondon_nodes/blondon_nodes_ingest.py")
RUN_JOB_SCRIPT = os.getenv(
    "BLONDON_NODES_RUN_JOB_SCRIPT_PATH",
    "/app/workers/uk_aq_blondon_nodes_cloud_run/run_job.py",
)
ACCEPTED_KEYS = {
    "start_time", "end_time", "site_code", "species",
    "max_stations", "max_api_calls", "dry_run", "trigger_mode",
}
SITE_CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
ALLOWED_SPECIES = {"PM25", "NO2", "PM25Index", "NO2Index"}
MAX_REQUEST_BYTES = 4096
RUN_LOCK = threading.Lock()


def has_valid_run_auth(headers) -> bool:
    expected = os.getenv("UK_AQ_EDGE_UPSTREAM_SECRET", "").strip()
    if not expected:
        return False
    upstream = (headers.get("x-uk-aq-upstream-auth") or "").strip()
    dispatch = (headers.get("x-uk-aq-dispatch-secret") or "").strip()
    return hmac.compare_digest(upstream, expected) or hmac.compare_digest(dispatch, expected)


class RequestValidationError(ValueError):
    pass


def _validate_timestamp(key: str, value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 40:
        raise RequestValidationError(f"{key} must be a non-empty ISO 8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RequestValidationError(f"{key} must be a valid ISO 8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise RequestValidationError(f"{key} must include a timezone")
    return value


def validated_cli_args(payload: Any) -> list[str]:
    if not isinstance(payload, dict):
        raise RequestValidationError("request body must be a JSON object")
    unknown = sorted(set(payload) - ACCEPTED_KEYS)
    if unknown:
        raise RequestValidationError(f"unsupported request key: {unknown[0]}")

    args: list[str] = ["python3", SCRIPT]
    for key, flag in (("start_time", "--start-time"), ("end_time", "--end-time")):
        if key in payload:
            args.extend((flag, _validate_timestamp(key, payload[key])))

    if "site_code" in payload:
        value = payload["site_code"]
        if not isinstance(value, str) or not SITE_CODE_RE.fullmatch(value):
            raise RequestValidationError(
                "site_code must be 1-64 letters, numbers, dots, underscores, or hyphens"
            )
        args.extend(("--site-code", value))

    if "species" in payload:
        value = payload["species"]
        if not isinstance(value, str) or not value or len(value) > 64:
            raise RequestValidationError("species must be a non-empty comma-separated string")
        species = value.split(",")
        if any(item not in ALLOWED_SPECIES for item in species) or len(set(species)) != len(species):
            raise RequestValidationError(
                "species may contain each of PM25, NO2, PM25Index, and NO2Index once"
            )
        args.extend(("--species", value))

    for key, flag, maximum in (
        ("max_stations", "--max-stations", 10000),
        ("max_api_calls", "--max-api-calls", 100000),
    ):
        if key in payload:
            value = payload[key]
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
                raise RequestValidationError(f"{key} must be an integer from 1 to {maximum}")
            args.extend((flag, str(value)))

    if "dry_run" in payload:
        if not isinstance(payload["dry_run"], bool):
            raise RequestValidationError("dry_run must be a boolean")
        if payload["dry_run"]:
            args.append("--dry-run")
    if "trigger_mode" in payload:
        trigger_mode = payload["trigger_mode"]
        if trigger_mode not in {"scheduled", "manual"}:
            raise RequestValidationError(
                "trigger_mode must be scheduled or manual"
            )
    return args


def parse_job_summary(stdout: str) -> dict[str, Any] | None:
    for line in reversed(stdout.splitlines()):
        if line.startswith("JOB_SUMMARY_JSON "):
            try:
                value = json.loads(line[len("JOB_SUMMARY_JSON "):])
            except json.JSONDecodeError:
                return None
            return value if isinstance(value, dict) else None
    return None


def run_job(payload: dict[str, Any], timeout: int) -> tuple[int, dict[str, Any]]:
    process = subprocess.Popen(
        ["python3", RUN_JOB_SCRIPT],
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(
            json.dumps(payload, separators=(",", ":")),
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.communicate()
        recovery = subprocess.run(
            ["python3", RUN_JOB_SCRIPT, "--record-timeout", str(timeout)],
            text=True,
            capture_output=True,
            timeout=30,
        )
        summary = parse_job_summary(recovery.stdout) or {
            "ok": False,
            "run_status": "failed",
            "run_message": f"cloud_run child_timeout after {timeout}s",
            "timed_out": True,
            "timeout_seconds": timeout,
        }
        return 504, summary

    summary = parse_job_summary(stdout)
    if summary is None:
        summary = {
            "ok": False,
            "run_status": "failed",
            "run_message": "job_summary_missing",
            "returncode": process.returncode,
            "stdout": stdout[-4000:],
            "stderr": stderr[-4000:],
        }
    if process.returncode != 0:
        summary["ok"] = False
        return 500, summary
    return 200, summary

class Handler(BaseHTTPRequestHandler):
    def _json_response(self, status: int, body: dict[str, Any]) -> None:
        encoded = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        if self.path == "/status":
            if not has_valid_run_auth(self.headers):
                self._json_response(403, {"ok": False, "error": "forbidden"})
                return
            try:
                from run_job import RunTracker

                connector = RunTracker().load_connector()
                if connector is None:
                    self._json_response(404, {"ok": False, "error": "connector_not_found"})
                    return
                self._json_response(200, {
                    "connector_code": connector.get("connector_code"),
                    "last_run_start": connector.get("last_run_start"),
                    "last_run_end": connector.get("last_run_end"),
                    "last_run_status": connector.get("last_run_status"),
                    "last_run_message": connector.get("last_run_message"),
                })
            except Exception:
                self._json_response(500, {"ok": False, "error": "status_unavailable"})
            return
        self.send_response(200); self.end_headers(); self.wfile.write(b"ok")

    def do_POST(self):
        if not has_valid_run_auth(self.headers):
            self._json_response(403, {"ok": False, "error": "forbidden"})
            return
        try:
            length = int(self.headers.get("content-length") or 0)
            if length < 0 or length > MAX_REQUEST_BYTES:
                raise RequestValidationError(f"request body must not exceed {MAX_REQUEST_BYTES} bytes")
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            payload = json.loads(raw or "{}")
            args = validated_cli_args(payload)
        except (UnicodeDecodeError, json.JSONDecodeError, RequestValidationError, ValueError) as exc:
            self._json_response(400, {"ok": False, "error": str(exc)})
            return

        del args
        if not RUN_LOCK.acquire(blocking=False):
            self._json_response(409, {"ok": False, "error": "run_in_flight"})
            return
        try:
            try:
                status, body = run_job(
                    payload,
                    int(os.getenv("BLONDON_NODES_MAX_RUNTIME_SECONDS", "780")),
                )
            except Exception as exc:
                status = 500
                body = {
                    "ok": False,
                    "run_status": "failed",
                    "run_message": f"cloud_run_wrapper_failed: {exc}",
                }
            self._json_response(status, body)
        finally:
            RUN_LOCK.release()


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
