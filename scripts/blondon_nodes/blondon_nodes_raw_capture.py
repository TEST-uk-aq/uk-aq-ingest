"""Best-effort raw /SensorData capture for Breathe London Nodes."""

from __future__ import annotations

import io
import json
import logging
import os
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Tuple

import requests


LOG = logging.getLogger("blondon_nodes_ingest")

DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token"
DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload"
DROPBOX_REQUEST_TIMEOUT_SECONDS = 15
DROPBOX_UPLOAD_TIMEOUT_SECONDS = 30
MAX_DIAGNOSTIC_LENGTH = 500


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalized_url(value: Optional[str]) -> str:
    return (value or "").strip().rstrip("/")


def normalize_dropbox_path(*parts: str) -> str:
    segments = []
    for part in parts:
        normalized = str(part or "").strip().replace("\\", "/")
        segments.extend(segment for segment in normalized.split("/") if segment)
    return "/" + "/".join(segments)


def build_archive_paths(root: str, created_at: datetime) -> Tuple[str, str]:
    timestamp = created_at.astimezone(timezone.utc)
    stamp = timestamp.strftime("%Y%m%dT%H%M%SZ")
    basename = f"uk_aq_raw_cloud_run_blondon_nodes_{stamp}"
    dropbox_path = normalize_dropbox_path(
        root,
        "connectors",
        "blondon_nodes",
        "raw_data",
        timestamp.strftime("%Y-%m-%d"),
        f"{basename}.zip",
    )
    return dropbox_path, f"{basename}.jsonl"


def bounded_error(prefix: str, exc: Exception) -> str:
    if isinstance(exc, requests.RequestException):
        detail = type(exc).__name__
    else:
        detail = " ".join(str(exc).split())
    message = f"{prefix}: {detail}" if detail else prefix
    return message[:MAX_DIAGNOSTIC_LENGTH]


@dataclass(frozen=True, repr=False)
class DropboxConfig:
    app_key: str
    app_secret: str
    refresh_token: str


def resolve_raw_capture_gate(
    environment: Mapping[str, str],
) -> Tuple[bool, str, Optional[DropboxConfig], str]:
    supabase_url = normalized_url(environment.get("SUPABASE_URL"))
    connector_allowed = normalized_url(
        environment.get("BLONDON_NODES_RAW_DROPBOX_ALLOWED_SUPABASE_URL")
    )
    generic_allowed = normalized_url(
        environment.get("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL")
    )
    allowed_url = connector_allowed or generic_allowed
    dropbox_root = environment.get("UK_AQ_DROPBOX_ROOT", "")

    app_key = (environment.get("DROPBOX_APP_KEY") or "").strip()
    app_secret = (environment.get("DROPBOX_APP_SECRET") or "").strip()
    refresh_token = (environment.get("DROPBOX_REFRESH_TOKEN") or "").strip()

    if not supabase_url:
        return False, "supabase_url_missing", None, dropbox_root
    if not allowed_url:
        return False, "raw_allowed_supabase_url_missing", None, dropbox_root
    if allowed_url != supabase_url:
        return False, "supabase_url_not_allowlisted", None, dropbox_root
    if not (app_key and app_secret and refresh_token):
        return False, "dropbox_credentials_missing", None, dropbox_root
    return (
        True,
        "enabled",
        DropboxConfig(app_key, app_secret, refresh_token),
        dropbox_root,
    )


class NodesRawCapture:
    """Collect and upload at most one raw archive without affecting ingest state."""

    def __init__(
        self,
        enabled: bool,
        reason: str,
        config: Optional[DropboxConfig],
        dropbox_root: str,
        created_at: Optional[datetime] = None,
    ) -> None:
        self.enabled = enabled
        self.reason = reason
        self.config = config
        self.dropbox_root = dropbox_root
        self.created_at = created_at or utcnow()
        self.response_count = 0
        self.uploaded = False
        self.dropbox_path: Optional[str] = None
        self.error: Optional[str] = None
        self._context_recorded = False
        self._finalized = False
        self._lines = []
        if self.enabled:
            self._append({"type": "meta", "created_at": iso_utc(self.created_at)})
        else:
            LOG.info("Nodes raw capture disabled reason=%s", self.reason)

    @classmethod
    def from_environment(cls) -> "NodesRawCapture":
        enabled, reason, config, root = resolve_raw_capture_gate(os.environ)
        return cls(enabled, reason, config, root)

    def _record_error(self, prefix: str, exc: Exception) -> None:
        message = bounded_error(prefix, exc)
        if self.error is None:
            self.error = message
        elif message not in self.error:
            self.error = f"{self.error}; {message}"[:MAX_DIAGNOSTIC_LENGTH]
        LOG.warning("Nodes raw capture warning: %s", message)

    def _append(self, record: Dict[str, Any]) -> None:
        self._lines.append(
            json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        )

    def record_context(self, context: Dict[str, Any]) -> None:
        if not self.enabled or self._context_recorded:
            return
        try:
            self._append(
                {
                    "type": "context",
                    "recorded_at": iso_utc(utcnow()),
                    "payload": context,
                }
            )
            self._context_recorded = True
        except Exception as exc:
            self._record_error("raw_context_record_failed", exc)

    def record_response(
        self,
        path: str,
        params: Dict[str, Any],
        status_code: int,
        payload: Any,
    ) -> None:
        if not self.enabled:
            return
        try:
            if not self._context_recorded:
                self.record_context({})
            self._append(
                {
                    "type": "response",
                    "fetched_at": iso_utc(utcnow()),
                    "path": path,
                    "params": params,
                    "status_code": status_code,
                    "payload": payload,
                }
            )
            self.response_count += 1
        except Exception as exc:
            self._record_error("raw_response_record_failed", exc)

    def _refresh_access_token(self) -> str:
        if self.config is None:
            raise RuntimeError("Dropbox configuration unavailable")
        response = requests.post(
            DROPBOX_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.config.refresh_token,
                "client_id": self.config.app_key,
                "client_secret": self.config.app_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=DROPBOX_REQUEST_TIMEOUT_SECONDS,
        )
        if not response.ok:
            raise RuntimeError(
                f"Dropbox token request failed with HTTP {response.status_code}"
            )
        token_payload = response.json()
        access_token = token_payload.get("access_token")
        if not access_token:
            raise RuntimeError("Dropbox token response missing access_token")
        return str(access_token)

    @staticmethod
    def _zip_jsonl(inner_name: str, lines: list[str]) -> bytes:
        content = ("\n".join(lines) + "\n").encode("utf-8")
        output = io.BytesIO()
        with zipfile.ZipFile(
            output,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            archive.writestr(inner_name, content)
        return output.getvalue()

    @staticmethod
    def _upload(
        access_token: str,
        dropbox_path: str,
        archive_bytes: bytes,
    ) -> requests.Response:
        return requests.post(
            DROPBOX_UPLOAD_URL,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Dropbox-API-Arg": json.dumps(
                    {
                        "path": dropbox_path,
                        "mode": "add",
                        "autorename": True,
                        "mute": False,
                    },
                    separators=(",", ":"),
                ),
                "Content-Type": "application/octet-stream",
            },
            data=archive_bytes,
            timeout=DROPBOX_UPLOAD_TIMEOUT_SECONDS,
        )

    def finalize(self) -> None:
        if self._finalized:
            return
        self._finalized = True
        if not self.enabled or self.response_count == 0:
            return

        dropbox_path, inner_name = build_archive_paths(
            self.dropbox_root,
            self.created_at,
        )
        try:
            archive_bytes = self._zip_jsonl(inner_name, self._lines)
            access_token = self._refresh_access_token()
            response = self._upload(access_token, dropbox_path, archive_bytes)
            if response.status_code == 401:
                access_token = self._refresh_access_token()
                response = self._upload(access_token, dropbox_path, archive_bytes)
            if not response.ok:
                raise RuntimeError(
                    f"Dropbox upload failed with HTTP {response.status_code}"
                )
            try:
                upload_result = response.json()
            except ValueError:
                upload_result = {}
            self.dropbox_path = str(
                upload_result.get("path_display")
                or upload_result.get("path_lower")
                or dropbox_path
            )
            self.uploaded = True
        except Exception as exc:
            self._record_error("raw_dropbox_finalization_failed", exc)

    def finalize_safely(self) -> None:
        try:
            self.finalize()
        except Exception as exc:
            try:
                self._record_error("raw_dropbox_finalization_failed", exc)
            except Exception:
                LOG.warning(
                    "Nodes raw capture finalization failed error_type=%s",
                    type(exc).__name__,
                )

    def summary_fields(self) -> Dict[str, Any]:
        return {
            "raw_capture_enabled": self.enabled,
            "raw_capture_reason": self.reason,
            "raw_source_responses_recorded": self.response_count,
            "raw_dropbox_uploaded": self.uploaded,
            "raw_dropbox_path": self.dropbox_path,
            "raw_dropbox_error": self.error,
        }
