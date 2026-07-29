#!/usr/bin/env python3
"""
Sensor.Community ingestion helper (UK only).

This script:
1) Fetches recent sensor values from data.sensor.community for GB.
2) Reads connector + upserts station metadata into Supabase.
3) Creates/updates timeseries per station + pollutant.
4) Inserts observations for the latest values.

Environment:
- SUPABASE_URL
- SB_SECRET_KEY
- SCOMM_BASE_URL (optional; defaults to https://data.sensor.community)
- SCOMM_CONNECTOR_CODE (optional; defaults to sensorcommunity; legacy SCOMM_CONNECTOR_REF supported)
- SCOMM_SERVICE_REF (optional; defaults to SCOMM_CONNECTOR_CODE)
- SCOMM_SERVICE_LABEL (optional; defaults to Sensor.Community; legacy SCOMM_CONNECTOR_LABEL supported)
- SCOMM_COUNTRY (optional; defaults to GB)
- SCOMM_USER_AGENT (optional; identifies your client per Sensor.Community guidance)
- SCOMM_INGEST_MET_FIELDS (optional; defaults to false; enable temperature/humidity/pressure ingestion)
- SCOMM_FILE_LOG_LEVEL (optional; defaults to INFO)
- DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN (for Dropbox logging)
- SCOMM_RAW_DROPBOX_ALLOWED_SUPABASE_URL (optional; gates raw Dropbox uploads)
- SCOMM_RAW_DROPBOX_FOLDER (optional; defaults to /connectors/sensorcommunity/raw_data; falls back to UK_AIR_RAW_DROPBOX_FOLDER)
- SCOMM_ERROR_DROPBOX_FOLDER (optional; defaults to /error_log; falls back to UK_AIR_ERROR_DROPBOX_FOLDER)

Example:
  python3 scripts/sensorcommunity/sensorcommunity_ingest.py --refresh-recent
"""

import argparse
import gzip
import json
import logging
import os
import sys
import tempfile
import time
import traceback
import uuid
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

warnings.filterwarnings(
    "ignore",
    message="urllib3 v2 only supports OpenSSL 1.1.1\\+",
    category=Warning,
    module="urllib3",
)

import requests
from dotenv import load_dotenv
from supabase import Client

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingest_helpers import station_coords, station_in_bbox_or_missing_coords
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client
from scripts.uk_aq_phenomena_rpc import upsert_phenomena_via_rpc

load_dotenv()

LOG = logging.getLogger("sensorcommunity_ingest")
DEFAULT_LOG_LEVEL = os.getenv("SCOMM_LOG_LEVEL", "INFO").upper()
DEFAULT_FILE_LOG_LEVEL = os.getenv("SCOMM_FILE_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))

SCOMM_BASE_URL = (os.getenv("SCOMM_BASE_URL") or "https://data.sensor.community").rstrip("/")
SCOMM_CONNECTOR_CODE = (
    os.getenv("SCOMM_CONNECTOR_CODE")
    or os.getenv("SCOMM_CONNECTOR_REF")
    or os.getenv("SCOMM_SERVICE_REF")
    or "sensorcommunity"
)
SCOMM_SERVICE_REF = os.getenv("SCOMM_SERVICE_REF") or SCOMM_CONNECTOR_CODE
SCOMM_SERVICE_LABEL = (
    os.getenv("SCOMM_SERVICE_LABEL")
    or os.getenv("SCOMM_CONNECTOR_LABEL")
    or "Sensor.Community"
)
SCOMM_COUNTRY = os.getenv("SCOMM_COUNTRY", "GB")
SCOMM_USER_AGENT = os.getenv("SCOMM_USER_AGENT", "uk-air-quality-networks")
SCOMM_INGEST_MET_FIELDS = (
    os.getenv("SCOMM_INGEST_MET_FIELDS", "false").strip().lower() in {"1", "true", "yes", "y", "on"}
)

DEFAULT_RAW_DROPBOX_FOLDER = "/connectors/sensorcommunity/raw_data"
DEFAULT_ERROR_DROPBOX_FOLDER = "/error_log"
DROPBOX_LOG_RETENTION_DAYS = 31
DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token"
DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload"
DROPBOX_LIST_FOLDER_URL = "https://api.dropboxapi.com/2/files/list_folder"
DROPBOX_DOWNLOAD_ZIP_URL = "https://content.dropboxapi.com/2/files/download_zip"
DROPBOX_DELETE_URL = "https://api.dropboxapi.com/2/files/delete_v2"

UK_BBOX = {
    "west": -11.0,
    "south": 49.0,
    "east": 2.0,
    "north": 61.0,
}

VALUE_TYPE_MAP = {
    "P1": {"pollutant": "pm10", "label": "PM10", "uom": "ug/m3"},
    "P2": {"pollutant": "pm2.5", "label": "PM2.5", "uom": "ug/m3"},
}

SCOMM_PHENOMENA = {
    "pm10": {
        "source_label": "sensorcommunity:pm10",
        "label": "PM10",
        "notation": "PM10",
        "pollutant_label": "pm10",
    },
    "pm2.5": {
        "source_label": "sensorcommunity:pm2.5",
        "label": "PM2.5",
        "notation": "PM2.5",
        "pollutant_label": "pm2.5",
    },
}

if SCOMM_INGEST_MET_FIELDS:
    VALUE_TYPE_MAP.update(
        {
            "temperature": {"pollutant": "temperature", "label": "Temperature", "uom": "degC"},
            "humidity": {"pollutant": "humidity", "label": "Humidity", "uom": "%"},
            "pressure": {"pollutant": "pressure", "label": "Pressure", "uom": "hPa"},
        }
    )
    SCOMM_PHENOMENA.update(
        {
            "temperature": {
                "source_label": "sensorcommunity:temperature",
                "label": "Temperature",
                "notation": "temperature",
                "pollutant_label": "temperature",
            },
            "humidity": {
                "source_label": "sensorcommunity:humidity",
                "label": "Humidity",
                "notation": "humidity",
                "pollutant_label": "humidity",
            },
            "pressure": {
                "source_label": "sensorcommunity:pressure",
                "label": "Pressure",
                "notation": "pressure",
                "pollutant_label": "pressure",
            },
        }
    )


@dataclass(frozen=True)
class DropboxConfig:
    app_key: str
    app_secret: str
    refresh_token: str
    folder: str


class RawPayloadRecorder:
    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self._handle = gzip.open(output_path, "wt", encoding="utf-8")
        self.count = 0
        self.record_event("meta", {"created_at": utcnow().isoformat()})

    def record_event(self, name: str, payload: Dict[str, Any]) -> None:
        self._write(
            {
                "type": name,
                "recorded_at": utcnow().isoformat(),
                "payload": payload,
            }
        )

    def record_response(
        self,
        path: str,
        params: Optional[Dict[str, Any]],
        status_code: int,
        payload: Any,
    ) -> None:
        self._write(
            {
                "type": "response",
                "fetched_at": utcnow().isoformat(),
                "path": path,
                "params": params,
                "status_code": status_code,
                "payload": payload,
            }
        )

    def _write(self, payload: Dict[str, Any]) -> None:
        self._handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
        self.count += 1

    def close(self) -> None:
        self._handle.close()


@dataclass
class RawDropboxSession:
    recorder: RawPayloadRecorder
    config: DropboxConfig
    temp_dir: tempfile.TemporaryDirectory
    log_path: Path
    log_handler: logging.Handler

    def finalize(self) -> None:
        logging.getLogger().removeHandler(self.log_handler)
        self.log_handler.close()
        self.recorder.close()
        try:
            access_token = _dropbox_refresh_access_token(self.config)
            if self.recorder.count > 1:
                dropbox_path = _dropbox_target_path(self.config.folder, self.recorder.output_path.name)
                _dropbox_upload_file(access_token, self.recorder.output_path, dropbox_path)
                _emit_info(f"Uploaded raw payloads to Dropbox: {dropbox_path}")
            else:
                LOG.debug("Raw payload capture produced no response entries; skipping raw Dropbox upload.")
            log_dropbox_path = _dropbox_log_target_path(self.config.folder, self.log_path.name)
            _dropbox_upload_file(access_token, self.log_path, log_dropbox_path)
            _dropbox_archive_logs(access_token, _dropbox_log_root_folder(self.config.folder), days=DROPBOX_LOG_RETENTION_DAYS)
        except Exception as exc:
            LOG.warning("Dropbox upload failed: %s", exc)
        finally:
            self.temp_dir.cleanup()


class ErrorLogger:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.dropbox_config = _load_error_dropbox_config()
        self._dropbox_access_token: Optional[str] = None

    def log_error(
        self,
        *,
        source: str,
        severity: str,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        connector_id: Optional[int] = None,
        station_id: Optional[int] = None,
        timeseries_id: Optional[int] = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        error_id = str(uuid.uuid4())
        created_at = utcnow().isoformat()
        stack = None
        if exc is not None:
            stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        payload: Dict[str, Any] = {
            "id": error_id,
            "source": source,
            "severity": severity,
            "message": message,
            "stack": stack,
            "context": context,
            "connector_id": connector_id,
            "station_id": station_id,
            "timeseries_id": timeseries_id,
        }
        try:
            self.raw.table("error_logs").insert(payload).execute()
        except Exception as insert_exc:
            LOG.warning("Failed to insert error_logs row: %s", insert_exc)
            return

        if not self.dropbox_config:
            return

        dropbox_path = _dropbox_error_target_path(
            self.dropbox_config.folder,
            _build_error_filename(created_at, error_id),
        )
        error_payload = {
            "id": error_id,
            "created_at": created_at,
            "source": source,
            "severity": severity,
            "message": message,
            "stack": stack,
            "context": context,
            "connector_id": connector_id,
            "station_id": station_id,
            "timeseries_id": timeseries_id,
        }
        try:
            if not self._dropbox_access_token:
                self._dropbox_access_token = _dropbox_refresh_access_token(self.dropbox_config)
            _dropbox_upload_bytes(
                self._dropbox_access_token,
                json.dumps(error_payload, ensure_ascii=True, indent=2).encode("utf-8"),
                dropbox_path,
            )
            self.raw.table("error_logs").update({"dropbox_path": dropbox_path}).eq(
                "id",
                error_id,
            ).execute()
        except Exception as exc:
            LOG.warning("Dropbox error log upload failed: %s", exc)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _configure_logging(console_level_name: str, file_level_name: str) -> None:
    console_level = getattr(logging, (console_level_name or "").upper(), logging.INFO)
    file_level = getattr(logging, (file_level_name or "").upper(), logging.INFO)
    logging.getLogger().setLevel(min(console_level, file_level))
    LOG.setLevel(min(console_level, file_level))
    for handler in logging.getLogger().handlers:
        handler.setLevel(console_level)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("supabase").setLevel(logging.WARNING)
    logging.getLogger("postgrest").setLevel(logging.WARNING)
    logging.getLogger("gotrue").setLevel(logging.WARNING)


def _emit_info(message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    print(f"{timestamp} INFO {message}")


def _add_file_logger(path: Path, level_name: str) -> logging.Handler:
    level = getattr(logging, (level_name or "").upper(), logging.INFO)
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(handler)
    return handler


def _dropbox_refresh_access_token(config: DropboxConfig) -> str:
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": config.refresh_token,
        "client_id": config.app_key,
        "client_secret": config.app_secret,
    }
    resp = requests.post(DROPBOX_TOKEN_URL, data=payload, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Dropbox token request failed ({resp.status_code}): {resp.text}")
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Dropbox token response missing access_token.")
    return token


def _dropbox_upload_file(access_token: str, local_path: Path, dropbox_path: str) -> None:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps(
            {
                "path": dropbox_path,
                "mode": "add",
                "autorename": True,
                "mute": False,
            }
        ),
        "Content-Type": "application/octet-stream",
    }
    with local_path.open("rb") as handle:
        resp = requests.post(DROPBOX_UPLOAD_URL, headers=headers, data=handle, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"Dropbox upload failed ({resp.status_code}): {resp.text}")


def _dropbox_upload_bytes(access_token: str, payload: bytes, dropbox_path: str) -> None:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps(
            {
                "path": dropbox_path,
                "mode": "add",
                "autorename": True,
                "mute": False,
            }
        ),
        "Content-Type": "application/octet-stream",
    }
    resp = requests.post(DROPBOX_UPLOAD_URL, headers=headers, data=payload, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"Dropbox upload failed ({resp.status_code}): {resp.text}")


def _dropbox_download_zip(access_token: str, folder_path: str) -> bytes:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps({"path": folder_path}),
    }
    resp = requests.post(DROPBOX_DOWNLOAD_ZIP_URL, headers=headers, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"Dropbox download_zip failed ({resp.status_code}): {resp.text}")
    return resp.content


def _normalize_dropbox_path(path: str) -> str:
    cleaned = (path or "").strip()
    if not cleaned:
        return ""
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned.rstrip("/")


def _dropbox_root_folder(folder: str) -> str:
    env_root = _normalize_dropbox_path(os.getenv("UK_AQ_DROPBOX_ROOT", ""))
    cleaned = _normalize_dropbox_path(folder)
    if cleaned.endswith("/raw_data"):
        cleaned = cleaned[: -len("/raw_data")]
    elif cleaned.endswith("/log"):
        cleaned = cleaned[: -len("/log")]
    elif cleaned.endswith("/error_log"):
        cleaned = cleaned[: -len("/error_log")]
    if env_root:
        if not cleaned:
            return env_root
        if cleaned == env_root or cleaned.startswith(f"{env_root}/"):
            return cleaned
        return f"{env_root}{cleaned}"
    return cleaned


def _dropbox_base_folder(folder: str) -> str:
    root = _dropbox_root_folder(folder or DEFAULT_RAW_DROPBOX_FOLDER)
    if root:
        return f"{root}/raw_data"
    return "/raw_data"


def _dropbox_target_path(folder: str, filename: str) -> str:
    date_folder = utcnow().strftime("%Y-%m-%d")
    return f"{_dropbox_base_folder(folder)}/{date_folder}/{filename}"


def _dropbox_log_root_folder(folder: str) -> str:
    root = _dropbox_root_folder(folder)
    if root:
        return f"{root}/log"
    return "/log"


def _dropbox_log_folder_path(folder: str, date_folder: Optional[str] = None) -> str:
    root = _dropbox_log_root_folder(folder)
    date_folder = date_folder or utcnow().strftime("%Y-%m-%d")
    return f"{root}/{date_folder}"


def _dropbox_log_target_path(folder: str, filename: str) -> str:
    return f"{_dropbox_log_folder_path(folder)}/{filename}"


def _dropbox_error_root_folder(folder: str) -> str:
    root = _dropbox_root_folder(folder or DEFAULT_ERROR_DROPBOX_FOLDER)
    if root.endswith("/error_log"):
        root = root[: -len("/error_log")]
    if root:
        return f"{root}/error_log"
    return "/error_log"


def _dropbox_error_folder_path(folder: str, date_folder: Optional[str] = None) -> str:
    root = _dropbox_error_root_folder(folder)
    date_folder = date_folder or utcnow().strftime("%Y-%m-%d")
    return f"{root}/{date_folder}"


def _dropbox_error_target_path(folder: str, filename: str) -> str:
    return f"{_dropbox_error_folder_path(folder)}/{filename}"


def _dropbox_archive_logs(
    access_token: str,
    log_root: str,
    days: int = DROPBOX_LOG_RETENTION_DAYS,
    archive_days: int = 365,
) -> None:
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    cutoff_date = (utcnow() - timedelta(days=days)).date()
    archive_cutoff = (utcnow() - timedelta(days=archive_days)).date()
    archive_root = f"{log_root}/archive"

    def list_folder(path: str) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {"path": path}
        entries: List[Dict[str, Any]] = []
        while True:
            resp = requests.post(DROPBOX_LIST_FOLDER_URL, headers=headers, json=payload, timeout=30)
            if resp.status_code == 409:
                return []
            if resp.status_code >= 400:
                raise RuntimeError(f"Dropbox list_folder failed ({resp.status_code}): {resp.text}")
            data = resp.json()
            entries.extend(data.get("entries", []))
            if not data.get("has_more"):
                return entries
            payload = {"cursor": data.get("cursor")}

    entries = list_folder(log_root)
    archive_entries = list_folder(archive_root)
    existing_archives = {
        entry.get("name")
        for entry in archive_entries
        if entry.get(".tag") == "file" and entry.get("name")
    }

    for entry in entries:
        if entry.get(".tag") != "folder":
            continue
        name = entry.get("name")
        if not name or name == "archive":
            continue
        try:
            folder_date = datetime.strptime(name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if folder_date >= cutoff_date:
            continue
        archive_name = f"{name}.zip"
        archive_path = f"{archive_root}/{archive_name}"
        folder_path = entry.get("path_lower") or entry.get("path_display")
        if not folder_path:
            continue
        if archive_name not in existing_archives:
            zip_payload = _dropbox_download_zip(access_token, folder_path)
            _dropbox_upload_bytes(access_token, zip_payload, archive_path)
        delete_resp = requests.post(
            DROPBOX_DELETE_URL,
            headers=headers,
            json={"path": folder_path},
            timeout=30,
        )
        if delete_resp.status_code >= 400:
            LOG.warning(
                "Dropbox log folder delete failed (%s): %s",
                delete_resp.status_code,
                delete_resp.text,
            )

    for entry in archive_entries:
        if entry.get(".tag") != "file":
            continue
        name = entry.get("name") or ""
        if not name.endswith(".zip"):
            continue
        date_part = name[:-4]
        try:
            archive_date = datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            continue
        if archive_date >= archive_cutoff:
            continue
        path = entry.get("path_lower") or entry.get("path_display")
        if not path:
            continue
        delete_resp = requests.post(
            DROPBOX_DELETE_URL,
            headers=headers,
            json={"path": path},
            timeout=30,
        )
        if delete_resp.status_code >= 400:
            LOG.warning(
                "Dropbox archive delete failed (%s): %s",
                delete_resp.status_code,
                delete_resp.text,
            )


def _build_raw_label(args: argparse.Namespace) -> str:
    stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
    parts = ["uk_aq_raw_scomm", stamp, SCOMM_COUNTRY.lower()]
    if args.no_filter:
        parts.append("nofilter")
    return "_".join(parts)


def _build_log_filename(args: argparse.Namespace) -> str:
    stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
    parts = ["uk_aq_log_scomm", stamp, SCOMM_COUNTRY.lower()]
    if args.no_filter:
        parts.append("nofilter")
    return f"{'_'.join(parts)}.log"


def _build_error_filename(created_at: str, error_id: str) -> str:
    try:
        stamp = datetime.fromisoformat(created_at.replace("Z", "+00:00")).strftime("%Y%m%dT%H%M%SZ")
    except ValueError:
        stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
    return f"uk_aq_error_scomm_{stamp}_{error_id}.json"


def _load_dropbox_config(folder_override: Optional[str]) -> Optional[DropboxConfig]:
    app_key = os.getenv("DROPBOX_APP_KEY", "").strip()
    app_secret = os.getenv("DROPBOX_APP_SECRET", "").strip()
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN", "").strip()
    folder = (
        folder_override
        or os.getenv("SCOMM_RAW_DROPBOX_FOLDER")
        or os.getenv("UK_AIR_RAW_DROPBOX_FOLDER")
        or DEFAULT_RAW_DROPBOX_FOLDER
    ).strip()
    if not (app_key and app_secret and refresh_token):
        LOG.warning("Dropbox credentials missing; skipping raw payload upload.")
        return None
    return DropboxConfig(
        app_key=app_key,
        app_secret=app_secret,
        refresh_token=refresh_token,
        folder=folder,
    )


def _load_error_dropbox_config() -> Optional[DropboxConfig]:
    app_key = os.getenv("DROPBOX_APP_KEY", "").strip()
    app_secret = os.getenv("DROPBOX_APP_SECRET", "").strip()
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN", "").strip()
    folder = (
        os.getenv("SCOMM_ERROR_DROPBOX_FOLDER")
        or os.getenv("UK_AIR_ERROR_DROPBOX_FOLDER")
        or DEFAULT_ERROR_DROPBOX_FOLDER
    ).strip()
    if not (app_key and app_secret and refresh_token):
        LOG.warning("Dropbox credentials missing; skipping error Dropbox upload.")
        return None
    return DropboxConfig(
        app_key=app_key,
        app_secret=app_secret,
        refresh_token=refresh_token,
        folder=folder,
    )


def _raw_dropbox_allowed() -> bool:
    supabase_url = os.getenv("SUPABASE_URL", "").strip()
    allowed_url = (
        os.getenv("SCOMM_RAW_DROPBOX_ALLOWED_SUPABASE_URL")
        or os.getenv("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL")
        or ""
    ).strip()
    if not allowed_url:
        LOG.warning("SCOMM_RAW_DROPBOX_ALLOWED_SUPABASE_URL not set; raw Dropbox upload disabled.")
        return False
    if supabase_url != allowed_url:
        LOG.warning("Raw Dropbox upload disabled (SUPABASE_URL does not match allowed URL).")
        return False
    return True


def _prepare_raw_dropbox_session(args: argparse.Namespace) -> Optional[RawDropboxSession]:
    if not args.raw_dropbox:
        return None
    if not _raw_dropbox_allowed():
        return None
    config = _load_dropbox_config(args.raw_dropbox_folder)
    if not config:
        return None
    temp_dir = tempfile.TemporaryDirectory(prefix="uk_aq_scomm_raw_")
    filename = f"{_build_raw_label(args)}.jsonl.gz"
    output_path = Path(temp_dir.name) / filename
    recorder = RawPayloadRecorder(output_path)
    log_path = Path(temp_dir.name) / _build_log_filename(args)
    log_handler = _add_file_logger(log_path, DEFAULT_FILE_LOG_LEVEL)
    LOG.debug("Raw Dropbox capture enabled (output=%s).", output_path.name)
    return RawDropboxSession(
        recorder=recorder,
        config=config,
        temp_dir=temp_dir,
        log_path=log_path,
        log_handler=log_handler,
    )


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        try:
            return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


class SensorCommunityClient:
    def __init__(
        self,
        base_url: str = SCOMM_BASE_URL,
        timeout: int = 60,
        retries: int = 3,
        raw_recorder: Optional[RawPayloadRecorder] = None,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": SCOMM_USER_AGENT})
        self.raw_recorder = raw_recorder

    def get(self, path: str) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    self._sleep(attempt)
                    continue
                resp.raise_for_status()
                payload = resp.json()
                if self.raw_recorder:
                    self.raw_recorder.record_response(path, None, resp.status_code, payload)
                return payload
            except requests.RequestException as exc:
                LOG.warning("Request failed (attempt %s/%s): %s", attempt, self.retries, exc)
                if attempt == self.retries:
                    raise
                self._sleep(attempt)
        return []

    def _sleep(self, attempt: int) -> None:
        time.sleep(min(30, 2**attempt))

    def recent_values(self) -> List[Dict[str, Any]]:
        payload = self.get(f"/airrohr/v1/filter/country={SCOMM_COUNTRY}")
        if isinstance(payload, list):
            LOG.info("Fetched %s recent sensor payloads.", len(payload))
            return payload
        return []


@dataclass(frozen=True)
class TimeseriesKey:
    station_ref: str
    pollutant: str


class SupabaseWriter:
    def __init__(self) -> None:
        self.client: Client = create_supabase_client()
        schemas = SupabaseSchemas.from_client(self.client)
        self.core = schemas.core
        self.raw = schemas.raw
        self.public = self.client.schema(os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public")

    def upsert_connector(self) -> Tuple[int, bool]:
        row = (
            self.core.table("connectors")
            .select("id,overwrite_station_name")
            .eq("connector_code", SCOMM_CONNECTOR_CODE)
            .single()
            .execute()
        )
        data = row.data if hasattr(row, "data") else row.get("data")
        if not data:
            raise RuntimeError("Connector not found for Sensor.Community. Run the list_stations job first.")
        overwrite_station_name = data.get("overwrite_station_name")
        return int(data["id"]), bool(overwrite_station_name)

    def fetch_station_names(
        self, connector_id: int, service_ref: str, station_refs: Iterable[str]
    ) -> Dict[str, Optional[str]]:
        refs = [str(ref) for ref in station_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, Optional[str]] = {}
        for chunk in chunked(refs, 200):
            resp = (
                self.core.table("stations")
                .select("station_ref,station_name")
                .eq("connector_id", connector_id)
                .eq("service_ref", str(service_ref))
                .in_("station_ref", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                mapping[str(row.get("station_ref"))] = row.get("station_name")
        return mapping

    def upsert_stations(
        self,
        stations: Iterable[Dict[str, Any]],
        connector_id: int,
        service_ref: str,
        overwrite_station_name: bool,
    ) -> int:
        rows_by_ref: Dict[str, Dict[str, Any]] = {}
        for station in stations:
            payload = normalize_station_payload(station)
            station_ref = payload.get("station_ref")
            if not station_ref:
                continue
            lon = payload.get("longitude")
            lat = payload.get("latitude")
            station_ref_value = str(station_ref)
            station_name = payload.get("station_name")
            if isinstance(station_name, str) and not station_name.strip():
                station_name = None
            candidate = {
                "station_ref": station_ref_value,
                "service_ref": str(service_ref),
                "label": payload.get("label") or f"Sensor.Community {station_ref_value}",
                "station_name": station_name,
                "station_type": payload.get("station_type"),
                "station_exposure": payload.get("station_exposure"),
                "geometry": (
                    f"SRID=4326;POINT({lon} {lat})"
                    if lon is not None and lat is not None
                    else None
                ),
                "connector_id": connector_id,
                "last_seen_at": utcnow().isoformat(),
                "removed_at": None,
            }
            existing = rows_by_ref.get(station_ref_value)
            if existing is None:
                rows_by_ref[station_ref_value] = candidate
            else:
                rows_by_ref[station_ref_value] = _merge_station_row(existing, candidate)
        rows = list(rows_by_ref.values())
        if rows and not overwrite_station_name:
            existing_names = self.fetch_station_names(
                connector_id,
                service_ref,
                [row.get("station_ref") for row in rows if row.get("station_ref")],
            )
            for row in rows:
                station_ref_value = row.get("station_ref")
                existing_name = existing_names.get(str(station_ref_value))
                if isinstance(existing_name, str) and not existing_name.strip():
                    existing_name = None
                if existing_name is not None:
                    row["station_name"] = existing_name
        if rows:
            self.core.table("stations").upsert(
                rows, on_conflict="connector_id,service_ref,station_ref"
            ).execute()
        return len(rows)

    def upsert_phenomena(self, connector_id: int) -> Dict[str, int]:
        payload = []
        for pollutant, meta in SCOMM_PHENOMENA.items():
            payload.append(
                {
                    "connector_id": connector_id,
                    "source_label": meta["source_label"],
                    "label": meta["label"],
                    "notation": meta["notation"],
                    "pollutant_label": meta["pollutant_label"],
                }
            )
        results = upsert_phenomena_via_rpc(self.public, payload)
        ids_by_source_label = {
            source_label: int(row["phenomenon_id"])
            for source_label, row in results.items()
        }
        ids_by_pollutant: Dict[str, int] = {}
        for pollutant, meta in SCOMM_PHENOMENA.items():
            phen_id = ids_by_source_label.get(meta["source_label"])
            if phen_id:
                ids_by_pollutant[pollutant] = phen_id
        return ids_by_pollutant

    def backfill_timeseries_phenomena(
        self, connector_id: int, service_ref: str, phenomenon_ids: Dict[str, int]
    ) -> int:
        resp = (
            self.core.table("timeseries")
            .select("id,timeseries_ref")
            .eq("connector_id", connector_id)
            .eq("service_ref", str(service_ref))
            .is_("phenomenon_id", "null")
            .execute()
        )
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        ids_by_pollutant: Dict[str, List[int]] = {"pm10": [], "pm2.5": []}
        for row in rows or []:
            ts_ref = str(row.get("timeseries_ref") or "")
            ts_ref_lower = ts_ref.lower()
            pollutant = None
            if ts_ref_lower.endswith(":pm10"):
                pollutant = "pm10"
            elif ts_ref_lower.endswith(":pm2.5"):
                pollutant = "pm2.5"
            if not pollutant:
                continue
            row_id = row.get("id")
            if row_id is None:
                continue
            ids_by_pollutant[pollutant].append(int(row_id))

        total_updated = 0
        for pollutant, ids in ids_by_pollutant.items():
            phen_id = phenomenon_ids.get(pollutant)
            if not phen_id or not ids:
                continue
            self.core.table("timeseries").update(
                {"phenomenon_id": phen_id}
            ).in_("id", ids).execute()
            total_updated += len(ids)
        return total_updated

    def fetch_station_ids(
        self, connector_id: int, service_ref: str, station_refs: Iterable[str]
    ) -> Dict[str, int]:
        refs = [str(ref) for ref in station_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        for chunk in chunked(refs, 200):
            resp = (
                self.core.table("stations")
                .select("id,station_ref")
                .eq("connector_id", connector_id)
                .eq("service_ref", str(service_ref))
                .in_("station_ref", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                mapping[str(row["station_ref"])] = int(row["id"])
        return mapping

    def upsert_timeseries(
        self,
        timeseries_rows: Iterable[Dict[str, Any]],
    ) -> None:
        rows = list(timeseries_rows)
        if rows:
            self.core.table("timeseries").upsert(
                rows, on_conflict="connector_id,service_ref,timeseries_ref"
            ).execute()

    def fetch_timeseries_ids(
        self, connector_id: int, service_ref: str, timeseries_refs: Iterable[str]
    ) -> Dict[str, int]:
        refs = [str(ref) for ref in timeseries_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        for chunk in chunked(refs, 200):
            resp = (
                self.core.table("timeseries")
                .select("id,timeseries_ref")
                .eq("connector_id", connector_id)
                .eq("service_ref", str(service_ref))
                .in_("timeseries_ref", list(chunk))
                .execute()
            )
            rows = resp.data if hasattr(resp, "data") else resp.get("data")
            for row in rows or []:
                mapping[str(row["timeseries_ref"])] = int(row["id"])
        return mapping

    def upsert_observations(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = list(rows)
        if not payload:
            return 0
        self.core.table("observations").upsert(
            payload, on_conflict="timeseries_id,observed_at"
        ).execute()
        return len(payload)


def chunked(values: List[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = 200
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def normalize_station_payload(station: Dict[str, Any]) -> Dict[str, Any]:
    location = station.get("location") if isinstance(station.get("location"), dict) else {}
    sensor = station.get("sensor") if isinstance(station.get("sensor"), dict) else {}
    sensor_type = station.get("sensor_type") if isinstance(station.get("sensor_type"), dict) else {}
    lat = location.get("latitude")
    lon = location.get("longitude")
    station_stub = {
        "properties": {
            "latitude": lat,
            "longitude": lon,
        }
    }
    lon_val, lat_val = station_coords(station_stub, bbox=UK_BBOX)
    station_ref = sensor.get("id") or station.get("sensor_id") or station.get("id")
    label = location.get("name") or station.get("location_name")
    station_type = sensor_type.get("name") or sensor_type.get("id")
    station_exposure = _station_exposure(location)
    return {
        "station_ref": str(station_ref) if station_ref is not None else None,
        "label": label,
        "station_name": label,
        "station_type": station_type,
        "station_exposure": station_exposure,
        "longitude": lon_val,
        "latitude": lat_val,
    }


def _station_exposure(location: Dict[str, Any]) -> Optional[str]:
    indoor = location.get("indoor")
    if indoor is None:
        return None
    if isinstance(indoor, bool):
        return "indoor" if indoor else "outdoor"
    if isinstance(indoor, (int, float)):
        if indoor == 1:
            return "indoor"
        if indoor == 0:
            return "outdoor"
        return None
    if isinstance(indoor, str):
        value = indoor.strip().lower()
        if value in {"1", "true", "yes", "y"}:
            return "indoor"
        if value in {"0", "false", "no", "n"}:
            return "outdoor"
    return None


def station_stub(station: Dict[str, Any]) -> Dict[str, Any]:
    location = station.get("location") if isinstance(station.get("location"), dict) else {}
    return {
        "properties": {
            "longitude": location.get("longitude"),
            "latitude": location.get("latitude"),
        }
    }


def _merge_station_row(existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(existing)
    for key, value in candidate.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        merged[key] = value
    return merged


def build_observation_rows(
    station_ref: str,
    record: Dict[str, Any],
    observed_at: datetime,
) -> Tuple[List[Tuple[TimeseriesKey, Optional[float]]], List[str]]:
    values = []
    timeseries_refs = []
    sensor_values = record.get("sensordatavalues")
    if not isinstance(sensor_values, list):
        return [], []
    for entry in sensor_values:
        if not isinstance(entry, dict):
            continue
        value_type = entry.get("value_type")
        mapped = VALUE_TYPE_MAP.get(str(value_type))
        if not mapped:
            continue
        value = coerce_float(entry.get("value"))
        if value is None:
            continue
        pollutant = mapped["pollutant"]
        key = TimeseriesKey(station_ref=station_ref, pollutant=pollutant)
        timeseries_ref = f"{station_ref}:{pollutant}"
        values.append((key, value))
        timeseries_refs.append(timeseries_ref)
    return values, timeseries_refs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Sensor.Community measurements for the UK.")
    parser.add_argument(
        "--refresh-recent",
        action="store_true",
        help="Fetch recent values and upsert observations.",
    )
    parser.add_argument(
        "--raw-output",
        help="Write raw payloads to this file (JSON).",
    )
    parser.add_argument(
        "--raw-dropbox",
        action="store_true",
        help="Upload raw payloads to Dropbox (testing only; gated by allowlist).",
    )
    parser.add_argument(
        "--raw-dropbox-folder",
        help="Dropbox folder path for raw payloads (optional).",
    )
    parser.add_argument(
        "--no-filter",
        action="store_true",
        help="Skip the UK bounding box filter and ingest all stations in the response.",
    )
    parser.add_argument(
        "--log-level",
        default=DEFAULT_LOG_LEVEL,
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _configure_logging(args.log_level, DEFAULT_FILE_LOG_LEVEL)
    if not args.refresh_recent:
        LOG.error("No action specified. Use --refresh-recent.")
        raise SystemExit(2)
    raw_session = _prepare_raw_dropbox_session(args)
    error_logger: Optional[ErrorLogger] = None
    try:
        client = SensorCommunityClient(raw_recorder=raw_session.recorder if raw_session else None)
        payload = client.recent_values()
        if not payload:
            LOG.warning("No sensor values returned from Sensor.Community.")
            return

        filtered = (
            payload
            if args.no_filter
            else [s for s in payload if station_in_bbox_or_missing_coords(station_stub(s), UK_BBOX)]
        )

        if args.raw_output:
            with open(args.raw_output, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "source": SCOMM_BASE_URL,
                        "fetched_at": utcnow().isoformat(),
                        "bbox": None if args.no_filter else UK_BBOX,
                        "count": len(filtered),
                        "stations": filtered,
                    },
                    handle,
                    indent=2,
                )

        writer = SupabaseWriter()
        error_logger = ErrorLogger(writer.client)
        connector_id, overwrite_station_name = writer.upsert_connector()
        service_ref = SCOMM_SERVICE_REF
        phenomenon_ids = writer.upsert_phenomena(connector_id)
        writer.upsert_stations(filtered, connector_id, service_ref, overwrite_station_name)

        station_refs = []
        observations_by_timeseries: Dict[TimeseriesKey, Tuple[Optional[float], datetime]] = {}
        timeseries_refs: List[str] = []
        for record in filtered:
            payload = normalize_station_payload(record)
            station_ref = payload.get("station_ref")
            if not station_ref:
                continue
            station_refs.append(station_ref)
            observed_at = parse_timestamp(record.get("timestamp")) or utcnow()
            values, series_refs = build_observation_rows(station_ref, record, observed_at)
            timeseries_refs.extend(series_refs)
            for key, value in values:
                existing = observations_by_timeseries.get(key)
                if existing is None or existing[1] < observed_at:
                    observations_by_timeseries[key] = (value, observed_at)

        station_id_map = writer.fetch_station_ids(connector_id, service_ref, station_refs)
        timeseries_payload = []
        for key, (value, observed_at) in observations_by_timeseries.items():
            station_id = station_id_map.get(key.station_ref)
            if not station_id:
                continue
            mapped = VALUE_TYPE_MAP.get(
                "P1" if key.pollutant == "pm10" else "P2"
            )
            label = f"{key.station_ref} {mapped['label']}" if mapped else key.pollutant
            timeseries_payload.append(
                {
                    "timeseries_ref": f"{key.station_ref}:{key.pollutant}",
                    "label": label,
                    "uom": mapped["uom"] if mapped else None,
                    "station_id": station_id,
                    "connector_id": connector_id,
                    "service_ref": str(service_ref),
                    "phenomenon_id": phenomenon_ids.get(key.pollutant),
                    "last_value_at": observed_at.isoformat(),
                    "last_value": value,
                }
            )

        writer.upsert_timeseries(timeseries_payload)
        backfilled = writer.backfill_timeseries_phenomena(connector_id, service_ref, phenomenon_ids)
        if backfilled:
            LOG.info("Backfilled phenomenon_id for %s timeseries rows.", backfilled)
        timeseries_id_map = writer.fetch_timeseries_ids(connector_id, service_ref, timeseries_refs)

        observation_rows = []
        for key, (value, observed_at) in observations_by_timeseries.items():
            timeseries_ref = f"{key.station_ref}:{key.pollutant}"
            timeseries_id = timeseries_id_map.get(timeseries_ref)
            if not timeseries_id:
                continue
            observation_rows.append(
                {
                    "timeseries_id": timeseries_id,
                    "observed_at": observed_at.isoformat(),
                    "value": value,
                    "status": None,
                }
            )

        inserted = writer.upsert_observations(observation_rows)
        LOG.info("Upserted %s observations.", inserted)
    except Exception as exc:
        LOG.warning("Unhandled ingest error: %s", exc)
        if error_logger:
            error_logger.log_error(
                source="sensorcommunity_ingest",
                severity="error",
                message="Unhandled ingest error.",
                context={
                    "country": SCOMM_COUNTRY,
                    "no_filter": args.no_filter,
                    "raw_dropbox": bool(args.raw_dropbox),
                },
                exc=exc,
            )
    finally:
        if raw_session:
            raw_session.finalize()


if __name__ == "__main__":
    main()
