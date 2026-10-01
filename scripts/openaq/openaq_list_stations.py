#!/usr/bin/env python3
"""
Fetch OpenAQ locations in the UK bounding box.

Examples:
  python3 scripts/openaq/openaq_list_stations.py
  python3 scripts/openaq/openaq_list_stations.py --format csv --output uk_openaq_stations.csv
  python3 scripts/openaq/openaq_list_stations.py --to-supabase
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

LOG = logging.getLogger("openaq_stations")
DEFAULT_LOG_LEVEL = os.getenv("OPENAQ_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.getLogger("httpx").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))
logging.getLogger("postgrest").setLevel(getattr(logging, DEFAULT_LOG_LEVEL, logging.INFO))


def parse_env_bool(name: str, default: bool) -> bool:
    value = (os.getenv(name) or "").strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def parse_positive_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


OPENAQ_BASE_URL = (os.getenv("OPENAQ_BASE_URL") or "https://api.openaq.org/v3").rstrip("/")
OPENAQ_API_KEY = (os.getenv("OPENAQ_API_KEY") or "").strip()
OPENAQ_CONNECTOR_CODE = os.getenv("OPENAQ_CONNECTOR_CODE") or "openaq"
OPENAQ_SERVICE_REF = os.getenv("OPENAQ_SERVICE_REF") or OPENAQ_CONNECTOR_CODE
OPENAQ_SERVICE_LABEL = os.getenv("OPENAQ_SERVICE_LABEL") or "OpenAQ"
OPENAQ_USER_AGENT = os.getenv("OPENAQ_USER_AGENT") or "uk-air-quality-networks"
OPENAQ_BBOX = os.getenv("OPENAQ_BBOX") or "-8.623555,49.863222,1.763337,60.871222"
OPENAQ_PAGE_LIMIT = int(os.getenv("OPENAQ_PAGE_LIMIT") or "1000")
OPENAQ_MAX_PAGES = int(os.getenv("OPENAQ_MAX_PAGES") or "0")
OPENAQ_RATE_LIMIT_PER_MIN = int(os.getenv("OPENAQ_RATE_LIMIT_PER_MIN") or "60")
OPENAQ_SHARED_BUDGET_ENFORCE = parse_env_bool("OPENAQ_SHARED_BUDGET_ENFORCE", True)
OPENAQ_SHARED_BUDGET_FAIL_OPEN = parse_env_bool("OPENAQ_SHARED_BUDGET_FAIL_OPEN", False)
OPENAQ_SHARED_BUDGET_KEY = (os.getenv("OPENAQ_SHARED_BUDGET_KEY") or OPENAQ_CONNECTOR_CODE).strip() or OPENAQ_CONNECTOR_CODE
OPENAQ_SHARED_BUDGET_CALLER = (os.getenv("OPENAQ_SHARED_BUDGET_CALLER") or "openaq_list_stations").strip() or "openaq_list_stations"
OPENAQ_SHARED_BUDGET_MINUTE_LIMIT = parse_positive_int("OPENAQ_SHARED_BUDGET_MINUTE_LIMIT", 50)
OPENAQ_SHARED_BUDGET_HOUR_LIMIT = parse_positive_int("OPENAQ_SHARED_BUDGET_HOUR_LIMIT", 1900)
OPENAQ_SHARED_BUDGET_WAIT_MAX_SECONDS = 180
OPENAQ_SHARED_BUDGET_WAIT_MAX_ATTEMPTS = 6
SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")

PROVIDER_SHORTNAMES = {
    "London Air Quality Network": "LAQN",
}

UK_BBOX = {
    "west": -8.623555,
    "south": 49.863222,
    "east": 1.763337,
    "north": 60.871222,
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_bbox(value: str) -> Tuple[str, Dict[str, float]]:
    parts = [part.strip() for part in value.split(",") if part.strip()]
    if len(parts) != 4:
        raise ValueError("OPENAQ_BBOX must include west,south,east,north.")
    numbers = [float(part) for part in parts]
    bbox = {
        "west": numbers[0],
        "south": numbers[1],
        "east": numbers[2],
        "north": numbers[3],
    }
    return ",".join(str(num) for num in numbers), bbox


def coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _provider_name(location: Dict[str, Any]) -> Optional[str]:
    provider = location.get("provider") if isinstance(location.get("provider"), dict) else {}
    name = provider.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def _provider_short_name(provider_name: Optional[str]) -> Optional[str]:
    if not provider_name:
        return None
    return PROVIDER_SHORTNAMES.get(provider_name, provider_name)


def _owner_name(location: Dict[str, Any]) -> Optional[str]:
    owner = location.get("owner")
    if isinstance(owner, str):
        return owner.strip() or None
    if isinstance(owner, dict):
        name = owner.get("name")
        if isinstance(name, str):
            return name.strip() or None
    return None


def _normalize_owner_name(owner_name: Optional[str]) -> Optional[str]:
    if not owner_name:
        return None
    trimmed = owner_name.strip()
    if not trimmed:
        return None
    if trimmed.lower().startswith("unknown"):
        return None
    return trimmed


def _station_name(location: Dict[str, Any]) -> Optional[str]:
    name = _location_name(location)
    provider = _provider_short_name(_provider_name(location))
    owner = _normalize_owner_name(_owner_name(location))
    if name and provider:
        base = f"{provider} {name}"
        if owner and owner.strip().lower() == provider.strip().lower():
            return base
        return f"{base} - {owner}" if owner else base
    return name


class OpenAQClient:
    def __init__(
        self,
        base_url: str = OPENAQ_BASE_URL,
        timeout: int = 60,
        retries: int = 3,
        budget_writer: Optional["DbWriter"] = None,
    ) -> None:
        if not OPENAQ_API_KEY:
            raise RuntimeError("OPENAQ_API_KEY is required.")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.last_request_at = 0.0
        self.min_interval_seconds = 0.0
        if OPENAQ_RATE_LIMIT_PER_MIN > 0:
            self.min_interval_seconds = max(0.0, 60.0 / float(OPENAQ_RATE_LIMIT_PER_MIN))
        self.budget_writer = budget_writer
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": OPENAQ_USER_AGENT,
                "Accept": "application/json",
                "X-API-Key": OPENAQ_API_KEY,
            }
        )
        if OPENAQ_SHARED_BUDGET_ENFORCE and not self.budget_writer:
            message = (
                "OPENAQ_SHARED_BUDGET_ENFORCE is true but no SUPABASE_DB_URL-backed "
                "writer is available for token reservation."
            )
            if OPENAQ_SHARED_BUDGET_FAIL_OPEN:
                LOG.warning("%s Continuing fail-open.", message)
            else:
                raise RuntimeError(message)

    def _reserve_shared_budget(self) -> None:
        if not OPENAQ_SHARED_BUDGET_ENFORCE:
            return
        if not self.budget_writer:
            if OPENAQ_SHARED_BUDGET_FAIL_OPEN:
                return
            raise RuntimeError("Shared token budget is enforced but DB writer is unavailable.")
        started_at = time.time()
        wait_attempt = 0
        while True:
            try:
                budget = self.budget_writer.reserve_openaq_token_budget(
                    tokens=1,
                    budget_key=OPENAQ_SHARED_BUDGET_KEY,
                    caller=OPENAQ_SHARED_BUDGET_CALLER,
                    minute_limit=OPENAQ_SHARED_BUDGET_MINUTE_LIMIT,
                    hour_limit=OPENAQ_SHARED_BUDGET_HOUR_LIMIT,
                )
            except Exception as exc:
                if OPENAQ_SHARED_BUDGET_FAIL_OPEN:
                    LOG.warning("OpenAQ shared token budget RPC failed (%s); continuing fail-open.", exc)
                    return
                raise RuntimeError(f"OpenAQ shared token budget RPC failed: {exc}") from exc

            granted = budget.get("granted") is True
            if granted:
                LOG.debug(
                    "OpenAQ shared budget granted minute_remaining=%s hour_remaining=%s",
                    budget.get("minute_remaining"),
                    budget.get("hour_remaining"),
                )
                return

            reason = str(budget.get("reason") or "unknown")
            retry_after = budget.get("retry_after_seconds")
            minute_remaining = budget.get("minute_remaining")
            hour_remaining = budget.get("hour_remaining")
            minute_reset_at = budget.get("minute_reset_at")
            hour_reset_at = budget.get("hour_reset_at")
            message = (
                "OpenAQ shared token budget denied "
                f"(reason={reason}, retry_after_seconds={retry_after}, "
                f"minute_remaining={minute_remaining}, hour_remaining={hour_remaining}, "
                f"minute_reset_at={minute_reset_at}, hour_reset_at={hour_reset_at})"
            )
            if OPENAQ_SHARED_BUDGET_FAIL_OPEN:
                LOG.warning("%s; continuing fail-open.", message)
                return

            wait_seconds = self._shared_budget_wait_seconds(budget)
            elapsed = time.time() - started_at
            remaining_wait_budget = max(0.0, OPENAQ_SHARED_BUDGET_WAIT_MAX_SECONDS - elapsed)
            should_wait = (
                reason in {"minute_limit", "hour_limit"}
                and wait_seconds > 0
                and remaining_wait_budget > 0
                and wait_attempt < OPENAQ_SHARED_BUDGET_WAIT_MAX_ATTEMPTS
            )
            if should_wait:
                sleep_for = min(wait_seconds, remaining_wait_budget)
                wait_attempt += 1
                LOG.warning(
                    "%s; waiting %.1fs before retry (%s/%s).",
                    message,
                    sleep_for,
                    wait_attempt,
                    OPENAQ_SHARED_BUDGET_WAIT_MAX_ATTEMPTS,
                )
                time.sleep(sleep_for)
                continue

            raise RuntimeError(message)

    def _shared_budget_wait_seconds(self, budget: Dict[str, Any]) -> float:
        retry_after = self._coerce_positive_float(budget.get("retry_after_seconds"))
        if retry_after is not None:
            return retry_after
        now = utcnow()
        minute_reset_at = self._coerce_datetime(budget.get("minute_reset_at"))
        hour_reset_at = self._coerce_datetime(budget.get("hour_reset_at"))
        candidates = [dt for dt in (minute_reset_at, hour_reset_at) if dt]
        if not candidates:
            return 0.0
        target = min(candidates)
        delay = (target - now).total_seconds()
        return max(0.0, delay)

    def _coerce_positive_float(self, value: Any) -> Optional[float]:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if parsed > 0 else None

    def _coerce_datetime(self, value: Any) -> Optional[datetime]:
        if not isinstance(value, str):
            return None
        raw = value.strip()
        if not raw:
            return None
        normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _rate_limit_info(self, resp: requests.Response) -> Dict[str, Optional[int]]:
        def to_int(value: Optional[str]) -> Optional[int]:
            if value is None:
                return None
            try:
                return int(value)
            except (TypeError, ValueError):
                return None

        headers = resp.headers
        return {
            "limit": to_int(headers.get("x-ratelimit-limit")),
            "remaining": to_int(headers.get("x-ratelimit-remaining")),
            "reset": to_int(headers.get("x-ratelimit-reset")),
            "used": to_int(headers.get("x-ratelimit-used")),
        }

    def _rate_limit_delay_seconds(self, reset: Optional[int]) -> float:
        if reset is None:
            return 0.0
        if reset > 1e12:
            return max(0.0, reset / 1000.0 - time.time())
        if reset > 1e9:
            return max(0.0, reset - time.time())
        return max(0.0, float(reset))

    def _respect_min_interval(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        elapsed = time.time() - self.last_request_at
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)

    def _maybe_sleep_for_rate_limit(self, resp: requests.Response) -> None:
        info = self._rate_limit_info(resp)
        remaining = info.get("remaining")
        reset = info.get("reset")
        if remaining is None or reset is None:
            return
        if remaining <= 1:
            delay = self._rate_limit_delay_seconds(reset)
            if delay > 0:
                LOG.info(
                    "OpenAQ rate limit low (remaining=%s). Sleeping %.1fs.",
                    remaining,
                    delay,
                )
                time.sleep(delay)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(1, self.retries + 1):
            try:
                self._reserve_shared_budget()
                self._respect_min_interval()
                resp = self.session.get(url, params=params, timeout=self.timeout)
                self.last_request_at = time.time()
                if resp.status_code in (429, 500, 502, 503, 504):
                    if resp.status_code == 429:
                        info = self._rate_limit_info(resp)
                        delay = self._rate_limit_delay_seconds(info.get("reset")) or min(60, 2**attempt)
                        LOG.warning(
                            "OpenAQ rate limit hit (remaining=%s). Sleeping %.1fs.",
                            info.get("remaining"),
                            delay,
                        )
                        time.sleep(delay)
                        continue
                    self._sleep(attempt)
                    continue
                resp.raise_for_status()
                self._maybe_sleep_for_rate_limit(resp)
                return resp.json()
            except requests.RequestException as exc:
                LOG.warning("Request failed (attempt %s/%s): %s", attempt, self.retries, exc)
                if attempt == self.retries:
                    raise
                self._sleep(attempt)
        return []

    def _sleep(self, attempt: int) -> None:
        time.sleep(min(30, 2**attempt))

    def list_locations(self, bbox: str, limit: int, max_pages: Optional[int]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        page = 1
        while True:
            payload = self.get(
                "locations",
                {
                    "bbox": bbox,
                    "limit": min(limit, 1000),
                    "page": page,
                },
            )
            page_results = payload.get("results") if isinstance(payload, dict) else []
            page_rows = page_results if isinstance(page_results, list) else []
            if not page_rows:
                break
            results.extend(page_rows)
            if len(page_rows) < min(limit, 1000):
                break
            page += 1
            if max_pages and page > max_pages:
                break
        return results


class DbWriter:
    def __init__(self, dsn: str) -> None:
        try:
            import psycopg2  # type: ignore
            from psycopg2.extras import execute_values  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "psycopg2 is required for --to-supabase. Install psycopg2-binary."
            ) from exc
        self._psycopg2 = psycopg2
        self._execute_values = execute_values
        self.conn = psycopg2.connect(dsn)

    def close(self) -> None:
        if self.conn:
            self.conn.close()

    def upsert_connector(self) -> Tuple[int, bool]:
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                """
                select id, poll_enabled, overwrite_station_name
                from uk_aq_core.connectors
                where connector_code = %s
                limit 1
                """,
                (OPENAQ_CONNECTOR_CODE,),
            )
            row = cursor.fetchone()
            poll_enabled = bool(row[1]) if row else False
            overwrite_station_name = True
            cursor.execute(
                """
                insert into uk_aq_core.connectors (
                  connector_code,
                  label,
                  display_name,
                  service_url,
                  stations_bbox_supported,
                  timeseries_station_filter_supported,
                  overwrite_station_name,
                  poll_enabled
                )
                values (%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict (connector_code) do update set
                  label = excluded.label,
                  display_name = excluded.display_name,
                  service_url = excluded.service_url,
                  stations_bbox_supported = excluded.stations_bbox_supported,
                  timeseries_station_filter_supported = excluded.timeseries_station_filter_supported,
                  overwrite_station_name = excluded.overwrite_station_name,
                  poll_enabled = excluded.poll_enabled
                """,
                (
                    OPENAQ_CONNECTOR_CODE,
                    OPENAQ_SERVICE_LABEL,
                    OPENAQ_SERVICE_LABEL,
                    OPENAQ_BASE_URL,
                    False,
                    False,
                    overwrite_station_name,
                    poll_enabled,
                ),
            )
            cursor.execute(
                """
                select id, overwrite_station_name
                from uk_aq_core.connectors
                where connector_code = %s
                """,
                (OPENAQ_CONNECTOR_CODE,),
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError("Failed to resolve connector id for OpenAQ.")
            return int(row[0]), bool(row[1])

    def get_poll_enabled(self) -> Optional[bool]:
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                """
                select poll_enabled
                from uk_aq_core.connectors
                where connector_code = %s
                limit 1
                """,
                (OPENAQ_CONNECTOR_CODE,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return bool(row[0])

    def set_poll_enabled(self, poll_enabled: bool) -> None:
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                """
                update uk_aq_core.connectors
                set poll_enabled = %s
                where connector_code = %s
                """,
                (poll_enabled, OPENAQ_CONNECTOR_CODE),
            )

    def upsert_stations(
        self,
        locations: Iterable[Dict[str, Any]],
        connector_id: int,
        service_ref: str,
    ) -> int:
        rows = [_station_row(location, connector_id, service_ref) for location in locations]
        rows = [row for row in rows if row.get("station_ref")]
        if not rows:
            return 0
        owner_by_ref: Dict[str, str] = {}
        for location in locations:
            station_ref = str(location.get("id")) if location.get("id") is not None else None
            if not station_ref:
                continue
            owner = _normalize_owner_name(_owner_name(location))
            if owner:
                owner_by_ref[station_ref] = owner
        values: List[Tuple[Any, ...]] = []
        for row in rows:
            values.append(
                (
                    row.get("station_ref"),
                    row.get("service_ref"),
                    row.get("label"),
                    row.get("station_name"),
                    row.get("station_type"),
                    row.get("region"),
                    row.get("geometry"),
                    row.get("connector_id"),
                    row.get("last_seen_at"),
                    row.get("removed_at"),
                )
            )
        insert_sql = """
            insert into uk_aq_core.stations (
              station_ref,
              service_ref,
              label,
              station_name,
              station_type,
              region,
              geometry,
              connector_id,
              last_seen_at,
              removed_at
            )
            values %s
            on conflict (connector_id, service_ref, station_ref) do update set
              label = excluded.label,
              station_name = excluded.station_name,
              station_type = excluded.station_type,
              region = excluded.region,
              geometry = excluded.geometry,
              last_seen_at = excluded.last_seen_at,
              removed_at = excluded.removed_at
        """
        template = (
            "(%s,%s,%s,%s,%s,%s,"
            "ST_GeogFromText(%s),"
            "%s,%s,%s)"
        )
        with self.conn, self.conn.cursor() as cursor:
            self._execute_values(cursor, insert_sql, values, template=template)
            if owner_by_ref:
                station_refs = list(owner_by_ref.keys())
                cursor.execute(
                    """
                    select id, station_ref
                    from uk_aq_core.stations
                    where connector_id = %s
                      and service_ref = %s
                      and station_ref = any(%s)
                    """,
                    (connector_id, service_ref, station_refs),
                )
                id_map = {str(row[1]): int(row[0]) for row in cursor.fetchall()}
                metadata_rows = []
                for station_ref, owner in owner_by_ref.items():
                    station_id = id_map.get(station_ref)
                    if not station_id:
                        continue
                    metadata_rows.append(
                        (station_id, json.dumps({"openaq_owner": owner}), utcnow())
                    )
                if metadata_rows:
                    cursor.executemany(
                        """
                        insert into uk_aq_core.station_metadata (station_id, attributes, updated_at)
                        values (%s, %s::jsonb, %s)
                        on conflict (station_id) do update set
                          attributes = uk_aq_core.station_metadata.attributes || excluded.attributes,
                          updated_at = excluded.updated_at
                        """,
                        metadata_rows,
                    )
        return len(rows)

    def fetch_station_ids_by_ref(
        self,
        connector_id: int,
        service_ref: str,
        station_refs: Iterable[str],
    ) -> Dict[str, int]:
        refs = [str(ref) for ref in station_refs if ref]
        if not refs:
            return {}
        mapping: Dict[str, int] = {}
        with self.conn, self.conn.cursor() as cursor:
            for chunk in chunked(refs, 200):
                cursor.execute(
                    """
                    select id, station_ref
                    from uk_aq_core.stations
                    where connector_id = %s
                      and service_ref = %s
                      and station_ref = any(%s)
                    """,
                    (connector_id, service_ref, chunk),
                )
                for row in cursor.fetchall():
                    mapping[str(row[1])] = int(row[0])
        return mapping

    def upsert_phenomena(
        self,
        connector_id: int,
        parameters: Dict[str, Dict[str, Optional[str]]],
    ) -> Dict[str, int]:
        if not parameters:
            return {}
        payload: List[Dict[str, Any]] = []
        for meta in parameters.values():
            name = str(meta.get("name") or "").strip()
            if not name:
                continue
            display_name = meta.get("display_name")
            payload.append(
                {
                    "connector_id": connector_id,
                    "source_label": f"openaq:{name}",
                    "label": display_name or name,
                    "notation": display_name or name,
                    "pollutant_label": name,
                }
            )
        if not payload:
            return {}
        source_labels = [str(row["source_label"]) for row in payload]
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                """
                select source_label, phenomenon_id, mapping_warning
                from uk_aq_public.uk_aq_rpc_phenomena_upsert(%s::jsonb)
                """,
                (json.dumps(payload),),
            )
            rpc_rows = cursor.fetchall()
        ids_by_source_label = {
            str(row[0]): int(row[1])
            for row in rpc_rows
            if row[0] is not None and row[1] is not None
        }
        warnings = [
            f"{row[0]}:{row[2]}" for row in rpc_rows if row[2] is not None
        ]
        missing = sorted(set(source_labels) - set(ids_by_source_label))
        if warnings or missing:
            raise RuntimeError(
                "Central phenomena RPC validation failed: "
                + "; ".join(warnings + [f"missing={','.join(missing)}"])
            )
        ids_by_name: Dict[str, int] = {}
        for meta in parameters.values():
            name = str(meta.get("name") or "").strip()
            if not name:
                continue
            phen_id = ids_by_source_label.get(f"openaq:{name}")
            if phen_id:
                ids_by_name[name] = phen_id
        return ids_by_name

    def upsert_timeseries(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = [row for row in rows if row.get("timeseries_ref")]
        if not payload:
            return 0
        values: List[Tuple[Any, ...]] = []
        for row in payload:
            values.append(
                (
                    row.get("timeseries_ref"),
                    row.get("label"),
                    row.get("uom"),
                    row.get("station_id"),
                    row.get("connector_id"),
                    row.get("service_ref"),
                    row.get("phenomenon_id"),
                )
            )
        insert_sql = """
            insert into uk_aq_core.timeseries (
              timeseries_ref,
              label,
              uom,
              station_id,
              connector_id,
              service_ref,
              phenomenon_id
            )
            values %s
            on conflict (connector_id, service_ref, timeseries_ref) do update set
              label = excluded.label,
              uom = excluded.uom,
              station_id = excluded.station_id,
              phenomenon_id = excluded.phenomenon_id
        """
        with self.conn, self.conn.cursor() as cursor:
            self._execute_values(cursor, insert_sql, values)
        return len(values)

    def reserve_openaq_token_budget(
        self,
        tokens: int,
        budget_key: str,
        caller: str,
        minute_limit: int,
        hour_limit: int,
    ) -> Dict[str, Any]:
        safe_tokens = max(0, int(tokens))
        safe_minute_limit = max(1, int(minute_limit))
        safe_hour_limit = max(1, int(hour_limit))
        safe_key = budget_key.strip() or OPENAQ_CONNECTOR_CODE
        safe_caller = caller.strip() or "openaq_list_stations"
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                """
                select *
                from uk_aq_public.uk_aq_rpc_openaq_token_budget_reserve(
                  p_budget_key => %s,
                  p_tokens => %s,
                  p_minute_limit => %s,
                  p_hour_limit => %s,
                  p_caller => %s
                )
                """,
                (
                    safe_key,
                    safe_tokens,
                    safe_minute_limit,
                    safe_hour_limit,
                    safe_caller,
                ),
            )
            row = cursor.fetchone()
            if row is None or cursor.description is None:
                raise RuntimeError("Shared token budget RPC returned no row.")
            columns = [col[0] for col in cursor.description]
        return {columns[idx]: row[idx] for idx in range(len(columns))}


def normalize_location(location: Dict[str, Any]) -> Dict[str, Any]:
    location_id = location.get("id")
    station_ref = str(location_id) if location_id is not None else None
    name = _location_name(location)
    station_name = _station_name(location)
    provider_name = _provider_name(location)
    coords = location.get("coordinates") if isinstance(location.get("coordinates"), dict) else {}
    longitude = coerce_float(coords.get("longitude"))
    latitude = coerce_float(coords.get("latitude"))
    sensors = location.get("sensors") if isinstance(location.get("sensors"), list) else []
    parameters = sorted(
        {
            str(sensor.get("parameter", {}).get("name")).strip()
            for sensor in sensors
            if isinstance(sensor, dict) and sensor.get("parameter")
        }
        - {"", "None"}
    )
    country = location.get("country") if isinstance(location.get("country"), dict) else {}
    owner_name = _normalize_owner_name(_owner_name(location))
    datetime_first = location.get("datetimeFirst") if isinstance(location.get("datetimeFirst"), dict) else {}
    datetime_last = location.get("datetimeLast") if isinstance(location.get("datetimeLast"), dict) else {}
    return {
        "station_ref": station_ref,
        "label": name or (f"OpenAQ {station_ref}" if station_ref else None),
        "station_name": station_name,
        "station_type": "mobile" if location.get("isMobile") else "fixed",
        "region": location.get("locality") or country.get("name"),
        "longitude": longitude,
        "latitude": latitude,
        "country_code": country.get("code"),
        "country_name": country.get("name"),
        "provider": provider_name,
        "owner": owner_name,
        "is_monitor": location.get("isMonitor"),
        "is_mobile": location.get("isMobile"),
        "sensor_parameters": ",".join(parameters),
        "sensors_count": len(sensors),
        "datetime_first_utc": datetime_first.get("utc"),
        "datetime_last_utc": datetime_last.get("utc"),
    }


def _location_name(location: Dict[str, Any]) -> Optional[str]:
    name = location.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    locality = location.get("locality")
    if isinstance(locality, str) and locality.strip():
        return locality.strip()
    return None


def _collect_parameters(
    locations: Iterable[Dict[str, Any]],
) -> Dict[str, Dict[str, Optional[str]]]:
    parameters: Dict[str, Dict[str, Optional[str]]] = {}
    for location in locations:
        sensors = location.get("sensors") if isinstance(location.get("sensors"), list) else []
        for sensor in sensors:
            if not isinstance(sensor, dict):
                continue
            parameter = sensor.get("parameter") if isinstance(sensor.get("parameter"), dict) else {}
            raw_name = parameter.get("name")
            name = str(raw_name).strip() if raw_name is not None else ""
            if not name:
                continue
            if name not in parameters:
                display_name = parameter.get("displayName")
                units = parameter.get("units")
                parameters[name] = {
                    "name": name,
                    "display_name": str(display_name).strip() if display_name else None,
                    "units": str(units).strip() if units else None,
                }
    return parameters


def _collect_timeseries_rows(
    locations: Iterable[Dict[str, Any]],
    connector_id: int,
    service_ref: str,
    station_id_by_ref: Dict[str, int],
    phenomenon_id_by_name: Dict[str, int],
) -> List[Dict[str, Any]]:
    rows_by_ref: Dict[str, Dict[str, Any]] = {}
    for location in locations:
        location_id = location.get("id")
        station_ref = str(location_id) if location_id is not None else None
        if not station_ref:
            continue
        station_id = station_id_by_ref.get(station_ref)
        if not station_id:
            continue
        sensors = location.get("sensors") if isinstance(location.get("sensors"), list) else []
        for sensor in sensors:
            if not isinstance(sensor, dict):
                continue
            sensor_id = sensor.get("id")
            if sensor_id is None:
                continue
            parameter = sensor.get("parameter") if isinstance(sensor.get("parameter"), dict) else {}
            raw_name = parameter.get("name")
            name = str(raw_name).strip() if raw_name is not None else ""
            if not name:
                continue
            phenomenon_id = phenomenon_id_by_name.get(name)
            if not phenomenon_id:
                continue
            display_name = parameter.get("displayName")
            units = parameter.get("units")
            timeseries_ref = str(sensor_id)
            rows_by_ref[timeseries_ref] = {
                "timeseries_ref": timeseries_ref,
                "label": f"{station_ref} {str(display_name).strip() if display_name else name}",
                "uom": str(units).strip() if units else None,
                "station_id": station_id,
                "connector_id": connector_id,
                "service_ref": str(service_ref),
                "phenomenon_id": phenomenon_id,
            }
    return list(rows_by_ref.values())


def _station_row(location: Dict[str, Any], connector_id: int, service_ref: str) -> Dict[str, Any]:
    location_id = location.get("id")
    station_ref = str(location_id) if location_id is not None else None
    name = _location_name(location)
    station_name = _station_name(location)
    coords = location.get("coordinates") if isinstance(location.get("coordinates"), dict) else {}
    longitude = coerce_float(coords.get("longitude"))
    latitude = coerce_float(coords.get("latitude"))
    geometry = None
    if longitude is not None and latitude is not None:
        geometry = f"SRID=4326;POINT({longitude} {latitude})"
    datetime_last = location.get("datetimeLast") if isinstance(location.get("datetimeLast"), dict) else {}
    last_seen_at = datetime_last.get("utc") or utcnow().isoformat()
    country = location.get("country") if isinstance(location.get("country"), dict) else {}
    return {
        "station_ref": station_ref,
        "service_ref": str(service_ref),
        "label": name or (f"OpenAQ {station_ref}" if station_ref else None),
        "station_name": station_name,
        "station_type": "mobile" if location.get("isMobile") else "fixed",
        "region": location.get("locality") or country.get("name"),
        "geometry": geometry,
        "connector_id": connector_id,
        "last_seen_at": last_seen_at,
        "removed_at": None,
    }


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")


def _write_csv(path: str, rows: Iterable[Dict[str, Any]]) -> None:
    rows = list(rows)
    fieldnames = [
        "station_ref",
        "label",
        "station_name",
        "station_type",
        "region",
        "longitude",
        "latitude",
        "country_code",
        "country_name",
        "provider",
        "owner",
        "is_monitor",
        "is_mobile",
        "sensor_parameters",
        "sensors_count",
        "datetime_first_utc",
        "datetime_last_utc",
    ]
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch OpenAQ locations for the UK.")
    parser.add_argument(
        "--output",
        default="openaq_stations.json",
        help="Output file path (default: openaq_stations.json).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (json or csv).",
    )
    parser.add_argument(
        "--bbox",
        default=OPENAQ_BBOX,
        help="Override bbox as west,south,east,north (default: OPENAQ_BBOX).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=OPENAQ_PAGE_LIMIT,
        help="OpenAQ page size (default: OPENAQ_PAGE_LIMIT).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=OPENAQ_MAX_PAGES,
        help="Stop after this many pages (0 = no limit).",
    )
    parser.add_argument(
        "--raw-output",
        help="Write raw location payloads to this file (JSON only).",
    )
    parser.add_argument(
        "--to-supabase",
        action="store_true",
        help="Upsert stations into Supabase (requires SUPABASE_DB_URL).",
    )
    parser.add_argument(
        "--toggle-polling",
        action="store_true",
        help="Temporarily disable connector polling while the script runs (requires --to-supabase).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_at = utcnow()
    bbox_str, bbox_map = parse_bbox(args.bbox)

    writer = None
    original_poll_enabled = None
    needs_db_writer = bool(args.to_supabase or OPENAQ_SHARED_BUDGET_ENFORCE)
    if needs_db_writer:
        if not SUPABASE_DB_URL:
            if OPENAQ_SHARED_BUDGET_ENFORCE and not OPENAQ_SHARED_BUDGET_FAIL_OPEN:
                raise RuntimeError(
                    "SUPABASE_DB_URL (or DATABASE_URL) is required when OPENAQ_SHARED_BUDGET_ENFORCE is true."
                )
            if args.to_supabase:
                raise RuntimeError("SUPABASE_DB_URL (or DATABASE_URL) is required for --to-supabase.")
        else:
            writer = DbWriter(SUPABASE_DB_URL)
    if args.to_supabase:
        if not writer:
            raise RuntimeError("SUPABASE_DB_URL (or DATABASE_URL) is required for --to-supabase.")
        if args.toggle_polling:
            original_poll_enabled = writer.get_poll_enabled()
            if original_poll_enabled is None:
                LOG.warning("OpenAQ connector not found; skipping poll_enabled toggle.")
            else:
                writer.set_poll_enabled(False)
                LOG.info("Disabled OpenAQ polling while listing stations.")

    try:
        client = OpenAQClient(budget_writer=writer)
        locations = client.list_locations(bbox_str, args.limit, args.max_pages or None)
        LOG.info("Fetched %s locations from OpenAQ.", len(locations))

        normalized = [normalize_location(location) for location in locations]
        missing_coords = sum(
            1 for row in normalized if row.get("longitude") is None or row.get("latitude") is None
        )
        LOG.info("Locations missing coords=%s", missing_coords)

        if args.to_supabase:
            connector_id, _ = writer.upsert_connector()
            stations_upserted = writer.upsert_stations(
                locations,
                connector_id,
                OPENAQ_SERVICE_REF,
            )
            station_refs = [
                str(location.get("id"))
                for location in locations
                if location.get("id") is not None
            ]
            station_id_by_ref = writer.fetch_station_ids_by_ref(
                connector_id,
                OPENAQ_SERVICE_REF,
                station_refs,
            )
            parameters = _collect_parameters(locations)
            phenomenon_id_by_name = writer.upsert_phenomena(connector_id, parameters)
            timeseries_rows = _collect_timeseries_rows(
                locations,
                connector_id,
                OPENAQ_SERVICE_REF,
                station_id_by_ref,
                phenomenon_id_by_name,
            )
            timeseries_upserted = writer.upsert_timeseries(timeseries_rows)
            LOG.info(
                "Upserted %s stations, %s phenomena, %s timeseries into Supabase.",
                stations_upserted,
                len(phenomenon_id_by_name),
                timeseries_upserted,
            )

        if args.format == "csv":
            _write_csv(args.output, normalized)
        else:
            if args.raw_output:
                raw_payload = {
                    "source": OPENAQ_BASE_URL,
                    "fetched_at": run_at.isoformat(),
                    "bbox": bbox_map,
                    "count": len(locations),
                    "locations": locations,
                }
                _write_json(args.raw_output, raw_payload)
            payload = {
                "source": OPENAQ_BASE_URL,
                "fetched_at": run_at.isoformat(),
                "bbox": bbox_map,
                "count": len(normalized),
                "connector_code": OPENAQ_CONNECTOR_CODE,
                "service_ref": OPENAQ_SERVICE_REF,
                "stations": normalized,
            }
            _write_json(args.output, payload)
        LOG.info("Wrote %s", args.output)
    finally:
        if writer and args.toggle_polling and original_poll_enabled is not None:
            writer.set_poll_enabled(original_poll_enabled)
            LOG.info("Restored OpenAQ polling to %s.", original_poll_enabled)
        if writer:
            writer.close()


def chunked(values: List[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = 200
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


if __name__ == "__main__":
    main()
