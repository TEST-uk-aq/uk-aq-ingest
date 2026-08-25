#!/usr/bin/env python3
"""
Long-term Air Quality England (AQE) public-site cadence probe.

Purpose:
- Discover the current active AQE station inventory from AQE_metadata.RData.
- Poll each active station's public latest-data HTML page once per hour.
- Spread stations deterministically across 60 one-minute shards.
- Record source update timestamps and a fingerprint of the Latest Data table in SQLite.

This is an observational probe only. It does not write to UK AQ production/test
ingestion tables and it does not merge AQE stations with stations from other
networks.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import html
import json
import math
import re
import sqlite3
import sys
import time
import warnings
from contextlib import contextmanager
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


AQE_METADATA_URL = (
    "https://www.airqualityengland.co.uk/assets/openair/R_data/AQE_metadata.RData"
)
AQE_LATEST_URL = "https://www.airqualityengland.co.uk/site/latest"
USER_AGENT = "UK-AQ-AQE-Cadence-Probe/0.1 (+https://ukaq.co.uk/)"
SHARD_COUNT = 60
SHARD_SECONDS = 60
DEFAULT_DELAY_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_METADATA_MAX_AGE_HOURS = 24.0
DEFAULT_DATA_DIR = Path.home() / "Library" / "Application Support" / "UK AQ" / "aqe-probe"


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS stations (
    site_id TEXT PRIMARY KEY,
    site_name TEXT,
    location_type TEXT,
    latitude REAL,
    longitude REAL,
    local_authority TEXT,
    parameters_json TEXT NOT NULL,
    shard INTEGER NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    first_seen_at_utc TEXT NOT NULL,
    metadata_seen_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stations_active_shard
    ON stations(is_active, shard, site_id);

CREATE TABLE IF NOT EXISTS probe_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    mode TEXT NOT NULL,
    shard INTEGER,
    station_count INTEGER NOT NULL DEFAULT 0,
    http_success_count INTEGER NOT NULL DEFAULT 0,
    parse_success_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS probes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER,
    probe_time_utc TEXT NOT NULL,
    site_id TEXT NOT NULL,
    shard INTEGER NOT NULL,
    http_status INTEGER,
    elapsed_ms INTEGER,
    response_bytes INTEGER,
    source_updated_raw TEXT,
    source_updated_at_utc TEXT,
    source_age_minutes REAL,
    source_status TEXT,
    latest_parameter_count INTEGER,
    latest_data_sha256 TEXT,
    parse_ok INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    FOREIGN KEY(run_id) REFERENCES probe_runs(id),
    FOREIGN KEY(site_id) REFERENCES stations(site_id)
);

CREATE INDEX IF NOT EXISTS idx_probes_site_time
    ON probes(site_id, probe_time_utc);

CREATE INDEX IF NOT EXISTS idx_probes_time
    ON probes(probe_time_utc);

CREATE INDEX IF NOT EXISTS idx_probes_source_updated
    ON probes(site_id, source_updated_at_utc);
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def normalise_space(value: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def stable_shard(site_id: str) -> int:
    digest = hashlib.sha256(site_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") % SHARD_COUNT


def auto_shard(now: float | None = None) -> int:
    if now is None:
        now = time.time()
    return (int(now) // SHARD_SECONDS) % SHARD_COUNT


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if bool(value != value):  # NaN, including numpy scalars.
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "<na>", "none"}:
        return None
    return text


def clean_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


class LatestPageParser(HTMLParser):
    """Collect visible text and table rows from an AQE latest-data page."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.text_parts: list[str] = []
        self.tables: list[list[list[str]]] = []
        self._table_depth = 0
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "table":
            self._table_depth += 1
            if self._table_depth == 1:
                self._table = []
        elif tag == "tr" and self._table_depth == 1:
            self._row = []
        elif tag in {"td", "th"} and self._table_depth == 1 and self._row is not None:
            self._cell_parts = []
        elif tag == "br":
            self.text_parts.append(" ")
            if self._cell_parts is not None:
                self._cell_parts.append(" ")

    def handle_data(self, data: str) -> None:
        self.text_parts.append(data)
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._cell_parts is not None and self._row is not None:
            self._row.append(normalise_space(" ".join(self._cell_parts)))
            self._cell_parts = None
        elif tag == "tr" and self._table_depth == 1 and self._row is not None:
            if any(cell for cell in self._row):
                assert self._table is not None
                self._table.append(self._row)
            self._row = None
            self._cell_parts = None
        elif tag == "table" and self._table_depth:
            if self._table_depth == 1 and self._table is not None:
                self.tables.append(self._table)
                self._table = None
            self._table_depth -= 1


SOURCE_UPDATED_RE = re.compile(
    r"This monitoring site data was last updated:\s*"
    r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})",
    re.IGNORECASE,
)
NO_CURRENT_DATA_RE = re.compile(
    r"This monitoring site data was last updated:\s*"
    r"Sorry,\s*no current data available\.\s*"
    r"The last data date time is:\s*"
    r"(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})",
    re.IGNORECASE,
)


def parse_latest_page(
    body: str,
) -> tuple[str | None, str | None, list[dict[str, str]], str | None]:
    parser = LatestPageParser()
    parser.feed(body)
    plain_text = normalise_space(" ".join(parser.text_parts))

    source_updated_raw: str | None = None
    source_status: str | None = None

    offline_match = NO_CURRENT_DATA_RE.search(plain_text)
    if offline_match:
        source_updated_raw = f"{offline_match.group(1)} {offline_match.group(2)}"
        source_status = "no_current_data"
    else:
        match = SOURCE_UPDATED_RE.search(plain_text)
        if match:
            source_updated_raw = f"{match.group(1)} {match.group(2)}"
            source_status = "current"

    latest_rows: list[dict[str, str]] = []
    for table in parser.tables:
        header_index: int | None = None
        for idx, row in enumerate(table):
            lowered = [normalise_space(cell).lower() for cell in row]
            joined = " | ".join(lowered)
            if (
                "parameter" in joined
                and "concentration" in joined
                and "period" in joined
            ):
                header_index = idx
                break

        if header_index is None:
            continue

        for row in table[header_index + 1 :]:
            if len(row) < 4:
                continue
            latest_rows.append(
                {
                    "parameter": row[0],
                    "pollution_band": row[1],
                    "concentration": row[2],
                    "period": row[3],
                }
            )
        if latest_rows:
            break

    parse_error = None
    if source_updated_raw is None:
        parse_error = "source update timestamp not found"

    return source_updated_raw, source_status, latest_rows, parse_error


def source_timestamp_to_utc(raw: str | None) -> datetime | None:
    if not raw:
        return None
    # AQE explicitly presents its observations as GMT hour ending. Treat the
    # displayed clock time as UTC all year, rather than local BST.
    value = datetime.strptime(raw, "%d/%m/%Y %H:%M")
    return value.replace(tzinfo=timezone.utc)


def make_session() -> requests.Session:
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/octet-stream;q=0.9,*/*;q=0.5",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(SCHEMA_SQL)
    return conn


@contextmanager
def single_instance_lock(path: Path) -> Iterable[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"Another AQE probe process already holds {path}; exiting.", file=sys.stderr)
            raise SystemExit(0)
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def metadata_needs_refresh(metadata_path: Path, max_age_hours: float) -> bool:
    if not metadata_path.exists():
        return True
    age_seconds = time.time() - metadata_path.stat().st_mtime
    return age_seconds >= max_age_hours * 3600


def download_metadata(
    session: requests.Session,
    metadata_path: Path,
    timeout_seconds: float,
) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = metadata_path.with_suffix(metadata_path.suffix + ".tmp")

    response = session.get(AQE_METADATA_URL, timeout=timeout_seconds)
    response.raise_for_status()
    if len(response.content) < 1000:
        raise RuntimeError(
            f"AQE metadata response unexpectedly small: {len(response.content)} bytes"
        )

    tmp_path.write_bytes(response.content)
    tmp_path.replace(metadata_path)


def load_active_metadata(metadata_path: Path) -> dict[str, dict[str, Any]]:
    try:
        import rdata
    except ImportError as exc:
        raise RuntimeError(
            "The 'rdata' package is required for AQE metadata. "
            "Install scripts/aqe/requirements.txt in the probe venv."
        ) from exc

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        objects = rdata.read_rda(metadata_path)

    if "metadata" not in objects:
        raise RuntimeError(
            f"AQE metadata object 'metadata' not found; objects={list(objects.keys())}"
        )

    frame = objects["metadata"]
    required = {
        "site_id",
        "site_name",
        "location_type",
        "latitude",
        "longitude",
        "parameter",
        "end_date",
        "local_authority",
    }
    missing = sorted(required - set(map(str, frame.columns)))
    if missing:
        raise RuntimeError(f"AQE metadata is missing expected columns: {missing}")

    sites: dict[str, dict[str, Any]] = {}
    for _, row in frame.iterrows():
        end_date = (clean_text(row["end_date"]) or "").lower()
        if end_date != "ongoing":
            continue

        site_id = clean_text(row["site_id"])
        if not site_id:
            continue

        site = sites.setdefault(
            site_id,
            {
                "site_id": site_id,
                "site_name": clean_text(row["site_name"]),
                "location_type": clean_text(row["location_type"]),
                "latitude": clean_float(row["latitude"]),
                "longitude": clean_float(row["longitude"]),
                "local_authority": clean_text(row["local_authority"]),
                "parameters": set(),
            },
        )
        parameter = clean_text(row["parameter"])
        if parameter:
            site["parameters"].add(parameter)

    for site in sites.values():
        site["parameters"] = sorted(site["parameters"])

    return sites


def sync_stations(
    conn: sqlite3.Connection,
    sites: dict[str, dict[str, Any]],
    seen_at: datetime,
) -> None:
    seen = iso_utc(seen_at)
    conn.execute("UPDATE stations SET is_active = 0")

    for site_id in sorted(sites):
        site = sites[site_id]
        conn.execute(
            """
            INSERT INTO stations (
                site_id, site_name, location_type, latitude, longitude,
                local_authority, parameters_json, shard, is_active,
                first_seen_at_utc, metadata_seen_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(site_id) DO UPDATE SET
                site_name = excluded.site_name,
                location_type = excluded.location_type,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                local_authority = excluded.local_authority,
                parameters_json = excluded.parameters_json,
                shard = excluded.shard,
                is_active = 1,
                metadata_seen_at_utc = excluded.metadata_seen_at_utc
            """,
            (
                site_id,
                site["site_name"],
                site["location_type"],
                site["latitude"],
                site["longitude"],
                site["local_authority"],
                json.dumps(site["parameters"], separators=(",", ":")),
                stable_shard(site_id),
                seen,
                seen,
            ),
        )
    conn.commit()


def ensure_station_inventory(
    conn: sqlite3.Connection,
    session: requests.Session,
    metadata_path: Path,
    max_age_hours: float,
    timeout_seconds: float,
    force_refresh: bool = False,
) -> tuple[int, bool]:
    active_count = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE is_active = 1"
    ).fetchone()[0]

    refresh = force_refresh or metadata_needs_refresh(metadata_path, max_age_hours)
    if not refresh and active_count > 0:
        return int(active_count), False

    if refresh:
        download_metadata(session, metadata_path, timeout_seconds)

    sites = load_active_metadata(metadata_path)
    if not sites:
        raise RuntimeError("AQE metadata contained no active sites; refusing to empty inventory")

    sync_stations(conn, sites, utc_now())
    return len(sites), True


def stations_for_mode(
    conn: sqlite3.Connection,
    all_stations: bool,
    shard: int | None,
    site_ids: list[str],
) -> list[sqlite3.Row]:
    if site_ids:
        placeholders = ",".join("?" for _ in site_ids)
        rows = conn.execute(
            f"""
            SELECT * FROM stations
            WHERE is_active = 1 AND site_id IN ({placeholders})
            ORDER BY site_id
            """,
            site_ids,
        ).fetchall()
        found = {row["site_id"] for row in rows}
        missing = [site_id for site_id in site_ids if site_id not in found]
        if missing:
            raise RuntimeError(f"Requested active AQE site(s) not found: {', '.join(missing)}")
        return rows

    if all_stations:
        return conn.execute(
            "SELECT * FROM stations WHERE is_active = 1 ORDER BY site_id"
        ).fetchall()

    assert shard is not None
    return conn.execute(
        """
        SELECT * FROM stations
        WHERE is_active = 1 AND shard = ?
        ORDER BY site_id
        """,
        (shard,),
    ).fetchall()


def save_failure_html(data_dir: Path, site_id: str, probe_at: datetime, body: str) -> Path:
    failures_dir = data_dir / "failures"
    failures_dir.mkdir(parents=True, exist_ok=True)
    stamp = probe_at.strftime("%Y%m%dT%H%M%SZ")
    path = failures_dir / f"{stamp}_{site_id}.html"
    path.write_text(body, encoding="utf-8", errors="replace")
    return path


def start_run(
    conn: sqlite3.Connection,
    mode: str,
    shard: int | None,
    station_count: int,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO probe_runs(started_at_utc, mode, shard, station_count)
        VALUES (?, ?, ?, ?)
        """,
        (iso_utc(utc_now()), mode, shard, station_count),
    )
    conn.commit()
    return int(cursor.lastrowid)


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    http_success_count: int,
    parse_success_count: int,
    error_count: int,
) -> None:
    conn.execute(
        """
        UPDATE probe_runs
        SET finished_at_utc = ?,
            http_success_count = ?,
            parse_success_count = ?,
            error_count = ?
        WHERE id = ?
        """,
        (
            iso_utc(utc_now()),
            http_success_count,
            parse_success_count,
            error_count,
            run_id,
        ),
    )
    conn.commit()


def probe_station(
    conn: sqlite3.Connection,
    session: requests.Session,
    run_id: int,
    station: sqlite3.Row,
    timeout_seconds: float,
    data_dir: Path,
    save_parse_failures: bool,
) -> tuple[bool, bool, str | None]:
    site_id = station["site_id"]
    probe_at = utc_now()
    query = urlencode({"site_id": site_id})
    url = f"{AQE_LATEST_URL}?{query}"

    http_status: int | None = None
    elapsed_ms: int | None = None
    response_bytes: int | None = None
    source_updated_raw: str | None = None
    source_updated_at: datetime | None = None
    source_age_minutes: float | None = None
    source_status: str | None = None
    latest_parameter_count: int | None = None
    latest_data_sha256: str | None = None
    parse_ok = False
    error_text: str | None = None
    http_ok = False
    body: str | None = None

    started = time.monotonic()
    try:
        response = session.get(url, timeout=timeout_seconds)
        elapsed_ms = round((time.monotonic() - started) * 1000)
        http_status = response.status_code
        response_bytes = len(response.content)
        body = response.text
        response.raise_for_status()
        http_ok = True

        source_updated_raw, source_status, latest_rows, parse_error = parse_latest_page(body)
        source_updated_at = source_timestamp_to_utc(source_updated_raw)

        if source_updated_at is not None:
            source_age_minutes = round(
                (probe_at - source_updated_at).total_seconds() / 60.0,
                2,
            )
            parse_ok = True

        if latest_rows:
            latest_parameter_count = len(latest_rows)
            latest_data_canonical = json.dumps(
                latest_rows,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            latest_data_sha256 = hashlib.sha256(
                latest_data_canonical.encode("utf-8")
            ).hexdigest()

        if parse_error:
            error_text = parse_error
            if save_parse_failures and body:
                failure_path = save_failure_html(data_dir, site_id, probe_at, body)
                error_text = f"{error_text}; saved={failure_path}"

    except requests.RequestException as exc:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        error_text = f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        error_text = f"{type(exc).__name__}: {exc}"
        if save_parse_failures and body:
            failure_path = save_failure_html(data_dir, site_id, probe_at, body)
            error_text = f"{error_text}; saved={failure_path}"

    conn.execute(
        """
        INSERT INTO probes (
            run_id, probe_time_utc, site_id, shard, http_status, elapsed_ms,
            response_bytes, source_updated_raw, source_updated_at_utc,
            source_age_minutes, source_status, latest_parameter_count,
            latest_data_sha256, parse_ok, error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            iso_utc(probe_at),
            site_id,
            int(station["shard"]),
            http_status,
            elapsed_ms,
            response_bytes,
            source_updated_raw,
            iso_utc(source_updated_at) if source_updated_at else None,
            source_age_minutes,
            source_status,
            latest_parameter_count,
            latest_data_sha256,
            1 if parse_ok else 0,
            error_text,
        ),
    )
    conn.commit()

    return http_ok, parse_ok, error_text


def print_inventory(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT shard, COUNT(*) AS station_count
        FROM stations
        WHERE is_active = 1
        GROUP BY shard
        ORDER BY shard
        """
    ).fetchall()
    total = sum(int(row["station_count"]) for row in rows)
    print(f"Active AQE stations: {total}")
    for row in rows:
        print(f"  shard {row['shard']:2d}: {row['station_count']:3d}")


def print_summary(conn: sqlite3.Connection) -> None:
    active_count = conn.execute(
        "SELECT COUNT(*) FROM stations WHERE is_active = 1"
    ).fetchone()[0]
    probe_count = conn.execute("SELECT COUNT(*) FROM probes").fetchone()[0]
    run_count = conn.execute("SELECT COUNT(*) FROM probe_runs").fetchone()[0]
    first_probe, last_probe = conn.execute(
        "SELECT MIN(probe_time_utc), MAX(probe_time_utc) FROM probes"
    ).fetchone()
    parse_failures = conn.execute(
        "SELECT COUNT(*) FROM probes WHERE parse_ok = 0"
    ).fetchone()[0]
    bytes_total = conn.execute(
        "SELECT COALESCE(SUM(response_bytes), 0) FROM probes"
    ).fetchone()[0]

    print(f"Database: active_stations={active_count:,} runs={run_count:,} probes={probe_count:,}")
    print(f"Probe window: {first_probe or '-'} -> {last_probe or '-'}")
    print(f"Parse failures: {parse_failures:,}")
    print(f"Downloaded response bytes recorded: {int(bytes_total):,}")

    if probe_count:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS n,
                AVG(source_age_minutes) AS avg_age,
                MIN(source_age_minutes) AS min_age,
                MAX(source_age_minutes) AS max_age
            FROM probes
            WHERE parse_ok = 1 AND source_age_minutes IS NOT NULL
            """
        ).fetchone()
        if row["n"]:
            print(
                "Source age minutes: "
                f"avg={row['avg_age']:.1f} min={row['min_age']:.1f} max={row['max_age']:.1f}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe public AQE latest-data pages and record update cadence in SQLite."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Local, non-repo state directory (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="SQLite database path. Defaults to <data-dir>/aqe_probe.sqlite3",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Probe all active AQE stations once.",
    )
    parser.add_argument(
        "--shard",
        type=int,
        choices=range(SHARD_COUNT),
        metavar="0-59",
        help="Probe one explicit one-minute shard.",
    )
    parser.add_argument(
        "--site",
        action="append",
        default=[],
        help="Probe one site ID; may be supplied more than once.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help=f"Seconds to wait between station requests (default: {DEFAULT_DELAY_SECONDS}).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Per-request timeout seconds (default: {DEFAULT_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--metadata-max-age-hours",
        type=float,
        default=DEFAULT_METADATA_MAX_AGE_HOURS,
        help=(
            "Refresh AQE metadata when the local copy is at least this old "
            f"(default: {DEFAULT_METADATA_MAX_AGE_HOURS})."
        ),
    )
    parser.add_argument(
        "--refresh-metadata",
        action="store_true",
        help="Force an AQE metadata download before probing.",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Refresh/synchronise station metadata and exit without probing.",
    )
    parser.add_argument(
        "--inventory",
        action="store_true",
        help="Show active station counts by shard and exit.",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show a compact summary of the probe database and exit.",
    )
    parser.add_argument(
        "--no-save-parse-failures",
        action="store_true",
        help="Do not save raw HTML when an HTTP 200 page cannot be parsed.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress per-station success output; errors and final summary remain.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.delay < 0:
        raise SystemExit("--delay must be >= 0")
    if args.timeout <= 0:
        raise SystemExit("--timeout must be > 0")
    if args.metadata_max_age_hours < 0:
        raise SystemExit("--metadata-max-age-hours must be >= 0")

    mode_count = sum(
        bool(value)
        for value in (
            args.all,
            args.shard is not None,
            bool(args.site),
        )
    )
    if mode_count > 1:
        raise SystemExit("Use only one of --all, --shard, or --site.")

    data_dir = args.data_dir.expanduser().resolve()
    db_path = (args.db or (data_dir / "aqe_probe.sqlite3")).expanduser().resolve()
    metadata_path = data_dir / "AQE_metadata.RData"
    lock_path = data_dir / "aqe_probe.lock"

    data_dir.mkdir(parents=True, exist_ok=True)

    with single_instance_lock(lock_path):
        conn = connect_db(db_path)
        session = make_session()
        try:
            if args.summary:
                print_summary(conn)
                return 0

            count, refreshed = ensure_station_inventory(
                conn=conn,
                session=session,
                metadata_path=metadata_path,
                max_age_hours=args.metadata_max_age_hours,
                timeout_seconds=args.timeout,
                force_refresh=args.refresh_metadata,
            )
            if refreshed:
                print(f"AQE metadata refreshed; active stations={count}")

            if args.metadata_only:
                return 0

            if args.inventory:
                print_inventory(conn)
                return 0

            shard: int | None = None
            if not args.all and not args.site:
                shard = args.shard if args.shard is not None else auto_shard()

            stations = stations_for_mode(
                conn=conn,
                all_stations=args.all,
                shard=shard,
                site_ids=args.site,
            )

            if args.site:
                mode = "site"
            elif args.all:
                mode = "all"
            elif args.shard is not None:
                mode = "explicit_shard"
            else:
                mode = "auto_shard"

            run_id = start_run(conn, mode, shard, len(stations))
            http_success_count = 0
            parse_success_count = 0
            error_count = 0

            for index, station in enumerate(stations):
                http_ok, parse_ok, error_text = probe_station(
                    conn=conn,
                    session=session,
                    run_id=run_id,
                    station=station,
                    timeout_seconds=args.timeout,
                    data_dir=data_dir,
                    save_parse_failures=not args.no_save_parse_failures,
                )
                http_success_count += int(http_ok)
                parse_success_count += int(parse_ok)
                if error_text:
                    error_count += 1

                if not args.quiet or error_text:
                    latest = conn.execute(
                        """
                        SELECT source_updated_raw, source_age_minutes, http_status
                        FROM probes
                        WHERE run_id = ? AND site_id = ?
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (run_id, station["site_id"]),
                    ).fetchone()
                    print(
                        f"{station['site_id']}: "
                        f"http={latest['http_status']} "
                        f"updated={latest['source_updated_raw'] or '-'} "
                        f"age_min={latest['source_age_minutes'] if latest['source_age_minutes'] is not None else '-'}"
                        + (f" error={error_text}" if error_text else "")
                    )

                if index + 1 < len(stations) and args.delay:
                    time.sleep(args.delay)

            finish_run(
                conn,
                run_id,
                http_success_count=http_success_count,
                parse_success_count=parse_success_count,
                error_count=error_count,
            )

            print(
                f"Run {run_id}: mode={mode} shard={shard if shard is not None else '-'} "
                f"stations={len(stations)} http_ok={http_success_count} "
                f"parsed={parse_success_count} errors={error_count}"
            )
            return 0
        finally:
            session.close()
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
