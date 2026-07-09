#!/usr/bin/env python3
"""
Compare DEFRA last-hour readings with Supabase observations for a station.

Fetches the DEFRA "last hour" page for a station, parses pollutant rows,
normalizes names/units, and compares them with the latest observations stored
in Supabase. Exits non-zero when mismatches exceed the tolerance so the script
can be used as a test.

Environment:
- SUPABASE_URL
- SB_SECRET_KEY

Examples:
  python3 scripts/sos/sos_compare.py
  python3 scripts/sos/sos_compare.py --station-id BR11 --tolerance 1.5
  python3 scripts/sos/sos_compare.py --defra-url "https://uk-air.defra.gov.uk/data/site-data?f_site_id=BR11&view=last_hour"
"""

import argparse
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from supabase import Client

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client

load_dotenv()

DEFAULT_DEFRA_URL = "https://uk-air.defra.gov.uk/data/site-data?f_site_id=BR11&view=last_hour"
UK_TZ = ZoneInfo("Europe/London")

POLLUTANT_ALIASES = {
    "nitrogen dioxide": "no2",
    "no2": "no2",
    "ozone": "o3",
    "o3": "o3",
    "pm10": "pm10",
    "pm 10": "pm10",
    "particulate matter 10": "pm10",
    "pm2.5": "pm2.5",
    "pm 2.5": "pm2.5",
    "pm25": "pm2.5",
    "particulate matter 2.5": "pm2.5",
    "sulphur dioxide": "so2",
    "sulfur dioxide": "so2",
    "so2": "so2",
    "carbon monoxide": "co",
    "co": "co",
}

UNIT_ALIASES = {
    "µg/m³": "ug/m3",
    "μg/m³": "ug/m3",
    "ug/m3": "ug/m3",
    "ug/m³": "ug/m3",
    "µg/m3": "ug/m3",
}

HEADER_ALIASES = {
    "pollutant": "pollutant",
    "parameter": "pollutant",
    "name": "pollutant",
    "value": "value",
    "reading": "value",
    "concentration": "value",
    "measurement": "value",
    "units": "units",
    "unit": "units",
    "date": "date",
    "time": "time",
    "timestamp": "timestamp",
}


@dataclass
class PollutantRow:
    key: str
    label: str
    value: Optional[float]
    units: Optional[str]
    timestamp: Optional[datetime]


class DefraTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: List[List[List[str]]] = []
        self._current_table: Optional[List[List[str]]] = None
        self._current_row: Optional[List[str]] = None
        self._current_cell: List[str] = []
        self._in_cell = False

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        if tag == "table":
            self._current_table = []
        elif tag == "tr" and self._current_table is not None:
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._in_cell = True
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._current_row is not None:
            text = " ".join(self._current_cell).strip()
            self._current_row.append(text)
            self._current_cell = []
            self._in_cell = False
        elif tag == "tr" and self._current_table is not None and self._current_row is not None:
            if any(cell.strip() for cell in self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._current_table is not None:
            if self._current_table:
                self.tables.append(self._current_table)
            self._current_table = None


def normalize_header(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.lower()).strip()
    if "timestamp" in cleaned or "date/time" in cleaned or ("date" in cleaned and "time" in cleaned):
        return "timestamp"
    for key, mapped in HEADER_ALIASES.items():
        if key in cleaned:
            return mapped
    return cleaned


def normalize_units(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.strip().lower().replace("³", "3")
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = re.sub(r"\([^)]*\)", "", cleaned)
    cleaned = cleaned.replace("μ", "u").replace("µ", "u")
    cleaned = re.sub(r"\s+", "", cleaned)
    return UNIT_ALIASES.get(value.strip(), UNIT_ALIASES.get(cleaned, cleaned))


def normalize_pollutant(value: str) -> str:
    cleaned = re.sub(r"\([^)]*\)", "", value)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
    cleaned = cleaned.replace("/", " ")
    cleaned = cleaned.replace("-", " ")
    cleaned = cleaned.replace("µ", "u")
    cleaned = cleaned.replace("μ", "u")
    cleaned = cleaned.replace("\u00b5", "u")
    cleaned = cleaned.replace("\u03bc", "u")
    cleaned = cleaned.replace("pm 2.5", "pm2.5").replace("pm 10", "pm10")
    cleaned = cleaned.replace("pm25", "pm2.5")
    cleaned = cleaned.strip()
    if "pm10" in cleaned:
        return "pm10"
    if "pm2.5" in cleaned or "pm25" in cleaned:
        return "pm2.5"
    return POLLUTANT_ALIASES.get(cleaned, cleaned.replace(" ", ""))


def parse_float(value: str) -> Optional[float]:
    if not value:
        return None
    cleaned = value.strip()
    if cleaned in {"-", "--", "n/a", "na"}:
        return None
    cleaned = cleaned.replace(",", "")
    match = re.search(r"-?\d+(?:\.\d+)?", cleaned)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    formats = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d %b %Y %H:%M",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(cleaned, fmt)
            return parsed.replace(tzinfo=UK_TZ).astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UK_TZ)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def round_to_hour(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    rounded = value.replace(minute=0, second=0, microsecond=0)
    if value.minute >= 30:
        rounded += timedelta(hours=1)
    return rounded


def parse_defra_table(html_text: str) -> List[PollutantRow]:
    parser = DefraTableParser()
    parser.feed(html_text)
    for table in parser.tables:
        header_row_index = None
        headers: List[str] = []
        for idx, row in enumerate(table):
            normalized = [normalize_header(cell) for cell in row]
            if "pollutant" in normalized and "value" in normalized:
                header_row_index = idx
                headers = normalized
                break
        if header_row_index is None:
            continue
        rows = []
        for row in table[header_row_index + 1 :]:
            if not row or len(row) < len(headers):
                continue
            record = {headers[i]: row[i] for i in range(min(len(headers), len(row)))}
            pollutant = record.get("pollutant")
            if not pollutant:
                continue
            value = parse_float(record.get("value", ""))
            units = normalize_units(record.get("units"))
            timestamp = parse_timestamp(record.get("timestamp"))
            if timestamp is None:
                date_value = record.get("date")
                time_value = record.get("time")
                if date_value and time_value:
                    timestamp = parse_timestamp(f"{date_value} {time_value}")
            key = normalize_pollutant(pollutant)
            rows.append(PollutantRow(key=key, label=pollutant, value=value, units=units, timestamp=timestamp))
        if rows:
            return rows
    return []


def fetch_defra_rows(url: str, timeout: int = 30) -> List[PollutantRow]:
    headers = {
        "User-Agent": "Mozilla/5.0 (sos_compare)"
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    rows = parse_defra_table(resp.text)
    if not rows:
        raise RuntimeError("No pollutant rows found in DEFRA response.")
    return rows


def build_supabase_client() -> Client:
    client = create_supabase_client()
    schemas = SupabaseSchemas.from_client(client)
    return schemas.core


def load_station(client: Client, station_ref: str) -> Dict[str, str]:
    resp = (
        client.table("stations")
        .select("id,label,station_ref,connector_id,service_ref")
        .eq("station_ref", station_ref)
        .execute()
    )
    data = resp.data if hasattr(resp, "data") else resp.get("data")
    if not data:
        raise RuntimeError(f"Station {station_ref} not found in Supabase.")
    return data[0]


def load_timeseries(client: Client, station_id: int) -> List[Dict[str, str]]:
    resp = (
        client.table("timeseries")
        .select("id,label,uom,phenomenon_id")
        .eq("station_id", station_id)
        .execute()
    )
    return resp.data if hasattr(resp, "data") else resp.get("data") or []


def load_phenomena(client: Client, phenomenon_ids: Iterable[int]) -> Dict[int, Dict[str, str]]:
    ids = [pid for pid in phenomenon_ids if pid is not None]
    if not ids:
        return {}
    resp = client.table("phenomena").select("id,label,notation,pollutant_label").in_("id", ids).execute()
    data = resp.data if hasattr(resp, "data") else resp.get("data")
    return {int(row["id"]): row for row in data or []}


def load_latest_observation(client: Client, timeseries_id: int) -> Optional[Dict[str, str]]:
    resp = (
        client.table("observations")
        .select("observed_at,value")
        .eq("timeseries_id", timeseries_id)
        .order("observed_at", desc=True)
        .limit(1)
        .execute()
    )
    data = resp.data if hasattr(resp, "data") else resp.get("data")
    if not data:
        return None
    return data[0]


def normalize_timeseries_key(timeseries: Dict[str, str], phenomenon: Optional[Dict[str, str]]) -> str:
    label_candidates = [
        phenomenon.get("notation") if phenomenon else None,
        phenomenon.get("pollutant_label") if phenomenon else None,
        phenomenon.get("label") if phenomenon else None,
        timeseries.get("label"),
    ]
    for label in label_candidates:
        if label:
            return normalize_pollutant(label)
    return normalize_pollutant(timeseries.get("label") or "unknown")


def build_db_map(client: Client, station_id: int) -> Dict[str, Dict[str, Optional[str]]]:
    timeseries = load_timeseries(client, station_id)
    phenomena = load_phenomena(client, {ts.get("phenomenon_id") for ts in timeseries})
    db_map: Dict[str, Dict[str, Optional[str]]] = {}
    for ts in timeseries:
        phenomenon = phenomena.get(ts.get("phenomenon_id")) if ts.get("phenomenon_id") else None
        key = normalize_timeseries_key(ts, phenomenon)
        observation = load_latest_observation(client, ts["id"])
        if observation is None:
            continue
        observed_at = parse_timestamp(observation.get("observed_at"))
        entry = {
            "timeseries_id": ts["id"],
            "label": ts.get("label"),
            "uom": normalize_units(ts.get("uom")),
            "value": observation.get("value"),
            "observed_at": observed_at,
        }
        existing = db_map.get(key)
        if existing is None:
            db_map[key] = entry
            continue
        existing_time = existing.get("observed_at")
        if existing_time is None or (observed_at and existing_time and observed_at > existing_time):
            db_map[key] = entry
    return db_map


def compare_values(
    defra_rows: List[PollutantRow],
    db_map: Dict[str, Dict[str, Optional[str]]],
    tolerance: float,
) -> List[str]:
    failures: List[str] = []
    defra_map = {row.key: row for row in defra_rows}
    keys = sorted(set(defra_map.keys()) | set(db_map.keys()))
    for key in keys:
        defra_row = defra_map.get(key)
        db_row = db_map.get(key)
        status = "PASS"
        reasons = []
        defra_value = defra_row.value if defra_row else None
        db_value = db_row.get("value") if db_row else None
        defra_unit = defra_row.units if defra_row else None
        db_unit = db_row.get("uom") if db_row else None
        defra_ts = round_to_hour(defra_row.timestamp if defra_row else None)
        db_ts = round_to_hour(db_row.get("observed_at") if db_row else None)

        if defra_row is None:
            status = "FAIL"
            reasons.append("missing DEFRA")
        if db_row is None:
            status = "FAIL"
            reasons.append("missing DB")
        if defra_unit and db_unit and defra_unit != db_unit:
            status = "FAIL"
            reasons.append(f"unit mismatch {defra_unit} vs {db_unit}")
        if defra_ts and db_ts and defra_ts != db_ts:
            status = "FAIL"
            reasons.append(f"timestamp mismatch {defra_ts.isoformat()} vs {db_ts.isoformat()}")
        if defra_value is None or db_value is None:
            if defra_value is None:
                reasons.append("missing DEFRA value")
                status = "FAIL"
            if db_value is None:
                reasons.append("missing DB value")
                status = "FAIL"
        else:
            try:
                delta = abs(float(defra_value) - float(db_value))
                if delta > tolerance:
                    status = "FAIL"
                    reasons.append(f"delta {delta:.3f} > {tolerance}")
            except (TypeError, ValueError):
                status = "FAIL"
                reasons.append("non-numeric value")

        defra_value_str = "n/a" if defra_value is None else f"{defra_value}"
        db_value_str = "n/a" if db_value is None else f"{db_value}"
        defra_ts_str = defra_ts.isoformat() if defra_ts else "n/a"
        db_ts_str = db_ts.isoformat() if db_ts else "n/a"
        print(
            f"[{status}] {key}: DEFRA={defra_value_str} {defra_unit or ''} @ {defra_ts_str} | "
            f"DB={db_value_str} {db_unit or ''} @ {db_ts_str}"
            f"{' (' + '; '.join(reasons) + ')' if reasons else ''}"
        )
        if status == "FAIL":
            failures.append(key)
    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare DEFRA last-hour data with Supabase observations.")
    parser.add_argument("--station-id", default="BR11", help="Station reference (default: BR11)")
    parser.add_argument(
        "--defra-url",
        default=DEFAULT_DEFRA_URL,
        help="DEFRA last-hour URL (default: BR11 last-hour page)",
    )
    parser.add_argument("--tolerance", type=float, default=1.0, help="Numeric tolerance for value comparison")
    parser.add_argument("--verbose", action="store_true", help="Print debug info")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        defra_rows = fetch_defra_rows(args.defra_url)
    except Exception as exc:
        print(f"Failed to fetch DEFRA data: {exc}", file=sys.stderr)
        return 2

    if args.verbose:
        print(f"Fetched {len(defra_rows)} DEFRA pollutant rows.")

    try:
        client = build_supabase_client()
        station = load_station(client, args.station_id)
    except Exception as exc:
        print(f"Failed to load station from Supabase: {exc}", file=sys.stderr)
        return 2

    if args.verbose:
        print(f"Station {station['station_ref']}: {station.get('label')}")

    db_map = build_db_map(client, station["id"])
    if args.verbose:
        print(f"Loaded {len(db_map)} DB pollutant series.")

    failures = compare_values(defra_rows, db_map, args.tolerance)
    if failures:
        print(f"Mismatches found: {', '.join(failures)}")
        return 1
    print("All pollutants matched within tolerance.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
