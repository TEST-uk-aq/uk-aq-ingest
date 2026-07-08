#!/usr/bin/env python3
"""
Download the UK-AIR monitoring sites CSV from the search results page.

Requires:
- UK_AIR_SOS_SITE_SEARCH_URL (if --search-url is not provided)
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client

LOG = logging.getLogger("uk_air_sos_site_register")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token"
DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload"

# Default match_type for pollutant rules is "contains"; use (match_type, value) tuples if needed.
NETWORK_POLLUTANT_RULES = {
    "Automatic Urban and Rural Monitoring Network (AURN)": [
        "nitrogen dioxide",
        "nitrogen oxides",
        "nitrogen monoxide",
        "ozone",
        "sulphur dioxide",
        "sulfur dioxide",
        "carbon monoxide",
        "pm10",
        "pm2.5",
        "particulate matter less than 10",
        "particulate matter less than 2.5",
        "particulate matter under 2.5",
    ],
    "UK Urban NO2 Network": ["nitrogen dioxide", "no2"],
    "UKEAP: Rural NO2": ["nitrogen dioxide", "no2"],
    "UKEAP: Acid Gases & Aerosol Network": [
        "nitric acid",
        "nitrous acid",
        "hno3",
        "hono",
        "sulphur dioxide",
        "sulfur dioxide",
        "so2",
        "nitrogen dioxide",
        "no2",
        "nitrate",
        "no3",
        "sulphate",
        "sulfate",
        "so4",
        "chloride",
        "cl",
        "calcium",
        "ca",
        "magnesium",
        "mg",
        "sodium",
        "na",
    ],
    "UKEAP: National Ammonia Monitoring Network": ["ammonia", "ammonium", "nh3", "nh4"],
    "UKEAP: Precip-Net": [
        "calcium",
        "ca",
        "magnesium",
        "mg",
        "sodium",
        "na",
        "potassium",
        "k",
        "ammonium",
        "nh4",
        "sulphate",
        "sulfate",
        "so4",
        "chloride",
        "cl",
        "nitrate",
        "no3",
    ],
    "Black Carbon": ["black carbon", "black_carbon"],
    "Heavy Metals": [
        "arsenic",
        "cadmium",
        "cobalt",
        "chromium",
        "copper",
        "iron",
        "manganese",
        "nickel",
        "lead",
        "selenium",
        "vanadium",
        "zinc",
        "aluminium",
        "aluminum",
        "barium",
        "beryllium",
        "caesium",
        "cesium",
        "lithium",
        "molybdenum",
        "rubidium",
        "antimony",
        "scandium",
        "tin",
        "strontium",
        "titanium",
        "uranium",
        "tungsten",
        "mercury",
    ],
    "Non-Automatic Hydrocarbon Network": ["benzene"],
    "Automatic Hydrocarbon Network": [
        "benzene",
        "toluene",
        "ethyl benzene",
        "ethylbenzene",
        "xylene",
        "butadiene",
        "butene",
        "pentene",
        "trimethylbenzene",
        "ethane",
        "ethene",
        "ethylene",
        "propane",
        "butane",
        "pentane",
        "methylbutane",
        "methylpentane",
        "methylpropane",
        "trimethylpentane",
    ],
    "PAH Digitel (solid phase)": [
        "benzo",
        "pyrene",
        "fluoranthene",
        "anthracene",
        "chrysene",
        "phenanthrene",
        "fluorene",
        "naphthalene",
        "acenaph",
        "indeno",
        "dibenz",
        "coronene",
        "perylene",
    ],
    "PAH Deposition": [
        "benzo",
        "pyrene",
        "fluoranthene",
        "anthracene",
        "chrysene",
        "phenanthrene",
        "fluorene",
        "naphthalene",
        "acenaph",
        "indeno",
        "dibenz",
        "coronene",
        "perylene",
    ],
    "Particle Concentrations and Numbers Network": [
        "particle number",
        "size distribution",
        "elemental carbon",
        "organic carbon",
        "speciation",
    ],
    "Particle Size Composition": ["particle size", "size distribution", "speciation"],
    "TOMPs": ["dioxin", "dibenzofuran", "pcdd", "pcdf", "furan"],
    "Rural Automatic Mercury network": [
        "mercury",
        "reactive mercury",
        "elemental mercury",
        "total gaseous mercury",
    ],
    "Ozone / UV": ["ozone", "o3", "uv"],
}


class CsvLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: List[Tuple[str, str]] = []
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        if tag != "a":
            return
        href = None
        for name, value in attrs:
            if name == "href":
                href = value
                break
        if href:
            self._current_href = href
            self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._current_href is None:
            return
        text = " ".join(self._current_text).strip()
        self.links.append((self._current_href, text))
        self._current_href = None
        self._current_text = []


def _find_csv_link(html_text: str, base_url: str) -> Optional[str]:
    parser = CsvLinkParser()
    parser.feed(html_text)

    for href, text in parser.links:
        if "download" in text.lower() and "csv" in text.lower():
            return urljoin(base_url, href)
    for href, _text in parser.links:
        if re.search(r"\.csv($|\\?)", href, flags=re.IGNORECASE):
            return urljoin(base_url, href)
    for href, _text in parser.links:
        if "csv" in href.lower() and "download" in href.lower():
            return urljoin(base_url, href)
    return None


def _write_csv_summary(content: bytes) -> Tuple[int, List[str]]:
    text = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    return len(rows), reader.fieldnames or []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download the UK-AIR monitoring sites CSV from the search page.",
    )
    parser.add_argument(
        "--search-url",
        default=os.getenv("UK_AIR_SOS_SITE_SEARCH_URL") or os.getenv("GOV_UK_AURN_SITE_SEARCH_URL"),
        help="Full URL to the UK-AIR search results page (with filters applied).",
    )
    parser.add_argument(
        "--csv-url",
        help="Direct CSV URL (skip HTML parsing).",
    )
    parser.add_argument(
        "--output",
        default="uk_air_sos_site_register.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Load the CSV into Supabase after download.",
    )
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Load a local CSV into Supabase without downloading.",
    )
    parser.add_argument(
        "--csv-path",
        help="CSV path for --load-only (required).",
    )
    parser.add_argument(
        "--source-url",
        help="Source search URL stored in the register table.",
    )
    parser.add_argument(
        "--source-file",
        help="Override source file label stored in the register table.",
    )
    parser.add_argument(
        "--snapshot-at",
        help="Snapshot timestamp (ISO format, default: now UTC).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per upsert batch (default: 500).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse the CSV but do not write to Supabase.",
    )
    parser.add_argument(
        "--dropbox-upload",
        action="store_true",
        help="Upload the CSV to Dropbox.",
    )
    parser.add_argument(
        "--dropbox-dir",
        default="network_info/uk_air_sos",
        help="Dropbox folder relative to UK_AQ_DROPBOX_ROOT.",
    )
    parser.add_argument(
        "--save-html",
        help="Optional path to save the search page HTML for debugging.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--user-agent",
        default=os.getenv(
            "UK_AIR_SOS_SITE_SEARCH_USER_AGENT",
            "Mozilla/5.0 (uk_air_sos_site_register)",
        ),
        help="Custom User-Agent string.",
    )
    return parser.parse_args()


def _dropbox_refresh_access_token() -> str:
    app_key = os.getenv("DROPBOX_APP_KEY", "").strip()
    app_secret = os.getenv("DROPBOX_APP_SECRET", "").strip()
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN", "").strip()
    if not (app_key and app_secret and refresh_token):
        raise RuntimeError("Dropbox credentials are required.")
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": app_key,
        "client_secret": app_secret,
    }
    resp = requests.post(DROPBOX_TOKEN_URL, data=payload, timeout=30)
    if resp.status_code >= 400:
        raise RuntimeError(f"Dropbox token request failed ({resp.status_code}): {resp.text}")
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Dropbox token response missing access_token.")
    return token


def _dropbox_upload_file(access_token: str, local_path: str, dropbox_path: str) -> None:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps(
            {
                "path": dropbox_path,
                "mode": "overwrite",
                "autorename": False,
                "mute": False,
            }
        ),
        "Content-Type": "application/octet-stream",
    }
    with open(local_path, "rb") as handle:
        resp = requests.post(DROPBOX_UPLOAD_URL, headers=headers, data=handle, timeout=120)
    if resp.status_code >= 400:
        raise RuntimeError(f"Dropbox upload failed ({resp.status_code}): {resp.text}")


def _normalize_dropbox_path(path: str) -> str:
    cleaned = (path or "").strip()
    if not cleaned:
        return ""
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned.rstrip("/")


def _dropbox_root_folder() -> str:
    return _normalize_dropbox_path(os.getenv("UK_AQ_DROPBOX_ROOT", ""))


def _join_dropbox_paths(root: str, subdir: str) -> str:
    root_clean = _normalize_dropbox_path(root)
    sub_clean = _normalize_dropbox_path(subdir).lstrip("/")
    if not root_clean:
        return f"/{sub_clean}" if sub_clean else ""
    if not sub_clean:
        return root_clean
    return f"{root_clean}/{sub_clean}"


def _timestamped_filename(filename: str) -> str:
    base, ext = os.path.splitext(filename)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{base}_{stamp}{ext}"


def _clean_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _parse_float(value: Optional[str]) -> Optional[float]:
    cleaned = _clean_str(value)
    if cleaned is None:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_date(value: Optional[str]) -> Optional[str]:
    cleaned = _clean_str(value)
    if cleaned is None:
        return None
    try:
        return datetime.strptime(cleaned, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _parse_networks(value: Optional[str]) -> List[str]:
    cleaned = _clean_str(value)
    if cleaned is None:
        return []
    return [item.strip() for item in cleaned.split(";") if item.strip()]


def _build_client() -> SupabaseSchemas:
    client = create_supabase_client()
    return SupabaseSchemas.from_client(client)


def _fetch_existing_networks(client) -> Dict[str, Dict[str, Any]]:
    resp = client.table("uk_air_sos_networks").select(
        "network_ref,network_code,network_display_name"
    ).execute()
    rows = resp.data if hasattr(resp, "data") else resp.get("data")
    existing = {}
    for row in rows or []:
        ref = row.get("network_ref")
        if ref:
            existing[str(ref)] = row
    return existing


def _build_network_pollutant_rows(network_refs: Iterable[str]) -> Tuple[List[Dict[str, str]], Set[str]]:
    rows: List[Dict[str, str]] = []
    missing: Set[str] = set()
    for ref in network_refs:
        rules = NETWORK_POLLUTANT_RULES.get(ref)
        if not rules:
            missing.add(ref)
            continue
        for rule in rules:
            if isinstance(rule, tuple):
                match_type, value = rule
            else:
                match_type, value = "contains", rule
            if not value:
                continue
            rows.append(
                {
                    "network_ref": ref,
                    "match_type": match_type,
                    "match_value": str(value),
                }
            )
    return rows, missing


def _upsert_batches(
    client,
    table: str,
    rows: List[Dict[str, Any]],
    batch_size: int,
    on_conflict: str,
) -> None:
    if not rows:
        return
    for idx in range(0, len(rows), batch_size):
        chunk = rows[idx : idx + batch_size]
        print(".", end="", flush=True)
        client.table(table).upsert(chunk, on_conflict=on_conflict).execute()
    print()


def _upsert_network_pollutants(
    client,
    network_refs: Iterable[str],
    batch_size: int,
) -> None:
    rows, missing = _build_network_pollutant_rows(network_refs)
    if not rows:
        LOG.warning("No network pollutant rules found for current register.")
        return
    _upsert_batches(
        client,
        "uk_air_sos_network_pollutants",
        rows,
        batch_size=batch_size,
        on_conflict="network_ref,match_type,match_value",
    )
    LOG.info("Upserted network pollutant rules: %s", len(rows))
    if missing:
        LOG.warning(
            "Missing pollutant rules for networks: %s",
            ", ".join(sorted(missing)),
        )


def _read_csv_rows(csv_path: str) -> Iterable[Dict[str, str]]:
    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def _load_register(
    csv_path: str,
    source_url: Optional[str],
    source_file: Optional[str],
    snapshot_at: Optional[str],
    batch_size: int,
    dry_run: bool,
) -> None:
    snapshot_value = snapshot_at or datetime.now(timezone.utc).isoformat()
    source_file_value = source_file or os.path.basename(csv_path)

    rows: List[Dict[str, Any]] = []
    network_refs: Set[str] = set()
    skipped_missing_ref = 0

    for raw in _read_csv_rows(csv_path):
        uk_air_ref = _clean_str(raw.get("UK-AIR ID"))
        if not uk_air_ref:
            skipped_missing_ref += 1
            continue
        networks = _parse_networks(raw.get("Networks"))
        for ref in networks:
            network_refs.add(ref)
        payload = {
            "uk_air_ref": uk_air_ref,
            "eu_site_ref": _clean_str(raw.get("EU Site ID")),
            "emep_site_ref": _clean_str(raw.get("EMEP Site ID")),
            "site_name": _clean_str(raw.get("Site Name")),
            "environment_type": _clean_str(raw.get("Environment Type")),
            "zone": _clean_str(raw.get("Zone")),
            "start_date": _parse_date(raw.get("Start Date")),
            "end_date": _parse_date(raw.get("End Date")),
            "latitude": _parse_float(raw.get("Latitude")),
            "longitude": _parse_float(raw.get("Longitude")),
            "northing": _parse_float(raw.get("Northing")),
            "easting": _parse_float(raw.get("Easting")),
            "altitude_m": _parse_float(raw.get("Altitude (m)")),
            "networks": networks,
            "aurn_pollutants_measured": _clean_str(raw.get("AURN Pollutants Measured")),
            "site_description": _clean_str(raw.get("Site Description")),
            "source_url": _clean_str(source_url),
            "source_file": _clean_str(source_file_value),
            "snapshot_at": snapshot_value,
            "raw_payload": json.loads(json.dumps(raw)),
        }
        rows.append(payload)

    LOG.info("Parsed rows: %s", len(rows))
    if skipped_missing_ref:
        LOG.warning("Skipped rows missing UK-AIR ref: %s", skipped_missing_ref)
    LOG.info("Unique network labels: %s", len(network_refs))

    if dry_run:
        LOG.info("Dry run enabled; no data written to Supabase.")
        return

    schemas = _build_client()
    existing_networks = _fetch_existing_networks(schemas.core)
    network_rows = []
    updated_at = datetime.now(timezone.utc).isoformat()
    for ref in sorted(network_refs):
        existing = existing_networks.get(ref, {})
        display_name = existing.get("network_display_name") or ref
        payload = {
            "network_ref": ref,
            "network_display_name": display_name,
            "updated_at": updated_at,
        }
        if existing.get("network_code"):
            payload["network_code"] = existing["network_code"]
        network_rows.append(payload)

    _upsert_batches(
        schemas.core,
        "uk_air_sos_networks",
        network_rows,
        batch_size=batch_size,
        on_conflict="network_ref",
    )
    _upsert_network_pollutants(schemas.core, network_refs, batch_size=batch_size)
    _upsert_batches(
        schemas.raw,
        "uk_air_sos_site_register",
        rows,
        batch_size=batch_size,
        on_conflict="uk_air_ref,snapshot_at",
    )
    LOG.info("Upserted networks: %s", len(network_rows))
    LOG.info("Upserted register rows: %s", len(rows))


def main() -> int:
    args = parse_args()
    headers = {"User-Agent": args.user_agent}

    if args.load_only and not args.csv_path:
        raise SystemExit("--load-only requires --csv-path.")
    if args.csv_path and not args.load_only:
        raise SystemExit("--csv-path is only supported with --load-only.")

    csv_url = None
    local_output = None

    if not args.load_only:
        csv_url = args.csv_url
        if not csv_url:
            if not args.search_url:
                raise SystemExit("Provide --search-url or set UK_AIR_SOS_SITE_SEARCH_URL.")
            resp = requests.get(args.search_url, headers=headers, timeout=args.timeout)
            resp.raise_for_status()
            if args.save_html:
                with open(args.save_html, "w", encoding="utf-8") as handle:
                    handle.write(resp.text)
            csv_url = _find_csv_link(resp.text, args.search_url)
            if not csv_url:
                raise SystemExit(
                    "Could not find a CSV link; use --csv-url or --save-html to inspect HTML."
                )

        resp = requests.get(csv_url, headers=headers, timeout=args.timeout)
        resp.raise_for_status()
        local_output = _timestamped_filename(args.output)
        with open(local_output, "wb") as handle:
            handle.write(resp.content)

        row_count, fields = _write_csv_summary(resp.content)
        LOG.info("Saved %s (rows=%s).", local_output, row_count)
        if fields:
            LOG.info("CSV columns: %s", ", ".join(fields))
        LOG.info("Source CSV URL: %s", csv_url)
        if args.dropbox_upload:
            root = _dropbox_root_folder()
            if not root:
                raise RuntimeError("UK_AQ_DROPBOX_ROOT must be set for Dropbox upload.")
            dropbox_dir = _join_dropbox_paths(root, args.dropbox_dir)
            dropbox_name = os.path.basename(local_output)
            dropbox_path = f"{dropbox_dir}/{dropbox_name}"
            token = _dropbox_refresh_access_token()
            _dropbox_upload_file(token, local_output, dropbox_path)
            LOG.info("Uploaded CSV to Dropbox: %s", dropbox_path)

    if args.load or args.load_only:
        csv_path = args.csv_path if args.load_only else local_output
        if not csv_path:
            raise SystemExit("No CSV path available for loading.")
        _load_register(
            csv_path,
            args.source_url or csv_url,
            args.source_file,
            args.snapshot_at,
            args.batch_size,
            args.dry_run,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
