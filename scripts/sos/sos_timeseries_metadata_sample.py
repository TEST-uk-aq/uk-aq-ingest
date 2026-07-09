#!/usr/bin/env python3
"""
Sample UK-AIR SOS timeseries metadata and highlight matches for key terms.

Examples:
  python3 scripts/sos/sos_timeseries_metadata_sample.py
  python3 scripts/sos/sos_timeseries_metadata_sample.py --station-limit 50
  python3 scripts/sos/sos_timeseries_metadata_sample.py --match-terms "model,wind,temperature"
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

LOG = logging.getLogger("sos_timeseries_sample")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SOS_BASE_URL = (
    os.getenv("SOS_BASE_URL")
    or os.getenv("UK_AIR_BASE_URL")
    or os.getenv("UKAIR_BASE_URL")
    or "https://uk-air.defra.gov.uk/sos-ukair/api/v1"
).rstrip("/")

DEFAULT_STATION_LIMIT = 25
DEFAULT_BATCH_SIZE = 25
DEFAULT_SAMPLE_LIMIT = 200
DEFAULT_MATCH_LIMIT = 50


def _extract_list(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("stations", "timeseries", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def _chunked(values: Sequence[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = DEFAULT_BATCH_SIZE
    for idx in range(0, len(values), size):
        yield list(values[idx : idx + size])


def _coerce_label(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        return payload.get("label")
    return None


def _coerce_id(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        value = payload.get("id")
        if value is not None:
            return str(value)
    if payload is not None:
        return str(payload)
    return None


class UkAirClient:
    def __init__(self, base_url: str, timeout: int = 60, retries: int = 3):
        self.base_url = base_url
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "uk-aq-metadata-sample/1.0"})

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    self._sleep(attempt)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                LOG.warning("Request failed (attempt %s/%s): %s", attempt, self.retries, exc)
                if attempt == self.retries:
                    raise
                self._sleep(attempt)
        return []

    def _sleep(self, attempt: int) -> None:
        time.sleep(min(30, 2**attempt))

    def stations(self) -> List[Dict[str, Any]]:
        payload = self.get("/stations", params={"expanded": "true"})
        return _extract_list(payload)

    def timeseries(
        self,
        station_ids: Sequence[str],
        service_ref: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not station_ids:
            return []
        params: Dict[str, Any] = {"expanded": "true", "station": list(station_ids)}
        if service_ref:
            params["service"] = service_ref
        payload = self.get("/timeseries", params=params)
        return _extract_list(payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample UK-AIR SOS timeseries metadata for quick inspection.",
    )
    parser.add_argument(
        "--station-limit",
        type=int,
        default=DEFAULT_STATION_LIMIT,
        help="Number of stations to sample (default: 25).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Batch size for timeseries requests (default: 25).",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=DEFAULT_SAMPLE_LIMIT,
        help="Max timeseries rows to include in the sample output (default: 200).",
    )
    parser.add_argument(
        "--match-limit",
        type=int,
        default=DEFAULT_MATCH_LIMIT,
        help="Max matched rows to include in output (default: 50).",
    )
    parser.add_argument(
        "--match-terms",
        default="model,wind,temperature",
        help="Comma-separated match terms (default: model,wind,temperature).",
    )
    parser.add_argument(
        "--service-ref",
        help="Optional service ref to include in the timeseries request.",
    )
    parser.add_argument(
        "--output",
        help="Output JSON file path (default: network_info/UK-Air-SOS/sos_timeseries_metadata_sample_<timestamp>.json).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = UkAirClient(SOS_BASE_URL)
    stations = client.stations()
    station_ids: List[str] = []
    for station in stations:
        props = station.get("properties") if isinstance(station.get("properties"), dict) else {}
        station_id = station.get("id") or props.get("id")
        if station_id is not None:
            station_ids.append(str(station_id))
    if not station_ids:
        LOG.warning("No station ids found in SOS payload.")
        return 0

    station_ids = station_ids[: max(1, args.station_limit)]
    series: List[Dict[str, Any]] = []
    for chunk in _chunked(station_ids, args.batch_size):
        series.extend(client.timeseries(chunk, service_ref=args.service_ref))

    sample_rows = []
    for ts in series:
        phenomenon = ts.get("phenomenon") or {}
        feature = ts.get("feature") or ts.get("featureOfInterest") or {}
        offering = ts.get("offering") or {}
        sample_rows.append(
            {
                "timeseries_ref": ts.get("id"),
                "label": ts.get("label"),
                "phenomenon_label": _coerce_label(phenomenon),
                "phenomenon_id": _coerce_id(phenomenon),
                "feature_id": _coerce_id(feature),
                "feature_label": _coerce_label(feature),
                "offering_id": _coerce_id(offering),
                "offering_label": _coerce_label(offering),
            }
        )

    terms = [term.strip().lower() for term in args.match_terms.split(",") if term.strip()]
    matches = []
    for row in sample_rows:
        text = " ".join(
            [
                str(row.get("label") or ""),
                str(row.get("phenomenon_label") or ""),
                str(row.get("offering_label") or ""),
                str(row.get("feature_label") or ""),
            ]
        ).lower()
        if any(term in text for term in terms):
            matches.append(row)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = args.output
    if not output_path:
        output_path = (
            Path("network_info/UK-Air-SOS")
            / f"sos_timeseries_metadata_sample_{stamp}.json"
        )
    else:
        output_path = Path(output_path)

    payload = {
        "source": SOS_BASE_URL,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "station_sample_count": len(station_ids),
        "timeseries_count": len(sample_rows),
        "match_terms": terms,
        "match_count": len(matches),
        "matches": matches[: args.match_limit],
        "sample": sample_rows[: args.sample_limit],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2))
    LOG.info("Wrote %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
