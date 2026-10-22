#!/usr/bin/env python3
"""Refresh UK-AIR Black Carbon stations, routing refs, and timeseries metadata.

This command deliberately does not acquire or write observations. It gathers and
validates the complete official reference plan before making any Supabase writes.
"""

from __future__ import annotations

import argparse
import csv
import html
import io
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qsl, unquote, urljoin, urlparse

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.uk_aq_phenomena_rpc import upsert_phenomena_via_rpc
from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client


LOG = logging.getLogger("ukair_bc_reference_refresh")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CONNECTOR_CODE = "ukair_bc"
NETWORK_CODE = "black_carbon"
SERVICE_REF = "ukair_bc"
SUPPORTED_FROM = date(2020, 1, 1)
UK_AIR_BASE_URL = "https://uk-air.defra.gov.uk"
DEFAULT_SEARCH_URL = (
    f"{UK_AIR_BASE_URL}/networks/find-sites?action=results&country_id=9999"
    "&group_id=16&location_type=9999&pollutant=&region_id=9999&site_name="
    "&view=advanced&closed=true"
)
SITE_INFO_URL = f"{UK_AIR_BASE_URL}/networks/site-info"
FLAT_FILES_URL = f"{UK_AIR_BASE_URL}/data/flat_files"
DEFAULT_USER_AGENT = "Mozilla/5.0 (ukair_bc_reference_refresh)"
PAGE_SIZE = 500
UPSERT_BATCH_SIZE = 200
UPSTREAM_UNAVAILABLE_EXIT_CODE = 75
RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
CATALOGUE_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
MAINTENANCE_HTML_LIMIT = 16 * 1024
ACCESS_CHALLENGE_RE = re.compile(
    r"<form\b|<input\b|\b(?:authenticat\w*|(?:un)?authori[sz]\w*|"
    r"password|log[ _-]?in|sign[ _-]?in|sso|denied|forbidden|blocked|restricted|"
    r"captcha|verification|401|403|permission\s+(?:error|required)|"
    r"verify\s+(?:you|that)|checking\s+(?:your\s+)?browser|just\s+a\s+moment|"
    r"are\s+you\s+(?:a\s+)?(?:human|robot)|(?:bot|browser|security)\s+(?:check|challenge)|"
    r"turnstile|recaptcha|hcaptcha|cdn-cgi|cf-chl|challenge(?:-platform)?|"
    r"enable\s+(?:javascript|cookies)|javascript\s+(?:is\s+)?required|"
    r"configuration\s+error|misconfigur\w*)\b",
    re.IGNORECASE,
)

REQUIRED_CATALOGUE_COLUMNS = frozenset(
    {
        "UK-AIR ID",
        "EU Site ID",
        "EMEP Site ID",
        "Site Name",
        "Environment Type",
        "Zone",
        "Start Date",
        "End Date",
        "Latitude",
        "Longitude",
        "Northing",
        "Easting",
        "Altitude (m)",
        "Networks",
        "AURN Pollutants Measured",
        "Site Description",
    }
)
UKA_RE = re.compile(r"^UKA[0-9]+$")
SITE_REF_RE = re.compile(r"^[A-Z0-9]+$")
BLACK_CARBON_NETWORK_LABELS = frozenset(
    {
        "black carbon",
        "black carbon network",
        "uk black carbon network",
    }
)
FLAT_FILES_SITE_REF_RE = re.compile(
    r"data/flat_files\?site_id=([A-Za-z0-9]+)", re.IGNORECASE
)
SITE_PHOTO_SITE_REF_RE = re.compile(
    r"assets/site-photos/([A-Za-z0-9]+)_site\.(?:jpg|jpeg|png)",
    re.IGNORECASE,
)

PROPERTY_CONFIG: Dict[str, Dict[str, Any]] = {
    "bc": {
        "source_label": "Black Carbon (880nm) ug/m-3",
        "notation": "Black Carbon (880nm)",
        "label": "Black Carbon (880 nm)",
        "filename_token": "BC",
    },
    "uv370": {
        "source_label": "UV Particulate Matter (370nm) ug/m-3",
        "notation": "UV Particulate Matter (370nm)",
        "label": "UV Particulate Matter (370 nm)",
        "filename_token": "U_Violet",
    },
}


class SourceFormatError(RuntimeError):
    """Raised when an official UK-AIR response is structurally incompatible."""


class SourceAcquisitionUnavailable(RuntimeError):
    """An official source request exhausted its bounded temporary-failure retries."""

    def __init__(
        self, url: str, failure_class: str, attempts: int, http_status: Optional[int] = None
    ) -> None:
        parsed = urlparse(url)
        # Keep diagnostics bounded and omit query/user information.
        self.source_url = f"{UK_AIR_BASE_URL}{parsed.path[:200]}"
        self.failure_class = failure_class
        self.attempts = attempts
        self.http_status = http_status
        detail = f"HTTP {http_status}" if http_status is not None else failure_class
        super().__init__(
            f"UK-AIR source unavailable ({detail}) after {attempts} attempts: "
            f"{self.source_url}"
        )


class RetainedReferenceSourceUnavailable(SourceAcquisitionUnavailable):
    """A pre-write source outage with independently verified retained references."""


def is_official_source_url(url: str) -> bool:
    parsed = urlparse(url)
    return (
        parsed.scheme == "https"
        and parsed.hostname == "uk-air.defra.gov.uk"
        and parsed.port in (None, 443)
        and parsed.username is None
        and parsed.password is None
    )


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: List[Tuple[str, str]] = []
        self._href: Optional[str] = None
        self._text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        href = next((value for name, value in attrs if name.lower() == "href"), None)
        if href:
            self._href = href
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._href is None:
            return
        self.links.append((self._href, " ".join(self._text).strip()))
        self._href = None
        self._text = []


@dataclass(frozen=True)
class CatalogueStation:
    uk_air_ref: str
    site_name: str
    environment_type: Optional[str]
    zone: Optional[str]
    start_date: Optional[date]
    end_date: Optional[date]
    latitude: Optional[float]
    longitude: Optional[float]
    description: Optional[str]
    raw: Dict[str, str]

    @property
    def active(self) -> bool:
        return self.end_date is None

    @property
    def relevant(self) -> bool:
        return self.end_date is None or self.end_date >= SUPPORTED_FROM


@dataclass(frozen=True)
class SiteRefEvidence:
    site_ref: Optional[str]
    source_url: Optional[str]
    checked_at: Optional[str]
    provenance: str


@dataclass(frozen=True)
class PlannedStation:
    catalogue: CatalogueStation
    site_ref: SiteRefEvidence
    supported_properties: frozenset[str]
    supporting_files: Dict[str, Tuple[str, ...]]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def normalize_uka(value: Any) -> str:
    normalized = (clean_str(value) or "").upper()
    if not UKA_RE.fullmatch(normalized):
        raise SourceFormatError(f"Invalid UK-AIR station identity: {value!r}")
    return normalized


def normalize_site_ref(value: Any) -> Optional[str]:
    normalized = (clean_str(value) or "").upper()
    if not normalized:
        return None
    if not SITE_REF_RE.fullmatch(normalized):
        raise SourceFormatError(f"Invalid UK-AIR short site_ref: {value!r}")
    return normalized


def parse_source_date(value: Any, field: str, uk_air_ref: str) -> Optional[date]:
    cleaned = clean_str(value)
    if cleaned is None:
        return None
    try:
        return datetime.strptime(cleaned, "%Y-%m-%d").date()
    except ValueError as exc:
        raise SourceFormatError(
            f"Invalid {field} for {uk_air_ref}: expected YYYY-MM-DD, got {cleaned!r}"
        ) from exc


def parse_float(value: Any, field: str, uk_air_ref: str) -> Optional[float]:
    cleaned = clean_str(value)
    if cleaned is None:
        return None
    try:
        parsed = float(cleaned)
    except ValueError as exc:
        raise SourceFormatError(f"Invalid {field} for {uk_air_ref}: {cleaned!r}") from exc
    return parsed


def split_networks(value: Any) -> List[str]:
    return [item.strip() for item in str(value or "").split(";") if item.strip()]


def has_black_carbon_membership(networks: Iterable[str]) -> bool:
    normalized = {re.sub(r"\s+", " ", value.strip().lower()) for value in networks}
    return bool(normalized & BLACK_CARBON_NETWORK_LABELS)


def parse_catalogue_csv(content: bytes) -> List[CatalogueStation]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SourceFormatError("UK-AIR catalogue CSV is not valid UTF-8") from exc
    reader = csv.DictReader(io.StringIO(text))
    fields = set(reader.fieldnames or [])
    missing = sorted(REQUIRED_CATALOGUE_COLUMNS - fields)
    if missing:
        raise SourceFormatError(
            "UK-AIR Black Carbon catalogue is missing required columns: "
            + ", ".join(missing)
        )

    stations: List[CatalogueStation] = []
    seen: Set[str] = set()
    for row_number, raw_row in enumerate(reader, start=2):
        raw = {str(key): str(value or "") for key, value in raw_row.items() if key is not None}
        uk_air_ref = normalize_uka(raw.get("UK-AIR ID"))
        if uk_air_ref in seen:
            raise SourceFormatError(f"Duplicate UK-AIR ID in catalogue: {uk_air_ref}")
        seen.add(uk_air_ref)
        networks = split_networks(raw.get("Networks"))
        if not has_black_carbon_membership(networks):
            raise SourceFormatError(
                f"Catalogue row {row_number} ({uk_air_ref}) lacks Black Carbon network membership"
            )
        site_name = clean_str(raw.get("Site Name"))
        if not site_name:
            raise SourceFormatError(f"Catalogue row {row_number} ({uk_air_ref}) has no Site Name")
        start_date = parse_source_date(raw.get("Start Date"), "Start Date", uk_air_ref)
        end_date = parse_source_date(raw.get("End Date"), "End Date", uk_air_ref)
        if start_date and end_date and end_date < start_date:
            raise SourceFormatError(f"End Date precedes Start Date for {uk_air_ref}")
        latitude = parse_float(raw.get("Latitude"), "Latitude", uk_air_ref)
        longitude = parse_float(raw.get("Longitude"), "Longitude", uk_air_ref)
        if latitude is not None and not -90 <= latitude <= 90:
            raise SourceFormatError(f"Latitude out of range for {uk_air_ref}")
        if longitude is not None and not -180 <= longitude <= 180:
            raise SourceFormatError(f"Longitude out of range for {uk_air_ref}")
        stations.append(
            CatalogueStation(
                uk_air_ref=uk_air_ref,
                site_name=site_name,
                environment_type=clean_str(raw.get("Environment Type")),
                zone=clean_str(raw.get("Zone")),
                start_date=start_date,
                end_date=end_date,
                latitude=latitude,
                longitude=longitude,
                description=clean_str(raw.get("Site Description")),
                raw=raw,
            )
        )
    if not stations:
        raise SourceFormatError("UK-AIR Black Carbon catalogue contains no data rows")
    return stations


def find_catalogue_csv_url(html_text: str, search_url: str) -> str:
    parser = LinkParser()
    parser.feed(html_text)
    for href, text in parser.links:
        if "download" in text.lower() and "csv" in text.lower():
            return urljoin(search_url, href)
    for href, _text in parser.links:
        if re.search(r"\.csv(?:$|[?#])", href, re.IGNORECASE):
            return urljoin(search_url, href)
    for href, _text in parser.links:
        if "csv" in href.lower() and "download" in href.lower():
            return urljoin(search_url, href)
    raise SourceFormatError("Official UK-AIR search page did not expose a catalogue CSV link")


def catalogue_request_url(
    url: str, *, relative_to: Optional[str] = None
) -> str:
    """Validate search redirect targets before requests can send credentials."""
    if (
        not url or len(url) > 4096 or not url.isascii()
        or re.search(r"[\s\x00-\x1f\x7f\\%]", url)
        or "#" in url or url.startswith("//")
        or (relative_to is not None and "?" in url)
    ):
        raise SourceFormatError("Ambiguous UK-AIR catalogue request URL")
    try:
        parts = urlparse(url)
        if (
            parts.params or ";" in parts.path
            or re.search(r"//|(?:^|/)\.{1,2}(?:/|$)", parts.path)
            or (parts.scheme and not parts.netloc)
        ):
            raise ValueError("ambiguous path")
        resolved = urljoin(relative_to, url) if relative_to is not None else url
        parts = urlparse(resolved)
        if not is_official_source_url(resolved):
            raise ValueError("unofficial source")
        # Reject alternate authority spellings, empty ports and other ambiguity.
        if parts.netloc.lower() not in (parts.hostname, f"{parts.hostname}:443"):
            raise ValueError("ambiguous authority")
        if relative_to is not None:
            base = urlparse(relative_to)
            if (parts.scheme, parts.hostname, parts.port or 443) != (
                base.scheme, base.hostname, base.port or 443
            ):
                raise ValueError("origin changed")
        if ACCESS_CHALLENGE_RE.search(parts.path):
            raise ValueError("access challenge destination")
    except ValueError as exc:
        raise SourceFormatError("Unsafe UK-AIR catalogue request destination") from exc
    return resolved


def check_catalogue_response_metadata(response: requests.Response) -> None:
    if any(name in response.headers for name in (
        "WWW-Authenticate", "Proxy-Authenticate", "Refresh", "CF-Mitigated"
    )) or any(
        ACCESS_CHALLENGE_RE.search(f"{name}: {value}")
        for name, value in response.headers.items()
    ):
        raise SourceFormatError("UK-AIR catalogue response has access/challenge metadata")


def bounded_catalogue_html(response: requests.Response) -> str:
    body = bytearray()
    for chunk in response.iter_content(chunk_size=MAINTENANCE_HTML_LIMIT + 1):
        body.extend(chunk)
        if len(body) > MAINTENANCE_HTML_LIMIT:
            raise SourceFormatError("UK-AIR redirect HTML exceeds the inspection limit")
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SourceFormatError("UK-AIR redirect HTML has an unsupported encoding") from exc
    if ACCESS_CHALLENGE_RE.search(html.unescape(text)) or re.search(
        r"http-equiv\s*=\s*['\"]?refresh\b", text, re.IGNORECASE
    ):
        raise SourceFormatError("UK-AIR redirect HTML contains access/challenge evidence")
    return text


def request_catalogue_search(
    session: requests.Session, url: str, *, timeout: int
) -> Tuple[requests.Response, bool]:
    """Follow at most two validated same-origin hops; never auto-follow a target."""
    current_url = catalogue_request_url(url)
    requested = urlparse(current_url)
    authorised = urlparse(DEFAULT_SEARCH_URL)
    eligible = (
        requested.path == authorised.path
        and sorted(parse_qsl(requested.query, keep_blank_values=True))
        == sorted(parse_qsl(authorised.query, keep_blank_values=True))
    )
    for redirects in range(3):
        response = session.get(
            current_url, timeout=timeout, allow_redirects=False, stream=True
        )
        try:
            if response.history or response.url != current_url:
                raise SourceFormatError("Incomplete/conflicting UK-AIR redirect metadata")
            check_catalogue_response_metadata(response)
            if response.status_code in CATALOGUE_REDIRECT_STATUSES:
                location = response.headers.get("Location", "")
                if redirects == 2 or not location or "," in location:
                    raise SourceFormatError("Invalid or excessive UK-AIR catalogue redirects")
                target = catalogue_request_url(location, relative_to=current_url)
                bounded_catalogue_html(response)
                response.close()
                current_url = target
                continue
            if "Location" in response.headers or 300 <= response.status_code < 400:
                raise SourceFormatError("Unexpected UK-AIR catalogue redirect metadata")
            if redirects and urlparse(current_url).path == "/maintenance/":
                if response.status_code == 200:
                    media_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
                    if media_type not in ("text/html", "application/xhtml+xml"):
                        raise SourceFormatError("UK-AIR maintenance destination is not HTML")
                    text = bounded_catalogue_html(response)
                    document = re.sub(
                        r"<!--.*?-->|<(script|style)\b[^>]*>.*?</\1\s*>",
                        "", text, flags=re.I | re.S,
                    )
                    if not (
                        not re.search(r"<!--|<(?:script|style)\b", document, re.I)
                        and re.match(r"\s*(?:<!doctype\s+html(?:\s[^<>]*)?>\s*)?<html(?:\s[^<>]*)?>", document, re.I)
                        and re.search(r"<body(?:\s[^<>]*)?>.*</body\s*>\s*</html\s*>\s*$", document, re.I | re.S)
                    ):
                        raise SourceFormatError("UK-AIR maintenance response is not a recognisable HTML document")
                    if not eligible:
                        raise SourceFormatError("Maintenance redirect outside authorised Black Carbon search")
                    return response, True
            # Preserve the existing eager read inside the retry boundary for
            # ordinary search pages (including connection/read failures).
            response.content
            return response, False
        except Exception:
            response.close()
            raise
    raise AssertionError("unreachable")


def request_with_retry(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    retries: int,
    catalogue_search: bool = False,
) -> requests.Response:
    if not is_official_source_url(url):
        raise SourceFormatError("Reference acquisition requires an official HTTPS UK-AIR URL")
    endpoint = f"{UK_AIR_BASE_URL}{urlparse(url).path[:200]}"
    for attempt in range(1, retries + 1):
        status = None
        try:
            maintenance_redirect = False
            if catalogue_search:
                response, maintenance_redirect = request_catalogue_search(session, url, timeout=timeout)
            else:
                response = session.get(url, timeout=timeout)
            if not is_official_source_url(response.url):
                raise SourceFormatError("UK-AIR acquisition redirected outside the official source")
            status = response.status_code
            if maintenance_redirect:
                failure_class = "maintenance_redirect"
            elif status not in RETRYABLE_HTTP_STATUSES:
                if status >= 400:
                    raise RuntimeError(f"Non-retryable UK-AIR HTTP {status}: {endpoint}")
                return response
            else:
                failure_class = f"http_{status}" if status < 500 else "http_5xx"
            response.close()
        except (requests.Timeout, requests.ConnectionError) as exc:
            # TLS/proxy configuration errors are not evidence of a temporary source outage.
            if isinstance(exc, (requests.exceptions.SSLError, requests.exceptions.ProxyError)):
                raise RuntimeError(f"UK-AIR {type(exc).__name__}: {endpoint}") from exc
            failed_url = getattr(getattr(exc, "request", None), "url", None) or url
            if not is_official_source_url(failed_url):
                raise RuntimeError("Connection failure outside the official UK-AIR source") from exc
            failure_class = (
                "upstream_timeout" if isinstance(exc, requests.Timeout) else "upstream_connection"
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Non-retryable UK-AIR {type(exc).__name__}: {endpoint}") from exc
        LOG.warning(
            "UK-AIR request failed (%s/%s, %s, HTTP %s): %s",
            attempt, retries, failure_class, status, endpoint,
        )
        if attempt == retries:
            raise SourceAcquisitionUnavailable(endpoint, failure_class, attempt, status)
        # No Retry-After override: remote headers cannot extend this bounded backoff.
        time.sleep(min(8.0, float(2 ** min(attempt - 1, 3))))
    raise AssertionError("unreachable")


def acquire_catalogue(
    session: requests.Session,
    *,
    search_url: str,
    csv_url: Optional[str],
    catalogue_csv: Optional[str],
    timeout: int,
    retries: int,
) -> Tuple[List[CatalogueStation], str, str]:
    if catalogue_csv:
        path = Path(catalogue_csv)
        content = path.read_bytes()
        source_url = csv_url or search_url
        return parse_catalogue_csv(content), source_url, utc_now_iso()
    resolved_csv_url = csv_url
    if not resolved_csv_url:
        search_response = request_with_retry(
            session, search_url, timeout=timeout, retries=retries, catalogue_search=True
        )
        resolved_csv_url = find_catalogue_csv_url(search_response.text, search_url)
    csv_response = request_with_retry(
        session, resolved_csv_url, timeout=timeout, retries=retries
    )
    return parse_catalogue_csv(csv_response.content), resolved_csv_url, utc_now_iso()


def response_rows(response: Any, context: str) -> List[Dict[str, Any]]:
    if hasattr(response, "data"):
        rows = response.data
    elif isinstance(response, dict):
        rows = response.get("data")
    else:
        raise RuntimeError(f"{context} response did not expose data rows")
    if rows is None:
        return []
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise RuntimeError(f"{context} response data is not a list of objects")
    return [dict(row) for row in rows]


def fetch_existing_bridge_rows(raw_client: Any, uka_refs: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    if not uka_refs:
        return {}
    rows = response_rows(
        raw_client.table("ukair_bc_station_refs")
        .select(
            "station_id,uk_air_ref,site_ref,site_ref_source_url,"
            "site_ref_source_checked_at,source_snapshot_at,raw_payload"
        )
        .in_("uk_air_ref", list(uka_refs))
        .execute(),
        "ukair_bc_station_refs",
    )
    return {normalize_uka(row.get("uk_air_ref")): row for row in rows}


def fetch_sos_site_refs(raw_client: Any, uka_refs: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    if not uka_refs:
        return {}
    latest: Dict[str, Dict[str, Any]] = {}
    offset = 0
    while True:
        rows = response_rows(
            raw_client.table("sos_site_register")
            .select("uk_air_ref,site_ref,site_ref_source_url,site_ref_source_checked_at,snapshot_at")
            .in_("uk_air_ref", list(uka_refs))
            .filter("site_ref", "not.is", "null")
            .order("snapshot_at", desc=True)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute(),
            "sos_site_register",
        )
        for row in rows:
            uk_air_ref = normalize_uka(row.get("uk_air_ref"))
            latest.setdefault(uk_air_ref, row)
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return latest


def choose_existing_site_ref(
    uk_air_ref: str,
    bridge_row: Optional[Mapping[str, Any]],
    sos_row: Optional[Mapping[str, Any]],
) -> Optional[SiteRefEvidence]:
    bridge_ref = normalize_site_ref((bridge_row or {}).get("site_ref"))
    sos_ref = normalize_site_ref((sos_row or {}).get("site_ref"))
    if bridge_ref and sos_ref and bridge_ref != sos_ref:
        raise RuntimeError(
            f"Contradictory authoritative site_ref evidence for {uk_air_ref}: "
            f"ukair_bc_station_refs={bridge_ref}, sos_site_register={sos_ref}"
        )
    if sos_ref:
        return SiteRefEvidence(
            site_ref=sos_ref,
            source_url=clean_str((sos_row or {}).get("site_ref_source_url"))
            or f"{SITE_INFO_URL}?site_id={sos_ref}",
            checked_at=clean_str((sos_row or {}).get("site_ref_source_checked_at"))
            or clean_str((sos_row or {}).get("snapshot_at")),
            provenance="sos_site_register",
        )
    if bridge_ref:
        return SiteRefEvidence(
            site_ref=bridge_ref,
            source_url=clean_str((bridge_row or {}).get("site_ref_source_url"))
            or f"{SITE_INFO_URL}?site_id={bridge_ref}",
            checked_at=clean_str((bridge_row or {}).get("site_ref_source_checked_at"))
            or clean_str((bridge_row or {}).get("source_snapshot_at")),
            provenance="ukair_bc_station_refs",
        )
    return None


def discover_site_ref(
    session: requests.Session,
    uk_air_ref: str,
    *,
    timeout: int,
    retries: int,
) -> SiteRefEvidence:
    url = f"{SITE_INFO_URL}?uka_id={uk_air_ref}"
    response = request_with_retry(session, url, timeout=timeout, retries=retries)
    text = response.text
    if uk_air_ref.lower() not in text.lower():
        raise SourceFormatError(
            f"Official UK-AIR site-info response did not confirm {uk_air_ref}"
        )
    matches = {value.upper() for value in FLAT_FILES_SITE_REF_RE.findall(text)}
    if not matches:
        matches = {value.upper() for value in SITE_PHOTO_SITE_REF_RE.findall(text)}
    if len(matches) > 1:
        raise SourceFormatError(
            f"Official site-info response exposed multiple site_ref values for {uk_air_ref}: "
            + ", ".join(sorted(matches))
        )
    if not matches:
        return SiteRefEvidence(None, url, utc_now_iso(), "site_info_unresolved")
    return SiteRefEvidence(
        normalize_site_ref(next(iter(matches))), url, utc_now_iso(), "site_info"
    )


def parse_property_support_html(
    html_text: str, site_ref: str
) -> Tuple[frozenset[str], Dict[str, Tuple[str, ...]]]:
    parser = LinkParser()
    parser.feed(html_text)
    site_files: Set[str] = set()
    prefix = f"{site_ref.upper()}_"
    for href, _text in parser.links:
        filename = unquote(Path(urlparse(href).path).name)
        if filename.upper().startswith(prefix) and filename.lower().endswith(".csv"):
            site_files.add(filename)
    if not site_files:
        raise SourceFormatError(
            f"Official flat-files page for {site_ref} exposed no site CSV links"
        )

    supported: Set[str] = set()
    supporting_files: Dict[str, Tuple[str, ...]] = {}
    for property_code, config in PROPERTY_CONFIG.items():
        token = re.escape(str(config["filename_token"]))
        pattern = re.compile(
            rf"^{re.escape(site_ref)}_{token}_([0-9]{{4}})\.csv$", re.IGNORECASE
        )
        matches = sorted(
            filename
            for filename in site_files
            if (match := pattern.fullmatch(filename))
            and int(match.group(1)) >= SUPPORTED_FROM.year
        )
        if matches:
            supported.add(property_code)
            supporting_files[property_code] = tuple(matches)
    return frozenset(supported), supporting_files


def discover_property_support(
    session: requests.Session,
    site_ref: str,
    *,
    timeout: int,
    retries: int,
) -> Tuple[frozenset[str], Dict[str, Tuple[str, ...]]]:
    url = f"{FLAT_FILES_URL}?site_id={site_ref}"
    response = request_with_retry(session, url, timeout=timeout, retries=retries)
    return parse_property_support_html(response.text, site_ref)


def resolve_single_code_row(
    table: Any, *, table_name: str, code_field: str, code: str, select: str
) -> Dict[str, Any]:
    rows = response_rows(
        table.select(select).eq(code_field, code).limit(2).execute(), table_name
    )
    if len(rows) != 1:
        raise RuntimeError(
            f"Expected exactly one {table_name} row with {code_field}={code}; found {len(rows)}"
        )
    return rows[0]


def resolve_metadata(schemas: SupabaseSchemas) -> Dict[str, Any]:
    connector = resolve_single_code_row(
        schemas.core.table("connectors"),
        table_name="connectors",
        code_field="connector_code",
        code=CONNECTOR_CODE,
        select="id,connector_code,default_network_id,poll_enabled",
    )
    network = resolve_single_code_row(
        schemas.core.table("networks"),
        table_name="networks",
        code_field="network_code",
        code=NETWORK_CODE,
        select="id,network_code,display_name",
    )
    if int(connector.get("default_network_id") or 0) != int(network["id"]):
        raise RuntimeError(
            f"{CONNECTOR_CODE} connector default_network_id does not match {NETWORK_CODE}"
        )
    if bool(connector.get("poll_enabled")):
        raise RuntimeError("ukair_bc.poll_enabled must remain false for reference refresh")

    properties = response_rows(
        schemas.core.table("observed_properties")
        .select("id,code,canonical_uom")
        .in_("code", sorted(PROPERTY_CONFIG))
        .execute(),
        "observed_properties",
    )
    properties_by_code = {str(row.get("code")): row for row in properties}
    if set(properties_by_code) != set(PROPERTY_CONFIG):
        raise RuntimeError("Canonical bc and uv370 observed properties are required")
    for code, row in properties_by_code.items():
        if row.get("canonical_uom") != "ug/m3":
            raise RuntimeError(f"Observed property {code} has unexpected canonical_uom")

    connector_id = int(connector["id"])
    source_labels = [str(config["source_label"]) for config in PROPERTY_CONFIG.values()]
    mappings = response_rows(
        schemas.core.table("observed_property_mappings")
        .select(
            "source_label,observed_property_id,observed_property_code,source_uom,"
            "mapping_kind,is_aqi_eligible,is_active"
        )
        .eq("connector_id", connector_id)
        .in_("source_label", source_labels)
        .execute(),
        "observed_property_mappings",
    )
    mappings_by_label = {str(row.get("source_label")): row for row in mappings}
    for property_code, config in PROPERTY_CONFIG.items():
        source_label = str(config["source_label"])
        mapping = mappings_by_label.get(source_label)
        expected_property_id = int(properties_by_code[property_code]["id"])
        if not mapping:
            raise RuntimeError(f"Missing explicit observed-property mapping for {source_label}")
        if (
            int(mapping.get("observed_property_id") or 0) != expected_property_id
            or mapping.get("observed_property_code") != property_code
            or mapping.get("source_uom") != "ug/m3"
            or mapping.get("mapping_kind") != "raw_observed_property"
            or mapping.get("is_aqi_eligible") is not False
            or mapping.get("is_active") is not True
        ):
            raise RuntimeError(f"Invalid observed-property mapping policy for {source_label}")
    return {
        "connector": connector,
        "network": network,
        "properties_by_code": properties_by_code,
    }


def build_phenomena_rows(connector_id: int) -> List[Dict[str, Any]]:
    return [
        {
            "connector_id": connector_id,
            "label": config["label"],
            "source_label": config["source_label"],
            "notation": config["notation"],
            "pollutant_label": property_code,
            "source_uom": "ug/m3",
            "mapping_kind": "raw_observed_property",
            "observed_property_code": property_code,
            "is_aqi_eligible": False,
        }
        for property_code, config in PROPERTY_CONFIG.items()
    ]


def validate_phenomena_results(
    rows: Sequence[Mapping[str, Any]], diagnostics: Mapping[str, Mapping[str, Any]]
) -> Tuple[Dict[str, int], Dict[str, int]]:
    phenomenon_ids: Dict[str, int] = {}
    property_ids: Dict[str, int] = {}
    for row in rows:
        source_label = str(row["source_label"])
        result = diagnostics.get(source_label)
        if not result or result.get("phenomenon_id") is None:
            raise RuntimeError(f"Central phenomena RPC omitted {source_label}")
        if result.get("observed_property_id") is None:
            raise RuntimeError(f"Central phenomena RPC omitted observed_property_id for {source_label}")
        if result.get("mapping_warning"):
            raise RuntimeError(f"Central phenomena RPC warned for {source_label}")
        if (
            result.get("observed_property_code") != row["observed_property_code"]
            or result.get("mapping_kind") != "raw_observed_property"
            or result.get("is_aqi_eligible") is not False
        ):
            raise RuntimeError(f"Central phenomena RPC returned invalid policy for {source_label}")
        property_code = str(row["observed_property_code"])
        phenomenon_ids[property_code] = int(result["phenomenon_id"])
        property_ids[property_code] = int(result["observed_property_id"])
    return phenomenon_ids, property_ids


def timeseries_ref(uk_air_ref: str, property_code: str) -> str:
    normalized_uka = normalize_uka(uk_air_ref)
    if property_code not in PROPERTY_CONFIG:
        raise RuntimeError(f"Unsupported UK-AIR Black Carbon property: {property_code}")
    return f"{normalized_uka}:{property_code}"


def build_station_row(
    planned: PlannedStation,
    *,
    connector_id: int,
    network_id: int,
    snapshot_at: str,
    existing: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    station = planned.catalogue
    current = existing or {}
    latitude = station.latitude if station.latitude is not None else current.get("latitude")
    longitude = station.longitude if station.longitude is not None else current.get("longitude")
    geometry = None
    if latitude is not None and longitude is not None:
        geometry = f"SRID=4326;POINT({longitude} {latitude})"
    return {
        "connector_id": connector_id,
        "network_id": network_id,
        "service_ref": SERVICE_REF,
        "station_ref": station.uk_air_ref,
        "label": station.site_name,
        "station_name": station.site_name,
        "station_type": station.environment_type or current.get("station_type"),
        "region": station.zone or current.get("region"),
        "latitude": latitude,
        "longitude": longitude,
        "geometry": geometry,
        "description": station.description or current.get("description"),
        "first_seen_at": (
            station.start_date.isoformat()
            if station.start_date
            else current.get("first_seen_at")
        ),
        "last_seen_at": station.end_date.isoformat() if station.end_date else snapshot_at,
        "removed_at": station.end_date.isoformat() if station.end_date else None,
        "updated_at": snapshot_at,
    }


def build_bridge_row(
    planned: PlannedStation, *, station_id: int, source_url: str, snapshot_at: str
) -> Dict[str, Any]:
    station = planned.catalogue
    return {
        "station_id": station_id,
        "uk_air_ref": station.uk_air_ref,
        "site_ref": planned.site_ref.site_ref,
        "site_ref_source_url": planned.site_ref.source_url,
        "site_ref_source_checked_at": planned.site_ref.checked_at,
        "source_snapshot_at": snapshot_at,
        "raw_payload": {
            "catalogue_source_url": source_url,
            "catalogue_row": station.raw,
            "site_ref_provenance": planned.site_ref.provenance,
            "supported_properties": sorted(planned.supported_properties),
            "supporting_files": {
                code: list(files) for code, files in sorted(planned.supporting_files.items())
            },
        },
        "updated_at": snapshot_at,
    }


def build_timeseries_rows(
    planned_stations: Sequence[PlannedStation],
    *,
    connector_id: int,
    station_ids: Mapping[str, int],
    phenomenon_ids: Mapping[str, int],
    property_ids: Mapping[str, int],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for planned in planned_stations:
        station = planned.catalogue
        station_id = station_ids.get(station.uk_air_ref)
        if station_id is None:
            raise RuntimeError(f"Missing canonical station_id for {station.uk_air_ref}")
        for property_code in sorted(planned.supported_properties):
            config = PROPERTY_CONFIG[property_code]
            rows.append(
                {
                    "connector_id": connector_id,
                    "station_id": station_id,
                    "service_ref": SERVICE_REF,
                    "timeseries_ref": timeseries_ref(station.uk_air_ref, property_code),
                    "label": f"{station.site_name} {config['label']}",
                    "uom": "ug/m3",
                    "phenomenon_id": phenomenon_ids[property_code],
                    "observed_property_id": property_ids[property_code],
                    "extras": {
                        "uk_air_ref": station.uk_air_ref,
                        "site_ref": planned.site_ref.site_ref,
                        "source_property": property_code,
                    },
                }
            )
    return rows


def chunked(values: Sequence[Any], size: int = UPSERT_BATCH_SIZE) -> Iterable[Sequence[Any]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def fetch_stations(core: Any, connector_id: int, refs: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    if not refs:
        return {}
    rows = response_rows(
        core.table("stations")
        .select(
            "id,connector_id,network_id,service_ref,station_ref,label,station_name,"
            "station_type,region,latitude,longitude,description,first_seen_at,last_seen_at,removed_at"
        )
        .eq("connector_id", connector_id)
        .eq("service_ref", SERVICE_REF)
        .in_("station_ref", list(refs))
        .execute(),
        "stations",
    )
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        station_ref = normalize_uka(row.get("station_ref"))
        if station_ref in result:
            raise RuntimeError(f"Duplicate stored Black Carbon station identity: {station_ref}")
        result[station_ref] = row
    return result


def fetch_timeseries(core: Any, connector_id: int) -> Dict[str, Dict[str, Any]]:
    found: Dict[str, Dict[str, Any]] = {}
    offset = 0
    while True:
        rows = response_rows(
            core.table("timeseries")
            .select(
                "id,connector_id,station_id,service_ref,timeseries_ref,label,uom,"
                "phenomenon_id,observed_property_id,extras"
            )
            .eq("connector_id", connector_id)
            .eq("service_ref", SERVICE_REF)
            .order("id")
            .range(offset, offset + PAGE_SIZE - 1)
            .execute(),
            "timeseries",
        )
        for row in rows:
            ref = str(row.get("timeseries_ref") or "")
            if ref in found:
                raise RuntimeError(f"Duplicate stored Black Carbon timeseries identity: {ref}")
            found[ref] = row
        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return found


def owned_row_matches(existing: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return all(existing.get(key) == value for key, value in expected.items())


def plan_source_evidence(
    session: requests.Session,
    relevant: Sequence[CatalogueStation],
    bridge_rows: Mapping[str, Mapping[str, Any]],
    sos_rows: Mapping[str, Mapping[str, Any]],
    *,
    timeout: int,
    retries: int,
) -> Tuple[List[PlannedStation], Dict[str, int]]:
    planned: List[PlannedStation] = []
    counts = {"existing_site_ref_reused": 0, "site_ref_newly_discovered": 0, "unresolved_site_ref": 0}
    for index, station in enumerate(relevant, start=1):
        LOG.info("Resolving Black Carbon source evidence %s/%s: %s", index, len(relevant), station.uk_air_ref)
        site_ref = choose_existing_site_ref(
            station.uk_air_ref,
            bridge_rows.get(station.uk_air_ref),
            sos_rows.get(station.uk_air_ref),
        )
        if site_ref is None:
            site_ref = discover_site_ref(
                session, station.uk_air_ref, timeout=timeout, retries=retries
            )
            if site_ref.site_ref:
                counts["site_ref_newly_discovered"] += 1
            else:
                counts["unresolved_site_ref"] += 1
        else:
            counts["existing_site_ref_reused"] += 1
        supported: frozenset[str] = frozenset()
        supporting_files: Dict[str, Tuple[str, ...]] = {}
        if site_ref.site_ref:
            supported, supporting_files = discover_property_support(
                session, site_ref.site_ref, timeout=timeout, retries=retries
            )
        planned.append(
            PlannedStation(station, site_ref, supported, supporting_files)
        )
    return planned, counts


def count_existing_phenomena(core: Any, connector_id: int) -> int:
    source_labels = [str(config["source_label"]) for config in PROPERTY_CONFIG.values()]
    rows = response_rows(
        core.table("phenomena")
        .select("id,source_label,observed_property_id")
        .eq("connector_id", connector_id)
        .in_("source_label", source_labels)
        .execute(),
        "phenomena",
    )
    return sum(
        1 for row in rows if row.get("id") is not None and row.get("observed_property_id") is not None
    )


def retained_reference_rows(query: Any, context: str) -> List[Dict[str, Any]]:
    """Read the complete ordered reference set, without a default API row-limit gap."""
    found: List[Dict[str, Any]] = []
    while True:
        offset = len(found)
        rows = response_rows(query.range(offset, offset + PAGE_SIZE - 1).execute(), context)
        found.extend(rows)
        if len(rows) < PAGE_SIZE:
            return found


def verify_retained_reference_state(
    schemas: SupabaseSchemas, catalogue: Sequence[CatalogueStation] = ()
) -> None:
    """Prove existing authority is safe to retain; never repair or invent references.

    Only called after a source-acquisition failure, before any refresh writes.
    Missing bootstrap state, database errors and inconsistent identities stay fatal.
    Unresolved routing is allowed only for a station with no supported timeseries,
    as in the accepted reference contract. Historical unlisted timeseries survive.
    """
    metadata = resolve_metadata(schemas)
    connector_id = int(metadata["connector"]["id"])
    network_id = int(metadata["network"]["id"])
    station_rows = retained_reference_rows(
        schemas.core.table("stations")
        .select("id,connector_id,network_id,service_ref,station_ref,label")
        .eq("connector_id", connector_id).order("id"),
        "retained Black Carbon stations",
    )
    stations: Dict[int, str] = {}
    for row in station_rows:
        ref = normalize_uka(row.get("station_ref"))
        if (
            row.get("station_ref") != ref
            or row.get("service_ref") != SERVICE_REF
            or int(row.get("network_id") or 0) != network_id
            or not clean_str(row.get("label"))
            or ref in stations.values()
        ):
            raise RuntimeError("Unsafe retained Black Carbon station identity/metadata")
        stations[int(row["id"])] = ref
    catalogue_refs = {station.uk_air_ref for station in catalogue}
    if not stations or not catalogue_refs.issubset(set(stations.values())):
        raise RuntimeError("Required retained Black Carbon stations are missing")

    phenomena = response_rows(
        schemas.core.table("phenomena").select("id,source_label,observed_property_id")
        .eq("connector_id", connector_id)
        .in_("source_label", [config["source_label"] for config in PROPERTY_CONFIG.values()])
        .execute(),
        "retained Black Carbon phenomena",
    )
    phenomenon_ids: Dict[str, int] = {}
    for code, config in PROPERTY_CONFIG.items():
        matches = [row for row in phenomena if row.get("source_label") == config["source_label"]]
        if len(matches) != 1 or int(matches[0].get("observed_property_id") or 0) != int(
            metadata["properties_by_code"][code]["id"]
        ):
            raise RuntimeError("Required retained Black Carbon phenomenon mapping is missing/invalid")
        phenomenon_ids[code] = int(matches[0]["id"])

    bridges = retained_reference_rows(
        schemas.raw.table("ukair_bc_station_refs")
        .select(
            "station_id,uk_air_ref,site_ref,site_ref_source_url,site_ref_source_checked_at,"
            "source_snapshot_at,raw_payload"
        )
        .order("station_id"),
        "retained Black Carbon routing bridges",
    )
    sos_rows = fetch_sos_site_refs(schemas.raw, list(stations.values()))
    by_station: Dict[int, Dict[str, Any]] = {}
    expected_refs: Set[str] = set()
    for bridge in bridges:
        station_id = int(bridge["station_id"])
        ref = stations.get(station_id)
        if ref is None or bridge.get("uk_air_ref") != ref or station_id in by_station:
            raise RuntimeError("Unsafe retained Black Carbon routing identity")
        # Retaining a bridge must not conceal contradictory authoritative mappings.
        choose_existing_site_ref(ref, bridge, sos_rows.get(ref))
        site_ref = normalize_site_ref(bridge.get("site_ref"))
        payload = bridge.get("raw_payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("catalogue_row"), dict):
            raise RuntimeError("Retained Black Carbon source evidence is missing")
        source_row = payload["catalogue_row"]
        if (
            normalize_uka(source_row.get("UK-AIR ID")) != ref
            or not has_black_carbon_membership(split_networks(source_row.get("Networks")))
            or not clean_str(source_row.get("Site Name"))
            or not is_official_source_url(str(payload.get("catalogue_source_url") or ""))
            or not clean_str(payload.get("site_ref_provenance"))
        ):
            raise RuntimeError("Invalid retained Black Carbon catalogue evidence")
        snapshot = datetime.fromisoformat(
            str(bridge.get("source_snapshot_at") or "").replace("Z", "+00:00")
        )
        if snapshot.tzinfo is None:
            raise RuntimeError("Retained Black Carbon source snapshot lacks a timezone")
        if site_ref and (
            bridge.get("site_ref") != site_ref
            or not is_official_source_url(str(bridge.get("site_ref_source_url") or ""))
            or not clean_str(bridge.get("site_ref_source_checked_at"))
        ):
            raise RuntimeError("Retained Black Carbon routing provenance is missing/invalid")
        supported = payload.get("supported_properties")
        files = payload.get("supporting_files")
        if (
            not isinstance(supported, list)
            or any(not isinstance(code, str) or code not in PROPERTY_CONFIG for code in supported)
            or len(set(supported)) != len(supported)
            or not isinstance(files, dict)
            or set(files) != set(supported)
            or (supported and not site_ref)
        ):
            raise RuntimeError("Retained Black Carbon property evidence is missing/invalid")
        for code in supported:
            token = re.escape(PROPERTY_CONFIG[code]["filename_token"])
            pattern = rf"{re.escape(site_ref)}_{token}_([0-9]{{4}})\.csv"
            if not isinstance(files[code], list) or not files[code]:
                raise RuntimeError("Retained Black Carbon supporting files are missing")
            for filename in files[code]:
                match = (
                    re.fullmatch(pattern, filename, re.IGNORECASE)
                    if isinstance(filename, str) else None
                )
                if not match or int(match.group(1)) < SUPPORTED_FROM.year:
                    raise RuntimeError("Invalid retained Black Carbon supporting file identity")
            expected_refs.add(timeseries_ref(ref, code))
        by_station[station_id] = bridge
    if set(by_station) != set(stations) or not expected_refs:
        raise RuntimeError("Required retained Black Carbon routing/property evidence is missing")

    stored_timeseries = retained_reference_rows(
        schemas.core.table("timeseries")
        .select("id,station_id,service_ref,timeseries_ref,uom,phenomenon_id,observed_property_id,extras")
        .eq("connector_id", connector_id).order("id"),
        "retained Black Carbon timeseries",
    )
    found_refs: Set[str] = set()
    for row in stored_timeseries:
        station_id = int(row["station_id"])
        ref = str(row.get("timeseries_ref") or "")
        code = ref.rsplit(":", 1)[-1]
        if station_id not in stations or code not in PROPERTY_CONFIG or ref in found_refs:
            raise RuntimeError("Invalid retained Black Carbon timeseries identity")
        bridge = by_station[station_id]
        expected = {
            "service_ref": SERVICE_REF,
            "timeseries_ref": timeseries_ref(stations[station_id], code),
            "uom": "ug/m3",
            "phenomenon_id": phenomenon_ids[code],
            "observed_property_id": int(metadata["properties_by_code"][code]["id"]),
        }
        extras = row.get("extras")
        if (
            not bridge.get("site_ref")
            or not owned_row_matches(row, expected)
            or not isinstance(extras, dict)
            or not owned_row_matches(extras, {
                "uk_air_ref": stations[station_id],
                "site_ref": bridge["site_ref"],
                "source_property": code,
            })
        ):
            raise RuntimeError("Unsafe retained Black Carbon timeseries mapping")
        found_refs.add(ref)
    if not expected_refs.issubset(found_refs):
        raise RuntimeError("Required retained Black Carbon timeseries are missing")


def run_refresh(args: argparse.Namespace) -> Dict[str, Any]:
    session = requests.Session()
    session.headers.update({"User-Agent": args.user_agent})
    try:
        catalogue, catalogue_source_url, snapshot_at = acquire_catalogue(
            session,
            search_url=args.search_url,
            csv_url=args.csv_url,
            catalogue_csv=args.catalogue_csv,
            timeout=args.timeout,
            retries=args.retries,
        )
    except SourceAcquisitionUnavailable as exc:
        schemas = SupabaseSchemas.from_client(create_supabase_client())
        verify_retained_reference_state(schemas)
        raise RetainedReferenceSourceUnavailable(
            exc.source_url, exc.failure_class, exc.attempts, exc.http_status
        ) from exc
    relevant = sorted((station for station in catalogue if station.relevant), key=lambda item: item.uk_air_ref)
    if not relevant:
        raise RuntimeError("Black Carbon catalogue contains no stations relevant to 2020 onwards")

    client = create_supabase_client()
    schemas = SupabaseSchemas.from_client(client)
    refs = [station.uk_air_ref for station in relevant]
    bridge_rows = fetch_existing_bridge_rows(schemas.raw, refs)
    sos_rows = fetch_sos_site_refs(schemas.raw, refs)
    try:
        planned, site_ref_counts = plan_source_evidence(
            session,
            relevant,
            bridge_rows,
            sos_rows,
            timeout=args.timeout,
            retries=args.retries,
        )
    except SourceAcquisitionUnavailable as exc:
        verify_retained_reference_state(schemas, relevant)
        raise RetainedReferenceSourceUnavailable(
            exc.source_url, exc.failure_class, exc.attempts, exc.http_status
        ) from exc
    metadata = resolve_metadata(schemas)
    connector_id = int(metadata["connector"]["id"])
    network_id = int(metadata["network"]["id"])
    existing_stations = fetch_stations(schemas.core, connector_id, refs)
    existing_timeseries = fetch_timeseries(schemas.core, connector_id)
    expected_refs = {
        timeseries_ref(item.catalogue.uk_air_ref, property_code)
        for item in planned
        for property_code in item.supported_properties
    }
    preserved_unlisted_refs = {
        ref
        for ref in existing_timeseries
        if re.fullmatch(r"UKA[0-9]+:(?:bc|uv370)", ref) and ref not in expected_refs
    }

    summary: Dict[str, Any] = {
        "mode": "to_supabase" if args.to_supabase else "dry_run",
        "source_catalogue_rows": len(catalogue),
        "relevant_2020_plus_stations": len(relevant),
        "active_stations": sum(1 for station in relevant if station.active),
        "closed_relevant_stations": sum(1 for station in relevant if not station.active),
        **site_ref_counts,
        "bc_supported_stations": sum(1 for item in planned if "bc" in item.supported_properties),
        "uv370_supported_stations": sum(1 for item in planned if "uv370" in item.supported_properties),
        "bc_timeseries_expected": sum(1 for ref in expected_refs if ref.endswith(":bc")),
        "uv370_timeseries_expected": sum(1 for ref in expected_refs if ref.endswith(":uv370")),
        "preserved_unlisted_timeseries": len(preserved_unlisted_refs),
        "stations_upserted_or_checked": len(relevant),
        "phenomena_planned": len(PROPERTY_CONFIG),
        "phenomena_resolved": count_existing_phenomena(schemas.core, connector_id),
        "timeseries_inserted_or_updated": 0,
        "missing_final_identities": 0,
        "mismatched_final_identities": 0,
        "changed_existing_station_ids": 0,
        "changed_existing_timeseries_ids": 0,
        "ok": True,
    }
    if not args.to_supabase:
        return summary

    station_rows = [
        build_station_row(
            item,
            connector_id=connector_id,
            network_id=network_id,
            snapshot_at=snapshot_at,
            existing=existing_stations.get(item.catalogue.uk_air_ref),
        )
        for item in planned
    ]
    for batch in chunked(station_rows):
        schemas.core.table("stations").upsert(
            list(batch), on_conflict="connector_id,service_ref,station_ref"
        ).execute()
    final_stations = fetch_stations(schemas.core, connector_id, refs)
    missing_station_refs = sorted(set(refs) - set(final_stations))
    changed_station_ids = sorted(
        ref
        for ref, row in existing_stations.items()
        if ref in final_stations and int(row["id"]) != int(final_stations[ref]["id"])
    )
    if missing_station_refs or changed_station_ids:
        raise RuntimeError(
            "Station identity verification failed: "
            f"missing={len(missing_station_refs)} changed_ids={len(changed_station_ids)}"
        )
    station_ids = {ref: int(row["id"]) for ref, row in final_stations.items()}

    bridge_payload = [
        build_bridge_row(
            item,
            station_id=station_ids[item.catalogue.uk_air_ref],
            source_url=catalogue_source_url,
            snapshot_at=snapshot_at,
        )
        for item in planned
    ]
    for batch in chunked(bridge_payload):
        schemas.raw.table("ukair_bc_station_refs").upsert(
            list(batch), on_conflict="station_id"
        ).execute()

    phenomena_rows = build_phenomena_rows(connector_id)
    public = client.schema(os.getenv("UK_AQ_PUBLIC_SCHEMA") or "uk_aq_public")
    diagnostics = upsert_phenomena_via_rpc(public, phenomena_rows)
    phenomenon_ids, property_ids = validate_phenomena_results(phenomena_rows, diagnostics)
    summary["phenomena_resolved"] = len(phenomenon_ids)
    for property_code, canonical in metadata["properties_by_code"].items():
        if property_ids[property_code] != int(canonical["id"]):
            raise RuntimeError(
                f"Central phenomena RPC returned wrong observed_property_id for {property_code}"
            )

    timeseries_rows = build_timeseries_rows(
        planned,
        connector_id=connector_id,
        station_ids=station_ids,
        phenomenon_ids=phenomenon_ids,
        property_ids=property_ids,
    )
    expected_by_ref = {str(row["timeseries_ref"]): row for row in timeseries_rows}
    rows_to_upsert = [
        row
        for ref, row in expected_by_ref.items()
        if ref not in existing_timeseries or not owned_row_matches(existing_timeseries[ref], row)
    ]
    for batch in chunked(rows_to_upsert):
        schemas.core.table("timeseries").upsert(
            list(batch), on_conflict="connector_id,service_ref,timeseries_ref"
        ).execute()
    summary["timeseries_inserted_or_updated"] = len(rows_to_upsert)

    final_bridge_rows = fetch_existing_bridge_rows(schemas.raw, refs)
    final_timeseries = fetch_timeseries(schemas.core, connector_id)
    missing_bridge_refs = sorted(set(refs) - set(final_bridge_rows))
    mismatched_bridge_refs = sorted(
        item.catalogue.uk_air_ref
        for item in planned
        if item.catalogue.uk_air_ref in final_bridge_rows
        and (
            int(final_bridge_rows[item.catalogue.uk_air_ref]["station_id"])
            != station_ids[item.catalogue.uk_air_ref]
            or normalize_uka(final_bridge_rows[item.catalogue.uk_air_ref]["uk_air_ref"])
            != item.catalogue.uk_air_ref
            or normalize_site_ref(final_bridge_rows[item.catalogue.uk_air_ref].get("site_ref"))
            != item.site_ref.site_ref
        )
    )
    missing_timeseries_refs = sorted(set(expected_by_ref) - set(final_timeseries))
    mismatched_timeseries_refs = sorted(
        ref
        for ref, expected in expected_by_ref.items()
        if ref in final_timeseries and not owned_row_matches(final_timeseries[ref], expected)
    )
    changed_timeseries_ids = sorted(
        ref
        for ref, row in existing_timeseries.items()
        if ref in final_timeseries and int(row["id"]) != int(final_timeseries[ref]["id"])
    )
    summary["missing_final_identities"] = len(missing_bridge_refs) + len(missing_timeseries_refs)
    summary["mismatched_final_identities"] = len(mismatched_bridge_refs) + len(mismatched_timeseries_refs)
    summary["changed_existing_station_ids"] = len(changed_station_ids)
    summary["changed_existing_timeseries_ids"] = len(changed_timeseries_ids)
    summary["ok"] = not any(
        (
            missing_bridge_refs,
            mismatched_bridge_refs,
            missing_timeseries_refs,
            mismatched_timeseries_refs,
            changed_station_ids,
            changed_timeseries_ids,
        )
    )
    return summary


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh dedicated UK-AIR Black Carbon reference metadata."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--to-supabase", action="store_true", help="Apply the validated reference plan."
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Acquire and plan reference metadata without mutations (default).",
    )
    parser.add_argument(
        "--search-url",
        default=os.getenv("UKAIR_BC_SITE_SEARCH_URL") or DEFAULT_SEARCH_URL,
        help="Official UK-AIR Black Carbon advanced site-search URL.",
    )
    parser.add_argument("--csv-url", help="Official catalogue CSV URL; skips HTML link discovery.")
    parser.add_argument(
        "--catalogue-csv",
        help="Local official catalogue CSV for deterministic parsing/planning checks.",
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument(
        "--user-agent", default=DEFAULT_USER_AGENT
    )
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.retries <= 0:
        parser.error("--timeout and --retries must be positive")
    return args


def emit_summary(summary: Mapping[str, Any]) -> None:
    print(
        "UKAIR_BC_REFERENCE_SUMMARY_JSON "
        + json.dumps(dict(summary), separators=(",", ":"), sort_keys=True),
        flush=True,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        summary = run_refresh(args)
    except RetainedReferenceSourceUnavailable as exc:
        LOG.warning("%s; existing reference state verified safe to retain", exc)
        emit_summary({
            "mode": "to_supabase" if args.to_supabase else "dry_run",
            "ok": False,
            "error": str(exc),
            "failure_class": exc.failure_class,
            "source_url": exc.source_url,
            "attempts": exc.attempts,
            **({"http_status": exc.http_status} if exc.http_status is not None else {}),
            "reference_state_retained": True,
        })
        if os.getenv("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
                handle.write(f"failure_class={exc.failure_class}\n")
        return UPSTREAM_UNAVAILABLE_EXIT_CODE
    except Exception as exc:
        LOG.error("UK-AIR Black Carbon reference refresh failed: %s", exc)
        emit_summary(
            {
                "mode": "to_supabase" if args.to_supabase else "dry_run",
                "ok": False,
                "error": str(exc)[:500],
            }
        )
        return 1
    emit_summary(summary)
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
