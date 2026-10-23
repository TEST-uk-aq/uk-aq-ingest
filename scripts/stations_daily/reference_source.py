"""Bounded source-only acquisition for opt-in Daily Stations reference commands."""

from __future__ import annotations

import html
import re
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urljoin, urlsplit

import requests
from urllib3.exceptions import ProxyError as UpstreamProxyError
from urllib3.exceptions import SSLError as UpstreamSSLError
from urllib3.exceptions import TimeoutError as UpstreamTimeoutError

SOURCE_ATTEMPTS = 3
SOURCE_RETRY_DELAYS = (1, 2)
HTML_LIMIT = 16 * 1024
METADATA_LIMIT = 32 * 1024 * 1024
RETRY_HTTP_STATUSES = frozenset((408, 429, 500, 502, 503, 504))
REDIRECT_STATUSES = frozenset((301, 302, 303, 307, 308))
ACCESS_CHALLENGE = re.compile(
    r"login|log[\s_-]*in|sign[\s_-]*in|authenticat|authori[sz]|access[\s_-]*denied|"
    r"forbidden|unauthori[sz]ed|captcha|verif(?:y|ication)|challenge|\bsso\b|"
    r"cf[-_]chl|checking (?:your )?browser|enable javascript|type\s*=\s*['\"]?password|"
    r"access.{0,16}(?:denied|blocked)|request.{0,16}blocked|bot detection|security check|"
    r"permission.{0,16}(?:denied|required|error)|\b(?:denied|blocked|restricted|password)\b|"
    r"<form\b|<input\b|turnstile|just\s+a\s+moment|"
    r"are\s+you\s+(?:a\s+)?(?:human|robot)|configuration\s+error|misconfigur|"
    r"(?:invalid|missing|expired).{0,20}(?:api[\s_-]*key|token|credential)",
    re.IGNORECASE,
)


class SourceResponseError(RuntimeError):
    """Fatal source/configuration error, with bounded diagnostics."""


class SourceUnavailable(RuntimeError):
    def __init__(self, failure_class: str, attempts: int = 1) -> None:
        super().__init__(failure_class)
        self.failure_class = failure_class
        self.attempts = attempts


class ReferencePrerequisiteError(RuntimeError):
    """Rate-budget or provider-safety prerequisites are never source outages."""


def _origin(url: str) -> tuple:
    parts = urlsplit(url)
    return parts.scheme, parts.hostname, parts.port or 443


def _source_url(
    url: str, origin_url: Optional[str] = None, *, canonical_url: Optional[str] = None
) -> str:
    # Reject ambiguous spellings before urljoin/Requests can normalise them.
    if (
        not url
        or len(url) > 4096
        or not url.isascii()
        or re.search(r"[\s\x00-\x1f\x7f\\%?#]", url)
        or url.startswith("//")
    ):
        raise SourceResponseError("Ambiguous Reference source destination")
    try:
        supplied = urlsplit(url)
        if (
            (supplied.scheme and not supplied.netloc)
            or ";" in supplied.path
            or re.search(r"//|(?:^|/)\.{1,2}(?:/|$)", supplied.path)
        ):
            raise ValueError
        resolved = urljoin(origin_url, url) if origin_url else url
        parts = urlsplit(resolved)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.port not in (None, 443)
            or parts.netloc.lower() not in (parts.hostname, f"{parts.hostname}:443")
            or ACCESS_CHALLENGE.search(parts.path)
        ):
            raise ValueError
        if origin_url:
            source = urlsplit(origin_url)
            if _origin(resolved) != _origin(origin_url):
                if (
                    canonical_url is None
                    or _origin(resolved) != _origin(canonical_url)
                    or parts.path != source.path
                ):
                    raise ValueError
    except ValueError:
        raise SourceResponseError("Unsafe Reference source destination") from None
    return resolved


def acquire_reference_bytes(
    session: Any,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    expected_format: str = "rdata",
    timeout: Any = (10, 60),
    before_request: Optional[Callable[[], None]] = None,
    response_observer: Optional[Callable[[Any], None]] = None,
    canonical_origin: Optional[str] = None,
    official_maintenance: bool = False,
) -> bytes:
    """Acquire a source; origin/maintenance exceptions require official caller opt-in.

    canonical_origin must be independently configured provider authority. It grants
    only a first-hop, same-resource change and never forwards caller credentials.
    """
    source_url = _source_url(url)
    canonical_url = None
    if canonical_origin is not None:
        # Authority comes from the official provider profile, never a Location.
        canonical = urlsplit(_source_url(canonical_origin))
        if canonical.path not in ("", "/") or params or expected_format != "rdata":
            raise SourceResponseError("Invalid Reference canonical origin configuration")
        canonical_url = canonical._replace(path=urlsplit(source_url).path).geturl()
    current_url = source_url
    request_client = session
    request_headers = headers
    request_params = params
    body: Optional[bytearray] = None
    try:
        for hop in range(3):
            if before_request is not None:
                try:
                    before_request()
                except Exception:
                    # In particular, a DB-backed token reservation may itself
                    # raise requests.Timeout. It must never enter a source catch.
                    raise ReferencePrerequisiteError(
                        "Reference request safety prerequisite failed"
                    ) from None
            with request_client.get(
                current_url,
                timeout=timeout,
                params=request_params,
                headers=request_headers,
                allow_redirects=False,
                stream=True,
            ) as response:
                if response_observer is not None:
                    try:
                        response_observer(response)
                    except Exception:
                        raise ReferencePrerequisiteError(
                            "Reference provider-safety response handling failed"
                        ) from None
                if (
                    response.history
                    or response.url
                    != requests.Request("GET", current_url, params=request_params).prepare().url
                ):
                    raise SourceResponseError(
                        "Unexpected Reference response destination"
                    )
                if any(
                    name in response.headers
                    for name in (
                        "WWW-Authenticate",
                        "Proxy-Authenticate",
                        "Refresh",
                        "CF-Mitigated",
                    )
                ) or any(
                    ACCESS_CHALLENGE.search(str(value))
                    for value in response.headers.values()
                ):
                    raise SourceResponseError(
                        "Reference response has access/challenge metadata"
                    )
                status = response.status_code
                if status in REDIRECT_STATUSES:
                    if hop == 2 or not response.headers.get("Location"):
                        raise SourceResponseError(
                            "Invalid or excessive Reference redirects"
                        )
                    # Validate before any request is sent to the destination.
                    location = response.headers["Location"]
                    target = _source_url(
                        location, current_url,
                        canonical_url=canonical_url if hop == 0 else None,
                    )
                    if official_maintenance and urlsplit(target).path not in (
                        urlsplit(source_url).path, "/maintenance/",
                    ):
                        raise SourceResponseError("Unexpected official Reference redirect path")
                    if _origin(target) != _origin(current_url):
                        # Public metadata only: no caller headers, parameters,
                        # auth, cookies or session state cross the origin boundary.
                        request_client = requests
                        request_headers = None
                        request_params = None
                    # Challenge evidence in redirect bodies also excludes isolation.
                    redirect_body = bytearray()
                    for chunk in response.iter_content(chunk_size=HTML_LIMIT + 1):
                        redirect_body.extend(chunk)
                        if len(redirect_body) > HTML_LIMIT:
                            raise SourceResponseError("Reference redirect exceeds inspection limit")
                    redirect_text = html.unescape(redirect_body.decode("utf-8", errors="replace"))
                    if ACCESS_CHALLENGE.search(redirect_text) or re.search(
                        r"http-equiv\s*=\s*['\"]?refresh\b", redirect_text, re.IGNORECASE
                    ):
                        raise SourceResponseError("Reference redirect contains access/challenge evidence")
                    current_url = target
                    continue
                if "Location" in response.headers:
                    raise SourceResponseError("Unexpected Reference redirect metadata")
                if status != 200 and status not in RETRY_HTTP_STATUSES:
                    raise SourceResponseError(f"Reference source HTTP {status}")
                content_type = (
                    response.headers.get("Content-Type", "")
                    .split(";", 1)[0]
                    .strip()
                    .lower()
                )
                is_html_type = content_type in ("text/html", "application/xhtml+xml")
                body = bytearray()
                for chunk in response.iter_content(chunk_size=HTML_LIMIT + 1):
                    body.extend(chunk)
                    limit = (
                        HTML_LIMIT
                        if is_html_type or status in RETRY_HTTP_STATUSES
                        else METADATA_LIMIT
                    )
                    if len(body) > limit:
                        raise SourceResponseError(
                            "Reference source exceeds the acquisition limit"
                        )
                if is_html_type or body[:HTML_LIMIT].lstrip().startswith(b"<"):
                    # Never let an unqualified HTML/login page reach the RData reader.
                    if not is_html_type or len(body) > HTML_LIMIT:
                        raise SourceResponseError("Unqualified Reference HTML response")
                    try:
                        document = html.unescape(body.decode("utf-8-sig"))
                    except UnicodeDecodeError:
                        raise SourceResponseError(
                            "Unsupported Reference HTML encoding"
                        ) from None
                    if ACCESS_CHALLENGE.search(document) or re.search(
                        r"http-equiv\s*=\s*['\"]?refresh\b", document, re.IGNORECASE
                    ):
                        raise SourceResponseError(
                            "Reference HTML contains access/challenge evidence"
                        )
                    if status in RETRY_HTTP_STATUSES:
                        raise SourceUnavailable(f"http_{status}")
                    if (
                        re.match(
                            r"\s*(?:<!doctype\s+html[^>]*>\s*)?<html\b[^>]*>",
                            document,
                            re.IGNORECASE,
                        )
                        and re.search(
                            r"<(head|body)\b[^>]*>.*</\1\s*>",
                            document,
                            re.IGNORECASE | re.DOTALL,
                        )
                        and re.search(r"</html\s*>\s*$", document, re.IGNORECASE)
                    ):
                        raise SourceUnavailable(
                            "official_maintenance_redirect"
                            if official_maintenance and hop > 0
                            and urlsplit(current_url).path == "/maintenance/"
                            else "unexpected_source_html"
                        )
                    raise SourceResponseError("Unrecognisable Reference HTML document")
                if status in RETRY_HTTP_STATUSES:
                    if ACCESS_CHALLENGE.search(body.decode("utf-8", errors="replace")):
                        raise SourceResponseError(
                            "Reference failure response contains access/challenge evidence"
                        )
                    raise SourceUnavailable(f"http_{status}")
                # Empty, truncated or unrelated text is not eligible as unreadable RData.
                if expected_format == "rdata" and (
                    len(body) < 8
                    or not bytes(body).startswith(
                        (
                            b"\x1f\x8b\x08",
                            b"BZh",
                            b"\xfd7zXZ\x00",
                            b"RDX2\n",
                            b"RDX3\n",
                            b"RDA2\n",
                            b"RDA3\n",
                            b"RDB2\n",
                            b"RDB3\n",
                        )
                    )
                ):
                    raise SourceResponseError(
                        "Unrecognised or incomplete RData payload"
                    )
                break
    except (requests.exceptions.SSLError, requests.exceptions.ProxyError):
        raise SourceResponseError("Reference source TLS/proxy failure") from None
    except requests.exceptions.Timeout:
        raise SourceUnavailable("upstream_timeout") from None
    except requests.exceptions.ConnectionError as exc:
        # Requests wraps streamed read timeouts (and sometimes TLS/proxy errors)
        # inside ConnectionError. Inspect bounded typed causes, never error text.
        pending: List[BaseException] = [exc]
        timeout = False
        seen = set()
        for _ in range(16):
            if not pending:
                break
            error = pending.pop()
            if id(error) in seen:
                continue
            seen.add(id(error))
            if isinstance(
                error,
                (
                    UpstreamSSLError,
                    UpstreamProxyError,
                    requests.exceptions.SSLError,
                    requests.exceptions.ProxyError,
                ),
            ):
                raise SourceResponseError(
                    "Reference source TLS/proxy failure"
                ) from None
            timeout = timeout or isinstance(
                error, (UpstreamTimeoutError, requests.exceptions.Timeout)
            )
            pending.extend(
                item
                for item in (
                    error.__cause__,
                    error.__context__,
                    getattr(error, "reason", None),
                    *error.args,
                )
                if isinstance(item, BaseException)
            )
        if pending:
            raise SourceResponseError(
                "Unresolved Reference transport failure"
            ) from None
        raise SourceUnavailable(
            "upstream_timeout" if timeout else "upstream_connection"
        ) from None
    if body is None:
        raise SourceResponseError("Reference acquisition did not produce a response body")
    return bytes(body)
