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


def _source_url(url: str, origin_url: Optional[str] = None) -> str:
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
            if (parts.scheme, parts.hostname, parts.port or 443) != (
                source.scheme,
                source.hostname,
                source.port or 443,
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
) -> bytes:
    source_url = _source_url(url)
    current_url = source_url
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
            with session.get(
                current_url,
                timeout=timeout,
                params=params,
                headers=headers,
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
                    response.url
                    != requests.Request("GET", current_url, params=params).prepare().url
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
                    current_url = _source_url(response.headers["Location"], current_url)
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
                        raise SourceUnavailable("unexpected_source_html")
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
    return bytes(body)
