"""Narrow process-level classification for retry-exhausted UK-AIR SOS outages."""

import html
import re
from typing import NoReturn, Optional
from urllib.parse import SplitResult, urljoin, urlsplit

import requests


SOS_UPSTREAM_UNAVAILABLE_EXIT_CODE = 75
SOS_UPSTREAM_UNAVAILABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
SOS_HTML_INSPECTION_LIMIT = 16384


class SosUpstreamUnavailableError(RuntimeError):
    """The remote SOS service remained unavailable after normal retries."""


def response_diagnostics(response: requests.Response) -> str:
    """Bounded HTTP context; exclude header parameters, URLs and body text."""
    media_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    if not media_type:
        media_type = "missing"
    elif len(media_type) > 100 or not re.fullmatch(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", media_type):
        media_type = "invalid"
    return f"HTTP {response.status_code}; Content-Type={media_type}"


_ACCESS_CHALLENGE = re.compile(
    r"<form\b|<input\b|\b(?:authenticat\w*|(?:un)?authori[sz]\w*|"
    r"password|log[ _-]?in|sign[ _-]?in|sso|(?:access|permission)\s+(?:is\s+)?denied|"
    r"forbidden|denied|blocked|restricted|captcha|verification|401|403|"
    r"permission\s+(?:error|required)|"
    r"verify\s+(?:you|that)|checking\s+(?:your\s+)?browser|just\s+a\s+moment|"
    r"are\s+you\s+(?:a\s+)?(?:human|robot)|(?:bot|browser|security)\s+(?:check|challenge)|"
    r"turnstile|recaptcha|hcaptcha|"
    r"challenge(?:-platform)?|configuration\s+error|misconfigur\w*)\b",
    re.I,
)


def _source_url(
    url: str, *, allow_query: bool = False, relative_to: Optional[str] = None
) -> Optional[SplitResult]:
    """Reject ambiguous URL forms before comparing configured HTTPS origins."""
    if not url or len(url) > 4096 or not url.isascii() or re.search(r"[\s\\]", url):
        return None
    if "#" in url or (not allow_query and "?" in url):
        return None
    try:
        parts = urlsplit(url)
        if re.search(r"%|//|(?:^|/)\.{1,2}(?:/|$)", parts.path):
            return None
        if relative_to is not None:
            if parts.scheme and not parts.netloc:
                return None
            parts = urlsplit(urljoin(relative_to, url))
        port = parts.port if parts.port is not None else 443
        if parts.scheme != "https" or not parts.hostname or parts.username is not None:
            return None
        if not port or "%" in parts.netloc:
            return None
        host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
        if parts.netloc.lower() not in (host, f"{host}:{port}"):
            return None
    except ValueError:
        return None
    return parts


def _origin(parts: SplitResult) -> tuple:
    return parts.scheme, parts.hostname, parts.port if parts.port is not None else 443


def _eligible_response_route(
    response: requests.Response, base_url: str, request_url: str
) -> bool:
    base = _source_url(base_url)
    requested = _source_url(request_url)
    if base is None or requested is None or _origin(base) != _origin(requested):
        return False
    if not requested.path.startswith(base.path.rstrip("/") + "/"):
        return False
    history = response.history
    if len(history) > 2:
        return False
    chain = [*history, response]
    for index, item in enumerate(chain):
        actual = _source_url(item.url, allow_query=index == 0)
        if actual is None or _origin(actual) != _origin(base):
            return False
        if index == 0:
            if actual.path != requested.path:
                return False
        elif actual.path != "/maintenance/":
            return False
        if any(key in item.headers for key in (
            "WWW-Authenticate", "Proxy-Authenticate", "Refresh", "CF-Mitigated"
        )):
            return False
        if index < len(history):
            body = item.content
            if len(body) > SOS_HTML_INSPECTION_LIMIT:
                return False
            if _ACCESS_CHALLENGE.search(html.unescape(body.decode("utf-8", errors="replace"))):
                return False
        if _ACCESS_CHALLENGE.search(actual.path):
            return False
        if index < len(history):
            if item.status_code not in (301, 302, 303, 307, 308):
                return False
            location = item.headers.get("Location", "")
            # Only the provider's explicit maintenance destination is expected.
            target = _source_url(location, relative_to=item.url)
            if (target is None or _origin(target) != _origin(base)
                    or target.path != "/maintenance/"):
                return False
        elif "Location" in item.headers:
            return False
    return True


def invalid_json_unavailability_reason(
    response: requests.Response, *, base_url: str, request_url: str
) -> Optional[str]:
    """Identify unusable SOS HTML after JSON decoding fails, not its root cause.

    Callers retain the permanent pre-write isolation gate and normal retries.
    Only bounded same-origin maintenance redirects are eligible.
    """
    if response.status_code != 200:
        return None
    if not _eligible_response_route(response, base_url, request_url):
        return None
    media_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    if media_type not in ("", "application/json", "text/html", "application/xhtml+xml", "text/plain"):
        return None
    body = response.content
    if not body.strip():
        # Empty content does not establish that the source returned HTML.
        return None
    if len(body) > SOS_HTML_INSPECTION_LIMIT:
        return None
    text = body.decode("utf-8", errors="replace").lower()
    if not re.match(r"\s*(?:<!doctype\s+html(?:\s[^<>]*)?>|<html(?:\s[^<>]*)?>)", text):
        return None
    if _ACCESS_CHALLENGE.search(html.unescape(text)):
        return None
    if "<!--" in re.sub(r"<!--.*?-->", "", text, flags=re.S):
        return None
    return "unexpected_html_response"


def raise_for_retry_exhausted_transport(
    exc: requests.RequestException,
    request_label: str,
) -> NoReturn:
    """Classify only connection/timeout failures as upstream unavailability."""
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        raise SosUpstreamUnavailableError(
            f"UK-AIR SOS transport unavailable after retries: {request_label}"
        ) from None
    # Request exception messages can contain credential-bearing URLs.
    raise requests.RequestException(
        f"UK-AIR SOS request failed after retries ({type(exc).__name__}): {request_label}"
    ) from None


def raise_for_retry_exhausted_status(status_code: int, request_label: str) -> NoReturn:
    """Classify only the explicitly retryable upstream HTTP statuses."""
    if status_code in SOS_UPSTREAM_UNAVAILABLE_HTTP_STATUSES:
        raise SosUpstreamUnavailableError(
            f"UK-AIR SOS returned HTTP {status_code} after retries: {request_label}"
        )
    raise ValueError(f"HTTP {status_code} is not an isolatable UK-AIR SOS failure")
