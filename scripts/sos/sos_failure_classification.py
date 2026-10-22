"""Narrow process-level classification for retry-exhausted UK-AIR SOS outages."""

import html
import re
from typing import NoReturn, Optional

import requests


SOS_UPSTREAM_UNAVAILABLE_EXIT_CODE = 75
SOS_UPSTREAM_UNAVAILABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})


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


def invalid_json_unavailability_reason(response: requests.Response) -> Optional[str]:
    """Recognise only explicit outage HTML, never empty bodies or arbitrary bad JSON.

    Redirects and authentication/access indicators are ambiguous even when the
    final page mentions maintenance. Large/unfamiliar pages stay fail-closed.
    This is called only after JSON decoding actually fails.
    """
    if response.status_code != 200 or response.history:
        return None
    if "WWW-Authenticate" in response.headers or "Location" in response.headers:
        return None
    media_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    if media_type not in ("", "application/json", "text/html", "application/xhtml+xml", "text/plain"):
        return None
    body = response.content
    if not body.strip():
        # Neither an empty 200 nor its media type positively identifies an outage.
        return None
    if len(body) > 16384:
        return None
    text = body.decode("utf-8", errors="replace").lower()
    if not re.match(r"\s*(?:<!doctype\s+html\b|<html\b)", text):
        return None
    if re.search(
        r"<form\b|<input\b|\b(?:authenticat\w*|(?:un)?authori[sz]\w*|"
        r"password|log[ -]?in|sign[ -]?in|(?:access|permission)\s+(?:is\s+)?denied|"
        r"forbidden|denied|blocked|restricted|captcha|verification|401|403)\b",
        html.unescape(text),
    ):
        return None
    # Require an explicit page title/primary heading, not a passing mention in
    # an otherwise unexpected page, script or comment.
    if "<!--" in re.sub(r"<!--.*?-->", "", text, flags=re.S):
        return None
    text = re.sub(r"<!--.*?-->|<(script|style)\b[^>]*>.*?</\1\s*>", "", text, flags=re.S)
    for _, heading in re.findall(r"<(title|h1)\b[^>]*>(.*?)</\1\s*>", text, flags=re.S):
        heading = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", heading)).split()).strip(" .!")
        if re.fullmatch(
            r"(?:uk-air\s*[:|–-]\s*)?(?:503\s*[-:]?\s*)?"
            r"(?:service (?:temporarily )?unavailable|temporarily unavailable|"
            r"(?:site|website|service) (?:is )?(?:under|down for) maintenance|"
            r"scheduled maintenance|maintenance in progress)",
            heading,
        ):
            return "temporary_outage_html"
    return None


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
