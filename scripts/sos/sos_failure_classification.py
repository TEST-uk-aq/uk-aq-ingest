"""Narrow process-level classification for retry-exhausted UK-AIR SOS outages."""

from typing import NoReturn

import requests


SOS_UPSTREAM_UNAVAILABLE_EXIT_CODE = 75
SOS_UPSTREAM_UNAVAILABLE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})


class SosUpstreamUnavailableError(RuntimeError):
    """The remote SOS service remained unavailable after normal retries."""


def raise_for_retry_exhausted_transport(
    exc: requests.RequestException,
    request_label: str,
) -> NoReturn:
    """Classify only connection/timeout failures as upstream unavailability."""
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        raise SosUpstreamUnavailableError(
            f"UK-AIR SOS transport unavailable after retries: {request_label}"
        ) from exc
    raise exc


def raise_for_retry_exhausted_status(status_code: int, request_label: str) -> NoReturn:
    """Classify only the explicitly retryable upstream HTTP statuses."""
    if status_code in SOS_UPSTREAM_UNAVAILABLE_HTTP_STATUSES:
        raise SosUpstreamUnavailableError(
            f"UK-AIR SOS returned HTTP {status_code} after retries: {request_label}"
        )
    raise ValueError(f"HTTP {status_code} is not an isolatable UK-AIR SOS failure")
