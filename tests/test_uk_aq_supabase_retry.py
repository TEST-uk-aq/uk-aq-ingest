from __future__ import annotations

import pytest

from scripts.uk_aq_supabase import retry_supabase_operation


class HttpFailure(Exception):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


def test_retries_transient_postgrest_status_with_short_increasing_delays() -> None:
    attempts = 0
    delays: list[float] = []

    def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise HttpFailure(504)
        return "ok"

    assert retry_supabase_operation("test lookup", operation, sleep=delays.append) == "ok"
    assert attempts == 3
    assert delays == [0.25, 0.5]


def test_does_not_retry_permanent_postgrest_error() -> None:
    attempts = 0

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise HttpFailure(400)

    with pytest.raises(HttpFailure):
        retry_supabase_operation("test lookup", operation, sleep=lambda _delay: None)
    assert attempts == 1


def test_retries_connection_failure_only_up_to_three_attempts() -> None:
    attempts = 0
    delays: list[float] = []

    def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise ConnectionError("connection reset")

    with pytest.raises(ConnectionError):
        retry_supabase_operation("test upsert", operation, sleep=delays.append)
    assert attempts == 3
    assert delays == [0.25, 0.5]
