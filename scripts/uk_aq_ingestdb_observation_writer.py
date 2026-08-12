"""Bounded, idempotent IngestDB observation writes for Python ingests."""

from __future__ import annotations

import json
import random as random_module
import re
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

DEFAULT_CONFIG = {
    "attempts": 3,
    "retry_base_ms": 500,
    "retry_max_ms": 5_000,
    "split_min_rows": 25,
    "split_max_depth": 5,
    "minimum_attempt_runtime_ms": 1_000,
    "shutdown_buffer_ms": 1_000,
}
CONFIG_BOUNDS = {
    "attempts": (1, 5),
    "retry_base_ms": (1, 10_000),
    "retry_max_ms": (2, 30_000),
    "split_min_rows": (1, 10_000),
    "split_max_depth": (0, 10),
    "minimum_attempt_runtime_ms": (1, 120_000),
    "shutdown_buffer_ms": (0, 30_000),
}
TRANSIENT_HTTP_STATUSES = {500, 502, 503, 504}
DEFAULT_POSTGREST_ATTEMPT_RUNTIME_MS = 120_000


class IngestDbObservationWriteError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        classification: str,
        terminal_reason: str,
        stats: dict[str, Any],
        cause: Exception | None = None,
        error_code: str | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.classification = classification
        self.terminal_reason = terminal_reason
        self.stats = stats
        self.error_code = error_code
        self.http_status = http_status
        self.__cause__ = cause


def parse_config(values: Mapping[str, Any] | None = None) -> dict[str, int]:
    source = values if isinstance(values, Mapping) else {}
    parsed: dict[str, int] = {}
    for key, fallback in DEFAULT_CONFIG.items():
        value = source.get(key)
        try:
            candidate = int(value)
            valid_integer = not isinstance(value, float) or value.is_integer()
        except (TypeError, ValueError):
            candidate = fallback
            valid_integer = False
        minimum, maximum = CONFIG_BOUNDS[key]
        parsed[key] = (
            candidate
            if valid_integer and minimum <= candidate <= maximum
            else fallback
        )
    if parsed["retry_max_ms"] <= parsed["retry_base_ms"]:
        parsed["retry_base_ms"] = DEFAULT_CONFIG["retry_base_ms"]
        parsed["retry_max_ms"] = DEFAULT_CONFIG["retry_max_ms"]
    return parsed


def _bounded_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        text = value if isinstance(value, str) else json.dumps(value, default=str)
    except (TypeError, ValueError):
        text = str(value)
    return str(text)[:500]


def _failure_fields(error: Exception) -> dict[str, Any]:
    queue: list[Any] = [error]
    seen: set[int] = set()
    messages: list[str] = []
    code: str | None = None
    http_status: int | None = None
    while queue and len(seen) < 12:
        current = queue.pop(0)
        if current is None:
            continue
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        if isinstance(current, str):
            messages.append(_bounded_text(current))
            try:
                parsed = json.loads(current)
            except (json.JSONDecodeError, TypeError):
                parsed = None
            if isinstance(parsed, (dict, list)):
                queue.append(parsed)
            continue
        if isinstance(current, Mapping):
            source = current
        else:
            source = vars(current) if hasattr(current, "__dict__") else {}
            text = _bounded_text(current)
            if text:
                messages.append(text)
        for key in ("message", "details", "hint", "text", "status_text"):
            value = source.get(key) if isinstance(source, Mapping) else None
            text = _bounded_text(value)
            if text:
                messages.append(text)
        if code is None:
            candidate = source.get("code") if isinstance(source, Mapping) else None
            if candidate is not None:
                code = str(candidate).strip().upper() or None
        if http_status is None:
            for key in ("http_status", "status_code", "status"):
                candidate = source.get(key) if isinstance(source, Mapping) else None
                try:
                    parsed_status = int(candidate)
                except (TypeError, ValueError):
                    continue
                if 100 <= parsed_status <= 599:
                    http_status = parsed_status
                    break
        for key in ("cause", "error", "response", "data", "body", "payload"):
            value = source.get(key) if isinstance(source, Mapping) else None
            if value is not None:
                queue.append(value)
        cause = getattr(current, "__cause__", None)
        if cause is not None:
            queue.append(cause)
    combined_message = " | ".join(messages)[:500]
    if http_status is None and code and re.fullmatch(r"[1-5][0-9]{2}", code):
        http_status = int(code)
    if http_status is None:
        status_match = re.search(
            r"(?:http|status(?:_code)?|code)[^0-9]{0,12}(429|500|502|503|504)\b",
            combined_message,
            re.IGNORECASE,
        )
        if status_match:
            http_status = int(status_match.group(1))
    return {
        "code": code,
        "http_status": http_status,
        "message": combined_message,
    }


def classify_failure(error: Exception) -> dict[str, Any]:
    fields = _failure_fields(error)
    code = fields["code"]
    message = fields["message"]
    http_status = fields["http_status"]
    if code == "57014" and re.search(
        r"statement timeout|canceling statement due to statement timeout",
        message,
        re.IGNORECASE,
    ):
        return {"classification": "statement_timeout", "retryable": True, **fields}
    if code == "57014":
        return {"classification": "non_retryable", "retryable": False, **fields}
    if code == "40P01" or re.search(r"deadlock detected", message, re.IGNORECASE):
        return {"classification": "deadlock", "retryable": True, **fields}
    if code == "40001" or re.search(
        r"serialization failure|could not serialize access", message, re.IGNORECASE
    ):
        return {"classification": "serialization_failure", "retryable": True, **fields}
    if (
        code and (code.startswith("08") or code in {"57P01", "57P02", "57P03"})
    ) or re.search(
        r"connection (?:terminated|reset|closed|refused)|econnreset|socket hang up|temporary network|network error|network request failed|fetch failed|error sending request|request timed out|operation was aborted",
        message,
        re.IGNORECASE,
    ):
        return {"classification": "connection_failure", "retryable": True, **fields}
    if http_status == 429:
        return {"classification": "rate_limited", "retryable": True, **fields}
    permanent = bool(
        code
        and (re.match(r"^(22|23|28|42)", code) or re.match(r"^PGRST[123]", code))
    ) or bool(
        re.search(
            r"authentication|authorization|permission denied|invalid (?:input|payload|timestamp|connector|timeseries)|malformed|unknown column|column .+ does not exist|relation .+ does not exist|schema cache|not-null|foreign key|unique constraint|violates .+ constraint",
            message,
            re.IGNORECASE,
        )
    )
    if permanent:
        return {"classification": "non_retryable", "retryable": False, **fields}
    if http_status in TRANSIENT_HTTP_STATUSES:
        return {
            "classification": "temporary_service_failure",
            "retryable": True,
            **fields,
        }
    return {"classification": "non_retryable", "retryable": False, **fields}


def empty_stats() -> dict[str, Any]:
    return {
        "input_rows": 0,
        "normal_chunk_size": 0,
        "committed_rows": 0,
        "write_requests": 0,
        "request_body_bytes": 0,
        "retry_attempts": 0,
        "retried_chunks": 0,
        "split_operations": 0,
        "smallest_attempted_chunk": 0,
        "unresolved_rows": 0,
        "terminal_failure_classification": None,
        "terminal_reason": None,
        "stopped_for_runtime_budget": False,
    }


def merge_stats(target: dict[str, Any], addition: Mapping[str, Any]) -> dict[str, Any]:
    target["normal_chunk_size"] = max(
        int(target.get("normal_chunk_size") or 0),
        int(addition.get("normal_chunk_size") or 0),
    )
    for key in (
        "input_rows",
        "committed_rows",
        "write_requests",
        "request_body_bytes",
        "retry_attempts",
        "retried_chunks",
        "split_operations",
        "unresolved_rows",
    ):
        target[key] += int(addition.get(key) or 0)
    sizes = [
        int(value)
        for value in (target["smallest_attempted_chunk"], addition.get("smallest_attempted_chunk"))
        if value and int(value) > 0
    ]
    target["smallest_attempted_chunk"] = min(sizes) if sizes else 0
    for key in ("terminal_failure_classification", "terminal_reason"):
        if addition.get(key):
            target[key] = addition[key]
    target["stopped_for_runtime_budget"] = bool(
        target["stopped_for_runtime_budget"]
        or addition.get("stopped_for_runtime_budget")
    )
    return target


def write_observations(
    rows: Sequence[Mapping[str, Any]],
    *,
    chunk_size: int,
    connector_code: str,
    write_chunk: Callable[[Sequence[Mapping[str, Any]]], Any],
    config: Mapping[str, Any] | None = None,
    logger: Any = None,
    sleep_fn: Callable[[float], Any] = time.sleep,
    random_fn: Callable[[], float] = random_module.random,
    should_stop: Callable[[], bool] | None = None,
    remaining_runtime_ms: Callable[[], float] | None = None,
    request_body_bytes: Callable[[Sequence[Mapping[str, Any]]], int] | None = None,
) -> dict[str, Any]:
    prepared = list(rows)
    settings = parse_config(config)
    if not isinstance(chunk_size, int) or not 1 <= chunk_size <= 100_000:
        chunk_size = max(1, len(prepared))
    stats = empty_stats()
    stats["input_rows"] = len(prepared)
    stats["normal_chunk_size"] = chunk_size if prepared else 0
    stats["unresolved_rows"] = len(prepared)

    def has_budget(required_ms: int) -> bool:
        if should_stop is not None and should_stop():
            return False
        if remaining_runtime_ms is None:
            return True
        try:
            return float(remaining_runtime_ms()) >= required_ms
        except (TypeError, ValueError):
            return True

    def emit(level: str, event: str, context: Mapping[str, Any]) -> None:
        method = getattr(logger, level, None)
        if callable(method):
            method("%s %s", event, json.dumps(context, default=str, sort_keys=True))

    def fail(
        cause: Exception | None,
        failure: Mapping[str, Any],
        terminal_reason: str,
        original_rows: int,
        final_rows: int,
        attempts: int,
        depth: int,
    ) -> IngestDbObservationWriteError:
        stats["unresolved_rows"] = stats["input_rows"] - stats["committed_rows"]
        stats["terminal_failure_classification"] = failure["classification"]
        stats["terminal_reason"] = terminal_reason
        stats["stopped_for_runtime_budget"] = terminal_reason == "runtime_budget"
        diagnostic = {
            "connector_code": connector_code[:100],
            "original_chunk_rows": original_rows,
            "final_chunk_rows": final_rows,
            "attempts": attempts,
            "split_depth": depth,
            "unresolved_rows": stats["unresolved_rows"],
            "failure_classification": failure["classification"],
            "terminal_reason": terminal_reason,
            "error_code": failure.get("code"),
            "http_status": failure.get("http_status"),
        }
        emit("error", "ingestdb_observation_upsert_terminal", diagnostic)
        return IngestDbObservationWriteError(
            f"IngestDB observation write failed ({terminal_reason}; "
            f"{failure['classification']}; unresolved_rows={stats['unresolved_rows']}).",
            classification=str(failure["classification"]),
            terminal_reason=terminal_reason,
            stats=dict(stats),
            cause=cause,
            error_code=failure.get("code"),
            http_status=failure.get("http_status"),
        )

    def budget_failure(chunk: Sequence[Any], depth: int, original_rows: int, attempts: int):
        return fail(
            None,
            {"classification": "runtime_budget", "code": None, "http_status": None},
            "runtime_budget",
            original_rows,
            len(chunk),
            attempts,
            depth,
        )

    def process(chunk: Sequence[Mapping[str, Any]], depth: int, original_rows: int) -> None:
        last_error: Exception | None = None
        last_failure: dict[str, Any] | None = None
        retried = False
        for attempt in range(1, settings["attempts"] + 1):
            if attempt > 1:
                exponential = min(
                    settings["retry_max_ms"] - settings["retry_base_ms"],
                    settings["retry_base_ms"] * (2 ** (attempt - 2)),
                )
                jitter_ceiling = max(
                    1,
                    min(
                        settings["retry_base_ms"],
                        settings["retry_max_ms"] - exponential,
                    ),
                )
                random_value = min(0.999999999, max(0.0, float(random_fn())))
                delay_ms = min(
                    settings["retry_max_ms"],
                    exponential + 1 + int(random_value * jitter_ceiling),
                )
                required = delay_ms + settings["minimum_attempt_runtime_ms"] + settings["shutdown_buffer_ms"]
                if not has_budget(required):
                    raise budget_failure(chunk, depth, original_rows, attempt - 1)
                emit(
                    "warning",
                    "ingestdb_observation_upsert_retry",
                    {
                        "connector_code": connector_code[:100],
                        "chunk_rows": len(chunk),
                        "attempt": attempt,
                        "maximum_attempts": settings["attempts"],
                        "failure_classification": last_failure["classification"],
                        "delay_ms": delay_ms,
                    },
                )
                sleep_fn(delay_ms / 1_000)
                if not has_budget(settings["minimum_attempt_runtime_ms"] + settings["shutdown_buffer_ms"]):
                    raise budget_failure(chunk, depth, original_rows, attempt - 1)
                stats["retry_attempts"] += 1
                if not retried:
                    retried = True
                    stats["retried_chunks"] += 1
            elif not has_budget(settings["minimum_attempt_runtime_ms"] + settings["shutdown_buffer_ms"]):
                raise budget_failure(chunk, depth, original_rows, 0)
            stats["write_requests"] += 1
            if request_body_bytes is not None:
                measured_bytes = request_body_bytes(chunk)
                if measured_bytes >= 0:
                    stats["request_body_bytes"] += int(measured_bytes)
            size = len(chunk)
            current_min = stats["smallest_attempted_chunk"]
            stats["smallest_attempted_chunk"] = size if not current_min else min(current_min, size)
            try:
                write_chunk(chunk)
                stats["committed_rows"] += size
                stats["unresolved_rows"] = stats["input_rows"] - stats["committed_rows"]
                return
            except Exception as error:
                last_error = error
                last_failure = classify_failure(error)
                if not last_failure["retryable"]:
                    raise fail(error, last_failure, "non_retryable_error", original_rows, size, attempt, depth)
        assert last_failure is not None
        can_split = (
            last_failure["classification"] == "statement_timeout"
            and depth < settings["split_max_depth"]
            and len(chunk) // 2 >= settings["split_min_rows"]
        )
        if can_split:
            if not has_budget(settings["minimum_attempt_runtime_ms"] + settings["shutdown_buffer_ms"]):
                raise budget_failure(chunk, depth, original_rows, settings["attempts"])
            midpoint = len(chunk) // 2
            left, right = chunk[:midpoint], chunk[midpoint:]
            if not left or not right:
                raise RuntimeError("IngestDB observation split produced an empty child.")
            stats["split_operations"] += 1
            emit(
                "warning",
                "ingestdb_observation_upsert_split",
                {
                    "connector_code": connector_code[:100],
                    "parent_chunk_rows": len(chunk),
                    "left_chunk_rows": len(left),
                    "right_chunk_rows": len(right),
                    "split_depth": depth + 1,
                    "failure_classification": last_failure["classification"],
                },
            )
            process(left, depth + 1, original_rows)
            process(right, depth + 1, original_rows)
            return
        terminal_reason = (
            "minimum_chunk_failed"
            if last_failure["classification"] == "statement_timeout"
            and (len(chunk) < settings["split_min_rows"] * 2 or settings["split_max_depth"] == 0)
            else "retry_exhausted"
        )
        raise fail(last_error, last_failure, terminal_reason, original_rows, len(chunk), settings["attempts"], depth)

    for offset in range(0, len(prepared), chunk_size):
        ordinary_chunk = prepared[offset : offset + chunk_size]
        process(ordinary_chunk, 0, len(ordinary_chunk))
    return stats


def build_compact_observation_rpc_args(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    args: dict[str, Any] = {
        "timeseries_ids": [row["timeseries_id"] for row in rows],
        "observed_ats": [row["observed_at"] for row in rows],
        "values": [row.get("value") for row in rows],
    }
    if any(row.get("status") is not None for row in rows):
        args["statuses"] = [row.get("status") for row in rows]
    return args


def serialized_json_utf8_bytes(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":")).encode("utf-8"))
