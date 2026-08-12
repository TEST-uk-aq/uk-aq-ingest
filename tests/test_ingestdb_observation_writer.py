from pathlib import Path

import pytest

from scripts.uk_aq_ingestdb_observation_writer import (
    DEFAULT_CONFIG,
    IngestDbObservationWriteError,
    parse_config,
    write_observations,
)


class StructuredError(RuntimeError):
    def __init__(self, message, *, code=None, http_status=None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.http_status = http_status


def timeout_error():
    return StructuredError(
        "canceling statement due to statement timeout",
        code="57014",
        http_status=500,
    )


def make_rows(count):
    return [
        {
            "connector_id": 1,
            "timeseries_id": index + 1,
            "observed_at": "2026-07-29T00:00:00Z",
        }
        for index in range(count)
    ]


def test_retry_then_success_has_accurate_stats_and_bounded_delay():
    calls = 0
    delays = []

    def write_chunk(_chunk):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise timeout_error()

    stats = write_observations(
        make_rows(3),
        chunk_size=3,
        connector_code="test",
        write_chunk=write_chunk,
        config={"attempts": 3, "retry_base_ms": 100, "retry_max_ms": 1_000},
        sleep_fn=delays.append,
        random_fn=lambda: 0,
        request_body_bytes=lambda _chunk: 7,
    )
    assert delays == [0.101, 0.201]
    assert stats["committed_rows"] == 3
    assert stats["normal_chunk_size"] == 3
    assert stats["write_requests"] == 3
    assert stats["request_body_bytes"] == 21
    assert stats["retry_attempts"] == 2
    assert stats["retried_chunks"] == 1


def test_timeout_split_preserves_successful_child_and_order():
    attempted = []

    def write_chunk(chunk):
        ids = [row["timeseries_id"] for row in chunk]
        attempted.append(ids)
        if len(chunk) == 8 or (len(chunk) == 4 and ids[0] == 5):
            raise timeout_error()

    stats = write_observations(
        make_rows(8),
        chunk_size=8,
        connector_code="test",
        write_chunk=write_chunk,
        config={"attempts": 1, "split_min_rows": 2, "split_max_depth": 3},
    )
    assert attempted == [
        [1, 2, 3, 4, 5, 6, 7, 8],
        [1, 2, 3, 4],
        [5, 6, 7, 8],
        [5, 6],
        [7, 8],
    ]
    assert stats["committed_rows"] == 8
    assert stats["split_operations"] == 2
    assert stats["smallest_attempted_chunk"] == 2


def test_non_retryable_failure_is_not_retried():
    calls = 0

    def write_chunk(_chunk):
        nonlocal calls
        calls += 1
        raise StructuredError("column bad does not exist", code="42703", http_status=500)

    with pytest.raises(IngestDbObservationWriteError) as raised:
        write_observations(
            make_rows(2),
            chunk_size=2,
            connector_code="test",
            write_chunk=write_chunk,
            sleep_fn=lambda _delay: pytest.fail("must not sleep"),
        )
    assert calls == 1
    assert raised.value.terminal_reason == "non_retryable_error"


def test_runtime_budget_stop_is_distinct():
    checks = iter([1_000, 100])

    with pytest.raises(IngestDbObservationWriteError) as raised:
        write_observations(
            make_rows(2),
            chunk_size=2,
            connector_code="test",
            write_chunk=lambda _chunk: (_ for _ in ()).throw(timeout_error()),
            config={
                "attempts": 3,
                "retry_base_ms": 10,
                "retry_max_ms": 100,
                "minimum_attempt_runtime_ms": 100,
                "shutdown_buffer_ms": 100,
            },
            remaining_runtime_ms=lambda: next(checks),
            sleep_fn=lambda _delay: pytest.fail("must not delay"),
        )
    assert raised.value.terminal_reason == "runtime_budget"
    assert raised.value.stats["stopped_for_runtime_budget"] is True


def test_config_is_bounded_and_rejects_invalid_combinations():
    assert parse_config({"attempts": 0, "split_max_depth": 99}) == DEFAULT_CONFIG
    parsed = parse_config({"retry_base_ms": 10_000, "retry_max_ms": 9_000})
    assert parsed["retry_base_ms"] == DEFAULT_CONFIG["retry_base_ms"]
    assert parsed["retry_max_ms"] == DEFAULT_CONFIG["retry_max_ms"]
    assert parse_config({"minimum_attempt_runtime_ms": 60_000})[
        "minimum_attempt_runtime_ms"
    ] == 60_000


def test_active_python_observation_writers_use_shared_contract_and_canonical_key():
    root = Path(__file__).resolve().parents[1]
    paths = [
        root / "scripts/blondon_nodes/blondon_nodes_ingest.py",
        root / "scripts/sensorcommunity/sensorcommunity_ingest.py",
        root / "scripts/erg_laqn/erg_laqn_ingest.py",
        root / "scripts/sos/sos_ingest.py",
    ]
    for path in paths:
        source = path.read_text(encoding="utf-8")
        assert "write_observations" in source
        assert (
            'on_conflict="connector_id,timeseries_id,observed_at"' in source
            or "uk_aq_rpc_observations_compact_upsert_v1" in source
        )
        assert "DEFAULT_POSTGREST_ATTEMPT_RUNTIME_MS" in source

    sensor_source = paths[1].read_text(encoding="utf-8")
    metadata_write = sensor_source.index("writer.upsert_timeseries(timeseries_metadata_payload)")
    observation_write = sensor_source.index("writer.upsert_observations(observation_rows)")
    last_value_write = sensor_source.index(
        "writer.upsert_timeseries(timeseries_payload)", observation_write
    )
    assert metadata_write < observation_write < last_value_write

    nodes_source = paths[0].read_text(encoding="utf-8")
    assert "if isinstance(exc, IngestDbObservationWriteError):" in nodes_source

    sos_source = paths[3].read_text(encoding="utf-8")
    assert sos_source.count("if isinstance(exc, IngestDbObservationWriteError):") >= 3

    dormant_source = (
        root / "scripts/blondon_communities/blondon_communities_list_stations.py"
    ).read_text(encoding="utf-8")
    assert "def upsert_observations" not in dormant_source
