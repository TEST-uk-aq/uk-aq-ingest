from __future__ import annotations

from pathlib import Path

from scripts.stations_daily.sync_obs_aqidb_uk_aq_core_batching import (
    delete_keys_in_batches,
    parse_delete_batch_size,
)


def test_delete_keys_are_split_into_bounded_batches():
    calls = []
    keys = [{"id": value} for value in range(549)]

    deleted = delete_keys_in_batches(
        table="stations",
        keys=keys,
        batch_size=50,
        delete_batch=lambda batch: calls.append(list(batch)) or len(batch),
        error_type=RuntimeError,
    )

    assert deleted == 549
    assert len(calls) == 11
    assert [len(batch) for batch in calls] == [50] * 10 + [49]
    assert [row["id"] for batch in calls for row in batch] == list(range(549))


def test_delete_batch_count_mismatch_fails_closed():
    try:
        delete_keys_in_batches(
            table="stations",
            keys=[{"id": 1}, {"id": 2}],
            batch_size=50,
            delete_batch=lambda batch: 1,
            error_type=RuntimeError,
        )
    except RuntimeError as exc:
        assert "delete count mismatch" in str(exc)
    else:
        raise AssertionError("Expected delete count mismatch to fail closed")


def test_delete_batch_size_validation():
    assert parse_delete_batch_size(None) == 50
    assert parse_delete_batch_size("25") == 25

    for raw in ("0", "501", "invalid"):
        try:
            parse_delete_batch_size(raw)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Expected invalid batch size to fail: {raw}")


def test_active_entry_point_installs_batching_wrapper():
    source = Path("scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py").read_text(
        encoding="utf-8"
    )
    assert "sync_obs_aqidb_uk_aq_core_legacy.py" in source
    assert "delete_keys_in_batches" in source
    assert "_legacy.PostgrestClient.delete_core_keys_via_rpc" in source
