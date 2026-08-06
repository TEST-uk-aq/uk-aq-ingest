from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path("scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py")
SPEC = importlib.util.spec_from_file_location("sync_obs_aqidb_uk_aq_core", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_delete_keys_are_split_into_bounded_batches():
    calls = []
    keys = [{"id": value} for value in range(549)]

    deleted = MODULE._delete_keys_in_batches(
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
        MODULE._delete_keys_in_batches(
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
    assert MODULE._parse_delete_batch_size(None) == 50
    assert MODULE._parse_delete_batch_size("25") == 25

    for raw in ("0", "501", "invalid"):
        try:
            MODULE._parse_delete_batch_size(raw)
        except ValueError:
            pass
        else:
            raise AssertionError(f"Expected invalid batch size to fail: {raw}")
