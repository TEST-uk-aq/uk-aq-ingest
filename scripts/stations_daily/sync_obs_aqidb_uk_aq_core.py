#!/usr/bin/env python3
"""Run the Obs AQI core mirror with bounded delete RPC batches.

The wrapper also extends the legacy mirror implementation with the canonical
`networks` table. IngestDB owns the authoritative network catalogue, including
stable numeric IDs and mutable display/enablement/priority fields. Networks are
upserted before connector/station rows and deleted only after dependent rows.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.stations_daily.sync_obs_aqidb_uk_aq_core_batching import (
    delete_keys_in_batches,
    parse_delete_batch_size,
)

LEGACY_PATH = Path(__file__).with_name("sync_obs_aqidb_uk_aq_core_legacy.py")


def _load_legacy_module():
    if not LEGACY_PATH.is_file():
        raise RuntimeError(f"Missing core mirror legacy implementation: {LEGACY_PATH}")
    spec = importlib.util.spec_from_file_location(
        "uk_aq_sync_obs_aqidb_core_legacy",
        LEGACY_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load core mirror legacy implementation: {LEGACY_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_legacy = _load_legacy_module()
_original_delete_core_keys_via_rpc = _legacy.PostgrestClient.delete_core_keys_via_rpc


# `networks` predates the generic mirror table set and was originally seeded
# independently in ObsAQIDB to keep numeric FK identities stable. It is now a
# normal authoritative mirrored core table. Preserve the source IDs by using the
# same primary-key mirror semantics as the other reference tables.
_NETWORKS_TABLE_META = {
    "pk": ["id"],
    "columns": [
        {"column_name": "id", "udt_name": "int8", "is_nullable": "NO", "column_default": None, "ordinal_position": 1},
        {"column_name": "network_code", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 2},
        {"column_name": "display_name", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 3},
        {"column_name": "network_type", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 4},
        {"column_name": "ingest_enabled", "udt_name": "bool", "is_nullable": "NO", "column_default": "true", "ordinal_position": 5},
        {"column_name": "public_display_enabled", "udt_name": "bool", "is_nullable": "NO", "column_default": "false", "ordinal_position": 6},
        {"column_name": "default_priority", "udt_name": "int4", "is_nullable": "NO", "column_default": "100", "ordinal_position": 7},
        {"column_name": "metadata", "udt_name": "jsonb", "is_nullable": "NO", "column_default": "'{}'::jsonb", "ordinal_position": 8},
        {"column_name": "created_at", "udt_name": "timestamptz", "is_nullable": "NO", "column_default": "now()", "ordinal_position": 9},
        {"column_name": "updated_at", "udt_name": "timestamptz", "is_nullable": "NO", "column_default": "now()", "ordinal_position": 10},
    ],
}

if "networks" not in _legacy.PRIMARY_TABLES:
    _legacy.PRIMARY_TABLES.insert(0, "networks")
if "networks" not in _legacy.SYNC_TABLES:
    _legacy.SYNC_TABLES.insert(0, "networks")
if "networks" not in _legacy.DELETE_ORDER:
    _legacy.DELETE_ORDER.append("networks")
_legacy.STATIC_SOURCE_TABLE_META["networks"] = _NETWORKS_TABLE_META


def _parse_delete_batch_size(raw: str | None = None) -> int:
    return parse_delete_batch_size(raw)


def _delete_keys_in_batches(
    *,
    table: str,
    keys: Sequence[Dict[str, Any]],
    batch_size: int,
    delete_batch,
    error_type=RuntimeError,
) -> int:
    return delete_keys_in_batches(
        table=table,
        keys=keys,
        batch_size=batch_size,
        delete_batch=delete_batch,
        error_type=error_type,
    )


def _batched_delete_core_keys_via_rpc(
    self,
    table: str,
    *,
    pk_columns: Sequence[str],
    keys: Sequence[Dict[str, Any]],
) -> int:
    key_rows = list(keys)
    if not key_rows:
        return 0
    try:
        batch_size = parse_delete_batch_size()
    except ValueError as exc:
        raise _legacy.SyncError(str(exc)) from exc

    return delete_keys_in_batches(
        table=table,
        keys=key_rows,
        batch_size=batch_size,
        delete_batch=lambda batch: _original_delete_core_keys_via_rpc(
            self,
            table,
            pk_columns=pk_columns,
            keys=batch,
        ),
        error_type=_legacy.SyncError,
    )


_legacy.PostgrestClient.delete_core_keys_via_rpc = _batched_delete_core_keys_via_rpc

# Preserve the module's existing public surface for scripts and tests that import it.
for _name, _value in vars(_legacy).items():
    if _name.startswith("__") or _name in globals():
        continue
    globals()[_name] = _value


def main() -> int:
    return _legacy.main()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except _legacy.SyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
