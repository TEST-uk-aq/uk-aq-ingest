#!/usr/bin/env python3
"""Bounded delete batching for the Obs AQI core mirror."""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Sequence, Type

DEFAULT_DELETE_BATCH_SIZE = 50
MAX_DELETE_BATCH_SIZE = 500
DELETE_BATCH_SIZE_ENV = "UK_AQ_CORE_MIRROR_DELETE_BATCH_SIZE"


def parse_delete_batch_size(raw: str | None = None) -> int:
    value = raw if raw is not None else os.getenv(DELETE_BATCH_SIZE_ENV, "")
    text = str(value or "").strip()
    if not text:
        return DEFAULT_DELETE_BATCH_SIZE
    try:
        size = int(text)
    except ValueError as exc:
        raise ValueError(f"{DELETE_BATCH_SIZE_ENV} must be an integer") from exc
    if size < 1 or size > MAX_DELETE_BATCH_SIZE:
        raise ValueError(
            f"{DELETE_BATCH_SIZE_ENV} must be between 1 and {MAX_DELETE_BATCH_SIZE}"
        )
    return size


def delete_keys_in_batches(
    *,
    table: str,
    keys: Sequence[Dict[str, Any]],
    batch_size: int,
    delete_batch: Callable[[Sequence[Dict[str, Any]]], int],
    error_type: Type[Exception] = RuntimeError,
) -> int:
    key_rows = list(keys)
    if not key_rows:
        return 0
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    batch_count = (len(key_rows) + batch_size - 1) // batch_size
    deleted_total = 0

    for batch_number, offset in enumerate(range(0, len(key_rows), batch_size), start=1):
        batch = key_rows[offset : offset + batch_size]
        print(
            "CORE_DELETE_BATCH "
            f"table={table} batch={batch_number}/{batch_count} "
            f"keys={len(batch)} total_keys={len(key_rows)}",
            flush=True,
        )
        deleted = int(delete_batch(batch))
        if deleted != len(batch):
            raise error_type(
                f"{table}: delete count mismatch for batch {batch_number}/{batch_count}; "
                f"expected={len(batch)} deleted={deleted}"
            )
        deleted_total += deleted

    if deleted_total != len(key_rows):
        raise error_type(
            f"{table}: batched delete total mismatch; "
            f"expected={len(key_rows)} deleted={deleted_total}"
        )

    print(
        "CORE_DELETE_BATCH_COMPLETE "
        f"table={table} batches={batch_count} deleted={deleted_total}",
        flush=True,
    )
    return deleted_total
