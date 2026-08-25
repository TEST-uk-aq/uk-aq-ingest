#!/usr/bin/env python3
"""Run the AQE cadence probe's balanced minute slot plus a small catch-up set.

Normal scheduled behaviour:
- divide all active AQE stations evenly across 60 one-minute slots;
- probe the current slot, giving 4 or 5 stations per minute with 256 active sites;
- find active stations whose last successful parse is more than 70 minutes old
  (or which have never succeeded);
- only retry those stations if they have not been attempted in the last 30 minutes;
- probe at most 10 catch-up stations in the same minute.

The slot assignment is deterministic for the current active inventory. Stations
are ordered by SHA-256 of site ID and then distributed round-robin across the 60
minute slots. This avoids empty/overloaded minutes while keeping the schedule
stable unless the active inventory itself changes.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


DEFAULT_DATA_DIR = (
    Path.home() / "Library" / "Application Support" / "UK AQ" / "aqe-probe"
)
SLOT_COUNT = 60
SLOT_SECONDS = 60
SUCCESS_MAX_AGE_MINUTES = 70
RECENT_ATTEMPT_MINUTES = 30
MAX_CATCHUP_STATIONS = 10
PROBE_SCRIPT = Path(__file__).with_name("aqe_probe.py")


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def current_slot(now: float | None = None) -> int:
    if now is None:
        now = time.time()
    return (int(now) // SLOT_SECONDS) % SLOT_COUNT


@contextmanager
def single_instance_lock(path: Path) -> Iterable[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"Another AQE probe runner already holds {path}; exiting.")
            raise SystemExit(0)
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def run_probe(data_dir: Path, site_ids: list[str]) -> int:
    command = [
        sys.executable,
        str(PROBE_SCRIPT),
        "--data-dir",
        str(data_dir),
        "--quiet",
    ]
    for site_id in site_ids:
        command.extend(["--site", site_id])

    completed = subprocess.run(command, check=False)
    return int(completed.returncode)


def balanced_slot_sites(db_path: Path, slot: int) -> tuple[list[str], int]:
    """Return this minute's balanced sites and persist all slot assignments."""
    if not db_path.exists():
        return [], 0

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        site_ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT site_id FROM stations WHERE is_active = 1"
            ).fetchall()
        ]
        site_ids.sort(
            key=lambda site_id: (
                hashlib.sha256(site_id.encode("utf-8")).digest(),
                site_id,
            )
        )

        assignments = [
            (index % SLOT_COUNT, site_id)
            for index, site_id in enumerate(site_ids)
        ]
        conn.executemany(
            "UPDATE stations SET shard = ? WHERE site_id = ?",
            assignments,
        )
        conn.commit()

        selected = [
            site_id
            for index, site_id in enumerate(site_ids)
            if index % SLOT_COUNT == slot
        ]
        return selected, len(site_ids)
    finally:
        conn.close()


def catchup_sites(db_path: Path, now: datetime) -> list[str]:
    if not db_path.exists():
        return []

    success_cutoff = iso_utc(now - timedelta(minutes=SUCCESS_MAX_AGE_MINUTES))
    attempt_cutoff = iso_utc(now - timedelta(minutes=RECENT_ATTEMPT_MINUTES))

    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            WITH station_probe_state AS (
                SELECT
                    s.site_id,
                    MAX(CASE WHEN p.parse_ok = 1 THEN p.probe_time_utc END)
                        AS last_success_at_utc,
                    MAX(p.probe_time_utc) AS last_attempt_at_utc
                FROM stations AS s
                LEFT JOIN probes AS p ON p.site_id = s.site_id
                WHERE s.is_active = 1
                GROUP BY s.site_id
            )
            SELECT site_id
            FROM station_probe_state
            WHERE
                (
                    last_success_at_utc IS NULL
                    OR last_success_at_utc < ?
                )
                AND
                (
                    last_attempt_at_utc IS NULL
                    OR last_attempt_at_utc < ?
                )
            ORDER BY
                CASE WHEN last_success_at_utc IS NULL THEN 0 ELSE 1 END,
                COALESCE(last_success_at_utc, ''),
                COALESCE(last_attempt_at_utc, ''),
                site_id
            LIMIT ?
            """,
            (success_cutoff, attempt_cutoff, MAX_CATCHUP_STATIONS),
        ).fetchall()
        return [str(row["site_id"]) for row in rows]
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the AQE balanced current-minute slot and up to 10 overdue "
            "catch-up stations."
        )
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"AQE probe state directory (default: {DEFAULT_DATA_DIR})",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = args.data_dir.expanduser().resolve()
    db_path = data_dir / "aqe_probe.sqlite3"
    runner_lock = data_dir / "aqe_probe_runner.lock"

    with single_instance_lock(runner_lock):
        slot = current_slot()
        sites, active_count = balanced_slot_sites(db_path, slot)
        if active_count == 0:
            print(
                "AQE balanced schedule found no active station inventory; "
                "run aqe_probe.py --metadata-only first."
            )
            return 1

        print(
            f"AQE schedule: slot={slot} stations={len(sites)} "
            f"active={active_count}"
        )
        current_returncode = run_probe(data_dir, sites)
        if current_returncode != 0:
            print(
                f"Current AQE slot probe exited {current_returncode}; "
                "skipping catch-up for this minute."
            )
            return current_returncode

        # aqe_probe.py may have refreshed AQE metadata during the scheduled run.
        # Re-apply the balanced assignment so any newly discovered station is
        # included and the persisted shard column reflects the real schedule.
        balanced_slot_sites(db_path, slot)

        sites = catchup_sites(db_path, datetime.now(timezone.utc))
        if not sites:
            print("AQE catch-up: 0 overdue stations")
            return 0

        print(f"AQE catch-up: {len(sites)} overdue station(s): {', '.join(sites)}")
        return run_probe(data_dir, sites)


if __name__ == "__main__":
    raise SystemExit(main())
