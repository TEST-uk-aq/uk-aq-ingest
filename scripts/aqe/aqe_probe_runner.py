#!/usr/bin/env python3
"""Run the AQE cadence probe's current shard plus a small catch-up set.

Normal scheduled behaviour:
- probe the current one-minute shard;
- find active stations whose last successful parse is more than 70 minutes old
  (or which have never succeeded);
- only retry those stations if they have not been attempted in the last 30 minutes;
- probe at most 10 catch-up stations in the same minute.

This keeps the probe focused on update cadence/latency while repairing short
scheduler gaps without creating a burst after a longer outage.
"""

from __future__ import annotations

import argparse
import fcntl
import sqlite3
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


DEFAULT_DATA_DIR = (
    Path.home() / "Library" / "Application Support" / "UK AQ" / "aqe-probe"
)
SUCCESS_MAX_AGE_MINUTES = 70
RECENT_ATTEMPT_MINUTES = 30
MAX_CATCHUP_STATIONS = 10
PROBE_SCRIPT = Path(__file__).with_name("aqe_probe.py")


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


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


def run_probe(data_dir: Path, site_ids: list[str] | None = None) -> int:
    command = [
        sys.executable,
        str(PROBE_SCRIPT),
        "--data-dir",
        str(data_dir),
        "--quiet",
    ]
    for site_id in site_ids or []:
        command.extend(["--site", site_id])

    completed = subprocess.run(command, check=False)
    return int(completed.returncode)


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
        description="Run the AQE current shard and up to 10 overdue catch-up stations."
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
        current_returncode = run_probe(data_dir)
        if current_returncode != 0:
            print(
                f"Current AQE shard probe exited {current_returncode}; "
                "skipping catch-up for this minute."
            )
            return current_returncode

        sites = catchup_sites(db_path, datetime.now(timezone.utc))
        if not sites:
            print("AQE catch-up: 0 overdue stations")
            return 0

        print(f"AQE catch-up: {len(sites)} overdue station(s): {', '.join(sites)}")
        return run_probe(data_dir, sites)


if __name__ == "__main__":
    raise SystemExit(main())
