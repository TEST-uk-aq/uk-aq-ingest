#!/usr/bin/env python3
"""Export canonical UK-AIR SOS station-to-network assignments."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import psycopg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate stations.network_id against networks.id for UK-AIR SOS stations."
    )
    parser.add_argument("--output", required=True, help="Output CSV path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database_url = os.getenv("SUPABASE_DB_URL", "").strip()
    if not database_url:
        raise SystemExit("SUPABASE_DB_URL is required.")

    query = """
        select
          s.id as station_id,
          s.station_ref,
          s.service_ref,
          s.network_id,
          n.network_code,
          n.display_name as network_label,
          n.public_display_enabled,
          c.connector_code,
          c.display_name as connector_label,
          (n.id is not null) as network_assignment_valid
        from uk_aq_core.stations s
        join uk_aq_core.connectors c on c.id = s.connector_id
        left join uk_aq_core.networks n on n.id = s.network_id
        where c.connector_code = 'sos'
        order by s.id
    """
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [column.name for column in cursor.description]
            rows = cursor.fetchall()

    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"Wrote {len(rows)} canonical network assignments to {output}")


if __name__ == "__main__":
    main()
