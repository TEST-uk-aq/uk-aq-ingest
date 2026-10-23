#!/usr/bin/env python3
"""Backfill Sensor.Community timeseries.phenomenon_id from timeseries_ref suffix.

Runs against rows where `phenomenon_id` is null for the Sensor.Community connector.
Intended for maintenance workflows (for example daily stations workflow), so ingest
hot paths do not spend runtime budget on this backfill.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if PROJECT_ROOT.name == "scripts":
    PROJECT_ROOT = PROJECT_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.uk_aq_supabase import SupabaseSchemas, create_supabase_client

load_dotenv()

SCOMM_CONNECTOR_CODE = (
    os.getenv("SCOMM_CONNECTOR_CODE")
    or os.getenv("SCOMM_CONNECTOR_REF")
    or os.getenv("SCOMM_SERVICE_REF")
    or "sensorcommunity"
)
SCOMM_SERVICE_REF = os.getenv("SCOMM_SERVICE_REF") or SCOMM_CONNECTOR_CODE

POLLUTANT_SUFFIXES = {
    ":pm10": "sensorcommunity:pm10",
    ":pm2.5": "sensorcommunity:pm2.5",
    ":temperature": "sensorcommunity:temperature",
    ":humidity": "sensorcommunity:humidity",
    ":pressure": "sensorcommunity:pressure",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Sensor.Community timeseries.phenomenon_id values.",
    )
    parser.add_argument("--connector-code", default=SCOMM_CONNECTOR_CODE)
    parser.add_argument("--service-ref", default=SCOMM_SERVICE_REF)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--limit", type=int, help="Optional cap on rows to process.")
    return parser.parse_args()



def main() -> int:
    args = parse_args()
    batch_size = max(1, int(args.batch_size))

    client = create_supabase_client()
    schemas = SupabaseSchemas.from_client(client)
    core = schemas.core

    connector_resp = (
        core.table("connectors")
        .select("id,connector_code")
        .eq("connector_code", str(args.connector_code))
        .limit(1)
        .execute()
    )
    connector_rows = connector_resp.data if hasattr(connector_resp, "data") else connector_resp.get("data")
    if not connector_rows:
        print(f"Connector not found for connector_code={args.connector_code}")
        return 1
    connector_id = int(connector_rows[0]["id"])

    uri_values = list(POLLUTANT_SUFFIXES.values())
    phenomena_resp = (
        core.table("phenomena")
        .select("id,source_label")
        .eq("connector_id", connector_id)
        .in_("source_label", uri_values)
        .execute()
    )
    phenomena_rows = phenomena_resp.data if hasattr(phenomena_resp, "data") else phenomena_resp.get("data")
    phen_by_uri = {
        str(row["source_label"]): int(row["id"])
        for row in (phenomena_rows or [])
        if row.get("source_label")
    }

    missing_uris = [uri for uri in uri_values if uri not in phen_by_uri]
    if missing_uris:
        print(f"Missing phenomena rows for URIs: {', '.join(missing_uris)}")

    offset = 0
    updated = 0
    scanned = 0
    while True:
        query = (
            core.table("timeseries")
            .select("id,timeseries_ref")
            .eq("connector_id", connector_id)
            .eq("service_ref", str(args.service_ref))
            .is_("phenomenon_id", None)
            .order("id", desc=False)
            .range(offset, offset + batch_size - 1)
        )
        resp = query.execute()
        rows = resp.data if hasattr(resp, "data") else resp.get("data")
        if not rows:
            break

        ids_by_phenomenon: Dict[int, List[int]] = {}
        for row in rows:
            scanned += 1
            ref = str(row.get("timeseries_ref") or "").lower()
            matched_uri: Optional[str] = None
            for suffix, uri in POLLUTANT_SUFFIXES.items():
                if ref.endswith(suffix):
                    matched_uri = uri
                    break
            if not matched_uri:
                continue
            phenomenon_id = phen_by_uri.get(matched_uri)
            if not phenomenon_id:
                continue
            ids_by_phenomenon.setdefault(phenomenon_id, []).append(int(row["id"]))

        for phenomenon_id, ids in ids_by_phenomenon.items():
            core.table("timeseries").update({"phenomenon_id": phenomenon_id}).in_("id", ids).execute()
            updated += len(ids)

        if args.limit is not None and scanned >= int(args.limit):
            break
        if len(rows) < batch_size:
            break
        offset += batch_size

    print(
        f"sensorcommunity_backfill_timeseries_phenomena done: connector_id={connector_id} "
        f"service_ref={args.service_ref} scanned={scanned} updated={updated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
