#!/usr/bin/env python3
"""Sync uk_aq_core reference tables from ingest Supabase to a destination Supabase.

Sync semantics:
- Source of truth rows are read from ingest via PostgREST.
- Destination rows are mirrored via `uk_aq_public` RPCs that operate on
  `uk_aq_core` internally.
- Destination rows are upserted by primary key and hard-deleted when missing
  from source.

Tables synced (dependency order):
1) connectors
2) phenomena
3) stations
4) timeseries
"""

from __future__ import annotations

import difflib
import hashlib
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests

PRIMARY_TABLES: List[str] = ["connectors", "phenomena", "stations", "timeseries"]
DEPENDENCY_TABLES: List[str] = [
    "observed_properties",
    "categories",
    "offerings",
    "features",
    "procedures",
]
SYNC_TABLES: List[str] = [
    "connectors",
    "observed_properties",
    "categories",
    "phenomena",
    "offerings",
    "features",
    "procedures",
    "stations",
    "timeseries",
]
DELETE_ORDER: List[str] = [
    "timeseries",
    "stations",
    "procedures",
    "features",
    "offerings",
    "phenomena",
    "categories",
    "observed_properties",
    "connectors",
]
CORE_SCHEMA = "uk_aq_core"
PUBLIC_SCHEMA = "uk_aq_public"

PAGE_SIZE = 1000
UPSERT_BATCH_SIZE = 500

RETRY_MAX_ATTEMPTS = 5
RETRY_INITIAL_DELAY_SECONDS = 1.0
RETRY_MULTIPLIER = 2.0
RETRY_MAX_DELAY_SECONDS = 30.0
RETRY_JITTER_FRACTION = 0.25
RETRYABLE_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}
RETRYABLE_REQUEST_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.SSLError,
    requests.exceptions.Timeout,
)

COLUMNS_RPC = "uk_aq_rpc_info_schema_columns"
PK_RPC = "uk_aq_rpc_info_schema_primary_keys"
CORE_SELECT_RPC = "uk_aq_rpc_core_table_select"
CORE_UPSERT_RPC = "uk_aq_rpc_core_table_upsert"
CORE_DELETE_KEYS_RPC = "uk_aq_rpc_core_table_delete_keys"
REPAIR_OBSERVED_PROPERTIES_RPC = "uk_aq_rpc_repair_observed_property_id_drift"
REPAIR_OBSERVED_PROPERTIES_ENV = "OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS"

# Static source metadata fallback copied from ingest uk_aq_core DDL
# (`schemas/ingest_db/uk_aq_core_schema.sql`) for the four mirrored tables.
STATIC_SOURCE_TABLE_META: Dict[str, Dict[str, Any]] = {
    "connectors": {
        "pk": ["id"],
        "columns": [
            {"column_name": "id", "udt_name": "int4", "is_nullable": "NO", "column_default": None, "ordinal_position": 1},
            {"column_name": "connector_code", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 2},
            {"column_name": "label", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 3},
            {"column_name": "display_name", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 4},
            {"column_name": "service_url", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 5},
            {"column_name": "station_display_name_template", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 6},
            {"column_name": "overwrite_station_name", "udt_name": "bool", "is_nullable": "YES", "column_default": "true", "ordinal_position": 7},
            {"column_name": "poll_enabled", "udt_name": "bool", "is_nullable": "YES", "column_default": "true", "ordinal_position": 8},
            {"column_name": "poll_interval_minutes", "udt_name": "int4", "is_nullable": "YES", "column_default": "60", "ordinal_position": 9},
            {"column_name": "poll_window_hours", "udt_name": "int4", "is_nullable": "YES", "column_default": "6", "ordinal_position": 10},
            {"column_name": "poll_timeseries_batch_size", "udt_name": "int4", "is_nullable": "YES", "column_default": None, "ordinal_position": 11},
            {"column_name": "scheduler_backend", "udt_name": "text", "is_nullable": "NO", "column_default": "'supabase_function'", "ordinal_position": 12},
            {"column_name": "stations_bbox_supported", "udt_name": "bool", "is_nullable": "YES", "column_default": "true", "ordinal_position": 13},
            {"column_name": "timeseries_station_filter_supported", "udt_name": "bool", "is_nullable": "YES", "column_default": "true", "ordinal_position": 14},
            {"column_name": "last_polled_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 15},
            {"column_name": "last_run_start", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 16},
            {"column_name": "last_run_end", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 17},
            {"column_name": "last_run_status", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 18},
            {"column_name": "last_run_message", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 19},
            {"column_name": "created_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": "now()", "ordinal_position": 20},
            {"column_name": "default_network_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 21},
            {"column_name": "config", "udt_name": "jsonb", "is_nullable": "NO", "column_default": "'{}'::jsonb", "ordinal_position": 22},
            {"column_name": "metadata", "udt_name": "jsonb", "is_nullable": "NO", "column_default": "'{}'::jsonb", "ordinal_position": 23},
            {"column_name": "updated_at", "udt_name": "timestamptz", "is_nullable": "NO", "column_default": "now()", "ordinal_position": 24},
        ],
    },
    "phenomena": {
        "pk": ["id"],
        "columns": [
            {"column_name": "id", "udt_name": "int8", "is_nullable": "NO", "column_default": None, "ordinal_position": 1},
            {"column_name": "label", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 2},
            {"column_name": "source_label", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 3},
            {"column_name": "notation", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 4},
            {"column_name": "pollutant_label", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 5},
            {"column_name": "observed_property_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 6},
            {"column_name": "connector_id", "udt_name": "int4", "is_nullable": "NO", "column_default": None, "ordinal_position": 7},
        ],
    },
    "stations": {
        "pk": ["id"],
        "columns": [
            {"column_name": "id", "udt_name": "int8", "is_nullable": "NO", "column_default": None, "ordinal_position": 1},
            {"column_name": "station_ref", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 2},
            {"column_name": "service_ref", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 3},
            {"column_name": "label", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 4},
            {"column_name": "station_name", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 5},
            {"column_name": "station_type", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 6},
            {"column_name": "station_exposure", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 7},
            {"column_name": "region", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 8},
            {"column_name": "la_code", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 9},
            {"column_name": "la_version", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 10},
            {"column_name": "pcon_code", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 11},
            {"column_name": "pcon_version", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 12},
            {"column_name": "geometry", "udt_name": "geography", "is_nullable": "YES", "column_default": None, "ordinal_position": 13},
            {"column_name": "connector_id", "udt_name": "int4", "is_nullable": "NO", "column_default": None, "ordinal_position": 14},
            {"column_name": "category_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 15},
            {"column_name": "first_seen_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": "now()", "ordinal_position": 16},
            {"column_name": "last_seen_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 17},
            {"column_name": "removed_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 18},
            {"column_name": "created_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": "now()", "ordinal_position": 19},
            {"column_name": "network_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 20},
            {"column_name": "match_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 21},
            {"column_name": "priority", "udt_name": "int4", "is_nullable": "NO", "column_default": "100", "ordinal_position": 22},
            {"column_name": "station_device_ref", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 23},
            {"column_name": "description", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 24},
            {"column_name": "photo_url", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 25},
            {"column_name": "sensor_height_m", "udt_name": "numeric", "is_nullable": "YES", "column_default": None, "ordinal_position": 26},
            {"column_name": "distance_to_road_m", "udt_name": "numeric", "is_nullable": "YES", "column_default": None, "ordinal_position": 27},
            {"column_name": "is_indoor", "udt_name": "bool", "is_nullable": "YES", "column_default": None, "ordinal_position": 28},
            {"column_name": "latitude", "udt_name": "float8", "is_nullable": "YES", "column_default": None, "ordinal_position": 29},
            {"column_name": "longitude", "udt_name": "float8", "is_nullable": "YES", "column_default": None, "ordinal_position": 30},
            {"column_name": "updated_at", "udt_name": "timestamptz", "is_nullable": "NO", "column_default": "now()", "ordinal_position": 31},
        ],
    },
    "timeseries": {
        "pk": ["id"],
        "columns": [
            {"column_name": "id", "udt_name": "int4", "is_nullable": "NO", "column_default": None, "ordinal_position": 1},
            {"column_name": "timeseries_ref", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 2},
            {"column_name": "label", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 3},
            {"column_name": "uom", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 4},
            {"column_name": "station_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 5},
            {"column_name": "service_ref", "udt_name": "text", "is_nullable": "NO", "column_default": None, "ordinal_position": 6},
            {"column_name": "connector_id", "udt_name": "int4", "is_nullable": "NO", "column_default": None, "ordinal_position": 7},
            {"column_name": "offering_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 8},
            {"column_name": "feature_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 9},
            {"column_name": "procedure_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 10},
            {"column_name": "phenomenon_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 11},
            {"column_name": "category_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 12},
            {"column_name": "first_value_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 13},
            {"column_name": "last_value_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 14},
            {"column_name": "last_value", "udt_name": "float8", "is_nullable": "YES", "column_default": None, "ordinal_position": 15},
            {"column_name": "extras", "udt_name": "jsonb", "is_nullable": "YES", "column_default": None, "ordinal_position": 16},
            {"column_name": "rendering_hints", "udt_name": "jsonb", "is_nullable": "YES", "column_default": None, "ordinal_position": 17},
            {"column_name": "status_intervals", "udt_name": "jsonb", "is_nullable": "YES", "column_default": None, "ordinal_position": 18},
            {"column_name": "created_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": "now()", "ordinal_position": 19},
            {"column_name": "updated_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": "now()", "ordinal_position": 20},
            {"column_name": "last_catalog_seen_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 21},
            {"column_name": "catalog_missing_runs", "udt_name": "int4", "is_nullable": "NO", "column_default": "0", "ordinal_position": 22},
            {"column_name": "ended_at", "udt_name": "timestamptz", "is_nullable": "YES", "column_default": None, "ordinal_position": 23},
            {"column_name": "observed_property_id", "udt_name": "int8", "is_nullable": "YES", "column_default": None, "ordinal_position": 24},
            {"column_name": "status", "udt_name": "text", "is_nullable": "YES", "column_default": None, "ordinal_position": 25},
            {"column_name": "metadata", "udt_name": "jsonb", "is_nullable": "NO", "column_default": "'{}'::jsonb", "ordinal_position": 26},
        ],
    },
}


class SyncError(RuntimeError):
    """Fatal sync error."""


@dataclass(frozen=True)
class ObservedPropertyIdMismatch:
    code: str
    source_id: int
    destination_id: int
    source_row: Dict[str, Any]


@dataclass(frozen=True)
class ColumnMeta:
    table_name: str
    column_name: str
    udt_name: str
    is_nullable: str
    column_default: Optional[str]
    ordinal_position: int

    def normalized(self) -> "ColumnMeta":
        return ColumnMeta(
            table_name=self.table_name,
            column_name=self.column_name,
            udt_name=self.udt_name,
            is_nullable=self.is_nullable,
            column_default=normalize_default(self.column_default),
            ordinal_position=self.ordinal_position,
        )


def required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise SyncError(f"Missing required environment variable: {name}")
    return value


def chunks(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    for i in range(0, len(items), size):
        yield items[i : i + size]


def normalize_default(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    # Strip wrapping parentheses that PostgreSQL may add.
    while text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        if not inner:
            break
        text = inner

    # Remove explicit casts for comparison stability.
    text = re.sub(r"::[a-zA-Z_][a-zA-Z0-9_\.\[\]]*", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def parse_udt_from_column_def(column_def: str) -> str:
    lowered = column_def.lower().strip()
    if lowered.startswith("integer") or lowered.startswith("int ") or lowered == "int":
        return "int4"
    if lowered.startswith("bigint"):
        return "int8"
    if lowered.startswith("smallint"):
        return "int2"
    if lowered.startswith("text"):
        return "text"
    if lowered.startswith("boolean"):
        return "bool"
    if lowered.startswith("timestamptz"):
        return "timestamptz"
    if lowered.startswith("double precision"):
        return "float8"
    if lowered.startswith("jsonb"):
        return "jsonb"
    if lowered.startswith("geography"):
        return "geography"
    if lowered.startswith("uuid"):
        return "uuid"
    if lowered.startswith("numeric"):
        return "numeric"
    raise SyncError(f"Unsupported column type in source DDL: {column_def}")


def extract_table_block(sql_text: str, table_name: str) -> str:
    pattern = re.compile(
        rf"create\s+table\s+if\s+not\s+exists\s+(?:\"?[a-zA-Z_][a-zA-Z0-9_]*\"?\.)?\"?{re.escape(table_name)}\"?\s*\(",
        re.IGNORECASE,
    )
    match = pattern.search(sql_text)
    if not match:
        raise SyncError(f"Could not find CREATE TABLE for {table_name} in source schema SQL")

    start_paren = match.end() - 1
    depth = 0
    end_pos = -1
    for idx in range(start_paren, len(sql_text)):
        char = sql_text[idx]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                end_pos = idx
                break

    if end_pos < 0:
        raise SyncError(f"Could not parse CREATE TABLE column block for {table_name}")

    return sql_text[start_paren + 1 : end_pos]


def parse_source_table_metadata(schema_sql_path: Path, table_names: Sequence[str]) -> Tuple[Dict[str, List[ColumnMeta]], Dict[str, List[str]]]:
    if not schema_sql_path.exists():
        raise SyncError(f"Source schema SQL file not found: {schema_sql_path}")

    sql_text = schema_sql_path.read_text(encoding="utf-8")
    columns_by_table: Dict[str, List[ColumnMeta]] = {}
    pk_by_table: Dict[str, List[str]] = {}

    for table in table_names:
        block = extract_table_block(sql_text, table)
        ordinal = 0
        cols: List[ColumnMeta] = []
        pks: List[str] = []

        for raw_line in block.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("--"):
                continue
            if line.endswith(","):
                line = line[:-1].strip()
            if not line:
                continue

            keyword = line.split(None, 1)[0].lower()
            if keyword in {"constraint", "foreign", "unique", "check"}:
                continue
            if keyword == "primary":
                pk_match = re.search(r"primary\s+key\s*\(([^)]+)\)", line, re.IGNORECASE)
                if pk_match:
                    for part in pk_match.group(1).split(","):
                        pks.append(part.strip().strip('"'))
                continue

            col_name = line.split(None, 1)[0].strip().strip('"')
            col_def = line[len(line.split(None, 1)[0]) :].strip()
            col_def_lower = col_def.lower()

            ordinal += 1
            udt_name = parse_udt_from_column_def(col_def)
            is_nullable = "NO" if " not null" in col_def_lower else "YES"
            if " primary key" in col_def_lower:
                is_nullable = "NO"
                pks.append(col_name)

            if "generated by default as identity" in col_def_lower:
                column_default: Optional[str] = None
            else:
                default_match = re.search(r"\bdefault\b\s+(.+)", col_def, re.IGNORECASE)
                if default_match:
                    expr = default_match.group(1).strip()
                    expr = re.split(
                        r"\s+(?:not\s+null|references|primary\s+key|check|constraint)\b",
                        expr,
                        maxsplit=1,
                        flags=re.IGNORECASE,
                    )[0].strip()
                    column_default = expr or None
                else:
                    column_default = None

            cols.append(
                ColumnMeta(
                    table_name=table,
                    column_name=col_name,
                    udt_name=udt_name,
                    is_nullable=is_nullable,
                    column_default=column_default,
                    ordinal_position=ordinal,
                )
            )

        if not cols:
            raise SyncError(f"No columns parsed for {table} from source schema SQL")
        if not pks:
            raise SyncError(f"No primary key parsed for {table} from source schema SQL")

        pk_set = set(pks)
        cols = [
            ColumnMeta(
                table_name=c.table_name,
                column_name=c.column_name,
                udt_name=c.udt_name,
                is_nullable="NO" if c.column_name in pk_set else c.is_nullable,
                column_default=c.column_default,
                ordinal_position=c.ordinal_position,
            )
            for c in cols
        ]

        columns_by_table[table] = cols
        pk_by_table[table] = pks

    return columns_by_table, pk_by_table


def static_source_metadata(table_names: Sequence[str]) -> Tuple[Dict[str, List[ColumnMeta]], Dict[str, List[str]]]:
    columns_by_table: Dict[str, List[ColumnMeta]] = {}
    pk_by_table: Dict[str, List[str]] = {}

    for table in table_names:
        meta = STATIC_SOURCE_TABLE_META.get(table)
        if not meta:
            raise SyncError(f"Static source metadata missing for table: {table}")

        raw_cols = meta.get("columns") or []
        raw_pk = meta.get("pk") or []
        if not raw_cols:
            raise SyncError(f"Static source metadata has no columns for table: {table}")
        if not raw_pk:
            raise SyncError(f"Static source metadata has no primary key for table: {table}")

        cols: List[ColumnMeta] = []
        for item in raw_cols:
            cols.append(
                ColumnMeta(
                    table_name=table,
                    column_name=str(item["column_name"]),
                    udt_name=str(item["udt_name"]),
                    is_nullable=str(item["is_nullable"]),
                    column_default=item.get("column_default"),
                    ordinal_position=int(item["ordinal_position"]),
                )
            )

        columns_by_table[table] = sorted(cols, key=lambda c: c.ordinal_position)
        pk_by_table[table] = [str(x) for x in raw_pk]

    return columns_by_table, pk_by_table


def load_source_metadata(
    *,
    schema_sql_path: Path,
    tables: Sequence[str],
    allow_fallback: bool = True,
) -> Tuple[Dict[str, List[ColumnMeta]], Dict[str, List[str]], str]:
    # Preferred: parse source DDL from local schema checkout when available.
    if schema_sql_path.exists():
        try:
            src_cols, src_pk = parse_source_table_metadata(schema_sql_path, tables)
            return src_cols, src_pk, f"schema_sql:{schema_sql_path}"
        except SyncError as exc:
            if not allow_fallback:
                raise
            print(f"WARN: source schema SQL parse failed: {exc}; falling back.", file=sys.stderr)

    # Final fallback: embedded static metadata from ingest DDL.
    if not allow_fallback:
        raise SyncError(f"Source schema SQL file not found: {schema_sql_path}")
    src_cols, src_pk = static_source_metadata(tables)
    return src_cols, src_pk, "embedded_static"


def is_missing_rpc_error(message: str) -> bool:
    return "PGRST202" in message or "Could not find the function" in message


def request_context_label(path: str, params: Optional[Dict[str, str]] = None) -> str:
    pieces = [f"path={path}"]
    if params:
        rendered = ",".join(f"{key}={params[key]}" for key in sorted(params))
        if rendered:
            pieces.append(f"params={rendered}")
    return " ".join(pieces)


def is_retryable_request_exception(exc: BaseException) -> bool:
    return isinstance(exc, RETRYABLE_REQUEST_EXCEPTIONS)


def retry_sleep_seconds(attempt_number: int) -> float:
    base_delay = RETRY_INITIAL_DELAY_SECONDS * (RETRY_MULTIPLIER ** max(attempt_number - 1, 0))
    base_delay = min(RETRY_MAX_DELAY_SECONDS, base_delay)
    jitter = random.uniform(0.0, base_delay * RETRY_JITTER_FRACTION)
    return min(RETRY_MAX_DELAY_SECONDS, base_delay + jitter)


def format_retry_prefix(method: str, path: str, params: Optional[Dict[str, str]] = None) -> str:
    return f"PostgREST method={method.upper()} {request_context_label(path, params)}"


class PostgrestClient:
    def __init__(
        self,
        *,
        base_url: str,
        secret_key: str,
        caller: str,
        project_label: str = "destination",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.secret_key = secret_key
        self.caller = caller
        self.project_label = project_label

    def _headers(self, profile: str, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers: Dict[str, str] = {
            "apikey": self.secret_key,
            "Authorization": f"Bearer {self.secret_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Accept-Profile": profile,
            "Content-Profile": profile,
            "x-ukaq-egress-caller": self.caller,
        }
        if extra:
            headers.update(extra)
        return headers

    def request_json(
        self,
        method: str,
        path: str,
        *,
        profile: str,
        params: Optional[Dict[str, str]] = None,
        payload: Optional[Any] = None,
        extra_headers: Optional[Dict[str, str]] = None,
        timeout: int = 60,
    ) -> Any:
        url = f"{self.base_url}{path}"
        request_label = format_retry_prefix(method, path, params)

        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=self._headers(profile, extra_headers),
                    params=params,
                    json=payload,
                    timeout=timeout,
                )
            except requests.RequestException as exc:
                if not is_retryable_request_exception(exc):
                    raise SyncError(f"{request_label} failed: {exc}") from exc
                if attempt >= RETRY_MAX_ATTEMPTS:
                    raise SyncError(
                        f"{request_label} failed after {RETRY_MAX_ATTEMPTS} attempts: {exc}"
                    ) from exc
                sleep_seconds = retry_sleep_seconds(attempt)
                print(
                    f"RETRY {request_label} attempt={attempt}/{RETRY_MAX_ATTEMPTS} "
                    f"error={exc.__class__.__name__}: {exc} sleep_seconds={sleep_seconds:.1f}",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(sleep_seconds)
                continue

            if response.ok:
                if response.status_code == 204 or not response.text.strip():
                    return []
                return response.json()

            body = response.text.strip()
            # Supabase/PostgREST returns 406 PGRST106 when schema is not exposed.
            if response.status_code == 406 and "PGRST106" in body and f"Invalid schema: {CORE_SCHEMA}" in body:
                raise SyncError(
                    f"Project API does not expose schema '{CORE_SCHEMA}' for "
                    f"{self.project_label}. Add '{CORE_SCHEMA}' to Supabase API "
                    "Exposed schemas for that project, or use uk_aq_public mirror RPCs."
                )
            if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
                if attempt >= RETRY_MAX_ATTEMPTS:
                    raise SyncError(
                        f"{request_label} failed after {RETRY_MAX_ATTEMPTS} attempts "
                        f"({response.status_code}): {body or response.reason}"
                    )
                sleep_seconds = retry_sleep_seconds(attempt)
                print(
                    f"RETRY {request_label} attempt={attempt}/{RETRY_MAX_ATTEMPTS} "
                    f"status={response.status_code} reason={response.reason} "
                    f"sleep_seconds={sleep_seconds:.1f}",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(sleep_seconds)
                continue
            raise SyncError(
                f"PostgREST {method} {path} failed ({response.status_code}): {body or response.reason}"
            )

    def fetch_all_rows(
        self,
        table: str,
        *,
        profile: str,
        select: str,
        order: Optional[str] = None,
        page_size: int = PAGE_SIZE,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        offset = 0

        while True:
            params: Dict[str, str] = {
                "select": select,
                "limit": str(page_size),
                "offset": str(offset),
            }
            if order:
                params["order"] = order

            batch = self.request_json(
                "GET",
                f"/rest/v1/{table}",
                profile=profile,
                params=params,
            )
            if not isinstance(batch, list):
                raise SyncError(f"Expected list response for {table}, got: {type(batch).__name__}")

            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += len(batch)

        return rows

    def rpc(self, name: str, *, profile: str, args: Dict[str, Any]) -> Any:
        return self.request_json(
            "POST",
            f"/rest/v1/rpc/{name}",
            profile=profile,
            payload=args,
            timeout=60,
        )

    def fetch_core_rows_via_rpc(
        self,
        table: str,
        *,
        select_columns: Optional[Sequence[str]] = None,
        order_columns: Optional[Sequence[str]] = None,
        page_size: int = PAGE_SIZE,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        offset = 0
        while True:
            try:
                payload = self.rpc(
                    CORE_SELECT_RPC,
                    profile=PUBLIC_SCHEMA,
                    args={
                        "p_table_name": table,
                        "p_select_columns": list(select_columns) if select_columns else None,
                        "p_order_columns": list(order_columns) if order_columns else None,
                        "p_limit": page_size,
                        "p_offset": offset,
                    },
                )
            except SyncError as exc:
                message = str(exc)
                if is_missing_rpc_error(message):
                    raise SyncError(
                        "Destination core mirror RPCs are missing. Ensure destination DB exposes "
                        "uk_aq_public.uk_aq_rpc_core_table_select(text, text[], text[], integer, integer), "
                        "uk_aq_public.uk_aq_rpc_core_table_upsert(text, jsonb, text[]), and "
                        "uk_aq_public.uk_aq_rpc_core_table_delete_keys(text, text[], jsonb)."
                    ) from exc
                raise

            if not isinstance(payload, list):
                raise SyncError(
                    f"Expected list payload from {CORE_SELECT_RPC} for {table}, got {type(payload).__name__}"
                )

            batch: List[Dict[str, Any]] = []
            for item in payload:
                if not isinstance(item, dict):
                    raise SyncError(
                        f"{CORE_SELECT_RPC} returned non-object row for {table}: {type(item).__name__}"
                    )
                row = item.get("row_data")
                if not isinstance(row, dict):
                    raise SyncError(
                        f"{CORE_SELECT_RPC} returned invalid row_data for {table}: {json.dumps(item, sort_keys=True)}"
                    )
                batch.append(row)

            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += len(batch)

        return rows

    def upsert_core_rows_via_rpc(
        self,
        table: str,
        *,
        rows: Sequence[Dict[str, Any]],
        on_conflict_columns: Sequence[str],
    ) -> int:
        if not rows:
            return 0
        try:
            payload = self.rpc(
                CORE_UPSERT_RPC,
                profile=PUBLIC_SCHEMA,
                args={
                    "p_table_name": table,
                    "p_rows": list(rows),
                    "p_on_conflict_columns": list(on_conflict_columns),
                },
            )
        except SyncError as exc:
            message = str(exc)
            if is_missing_rpc_error(message):
                raise SyncError(
                    "Destination core mirror RPCs are missing. Ensure destination DB exposes "
                    "uk_aq_public.uk_aq_rpc_core_table_select(text, text[], text[], integer, integer), "
                    "uk_aq_public.uk_aq_rpc_core_table_upsert(text, jsonb, text[]), and "
                    "uk_aq_public.uk_aq_rpc_core_table_delete_keys(text, text[], jsonb)."
                ) from exc
            raise
        return extract_single_count(payload, "rows_upserted", CORE_UPSERT_RPC, table)

    def delete_core_keys_via_rpc(
        self,
        table: str,
        *,
        pk_columns: Sequence[str],
        keys: Sequence[Dict[str, Any]],
    ) -> int:
        if not keys:
            return 0
        try:
            payload = self.rpc(
                CORE_DELETE_KEYS_RPC,
                profile=PUBLIC_SCHEMA,
                args={
                    "p_table_name": table,
                    "p_pk_columns": list(pk_columns),
                    "p_keys": list(keys),
                },
            )
        except SyncError as exc:
            message = str(exc)
            if is_missing_rpc_error(message):
                raise SyncError(
                    "Destination core mirror RPCs are missing. Ensure destination DB exposes "
                    "uk_aq_public.uk_aq_rpc_core_table_select(text, text[], text[], integer, integer), "
                    "uk_aq_public.uk_aq_rpc_core_table_upsert(text, jsonb, text[]), and "
                    "uk_aq_public.uk_aq_rpc_core_table_delete_keys(text, text[], jsonb)."
                ) from exc
            raise
        return extract_single_count(payload, "rows_deleted", CORE_DELETE_KEYS_RPC, table)


def extract_single_count(payload: Any, key: str, rpc_name: str, table: str) -> int:
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise SyncError(
            f"{rpc_name} returned unexpected payload for {table}: {json.dumps(payload, sort_keys=True)}"
        )
    value = payload[0].get(key)
    try:
        return int(value or 0)
    except (TypeError, ValueError) as exc:
        raise SyncError(
            f"{rpc_name} returned non-integer {key} for {table}: {json.dumps(payload[0], sort_keys=True)}"
        ) from exc


def build_meta_maps(
    column_rows: Sequence[Dict[str, Any]],
    pk_rows: Sequence[Dict[str, Any]],
) -> Tuple[Dict[str, List[ColumnMeta]], Dict[str, List[str]]]:
    columns_by_table: Dict[str, List[ColumnMeta]] = {}
    for row in column_rows:
        table_name = str(row.get("table_name") or "").strip()
        if not table_name:
            continue
        item = ColumnMeta(
            table_name=table_name,
            column_name=str(row.get("column_name") or "").strip(),
            udt_name=str(row.get("udt_name") or "").strip(),
            is_nullable=str(row.get("is_nullable") or "").strip(),
            column_default=row.get("column_default"),
            ordinal_position=int(row.get("ordinal_position") or 0),
        )
        columns_by_table.setdefault(table_name, []).append(item)

    for table_name, cols in list(columns_by_table.items()):
        columns_by_table[table_name] = sorted(cols, key=lambda c: c.ordinal_position)

    pk_by_table: Dict[str, List[Tuple[int, str]]] = {}
    for row in pk_rows:
        table_name = str(row.get("table_name") or "").strip()
        if not table_name:
            continue
        pk_by_table.setdefault(table_name, []).append(
            (int(row.get("ordinal_position") or 0), str(row.get("column_name") or "").strip())
        )

    ordered_pk: Dict[str, List[str]] = {}
    for table_name, values in pk_by_table.items():
        ordered_pk[table_name] = [col for _, col in sorted(values, key=lambda x: x[0])]

    return columns_by_table, ordered_pk


def schema_column_key(column: ColumnMeta) -> Tuple[str, str]:
    return (column.table_name, column.column_name)


def schema_column_signature(column: ColumnMeta) -> Dict[str, Any]:
    normalized = column.normalized()
    return {
        "table": normalized.table_name,
        "column_name": normalized.column_name,
        "udt_name": normalized.udt_name,
        "is_nullable": normalized.is_nullable,
        "column_default": normalized.column_default,
    }


def format_column_lines(columns: Sequence[ColumnMeta]) -> List[str]:
    return [
        json.dumps(schema_column_signature(c), sort_keys=True)
        for c in sorted(columns, key=schema_column_key)
    ]


def explicit_select_columns(columns_by_table: Dict[str, List[ColumnMeta]], table: str) -> List[str]:
    columns = columns_by_table.get(table, [])
    if not columns:
        raise SyncError(f"{table}: no columns available for explicit select")
    return [c.column_name for c in sorted(columns, key=lambda c: c.ordinal_position)]


def verify_schema_matches(
    *,
    source_columns_by_table: Dict[str, List[ColumnMeta]],
    source_pk_by_table: Dict[str, List[str]],
    dest_columns_by_table: Dict[str, List[ColumnMeta]],
    dest_pk_by_table: Dict[str, List[str]],
    tables: Sequence[str],
) -> None:
    errors: List[str] = []

    for table in tables:
        src_cols = [c.normalized() for c in source_columns_by_table.get(table, [])]
        dst_cols = [c.normalized() for c in dest_columns_by_table.get(table, [])]
        if not src_cols:
            errors.append(f"{table}: source columns missing")
            continue
        if not dst_cols:
            errors.append(f"{table}: destination columns missing")
            continue

        src_by_name = {c.column_name: c for c in src_cols}
        dst_by_name = {c.column_name: c for c in dst_cols}
        missing_in_dest = sorted(set(src_by_name) - set(dst_by_name))
        extra_in_dest = sorted(set(dst_by_name) - set(src_by_name))
        mismatched_columns = [
            name
            for name in sorted(set(src_by_name) & set(dst_by_name))
            if schema_column_signature(src_by_name[name]) != schema_column_signature(dst_by_name[name])
        ]
        if missing_in_dest or extra_in_dest or mismatched_columns:
            src_lines = format_column_lines(src_cols)
            dst_lines = format_column_lines(dst_cols)
            diff = "\n".join(
                difflib.unified_diff(
                    src_lines,
                    dst_lines,
                    fromfile=f"source:{table}",
                    tofile=f"dest:{table}",
                    lineterm="",
                )
            )
            details = []
            if missing_in_dest:
                details.append(f"missing_in_destination={missing_in_dest}")
            if extra_in_dest:
                details.append(f"extra_in_destination={extra_in_dest}")
            if mismatched_columns:
                details.append(f"mismatched_columns={mismatched_columns}")
            errors.append(f"{table}: column definition mismatch ({'; '.join(details)})\n{diff}")

        src_pk = source_pk_by_table.get(table, [])
        dst_pk = dest_pk_by_table.get(table, [])
        if src_pk != dst_pk:
            errors.append(
                f"{table}: primary key mismatch\n"
                f"  source={src_pk}\n"
                f"  dest={dst_pk}"
            )

    if errors:
        raise SyncError("Schema verification failed:\n\n" + "\n\n".join(errors))


def pk_tuple(row: Dict[str, Any], pk_columns: Sequence[str]) -> Tuple[Any, ...]:
    return tuple(row.get(col) for col in pk_columns)


def as_int(value: Any, *, context: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SyncError(f"{context}: expected integer, got {value!r}") from exc


def build_connector_code_map(rows: Sequence[Dict[str, Any]]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for row in rows:
        cid = as_int(row.get("id"), context="connector.id")
        code = str(row.get("connector_code") or "").strip()
        out[cid] = code or f"connector_{cid}"
    return out


def observed_property_key(row: Dict[str, Any]) -> str:
    code = str(row.get("code") or "").strip()
    if not code:
        raise SyncError(f"observed_properties row is missing code: {json.dumps(row, sort_keys=True)}")
    return code


def collect_observed_properties_id_alignment(
    *,
    src_client: PostgrestClient,
    dst_client: PostgrestClient,
) -> List[ObservedPropertyIdMismatch]:
    src_rows = src_client.fetch_all_rows(
        "observed_properties",
        profile=CORE_SCHEMA,
        select="id,code,display_name,domain,canonical_uom,created_at,updated_at",
        order="code.asc",
    )
    dst_rows = dst_client.fetch_core_rows_via_rpc(
        "observed_properties",
        select_columns=["id", "code", "display_name", "domain", "canonical_uom"],
        order_columns=["code"],
    )

    src_by_code = {observed_property_key(row): row for row in src_rows}
    dst_by_code = {observed_property_key(row): row for row in dst_rows}

    missing_in_dst: List[Tuple[str, int]] = []
    extra_in_dst: List[Tuple[str, int]] = []
    id_mismatches: List[ObservedPropertyIdMismatch] = []

    for code in sorted(set(src_by_code) | set(dst_by_code)):
        src_row = src_by_code.get(code)
        dst_row = dst_by_code.get(code)
        src_id = as_int(src_row.get("id"), context="source observed_properties.id") if src_row else None
        dst_id = as_int(dst_row.get("id"), context="destination observed_properties.id") if dst_row else None
        if src_id is None and dst_id is not None:
            extra_in_dst.append((code, dst_id))
        elif dst_id is None and src_id is not None:
            missing_in_dst.append((code, src_id))
        elif src_id is not None and dst_id is not None and src_id != dst_id:
            id_mismatches.append(ObservedPropertyIdMismatch(code, src_id, dst_id, src_row or {}))

    print(
        "Observed properties pre-sync alignment summary: "
        f"id_mismatch={len(id_mismatches)} missing_in_destination={len(missing_in_dst)} "
        f"extra_in_destination={len(extra_in_dst)}"
    )

    for mismatch in id_mismatches[:100]:
        print(
            "OBSERVED_PROPERTY_ID_MISMATCH "
            f"code={mismatch.code} source_id={mismatch.source_id} destination_id={mismatch.destination_id}"
        )

    return id_mismatches


def verify_observed_properties_id_alignment(
    *,
    src_client: PostgrestClient,
    dst_client: PostgrestClient,
) -> None:
    id_mismatches = collect_observed_properties_id_alignment(src_client=src_client, dst_client=dst_client)
    if id_mismatches:
        raise SyncError(
            "Observed properties ID alignment check failed: destination rows share source natural keys "
            "but have different IDs. This mirror sync preserves source database IDs, so these rows must "
            f"be reconciled before upsert. To run the guarded generic repair once, set {REPAIR_OBSERVED_PROPERTIES_ENV}=1 "
            "and re-run this core sync; it will re-check alignment after repair before continuing."
        )


def repair_observed_properties_id_alignment(
    *,
    src_client: PostgrestClient,
    dst_client: PostgrestClient,
) -> None:
    id_mismatches = collect_observed_properties_id_alignment(src_client=src_client, dst_client=dst_client)
    if not id_mismatches:
        return

    print("Observed properties repair mode enabled; proposed changes:")
    repairs = []
    for mismatch in id_mismatches:
        print(
            "OBSERVED_PROPERTY_ID_REPAIR_PROPOSED "
            f"code={mismatch.code} rewire_destination_observed_property_fks={mismatch.destination_id}->{mismatch.source_id} "
            f"remove_stale_destination_id={mismatch.destination_id}"
        )
        repairs.append(
            {
                "code": mismatch.code,
                "source_id": mismatch.source_id,
                "destination_id": mismatch.destination_id,
                "source_row": mismatch.source_row,
            }
        )

    try:
        payload = dst_client.rpc(
            REPAIR_OBSERVED_PROPERTIES_RPC,
            profile=PUBLIC_SCHEMA,
            args={"p_repairs": repairs},
        )
    except SyncError as exc:
        if is_missing_rpc_error(str(exc)):
            raise SyncError(
                f"Destination repair RPC is missing. Apply supabase/sql/20260617_observed_properties_id_drift_repair_rpc.sql "
                f"to ObsAQIDB, then re-run with {REPAIR_OBSERVED_PROPERTIES_ENV}=1."
            ) from exc
        raise
    print(f"Observed properties repair RPC result: {json.dumps(payload, sort_keys=True)}")

    remaining = collect_observed_properties_id_alignment(src_client=src_client, dst_client=dst_client)
    if remaining:
        raise SyncError("Observed properties ID repair ran, but mismatches remain; refusing to continue sync.")

def timeseries_key(row: Dict[str, Any]) -> Tuple[int, str, str]:
    return (
        as_int(row.get("connector_id"), context="timeseries.connector_id"),
        str(row.get("service_ref") or ""),
        str(row.get("timeseries_ref") or ""),
    )


def md5_lines(lines: Sequence[str]) -> str:
    payload = "\n".join(lines)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def summarize_timeseries_by_connector(
    rows: Sequence[Dict[str, Any]],
    connector_codes: Dict[int, str],
) -> Dict[str, Dict[str, Any]]:
    by_connector: Dict[str, List[Tuple[str, str, int]]] = {}
    for row in rows:
        cid, service_ref, ts_ref = timeseries_key(row)
        ts_id = as_int(row.get("id"), context="timeseries.id")
        code = connector_codes.get(cid, f"connector_{cid}")
        by_connector.setdefault(code, []).append((service_ref, ts_ref, ts_id))

    summary: Dict[str, Dict[str, Any]] = {}
    for code, items in by_connector.items():
        key_only = sorted((sref, tref) for sref, tref, _ in items)
        key_plus_id = sorted((sref, tref, tsid) for sref, tref, tsid in items)
        summary[code] = {
            "row_count": len(items),
            "key_only_hash": md5_lines([f"{sref}|{tref}" for sref, tref in key_only]),
            "key_plus_id_hash": md5_lines([f"{sref}|{tref}|{tsid}" for sref, tref, tsid in key_plus_id]),
        }
    return summary


def verify_timeseries_id_alignment(
    *,
    src_client: PostgrestClient,
    dst_client: PostgrestClient,
) -> None:
    src_connectors = src_client.fetch_all_rows(
        "connectors",
        profile=CORE_SCHEMA,
        select="id,connector_code",
        order="id.asc",
    )
    dst_connectors = dst_client.fetch_core_rows_via_rpc(
        "connectors",
        select_columns=["id", "connector_code"],
        order_columns=["id"],
    )
    src_connector_codes = build_connector_code_map(src_connectors)
    dst_connector_codes = build_connector_code_map(dst_connectors)

    src_timeseries = src_client.fetch_all_rows(
        "timeseries",
        profile=CORE_SCHEMA,
        select="id,connector_id,service_ref,timeseries_ref",
        order="connector_id.asc,id.asc",
    )
    dst_timeseries = dst_client.fetch_core_rows_via_rpc(
        "timeseries",
        select_columns=["id", "connector_id", "service_ref", "timeseries_ref"],
        order_columns=["connector_id", "id"],
    )

    src_by_key = {timeseries_key(row): as_int(row.get("id"), context="source timeseries.id") for row in src_timeseries}
    dst_by_key = {timeseries_key(row): as_int(row.get("id"), context="destination timeseries.id") for row in dst_timeseries}

    missing_in_dst: List[Tuple[Tuple[int, str, str], int]] = []
    extra_in_dst: List[Tuple[Tuple[int, str, str], int]] = []
    id_mismatches: List[Tuple[Tuple[int, str, str], int, int]] = []

    for key in sorted(set(src_by_key) | set(dst_by_key)):
        src_id = src_by_key.get(key)
        dst_id = dst_by_key.get(key)
        if src_id is None and dst_id is not None:
            extra_in_dst.append((key, dst_id))
        elif dst_id is None and src_id is not None:
            missing_in_dst.append((key, src_id))
        elif src_id is not None and dst_id is not None and src_id != dst_id:
            id_mismatches.append((key, src_id, dst_id))

    src_summary = summarize_timeseries_by_connector(src_timeseries, src_connector_codes)
    dst_summary = summarize_timeseries_by_connector(dst_timeseries, dst_connector_codes)

    print(
        "Timeseries pre-sync alignment summary: "
        f"id_mismatch={len(id_mismatches)} missing_in_destination={len(missing_in_dst)} "
        f"extra_in_destination={len(extra_in_dst)}"
    )
    print(
        "connector_code,source_row_count,destination_row_count,"
        "source_key_only_hash,destination_key_only_hash,"
        "source_key_plus_id_hash,destination_key_plus_id_hash"
    )
    for code in sorted(set(src_summary) | set(dst_summary)):
        src_item = src_summary.get(code, {})
        dst_item = dst_summary.get(code, {})
        print(
            f"{code},"
            f"{src_item.get('row_count', 0)},{dst_item.get('row_count', 0)},"
            f"{src_item.get('key_only_hash', '')},{dst_item.get('key_only_hash', '')},"
            f"{src_item.get('key_plus_id_hash', '')},{dst_item.get('key_plus_id_hash', '')}"
        )

    for key, src_id, dst_id in id_mismatches[:100]:
        cid, service_ref, ts_ref = key
        connector_code = src_connector_codes.get(cid) or dst_connector_codes.get(cid) or f"connector_{cid}"
        print(
            "TIMESERIES_ID_MISMATCH "
            f"connector_code={connector_code} connector_id={cid} "
            f"service_ref={service_ref} timeseries_ref={ts_ref} "
            f"source_id={src_id} destination_id={dst_id}"
        )

    # Missing/extra natural keys are expected drift that this mirror sync can
    # reconcile via upsert + delete phases. Only block when the same natural
    # key exists on both sides with different numeric IDs.
    if id_mismatches:
        raise SyncError(
            "Timeseries ID alignment check failed: destination timeseries IDs do not match source for shared natural keys. "
            "Run scripts/stations_daily/uk_aq_repair_obs_aqidb_timeseries_ids.py first, "
            "then re-run this core sync."
        )


def main() -> int:
    src_url = required_env("SRC_SUPABASE_URL")
    src_key = required_env("SRC_SECRET_KEY")
    dst_url = required_env("DST_SUPABASE_URL")
    dst_key = required_env("DST_SECRET_KEY")
    sync_target_label = (os.getenv("SYNC_TARGET_LABEL") or "destination").strip() or "destination"
    caller_prefix_raw = (os.getenv("SYNC_CALLER_PREFIX") or "stations_daily_sync_core").strip().lower()
    caller_prefix = re.sub(r"[^a-z0-9_]+", "_", caller_prefix_raw).strip("_") or "stations_daily_sync_core"

    schema_sql_path_raw = (os.getenv("UK_AQ_INGEST_CORE_SCHEMA_SQL_PATH") or "").strip()
    schema_sql_path = Path(schema_sql_path_raw) if schema_sql_path_raw else Path("__unset__")

    src_client = PostgrestClient(
        base_url=src_url,
        secret_key=src_key,
        caller=f"{caller_prefix}_source",
        project_label="source",
    )
    dst_client = PostgrestClient(
        base_url=dst_url,
        secret_key=dst_key,
        caller=f"{caller_prefix}_dest",
        project_label=sync_target_label,
    )

    source_columns, source_pk, source_meta_mode = load_source_metadata(
        schema_sql_path=schema_sql_path,
        tables=PRIMARY_TABLES,
        allow_fallback=True,
    )
    print(f"Loaded source schema metadata via: {source_meta_mode}")

    try:
        dst_column_rows = dst_client.rpc(
            COLUMNS_RPC,
            profile=PUBLIC_SCHEMA,
            args={"p_schema": CORE_SCHEMA, "p_table_names": SYNC_TABLES},
        )
        dst_pk_rows = dst_client.rpc(
            PK_RPC,
            profile=PUBLIC_SCHEMA,
            args={"p_schema": CORE_SCHEMA, "p_table_names": SYNC_TABLES},
        )
    except SyncError as exc:
        message = str(exc)
        if is_missing_rpc_error(message):
            raise SyncError(
                "Destination metadata RPCs are missing. Ensure destination DB exposes "
                "uk_aq_public.uk_aq_rpc_info_schema_columns(text, text[]) and "
                "uk_aq_public.uk_aq_rpc_info_schema_primary_keys(text, text[])."
            ) from exc
        raise

    if not isinstance(dst_column_rows, list) or not isinstance(dst_pk_rows, list):
        raise SyncError("Destination metadata RPCs returned unexpected payloads")

    dest_columns, dest_pk = build_meta_maps(dst_column_rows, dst_pk_rows)

    verify_schema_matches(
        source_columns_by_table=source_columns,
        source_pk_by_table=source_pk,
        dest_columns_by_table=dest_columns,
        dest_pk_by_table=dest_pk,
        tables=PRIMARY_TABLES,
    )

    print("Schema verification passed for all primary sync tables.")
    if (os.getenv(REPAIR_OBSERVED_PROPERTIES_ENV) or "").strip().lower() in {"1", "true", "yes", "on"}:
        repair_observed_properties_id_alignment(src_client=src_client, dst_client=dst_client)
    else:
        verify_observed_properties_id_alignment(src_client=src_client, dst_client=dst_client)
    verify_timeseries_id_alignment(src_client=src_client, dst_client=dst_client)

    table_stats: Dict[str, Dict[str, Any]] = {}
    missing_by_table: Dict[str, List[Tuple[Any, ...]]] = {}

    # Phase 1: upsert all rows for dependency-safe set of tables.
    for table in SYNC_TABLES:
        pk_columns = dest_pk.get(table, [])
        if not pk_columns:
            raise SyncError(f"{table}: no destination PK columns found")

        order_expr = ",".join(f"{col}.asc" for col in pk_columns)

        source_select_columns = explicit_select_columns(dest_columns, table)
        source_rows = src_client.fetch_all_rows(
            table,
            profile=CORE_SCHEMA,
            select=",".join(source_select_columns),
            order=order_expr,
        )

        source_count = len(source_rows)
        for batch in chunks(source_rows, UPSERT_BATCH_SIZE):
            dst_client.upsert_core_rows_via_rpc(
                table,
                rows=batch,
                on_conflict_columns=pk_columns,
            )

        source_pk_set = {pk_tuple(row, pk_columns) for row in source_rows}
        dst_pk_rows_before_delete = dst_client.fetch_core_rows_via_rpc(
            table,
            select_columns=pk_columns,
            order_columns=pk_columns,
        )
        dest_pk_set = {pk_tuple(row, pk_columns) for row in dst_pk_rows_before_delete}
        missing_by_table[table] = sorted(dest_pk_set - source_pk_set)
        table_stats[table] = {
            "table": table,
            "source_row_count": source_count,
            "upsert_attempted": source_count,
            "destination_row_count_after_sync": len(dest_pk_set),
            "deleted": 0,
            "pk_columns": pk_columns,
        }

    # Phase 2: hard-delete rows missing in source in FK-safe reverse order.
    for table in DELETE_ORDER:
        pk_columns = dest_pk.get(table, [])
        if not pk_columns:
            raise SyncError(f"{table}: no destination PK columns found for delete phase")
        missing_keys = missing_by_table.get(table, [])
        missing_key_rows = [
            {col: value for col, value in zip(pk_columns, key)}
            for key in missing_keys
        ]

        deleted = dst_client.delete_core_keys_via_rpc(
            table=table,
            pk_columns=pk_columns,
            keys=missing_key_rows,
        )

        dst_pk_rows_after_delete = dst_client.fetch_core_rows_via_rpc(
            table,
            select_columns=pk_columns,
            order_columns=pk_columns,
        )
        table_stats[table]["deleted"] = deleted
        table_stats[table]["destination_row_count_after_sync"] = len(dst_pk_rows_after_delete)

    for table in SYNC_TABLES:
        print(json.dumps(table_stats[table], sort_keys=True))

    print(f"uk_aq_core sync to {sync_target_label} completed successfully.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
