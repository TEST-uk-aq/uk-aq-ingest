"""Pooled IngestDB transport for Breathe London Nodes."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping, Sequence
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DATABASE_URL_ENV = "UK_AQ_GCP_CLOUD_RUN_DATABASE_URL"
APPLICATION_NAME = "uk_aq_blondon_nodes_ingest"
CONNECT_TIMEOUT_SECONDS = 10
STATEMENT_TIMEOUT_SECONDS = 30


class BlondonNodesDatabase:
    """Invoke the authorised Nodes database functions on one connection."""

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url.strip()
        if not self._database_url:
            raise RuntimeError(f"{DATABASE_URL_ENV} is required in database mode.")
        try:
            conninfo = conninfo_to_dict(self._database_url)
        except Exception as exc:
            raise RuntimeError(
                f"{DATABASE_URL_ENV} is not valid PostgreSQL conninfo."
            ) from exc
        if str(conninfo.get("port") or "") != "6543":
            raise RuntimeError(
                f"{DATABASE_URL_ENV} must use the Shared Pooler transaction port 6543."
            )
        self._connection: psycopg.Connection[Any] | None = None

    @classmethod
    def from_environment(cls) -> "BlondonNodesDatabase":
        return cls(os.getenv(DATABASE_URL_ENV) or "")

    def _connect(self) -> psycopg.Connection[Any]:
        if self._connection is None or self._connection.closed:
            self._connection = psycopg.connect(
                self._database_url,
                application_name=APPLICATION_NAME,
                autocommit=True,
                connect_timeout=CONNECT_TIMEOUT_SECONDS,
                prepare_threshold=None,
                sslmode="require",
            )
        return self._connection

    def _discard_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass

    def close(self) -> None:
        self._discard_connection()

    def _read_rows(self, query: str, arguments: tuple[Any, ...]) -> list[dict[str, Any]]:
        connection = self._connect()
        try:
            with connection.transaction():
                with connection.cursor(row_factory=dict_row) as cursor:
                    cursor.execute(
                        f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_SECONDS}s'"
                    )
                    cursor.execute(query, arguments)
                    return cursor.fetchall()
        except Exception:
            self._discard_connection()
            raise

    def read_timeseries_references(
        self, connector_id: int, refs: Sequence[str]
    ) -> list[dict[str, Any]]:
        return self._read_rows(
            """select * from uk_aq_public.uk_aq_rpc_timeseries_reference_read_v1(
                %s::integer, %s::text[]
            )""",
            (connector_id, list(refs)),
        )

    def read_timeseries_value_bounds(self, ids: Sequence[int]) -> list[dict[str, Any]]:
        # Keep timestamps that the previous ISO parser could represent in Python.
        rows = self._read_rows(
            """
            select id,
                case when first_value_at >= timestamptz '0001-01-01 00:00:00+00'
                      and first_value_at < timestamptz '10000-01-01 00:00:00+00'
                     then first_value_at end as first_value_at,
                case when last_value_at >= timestamptz '0001-01-01 00:00:00+00'
                      and last_value_at < timestamptz '10000-01-01 00:00:00+00'
                     then last_value_at end as last_value_at,
                last_value
            from uk_aq_public.uk_aq_rpc_timeseries_value_bounds_read_v1(%s::integer[])
            """,
            (list(ids),),
        )
        for row in rows:
            value = row["last_value"]
            if value is not None and not math.isfinite(value):
                # PostgreSQL JSON represents non-finite floats as strings.
                row["last_value"] = {
                    "inf": "Infinity", "-inf": "-Infinity", "nan": "NaN"
                }[str(value)]
        return rows

    def recover_observations(
        self, rows: Sequence[Mapping[str, Any]], config: Mapping[str, Any],
        retention_days: int, claim_token: str,
    ) -> dict[str, Any]:
        """Recovery-only function; no normal compact-v1 or transport fallback."""
        connection = self._connect()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout = '10s'")
                    cursor.execute(
                        """select uk_aq_public.uk_aq_rpc_blondon_nodes_recovery_insert_v1(
                            %s::integer[], %s::timestamptz[], %s::double precision[],
                            %s::text[], %s::jsonb, %s::integer, %s::uuid
                        )""",
                        ([row["timeseries_id"] for row in rows],
                         [row["observed_at"] for row in rows],
                         [row["value"] for row in rows],
                         [row.get("status") for row in rows],
                         Jsonb(dict(config)), retention_days, claim_token),
                    )
                    result = cursor.fetchone()
            if result is None or not isinstance(result[0], dict):
                raise RuntimeError("Unexpected Nodes recovery database result")
            return result[0]
        except Exception:
            self._discard_connection()
            raise

    def upsert_compact_observations_v1(
        self, arguments: Mapping[str, Sequence[Any] | None]
    ) -> None:
        connection = self._connect()
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_SECONDS}s'"
                    )
                    cursor.execute(
                        """
                        select *
                        from uk_aq_public.uk_aq_rpc_observations_compact_upsert_v1(
                            %s::integer[],
                            %s::timestamptz[],
                            %s::double precision[],
                            %s::text[]
                        );
                        """,
                        (
                            arguments["timeseries_ids"],
                            arguments["observed_ats"],
                            arguments["values"],
                            arguments.get("statuses"),
                        ),
                    )
                    cursor.fetchall()
        except Exception:
            self._discard_connection()
            raise
