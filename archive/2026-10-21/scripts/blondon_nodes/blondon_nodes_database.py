"""Pooled IngestDB transport for Breathe London Nodes observations."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

DATABASE_URL_ENV = "UK_AQ_GCP_CLOUD_RUN_DATABASE_URL"
APPLICATION_NAME = "uk_aq_blondon_nodes_ingest"
CONNECT_TIMEOUT_SECONDS = 10
STATEMENT_TIMEOUT_SECONDS = 30


class BlondonNodesDatabase:
    """Invoke only the authorised compact observation v1 database function."""

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
