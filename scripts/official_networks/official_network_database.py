"""Pooled IngestDB transport for devolved official-network observations."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

DATABASE_URL_ENV = "UK_AQ_GCP_CLOUD_RUN_DATABASE_URL"
APPLICATION_NAME = "uk_aq_official_network_ingest"
CONNECT_TIMEOUT_SECONDS = 10
STATEMENT_TIMEOUT_SECONDS = 30


class OfficialNetworkDatabase:
    """Invoke only the authorised compact observation v2 database function."""

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
    def from_environment(cls) -> "OfficialNetworkDatabase":
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

    @staticmethod
    def _required_count(row: Sequence[Any] | None) -> int:
        if row is None or not row or row[0] is None:
            raise RuntimeError(
                "Official-network database observation upsert returned no usable row count."
            )
        try:
            return int(row[0])
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Official-network database observation upsert returned no usable row count."
            ) from exc

    def upsert_compact_observations_v2(
        self, arguments: Mapping[str, Sequence[Any] | str | None]
    ) -> int:
        try:
            connection = self._connect()
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_SECONDS}s'"
                    )
                    cursor.execute(
                        """
                        select observations_upserted
                        from uk_aq_public.uk_aq_rpc_observations_compact_upsert_v2(
                            %s::integer[],
                            %s::timestamptz[],
                            %s::double precision[],
                            %s::text,
                            %s::text[]
                        );
                        """,
                        (
                            arguments["timeseries_ids"],
                            arguments["observed_ats"],
                            arguments["values"],
                            arguments["acquisition_method"],
                            arguments.get("statuses"),
                        ),
                    )
                    return self._required_count(cursor.fetchone())
        except Exception:
            # Preserve the original psycopg exception/sqlstate for callers. A later
            # same-transport attempt may reconnect; there is no PostgREST fallback.
            self._discard_connection()
            raise
