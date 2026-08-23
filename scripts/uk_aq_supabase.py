import os
from dataclasses import dataclass
from typing import Any, Optional

from postgrest import SyncPostgrestClient
from supabase import Client, ClientOptions, create_client

from scripts.uk_aq_service_egress_metrics import (
    configured_service_egress_metrics,
)

DEFAULT_CORE_SCHEMA = os.getenv("UK_AQ_CORE_SCHEMA", "uk_aq_core")
DEFAULT_RAW_SCHEMA = os.getenv("UK_AQ_RAW_SCHEMA", "uk_aq_raw")
DEFAULT_POP_SCHEMA = os.getenv("UK_AQ_POP_SCHEMA", "uk_aq_pop")


def _resolve_schema_client(client: Client, schema: str) -> Client:
    if not schema or schema == "public":
        return client
    if hasattr(client, "schema"):
        return client.schema(schema)
    postgrest = getattr(client, "postgrest", None)
    if postgrest is not None and hasattr(postgrest, "schema"):
        return postgrest.schema(schema)
    raise RuntimeError("Supabase client does not support schema selection.")


class SchemaClient:
    def __init__(self, client: Client, schema: str) -> None:
        self._client = _resolve_schema_client(client, schema)

    def table(self, name: str):
        if hasattr(self._client, "table"):
            return self._client.table(name)
        if hasattr(self._client, "from_"):
            return self._client.from_(name)
        raise RuntimeError("Supabase client does not expose table/from_ methods.")

    def rpc(self, fn: str, params: Optional[dict] = None):
        if hasattr(self._client, "rpc"):
            return self._client.rpc(fn, params)
        raise RuntimeError("Supabase client does not expose rpc.")


@dataclass
class SupabaseSchemas:
    client: Client
    core: SchemaClient
    raw: SchemaClient
    pop: SchemaClient
    public: SchemaClient

    @classmethod
    def from_client(
        cls,
        client: Client,
        core_schema: str = DEFAULT_CORE_SCHEMA,
        raw_schema: str = DEFAULT_RAW_SCHEMA,
        pop_schema: str = DEFAULT_POP_SCHEMA,
    ) -> "SupabaseSchemas":
        return cls(
            client=client,
            core=SchemaClient(client, core_schema),
            raw=SchemaClient(client, raw_schema),
            pop=SchemaClient(client, pop_schema),
            public=SchemaClient(client, "public"),
        )


class _MeteredPostgrestSupabaseClient:
    """PostgREST-only Supabase facade using the public shared HTTP client API."""

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        collector = configured_service_egress_metrics()
        if collector is None:
            raise RuntimeError("Service egress metrics collector is not configured.")
        self._rest_url = f"{supabase_url.rstrip('/')}/rest/v1"
        self._http_client = collector.create_httpx_client(supabase_url)
        default_headers = dict(ClientOptions().headers)
        self._headers = {
            **default_headers,
            "apiKey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        }
        self._public = self._schema_client("public")

    def _schema_client(self, schema: str) -> SyncPostgrestClient:
        return SyncPostgrestClient(
            self._rest_url,
            schema=schema,
            headers=dict(self._headers),
            http_client=self._http_client,
        )

    @property
    def postgrest(self) -> SyncPostgrestClient:
        return self._public

    def schema(self, schema: str) -> SyncPostgrestClient:
        return self._schema_client(schema)

    def table(self, name: str):
        return self._public.table(name)

    def from_(self, name: str):
        return self._public.from_(name)

    def rpc(self, fn: str, params: Optional[dict] = None):
        return self._public.rpc(fn, params)


def create_supabase_client(
    supabase_url: Optional[str] = None,
    supabase_key: Optional[str] = None,
) -> Any:
    supabase_url = supabase_url or os.getenv("SUPABASE_URL")
    supabase_key = supabase_key or os.getenv("SB_SECRET_KEY") or os.getenv(
        "SUPABASE_KEY"
    )
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL and SB_SECRET_KEY are required.")
    collector = configured_service_egress_metrics()
    if collector is not None and collector.enabled:
        return _MeteredPostgrestSupabaseClient(supabase_url, supabase_key)
    return create_client(supabase_url, supabase_key)
