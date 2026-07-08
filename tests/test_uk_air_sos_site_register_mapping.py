from types import SimpleNamespace

import pytest

from scripts.uk_air_sos.uk_air_sos_site_register import (
    _refresh_site_timeseries_refs,
)


class FakeRpc:
    def __init__(self, response):
        self.response = response

    def execute(self):
        return SimpleNamespace(data=self.response)


class FakeSchema:
    def __init__(self, response):
        self.response = response
        self.call = None

    def rpc(self, name, params):
        self.call = (name, params)
        return FakeRpc(self.response)


class FakeClient:
    def __init__(self, response):
        self.schema_client = FakeSchema(response)
        self.schema_name = None

    def schema(self, name):
        self.schema_name = name
        return self.schema_client


def test_refresh_site_timeseries_refs_calls_public_rpc(monkeypatch):
    monkeypatch.setenv("UK_AQ_PUBLIC_SCHEMA", "uk_aq_public")
    response = {
        "mapping_rows_upserted": 12,
        "mapped_site_refs": 5,
        "unmapped_aurn_sites": 2,
        "ambiguous_groups": 0,
        "invalid_intervals": 0,
    }
    client = FakeClient(response)
    schemas = SimpleNamespace(client=client)

    result = _refresh_site_timeseries_refs(
        schemas,
        "2026-07-08T18:56:57+00:00",
    )

    assert result == response
    assert client.schema_name == "uk_aq_public"
    assert client.schema_client.call == (
        "uk_aq_rpc_uk_air_sos_site_timeseries_refs_refresh",
        {"p_source_snapshot_at": "2026-07-08T18:56:57+00:00"},
    )


def test_refresh_site_timeseries_refs_rejects_invalid_response():
    client = FakeClient([{"mapping_rows_upserted": 12}])
    schemas = SimpleNamespace(client=client)

    with pytest.raises(RuntimeError, match="invalid response"):
        _refresh_site_timeseries_refs(
            schemas,
            "2026-07-08T18:56:57+00:00",
        )
