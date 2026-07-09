from types import SimpleNamespace

import pytest
import requests

from scripts.sos.sos_site_register import (
    _load_register,
    _get_with_retry,
    _refresh_station_uk_air_refs,
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
        "uk_aq_rpc_sos_station_timeseries_site_refs_refresh",
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


def test_refresh_station_uk_air_refs_calls_public_rpc(monkeypatch):
    monkeypatch.setenv("UK_AQ_PUBLIC_SCHEMA", "uk_aq_public")
    response = {
        "mapping_rows_upserted": 8,
        "matched_station_refs": 8,
        "deleted_station_refs": 2,
        "name_distance_matches": 6,
        "distance_matches": 2,
        "ambiguous_register_rows": 1,
        "unmatched_register_rows": 4,
    }
    client = FakeClient(response)
    schemas = SimpleNamespace(client=client)

    result = _refresh_station_uk_air_refs(
        schemas,
        "2026-07-08T18:56:57+00:00",
    )

    assert result == response
    assert client.schema_name == "uk_aq_public"
    assert client.schema_client.call == (
        "uk_aq_rpc_sos_station_uk_air_refs_refresh",
        {"p_source_snapshot_at": "2026-07-08T18:56:57+00:00"},
    )


def test_get_with_retry_retries_transient_failures(monkeypatch):
    calls = {"count": 0}

    class DummyResponse:
        text = "ok"

        def raise_for_status(self):
            return None

    def fake_get(url, headers=None, timeout=None):
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.exceptions.ConnectionError("transient failure")
        return DummyResponse()

    monkeypatch.setattr("scripts.sos.sos_site_register.requests.get", fake_get)
    monkeypatch.setattr("scripts.sos.sos_site_register.time.sleep", lambda _seconds: None)

    resp = _get_with_retry("https://example.invalid", {"User-Agent": "pytest"}, 5)

    assert resp.text == "ok"
    assert calls["count"] == 2


def test_load_register_refreshes_bridge_before_timeseries(monkeypatch, tmp_path):
    csv_path = tmp_path / "sos_site_register.csv"
    csv_path.write_text(
        "\n".join(
            [
                "UK-AIR ID,Site Name,Latitude,Longitude,Networks",
                "UKA00591,Ealing Horn Lane,51.5123,-0.3045,Automatic Urban and Rural Monitoring Network (AURN)",
            ]
        ),
        encoding="utf-8",
    )

    class DummySchemas:
        core = object()
        raw = object()

    calls = []

    monkeypatch.setattr(
        "scripts.sos.sos_site_register._build_client",
        lambda: DummySchemas(),
    )
    monkeypatch.setattr(
        "scripts.sos.sos_site_register._fetch_existing_networks",
        lambda _core: {},
    )
    monkeypatch.setattr(
        "scripts.sos.sos_site_register._upsert_batches",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "scripts.sos.sos_site_register._upsert_network_pollutants",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "scripts.sos.sos_site_register._refresh_station_uk_air_refs",
        lambda _schemas, snapshot: calls.append(("bridge", snapshot)),
    )
    monkeypatch.setattr(
        "scripts.sos.sos_site_register._refresh_site_timeseries_refs",
        lambda _schemas, snapshot: calls.append(("timeseries", snapshot)),
    )

    _load_register(
        csv_path=str(csv_path),
        source_url=None,
        source_file=None,
        snapshot_at="2026-07-08T18:56:57+00:00",
        site_ref_map_csv=None,
        validate_site_ref_map=False,
        discover_site_refs=False,
        timeout=5,
        user_agent="pytest",
        batch_size=100,
        dry_run=False,
    )

    assert calls == [
        ("bridge", "2026-07-08T18:56:57+00:00"),
        ("timeseries", "2026-07-08T18:56:57+00:00"),
    ]
