import json
from typing import Any, Dict, List

import pytest
import requests

from scripts.stations_daily import sync_obs_aqidb_uk_aq_core as sync_mod


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        json_data: Any = None,
        text: str = "",
        reason: str = "OK",
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data
        self.text = text
        self.reason = reason
        self.ok = 200 <= status_code < 400
        self.headers: Dict[str, str] = {}

    def json(self) -> Any:
        return self._json_data


def make_client() -> sync_mod.PostgrestClient:
    return sync_mod.PostgrestClient(
        base_url="https://example.test",
        secret_key="secret",
        caller="caller",
        project_label="source",
    )


def patch_retry_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sync_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(sync_mod.random, "uniform", lambda _a, _b: 0.0)


def test_request_json_retries_ssl_error_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    client = make_client()
    patch_retry_helpers(monkeypatch)
    calls: List[Dict[str, Any]] = []

    def fake_request(*, method, url, headers, params, json, timeout):
        calls.append({"method": method, "url": url, "params": params, "timeout": timeout})
        if len(calls) == 1:
            raise requests.exceptions.SSLError("EOF while reading")
        return FakeResponse(json_data=[{"id": 1}], text='[{"id":1}]')

    monkeypatch.setattr(sync_mod.requests, "request", fake_request)

    result = client.request_json(
        "GET",
        "/rest/v1/stations",
        profile="uk_aq_core",
        params={"select": "*"},
    )

    assert result == [{"id": 1}]
    assert len(calls) == 2


def test_request_json_retries_timeout_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    client = make_client()
    patch_retry_helpers(monkeypatch)
    calls: List[Dict[str, Any]] = []

    def fake_request(*, method, url, headers, params, json, timeout):
        calls.append({"method": method, "url": url, "params": params, "timeout": timeout})
        if len(calls) == 1:
            raise requests.exceptions.Timeout("timed out")
        return FakeResponse(json_data=[{"id": 2}], text='[{"id":2}]')

    monkeypatch.setattr(sync_mod.requests, "request", fake_request)

    result = client.request_json(
        "GET",
        "/rest/v1/stations",
        profile="uk_aq_core",
        params={"select": "*"},
    )

    assert result == [{"id": 2}]
    assert len(calls) == 2


def test_request_json_retries_http_503_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    client = make_client()
    patch_retry_helpers(monkeypatch)
    calls: List[Dict[str, Any]] = []

    def fake_request(*, method, url, headers, params, json, timeout):
        calls.append({"method": method, "url": url, "params": params, "timeout": timeout})
        if len(calls) == 1:
            return FakeResponse(status_code=503, text="Service unavailable", reason="Service Unavailable")
        return FakeResponse(json_data=[{"id": 3}], text='[{"id":3}]')

    monkeypatch.setattr(sync_mod.requests, "request", fake_request)

    result = client.request_json(
        "GET",
        "/rest/v1/stations",
        profile="uk_aq_core",
        params={"select": "*"},
    )

    assert result == [{"id": 3}]
    assert len(calls) == 2


def test_request_json_fails_fast_on_http_400(monkeypatch: pytest.MonkeyPatch) -> None:
    client = make_client()
    patch_retry_helpers(monkeypatch)
    calls: List[Dict[str, Any]] = []

    def fake_request(*, method, url, headers, params, json, timeout):
        calls.append({"method": method, "url": url, "params": params, "timeout": timeout})
        return FakeResponse(status_code=400, text='{"error":"bad request"}', reason="Bad Request")

    monkeypatch.setattr(sync_mod.requests, "request", fake_request)

    with pytest.raises(sync_mod.SyncError, match=r"400"):
        client.request_json(
            "GET",
            "/rest/v1/stations",
            profile="uk_aq_core",
            params={"select": "*"},
        )

    assert len(calls) == 1


def test_request_json_retries_exhausted_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    client = make_client()
    patch_retry_helpers(monkeypatch)
    calls: List[Dict[str, Any]] = []

    def fake_request(*, method, url, headers, params, json, timeout):
        calls.append({"method": method, "url": url, "params": params, "timeout": timeout})
        return FakeResponse(status_code=503, text="Service unavailable", reason="Service Unavailable")

    monkeypatch.setattr(sync_mod.requests, "request", fake_request)

    with pytest.raises(sync_mod.SyncError, match=r"failed after 5 attempts"):
        client.request_json(
            "GET",
            "/rest/v1/stations",
            profile="uk_aq_core",
            params={"select": "*"},
        )

    assert len(calls) == sync_mod.RETRY_MAX_ATTEMPTS


def test_fetch_all_rows_retries_middle_page_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    client = make_client()
    patch_retry_helpers(monkeypatch)
    calls: List[Dict[str, Any]] = []

    responses = [
        FakeResponse(json_data=[{"id": 1}, {"id": 2}], text=json.dumps([{"id": 1}, {"id": 2}])),
        requests.exceptions.ConnectionError("temporary disconnect"),
        FakeResponse(json_data=[{"id": 3}, {"id": 4}], text=json.dumps([{"id": 3}, {"id": 4}])),
        FakeResponse(json_data=[{"id": 5}], text=json.dumps([{"id": 5}])),
    ]

    def fake_request(*, method, url, headers, params, json, timeout):
        calls.append({"method": method, "url": url, "params": dict(params or {}), "timeout": timeout})
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(sync_mod.requests, "request", fake_request)

    rows = client.fetch_all_rows(
        "stations",
        profile="uk_aq_core",
        select="*",
        order="id.asc",
        page_size=2,
    )

    assert [row["id"] for row in rows] == [1, 2, 3, 4, 5]
    assert len(calls) == 4
    assert calls[0]["params"]["offset"] == "0"
    assert calls[1]["params"]["offset"] == "2"
    assert calls[2]["params"]["offset"] == "2"
    assert calls[3]["params"]["offset"] == "4"


def test_verify_observed_properties_id_alignment_allows_missing_and_extra() -> None:
    class FakeClient:
        def __init__(self, rows):
            self.rows = rows

        def fetch_all_rows(self, *args, **kwargs):
            return self.rows

        def fetch_core_rows_via_rpc(self, *args, **kwargs):
            return self.rows

    src = FakeClient([
        {"id": 17, "code": "oc6h4ch32"},
        {"id": 18, "code": "no2"},
    ])
    dst = FakeClient([
        {"id": 17, "code": "oc6h4ch32"},
        {"id": 19, "code": "pm10"},
    ])

    sync_mod.verify_observed_properties_id_alignment(src_client=src, dst_client=dst)


def test_verify_observed_properties_id_alignment_blocks_shared_code_id_mismatch() -> None:
    class SourceClient:
        def fetch_all_rows(self, *args, **kwargs):
            return [{"id": 17, "code": "oc6h4ch32"}]

    class DestinationClient:
        def fetch_core_rows_via_rpc(self, *args, **kwargs):
            return [{"id": 16, "code": "oc6h4ch32"}]

    with pytest.raises(sync_mod.SyncError, match="Observed properties ID alignment check failed"):
        sync_mod.verify_observed_properties_id_alignment(
            src_client=SourceClient(),
            dst_client=DestinationClient(),
        )


def test_repair_observed_properties_id_alignment_rewires_wrong_destination_id() -> None:
    class SourceClient:
        def fetch_all_rows(self, *args, **kwargs):
            return [
                {
                    "id": 42,
                    "code": "124c6h3ch33",
                    "display_name": "1,2,4-trimethylbenzene",
                    "domain": "aq",
                    "canonical_uom": None,
                    "created_at": "2026-06-09T00:00:00+00:00",
                    "updated_at": "2026-06-09T00:00:00+00:00",
                }
            ]

    class DestinationClient:
        def __init__(self):
            self.observed = [{"id": 41, "code": "124c6h3ch33"}]
            self.phenomena = [{"id": 100, "observed_property_id": 41}]
            self.rpc_calls = []

        def fetch_core_rows_via_rpc(self, *args, **kwargs):
            return list(self.observed)

        def rpc(self, name, *, profile, args):
            self.rpc_calls.append({"name": name, "profile": profile, "args": args})
            assert name == sync_mod.REPAIR_OBSERVED_PROPERTIES_RPC
            repair = args["p_repairs"][0]
            assert repair["code"] == "124c6h3ch33"
            assert repair["source_id"] == 42
            assert repair["destination_id"] == 41
            self.observed = [{"id": 42, "code": "124c6h3ch33"}]
            for row in self.phenomena:
                if row["observed_property_id"] == 41:
                    row["observed_property_id"] = 42
            return [{"code": "124c6h3ch33", "source_id": 42, "destination_id": 41, "dependent_rewrites": {"uk_aq_core.phenomena.observed_property_id": 1}, "stale_rows_deleted": 1}]

    dst = DestinationClient()
    sync_mod.repair_observed_properties_id_alignment(src_client=SourceClient(), dst_client=dst)

    assert dst.rpc_calls
    assert dst.observed == [{"id": 42, "code": "124c6h3ch33"}]
    assert dst.phenomena == [{"id": 100, "observed_property_id": 42}]


def test_repair_observed_properties_id_alignment_refuses_remaining_ambiguous_mismatch() -> None:
    class SourceClient:
        def fetch_all_rows(self, *args, **kwargs):
            return [{"id": 42, "code": "124c6h3ch33", "display_name": "x", "domain": "aq", "canonical_uom": None}]

    class DestinationClient:
        def fetch_core_rows_via_rpc(self, *args, **kwargs):
            # Simulates the SQL RPC refusing an ambiguous case (source ID already used by another code)
            # or otherwise leaving the mismatch unresolved.
            return [{"id": 41, "code": "124c6h3ch33"}, {"id": 42, "code": "different_code"}]

        def rpc(self, name, *, profile, args):
            return []

    with pytest.raises(sync_mod.SyncError, match="mismatches remain"):
        sync_mod.repair_observed_properties_id_alignment(src_client=SourceClient(), dst_client=DestinationClient())


def test_verify_blondon_daqi_index_metadata_accepts_index_links() -> None:
    class SourceClient:
        def fetch_all_rows(self, table, *args, **kwargs):
            if table == "connectors":
                return [{"id": 2, "connector_code": "blondon_nodes"}]
            if table == "observed_properties":
                return [
                    {"id": 19, "code": "pm25index"},
                    {"id": 20, "code": "no2index"},
                    {"id": 21, "code": "pm10index"},
                ]
            if table == "phenomena":
                return [
                    {
                        "id": 101,
                        "connector_id": 2,
                        "source_label": "breathelondon_nodes:pm2.5:daqi",
                        "observed_property_id": 19,
                    },
                    {
                        "id": 102,
                        "connector_id": 2,
                        "source_label": "breathelondon_nodes:no2:daqi",
                        "observed_property_id": 20,
                    },
                ]
            if table == "timeseries":
                return [
                    {
                        "id": 1001,
                        "connector_id": 2,
                        "phenomenon_id": 101,
                        "observed_property_id": 19,
                        "extras": {"measurement_kind": "daqi_index", "species": "PM25Index"},
                    },
                    {
                        "id": 1002,
                        "connector_id": 2,
                        "phenomenon_id": 102,
                        "observed_property_id": 20,
                        "extras": {"measurement_kind": "daqi_index", "species": "NO2Index"},
                    },
                ]
            raise AssertionError(table)

    sync_mod.verify_blondon_daqi_index_metadata(src_client=SourceClient())


def test_verify_blondon_daqi_index_metadata_blocks_null_index_links() -> None:
    class SourceClient:
        def fetch_all_rows(self, table, *args, **kwargs):
            if table == "connectors":
                return [{"id": 2, "connector_code": "blondon_nodes"}]
            if table == "observed_properties":
                return [
                    {"id": 19, "code": "pm25index"},
                    {"id": 20, "code": "no2index"},
                ]
            if table == "phenomena":
                return [
                    {
                        "id": 101,
                        "connector_id": 2,
                        "source_label": "breathelondon_nodes:pm2.5:daqi",
                        "observed_property_id": None,
                    }
                ]
            if table == "timeseries":
                return [
                    {
                        "id": 1001,
                        "connector_id": 2,
                        "phenomenon_id": 101,
                        "observed_property_id": None,
                        "extras": {"measurement_kind": "daqi_index", "species": "PM25Index"},
                    }
                ]
            raise AssertionError(table)

    with pytest.raises(sync_mod.SyncError, match="Refusing to mirror Breathe London DAQI index metadata"):
        sync_mod.verify_blondon_daqi_index_metadata(src_client=SourceClient())


def col(
    table: str,
    name: str,
    udt: str = "text",
    nullable: str = "YES",
    default: Any = None,
    ordinal: int = 1,
) -> sync_mod.ColumnMeta:
    return sync_mod.ColumnMeta(
        table_name=table,
        column_name=name,
        udt_name=udt,
        is_nullable=nullable,
        column_default=default,
        ordinal_position=ordinal,
    )


def verify_single_table(source_cols, dest_cols) -> None:
    sync_mod.verify_schema_matches(
        source_columns_by_table={"timeseries": source_cols},
        source_pk_by_table={"timeseries": ["id"]},
        dest_columns_by_table={"timeseries": dest_cols},
        dest_pk_by_table={"timeseries": ["id"]},
        tables=["timeseries"],
    )


def test_verify_schema_matches_ignores_physical_column_order() -> None:
    source_cols = [
        col("timeseries", "id", "int4", "NO", None, 1),
        col("timeseries", "last_catalog_seen_at", "timestamptz", "YES", None, 2),
        col("timeseries", "catalog_missing_runs", "int4", "NO", "0", 3),
        col("timeseries", "ended_at", "timestamptz", "YES", None, 4),
        col("timeseries", "created_at", "timestamptz", "YES", "now()", 5),
        col("timeseries", "updated_at", "timestamptz", "YES", "now()", 6),
    ]
    dest_cols = [
        col("timeseries", "id", "int4", "NO", None, 1),
        col("timeseries", "created_at", "timestamptz", "YES", "now()", 2),
        col("timeseries", "updated_at", "timestamptz", "YES", "now()", 3),
        col("timeseries", "last_catalog_seen_at", "timestamptz", "YES", None, 4),
        col("timeseries", "catalog_missing_runs", "int4", "NO", "0", 5),
        col("timeseries", "ended_at", "timestamptz", "YES", None, 6),
    ]

    verify_single_table(source_cols, dest_cols)


@pytest.mark.parametrize(
    "dest_cols,match_text",
    [
        ([col("timeseries", "id", "int4", "NO", None, 1)], "missing_in_destination"),
        (
            [
                col("timeseries", "id", "int4", "NO", None, 1),
                col("timeseries", "label", "text", "YES", None, 2),
                col("timeseries", "extra_col", "text", "YES", None, 3),
            ],
            "extra_in_destination",
        ),
        (
            [
                col("timeseries", "id", "int8", "NO", None, 1),
                col("timeseries", "label", "text", "YES", None, 2),
            ],
            "mismatched_columns",
        ),
        (
            [
                col("timeseries", "id", "int4", "YES", None, 1),
                col("timeseries", "label", "text", "YES", None, 2),
            ],
            "mismatched_columns",
        ),
        (
            [
                col("timeseries", "id", "int4", "NO", "0", 1),
                col("timeseries", "label", "text", "YES", None, 2),
            ],
            "mismatched_columns",
        ),
    ],
)
def test_verify_schema_matches_still_blocks_real_schema_differences(dest_cols, match_text: str) -> None:
    source_cols = [
        col("timeseries", "id", "int4", "NO", None, 1),
        col("timeseries", "label", "text", "YES", None, 2),
    ]

    with pytest.raises(sync_mod.SyncError, match=match_text):
        verify_single_table(source_cols, dest_cols)


def test_station_matches_is_mirrored_before_stations() -> None:
    assert "station_matches" in sync_mod.SYNC_TABLES
    assert sync_mod.SYNC_TABLES.index("station_matches") < sync_mod.SYNC_TABLES.index("stations")
    assert "station_matches" in sync_mod.PRIMARY_TABLES


def test_station_matches_is_deleted_after_stations() -> None:
    assert "station_matches" in sync_mod.DELETE_ORDER
    assert sync_mod.DELETE_ORDER.index("station_matches") > sync_mod.DELETE_ORDER.index("stations")


def test_station_matches_static_metadata_matches_canonical_source_shape() -> None:
    metadata = sync_mod.STATIC_SOURCE_TABLE_META["station_matches"]

    assert metadata["pk"] == ["id"]
    assert [column["column_name"] for column in metadata["columns"]] == [
        "id",
        "uk_air_ref",
        "match_name",
        "latitude",
        "longitude",
        "geometry",
        "match_method",
        "match_confidence",
        "notes",
        "metadata",
        "created_at",
        "updated_at",
    ]
    assert metadata["columns"][0] == {
        "column_name": "id",
        "udt_name": "int8",
        "is_nullable": "NO",
        "column_default": None,
        "ordinal_position": 1,
    }


def test_station_matches_upsert_preserves_explicit_source_id() -> None:
    metadata = sync_mod.STATIC_SOURCE_TABLE_META["station_matches"]
    client = make_client()
    calls = []

    def fake_rpc(name, *, profile, args):
        calls.append({"name": name, "profile": profile, "args": args})
        return [{"rows_upserted": 1}]

    client.rpc = fake_rpc
    row = {
        "id": 160,
        "uk_air_ref": "UKA00933",
        "match_name": "Example canonical site",
        "latitude": 51.5,
        "longitude": -0.1,
        "geometry": None,
        "match_method": "uk_air_canonical_reference",
        "match_confidence": 1,
        "notes": None,
        "metadata": {},
        "created_at": "2026-10-04T00:00:00+00:00",
        "updated_at": "2026-10-04T00:00:00+00:00",
    }

    assert client.upsert_core_rows_via_rpc(
        "station_matches",
        rows=[row],
        on_conflict_columns=metadata["pk"],
    ) == 1
    assert calls == [
        {
            "name": sync_mod.CORE_UPSERT_RPC,
            "profile": sync_mod.PUBLIC_SCHEMA,
            "args": {
                "p_table_name": "station_matches",
                "p_rows": [row],
                "p_on_conflict_columns": ["id"],
            },
        }
    ]
