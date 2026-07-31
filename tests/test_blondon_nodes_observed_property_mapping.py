from dataclasses import dataclass
from typing import Any

from scripts.blondon_nodes.blondon_nodes_ingest import SupabaseWriter
from scripts.blondon_nodes.blondon_nodes_reference_data import (
    SPECIES_CONFIG,
    build_nodes_timeseries_rows,
)


@dataclass
class FakeResponse:
    data: list[dict[str, Any]]


class FakeRpc:
    def __init__(self, response_rows: list[dict[str, Any]]) -> None:
        self.response_rows = response_rows

    def execute(self) -> FakeResponse:
        return FakeResponse(self.response_rows)


class FakePublic:
    def __init__(self, response_rows: list[dict[str, Any]]) -> None:
        self.response_rows = response_rows
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def rpc(self, name: str, params: dict[str, Any]) -> FakeRpc:
        self.calls.append((name, params))
        return FakeRpc(self.response_rows)


def expected_diagnostics(species: list[str]) -> list[dict[str, Any]]:
    rows = []
    for index, name in enumerate(species, start=1):
        config = SPECIES_CONFIG[name]
        rows.append(
            {
                "source_label": config["source_label"],
                "phenomenon_id": index,
                "observed_property_id": (
                    9
                    if name == "PM25"
                    else 12
                    if name == "NO2"
                    else 19
                    if name == "PM25Index"
                    else 20
                    if name == "NO2Index"
                    else None
                ),
                "observed_property_code": config["observed_property_code"],
                "mapping_kind": config["mapping_kind"],
                "is_aqi_eligible": config["is_aqi_eligible"],
                "mapping_status": "existing",
                "mapping_warning": None,
            }
        )
    return rows


def test_species_mapping_contract() -> None:
    assert SPECIES_CONFIG["PM25"]["observed_property_code"] == "pm25"
    assert SPECIES_CONFIG["NO2"]["observed_property_code"] == "no2"
    assert SPECIES_CONFIG["PM25"]["is_aqi_eligible"] is True
    assert SPECIES_CONFIG["NO2"]["is_aqi_eligible"] is True
    assert SPECIES_CONFIG["PM25Index"]["mapping_kind"] == "derived_index"
    assert SPECIES_CONFIG["NO2Index"]["mapping_kind"] == "derived_index"
    assert SPECIES_CONFIG["PM25Index"]["observed_property_code"] == "pm25index"
    assert SPECIES_CONFIG["NO2Index"]["observed_property_code"] == "no2index"


def test_shared_builder_constructs_deterministic_complete_timeseries_rows() -> None:
    phenomenon_ids = {
        config["source_label"]: index
        for index, config in enumerate(SPECIES_CONFIG.values(), start=1)
    }
    observed_property_ids = {
        config["source_label"]: index
        for index, config in enumerate(SPECIES_CONFIG.values(), start=11)
    }
    rows = build_nodes_timeseries_rows(
        [{"id": 42, "station_ref": "BL0001", "station_name": "Example"}],
        connector_id=7,
        phenomenon_ids=phenomenon_ids,
        observed_property_ids=observed_property_ids,
        service_ref="breathelondon",
    )

    assert [row["timeseries_ref"] for row in rows] == [
        "BL0001:PM25",
        "BL0001:NO2",
        "BL0001:PM25Index",
        "BL0001:NO2Index",
    ]
    assert rows[0] == {
        "timeseries_ref": "BL0001:PM25",
        "label": "Example PM2.5",
        "uom": "ug.m-3",
        "station_id": 42,
        "service_ref": "breathelondon",
        "connector_id": 7,
        "phenomenon_id": 1,
        "observed_property_id": 11,
        "extras": {
            "site_code": "BL0001",
            "species": "PM25",
            "measurement_kind": "pollutant",
            "api_units": "ug.m-3",
        },
    }


def test_upsert_phenomena_uses_central_rpc_and_returns_ids() -> None:
    species = ["PM25", "NO2", "PM25Index", "NO2Index"]
    writer = SupabaseWriter.__new__(SupabaseWriter)
    writer.public = FakePublic(expected_diagnostics(species))

    result, observed_property_ids = writer.upsert_phenomena(2, species)

    assert len(result) == 4
    assert observed_property_ids["breathelondon_nodes:pm2.5"] == 9
    assert observed_property_ids["breathelondon_nodes:no2"] == 12
    assert observed_property_ids["breathelondon_nodes:pm2.5:daqi"] == 19
    assert observed_property_ids["breathelondon_nodes:no2:daqi"] == 20
    assert writer.public.calls[0][0] == "uk_aq_rpc_phenomena_upsert"
    payload = writer.public.calls[0][1]
    assert set(payload) == {"rows"}
    assert payload["rows"][0]["mapping_kind"] == "raw_observed_property"
    assert payload["rows"][2]["mapping_kind"] == "derived_index"
    assert payload["rows"][2]["observed_property_code"] == "pm25index"
    assert payload["rows"][3]["observed_property_code"] == "no2index"


def test_upsert_phenomena_rejects_rpc_warning() -> None:
    species = ["PM25"]
    response = expected_diagnostics(species)
    response[0]["mapping_warning"] = "unknown_source_label"
    writer = SupabaseWriter.__new__(SupabaseWriter)
    writer.public = FakePublic(response)

    try:
        writer.upsert_phenomena(2, species)
    except RuntimeError as exc:
        assert "unknown_source_label" in str(exc)
    else:
        raise AssertionError("Expected mapping warning to fail the ingest metadata write")
