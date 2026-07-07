from dataclasses import dataclass
from typing import Any

from scripts.blondon_nodes.blondon_nodes_ingest import SPECIES_CONFIG, SupabaseWriter


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
    assert SPECIES_CONFIG["PM25Index"]["observed_property_code"] is None
    assert SPECIES_CONFIG["NO2Index"]["observed_property_code"] is None


def test_upsert_phenomena_uses_central_rpc_and_returns_ids() -> None:
    species = ["PM25", "NO2", "PM25Index", "NO2Index"]
    writer = SupabaseWriter.__new__(SupabaseWriter)
    writer.public = FakePublic(expected_diagnostics(species))

    result, observed_property_ids = writer.upsert_phenomena(2, species)

    assert len(result) == 4
    assert observed_property_ids["breathelondon_nodes:pm2.5"] == 9
    assert observed_property_ids["breathelondon_nodes:no2"] == 12
    assert observed_property_ids["breathelondon_nodes:pm2.5:daqi"] is None
    assert writer.public.calls[0][0] == "uk_aq_rpc_phenomena_upsert"
    payload = writer.public.calls[0][1]
    assert set(payload) == {"rows"}
    assert payload["rows"][0]["mapping_kind"] == "raw_observed_property"
    assert payload["rows"][2]["mapping_kind"] == "derived_index"


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
