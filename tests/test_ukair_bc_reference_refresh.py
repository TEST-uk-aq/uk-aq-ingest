from datetime import date
from pathlib import Path

import pytest

from scripts.ukair_bc.ukair_bc_reference_refresh import (
    PROPERTY_CONFIG,
    CatalogueStation,
    PlannedStation,
    SiteRefEvidence,
    SourceFormatError,
    build_phenomena_rows,
    build_station_row,
    build_timeseries_rows,
    choose_existing_site_ref,
    parse_catalogue_csv,
    parse_property_support_html,
    timeseries_ref,
)


HEADER = (
    "UK-AIR ID,EU Site ID,EMEP Site ID,Site Name,Environment Type,Zone,"
    "Start Date,End Date,Latitude,Longitude,Northing,Easting,Altitude (m),"
    "Networks,AURN Pollutants Measured,Site Description\n"
)


def catalogue_row(
    uka: str = "UKA01055", *, end_date: str = "", networks: str = "UK Black Carbon Network"
) -> bytes:
    return (
        HEADER
        + f'{uka},GB0000A,,Shrewsbury Underdale,Urban Background,West Midlands,'
        f'2019-01-01,{end_date},52.7,-2.7,1,2,10,"{networks}",,Description\n'
    ).encode()


def test_catalogue_parser_filters_supported_period_without_fixed_counts() -> None:
    current = parse_catalogue_csv(catalogue_row())[0]
    relevant_closed = parse_catalogue_csv(catalogue_row(end_date="2020-01-01"))[0]
    old_closed = parse_catalogue_csv(catalogue_row(end_date="2019-12-31"))[0]
    assert current.relevant and current.active
    assert relevant_closed.relevant and not relevant_closed.active
    assert not old_closed.relevant


def test_catalogue_parser_rejects_wrong_network_and_duplicate_uka() -> None:
    with pytest.raises(SourceFormatError, match="lacks Black Carbon"):
        parse_catalogue_csv(catalogue_row(networks="Automatic Urban and Rural Monitoring Network (AURN)"))
    duplicate = catalogue_row() + catalogue_row().split(b"\n", 1)[1]
    with pytest.raises(SourceFormatError, match="Duplicate UK-AIR ID"):
        parse_catalogue_csv(duplicate)


def test_property_support_uses_only_exact_supported_year_filename_families() -> None:
    html = """
      <a href="/datastore/data_files/site_pol_data/SHUN_BC_2019.csv">old BC</a>
      <a href="/datastore/data_files/site_pol_data/SHUN_BC_2020.csv">BC</a>
      <a href="/datastore/data_files/site_pol_data/SHUN_U_Violet_2024.csv">UV</a>
      <a href="/datastore/data_files/site_pol_data/SHUN_UVPM_2024.csv">UVPM</a>
      <a href="/datastore/data_files/site_pol_data/SHUN_AE33_2024.csv">AE33</a>
    """
    supported, files = parse_property_support_html(html, "SHUN")
    assert supported == frozenset({"bc", "uv370"})
    assert files == {
        "bc": ("SHUN_BC_2020.csv",),
        "uv370": ("SHUN_U_Violet_2024.csv",),
    }


def test_existing_good_site_ref_is_preserved_and_conflicts_fail_closed() -> None:
    bridge = {
        "site_ref": "SHUN",
        "site_ref_source_url": "https://example.invalid/bridge",
        "site_ref_source_checked_at": "2026-09-20T00:00:00Z",
    }
    chosen = choose_existing_site_ref("UKA01055", bridge, None)
    assert chosen and chosen.site_ref == "SHUN"
    with pytest.raises(RuntimeError, match="Contradictory authoritative"):
        choose_existing_site_ref("UKA01055", bridge, {"site_ref": "OTHER"})


def test_canonical_station_and_timeseries_identity_do_not_use_short_code() -> None:
    station = CatalogueStation(
        uk_air_ref="UKA01055",
        site_name="Shrewsbury Underdale",
        environment_type="Urban Background",
        zone="West Midlands",
        start_date=date(2019, 1, 1),
        end_date=None,
        latitude=52.7,
        longitude=-2.7,
        description="Description",
        raw={},
    )
    planned = PlannedStation(
        station,
        SiteRefEvidence("SHUN", "https://example.invalid", "2026-09-23T00:00:00Z", "test"),
        frozenset({"bc", "uv370"}),
        {},
    )
    station_row = build_station_row(
        planned, connector_id=42, network_id=12, snapshot_at="2026-09-23T00:00:00Z"
    )
    rows = build_timeseries_rows(
        [planned],
        connector_id=42,
        station_ids={"UKA01055": 99},
        phenomenon_ids={"bc": 101, "uv370": 102},
        property_ids={"bc": 201, "uv370": 202},
    )
    assert station_row["station_ref"] == "UKA01055"
    assert {row["timeseries_ref"] for row in rows} == {"UKA01055:bc", "UKA01055:uv370"}
    assert all("SHUN" not in row["timeseries_ref"] for row in rows)
    assert timeseries_ref("uka01055", "bc") == "UKA01055:bc"


def test_phenomena_payload_uses_central_mapping_contract() -> None:
    rows = build_phenomena_rows(42)
    assert {row["source_label"] for row in rows} == {
        "Black Carbon (880nm) ug/m-3",
        "UV Particulate Matter (370nm) ug/m-3",
    }
    assert {row["observed_property_code"] for row in rows} == set(PROPERTY_CONFIG)
    assert all(row["mapping_kind"] == "raw_observed_property" for row in rows)
    assert all(row["is_aqi_eligible"] is False for row in rows)
    assert all(row["source_uom"] == "ug/m3" for row in rows)


def test_reference_refresh_has_no_observation_write_path() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "scripts/ukair_bc/ukair_bc_reference_refresh.py"
    ).read_text()
    assert 'table("observations")' not in source
    assert "uk_aq_rpc_observations" not in source
