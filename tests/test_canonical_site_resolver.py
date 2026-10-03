from scripts.official_networks.canonical_site_resolver import (
    choose_regional_match,
    coordinates_identical,
    merge_member_evidence,
)


def _site(**overrides):
    row = {
        "uk_air_ref": "UKA00217",
        "site_ref": "CARD",
        "site_name": "Cardiff Centre",
        "latitude": 51.48178,
        "longitude": -3.17625,
        "networks": ["Automatic Urban and Rural Monitoring Network (AURN)"],
    }
    row.update(overrides)
    return row


def _station(**overrides):
    row = {
        "id": 100,
        "station_ref": "CARD",
        "latitude": 51.48178,
        "longitude": -3.17625,
    }
    row.update(overrides)
    return row


def test_exact_code_uses_the_unique_aurn_register_row():
    decision = choose_regional_match(_station(), [_site()])

    assert decision.status == "matched"
    assert decision.uk_air_ref == "UKA00217"
    assert decision.method == "regional_exact_site_ref"


def test_different_code_accepts_identical_official_coordinates():
    decision = choose_regional_match(
        _station(station_ref="NPT1"),
        [_site(uk_air_ref="UKA00380", site_ref="NPT3")],
    )

    assert coordinates_identical(_station(), _site())
    assert decision.status == "matched"
    assert decision.method == "regional_unique_50m_identical_coordinates"


def test_different_code_requires_stronger_official_evidence():
    decision = choose_regional_match(
        _station(station_ref="ARM5", latitude=54.353744, longitude=-6.654532),
        [_site(
            uk_air_ref="UKA00541",
            site_ref="ARM6",
            site_name="Armagh Roadside",
            latitude=54.353728,
            longitude=-6.654558,
        )],
        official_evidence=lambda _station, _site: {
            "accepted": True,
            "kind": "regional_page_aurn_counterpart",
            "source_url": "https://www.airqualityni.co.uk/site/ARM5",
        },
    )

    assert decision.status == "matched"
    assert decision.method == "regional_unique_50m_official_evidence"
    assert decision.evidence["kind"] == "regional_page_aurn_counterpart"


def test_ambiguous_nearby_aurn_candidates_fail_closed():
    decision = choose_regional_match(
        _station(station_ref="OTHER"),
        [
            _site(uk_air_ref="UKA00001", site_ref="ONE", latitude=51.48179),
            _site(uk_air_ref="UKA00002", site_ref="TWO", latitude=51.48180),
        ],
    )

    assert decision.status == "ambiguous"
    assert decision.uk_air_ref is None


def test_closed_aurn_site_is_not_a_different_code_candidate():
    decision = choose_regional_match(
        _station(station_ref="OTHER"),
        [_site(site_ref="OLD", _current_at_snapshot=False)],
    )

    assert decision.status == "unmatched"
    assert decision.evidence["reason"] == "no_candidate_within_50m"


def test_refresh_retains_previous_accepted_alias_evidence():
    evidence = merge_member_evidence(
        {"member_evidence": [{"station_id": 8, "method": "accepted_alias"}]},
        [{"station_id": 1, "method": "sos_bridge"}],
    )

    assert evidence == [
        {"station_id": 1, "method": "sos_bridge"},
        {"station_id": 8, "method": "accepted_alias"},
    ]
