from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EDGE_DOC = (ROOT / "system_docs/uk_aq_edge_functions.md").read_text()
SOS_DOC = (ROOT / "system_docs/sos.md").read_text()


def test_edge_docs_define_v2_network_contract() -> None:
    for marker in (
        "/api/aq/networks",
        "contract_version: 2",
        "network_id",
        "network_code",
        "network_label",
        "network_type",
        "HTTP 400",
        "breathelondon",
    ):
        assert marker in EDGE_DOC


def test_sos_docs_use_canonical_assignment_report() -> None:
    assert "stations.network_id -> networks.id" in SOS_DOC
    assert "sos_network_assignment_report.py" in SOS_DOC
