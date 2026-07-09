from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WRITERS = (
    "scripts/blondon_communities/blondon_communities_list_stations.py",
    "scripts/openaq/openaq_list_stations.py",
    "scripts/sensorcommunity/sensorcommunity_ingest.py",
    "scripts/erg_laqn/erg_laqn_ingest.py",
    "scripts/sos/sos_ingest.py",
    "scripts/sos/sos_list_stations.py",
    "scripts/blondon_nodes/blondon_nodes_ingest.py",
)


def test_active_ingests_do_not_directly_upsert_phenomena() -> None:
    for relative_path in WRITERS:
        source = (ROOT / relative_path).read_text()
        assert 'table("phenomena").upsert' not in source
        assert "insert into uk_aq_core.phenomena" not in source.lower()


def test_active_ingests_use_central_phenomena_rpc() -> None:
    helper_users = WRITERS[:1] + WRITERS[2:]
    for relative_path in helper_users:
        source = (ROOT / relative_path).read_text()
        assert "upsert_phenomena_via_rpc" in source
    assert "uk_aq_rpc_phenomena_upsert" in (ROOT / WRITERS[1]).read_text()
