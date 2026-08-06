import ast
from pathlib import Path


def test_sos_identity_loader_is_structurally_safe():
    source_path = Path("scripts/sos/sos_ingest.py")
    legacy_path = Path("scripts/sos/sos_ingest_legacy.py")
    loader = source_path.read_text(encoding="utf-8")

    ast.parse(loader, filename=str(source_path))
    assert legacy_path.is_file()
    assert "SOS timeseries station ownership change refused" in loader
    assert "_extract_station_ref_from_label(ts.get" in loader
    assert "forbidden_fragments" in loader
    assert "_load_patched_source" in loader
