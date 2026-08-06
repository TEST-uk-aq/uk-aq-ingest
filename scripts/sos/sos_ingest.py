#!/usr/bin/env python3
"""Load the SOS ingest implementation with guarded station-identity corrections."""

from __future__ import annotations

import os
import runpy
import tempfile
from pathlib import Path
from typing import Any, Dict


REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_SOURCE = Path(__file__).with_name("sos_ingest_legacy.py")
PATCH_SOURCE = REPO_ROOT / "scripts/dev/_apply_sos_station_identity_fix.py"
PATCH_ANCHOR_FIX = REPO_ROOT / "scripts/dev/_fix_sos_patch_anchor.py"


def _load_patched_source() -> str:
    missing = [
        str(path)
        for path in (LEGACY_SOURCE, PATCH_SOURCE, PATCH_ANCHOR_FIX)
        if not path.is_file()
    ]
    if missing:
        raise RuntimeError(
            "SOS station identity loader is missing required file(s): "
            + ", ".join(missing)
        )

    with tempfile.TemporaryDirectory(prefix="uk_aq_sos_identity_") as temp_name:
        temp_root = Path(temp_name)
        temp_sos = temp_root / "scripts/sos"
        temp_dev = temp_root / "scripts/dev"
        temp_tests = temp_root / "tests"
        temp_sos.mkdir(parents=True)
        temp_dev.mkdir(parents=True)
        temp_tests.mkdir(parents=True)

        target_source = temp_sos / "sos_ingest.py"
        target_patch = temp_dev / PATCH_SOURCE.name
        target_anchor = temp_dev / PATCH_ANCHOR_FIX.name

        target_source.write_bytes(LEGACY_SOURCE.read_bytes())
        target_patch.write_bytes(PATCH_SOURCE.read_bytes())
        target_anchor.write_bytes(PATCH_ANCHOR_FIX.read_bytes())

        original_cwd = Path.cwd()
        try:
            os.chdir(temp_root)
            runpy.run_path(str(target_anchor), run_name="__uk_aq_sos_patch_anchor__")
            runpy.run_path(str(target_patch), run_name="__uk_aq_sos_patch__")
        finally:
            os.chdir(original_cwd)

        patched_source = target_source.read_text(encoding="utf-8")

    required_fragments = (
        "get_timeseries_station_id_map",
        "SOS timeseries station ownership change refused",
        "catalogue_station_refs=station_refs",
    )
    missing_fragments = [
        fragment for fragment in required_fragments if fragment not in patched_source
    ]
    if missing_fragments:
        raise RuntimeError(
            "SOS station identity patch did not produce required safeguards: "
            + ", ".join(missing_fragments)
        )

    discover_start = patched_source.index("    def discover_timeseries(")
    discover_end = patched_source.index("    def backfill_year(", discover_start)
    discover_source = patched_source[discover_start:discover_end]
    forbidden_fragments = (
        "Created station row",
        "_extract_station_ref_from_label(ts.get",
        "created_rows",
    )
    present_forbidden = [
        fragment for fragment in forbidden_fragments if fragment in discover_source
    ]
    if present_forbidden:
        raise RuntimeError(
            "SOS station identity patch left forbidden fallback logic in place: "
            + ", ".join(present_forbidden)
        )

    return patched_source


def _execute_patched_module() -> None:
    patched_source = _load_patched_source()
    module_globals: Dict[str, Any] = globals()
    module_globals["__file__"] = str(Path(__file__).resolve())
    module_globals["__cached__"] = None
    exec(
        compile(patched_source, str(Path(__file__).resolve()), "exec"),
        module_globals,
        module_globals,
    )


_execute_patched_module()
