#!/usr/bin/env python3
"""Read-only manual LAQN entry point. Writes use the claimed Cloud Run job."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if __name__ == "__main__":
    from scripts.official_networks.official_network_ingest import main

    raise SystemExit(main("erg_laqn", force_dry_run=True))
