# SOS station identity protection

The active `scripts/sos/sos_ingest.py` entry point loads the archived legacy implementation through a guarded transformation. The transformation removes interpretation of timeseries label numbers and feature identifiers as station catalogue references, removes synthetic station creation, preserves established timeseries ownership when discovery is unresolved, and rejects attempted station re-parenting.

The unchanged pre-fix implementation is retained in `scripts/sos/sos_ingest_legacy.py`. The loader validates both required safeguards and absence of the forbidden discovery fallbacks before executing the transformed source.
