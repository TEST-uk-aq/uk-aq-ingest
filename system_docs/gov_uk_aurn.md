# Gov UK AURN

Authoritative AURN membership derived from the UK-AIR monitoring sites register.

## Status
- UK-AIR "Search for monitoring sites" CSV is the authoritative source for membership.
- SOS station listings do not expose `stationType`, so AURN membership must come from the UK-AIR CSV.

## Register download
- Use `scripts/sos/sos_site_register.py` to fetch the CSV from the search results page.
- The CSV includes network membership for each site; filter to AURN for derived reports.
- Use `--dropbox-upload` to store the CSV under `{UK_AQ_DROPBOX_ROOT}/network_info/sos`.
- Output files are timestamped (e.g., `sos_site_register_YYYYMMDDTHHMMSSZ.csv`).
