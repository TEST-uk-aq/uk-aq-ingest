# Ingest Instructions

This doc covers the manual steps to run the ingest scripts in this repo.

## Prereqs
- Use `python3`.
- Create and activate a virtual environment, then install dependencies:
```
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
```
- Provide Supabase credentials via environment variables (a `.env` in repo root is loaded):
  - `SUPABASE_URL`
  - `SB_SECRET_KEY`

## Sensor.Community (SCOMM)
1) (Optional) Set Sensor.Community env vars:
   - `SCOMM_USER_AGENT` (recommended)
   - `SCOMM_COUNTRY` (defaults to `GB`)
   - `SCOMM_BASE_URL` (defaults to `https://data.sensor.community`)
   - `SCOMM_INGEST_MET_FIELDS` (defaults to `false`; set `true` to ingest temperature/humidity/pressure)
2) Run the recent refresh:
```
python3 scripts/sensorcommunity/sensorcommunity_ingest.py --refresh-recent
```
3) (Optional) Save raw API output:
```
python3 scripts/sensorcommunity/sensorcommunity_ingest.py --refresh-recent --raw-output sensorcommunity_raw.json
```

## UK-AIR SOS
1) (Optional) Set UK-AIR SOS env vars:
   - `SOS_BASE_URL` (defaults to `https://uk-air.defra.gov.uk/sos-ukair/api/v1`)
   - `SOS_SERVICE_LABEL` (defaults to `SOS`)
2) Discover services/stations/timeseries and backfill a year:
```
python3 scripts/sos/sos_ingest.py --discover --backfill-2025
```
3) Refresh the last N hours (default 6):
```
python3 scripts/sos/sos_ingest.py --refresh-recent --hours 6
```
4) (Optional) Use filters like `--region`, `--station-like`, or `--pollutants`. See:
```
python3 scripts/sos/sos_ingest.py --help
```

## Troubleshooting
- `ModuleNotFoundError: No module named 'requests'` means dependencies were not installed; run the `pip install` step above.
