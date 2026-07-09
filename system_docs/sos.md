# UK-AIR SOS Network

UK-AIR SOS phenomena are written through the central
`uk_aq_public.uk_aq_rpc_phenomena_upsert` function by both discovery paths.
Verified Eionet source URIs are seeded as authoritative mappings. A new URI
fails closed as unknown until reviewed, preventing silent canonical
misclassification.

This network pulls stations from the UK-AIR SOS API with configurable filters.

## Source
- UK-AIR SOS REST API
- Base URL: `https://uk-air.defra.gov.uk/sos-ukair/api/v1`

## Filters
Applied in `scripts/sos/sos_ingest.py`:
- Bounding box: default is UK bbox (west -11.0, south 49.0, east 2.0, north 61.0)
- Region label: optional
- Station type: optional (e.g., `AURN`)
- Pollutants: default `NO2`, `O3`, `PM10`, `PM2.5` (use `--all-pollutants` to disable filtering)
  - `--strict-bbox` excludes stations with missing coordinates
  - Pollutant matching is tolerant (aliases like NO2/Nitrogen Dioxide, PM2.5/PM25).

## Ingestion flow
1) Discover service metadata (`/services`).
2) Fetch stations (`/stations`) and apply filters.
3) Fetch timeseries (`/timeseries?expanded=true`) and filter to target pollutants (if set).
4) Backfill 2025 observations (`/timeseries/{id}/getData?timespan=2025-01-01/2026-01-01`).
5) Refresh recent observations for the last N hours (default 6h).

## Destination tables
- `connectors`
- `stations`
- `station_metadata`
- `networks` (referenced by `stations.network_id`)
- `timeseries`
- `observations`
- `phenomena`
- `procedures`
- `offerings`
- `categories`
- `features`

## Connector creation
- Connector rows are created by the stations sync; ingests expect the connector to exist and do not create it.

## Station metadata and network assignment
- `station_metadata` is populated by the station listing script.
- SOS stations use one canonical `stations.network_id` referencing `networks.id`.
- `sos_station_uk_air_refs` stores the resolved UK-AIR site id for each SOS station to join against the site register.
- `stations.station_type` is backfilled with the primary network code (single network or AURN-priority).
- Public network code and label values come from the canonical `networks` row.
- Validate assignments with
  `python3 scripts/sos/sos_network_assignment_report.py --output <path.csv>`.
  The report checks `stations.network_id -> networks.id` and keeps connector
  provenance separate; it does not populate or repair retired membership data.

## Site register
- `scripts/sos/sos_site_register.py` downloads the UK-AIR monitoring sites CSV.
- The CSV network fields remain source metadata; they do not define the public station contract.
- Use `--load` to load the CSV into `sos_site_register` and `sos_networks` in the same run.
- Use `--load-only` with `--csv-path` to load a local CSV without downloading.
- The load step can discover DEFRA flat-file `site_ref` values from official UK-AIR `site-info` pages for archive backfill.
- `network_info/sos/sos_site_refs.csv` is a seed/override map for refs that need explicit control.
- The monthly register load refreshes `sos_station_uk_air_refs` first, then `sos_station_timeseries_site_refs` after the register snapshot is written. It maps through the authoritative `uk_air_ref` station link and exact canonical pollutant code.
- Ambiguous active mappings fail the monthly workflow. Unmapped AURN sites remain unmapped and are reported in the workflow log.
- The monthly workflow validates mapped and discovered refs against official UK-AIR site-info and flat-file pages before loading.
- The load step keeps existing `sos_networks.network_display_name` values and seeds `sos_network_pollutants`.

## Station pollutant coverage
- Station-to-pollutant coverage is derived from `timeseries` (via `timeseries.phenomenon_id`).
- `stations` does not store a single pollutant because stations often monitor multiple pollutants.
- UK-AIR SOS exposes a separate station id per pollutant. The numeric value that appears between the
  Eionet pollutant URI and the label (e.g., `.../pollutant/8 794 - Bristol St Paul's-Nitrogen dioxide`)
  is the SOS `station_ref` for that pollutant-specific station.
- Label format: `<eionet pollutant URI> <station_ref> - <station label>`

## IDs
- `connectors.id` and `timeseries.id` use integer internally; `stations.id` remains bigint. Upstream identifiers are stored in `service_ref`, `station_ref`, and `timeseries_ref`.
- Any upstream identifier that arrives as text (even if numeric) uses a `*_ref` column; internal joins use `*_id` columns.
- `observations` references `timeseries.id`.

## Commands
```
python3 scripts/sos/sos_ingest.py --discover --backfill-2025
python3 scripts/sos/sos_ingest.py --refresh-recent --hours 6

# Example: AURN stations in Bristol only
python3 scripts/sos/sos_ingest.py --station-type AURN --region Bristol --bbox -2.75,51.30,-2.45,51.55 --discover
```

## Edge function polling
The Edge Function `ingest_sos` polls recent observations using the existing `timeseries` rows.
It does not update `stations.station_name` (station metadata comes from the ingest/list scripts).
`sos_timeseries_checkpoints` tracks the last poll attempt per timeseries so the dispatcher can rotate batches.
Both edge and Cloud Run polling paths only select active rows (`timeseries.ended_at is null`).
Pollutant filters now match canonical observed-property codes/display names (via `phenomena.observed_property_id -> observed_properties`) with fallback to legacy `notation`/`label`/`source_label`.

## Timeseries lifecycle reconciliation
- Daily full-catalog UK-AIR discovery (`scripts/sos/sos_ingest.py --discover` when timeseries are not station-scoped) is the source of truth for timeseries lifecycle.
- `timeseries.last_catalog_seen_at` stores the last discovery run where the source `timeseries_ref` was present.
- `timeseries.catalog_missing_runs` increments when an active timeseries is absent from a full-catalog discovery.
- `timeseries.ended_at` is set after 2 consecutive missing runs.
- If a previously ended `timeseries_ref` reappears in discovery, it is automatically reactivated (`ended_at` cleared, `catalog_missing_runs` reset to `0`).

## Cloud Run polling
- Cloud Run worker: `workers/uk_aq_sos_cloud_run/run_job.ts`
- Triggered when `connectors.scheduler_backend='google_cloud_run'`.
- Uses station-level checkpointing:
  - selector RPC: `uk_aq_core.sos_select_station_refs(batch_limit, stale_limit)`
  - table: `uk_aq_raw.sos_station_checkpoints`
- Flow: select due stations -> resolve scoped timeseries ids -> call `ingest_sos` once with `timeseries_ids`.
- Edge behavior stays unchanged; the edge path still uses `sos_timeseries_checkpoints`.

Environment variables (Supabase secrets):
- `SB_SUPABASE_URL`
- `SB_SECRET_KEY`
- `SOS_BASE_URL` (optional override)
- `SOS_SERVICE_LABEL` (optional override)
- `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN` (optional; enables log upload)
- `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (optional; must match `SB_SUPABASE_URL` to enable log upload)

For local runs, keep the `SUPABASE_*` values in `.env` (gitignored). For Edge Functions, use `SB_*` secrets instead. `SUPABASE_ACCESS_TOKEN` is only needed for deployment, not for runtime polling.

Env quick reference (Supabase blocks secrets prefixed with `SUPABASE_`):

| Context | Required | Optional |
| --- | --- | --- |
| Local scripts (.env) | `SUPABASE_URL`, `SB_SECRET_KEY` | `SOS_BASE_URL`, `SOS_SERVICE_LABEL` |
| Edge function runtime (Supabase secrets) | `SB_SUPABASE_URL`, `SB_SECRET_KEY` | `SOS_BASE_URL`, `SOS_SERVICE_LABEL` |
| GitHub Actions deploy | `SUPABASE_ACCESS_TOKEN`, `SUPABASE_URL`, `SB_SECRET_KEY`, `SUPABASE_PROJECT_REF` (Secrets) | `SOS_BASE_URL`, `SOS_SERVICE_LABEL` (Secrets) |

Request body options (JSON):
- `connector_id` (optional; defaults to the `sos` connector)
- `window_hours` (optional; defaults to `connectors.poll_window_hours` or 6)
- `pollutants` (optional; array or comma-separated list)
- `timeseries_ids` (optional; array or comma-separated list of internal `timeseries.id` values only)
- `timeseries_limit` (optional; integer)

When `connector_id` is provided, the function uses `connectors.service_url` from the database.
Environment variables are only a fallback for discovery or missing connector rows.

If `timeseries_limit` is not provided, the function uses `connectors.poll_timeseries_batch_size` when set.

Helper RPC SQL lives in `supabase/uk_aq_polling_helpers.sql` and is used by the dispatcher.
The default schedule runs 5 minutes past each quarter-hour (:05, :20, :35, :50) to align with on-the-hour measurements.

Dropbox log output (optional):
- When Dropbox credentials and the allowlist URL are set, each run uploads a log file to `/log/YYYY-MM-DD` in the Dropbox app root.
- Logs older than 31 days are zipped into `/log/archive/YYYY-MM-DD.zip`; archive files older than 1 year are removed.
- When the allowlist URL matches the test Supabase project, the Edge Function also uploads a zipped raw payload capture to `/raw_data/YYYY-MM-DD`.
