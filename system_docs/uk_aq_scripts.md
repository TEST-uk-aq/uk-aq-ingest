# UK-AQ Scripts

This document summarizes the UK-AQ helper scripts and their inputs/outputs.

## Environment

**Supabase**
- `SUPABASE_URL`
- `SB_SECRET_KEY`

**UK-AIR SOS**
- `UK_AIR_SOS_BASE_URL` (optional; defaults to `https://uk-air.defra.gov.uk/sos-ukair/api/v1`)
  - The scripts also accept the legacy `UK_AIR_BASE_URL` or `UKAIR_BASE_URL` if set.
- `UK_AIR_SOS_SERVICE_LABEL` (optional; defaults to `UK-AIR-SOS`)

**Sensor.Community**
- `SCOMM_BASE_URL` (optional; defaults to `https://data.sensor.community`)
- `SCOMM_CONNECTOR_CODE` (optional; defaults to `sensorcommunity`; legacy `SCOMM_CONNECTOR_REF` supported)
- `SCOMM_SERVICE_REF` (optional; defaults to `SCOMM_CONNECTOR_CODE`)

**OpenAQ**
- `SCOMM_SERVICE_LABEL` (optional; defaults to `Sensor.Community`; legacy `SCOMM_CONNECTOR_LABEL` supported)
- `SCOMM_COUNTRY` (optional; defaults to `GB`)
- `SCOMM_USER_AGENT` (optional; identifies your client when polling Sensor.Community)
- `SCOMM_INGEST_MET_FIELDS` (optional; defaults to `false`; enable temperature/humidity/pressure ingestion)
- `SCOMM_LOG_LEVEL` (optional; defaults to `INFO`)
- `OPENAQ_BASE_URL` (optional; defaults to `https://api.openaq.org/v3`)
- `OPENAQ_API_KEY` (required; OpenAQ API key)
- `OPENAQ_CONNECTOR_CODE` (optional; defaults to `openaq`)
- `OPENAQ_SERVICE_REF` (optional; defaults to `OPENAQ_CONNECTOR_CODE`)
- `OPENAQ_SERVICE_LABEL` (optional; defaults to `OpenAQ`)
- `OPENAQ_USER_AGENT` (optional; defaults to `uk-air-quality-networks`)
- `OPENAQ_BBOX` (optional; defaults to `-8.623555,49.863222,1.763337,60.871222`)
- `OPENAQ_PAGE_LIMIT` (optional; defaults to `1000`)
- `OPENAQ_MAX_PAGES` (optional; defaults to `0` meaning no cap)
- `OPENAQ_LOG_LEVEL` (optional; defaults to `INFO`)

## Scripts

### `scripts/uk_aq_supabase.py`
Purpose:
- Central helper for Supabase clients that target `uk_aq_core`, `uk_aq_raw`, and `uk_aq_pop`.
- Provides `create_supabase_client` plus `SupabaseSchemas` / `SchemaClient` wrappers for schema-specific `.table()` and `.rpc()` calls.

### `scripts/uk_aq_phenomena_rpc.py`
Purpose:
- Shared fail-closed client contract for
  `uk_aq_public.uk_aq_rpc_phenomena_upsert`.
- Normalizes connector payloads, rejects duplicate source labels, validates
  that every source receives a phenomenon ID, and raises on unknown mapping
  warnings.
- Supports explicit administrative mapping registration only when the caller
  opts in.

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY` (or `SUPABASE_KEY` fallback)
- `UK_AQ_CORE_SCHEMA` (optional; defaults to `uk_aq_core`)
- `UK_AQ_RAW_SCHEMA` (optional; defaults to `uk_aq_raw`)
- `UK_AQ_POP_SCHEMA` (optional; defaults to `uk_aq_pop`)

### `scripts/uk_aq_inject_project_ref.mjs`
Purpose:
- Replace Supabase placeholders in web assets during GitHub Actions deploys.

Placeholders:
- `__SUPABASE_PROJECT_REF__` or `{{SUPABASE_PROJECT_REF}}`
- `__SB_PUBLISHABLE_DEFAULT_KEY__` or `{{SB_PUBLISHABLE_DEFAULT_KEY}}`

Notes:
- If no placeholders are found, the script exits without changes.
- Optional: `UK_AQ_INJECT_PATHS` (comma-separated file paths) to limit which files are scanned.

Environment:
- `SUPABASE_PROJECT_REF`
- `SB_PUBLISHABLE_DEFAULT_KEY`

### `scripts/uk_aq_check_env.sh`
Purpose:
- Run one-pass validation for local Supabase env variables used by Ingest + History.
- Check variable presence, project-ref alignment, masked secret previews, JWT-formatted key claims, and optional live HTTP checks.

Common commands:
```
./scripts/uk_aq_check_env.sh
./scripts/uk_aq_check_env.sh --no-network
./scripts/uk_aq_check_env.sh --env-file .env.supabase
```

Notes:
- Exit code `0` = pass (warnings allowed); exit code `1` = one or more failures.
- Network mode validates:
  - `SUPABASE_ACCESS_TOKEN` against Supabase Management API.
  - Main/history REST root access with `SB_PUBLISHABLE_DEFAULT_KEY`, main privileged key (`SB_SECRET_KEY` preferred), and `OBS_AQIDB_SECRET_KEY`.
- Secret values are masked in output.

### `scripts/uk_aq_sync_github_secrets.sh`
Purpose:
- Sync local env files to GitHub Actions secrets/variables.
- Route each key using `config/uk_aq_github_env_targets.csv` (`secret`, `variable`, `both`, or `local`).
- Upload non-local keys from `.env.supabase` into the `SUPABASE_SECRETS_ENV` GitHub secret for edge deploy.

Common commands:
```bash
scripts/uk_aq_sync_github_secrets.sh --dry-run
scripts/uk_aq_sync_github_secrets.sh --repo owner/repo
scripts/uk_aq_sync_github_secrets.sh --targets-file config/uk_aq_github_env_targets.csv
```

Notes:
- Unmapped keys default to `local` (not synced to GitHub).
- `GCP_SA_KEY` uploads file contents when the value points to a local path.
- `SUPABASE_DB_URL` is normalized to avoid accidental double-encoding before sync.
- `--dry-run` prints key names and value lengths (not raw values).
- `SUPABASE_SECRETS_ENV` includes only non-local keys from the Supabase env file.
- Any `SUPABASE_SECRETS_ENV=...` line in env files is ignored; the value is always rebuilt by the script.
- Keep `config/uk_aq_github_env_targets.csv` aligned with workflow `vars.*` / `secrets.*` references.

### `scripts/uk_aq_run_ingestdb_prune.sh`
Purpose:
- Invoke `uk-aq-ingestdb-prune-service` with window controls (`retentionDays`, `maxHours`).
- Support local user-friendly auth via `gcloud run services proxy` (default).
- Optionally call with service-account identity tokens via impersonation.

Common commands:
```bash
scripts/uk_aq_run_ingestdb_prune.sh --dry-run --start-date 2026-02-10 --max-hours 48
scripts/uk_aq_run_ingestdb_prune.sh --dry-run --retention-days 9 --max-hours 48
scripts/uk_aq_run_ingestdb_prune.sh --live --window-start 2026-02-10 --window-end 2026-02-12
scripts/uk_aq_run_ingestdb_prune.sh --auth-mode impersonate \
  --impersonate-service-account uk-aq-ops-job@astute-lyceum-484111-k5.iam.gserviceaccount.com
```

Notes:
- Defaults:
  - `--project`: `GCP_PROJECT_ID` or `astute-lyceum-484111-k5`
  - `--region`: `GCP_REGION` or `europe-west2`
  - `--service`: `uk-aq-ingestdb-prune-service`
  - `--auth-mode`: `proxy`
- `--proxy-timeout-seconds` controls proxy readiness wait (default `60`).
- `--window-start/--window-end` computes `retentionDays` + `maxHours` automatically (UTC), with `window-end` treated as inclusive.
- `--start-date` + `--max-hours` computes `retentionDays` automatically.
- With current prune API, start-date mode requires `start-date + max-hours` to land on `00:00 UTC` (for example 24/48/72 hours).
- `proxy` mode avoids the common user-account `print-identity-token --audiences` error.

### `scripts/uk_aq_int4_migration_all_clear.sh`
Purpose:
- Run post-migration all-clear checks for the connector/timeseries ID `int4` migration on MAIN and HISTORY DBs.
- Validate target column types, FK type parity, key RPC signatures, and basic smoke queries.

Common commands:
```bash
scripts/uk_aq_int4_migration_all_clear.sh
scripts/uk_aq_int4_migration_all_clear.sh --main-only
scripts/uk_aq_int4_migration_all_clear.sh --history-only --history-db-url "$OBS_AQIDB_SUPABASE_DB_URL"
scripts/uk_aq_int4_migration_all_clear.sh --env-file .env
```

Notes:
- The script defaults to `.env` in the ingest repo and auto-loads DB URLs from env if flags are not passed.
- It sets `PGOPTIONS` to disable statement/lock/idle transaction timeouts when not already set.
- `--main-only` and `--history-only` allow targeted validation runs.

Environment:
- MAIN DB URL: `SUPABASE_DB_URL` (or `--main-db-url`)
- HISTORY DB URL: `OBS_AQIDB_SUPABASE_DB_URL` or `SBASE_HISTORY_DB_URL` (or `--history-db-url`)

### `scripts/gcp/uk_aq_secret_upsert_if_changed.sh`
Purpose:
- Upsert one GCP Secret Manager secret from stdin.
- Compare against the latest enabled secret version and avoid creating a new version when unchanged.
- When changed, create a new version and destroy older active versions so one active version remains.
- Detect Cloud Run secret refs pinned to numeric versions and update them to `latest` before cleanup (apply mode).

Common commands:
```bash
printf '%s' "$SB_SECRET_KEY" | \
  scripts/gcp/uk_aq_secret_upsert_if_changed.sh \
    --project "$GCP_PROJECT_ID" \
    --region "$GCP_REGION" \
    --secret "SB_SECRET_KEY" \
    --required 1 \
    --apply

printf '%s' "$OPENAQ_API_KEY" | \
  scripts/gcp/uk_aq_secret_upsert_if_changed.sh \
    --project "$GCP_PROJECT_ID" \
    --region "$GCP_REGION" \
    --secret "OPENAQ_API_KEY" \
    --required 1
```

Notes:
- Default mode is dry-run; use `--apply` for real changes.
- Secret payload values are never printed.
- Reports, per secret: changed/unchanged, whether a new version will be created, old versions planned for destroy, and active-version count after completion.
- Apply mode fails safe if the current enabled version cannot be accessed for comparison.

### `scripts/gcp/uk_aq_cleanup_secret_versions.sh`
Purpose:
- Cleanup utility to keep exactly one active Secret Manager version per secret.
- Dry-run by default, with optional apply mode.
- Checks Cloud Run secret references and skips pinned numeric-version cases unless pin-fix is explicitly enabled.

Common commands:
```bash
scripts/gcp/uk_aq_cleanup_secret_versions.sh \
  --project "$GCP_PROJECT_ID" \
  --region "$GCP_REGION"

scripts/gcp/uk_aq_cleanup_secret_versions.sh \
  --project "$GCP_PROJECT_ID" \
  --region "$GCP_REGION" \
  --fix-cloud-run-pins 1 \
  --apply
```

Notes:
- Works across all secrets by default, or one secret at a time via repeated `--secret`.
- In apply mode, destroys older active versions and verifies one active version remains.
- Destroyed versions cannot be restored from GCP.

### `scripts/gcp_billing_export_check.sh`
Purpose:
- Check whether Cloud Billing export to BigQuery is enabled and producing billing export tables.
- Confirm whether export table schema includes a `labels` field for label-based cost analysis.

Common commands:
```bash
BILLING_EXPORT_PROJECT=my-billing-proj BILLING_EXPORT_DATASET=billing_export \
  ./scripts/gcp_billing_export_check.sh

./scripts/gcp_billing_export_check.sh --project my-billing-proj --dataset billing_export
```

Notes:
- Reports `PASS` only when billing export tables are present.
- If a dataset exists but no export tables are present yet, reports `FAIL` with a startup-delay warning.

### `../CIC-test-uk-aq-Operations/CIC-test-uk-aq-ops/scripts/backup_r2/uk_aq_build_r2_history_index.mjs`
Purpose:
- Rebuild the derived R2 history index manifests used by the history-days API fast path:
  - `history/_index/observations_latest.json`
  - `history/_index/aqilevels_latest.json`
- Read committed top-level day manifests only; does not scan Parquet row data.
- Preserve per-day connector row counts inside the derived index files for future dashboard/report use.

Common commands:
```bash
node ../CIC-test-uk-aq-Operations/CIC-test-uk-aq-ops/scripts/backup_r2/uk_aq_build_r2_history_index.mjs
node ../CIC-test-uk-aq-Operations/CIC-test-uk-aq-ops/scripts/backup_r2/uk_aq_build_r2_history_index.mjs --domain observations
```

Notes:
- This script lives in the ops repo, not the ingest repo.
- It is called automatically after successful non-dry local monthly `source_to_r2` runs.
- The daily ingestdb prune service also rebuilds the same index files after successful non-dry Phase B history export.

### `../CIC-test-uk-aq-Operations/CIC-test-uk-aq-ops/scripts/backup_r2/uk_aq_cleanup_sos_empty_mirror_files.mjs`
Purpose:
- Clean up legacy UK-AIR SOS mirror files that only contain exact empty payloads such as `{"values":[]}`.
- Migrate those known-empty timeseries/day combinations into the newer per-day `_no_data_timeseries.json` manifest format.

Common commands:
```bash
node ../CIC-test-uk-aq-Operations/CIC-test-uk-aq-ops/scripts/backup_r2/uk_aq_cleanup_sos_empty_mirror_files.mjs
node ../CIC-test-uk-aq-Operations/CIC-test-uk-aq-ops/scripts/backup_r2/uk_aq_cleanup_sos_empty_mirror_files.mjs --apply
node ../CIC-test-uk-aq-Operations/CIC-test-uk-aq-ops/scripts/backup_r2/uk_aq_cleanup_sos_empty_mirror_files.mjs --apply --day 2025-01-15
```

Notes:
- Dry-run is the default; `--apply` is required to write manifests and delete old empty files.
- The script reads `UK_AQ_BACKFILL_SOS_RAW_MIRROR_ROOT` unless `--root` is passed.
- Non-empty SOS mirror files are left untouched.
- Console path to enable export: `Billing -> Billing export -> BigQuery export`.

### `scripts/uk_aq_export_connectors_snapshot.py`
Purpose:
- Export connector polling settings and station/timeseries counts to a CSV for spreadsheet review.

Common commands:
```
python3 scripts/uk_aq_export_connectors_snapshot.py
python3 scripts/uk_aq_export_connectors_snapshot.py --output network_info/uk_aq/uk_aq_connectors_snapshot.csv
```

Notes:
- Output includes `hours_since_*` fields derived from connector `last_polled_at` / `last_run_end` and timeseries `last_value_at`.

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`

### `scripts/uk_aq_dashboard_local.py`
Purpose:
- Run a local dashboard server that exposes PM2.5, PM10, and NO2 freshness buckets (timeseries last_value_at).
- Full system documentation: `system_docs/uk_aq_dashboard.md`.

Common commands:
```
python3 scripts/uk_aq_dashboard_local.py --port 8045
```

Notes:
- Serves the UI at `http://127.0.0.1:8045` and JSON at `/api/dashboard`.
- Also exposes `/api/storage_coverage` for calendar-only payloads.
- The HTML lives at `data/uk_aq_dashboard/uk_aq_dashboard.html`.
- Local dashboard assets under `data/uk_aq_dashboard/` are served via `/assets/...` (for example `/assets/dropbox-icon.svg`).
- Frontend now loads non-calendar panels first via `/api/dashboard?include_storage_coverage=0`, then refreshes the calendar asynchronously from `/api/storage_coverage`.
- Storage coverage calendar includes a `Force Refresh` button (left of `Previous`) that calls `/api/dashboard?force=1` to bypass server cache and rebuild calendar rows immediately.
- Storage coverage calendar includes a `Today` button (between the `Monthly/Yearly` selector and `Force Refresh`) that jumps to the current UTC month in monthly mode and current UTC year in yearly mode.
- Storage coverage calendar has a `Monthly`/`Yearly` view selector. Monthly view keeps the 3-row labeled bars (top `Ingest DB` or `R2 History - Observs`, middle `ObsAQI DB - Observs` with the R2 2-box middle-shift rule, and AQI levels on bottom with yellow/green striping only when both AQI sources are present), and today is rendered with half-width bars (no labels). Yearly view shows per-day 2x2 colored squares without labels/day numbers and weekday letters `M T W T F S S` above each month.
- Monthly bars are left-aligned with reduced corner radius; when a day/domain exists in the Dropbox checkpoint state, that bar shows a second line (`Dropbox` + icon). If no Dropbox day record exists, the bar remains single-line.
- Yearly mode excludes today (`complete days only`), so no colored squares are rendered for the current UTC day.
- Storage coverage cache now refreshes hourly at `:58` UTC.
- Dispatcher feed shows gap-station context for OpenAQ runs as `(<n> GAP)` under Stations when `gap_stations_polled > 0`.
- Local dashboard request handlers suppress client-disconnect socket noise (`BrokenPipeError` / `ConnectionResetError`) so refresh/closed-tab events do not flood local logs.
- Includes a DB cluster size panel with period selector (`6h`, `12h`, `24h`, `48h`, `7d`, `14d`, `28d`): line chart for `ingestdb` + `obs_aqidb` cluster MB (dynamic Y max), schema stacked area chart for `uk_aq_observs` + `uk_aq_aqilevels` MB, and R2 History domain stacked area chart for `observations` + `aqilevels` MB; missing series values render as `0`, stacked charts expose full-height bucket hover tooltips showing both series plus their total at the hovered datetime, the schema oldest-day legend row is `uk_aq_observs >= DD/MM/YYYY   uk_aq_aqilevels >= DD/MM/YYYY`, and calendar/chart colors are fixed to: ingest red `#FE2E2E`, R2 observations orange `#F48021`, R2 AQI levels yellow `#F4C04B`, ObsAQI observations blue `#3C82F5`, ObsAQI AQI levels green `#61D836`.
- Direct Supabase metric fallback reads are paginated (`offset`/`limit`) so chart windows are not clipped by PostgREST row caps.
- External DB-size API payloads are accepted when DB series is current; if schema/R2 lag DB by more than 6 hours, dashboard keeps external rows and performs targeted direct-Supabase top-up only for lagging series (avoids full all-series fallback on each refresh).
- R2 usage refresh gathers storage and operation metrics in parallel to reduce force-refresh latency.
- R2 history-days API reads are cached in-memory for 5 minutes and reused across `/api/dashboard`, `/api/storage_coverage`, and `/api/r2_metrics`.
- Requires a service role key (anon/authenticated JWTs will be rejected).

Data sources used by storage coverage:
- Ingest/ObsAQI oldest-day bounds: `uk_aq_db_size_metrics_hourly` + `uk_aq_schema_size_metrics_hourly`.
- ObsAQI exact day counts (preferred): `uk_aq_public.uk_aq_obs_aqidb_day_counts_current`.
- ObsAQI Observs day presence fallback: `uk_aq_public.uk_aq_rpc_observs_drop_candidates` + `uk_aq_public.uk_aq_rpc_observations_hourly_fingerprint` (`observation_count > 0`).
- ObsAQI AQI day presence fallback: `uk_aq_public.uk_aq_rpc_aqilevels_drop_candidates` with `hourly_rows > 0`.
- R2 History exact day presence (preferred): external API `/v1/r2-history-days` (configurable via `UK_AQ_R2_HISTORY_DAYS_API_URL`).
  - bucket selection is fixed in the Worker env (`CFLARE_R2_BUCKET`).
- R2 History bounds fallback: RPC `uk_aq_public.uk_aq_rpc_r2_history_window` (configurable via `UK_AQ_R2_HISTORY_WINDOW_RPC`).
- R2 domain presence guard fallback: `uk_aq_r2_domain_size_metrics_hourly` latest non-zero domain series (used only when exact day presence API is unavailable).
- Dropbox backup presence (per day/domain): checkpoint JSON `r2_history_backup_state_v1.json` (`domains.<domain>.days.<YYYY-MM-DD>` keys).

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `UK_AQ_PUBLIC_SCHEMA` (optional; default `uk_aq_public`, used for DB size metrics view reads)
- `UK_AQ_DB_SIZE_LOOKBACK_DAYS` (optional; default `28`)
- `UK_AQ_DB_SIZE_API_URL` (optional; Cloudflare/API endpoint for DB size metrics fan-in)
- `UK_AQ_DB_SIZE_API_TOKEN` (optional; bearer token for `UK_AQ_DB_SIZE_API_URL`)
- `OBS_AQIDB_SUPABASE_URL` / `OBS_AQIDB_SECRET_KEY` (optional direct fallback for `obs_aqidb` DB-size series when `UK_AQ_DB_SIZE_API_URL` is not set/unavailable)
- `UK_AQ_R2_HISTORY_DAYS_API_URL` (optional; if unset and `UK_AQ_DB_SIZE_API_URL` is set, dashboard derives `<origin>/v1/r2-history-days`)
- `UK_AQ_R2_HISTORY_DAYS_API_TOKEN` (optional; defaults to `UK_AQ_DB_SIZE_API_TOKEN`)
- `UK_AQ_R2_HISTORY_DAYS_API_MAX_DAYS` (optional; default `3660`)
- `UK_AQ_R2_HISTORY_WINDOW_RPC` (optional; default `uk_aq_rpc_r2_history_window`)
- `UK_AQ_R2_HISTORY_DROPBOX_STATE_FILE` (optional explicit local path to checkpoint JSON; highest priority)
- `UK_AQ_DROPBOX_LOCAL_ROOT` (optional local Dropbox sync root; default auto-detect: `~/Dropbox`)
- `UK_AQ_DROPBOX_APP_FOLDER` (optional app-folder name under `.../Dropbox/Apps/`; if unset dashboard scans app folders)
- `UK_AQ_DROPBOX_ROOT` (used to build checkpoint path under local Dropbox root)
- `UK_AQ_R2_HISTORY_DROPBOX_DIR` (optional; default `R2_history_backup`)
- `UK_AQ_R2_HISTORY_BACKUP_STATE_REL_PATH` (optional; default `_ops/checkpoints/r2_history_backup_state_v1.json`)
- `UK_AQ_COVERAGE_DAY_FETCH_LIMIT` (optional; default `1000`, page size for per-day coverage fetches)
- `UK_AQ_AQILEVELS_COVERAGE_DAYS_VIEW` (optional; default `uk_aq_timeseries_aqi_daily`)

Dropbox checkpoint local path resolution (for monthly bar second-line status):
- First: `UK_AQ_R2_HISTORY_DROPBOX_STATE_FILE` if set.
- Otherwise: `<UK_AQ_DROPBOX_LOCAL_ROOT>/<UK_AQ_DROPBOX_ROOT>/<UK_AQ_R2_HISTORY_DROPBOX_DIR>/<UK_AQ_R2_HISTORY_BACKUP_STATE_REL_PATH>`.
- Also checks app-folder layouts: `<UK_AQ_DROPBOX_LOCAL_ROOT>/Apps/<app>/<UK_AQ_DROPBOX_ROOT>/<UK_AQ_R2_HISTORY_DROPBOX_DIR>/<UK_AQ_R2_HISTORY_BACKUP_STATE_REL_PATH>` (prefers `github-uk-air-quality-networks`).
- If no checkpoint file is found/readable, the dashboard omits Dropbox second-line labels.

### `scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py`
Purpose:
- Mirror `uk_aq_core` reference tables from ingest DB into Obs AQI DB as an exact, ID-preserving PK set match. The sync preserves ingest numeric IDs for core reference tables rather than remapping natural keys, because downstream rows reference those numeric IDs through FKs such as `phenomena.observed_property_id` and `timeseries.phenomenon_id`.
- Sync scope is limited to:
  - `uk_aq_core.connectors`
  - `uk_aq_core.phenomena`
  - `uk_aq_core.stations`
  - `uk_aq_core.timeseries`

Behavior:
- Reads source rows from ingest via PostgREST (`Accept-Profile`/`Content-Profile: uk_aq_core`).
- Uses destination mirror RPCs in `uk_aq_public` (not direct `uk_aq_core` table endpoints):
  - `uk_aq_rpc_core_table_select`
  - `uk_aq_rpc_core_table_upsert`
  - `uk_aq_rpc_core_table_delete_keys`
- Retries transient REST/network failures per page using exponential backoff with jitter:
  `SSLError`, `ConnectionError`, `Timeout`, HTTP `429`, and HTTP `5xx`.
- Runs a pre-sync timeseries alignment check keyed by `(connector_id, service_ref, timeseries_ref)`:
  - prints per-connector row-count/hash summary (`key_only_hash`, `key_plus_id_hash`) for source vs destination
  - reports key-only gaps (`missing_in_destination`, `extra_in_destination`) and ID mismatches
- If any key-set mismatch or ID mismatch is detected, sync fails fast by default (no remap is applied).
- Runs a pre-sync observed-properties alignment check keyed by `code`; by default it reports `OBSERVED_PROPERTY_ID_MISMATCH` rows and exits before writes if the same code exists in source and destination under different IDs.
- Optional observed-properties ID repair mode: set `OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS=1` for a one-off run after applying `supabase/sql/20260617_observed_properties_id_drift_repair_rpc.sql` to ObsAQIDB. The script prints proposed `observed_properties` repairs, calls `uk_aq_rpc_repair_observed_property_id_drift`, discovers every destination FK referencing `uk_aq_core.observed_properties(id)` and rewires those dependent rows from stale destination IDs to source IDs, removes stale duplicate rows, advances the identity sequence, and then re-runs the alignment check before normal sync continues. The SQL RPC refuses ambiguous repairs, including cases where the target source ID is already occupied by a different code or where any dependent FK rows would remain on the stale ID.
- Repair path for ID drift:
  - `python3 scripts/stations_daily/uk_aq_repair_obs_aqidb_timeseries_ids.py` (dry-run)
  - `python3 scripts/stations_daily/uk_aq_repair_obs_aqidb_timeseries_ids.py --apply` (repair)
- Upserts destination rows by table primary key and hard-deletes destination rows whose PKs no longer exist in ingest.
- Also syncs FK dependency tables (`observed_properties`, `categories`, `offerings`, `features`, `procedures`) in dependency-safe order so mirrored rows can insert/delete cleanly.
- Validates destination schema against source metadata (column order/name/type/nullability/default + PK) before any write.
  Source metadata is parsed from the schema SQL path provided by `UK_AQ_INGEST_CORE_SCHEMA_SQL_PATH` when present.
  If the file is unavailable or cannot be parsed, the script falls back to embedded static metadata.
- Fails fast (non-zero exit) on schema mismatch or sync errors.

Environment:
- `SRC_SUPABASE_URL`
- `SRC_SECRET_KEY`
- `DST_SUPABASE_URL`
- `DST_SECRET_KEY`
- `UK_AQ_INGEST_CORE_SCHEMA_SQL_PATH` (optional absolute/relative path to source `uk_aq_core_schema.sql`; when omitted, the script skips file-parse and uses embedded static metadata fallback)
- `OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS` (optional; default unset/false. Set to `1` for a single guarded repair run for observed-properties ID drift, then unset before the next normal scheduled sync.)

Notes:
- Destination metadata is read via `uk_aq_public.uk_aq_rpc_info_schema_columns` and `uk_aq_public.uk_aq_rpc_info_schema_primary_keys`.
- Destination `uk_aq_core` does not need to be API-exposed for this script, as long as the mirror RPCs above exist in `uk_aq_public`.
- Apply agg_daily schema SQL first on Obs AQI DB:
  - `CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_schema.sql`
  - Focused apply option for mirror RPC updates: `CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_core_mirror_rpcs.sql`

### `scripts/stations_daily/uk_aq_repair_obs_aqidb_timeseries_ids.py`
Purpose:
- Normalize `obs_aqidb.uk_aq_core.timeseries.id` values so they match ingest DB canonical IDs for the same natural keys.
- Repairs historical ID drift that remap-only syncs do not resolve.

Behavior:
- Compares source ingest vs destination obs_aqidb timeseries keyed by:
  - `(connector_code, service_ref, timeseries_ref)`
- Reports:
  - `id_mismatches`
  - `missing_in_destination`
  - `extra_in_destination`
- Default mode is dry-run (no writes).
- `--apply` mode performs a transactional two-phase ID move in destination:
  - `dst_id -> temp_id -> src_id`
  - keeps FK tables valid while IDs move
- Auto-discovers and updates:
  - FK tables referencing `uk_aq_core.timeseries(id)` (AQI hourly/daily/monthly)
  - additional `timeseries_id` tables (for example `uk_aq_observs` partitions, `uk_aq_ops.chart_load_metrics`)
- Resets `uk_aq_core.timeseries` identity sequence after repair.
- Runs post-apply validation that shared natural keys now have matching IDs.

Common commands:
```
# Dry-run all connectors
python3 scripts/stations_daily/uk_aq_repair_obs_aqidb_timeseries_ids.py

# Dry-run OpenAQ only
python3 scripts/stations_daily/uk_aq_repair_obs_aqidb_timeseries_ids.py --connector-code openaq

# Apply repair (all connectors)
python3 scripts/stations_daily/uk_aq_repair_obs_aqidb_timeseries_ids.py --apply
```

Environment:
- `SUPABASE_DB_URL` (source ingest DB URL)
- `OBS_AQIDB_SUPABASE_DB_URL` (destination Obs AQI DB URL)

Notes:
- Apply is blocked by default when natural-key sets differ (`missing_in_destination` or `extra_in_destination` non-zero). Use `--allow-key-delta` only when partial repair is intentional.
- Requires `psql` in `PATH`.

### `scripts/uk_aq_station_snapshot_local.py`
Purpose:
- Run a local station snapshot dashboard that prioritizes direct service-role reads from ingestdb and ObsAQIDB, with edge fallback support.

Common commands:
```
python3 scripts/uk_aq_station_snapshot_local.py --port 8046
python3 scripts/uk_aq_station_snapshot_local.py --edge-url https://<project>.supabase.co/functions/v1/uk_aq_station_snapshot
```

Notes:
- Serves the UI at `http://127.0.0.1:8046` plus local APIs:
  - `/api/config` for startup defaults/mode
  - `/api/snapshot` for aggregated station payloads
  - `/api/token` for edge-fallback token refresh
- The HTML lives at `data/uk_aq_station_snapshot/uk_aq_station_snapshot.html`.
- In service-role mode (preferred), `/api/snapshot` reads directly from:
  - ingestdb: `uk_aq_core.stations`, `uk_aq_core.timeseries`, `uk_aq_raw.openaq_station_checkpoints`, `uk_aq_raw.openaq_timeseries_checkpoints`, `uk_aq_core.observations`
  - ObsAQIDB: `uk_aq_observs.observations`, `uk_aq_aqilevels.timeseries_aqi_hourly`, `uk_aq_aqilevels.timeseries_aqi_daily` (via direct SQL when DB URL is available)
- The page renders ingestdb selected-timeseries observations, ingestdb all-timeseries observations, ObsAQIDB observs, and ObsAQIDB AQI hourly/daily rows.
- `obs_limit` supports `all` or positive integer values in the UI.
- If service-role ingest access is not configured, script falls back to edge mode and uses `uk_aq_station_snapshot` with JWT auth.
- Edge fallback token behavior:
  - Access token comes from `UK_AQ_DEV_JWT` (or `--dev-jwt`) and is exposed by `/api/config`.
  - If `UK_AQ_DEV_REFRESH_TOKEN` is set, local server can auto-refresh expired access tokens via `/api/token`.
  - Rotated refresh tokens are written back to the env file (default `.env.supabase`) so restarts keep working.
- Snapshot `window` selector supports: `6h`, `24h`, `7d`, `21d`, `31d`, `90d`.

Environment:
- Service-role ingest mode (preferred):
  - `SUPABASE_URL` or `SB_SUPABASE_URL`
  - `SB_SECRET_KEY` (or `SUPABASE_SERVICE_ROLE_KEY`)
- ObsAQIDB enrichment:
  - `OBS_AQIDB_SUPABASE_DB_URL` (preferred for direct SQL reads)
  - optional fallback path: `OBS_AQIDB_SUPABASE_URL` + `OBS_AQIDB_SECRET_KEY`
- Optional edge fallback:
  - `UK_AQ_STATION_SNAPSHOT_EDGE_URL` (optional explicit edge URL)
  - `UK_AQ_DEV_JWT` (or `UK_AQ_DEV_REFRESH_TOKEN`)
  - `UK_AQ_DEV_REFRESH_TOKEN` (optional; enables auto-refresh)
  - `UK_AQ_DEV_ENV_FILE` (optional; env file to persist rotated refresh tokens, default `.env.supabase`)
  - `SB_PUBLISHABLE_DEFAULT_KEY` required when using auto-refresh
- Optional tuning:
  - `UK_AQ_STATION_SNAPSHOT_PAGE_SIZE` (default `1000`)
  - `UK_AQ_STATION_SNAPSHOT_MAX_ROWS` (default `200000`)

### `scripts/uk_aq_issue_dev_auth_tokens.py`
Purpose:
- Issue fresh Supabase auth tokens for local dashboard use and optionally write them into an env file.

Common commands:
```
python3 scripts/uk_aq_issue_dev_auth_tokens.py
python3 scripts/uk_aq_issue_dev_auth_tokens.py --write-env-file .env.supabase
python3 scripts/uk_aq_issue_dev_auth_tokens.py --refresh-token "$UK_AQ_DEV_REFRESH_TOKEN"
```

Notes:
- Uses password grant when `--email/--password` (or `UK_AQ_DEV_USER_EMAIL` / `UK_AQ_DEV_USER_PASSWORD`) are provided.
- Uses refresh-token grant when `--refresh-token` (or `UK_AQ_DEV_REFRESH_TOKEN`) is provided.
- Outputs `UK_AQ_DEV_JWT`, `UK_AQ_DEV_REFRESH_TOKEN`, and `UK_AQ_DEV_JWT_EXPIRES_AT` for export by default.

Environment:
- `SUPABASE_URL` or `SB_SUPABASE_URL`
- `SB_PUBLISHABLE_DEFAULT_KEY`
- `UK_AQ_DEV_USER_EMAIL` + `UK_AQ_DEV_USER_PASSWORD` (for password grant), or `UK_AQ_DEV_REFRESH_TOKEN` (for refresh grant)

### `dev_dashboards.sh` and `dev_dashboards_stop.sh`
Purpose:
- Start/stop both local dashboard servers on-demand:
  - `scripts/uk_aq_dashboard_local.py`
  - `scripts/uk_aq_station_snapshot_local.py`

Common commands:
```
./dev_dashboards.sh
./dev_dashboards_stop.sh
```

Notes:
- `dev_dashboards.sh` writes `./.dashboards.pids` and logs to `./logs/`.
- `dev_dashboards.sh` prefers `./.venv/bin/python3` when present, otherwise falls back to `python3`, and it exits early if the selected interpreter cannot import `requests`.
- `dev_dashboards_stop.sh` only stops exact PIDs listed in `./.dashboards.pids` (no broad `pkill`).

Environment:
- Required: `SUPABASE_URL`, `SB_PUBLISHABLE_DEFAULT_KEY`, and either `UK_AQ_DEV_JWT` or `UK_AQ_DEV_REFRESH_TOKEN`
- Optional overrides: `HOST`, `SCHEDULER_PORT`, `SNAPSHOT_PORT`

### `scripts/uk_air_sos/uk_air_sos_ingest.py`
Purpose:
- Discover stations and timeseries with optional filters.
- Backfill observations for a chosen year.
- Refresh recent observations for the last N hours.

Common commands:
```
python3 scripts/uk_air_sos/uk_air_sos_ingest.py --discover --backfill-2025
python3 scripts/uk_air_sos/uk_air_sos_ingest.py --refresh-recent --hours 6
```

Writes to:
- `connectors`, `stations`, `timeseries`, `observations`

Key flags:
- `--bbox west,south,east,north` (default: UK bbox)
- `--region Bristol` (optional)
- `--station-like Bristol` (optional label filter)
- `--station-type AURN` (optional)
- `--strict-bbox` to exclude stations with missing coordinates
- `--pollutants no2,o3,pm10,pm2.5` (default common pollutants)
- `--all-pollutants` to disable pollutant filtering
- `--backfill-year 2025` to backfill a specific year
- `--service-ref` (alias `--service-id`) or `--service-label` to target a specific SOS service
- `--sample-timeseries 1` to log a short summary of the first N timeseries objects
- `--raw-dropbox` to write raw payloads to Dropbox (testing only; guarded by `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL`)
- `--raw-dropbox-folder /connectors/uk_air_sos/raw_data` to override the Dropbox folder
- `--log-level WARNING` to reduce logging output
    - Default output prints only station count, error count, and Dropbox upload info.
Batching:
- If `connectors.poll_timeseries_batch_size` is set for the chosen connector, it overrides the default batch size for timeseries discovery.
Stations bbox:
- If `connectors.stations_bbox_supported` is false, the script skips bbox when calling `/stations`.
Timeseries station filter:
- If `connectors.timeseries_station_filter_supported` is false, the script skips station filtering for `/timeseries`.
- UK-AIR `firstValue`/`lastValue` metadata can arrive as nested objects (`{timestamp,value}`); the script parses those shapes and only writes `first_value_at` / `last_value_at` / `last_value` when source values are present so discovery runs do not null-clobber existing freshness fields.
- On full-catalog discovery runs (timeseries not station-scoped), the script reconciles UK-AIR timeseries lifecycle:
  present refs set `last_catalog_seen_at` and reset `catalog_missing_runs`; missing active refs increment
  `catalog_missing_runs`; `ended_at` is set after 2 consecutive misses; reappearing refs auto-reactivate.
Phenomenon lookup:
- If a timeseries label contains a `dd.eionet.europa.eu/vocabulary/aq/pollutant/` URL and `phenomenon` is missing, the script resolves Eionet metadata and stores `phenomena.source_label` + `phenomena.notation` (shortname), with `label` falling back to `prefLabel`.

Raw payloads (testing only):
- Raw payload uploads are disabled unless `SUPABASE_URL` matches `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL`.
- Dropbox credentials required: `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`.
- The raw capture writes all SOS responses fetched during the run into a single gzipped JSONL file and uploads it to Dropbox.
- Uploads are organized under `connectors/uk_air_sos/raw_data/YYYY-MM-DD` within the configured Dropbox folder (for scoped apps, do not include `/Apps/<app>` in the path).
- Each run also uploads a log file to `/connectors/uk_air_sos/log/YYYY-MM-DD/` (Dropbox app root).
- Logs older than 31 days are zipped into `/connectors/uk_air_sos/log/archive/YYYY-MM-DD.zip`; archive files older than 1 year are removed.
- If `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL` is unset in live environments, the upload never runs (even if `--raw-dropbox` is passed).

### `scripts/erg_laqn/erg_laqn_list_stations.py`
Purpose:
- Fetch LAQN monitoring sites from the ERG AirQuality API.
- Optionally upsert LAQN stations, station_metadata, and seed timeseries rows into Supabase.

Common commands:
```
python3 scripts/erg_laqn/erg_laqn_list_stations.py
python3 scripts/erg_laqn/erg_laqn_list_stations.py --format csv --output laqn_stations.csv
python3 scripts/erg_laqn/erg_laqn_list_stations.py --to-supabase
```

Key flags:
- `--group` to override the GroupName filter (default: London).
- `--no-filter` to skip UK bounding box filtering.
- `--skip-station-metadata` to avoid station_metadata updates.
- `--skip-timeseries` to avoid seeding timeseries rows for each station/species.

Notes:
- Connector upserts preserve existing `poll_enabled`; new connectors default to `poll_enabled=false`.


Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `LAQN_BASE_URL` (optional; defaults to `https://api.erg.ic.ac.uk/AirQuality`)
- `LAQN_DEFAULT_GROUP` (optional; defaults to `London`)
- `LAQN_MONITORING_SITES_PATHS` (optional; comma-separated API paths to try)
- `LAQN_CONNECTOR_CODE` (optional; defaults to `erg_laqn`)
- `LAQN_CONNECTOR_LABEL` (optional; defaults to `ERG London Air`, falls back to `LAQN_SERVICE_LABEL`)
- `LAQN_CONNECTOR_DISPLAY_NAME` (optional; defaults to `London Air LAQN`)
- `LAQN_SERVICE_REF` (optional; defaults to `LAQN_CONNECTOR_CODE`)
- `LAQN_USER_AGENT` (optional)
- `LAQN_TIMESERIES_SPECIES` (optional; defaults to `NO2,PM10,PM25,O3`)

### `scripts/openaq/openaq_list_stations.py`
Purpose:
- Fetch OpenAQ locations within the UK bounding box and optionally upsert stations into Supabase.

Common commands:
```
python3 scripts/openaq/openaq_list_stations.py
python3 scripts/openaq/openaq_list_stations.py --format csv --output uk_openaq_stations.csv
python3 scripts/openaq/openaq_list_stations.py --to-supabase
```

Notes:
- Connector upserts preserve existing `poll_enabled`; new connectors default to `poll_enabled=false`.
- Shared token budget enforcement is enabled by default and reserves one token per OpenAQ request via `uk_aq_rpc_openaq_token_budget_reserve` (same shared key/caps used by ingest runtime).

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `SUPABASE_DB_URL` (required for `--to-supabase`; also required when `OPENAQ_SHARED_BUDGET_ENFORCE=true` unless `OPENAQ_SHARED_BUDGET_FAIL_OPEN=true`)
- `OPENAQ_BASE_URL` (optional; defaults to `https://api.openaq.org/v3`)
- `OPENAQ_API_KEY` (required)
- `OPENAQ_CONNECTOR_CODE` (optional; defaults to `openaq`)
- `OPENAQ_SERVICE_REF` (optional; defaults to `OPENAQ_CONNECTOR_CODE`)
- `OPENAQ_SERVICE_LABEL` (optional; defaults to `OpenAQ`)
- `OPENAQ_USER_AGENT` (optional; defaults to `uk-air-quality-networks`)
- `OPENAQ_BBOX` (optional; defaults to `-8.623555,49.863222,1.763337,60.871222`)
- `OPENAQ_PAGE_LIMIT` (optional; defaults to `1000`)
- `OPENAQ_MAX_PAGES` (optional; defaults to `0` meaning no cap)
- `OPENAQ_RATE_LIMIT_PER_MIN` (optional; defaults to `60`)
- `OPENAQ_SHARED_BUDGET_ENFORCE` (optional; defaults to `true`)
- `OPENAQ_SHARED_BUDGET_KEY` (optional; defaults to `openaq`)
- `OPENAQ_SHARED_BUDGET_CALLER` (optional; defaults to `openaq_list_stations`)
- `OPENAQ_SHARED_BUDGET_MINUTE_LIMIT` (optional; defaults to `50`)
- `OPENAQ_SHARED_BUDGET_HOUR_LIMIT` (optional; defaults to `1500`)
- `OPENAQ_SHARED_BUDGET_FAIL_OPEN` (optional; defaults to `false`; when `true`, script continues if budget RPC is unavailable/denied)
- `OPENAQ_LOG_LEVEL` (optional; defaults to `INFO`)

### `scripts/erg_laqn/erg_laqn_ingest.py`
Purpose:
- Ingest LAQN observations from the ERG AirQuality API into Supabase.

Common commands:
```
python3 scripts/erg_laqn/erg_laqn_ingest.py --species NO2,PM10
python3 scripts/erg_laqn/erg_laqn_ingest.py --days 3 --limit 5 --dry-run
```

Key flags:
- `--species` to set pollutant species codes (default: NO2,PM10,PM25,O3).
- `--days` or `--start-date`/`--end-date` to control the ingest window.
- `--index-days` is not supported by LAQN raw data endpoints; the script logs a warning and uses the date range.
- `--site-codes` to ingest a subset of station refs.
- `--stations-json` to use a local LAQN stations snapshot instead of the live API.
- `--skip-stations` to avoid station upserts.
- `--dry-run` to skip Supabase writes while still fetching observations (outputs use a `timeseries_id` of `0`).
- `--output-raw-responses` to write raw API responses per station/species.

Notes:
- Skips zero-valued observations from the most recent hour so placeholder zeros are not written to the DB.

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `LAQN_BASE_URL` (optional; defaults to `https://api.erg.ic.ac.uk/AirQuality`)
- `LAQN_RAW_DATA_URL_TEMPLATE` (optional; overrides the raw data endpoint URL template)
- `LAQN_CONNECTOR_CODE` (optional; defaults to `erg_laqn`)
- `LAQN_CONNECTOR_LABEL` (optional; defaults to `ERG London Air`, falls back to `LAQN_SERVICE_LABEL`)
- `LAQN_CONNECTOR_DISPLAY_NAME` (optional; defaults to `London Air LAQN`)
- `LAQN_SERVICE_REF` (optional; defaults to `LAQN_CONNECTOR_CODE`)
- `LAQN_USER_AGENT` (optional)

### `scripts/erg_laqn/erg_laqn_latest_check.py`
Purpose:
- Check the latest available LAQN observations for a sample of active sites/species.

Common commands:
```
python3 scripts/erg_laqn/erg_laqn_latest_check.py --days 2 --species NO2,PM10
```

Key flags:
- `--days` lookback window in days (default: 2).
- `--species` comma-separated species list (default: NO2).
- `--max-sites` number of active sites to test (default: 5).
- `--stations-json` path to a stations JSON snapshot (default: `erg_laqn_stations.json`).
- `--base-url` ERG API base URL.
- `--timeout` HTTP timeout in seconds.

Environment:
- `LAQN_BASE_URL` (optional; defaults to `https://api.erg.ic.ac.uk/AirQuality`)
- `LAQN_STATIONS_JSON` (optional; defaults to `erg_laqn_stations.json`)

### `scripts/uk_aq_move_history_observations.sh`
Purpose:
- Move observations older than a cutoff from the main DB into the history DB in batches.

Common commands:
```
CUTOFF_DAYS=14 BATCH_SIZE=50000 ./scripts/uk_aq_move_history_observations.sh
./scripts/uk_aq_move_history_observations.sh --days 21 --batch-size 20000
```

Key flags:
- `--days` cutoff age in days (default: 14).
- `--batch-size` rows per batch (default: 50,000).

Environment:
- `SUPABASE_DB_URL` (main DB)
- `SBASE_HISTORY_DB_URL` (history DB)
- `CUTOFF_DAYS` (optional; default 14)
- `BATCH_SIZE` (optional; default 50,000)

### `scripts/uk_aq_refresh_station_geo_r2.py`
Purpose:
- Look up PCON + LA codes from the PCON/LA R2 shard lookup and update missing values in `stations`.

Common commands:
```
python3 scripts/uk_aq_refresh_station_geo_r2.py
python3 scripts/uk_aq_refresh_station_geo_r2.py --page-size 200 --dry-run
```

Key flags:
- `--page-size` Supabase page size (default: 500).
- `--limit` max stations to process (default: 0 = no limit).
- `--sleep-seconds` sleep between updates (default: 0).
- `--bucket` override the R2 bucket name.
- `--prefix` override the R2 prefix root.
- `--dry-run` log updates without writing.

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `UK_AQ_DOMAIN_CLOUDFLARE_ACCOUNT_ID`
- `UK_AQ_DOMAIN_CLOUDFLARE_API_TOKEN`
- `UK_AQ_GEO_R2_BUCKET` (optional; defaults to `uk-aq-pcon-la-lookup`)
- `UK_AQ_GEO_R2_PREFIX` (optional; defaults to `v1`)

### `scripts/uk_aq_resolve_dropbox_geojson.py`
Purpose:
- Resolve and download a GeoJSON file from Dropbox, selecting the latest version when needed.

Common commands:
```
python3 scripts/uk_aq_resolve_dropbox_geojson.py --dropbox-base "/GeoJSON/PCON" --output tmp/pcon.geojson --env-prefix PCON
```

Key flags:
- `--dropbox-base` folder path to search (optional if `--dropbox-path` is provided).
- `--dropbox-path` direct path to a GeoJSON file.
- `--version` target year/version (optional).
- `--output` local output path (required).
- `--env-prefix` prefix for writing `*_VERSION` + `*_GEOJSON_PATH` into `GITHUB_ENV`.

Environment:
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`

### `scripts/uk_aq_load_pcon_boundaries_aiven.py`
Purpose:
- Load PCON GeoJSON boundaries into Aiven PostGIS.

Common commands:
```
python3 scripts/uk_aq_load_pcon_boundaries_aiven.py --geojson tmp/pcon.geojson --pcon-version 2024
```

Key flags:
- `--code-field` GeoJSON property for PCON code (default: `PCON24CD`).
- `--name-field` GeoJSON property for PCON name (default: `PCON24NM`).
- `--skip-if-exists` skip upload if version already exists.

Environment:
- `PCON_AIVEN_PG_DSN`

Note:
- Legacy Supabase boundary loaders moved to `archive/2026-01-25/scripts/`.

### `scripts/uk_aq_load_la_boundaries_aiven.py`
Purpose:
- Load LA GeoJSON boundaries into Aiven PostGIS.

Common commands:
```
python3 scripts/uk_aq_load_la_boundaries_aiven.py --geojson tmp/la.geojson --la-version 2024
```

Key flags:
- `--code-field` GeoJSON property for LA code (default: `la_code`).
- `--name-field` GeoJSON property for LA name (default: `la_name`).
- `--source-srid` SRID of the GeoJSON coordinates (default: 4326; LAD 2025 BGC uses 27700).
- `--skip-if-exists` skip upload if version already exists.

Environment:
- `PCON_AIVEN_PG_DSN`

### `scripts/uk_aq_load_guidelines.py`
Purpose:
- Load WHO GAQG 2021 guideline limits into `uk_aq_guidelines`.

Common commands:
```
python3 scripts/uk_aq_load_guidelines.py
python3 scripts/uk_aq_load_guidelines.py --csv data/WHO-guidelines/WHO_GAQG_2021_pollutant_limits.csv
```

Inputs:
- CSV with columns: pollutant, averaging_time, unit, AQG_2021, IT1, IT2, IT3, IT4, notes, source.

Key flags:
- `--source` to override the CSV source column value for all rows.
- `--batch-size` (default: 200)

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`

### `scripts/uk_aq_fix_station_geometry.py`
Purpose:
- Fix swapped station geometry coordinates (lat/lon reversed).

Common commands:
```
python3 scripts/uk_aq_fix_station_geometry.py
```

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`

### `scripts/uk_aq_enrich_station_names.py`
Purpose:
- Preview OSNI Gazetteer place-name matches for stations missing `station_name`.
 - Optionally backfill `stations.region` using OS Open Names GB lookups.

Common commands:
```
python3 scripts/uk_aq_enrich_station_names.py --matches 5
```

Inputs:
- GeoJSON point files:
  - Placenames (default: `data/geojson/OSNI/osni_open_data_-_gazetteer_-_place_names.geojson`).
  - Streetnames (default: `data/geojson/OSNI/osni_open_data_-_gazetteer_-_streetnames.geojson`).
- Optional GB GPKG: `data/gpkg/OS/os_open_names_gpkg/Data/opname_gb.gpkg` (downloaded from Dropbox if missing and a Dropbox path is provided).
  - If the GPKG CRS is not EPSG:4326, install `pyproj` so the script can project station coordinates.

Key flags:
- `--limit` number of stations to inspect (0 means no limit).
- `--matches` number of nearby names to list per station.
- `--max-distance-m` optional maximum distance in meters.
- `--streetnames-geojson` override streetnames GeoJSON path.
- `--no-ni-filter` to also attempt OSNI matching for non-NI stations (debugging only).
- `--apply` update `stations.station_name` for rows with proposed names.
- `--apply` also updates `stations.region` when a GB match provides a region and the station is missing one.
- `--apply-batch-size` batch size for station_name updates (default: 200).
- In `--apply` mode, the script skips automatic summary lookups for pollutants/latest observations unless `--include-pollutants` or `--include-latest` is passed explicitly.

### `scripts/uk_aq_backfill_station_regions.py`
Purpose:
- Backfill `stations.region` using OS Open Names GB lookups for stations missing a region.

Common commands:
```
python3 scripts/uk_aq_backfill_station_regions.py
python3 scripts/uk_aq_backfill_station_regions.py --apply
```

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- Optional Dropbox credentials if `--download-gb-gpkg` is used.

### `scripts/uk_aq_enrich_test_script.py`
Purpose:
- Debug the Supabase REST counts used to decide whether enrichment runs.

Common commands:
```
python3 scripts/uk_aq_enrich_test_script.py
python3 scripts/uk_aq_enrich_test_script.py --samples 10 --verbose
```

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `--page-size` Supabase pagination batch size.
- `--gb-gpkg-path` local path for the OS Open Names GB GeoPackage.
- `--gb-gpkg-dropbox-path` Dropbox path for the GB GPKG (defaults to `UK_AQ_OS_OPEN_NAMES_GB_DROPBOX_PATH` or the local path).
- `--download-gb-gpkg` download the GB GPKG from Dropbox if missing (also auto-downloads when a Dropbox path is set).
- `--include-gb`/`--no-include-gb` include GB stations using OS Open Names lookups (default: on).
- `--gb-search-radius-m` search radius for OS Open Names in meters (default: 5000).
  - GB matches are split into place/street/other based on `local_type`.
  - Place matches also use `populated_place` (fallback to district/borough).
  - GB lookups now scan all candidates within the search radius to find the nearest street.
  - When no GB street matches are found, the closest `gb_other_matches` entry is used for the proposed name.
  - Postcode fallbacks keep their original casing.
- `--include-pollutants` to include pollutant names per station (timeseries/phenomena lookup).
- `--include-latest` to include latest observations per station by phenomenon.
- `--output-format` set to `summary` (default, JSON lines) or `json` (full payload).
  - NI matches use `ni_place_matches`/`ni_street_matches` to avoid confusion with GB matches.

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `UK_AQ_OS_OPEN_NAMES_GB_DROPBOX_PATH` (optional Dropbox path for the GB GPKG).
- `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN` (needed if a Dropbox download is triggered).
- `PYPROJ_NETWORK` (optional; set to `ON` if pyproj needs to download grid data).

### `scripts/uk_aq_enrich_station_names_report.py`
Purpose:
- Write station name enrichment results to JSON files for review.

Outputs:
- `station_names_proposed_YYYYMMDD_HHMMSS.json` (summary for every station with `station_name` null).
- `station_names_missing_YYYYMMDD_HHMMSS.json` (detailed payloads where `proposed_station_name` is null, including match lists and a missing summary).

Common commands:
```
python3 scripts/uk_aq_enrich_station_names_report.py
python3 scripts/uk_aq_enrich_station_names_report.py --limit 50 --matches 10
```

Notes:
- Uses the same enrichment logic as `scripts/uk_aq_enrich_station_names.py` so changes there apply here.
- Always includes pollutants and latest observation details in the outputs.

### `scripts/uk_aq_backfill_timeseries_stations.py`
Purpose:
- Backfill timeseries rows missing station/feature mappings by re-querying SOS metadata.

Common commands:
```
python3 scripts/uk_aq_backfill_timeseries_stations.py
python3 scripts/uk_aq_backfill_timeseries_stations.py --connector-code uk_air_sos --service-ref 1
```

Key flags:
- `--connector-id` or `--connector-code` to scope the backfill.
- `--service-ref` to scope to a specific SOS service within the connector.
- `--batch-size` (default: 200)
- `--limit` to cap total rows processed.
- `--sleep-seconds` (default: 0.2) between API calls.

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`

### `scripts/uk_air_sos/uk_air_sos_network_assignment_report.py`
Purpose:
- Export and validate canonical UK-AIR SOS assignments using
  `stations.network_id -> networks.id`.
- Preserve connector provenance columns in the report.

Common commands:
```
python3 scripts/uk_aq_backfill_station_memberships.py
python3 scripts/uk_aq_backfill_station_memberships.py --service-ref-from-timeseries
python3 scripts/uk_aq_backfill_station_memberships.py --no-filter --limit 500
python3 scripts/uk_aq_backfill_station_memberships.py --source sos
```

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `UK_AIR_SOS_BASE_URL` (optional override)
Notes:
- Uses the latest `uk_air_sos_site_register.snapshot_at` by default; use `--snapshot-at` to target a specific snapshot.
- Adjust match tolerances with `--match-distance-m` and `--match-distance-no-name-m` if needed.
- Ensure `uk_air_sos_network_pollutants` is populated (via `scripts/uk_air_sos/uk_air_sos_site_register.py --load`).

### `scripts/uk_air_sos/uk_air_sos_site_register.py`
Purpose:
- Download the UK-AIR "Search for monitoring sites" CSV (all sites).
- Use the CSV as the authoritative register for site refs, names, coordinates, and network membership.
- Populate DEFRA flat-file `site_ref` values from official UK-AIR site-info pages when loading.
- Use `network_info/uk_air_sos/uk_air_sos_site_refs.csv` as a seed/override map where needed.

Common commands:
```
python3 scripts/uk_air_sos/uk_air_sos_site_register.py --search-url "<search url>" --output uk_air_sos_site_register.csv
python3 scripts/uk_air_sos/uk_air_sos_site_register.py --csv-url "<direct csv url>" --output uk_air_sos_site_register.csv
python3 scripts/uk_air_sos/uk_air_sos_site_register.py --search-url "<search url>" --dropbox-upload
python3 scripts/uk_air_sos/uk_air_sos_site_register.py --search-url "<search url>" --dropbox-upload --load
python3 scripts/uk_air_sos/uk_air_sos_site_register.py --load-only --csv-path /path/to/uk-air-search-results.csv
python3 scripts/uk_air_sos/uk_air_sos_site_register.py --load-only --csv-path /path/to/uk-air-search-results.csv --site-ref-map-csv network_info/uk_air_sos/uk_air_sos_site_refs.csv
python3 scripts/uk_air_sos/uk_air_sos_site_register.py --load-only --csv-path /path/to/uk-air-search-results.csv --discover-site-refs --validate-site-ref-map --dry-run
```

Environment:
- `UK_AIR_SOS_SITE_SEARCH_URL` (optional; used when `--search-url` is omitted)
- `UK_AIR_SOS_SITE_SEARCH_USER_AGENT` (optional)
- `UK_AQ_DROPBOX_ROOT` (required for `--dropbox-upload`)
- `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN` (required for `--dropbox-upload`)
- `SUPABASE_URL`, `SB_SECRET_KEY` (required for `--load`/`--load-only`)
Notes:
- The script writes a timestamped filename locally and to Dropbox (e.g., `uk_air_sos_site_register_YYYYMMDDTHHMMSSZ.csv`).
- When `--load` is used, it preserves existing `uk_air_sos_networks.network_display_name` values and upserts `uk_air_sos_network_pollutants`.
- `--discover-site-refs` checks official `networks/site-info?uka_id=<uk_air_ref>` pages for flat-file links.
- `--site-ref-map-csv` is optional; unresolved UK-AIR refs are loaded with `site_ref = null` rather than guessed.
- `--validate-site-ref-map` checks mapped refs against official UK-AIR site-info and flat-file pages before loading.

### `scripts/uk_air_sos/uk_air_sos_membership_report.py`
Purpose:
- Generate a detailed CSV report for SOS membership backfills (pollutant keys, register networks, allowed/filtered networks, memberships).

Common commands:
```
python3 scripts/uk_air_sos/uk_air_sos_membership_report.py
python3 scripts/uk_air_sos/uk_air_sos_membership_report.py --snapshot-at "<timestamp>"
python3 scripts/uk_air_sos/uk_air_sos_membership_report.py --output network_info/UK-Air-SOS/uk_air_sos_membership_report.csv
```

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`

Notes:
- Defaults to the latest `uk_air_sos_site_register.snapshot_at`.
- Writes to `network_info/UK-Air-SOS/` with a timestamped filename when `--output` is omitted.


### `scripts/uk_air_sos/uk_air_sos_list_stations.py`
Purpose:
- Fetch all current stations from UK-AIR SOS.
- Filter to UK bounding box (keeps stations with missing coordinates; `geometry` will be null in Supabase).
- Optional upsert into Supabase.

Common commands:
```
python3 scripts/uk_air_sos/uk_air_sos_list_stations.py
python3 scripts/uk_air_sos/uk_air_sos_list_stations.py --format csv --output uk_stations.csv
python3 scripts/uk_air_sos/uk_air_sos_list_stations.py --to-supabase
python3 scripts/uk_air_sos/uk_air_sos_list_stations.py --no-filter --output uk_aq_stations_all.json
python3 scripts/uk_air_sos/uk_air_sos_list_stations.py --raw-output uk_aq_stations_raw.json
python3 scripts/uk_air_sos/uk_air_sos_list_stations.py --service-id-from-timeseries
python3 scripts/uk_air_sos/uk_air_sos_list_stations.py --check-timeseries-links --check-output uk_air_sos_timeseries_link_check.csv
```

Notes:
- Connector upserts preserve existing `poll_enabled`; new connectors default to `poll_enabled=false`.
- If SOS returns no usable `/services` payload, the script reuses the existing `uk_air_sos` connector id instead of failing connector resolution.
- If SOS returns zero non-placeholder stations, Supabase station writes are skipped for that run (including `mark_removed`) to avoid false removals during upstream outages.

Default outputs:
- `uk_air_sos_stations.json`
- `uk_aq_stations_all.json` (when using `--no-filter`)
Optional raw output:
- `--raw-output` writes raw station payloads to a separate JSON file.
Service refs:
- By default, if the SOS reports a single service, that service ref is applied to stations in the JSON output.
- The JSON output also includes a top-level `service_ref` when a single service is detected.
- Use `--service-ref-from-timeseries` (alias `--service-id-from-timeseries`) to resolve `service_ref` from timeseries metadata.
- The internal attribute is named `service_ref_from_timeseries` to match the `_ref` convention; the legacy flag name still works for compatibility.

Notes:
- When `--to-supabase` is enabled, station-name backfills include the existing station metadata needed to satisfy NOT NULL constraints.
- Optional flags: `--skip-station-metadata`, `--skip-network-memberships`, `--skip-station-type-backfill`.
- `--check-timeseries-links` compares payload station_ref/timeseries_ref links against Supabase and writes a CSV report (no data is changed).
- Placeholder SOS station refs (e.g., `9999999999`) are skipped from outputs/upserts and flagged in `station_metadata` with `exclude_from_ui=true`.

Writes to (when `--to-supabase` is set):
- `connectors`, `stations`, `station_metadata`
- `phenomena`, `procedures`, `offerings` (unless `--skip-metadata` is used)

### `scripts/uk_air_sos/uk_air_sos_timeseries_metadata_sample.py`
Purpose:
- Sample SOS timeseries metadata for a small set of stations and highlight matches for keywords (e.g., modelled wind/temp).

Common commands:
```
python3 scripts/uk_air_sos/uk_air_sos_timeseries_metadata_sample.py
python3 scripts/uk_air_sos/uk_air_sos_timeseries_metadata_sample.py --station-limit 50
python3 scripts/uk_air_sos/uk_air_sos_timeseries_metadata_sample.py --match-terms "model,wind,temperature"
python3 scripts/uk_air_sos/uk_air_sos_timeseries_metadata_sample.py --output network_info/UK-Air-SOS/uk_air_sos_timeseries_metadata_sample.json
```

Default output:
- `network_info/UK-Air-SOS/uk_air_sos_timeseries_metadata_sample_<timestamp>.json`
  - `stations` lifecycle fields: `first_seen_at`, `last_seen_at`, `removed_at`
  - Stations not seen in the current run are marked with `removed_at`.

### `scripts/uk_aq_export_stations_dropbox.py`
Purpose:
- Export a combined stations snapshot from Supabase and upload it to Dropbox.

Output:
- `uk_aq_stations_<timestamp>.json` uploaded to the Dropbox folder (default `uk_aq_stations/<YYYY-MM>`).
- `daily_summary_{YYYY-MM-DD}.json` uploaded alongside the stations snapshot (connector/network counts + OpenAQ provider counts).

Environment:
- `SUPABASE_DB_URL` (required; direct Postgres connection)
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`
- `UK_AQ_DROPBOX_ROOT`
- `UK_AQ_STATIONS_DROPBOX_DIR` (optional)

Error logging:

- Writes JSON error logs to `error_log/<YYYY-MM-DD>/uk_aq_error_<timestamp>_<uuid>.json`.
- Uploads the error log to Dropbox under `<UK_AQ_DROPBOX_ROOT>/error_log/<YYYY-MM-DD>/` when credentials are available.


### `scripts/sensorcommunity/sensorcommunity_list_stations.py`
Purpose:
- Fetch all current Sensor.Community stations for `SCOMM_COUNTRY` (default `GB`).
- Filter to UK bounding box (keeps stations with missing coordinates; `geometry` will be null in Supabase).
- Optional upsert into Supabase.

Common commands:
```
python3 scripts/sensorcommunity/sensorcommunity_list_stations.py
python3 scripts/sensorcommunity/sensorcommunity_list_stations.py --format csv --output uk_sensorcommunity_stations.csv
python3 scripts/sensorcommunity/sensorcommunity_list_stations.py --to-supabase
```

Writes to (when `--to-supabase` is set):
- `connectors`, `stations`
Notes:
- Uses `SCOMM_SERVICE_REF` (defaults to `SCOMM_CONNECTOR_CODE`) for `stations.service_ref`.
- Sets `stations.station_exposure` to `indoor`/`outdoor` when `location.indoor` is present.
- Connector upserts preserve existing `poll_enabled`; new connectors default to `poll_enabled=false`.

### `scripts/sensorcommunity/sensorcommunity_backfill_timeseries_phenomena.py`
Purpose:
- Backfill `timeseries.phenomenon_id` for Sensor.Community rows where it is null.
- Uses `timeseries_ref` suffix mapping (for example `:pm10`, `:pm2.5`) and connector-specific `phenomena` rows.
- Intended for maintenance runs outside ingest hot paths.

Common commands:
```
python3 scripts/sensorcommunity/sensorcommunity_backfill_timeseries_phenomena.py
python3 scripts/sensorcommunity/sensorcommunity_backfill_timeseries_phenomena.py --batch-size 2000
```

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `SCOMM_CONNECTOR_CODE` (optional; defaults to `sensorcommunity`)
- `SCOMM_SERVICE_REF` (optional; defaults to connector code)

### `scripts/sensorcommunity/sensorcommunity_ingest.py`
Purpose:
- Fetch recent Sensor.Community values for `SCOMM_COUNTRY` (default `GB`).
- Read connector + upsert station metadata.
- Insert latest observations for PM10 and PM2.5.

Common commands:
```
python3 scripts/sensorcommunity/sensorcommunity_ingest.py --refresh-recent
python3 scripts/sensorcommunity/sensorcommunity_ingest.py --refresh-recent --raw-output sensorcommunity_raw.json
python3 scripts/sensorcommunity/sensorcommunity_ingest.py --refresh-recent --raw-dropbox
```

Writes to:
- `stations`, `timeseries`, `observations`
Notes:
- Uses `SCOMM_SERVICE_REF` (defaults to `SCOMM_CONNECTOR_CODE`) for `stations.service_ref` and `timeseries.service_ref`.
- Ensures `phenomena` rows for `pm10`/`pm2.5` and sets `timeseries.phenomenon_id`.
- When `SCOMM_INGEST_MET_FIELDS=true`, also ingests `temperature`, `humidity`, and `pressure`.
- `SCOMM_FILE_LOG_LEVEL` controls file log verbosity when raw Dropbox capture is enabled.
- Raw Dropbox uploads are gated by `SCOMM_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (or `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL`).
- Dropbox credentials required: `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`.
- Optional folders: `SCOMM_RAW_DROPBOX_FOLDER` (defaults to `/connectors/sensorcommunity/raw_data`) and
  `SCOMM_ERROR_DROPBOX_FOLDER` (defaults to `/error_log`), with `UK_AIR_*` fallbacks.
- Sets `stations.station_exposure` to `indoor`/`outdoor` when `location.indoor` is present.

### `scripts/uk_air_sos/uk_air_sos_compare.py`
Purpose:
- Fetch DEFRA last-hour readings for a station.
- Compare DEFRA values to the latest Supabase observations for the same station.
- Exit non-zero when mismatches exceed the configured tolerance.

Common commands:
```
python3 scripts/uk_air_sos/uk_air_sos_compare.py
python3 scripts/uk_air_sos/uk_air_sos_compare.py --station-id BR11 --tolerance 1.5
python3 scripts/uk_air_sos/uk_air_sos_compare.py --defra-url "https://uk-air.defra.gov.uk/data/site-data?f_site_id=BR11&view=last_hour"
```

Inputs:
- DEFRA last-hour page (HTML)
- `stations`, `timeseries`, `observations`, `phenomena`

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`

Output:
- Console report per pollutant (PASS/FAIL) with timestamps/units.
- Exit code 0 on success, 1 on mismatch, 2 on fetch/query errors.

### `scripts/uk_aq_dropbox_test.py`
Purpose:
- Validate Dropbox OAuth refresh token and optionally upload a small test file.

Common commands:
```
python3 scripts/uk_aq_dropbox_test.py
python3 scripts/uk_aq_dropbox_test.py --upload
```

Environment:
- `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`
- Optional `UK_AIR_RAW_DROPBOX_FOLDER` (defaults to `/raw_data`)

### `scripts/uk_aq_error_log_archive.py`
Purpose:
- Zip each day of per-error Dropbox logs into `/error_log/YYYY-MM-DD.zip`.
- Delete the original per-error folder after archiving.
- Delete archived ZIPs older than the retention window (default: 365 days).

Common commands:
```
python3 scripts/uk_aq_error_log_archive.py
python3 scripts/uk_aq_error_log_archive.py --date 2026-01-07
```

Environment:
- `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`
- `SUPABASE_URL` + `UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` (must match to run)
- Optional `UK_AIR_ERROR_DROPBOX_FOLDER` (defaults to `/error_log`)

### `scripts/uk_aq_check_error_logs.py`
Purpose:
- Fetch recent `uk_aq_raw.error_logs` rows for debugging edge-function failures.

Common commands:
```
python3 scripts/uk_aq_check_error_logs.py
python3 scripts/uk_aq_check_error_logs.py --source erg_laqn --since-hours 6 --limit 100
```

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- Optional `UK_AQ_RAW_SCHEMA` (defaults to `uk_aq_raw`)

### `scripts/gov_uk_waqn/gov_uk_waqn_ingest.py`
Purpose:
- Placeholder for the Wales Air Quality Network ingest pipeline.

Common commands:
```
python3 scripts/gov_uk_waqn/gov_uk_waqn_ingest.py
```

### `scripts/gov_uk_waqn/gov_uk_waqn_list_stations.py`
Purpose:
- Placeholder for the Wales Air Quality Network station listing.

Common commands:
```
python3 scripts/gov_uk_waqn/gov_uk_waqn_list_stations.py
```

### `scripts/erg_laqn/erg_laqn_list_groups.py`
Purpose:
- List available ERG LAQN group names.

Common commands:
```
python3 scripts/erg_laqn/erg_laqn_list_groups.py
python3 scripts/erg_laqn/erg_laqn_list_groups.py --format json
```

Environment:
- `LAQN_BASE_URL` (optional; defaults to `https://api.erg.ic.ac.uk/AirQuality`)
- `LAQN_USER_AGENT` (optional)

### `scripts/blondon_communities/blondon_communities_ingest.py`
Purpose:
- Ingest Breathe London Communities observations using staged checkpoints in Supabase.
- Pulls IPM25 and INO2 data per site and stores checkpoints in `blondon_communities_timeseries_checkpoints`.

Common commands:
```
python3 scripts/blondon_communities/blondon_communities_ingest.py
python3 scripts/blondon_communities/blondon_communities_ingest.py --initial-days 30 --window-hours 12
python3 scripts/blondon_communities/blondon_communities_ingest.py --limit 5 --dry-run
python3 scripts/blondon_communities/blondon_communities_ingest.py --skip-stations --limit 5 --dry-run --window-hours 1
python3 scripts/blondon_communities/blondon_communities_ingest.py --limit 5 --dry-run --output-timeseries network_info/breathelondon_timeseries.json --output-observations network_info/breathelondon_observations.json --output-checkpoints network_info/breathelondon_checkpoints.json
python3 scripts/blondon_communities/blondon_communities_ingest.py --skip-stations --limit 5 --dry-run --ignore-checkpoints --start-date 2026-01-19T01:00:00Z --window-hours 12
python3 scripts/blondon_communities/blondon_communities_ingest.py --skip-stations --recent-stations --limit 5 --dry-run
```

Environment:
- `BLONDON_COMMUNITIES_API_KEY`
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `BLONDON_COMMUNITIES_BASE_URL` (optional override)
- `BLONDON_COMMUNITIES_CONNECTOR_CODE` / `BLONDON_COMMUNITIES_SERVICE_REF` (optional override)
- `BLONDON_COMMUNITIES_SERVICE_LABEL` (optional override)
- `BLONDON_COMMUNITIES_USER_AGENT` (optional override)

Notes:
- `--skip-stations` skips `ListSensors` and loads station refs from Supabase instead.
- `--output-timeseries` / `--output-observations` write JSON snapshots (best paired with `--limit`).
- `--output-checkpoints` writes the checkpoint rows pulled from Supabase.
- `--ignore-checkpoints` forces backfill even when checkpoints already exist (use for dry-run testing).
- `--recent-stations` picks stations with the most recent `timeseries.last_value_at` when used with `--skip-stations` (falls back to `observations` if needed).
- Updates `connectors.last_polled_at` on successful non-dry runs.

### `scripts/blondon_communities/blondon_communities_batch.py`
Purpose:
- Batch station refs from Supabase and invoke `ingest_breathelondon` per chunk.
- Used by GitHub Actions to avoid edge runtime limits.

Common commands:
```
python3 scripts/blondon_communities/blondon_communities_batch.py --connector-code blondon_communities --service-ref breathelondon --batch-size 10 --active-only --skip-stations
```

Environment:
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `SB_PUBLISHABLE_DEFAULT_KEY`
- `SB_UK_AQ_CRON_SECRET` (optional)
- `BLONDON_COMMUNITIES_CONNECTOR_CODE` (optional override)
- `BLONDON_COMMUNITIES_SERVICE_REF` (optional override)

Notes:
- `--active-only` honors `stations.removed_at is null`.
- `--skip-stations` avoids `ListSensors` and uses the Supabase station list instead.
- Stations are ordered by `blondon_communities_station_checkpoints.last_polled_at` (nulls first), then `next_due_at`.

### `scripts/blondon_communities/blondon_communities_list_stations.py`
Purpose:
- Fetch Breathe London station metadata and optionally upsert stations + metadata in Supabase.

Common commands:
```
python3 scripts/blondon_communities/blondon_communities_list_stations.py
python3 scripts/blondon_communities/blondon_communities_list_stations.py --format csv --output uk_breathelondon_stations.csv
python3 scripts/blondon_communities/blondon_communities_list_stations.py --to-supabase
```

Environment:
- `BLONDON_COMMUNITIES_API_KEY`
- `SUPABASE_URL` (required for `--to-supabase`)
- `SB_SECRET_KEY` (required for `--to-supabase`)
- `BLONDON_COMMUNITIES_BASE_URL` (optional override)
- `BLONDON_COMMUNITIES_CONNECTOR_CODE` / `BLONDON_COMMUNITIES_SERVICE_REF` (optional override)
- `BLONDON_COMMUNITIES_SERVICE_LABEL` (optional override)
- `BLONDON_COMMUNITIES_USER_AGENT` (optional override)

Notes:
- Connector upserts preserve existing `poll_enabled`; new connectors default to `poll_enabled=false`.

### `scripts/blondon_nodes/blondon_nodes_list_stations.py`

- Fetches Breathe London Nodes station metadata from the Nodes `/ListSensors` API.
- Maps Nodes rows to `stations` with connector `blondon_nodes`, service ref `breathelondon`, and the public Breathe London network.
- Uses explicit `Latitude` and `Longitude` fields because sample `Location.geometry.coordinates` is latitude/longitude, not standard GeoJSON longitude/latitude.
- Writes initial source attributes such as `InstallationCode`, `Facility`, `SponsorName`, `PowerTag`, and `SensorContract` to `station_initial_metadata` only for new metadata rows; normal refreshes do not update those rows.
- Preserves `InstallationCode` for later matching to Communities station refs using a `blondon_installation:` match key.

Examples:

```bash
python3 scripts/blondon_nodes/blondon_nodes_list_stations.py --dry-run
python3 scripts/blondon_nodes/blondon_nodes_list_stations.py --input-json network_info/blondon_nodes/list_sensors_sample.json --dry-run
python3 scripts/blondon_nodes/blondon_nodes_list_stations.py --to-supabase
```

Environment:

- `BLONDON_NODES_API_KEY`
- `BLONDON_NODES_BASE_URL` (optional, defaults to `https://breathe-london-7x54d7qf.ew.gateway.dev`)
- `BLONDON_NODES_CONNECTOR_CODE` (optional, must be `blondon_nodes` when set)
- `BLONDON_NODES_SERVICE_REF` (optional, defaults to `breathelondon`)
- `BLONDON_NODES_SERVICE_LABEL` (optional, defaults to `Breathe London`)

### `scripts/uk_aq_invoke_edge.py`
Purpose:
- Invoke Supabase Edge Functions (one at a time) for ad-hoc testing.

Common commands:
```
python3 scripts/uk_aq_invoke_edge.py --function ingest_breathelondon --connector-code blondon_communities
python3 scripts/uk_aq_invoke_edge.py --function ingest_sensorcommunity --connector-code sensorcommunity --payload '{"dry_run":true}'
python3 scripts/uk_aq_invoke_edge.py --function uk_aq_latest --connector-code blondon_communities --method GET --params '{"limit":5}'
```

Environment:
- `SUPABASE_URL`
- `SB_PUBLISHABLE_DEFAULT_KEY`
- `SB_UK_AQ_CRON_SECRET` (required for ingest functions when set in Supabase)

### `scripts/uk_aq_station_duplicate_candidates.py`
Purpose:
- Build pollutant-aware possible duplicate station/timeseries groups from latest station JSON + latest AURN register CSV.
- Uses DB-backed station/timeseries IDs (`uk_aq_core.timeseries`) and writes one long-format CSV for review.

Common commands:
```bash
python3 scripts/uk_aq_station_duplicate_candidates.py
python3 scripts/uk_aq_station_duplicate_candidates.py \
  --distance-m 30 \
  --min-group-size 2
```

Output:
- `plans/uk_aq_station_duplicate_candidates_long.csv`

Notes:
- JSON station rows are expanded to all DB timeseries for that station before duplicate grouping.
- Duplicate groups are pollutant-aware and must contain at least two different connectors.
- Groups are excluded when every row has blank `last_value`.
- Default `--json-root` is derived from `UK_AQ_DROPBOX_ROOT` (with local root from `UK_AQ_DROPBOX_LOCAL_ROOT`, default `~/Dropbox`).

## SOS metadata glossary
- `phenomenon`: The observed property (pollutant/parameter), e.g., NO2, O3, PM2.5.
- `procedure`: The sensor or measurement method used to produce the observation.
- `offering`: A logical grouping of observations, often representing a dataset or station-level collection.

## Keys
- `stations` uses bigint `id` with `station_ref` for upstream identifiers (unique by `connector_id, service_ref, station_ref`).
- `timeseries` uses integer `id` with `timeseries_ref` for upstream identifiers (unique by `connector_id, service_ref, timeseries_ref`).
- `observations` references `timeseries.id` (integer) and uses `(connector_id, timeseries_id, observed_at)` as the primary key.
- `connectors.id` and all `connector_id` FKs are integer. External identifiers that arrive as text (even if numeric) use `*_ref`; internal joins use `*_id`.

### `scripts/codeql_alerts_export.py`
Purpose:
- Export open GitHub CodeQL code-scanning alerts and per-alert instance locations using the REST API.
- Write deterministic local snapshots for batching/remediation planning.

Common commands:
```bash
python3 scripts/codeql_alerts_export.py
python3 scripts/codeql_alerts_export.py --repo ChronicChannel-test/uk-aq-ingest --state open --per-page 100
```

Notes:
- Auth order: `GITHUB_TOKEN`, then `GH_TOKEN`, then `gh auth token` fallback.
- Permission diagnostics are explicit for GitHub API failures (401/403/404) with fine-grained PAT guidance.
- Output defaults to `.codeql/exports/<YYYY-MM-DD>/alerts.json` plus `instances/<alert_number>.json`.

### `scripts/codeql_batch.py`
Purpose:
- Convert exported CodeQL alerts into deterministic remediation batches.
- Sort by severity, then rule ID, then alert number.

Common commands:
```bash
python3 scripts/codeql_batch.py --batch-size 10
python3 scripts/codeql_batch.py --batch-size 10 --max-batches 2
```

Notes:
- Uses `most_recent_instance` when available, else falls back to exported instance files.
- Output defaults to `.codeql/batches/<YYYY-MM-DD>/batch-XX.json`.

### `scripts/codeql_make_task_specs.py`
Purpose:
- Generate per-batch markdown remediation specs to seed follow-up Codex fix tasks.

Common commands:
```bash
python3 scripts/codeql_make_task_specs.py --batches-dir .codeql/batches/<YYYY-MM-DD>
python3 scripts/codeql_make_task_specs.py --batches-dir .codeql/batches/<YYYY-MM-DD> --batch batch-01.json
```

Notes:
- Output defaults to `.codeql/task-specs/<YYYY-MM-DD>/batch-XX.md`.
- Specs include scope, strict change rules, verification steps, and PR instructions.

### `scripts/backup_r2/uk_aq_core_snapshot_to_r2.mjs` (ops repo)
Purpose:
- Export a deterministic daily snapshot of selected `uk_aq_core` tables from ingest DB to R2 History.
- Write per-day `manifest.json` + `checksums.sha256` + table `rows.ndjson.gz` files.
- Skip object writes when the existing day manifest hash already matches.

Repo / workflow:
- Script path: `CIC-test-uk-aq-ops/scripts/backup_r2/uk_aq_core_snapshot_to_r2.mjs`
- Workflow: `CIC-test-uk-aq-ops/.github/workflows/uk_aq_r2_core_snapshot.yml`

Common commands:
```bash
node scripts/backup_r2/uk_aq_core_snapshot_to_r2.mjs \
  --day-utc 2026-03-11 \
  --report-out ./tmp/uk_aq_core_snapshot_to_r2_report.json

node scripts/backup_r2/uk_aq_core_snapshot_to_r2.mjs \
  --dry-run \
  --tables connectors,stations,timeseries
```

Environment:
- `UK_AQ_INGEST_DATABASE_URL` (or `SUPABASE_DB_URL`)
- `CFLARE_R2_ENDPOINT`
- `CFLARE_R2_BUCKET`
- `CFLARE_R2_REGION` (optional; default `auto`)
- `CFLARE_R2_ACCESS_KEY_ID`
- `CFLARE_R2_SECRET_ACCESS_KEY`
- `UK_AQ_R2_HISTORY_CORE_PREFIX` (optional; default `history/v1/core`)

### `scripts/uk_aq_backfill_local_monthly.sh` (ops repo)
Purpose:
- Run local backfill month-by-month over one date window.
- Invoke `workers/uk_aq_backfill_cloud_run/run_job.ts` once per month and store per-month logs.

Repo / docs:
- Script path: `CIC-test-uk-aq-ops/scripts/uk_aq_backfill_local_monthly.sh`
- Usage notes: `CIC-test-uk-aq-ops/system_docs/uk-aq-backfill-cloud-run-script.md`

Common commands:
```bash
export UK_AQ_BACKFILL_RUN_MODE="source_to_r2"
export UK_AQ_BACKFILL_DRY_RUN="false"
export UK_AQ_BACKFILL_FORCE_REPLACE="false"
export UK_AQ_BACKFILL_FROM_DAY_UTC="2025-01-01"
export UK_AQ_BACKFILL_TO_DAY_UTC="2025-12-31"
unset UK_AQ_BACKFILL_CONNECTOR_IDS

./scripts/uk_aq_backfill_local_monthly.sh
```

Notes:
- Leave `UK_AQ_BACKFILL_CONNECTOR_IDS` unset to include all available source adapters.
- With `UK_AQ_BACKFILL_FORCE_REPLACE=false`, existing connector/day outputs are skipped.

## Breathe London Nodes observations

- `scripts/blondon_nodes/blondon_nodes_ingest.py` ingests `connector_code='blondon_nodes'` observations from the Breathe London Nodes `/SensorData` endpoint. Normal runs select active due stations using `uk_aq_raw.blondon_nodes_station_checkpoints`; manual `--site-code`/`--start-time` runs can bypass due filtering. It keeps raw PM2.5/NO2 separate from DAQI/index timeseries, always writes ingest DB observations before the additional Observs path, publishes Pub/Sub observations only to `GCP_OBSERVS_PUBSUB_TOPIC`, maintains non-regressing timeseries first/last values, and emits a machine-readable run summary. Secondary delivery errors do not become station/species checkpoint errors. The Nodes Cloud Run job claims the connector, updates connector runtime fields, and inserts `uk_aq_ingest_runs`.
