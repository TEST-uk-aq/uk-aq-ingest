# CIC UK Air Quality Networks

Tools for ingesting UK-AIR SOS data into Supabase.

## Website
The static web UI now lives in the `uk-aq` repo (under `CIC Website/uk-aq`).
This repo focuses on ingest, data management, and Supabase Edge Functions.

## Prerequisites
- Python 3.10+
- Supabase project with the shared schema applied from the `uk-aq-schema` repo (`schemas/uk_air_quality_schema.sql`).
  - This schema uses internal ids; `connectors.id` and `timeseries.id` (and their `connector_id`/`timeseries_id` FKs) are integer, while other ids may still be bigint. External identifiers stored as text use `_ref` (even if numeric).

## Setup
Create a `.env` file in the repo root with:

```
SUPABASE_URL=your_supabase_url
SB_SECRET_KEY=your_service_role_key
# Optional override (default shown)
SOS_BASE_URL=https://uk-air.defra.gov.uk/sos-ukair/api/v1
# Optional override for the service label
SOS_SERVICE_LABEL=SOS
# Legacy support: UK_AIR_BASE_URL, UK_AIR_SERVICE_LABEL, and UKAIR_BASE_URL also work
```

`.env` is local-only. Keep it out of git and mirror the same values in GitHub Secrets/Vars so CI matches your local runs.

Env quick reference (Supabase blocks secrets prefixed with `SUPABASE_`):

| Context | Required | Optional |
| --- | --- | --- |
| Local scripts (.env) | `SUPABASE_URL`, `SB_SECRET_KEY` | `SOS_BASE_URL`, `SOS_SERVICE_LABEL` |
| Edge function runtime (Supabase secrets) | `SB_SUPABASE_URL`, `SB_SECRET_KEY` | `SOS_BASE_URL`, `SOS_SERVICE_LABEL`, `OBS_AQIDB_SUPABASE_URL`, `OBS_AQIDB_SECRET_KEY`, `OBSERVS_UPSERT_RPC`, `OBSERVS_OUTBOX_FLUSH_LIMIT`, `OBSERVS_UPSERT_CHUNK_SIZE`, `UK_AQ_EGRESS_LOG_SAMPLE_RATE`, `UK_AQ_EGRESS_METRICS_DB_ENABLED`, `UK_AQ_EGRESS_METRICS_CLEANUP_SAMPLE_RATE`, `UK_AQ_EGRESS_METRICS_CLEANUP_MIN_INTERVAL_MS`, `UK_AQ_EGRESS_METRICS_AGG_RETENTION_DAYS`, `UK_AQ_EGRESS_METRICS_RAW_RETENTION_DAYS`, `DISPATCH_TIME_BUDGET_MS`, `DISPATCH_SHUTDOWN_BUFFER_MS`, `DISPATCH_EDGE_CALL_TIMEOUT_MS` |
| GitHub Actions deploy | `SUPABASE_ACCESS_TOKEN`, `SUPABASE_URL`, `SB_SECRET_KEY`, `SUPABASE_PROJECT_REF` (Secrets) | `SOS_BASE_URL`, `SOS_SERVICE_LABEL` (Secrets) |

Install dependencies in a virtual environment:

```
python3 -m venv .venv
source .venv/bin/activate
pip install requests python-dotenv supabase
```

## Testing
- Install dev tools: `pip install -r requirements-dev.txt` (contains pytest + mocks).
- Run mocked/unit tests (no network): `pytest`
- Run live SOS integration tests: `UKAIR_LIVE=1 pytest -m live` (read-only; skips by default)
- Optional DB writes (should stay off for tests): `UKAIR_WRITE_DB=1` (default is no writes)

## Run the UK-AIR SOS ingestion
Discover stations and timeseries, then backfill 2025:

```
python3 scripts/sos/sos_ingest.py --discover --backfill-2025
```

Refresh the last N hours (default 6h):

```
python3 scripts/sos/sos_ingest.py --refresh-recent --hours 6
```

Optional backfill chunk size (days):

```
python3 scripts/sos/sos_ingest.py --backfill-2025 --chunk-days 14
```

## Notes
- Filters are configurable in `scripts/sos/sos_ingest.py` (bbox, region, station type, pollutants).
- The script upserts into `connectors`, `stations`, `timeseries`, `observations`, and reference tables.

## Edge function polling (optional)
For continuous updates, deploy the Edge Function in `supabase/functions/ingest_sos`.
Deploying the Edge Function does not create a schedule; helper RPCs live in `supabase/uk_aq_polling_helpers.sql`.

Supabase secrets required (Edge Function runtime):
```
SB_SUPABASE_URL=your_supabase_url
SB_SECRET_KEY=your_service_role_key
SOS_BASE_URL=https://uk-air.defra.gov.uk/sos-ukair/api/v1
SOS_SERVICE_LABEL=SOS
```

Helper RPC SQL for the poller lives in `supabase/uk_aq_polling_helpers.sql`.
Endpoint egress metrics SQL (minute aggregates + error/304 raw events) lives in `supabase/uk_aq_egress_metrics.sql`.

GitHub Actions deployment secrets (used by `.github/workflows/supabase_edge_deploy.yml`):
- `SUPABASE_ACCESS_TOKEN`
- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `SUPABASE_PROJECT_REF`

Note: `SUPABASE_ACCESS_TOKEN` is only required for deployments (GitHub Actions or `supabase` CLI). The publishable key is safe to expose; the service role key is not.

## Fresh DB setup (dual-write)
Use this flow when creating fresh MAIN + HISTORY projects.

1. MAIN DB project:
   - In the schema repo (`../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/ingest_db`), run/paste:
     - `uk_aq_core_schema.sql`
     - `uk_aq_raw_schema.sql`
     - `uk_aq_pop_schema.sql`
     - `uk_aq_rpc.sql`
     - `uk_aq_public_views.sql`
     - `uk_aq_security.sql`
     - `main_db_dualwrite_bootstrap.sql`
       - Includes Phase B backup ops objects (`uk_aq_ops.backup_candidates`, `uk_aq_ops.prune_day_gates`) and server-side backup projection function `uk_aq_ops.uk_aq_phase_b_backup_rows(...)`.
   - In this repo, run/paste:
     - `supabase/uk_aq_polling_helpers.sql`
2. HISTORY DB project:
   - In the schema repo (`../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/obs_aqi_db`), run/paste:
     - `uk_aq_obs_aqi_db_schema.sql`
     - `uk_aq_obs_aqi_db_dualwrite_bootstrap.sql`
   - History observations are keyed by `(connector_id, timeseries_id, observed_at)`.
3. Set MAIN runtime secrets:
   - `OBS_AQIDB_SUPABASE_URL`
   - `OBS_AQIDB_SECRET_KEY`
   - Optional overrides:
     - `OBSERVS_UPSERT_RPC` (default `uk_aq_rpc_observs_observations_upsert`)
     - `OBSERVS_OUTBOX_FLUSH_LIMIT` (default `40`)
     - `OBSERVS_UPSERT_CHUNK_SIZE` (default `5000`)
     - `OBSERVS_OUTBOX_CLOUD_RUN_MAX_BATCHES` (Cloud Run outbox batches per run; default `30`)
     - `OBSERVS_OUTBOX_CLOUD_RUN_CLAIM_BATCH_LIMIT` (Cloud Run claim size per batch; default `20`)
     - `OBSERVS_OUTBOX_CLOUD_RUN_BUDGET_SECONDS` (Cloud Run per-run budget; default `540`)
4. Operational notes:
   - Outbox retries history delivery without backfill exports.
   - `uk_aq_raw.history_sync_receipt_daily` records per-day delivery receipts for future safe retention deletes.

## Station Snapshot Dashboard (local only)
Local dashboard entrypoint:
```
python3 scripts/uk_aq_station_snapshot_local.py --port 8046
```

Required runtime values:
- `SUPABASE_URL` (or pass `--edge-url` directly)
- `UK_AQ_DEV_JWT` or `UK_AQ_DEV_REFRESH_TOKEN`

Optional:
- `UK_AQ_STATION_SNAPSHOT_EDGE_URL` to override the edge URL
- `UK_AQ_DEV_REFRESH_TOKEN` to auto-refresh expired access tokens
- `UK_AQ_DEV_ENV_FILE` to persist rotated refresh tokens (default `.env.supabase`)

Issue fresh dashboard auth tokens:
```
python3 scripts/uk_aq_issue_dev_auth_tokens.py --write-env-file .env.supabase
```
This updates:
- `UK_AQ_DEV_JWT`
- `UK_AQ_DEV_REFRESH_TOKEN`
- `UK_AQ_DEV_JWT_EXPIRES_AT`

The page calls the protected edge function:
- Path: `supabase/functions/uk_aq_station_snapshot`
- Query params:
  - `station_id` or `station_ref` (one required)
  - `timeseries_id` (optional)
  - `window=6h|24h|7d` (default `6h`)
  - `obs_limit=100|1000` (default `100`)
- Authorization:
  - `Authorization: Bearer <JWT>` required (provided by the local server from `UK_AQ_DEV_JWT`)
  - If `UK_AQ_DEV_REFRESH_TOKEN` is set, the local server refreshes the access token on demand.

Response shape:
```json
{
  "station": {},
  "timeseries": [],
  "stations_checkpoints": [],
  "timeseries_checkpoints": [],
  "selected_timeseries_id": 123,
  "observations": [],
  "meta": {
    "window": "6h",
    "window_start": "2026-02-04T10:00:00Z",
    "window_end": "2026-02-04T16:00:00Z",
    "obs_limit": 100,
    "default_timeseries_rule": "lowest_timeseries_id_for_station"
  }
}
```

## Run Both Local Dashboards On-Demand
Start both servers with one command:
```
./dev_dashboards.sh
```

Stop both servers cleanly:
```
./dev_dashboards_stop.sh
```

Required environment variables:
- `SUPABASE_URL`
- `SB_PUBLISHABLE_DEFAULT_KEY`
- `UK_AQ_DEV_JWT` or `UK_AQ_DEV_REFRESH_TOKEN`

Override host/ports:
```
HOST=0.0.0.0 SCHEDULER_PORT=9000 SNAPSHOT_PORT=9001 ./dev_dashboards.sh
```

Logs:
- `logs/scheduler.log`
- `logs/station_snapshot.log`

## Environment naming convention
For new networks, use `NETWORK_BASE_URL` and `NETWORK_SERVICE_LABEL`.
Examples:
- `SOS_BASE_URL`, `SOS_SERVICE_LABEL`
- `SCOMM_BASE_URL`, `SCOMM_SERVICE_LABEL` (Sensor.Community)
