# uk_aq Sensor.Community Cloud Run service

This worker runs Sensor.Community ingest directly in Cloud Run Service
(without calling the Supabase Edge function).

## Behavior

- Reads `uk_aq_core.connectors` for `sensorcommunity`.
- Runs only when:
  - `poll_enabled = true`
  - `scheduler_backend = 'google_cloud_run'`
  - run is due based on `poll_interval_minutes` (`last_run_start`/`last_polled_at` anchor)
- Claims dispatch with `uk_aq_public.uk_aq_rpc_dispatch_claim`.
- Fetches Sensor.Community data directly from `data.sensor.community`.
- Upserts stations, phenomena, timeseries, and observations directly via PostgREST.
- Dual-writes observations to observs DB (with main DB outbox fallback) when observs env is configured.
- Supports `OBSERVS_WRITE_MODE=pubsub_only` to publish observs rows directly to GCP Pub/Sub.
- Normalizes and deduplicates observs observation rows on `(connector_id, timeseries_id, observed_at)` before observs upsert/outbox enqueue.
- Uploads run log + raw payload snapshot to Dropbox when Dropbox env/secrets are configured and allowed for the active Supabase URL.
  - Log artifact: `uk_aq_log_cloud_run_scomm_<timestamp>.json`
  - Raw artifact: `uk_aq_raw_cloud_run_scomm_<timestamp>.zip`
  - On source-fetch failure, the log payload now includes `payload.details` with fetch context such as source URL, retry count, timeout, final attempt, and HTTP status or transport error.
  - On source-fetch failure before rows are downloaded, the raw Dropbox artifact still includes the attempted source URL and fetch error details.
- Mirrors the inserted `uk_aq_raw.error_logs` row for direct ingest failures into `{UK_AQ_DROPBOX_ROOT}/error_log/YYYY-MM-DD/` when Dropbox error logging is enabled, and patches `error_logs.dropbox_path`.
- Evaluates failure monitor rules from recent `uk_aq_ingest_runs` history:
  - consecutive server-error streak threshold (`SCOMM_ALERT_CONSECUTIVE_500_THRESHOLD`, default `3`)
  - lookback failure-rate threshold (`SCOMM_ALERT_FAILURE_RATE_THRESHOLD`, default `0.5`) over `SCOMM_ALERT_FAILURE_RATE_LOOKBACK_MINUTES` (default `60`)
- On rule threshold crossing, inserts a warning row in `uk_aq_raw.error_logs` and (when Dropbox error logging is enabled) uploads alert JSON to:
  - `{UK_AQ_DROPBOX_ROOT}/error_log/YYYY-MM-DD/`
- Writes run status back to `connectors` and inserts `uk_aq_ingest_runs` row.
- Inserts `error_logs` row on ingest failure.

The previous proxy worker (Cloud Run -> Supabase Edge function) is archived at:
`archive/2026-02-11/workers/uk_aq_sensorcommunity_cloud_run/index.proxy_edge_invoker.mjs`

## Required env vars

- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `UK_AQ_EDGE_UPSTREAM_SECRET`
- `UK_AQ_CORE_SCHEMA` (optional; default `uk_aq_core`)
- `UK_AQ_RAW_SCHEMA` (optional; default `uk_aq_raw`)
- `OBS_AQIDB_SUPABASE_URL` (required when `OBSERVS_WRITE_MODE=direct`; optional for `pubsub_only`/`outbox_only`)
- `OBS_AQIDB_SECRET_KEY` (required when `OBSERVS_WRITE_MODE=direct`; not injected for `pubsub_only`/`outbox_only`)
- `OBS_AQIDB_RPC_SCHEMA` (optional; default `uk_aq_public`; used for direct mode RPC profile)
- `DROPBOX_APP_KEY` (required for Dropbox upload)
- `DROPBOX_APP_SECRET` (required for Dropbox upload)
- `DROPBOX_REFRESH_TOKEN` (required for Dropbox upload)
- `SCOMM_RAW_DROPBOX_ALLOWED_SUPABASE_URL` or `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL`
  must match `SUPABASE_URL` for Dropbox upload to be enabled.

## Optional env vars (existing)

- `SCOMM_COUNTRY` (default `GB`)
- `SCOMM_BASE_URL` (default `https://data.sensor.community`)
- `SCOMM_SERVICE_REF` (default `sensorcommunity`)
- `SCOMM_USER_AGENT` (default `uk-air-quality-networks`)
- `SCOMM_INGEST_MET_FIELDS` (default `false`)
- `SCOMM_DEFAULT_INTERVAL_MINUTES` (default `15`)
- `SCOMM_IN_FLIGHT_TIMEOUT_MINUTES` (default `30`)
- `SCOMM_CLAIM_TIMEOUT_MINUTES` (default `30`)
- `SCOMM_HTTP_TIMEOUT_MS` (default `60000`)
- `SCOMM_SOURCE_TIMEOUT_MS` (default `90000`)
- `SCOMM_SOURCE_RETRIES` (default `3`)
- `SCOMM_UPSERT_CHUNK_SIZE` (default `500`)
- `SCOMM_TRIGGER_MODE` (default `manual`; set by service wrapper for observability)
- `OBSERVS_UPSERT_RPC` (default `uk_aq_rpc_observs_observations_compact_upsert_v1`)
- `OBSERVS_UPSERT_CHUNK_SIZE` (default `5000`)
- `OBSERVS_WRITE_MODE` (default `outbox_only`; supports `outbox_only`, `direct`, `pubsub_only`)
- `GCP_OBSERVS_PUBSUB_TOPIC` (required when `OBSERVS_WRITE_MODE=pubsub_only`)
- `OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE` (default `500`; publish chunk size when `OBSERVS_WRITE_MODE=pubsub_only`)
- `UK_AQ_DROPBOX_ROOT`
- `SCOMM_RAW_DROPBOX_FOLDER` or `UK_AIR_RAW_DROPBOX_FOLDER`
  (default `/connectors/sensorcommunity/raw_data`)
- `SCOMM_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` or `UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL`
  (optional; defaults to raw allowlist env value)
- `SCOMM_ERROR_DROPBOX_FOLDER` or `UK_AIR_ERROR_DROPBOX_FOLDER`
  (default `/error_log`)
- `SCOMM_ALERT_CONSECUTIVE_500_THRESHOLD` (default `3`)
- `SCOMM_ALERT_FAILURE_RATE_LOOKBACK_MINUTES` (default `60`)
- `SCOMM_ALERT_FAILURE_RATE_THRESHOLD` (default `0.5`; must be between `0` and `1`)
- `SCOMM_ALERT_FAILURE_RATE_MIN_RUNS` (default `3`)
- `SCOMM_ALERT_RUN_SAMPLE_LIMIT` (default `240`)

## Build image

```bash
PROJECT_ID="your-gcp-project"
REGION="europe-west2"
REPO="uk-aq"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/uk-aq-scomm:latest"

cd workers/uk_aq_sensorcommunity_cloud_run
gcloud builds submit --tag "${IMAGE}" .
```

## Create/update Cloud Run Service

The service allows unauthenticated transport access for Cloudflare, but every
POST must provide `x-uk-aq-dispatch-secret` or `x-uk-aq-upstream-auth` matching
`UK_AQ_EDGE_UPSTREAM_SECRET`. GET remains a health check.

```bash
PROJECT_ID="your-gcp-project"
REGION="europe-west2"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/uk-aq/uk-aq-scomm:latest"

# Create or update
gcloud run deploy uk-aq-scomm-ingest \
  --region "${REGION}" \
  --image "${IMAGE}" \
  --service-account "uk-aq-scomm-job@${PROJECT_ID}.iam.gserviceaccount.com" \
  --cpu "1" \
  --memory "512Mi" \
  --timeout "600" \
  --concurrency "1" \
  --max-instances "1" \
  --min-instances "0" \
  --no-cpu-boost \
  --allow-unauthenticated
```

## Manual trigger

```bash
REGION="europe-west2"
SERVICE_URL="https://uk-aq-scomm-ingest-<hash>-nw.a.run.app"

TOKEN="$(gcloud auth print-identity-token)"
curl -i -X POST "${SERVICE_URL}" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "X-UK-AQ-Upstream-Auth: ${UK_AQ_EDGE_UPSTREAM_SECRET}" \
  -H "Content-Type: application/json" \
  -d '{"trigger_mode":"manual"}'
```
