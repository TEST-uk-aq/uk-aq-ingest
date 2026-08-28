# uk_aq UK-AIR SOS Cloud Run service

This Cloud Run service runs UK-AIR SOS ingest in Google Cloud using the existing
`supabase/functions/ingest_sos/index.ts` logic.

## How it works

1. Checks connector due state in `uk_aq_core.connectors`.
2. Claims the connector via `uk_aq_public.uk_aq_rpc_dispatch_claim`.
3. Selects due SOS station refs with `uk_aq_core.sos_select_station_refs`.
4. Resolves scoped active `timeseries_ids` (`timeseries.ended_at is null`) for those stations and invokes local SOS ingest once.
5. Records run status in `connectors` + `uk_aq_ingest_runs` (+ `error_logs` on failure).
6. Updates `uk_aq_raw.sos_station_checkpoints` after successful/partial runs.
7. Writes observs via shared observs client mode (`OBSERVS_WRITE_MODE`, workflow default `pubsub_only`).
8. Probes UK-AIR SOS upstream availability before per-timeseries polling; when upstream is unavailable (for example HTTP 502), the run exits early with a failed HTTP status instead of logging hundreds of per-timeseries failures.

Dropbox behavior in Cloud Run:
- Wrapper-inserted direct failure `error_logs` rows are mirrored into `/error_log/YYYY-MM-DD/` and patch `error_logs.dropbox_path` when Dropbox error logging is enabled.
- Existing SOS log/raw/error uploads from the local ingest runtime still use `SOS_DROPBOX_UPLOAD_SOURCE=cloud_run`.

Run feed note:
- If the ingest response omits `last_observed_at`, the worker derives it from
  `max(timeseries.last_value_at)` across the run's selected timeseries ids.
- Run payloads distinguish `stations_selected`, `stations_attempted`, and
  `stations_polled`; the last is the distinct station count backed by a
  successful current-run timeseries checkpoint.
- Station batch note:
  - Station selection is independently capped by `SOS_STATION_BATCH_LIMIT`
    (default `500`) or an explicit request `station_batch_limit` override.
  - `connectors.poll_timeseries_batch_size` independently caps acquired
    timeseries and does not determine the station limit.
  - The station limit is a total cap across tier1, tier2, and stale picks.
    `SOS_STALE_LIMIT` reserves minimum stale progress; stale work may fill more
    unused places when ordinary work under-fills the total cap.

If no station refs are due, run is recorded as `skipped` (`no_station_refs`).
If station refs are selected but no timeseries are found, run is `skipped` (`no_timeseries_ids`).

## Edge compatibility

- Edge SOS path is unchanged and still uses
  `uk_aq_core.sos_select_timeseries_ids` +
  `uk_aq_raw.sos_timeseries_checkpoints`.
- Cloud Run SOS path adds station-level scheduling only for
  `scheduler_backend='google_cloud_run'`.

## Build and push

```bash
PROJECT_ID="your-project-id"
REGION="europe-west2"
REPO="uk-aq"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/uk-aq-sos:latest"

docker build -f workers/uk_aq_sos_cloud_run/Dockerfile -t "${IMAGE}" .
docker push "${IMAGE}"
```

## Cloud Run service deploy

The service allows unauthenticated transport access for Cloudflare, but every
POST must provide `x-uk-aq-dispatch-secret` or `x-uk-aq-upstream-auth` matching
`UK_AQ_EDGE_UPSTREAM_SECRET`. GET remains a health check.

```bash
gcloud run deploy uk-aq-sos-ingest \
  --region europe-west2 \
  --image "${IMAGE}" \
  --cpu 0.25 \
  --memory 256Mi \
  --concurrency 1 \
  --max-instances 1 \
  --min-instances 0 \
  --no-cpu-boost \
  --allow-unauthenticated
```

## Required env vars / secrets

- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `UK_AQ_EDGE_UPSTREAM_SECRET`

## Optional env vars

- `SOS_BASE_URL`
- `SOS_SERVICE_LABEL`
- `SOS_CONNECTOR_CODE` (default `sos`)
- `SOS_DEFAULT_INTERVAL_MINUTES` (default `60`)
- `SOS_IN_FLIGHT_TIMEOUT_MINUTES` (default `30`)
- `SOS_CLAIM_TIMEOUT_MINUTES` (default `30`)
- `SOS_DEFAULT_WINDOW_HOURS` (default `6`)
- `SOS_DEFAULT_TIMESERIES_LIMIT` (default `100`)
- `SOS_STATION_BATCH_LIMIT` (default `500`)
- `SOS_STALE_LIMIT` (default `4`)
- `SOS_INGEST_SCRIPT_PATH` (default `/app/runtime/ingest_sos/index.ts`)
- `SOS_MAX_RUNTIME_SECONDS` (ingest runtime budget inside handler)
- `SOS_LOCAL_PORT` (default `8000`; local ingest server port, separate from Cloud Run `PORT`)
- `SB_UK_AQ_CRON_SECRET` (if set, local call sends `x-cron-secret`)
- `OBSERVS_WRITE_MODE` (workflow default: `pubsub_only`)
- `GCP_OBSERVS_PUBSUB_TOPIC` (required for `OBSERVS_WRITE_MODE=pubsub_only`)
- `OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE` (default `500`)
- `OBS_AQIDB_SUPABASE_URL`, `OBS_AQIDB_SECRET_KEY`, `OBS_AQIDB_RPC_SCHEMA` (required when `OBSERVS_WRITE_MODE=direct`; not injected for `pubsub_only`/`outbox_only`)
- `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`
- `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL`
- `SOS_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` or `UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL`
- `UK_AQ_DROPBOX_ROOT`, `UK_AIR_RAW_DROPBOX_FOLDER`
