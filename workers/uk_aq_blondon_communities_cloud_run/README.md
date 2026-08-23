# UK AQ Breathe London Communities Cloud Run service

This Cloud Run service runs Breathe London Communities ingest in Google Cloud using the
existing `supabase/functions/ingest_breathelondon/index.ts` logic.

It keeps behavior aligned with the Edge function path:

- station/timeseries/observation ingest
- observs dual-write via shared mode (workflow default `OBSERVS_WRITE_MODE=pubsub_only`)
- Dropbox raw/log/error uploads
- connector run status updates
- `uk_aq_ingest_runs` run row insert
- `error_logs` insert on failure

## How it works

1. Service wrapper (`run_service.ts`) invokes the worker (`run_job.ts`) per POST.
2. Worker starts the BL ingest handler locally inside the container.
3. Worker builds payload from connector settings (`poll_window_hours`, `poll_timeseries_batch_size`)
   plus fresh station refs from `uk_aq_core.blondon_communities_select_station_refs`.
4. Worker sends one local POST request (with `x-cron-secret` when configured).
5. Worker parses response and writes run telemetry into main DB.
6. Worker exits non-zero if ingest failed.

When `UK_AQ_SERVICE_EGRESS_METRICS_ENABLED=true`, the wrapper and ingest child
aggregate their explicit PostgREST response measurements under
`ingest.blondon_communities` and flush best-effort batches to the service-metrics
RPC. Main traffic is labelled `ingestdb`; direct Observs traffic is labelled
`obs_aqidb`. Explicit requests bypass the older per-request egress persistence,
and the batch RPC bypasses both collectors, so traffic is not double-counted or
recursively measured.

If no station refs are due, the run is recorded as `skipped` (`no_station_refs`)
and no local ingest call is made.

Dropbox behavior in Cloud Run:
- Log uploads are always attempted when Dropbox credentials are present.
- Raw uploads are gated by `BLONDON_COMMUNITIES_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (or `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL`) matching `SUPABASE_URL`.
- Wrapper-inserted direct failure `error_logs` rows are mirrored into `/error_log/YYYY-MM-DD/` and patch `error_logs.dropbox_path` when Dropbox error logging is enabled.
- File prefixes are `uk_aq_log_cloud_run_*` and `uk_aq_raw_cloud_run_*`.
- Runtime budget in `ingest_breathelondon` is disabled by default in Cloud Run (`BLONDON_COMMUNITIES_DROPBOX_UPLOAD_SOURCE=cloud_run`).
  - Set `BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET=true` to re-enable the edge-style cutoff.
- The Cloud Run wrapper still enforces a hard child-process timeout at 14 minutes, one minute before the default 15-minute Cloud Run service timeout. On timeout it terminates the child process, returns HTTP 504 with `timed_out=true`, and clears the in-process run lock so the next scheduled request is not blocked indefinitely.

## Build and push

```bash
PROJECT_ID="your-project-id"
REGION="europe-west2"
REPO="uk-aq"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/uk-aq-blondon-communities:latest"

docker build -f workers/uk_aq_blondon_communities_cloud_run/Dockerfile -t "${IMAGE}" .
docker push "${IMAGE}"
```

## Cloud Run service deploy

The service allows unauthenticated transport access for Cloudflare, but every
POST must provide `x-uk-aq-dispatch-secret` or `x-uk-aq-upstream-auth` matching
`UK_AQ_EDGE_UPSTREAM_SECRET`. GET remains a health check.

```bash
gcloud run deploy uk-aq-blondon-communities-ingest \
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
- `BLONDON_COMMUNITIES_API_KEY`
- `UK_AQ_EDGE_UPSTREAM_SECRET`

## Optional but recommended

- `OBSERVS_WRITE_MODE` (workflow default: `pubsub_only`)
- `GCP_OBSERVS_PUBSUB_TOPIC` (required for `OBSERVS_WRITE_MODE=pubsub_only`)
- `OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE` (default `500`)
- `OBS_AQIDB_SUPABASE_URL`, `OBS_AQIDB_SECRET_KEY`, `OBS_AQIDB_RPC_SCHEMA` (required when `OBSERVS_WRITE_MODE=direct`; not injected for `pubsub_only`/`outbox_only`)
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`
- `BLONDON_COMMUNITIES_RAW_DROPBOX_ALLOWED_SUPABASE_URL` or `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (raw upload gate only)
- `SB_UK_AQ_CRON_SECRET`
- `BLONDON_COMMUNITIES_REQUEST_PAYLOAD` (JSON object overrides; dynamic connector-derived station/window/batch still apply)
- `BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET` (optional; defaults to `false` in Cloud Run)
- `BLONDON_COMMUNITIES_IN_FLIGHT_TIMEOUT_MINUTES` (default `14`)
- `BLONDON_COMMUNITIES_CLAIM_TIMEOUT_MINUTES` (default `14`)
- `BLONDON_COMMUNITIES_LOCAL_PORT` (default `8000`; local ingest server port, separate from Cloud Run `PORT`)
- `BLONDON_COMMUNITIES_CONNECTOR_CODE` (default `blondon_communities`)
- `BLONDON_COMMUNITIES_SERVICE_REF` (default `breathelondon`; shared Breathe London service family)
- `BLONDON_COMMUNITIES_INGEST_SCRIPT_PATH` (default `/app/runtime/ingest_blondon_communities/index.ts`)
- `UK_AQ_SERVICE_EGRESS_METRICS_ENABLED` (default `false`)
- `UK_AQ_SERVICE_EGRESS_METRICS_SUPABASE_URL` (defaults to
  `OBS_AQIDB_SUPABASE_URL` in the TEST deployment workflow)
- `UK_AQ_SERVICE_EGRESS_METRICS_SCHEMA` (default `uk_aq_public`)
- `UK_AQ_SERVICE_EGRESS_METRICS_RPC` (default
  `uk_aq_rpc_service_egress_metrics_batch_upsert`)
- `UKAQ_ENV_NAME` (default `TEST` in this TEST repository)
- `UK_AQ_SERVICE_EGRESS_METRICS_SB_SECRET_KEY` (required when metrics are
  enabled; the workflow binds the existing OBS/AQI secret)

Any supplied connector code other than `blondon_communities` is rejected. In
particular, the old connector code `breathelondon` is not accepted as an alias.
