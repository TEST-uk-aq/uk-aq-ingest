# uk_aq OpenAQ Cloud Run service

This Cloud Run service runs OpenAQ ingest in Google Cloud using the existing
`supabase/functions/ingest_openaq/index.ts` logic.

## How it works

1. Checks connector state (`poll_enabled`, `scheduler_backend`) in `uk_aq_core.connectors`.
2. Claims the connector via `uk_aq_public.uk_aq_rpc_dispatch_claim`.
3. Selects due station refs using `uk_aq_public.uk_aq_rpc_openaq_select_station_refs`
   with tiered + stale limits derived from connector `batch_size`.
4. Calls local OpenAQ ingest once with scoped `station_refs`.
5. Records run status in `connectors` + `uk_aq_ingest_runs` (+ `error_logs` on failure).
6. Schedules the next run as a one-off Cloud Task at computed due time
   (fallback to a short delay when no due checkpoint is available).
7. Writes observs via shared observs client mode:
   - `OBSERVS_WRITE_MODE=pubsub_only` publishes per-row observs messages to
     Pub/Sub (direct cutover path for this worker).
   - `OBSERVS_WRITE_MODE=outbox_only` keeps main DB outbox behavior.
   - `OBSERVS_WRITE_MODE=direct` performs direct observs RPC writes.

Dropbox behavior in Cloud Run:
- Wrapper-inserted direct failure `error_logs` rows are mirrored into `/error_log/YYYY-MM-DD/` and patch `error_logs.dropbox_path` when Dropbox error logging is enabled.
- Existing OpenAQ log/raw uploads remain controlled by the ingest runtime.
- Shared-budget throttles (`shared_budget_minute_limit` / `shared_budget_hour_limit`) are protective stops, not direct failures. They are persisted in the current `uk_aq_ingest_runs.response_payload.warnings` array and normal OpenAQ log, and are not inserted into `error_logs` or mirrored to `/error_log/YYYY-MM-DD/`.

If no station refs are due, run is recorded as `skipped` (`no_station_refs`) and
the worker only schedules the next check task.
If station refs are selected but do not meet minimum station thresholds
(`OPENAQ_MIN_GAP_STATIONS`, default `1`; `OPENAQ_MIN_NON_GAP_STATIONS`, default
`10`), ingest returns `skipped` with `stations_polled=0`.
If OpenAQ polling is disabled (`poll_enabled=false`), the worker records a
`skipped` no-op run with an `openaq_polling_disabled` warning so the stopped
state is visible without using the failure log path.

## Triggering model

- Primary trigger: one-off Cloud Tasks created by the worker itself.
- Queue reconciliation rule:
  - If an earlier/equal pending OpenAQ task exists, the worker does not enqueue another.
  - If only later pending OpenAQ task(s) exist, the worker deletes them and enqueues the newly computed earlier task.
- Safety trigger: Cloudflare cron scheduler every 30 minutes to recover
  from missed/deleted tasks and to bootstrap if task creation fails.
- A safety invocation exits successfully without ingest when a sufficiently
  recent `succeeded`, `success`, `partial`, or `skipped` ingest-run row exists;
  this guard does not separately verify next-task creation. Normal one-off
  Cloud Tasks remain the primary scheduling path.

## Build and push

```bash
PROJECT_ID="your-project-id"
REGION="europe-west2"
REPO="uk-aq"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/uk-aq-openaq:latest"

docker build -f workers/uk_aq_openaq_cloud_run/Dockerfile -t "${IMAGE}" .
docker push "${IMAGE}"
```

## Cloud Run service deploy

The service allows unauthenticated transport access for Cloudflare, but every
POST must provide `x-uk-aq-dispatch-secret` or `x-uk-aq-upstream-auth` matching
`UK_AQ_EDGE_UPSTREAM_SECRET`. OpenAQ self-created Cloud Tasks use the upstream
header. GET remains a health check.

```bash
gcloud run deploy uk-aq-openaq-ingest \
  --region europe-west2 \
  --image "${IMAGE}" \
  --cpu 0.5 \
  --memory 512Mi \
  --concurrency 1 \
  --max-instances 1 \
  --min-instances 0 \
  --no-cpu-boost \
  --allow-unauthenticated
```

## Required env vars / secrets

- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `OPENAQ_API_KEY`
- `UK_AQ_EDGE_UPSTREAM_SECRET`

## Optional env vars

- `OPENAQ_BASE_URL` (default `https://api.openaq.org/v3`)
- `OPENAQ_CONNECTOR_CODE` (default `openaq`)
- `OPENAQ_SERVICE_REF` (default `openaq`)
- `OPENAQ_DEFAULT_WINDOW_HOURS` (default `6`)
- `OPENAQ_DEFAULT_BATCH_LIMIT` (default `56`)
- `OPENAQ_MAX_REQUESTS_PER_HOUR` (default `1900`; hourly wrapper guard to stay below OpenAQ account cap)
- `OPENAQ_SHARED_BUDGET_ENFORCE` (default `true`; enforce DB-backed shared minute/hour token budget before each OpenAQ API call)
- `OPENAQ_SHARED_BUDGET_KEY` (default `openaq`; shared budget key used across all OpenAQ callers)
- `OPENAQ_SHARED_BUDGET_CALLER` (default `ingest_openaq`; caller label written into budget telemetry)
- `OPENAQ_SHARED_BUDGET_MINUTE_LIMIT` (default `50`; hard shared per-minute cap)
- `OPENAQ_SHARED_BUDGET_HOUR_LIMIT` (default `1500`; hard shared rolling-hour cap)
- `OPENAQ_STALE_LIMIT` (default `4`)
- `OPENAQ_TIER1_RETRY_SECONDS` (default `300`; minimum seconds since `last_polled_at` for tier1 due candidates)
- `OPENAQ_MIN_GAP_STATIONS` (default `1`; minimum selected gap stations needed to run regardless of non-gap count)
- `OPENAQ_MIN_NON_GAP_STATIONS` (default `10`; skip when no gap stations and non-gap selected stations are below this threshold)
- `OPENAQ_IN_FLIGHT_TIMEOUT_MINUTES` (default `30`)
- `OPENAQ_CLAIM_TIMEOUT_MINUTES` (default `30`)
- `OPENAQ_REQUEST_PAYLOAD` (JSON object overrides)
- `OPENAQ_TASKS_ENABLED` (default `true`)
- `OPENAQ_NEXT_CHECK_MIN_SECONDS` (default `60`)
- `OPENAQ_NEXT_CHECK_PARTIAL_MIN_SECONDS` (default `60`; minimum delay floor when run result is `partial`)
- `OPENAQ_NEXT_CHECK_SKIPPED_MIN_SECONDS` (default `60`; minimum delay floor when run result is `skipped`)
- `OPENAQ_RATE_LIMIT_FALLBACK_SECONDS` (default `300`; fallback delay when no OpenAQ reset timestamp is returned)
- `OPENAQ_FAILURE_RETRY_SECONDS` (default `120`)
- `OPENAQ_AUTH_SAFETY_DISABLE_POLLING` (default `true`; auto-disables connector polling on OpenAQ auth 401/403 and skips self-reschedule)
- `OPENAQ_LAG_STAT` (default `min`; options: `min`, `median`, `p25` for non-gap checkpoint lag scheduling)
- `OPENAQ_GCP_PROJECT_ID`, `OPENAQ_GCP_REGION`
- `OPENAQ_CLOUD_RUN_TARGET` (recommended: `service`)
- `OPENAQ_CLOUD_RUN_JOB_NAME` (default `uk-aq-openaq-ingest`)
- `OPENAQ_CLOUD_RUN_SERVICE_NAME` (default falls back to `OPENAQ_CLOUD_RUN_JOB_NAME`)
- `OPENAQ_CLOUD_RUN_SERVICE_URL` (optional explicit service URL; otherwise resolved via Run API)
- `OPENAQ_TASK_QUEUE_ID` (default `uk-aq-openaq-trigger-queue`)
- `OPENAQ_TASK_INVOKER_SERVICE_ACCOUNT` (service account Cloud Tasks uses for authenticated service requests)
- `OPENAQ_LOCAL_PORT` (default `8000`; local ingest server port, separate from Cloud Run `PORT`)
- `OPENAQ_INGEST_SCRIPT_PATH` (default `/app/runtime/ingest_openaq/index.ts`)
- `OPENAQ_DROPBOX_UPLOAD_SOURCE` (default `cloud_run` for this worker)
- `SB_UK_AQ_CRON_SECRET` (if set, local call sends `x-cron-secret`)
- `OBS_AQIDB_SUPABASE_URL`, `OBS_AQIDB_SECRET_KEY`, `OBS_AQIDB_RPC_SCHEMA` (required when `OBSERVS_WRITE_MODE=direct`; not injected for `pubsub_only`/`outbox_only`)
- `OBSERVS_WRITE_MODE` (default in deploy workflow: `pubsub_only`)
- `GCP_OBSERVS_PUBSUB_TOPIC` (required when `OBSERVS_WRITE_MODE=pubsub_only`)
- `OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE` (optional; defaults to `500`)
- `DROPBOX_APP_KEY`, `DROPBOX_APP_SECRET`, `DROPBOX_REFRESH_TOKEN`
- `OPENAQ_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (or `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL`)
- `OPENAQ_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` (optional; falls back to raw allowlist env)
- `UK_AQ_DROPBOX_ROOT`

## Task Queue Reconciliation

- If an earlier/equal pending OpenAQ self-task exists, enqueue is skipped.
- If only later pending OpenAQ self-task(s) exist, those later tasks are deleted and the newly computed earlier task is enqueued.
- If a run returns `rate_limit_reset_at`, any pending self-task scheduled before that reset time is deleted and replaced with a post-reset task.
- Shared-budget reset hints are also honored (`shared_budget_hour_reset_at`, `shared_budget_minute_reset_at`, `shared_budget_retry_after_seconds`) when OpenAQ header reset metadata is absent.
