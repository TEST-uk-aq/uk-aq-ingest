# UK AQ Edge Functions

This project uses Supabase Edge Functions for polling and serving data. The Edge
functions run inside Supabase and need their own environment variables (Project
Settings -> Functions -> Environment Variables). They do not read the local .env.

History dual-write note: shared history writes normalize `OBS_AQIDB_RPC_SCHEMA`
values `uk_aq_history` and `public` to `uk_aq_public` for RPC calls, because
history RPCs are exposed from `uk_aq_public`.
History dual-write write-path note: rows are normalized and deduplicated on
`(connector_id, timeseries_id, observed_at)` before history upsert and outbox
enqueue to reduce duplicate payload bytes, write churn, and request overhead.
OpenAQ/Sensor.Community pre-write note: ingest write payloads now run an exact
duplicate pass on `(connector_id, timeseries_id, observed_at, value, status)`
before both main observations write and history enqueue/publish; changed
`value`/`status` at the same timestamp are preserved.
History write mode note: shared history writes now default to
`OBSERVS_WRITE_MODE=outbox_only` (enqueue on main DB, flush asynchronously via
outbox worker). Set `OBSERVS_WRITE_MODE=direct` only when immediate history
writes are required. `OBSERVS_WRITE_MODE=pubsub_only` publishes history rows to
GCP Pub/Sub for asynchronous writer jobs (for example OpenAQ Cloud Run direct
cutover).
History outbox flush note: claimed outbox payloads are merged per flush batch
before a single history upsert call, which cuts history RPC call count and
egress overhead.
History value precision note: history write payloads now carry
`value_float8_hex` (bit-exact float representation) alongside `value` across
outbox/pubsub/direct routes. Writers and upsert paths prefer this field when
present so history values can be restored byte-identically.

Endpoint egress observability note: public read endpoints emit sampled egress
metrics and persist them via RPCs defined in `supabase/uk_aq_egress_metrics.sql`
(`uk_aq_record_endpoint_metric`, `uk_aq_cleanup_endpoint_metrics`).
PostgREST egress capture note: all edge functions and Cloud Run worker entrypoints
import `_shared/fetch_egress_patch.ts`, which instruments outgoing
`/rest/v1/*` calls and records response size + duration metrics as
`postgrest:<path>` endpoint rows via the same RPC. `2xx` capture defaults to
full capture (`UK_AQ_POSTGREST_EGRESS_CAPTURE_SAMPLE_RATE=1`); `304`/`4xx`/`5xx`
remain always logged.
Multi-project capture note: PostgREST capture supports multiple Supabase origins
via `UK_AQ_POSTGREST_EGRESS_CAPTURE_URLS` (comma-separated) and automatically
includes `SUPABASE_URL`/`SB_SUPABASE_URL` and `OBS_AQIDB_SUPABASE_URL` when set.
Caller attribution note: PostgREST metric endpoints now include caller tags
when available (`postgrest:<path>|caller=<function_name>`), and outgoing
PostgREST requests from major edge functions set `x-ukaq-egress-caller` so
egress can be attributed by function.

Maintenance note (2026-02-09): removed `@ts-nocheck` from ingest/stations edge
functions and fixed strict typing/lint issues without changing runtime behavior.

## Functions

### uk_aq_dispatch_polls
- Purpose: Two-stage dispatcher endpoint.
  - `mode=enqueue` selects due connectors and enqueues them.
  - `mode=run_queue` claims queued work and executes ingest for queued connectors.
  - `mode=legacy` keeps the previous direct-dispatch behavior.
  - If `mode` is omitted, defaults to `legacy` for backward compatibility with single-call schedulers.
- Triggered by: External scheduler (Cloudflare Worker cron) calling the edge function directly.
- Flow reference: `system_docs/uk_aq_cloudflare_scheduler_ingest_flow.md`
- Reads:
  - `connectors` (`poll_enabled`, `poll_interval_minutes`, `poll_window_hours`, `poll_timeseries_batch_size`, `scheduler_backend`, `last_polled_at`)
  - `dispatcher_settings` (`max_runs_per_dispatch_call` is the effective concurrency setting)
- Queue tables/RPCs (from `supabase/uk_aq_polling_helpers.sql`):
  - `uk_aq_raw.dispatch_connector_queue`
  - `uk_aq_core.uk_aq_dispatch_queue_enqueue`
  - `uk_aq_core.uk_aq_dispatch_queue_claim`
  - `uk_aq_core.uk_aq_dispatch_queue_resolve`
- Station batch helpers: `blondon_communities_select_station_refs`, `erg_laqn_select_station_refs` (defined in `supabase/uk_aq_polling_helpers.sql`)
- Calls:
  - In `mode=run_queue` only:
  - `ingest_sos` (`window_hours`)
  - `ingest_sensorcommunity` (`country=GB`)
  - `ingest_openaq` (`window_hours`, `batch_size` from `connectors.poll_timeseries_batch_size`, default 56)
  - `ingest_breathelondon` (`station_refs`, `window_hours`, `initial_days=2`, `skip_stations=true`)
  - `ingest_erg_laqn` (`station_refs`, `days=ceil(poll_window_hours/24)`, `group=London`)
- Notes:
  - Requires `X-Cron-Secret` when `SB_UK_AQ_CRON_SECRET` is set.
  - Uses `SB_SECRET_KEY` (preferred) with fallback to `SUPABASE_SERVICE_ROLE_KEY` for internal PostgREST reads/writes.
  - Calls ingest functions with `SB_PUBLISHABLE_DEFAULT_KEY` (or `SB_SECRET_KEY` fallback) plus `X-Cron-Secret`; `verify_jwt=false` is set for dispatcher/ingest functions.
  - Uses a runtime budget guard to avoid platform timeout overruns:
    - `DISPATCH_TIME_BUDGET_MS` (default `150000`)
    - `DISPATCH_SHUTDOWN_BUFFER_MS` (default `10000`)
    - `DISPATCH_EDGE_CALL_TIMEOUT_MS` (default `140000`)
    - `DISPATCH_MIN_START_EDGE_CALL_MS` (default `30000`; minimum remaining budget required before starting the next child ingest call)
    - Tiny/invalid ms values are ignored and reset to defaults (minimums: budget `>=6000`, shutdown buffer `>=1000`, edge timeout `>=5000`).
  - If `DISPATCH_SHUTDOWN_BUFFER_MS` is set too high, dispatcher now clamps it to keep at least one edge-call timeout window available (prevents no-op runs that skip outbox drain with `dispatch_time_budget`).
  - Only enqueues/dispatches connectors with `poll_enabled=true` (null/false are skipped).
  - `mode=enqueue` selects oldest due connectors by dispatch anchor (`last_run_start` fallback `last_polled_at`, null first).
    - Enqueues up to `max_runs_per_dispatch_call` connectors per call.
- In-flight behavior:
  - `mode=enqueue` skips global dispatch if any connector is in-flight when `max_runs_per_dispatch_call=1`.
  - Simple guard applies for all connectors:
    - If `last_run_end` is null and `last_run_start` is recent, do not enqueue/dispatch again.
    - If a run started within the connector interval, do not dispatch again yet.
  - `mode=run_queue` claims queued jobs using per-request `run_queue_claim_limit` (or `DISPATCH_QUEUE_CLAIM_BATCH_LIMIT` default) and then uses `uk_aq_rpc_dispatch_claim` for per-connector in-flight safety.
  - Safety guard: when `mode=run_queue` finds an empty queue but connectors are currently due, dispatcher returns `409 queue_empty_with_due_connectors` and writes a warning to `error_logs` (prevents silent stalls from run_queue-only scheduling).
  - In `mode=run_queue`, queued jobs blocked by this guard are resolved with retry (`in_flight_running` or `started_within_interval`) instead of starting an overlap run.
  - When `max_runs_per_dispatch_call>1`, in-flight checks are per connector; other connectors can still dispatch.
  - Stale in-flight runs (>10 minutes) are auto-closed as `failed` with `in_flight_timeout` and a `uk_aq_ingest_runs` row is inserted.
  - Loads latest run state via `uk_aq_rpc_latest_ingest_runs` (lookback-bounded) so dispatcher reads one row per connector instead of broad `uk_aq_ingest_runs` scans.
  - If `uk_aq_rpc_latest_ingest_runs` is unavailable, falls back to a bounded `uk_aq_ingest_runs` read (`run_started_at >= now()-lookback`, small limit).
  - If a connector has `last_run_end` null but the latest `uk_aq_ingest_runs` row has `run_ended_at`, the connector row is reconciled as `ingest_runs_reconciled`.
  - Cloudflare worker cron runs every 1 minute (`workers/uk_aq_ingest_poller/wrangler.toml`) and calls:
    - `mode=enqueue` then
    - `mode=run_queue` fan-out calls in parallel, with fan-out count from `max_runs_per_dispatch_call`
  - Worker sends `run_queue_claim_limit=1` with each `mode=run_queue` call to isolate each claim/call.
  - Worker fallback: if either queue-mode call fails, worker falls back to `mode=legacy` for that cron tick.
  - Disabled connectors are auto-resolved from queue in `mode=run_queue` (`queue_entry_disabled_connector`) so stale retries do not keep firing after `poll_enabled=false`.
  - Connectors with `scheduler_backend='google_cloud_run'` are skipped in dispatcher selection and auto-resolved from queue in `mode=run_queue` (`queue_entry_external_scheduler`) for the Cloud Run allowlist connectors (`sos`, `sensorcommunity`, `blondon_communities`, `openaq`).
  - For `sos` on the edge path (`scheduler_backend='supabase_function'`), dispatcher uses `poll_timeseries_batch_size` with `sos_select_timeseries_ids` (`sos_timeseries_checkpoints`) and passes `timeseries_ids`/`timeseries_limit`.
  - Uses `uk_aq_public.uk_aq_rpc_dispatch_claim` to atomically claim a connector slot before dispatch.
  - Updates `connectors.last_run_start`, `last_run_end`, `last_run_status`, `last_run_message`, and `last_polled_at` for each attempted dispatch.
  - Inserts per-run summaries into `uk_aq_ingest_runs` (status, counts, last_observed_at, response payload) for dashboard feeds.
  - In ingest DB, run-log retention is 30 days via
    `uk_aq_public.uk_aq_rpc_ingest_runs_cleanup`, scheduled by
    `pg_cron` job `uk_aq_ingest_runtime_metrics_cleanup_daily`.
  - Dispatcher write calls to PostgREST (`connectors`, `uk_aq_ingest_runs`, `error_logs`) now use `Prefer: return=minimal` to reduce PostgREST egress.
  - Stored `uk_aq_ingest_runs.response_payload` is compacted to dashboard-relevant summary keys (counts/partial flags/rate-limit summary/error message) instead of full child ingest responses.
  - When a child ingest returns partial/limit signals, dispatcher records run status as `partial` using reason precedence from payload (`stopped_reason`, rate-limit stop flags/reasons, request-budget saturation); runtime timeout remains `runtime_budget_exceeded`.
  - Stores `series_polled` from ingest responses when available (used by OpenAQ and Breathe London).
  - Logs whether the cron secret is present (boolean + length) for debugging.
  - Logs each dispatched edge call with the target function name and cron secret presence (length only).
  - Writes dispatch errors to `error_logs`.

### History Outbox Flusher (Cloud Run)
- Purpose: Flush history dual-write outbox rows in bounded batches.
- Triggered by: Cloud Scheduler -> Cloud Run job (`workers/uk_aq_observs_outbox_cloud_run`).
- Notes:
  - This is no longer run by an edge function.
  - Flush batches merge claimed outbox payloads before history upsert.
  - Main DB observation-write payload metrics are exposed via
    `uk_aq_public.uk_aq_observation_rpc_metrics_minute` (main DB).
  - History-side RPC pressure can be monitored via
    `uk_aq_public.uk_aq_history_rpc_metrics_minute` and
    `uk_aq_public.uk_aq_observation_rpc_metrics_minute` (history DB alias view).
  - In ingest DB, observation write metrics retention is 30 days via
    `uk_aq_public.uk_aq_rpc_observation_rpc_metrics_cleanup`, scheduled by
    `pg_cron` job `uk_aq_ingest_runtime_metrics_cleanup_daily`.
  - Archived edge/cloudflare implementation is under `archive/2026-02-12_observs_outbox_migration/`.

### History Pub/Sub Writer (Cloud Run)
- Purpose: Pull history messages from Pub/Sub and write merged mixed-row batches to history DB.
- Triggered by: Cloud Scheduler -> Cloud Run service (`workers/uk_aq_observs_pubsub_cloud_run`).
- Notes:
  - This is external to edge runtime; edge/shared ingest logic publishes when `OBSERVS_WRITE_MODE=pubsub_only`.
  - Writer dedupes by `(connector_id, timeseries_id, observed_at)` before upsert and writes sync receipts to main DB.
  - Mixed rows across connectors are processed in the same batch, reducing history RPC call overhead.

### DB Size Logger (Cloud Run)
- Purpose: Sample current project DB size (`pg_database_size(current_database())`) from ingest DB and Obs AQI DB once per run, then upsert hourly points into ingest DB. It also writes R2 History domain metrics, and can still write Obs AQI schema-size metrics only when explicitly re-enabled as a fallback.
- Triggered by: Cloud Scheduler -> Cloud Run service (`workers/uk_aq_db_size_logger_cloud_run`).
- Reads:
  - Ingest DB RPC: `uk_aq_public.uk_aq_rpc_database_size_bytes`
  - Obs AQI DB RPC: `uk_aq_public.uk_aq_rpc_database_size_bytes`
- Writes (ingest DB):
  - RPC: `uk_aq_public.uk_aq_rpc_db_size_metric_upsert`
  - RPC: `uk_aq_public.uk_aq_rpc_db_size_metric_cleanup`
  - Table: `uk_aq_ops.db_size_metrics_hourly`
  - View: `uk_aq_public.uk_aq_db_size_metrics_hourly`
- Notes:
  - Upsert key is `(bucket_hour, database_label)` with labels `ingestdb` and `obs_aqidb`.
  - Size RPC payload includes `oldest_observed_at` (min timestamp in that DB); logger stores it in `uk_aq_ops.db_size_metrics_hourly.oldest_observed_at`.
  - Cleanup RPC trims old rows by retention days (`UK_AQ_DB_SIZE_RETENTION_DAYS`, default `120`).
  - Obs AQI DB local scheduling is staggered off the hour: DB size at `1 * * * *`, schema size at `2 * * * *`.
  - Cloud Run feature flags:
    - `UK_AQ_DB_SIZE_CLOUD_RUN_ENABLED=false` skips DB-size sampling/upserts and leaves DB-size to local `pg_cron`.
    - `UK_AQ_SCHEMA_SIZE_CLOUD_RUN_ENABLED=false` skips schema-size sampling/upserts and leaves schema-size to local `pg_cron`.
    - with both disabled, Cloud Run still handles the R2 domain-size metrics path.
  - Cloud Run CPU/memory/concurrency are managed in deploy workflow vars (`GCP_DB_SIZE_LOGGER_*`).

### uk_aq_egress_monitor
- Purpose: Lightweight monitor endpoint that summarizes egress metrics and raises a warning when total MB over a lookback window exceeds a threshold.
- Triggered by:
  - `.github/workflows/uk_aq_egress_monitor.yml` (main ingest Supabase project; every 5 minutes)
  - `.github/workflows/uk_aq_observs_egress_monitor.yml` (history Supabase project; every 5 minutes)
  - Manual invocation.
- Reads:
  - Primary: `uk_aq_public.uk_aq_endpoint_egress_metrics_minute`
  - Fallback: `uk_aq_raw.endpoint_egress_metrics_minute` (when public view path fails)
- Writes:
  - Optional `uk_aq_raw.error_logs` warning row when threshold is exceeded.
- Auth:
  - Requires `X-Cron-Secret` only when `SB_UK_AQ_CRON_SECRET` is set.
- Notes:
  - Internal PostgREST access uses `SB_SECRET_KEY` via `apikey` (with `SUPABASE_SERVICE_ROLE_KEY` fallback).
  - For history-project deployment, use `.github/workflows/uk_aq_observs_edge_deploy.yml` which deploys with `--no-verify-jwt` so invocations can remain publishable-key based.
  - Uses `x-ukaq-egress-bypass: 1` on its own PostgREST calls so monitor traffic does not recursively inflate egress metrics.
  - Paginates through `uk_aq_endpoint_egress_metrics_minute` for the lookback window (not capped to a single page).
  - Enforces a runtime budget and per-request timeout during pagination; returns partial results with `rows_truncated=true` and `rows_truncated_reason` when limits are hit.
  - Returns both observed sampled totals and sampling-adjusted estimated totals; alert threshold uses `estimated_mb`.
  - Aggregates endpoint totals with caller tags normalized back to base endpoint names, and also returns `top_endpoint_callers_estimated` for endpoint+caller attribution.
  - Supports query params `lookback_minutes`, `top_n`, `alert_mb`, `write_error_log`, `page_size`, `max_rows`, `runtime_budget_ms`, `request_timeout_ms`.
  - On failures, response body includes `message` for faster workflow-side diagnostics.

### ingest_sos
- Purpose: Poll UK-AIR SOS timeseries and write observations + last_value fields.
- Triggered by:
  - `uk_aq_dispatch_polls` when `connectors.scheduler_backend='supabase_function'` (edge path).
  - `workers/uk_aq_sos_cloud_run` when `connectors.scheduler_backend='google_cloud_run'` (Cloud Run path).
- Cloud Run setup reference: `system_docs/uk_aq_sos_cloud_run.md`.
- Note: Deploying the Edge Function does not create a schedule; use the Cloudflare Worker cron for regular runs.
- Notes:
  - Requires an existing connector row; the ingest does not create connectors.
  - Edge path checkpointing is unchanged and uses `sos_timeseries_checkpoints`.
  - Polling reads only active SOS rows (`uk_aq_core.timeseries.ended_at is null`); ended rows are excluded from edge and Cloud Run polling scopes.
  - Cloud Run path uses station-level selector/checkpoints (`sos_select_station_refs`, `sos_station_checkpoints`) before passing scoped `timeseries_ids` into the same ingest handler.
  - `timeseries_ids` scoping matches internal `uk_aq_core.timeseries.id` only (no `timeseries_ref` fallback).
  - Daily full-catalog UK-AIR discovery reconciles timeseries lifecycle: rows missing for 2 consecutive runs get `timeseries.ended_at`; reappearing rows are auto-reactivated.
  - Logs cron secret mismatch diagnostics (presence/length only) when authorization fails.
  - Skips timeseries with missing `last_value_at` or `last_value_at` older than the poll window.
  - When `timeseries_ids` are explicitly scoped (Cloud Run path), stale `last_value_at` rows are used to fill remaining batch budget after recency filtering; if recency filtering yields zero work, stale rows are used for the whole scoped batch. This prevents mixed-pollutant starvation after SOS outages.
  - Handles UK-AIR nested value shapes for freshness updates (for example `lastValue: { timestamp, value }`) when deriving `timeseries.last_value` / `timeseries.last_value_at`.
  - Enforces a runtime budget and will return partial progress with `partial=true` when exceeded.
  - Dedupes observations by `observed_at` per timeseries before upsert to avoid duplicate conflict errors.
  - History dual-write rows are buffered and flushed in batches to reduce History RPC request count (`HISTORY_BUFFER_FLUSH_ROWS`, default `5000`).
  - Response payload includes history write counters: `history_written`, `history_receipts_upserted`, `history_enqueued`, `history_flushes`.
- Writes:
  - `observations` (upsert by connector_id + timeseries_id + observed_at)
  - `timeseries.last_value` and `timeseries.last_value_at` (update by id)
- Logs:
  - Writes a log file to Dropbox `/connectors/sos/log/YYYY-MM-DD/`
  - Writes raw payloads to Dropbox `/connectors/sos/raw_data/YYYY-MM-DD/` as ZIP
  - Writes errors to `error_logs` and `/error_log/YYYY-MM-DD/`
  - Filename prefixes are runtime-specific via `SOS_DROPBOX_UPLOAD_SOURCE`:
    - edge runtime: `uk_aq_*_edge_*`
    - Cloud Run runtime: `uk_aq_*_cloud_run_*`
  - Logs a "No datapoints parsed" warning with row count when the SOS payload has no rows.

### ingest_sensorcommunity
- Purpose: Poll Sensor.Community recent values and write stations, timeseries, and observations.
- Triggered by:
  - `uk_aq_dispatch_polls` when `connectors.scheduler_backend='supabase_function'`.
  - `workers/uk_aq_sensorcommunity_cloud_run` (Cloud Run job) when `connectors.scheduler_backend='google_cloud_run'`.
- Cloud Run setup reference: `system_docs/uk_aq_sensorcommunity_cloud_run.md`.
- Writes:
  - `connectors` (last_polled_at updates), `stations`, `phenomena`, `timeseries`, `observations`
- Notes:
  - Requires an existing connector row; the ingest does not create connectors.
  - Uses `SCOMM_*` environment variables for base URL, service metadata, and country.
  - `SCOMM_INGEST_MET_FIELDS=true` enables temperature/humidity/pressure ingestion.
  - Filters to the UK bounding box by default; stations with missing coordinates are kept.
  - Sets `stations.station_exposure` to `indoor`/`outdoor` when `location.indoor` is present (0/1 or boolean).
  - Honors `connectors.overwrite_station_name` to decide when `stations.station_name` can be overwritten (false keeps existing non-null names).
  - Enforces a runtime budget and returns partial progress (`partial=true`, `stopped_reason=runtime_budget_exceeded`, optional `stopped_phase`) when exceeded.
  - Reserves a response buffer (`SCOMM_RESPONSE_BUFFER_MS`, default `10000`) and skips non-critical Dropbox uploads when the remaining budget is too low.
  - Performs dual-write to main observations and history in parallel to reduce end-to-end runtime.
  - Applies strict pre-write exact dedupe on `(connector_id, timeseries_id, observed_at, value, status)` before main observations upsert and before history write enqueue/publish.
  - Runtime ingest no longer performs Sensor.Community timeseries phenomenon backfill; run that as maintenance in the daily stations workflow.
- Logs:
  - Writes a log file to Dropbox `/connectors/sensorcommunity/log/YYYY-MM-DD/` (prefix `uk_aq_log_edge_scomm_`).
  - Writes raw payloads to Dropbox `/connectors/sensorcommunity/raw_data/YYYY-MM-DD/` as ZIP (prefix `uk_aq_raw_edge_scomm_`).
  - Writes errors to `error_logs` and `/error_log/YYYY-MM-DD/`.

### ingest_openaq
- Purpose: Poll OpenAQ locations within the UK bounding box and write stations, timeseries, and observations.
- Triggered by:
  - `uk_aq_dispatch_polls` when `connectors.scheduler_backend='supabase_function'` (edge path).
  - `workers/uk_aq_openaq_cloud_run` when `connectors.scheduler_backend='google_cloud_run'` (Cloud Run path).
- Cloud Run setup reference: `system_docs/uk_aq_openaq_cloud_run.md`.
- Writes:
  - `stations`, `phenomena`, `timeseries`, `observations`
  - `openaq_station_checkpoints`, `openaq_timeseries_checkpoints`
- Notes:
  - Requires an existing connector row; the ingest does not create connectors.
  - Cloud Run scheduling model for OpenAQ is due-driven: each run enqueues a one-off next-run Cloud Task based on earliest due checkpoint, with a 15-minute Cloud Scheduler safety trigger.
  - Queue reconciliation for OpenAQ Cloud Run tasks:
    - if an earlier/equal pending OpenAQ task exists, do not enqueue another task;
    - if only later pending OpenAQ task(s) exist, delete those later task(s) and enqueue the newly computed earlier task.
  - Uses `OPENAQ_*` environment variables for base URL, API key, and bbox paging.
  - Fetches locations via `/v3/locations` (bbox) and latest values via `/v3/locations/{id}/latest`.
  - Performs a pre-call gap check using `openaq_timeseries_checkpoints.last_observed_at` (gap if any timeseries is 2–24 hours old); when true, polls `/v3/sensors/{id}/measurements/hourly` instead of `/latest` for that station.
  - Hourly gap observations are keyed to the requested `timeseries_ref` (sensor id) to avoid payload `sensorsId` mismatches.
  - Hourly `observed_at` is derived from `coverage.datetimeTo.utc` when available; otherwise uses `period.datetimeTo.utc` (fallbacks to `datetime.utc` / `period.datetimeFrom.utc`) and clamps future timestamps back to `period.datetimeFrom.utc` or `now` to avoid future `last_observed_at`.
  - When `locations_fetched=false`, loads timeseries refs for all selected stations via `uk_aq_rpc_timeseries_refs_by_station_ids` so timeseries checkpoints can always be updated.
  - Uses sensor IDs as `timeseries_ref` and `openaq:{parameter}` as `phenomena.source_label`.
  - Phenomena upserts include canonical observed-property mapping (`observed_property_code`, domain, optional canonical unit) via `uk_aq_rpc_phenomena_upsert`.
  - If `station_refs` are provided, limits polling to those location ids; otherwise uses a tiered selector (`uk_aq_rpc_openaq_select_station_refs`) that returns both station refs and station ids.
  - Uses `batch_size` (from dispatcher `connectors.poll_timeseries_batch_size`) as `OPENAQ_MAX_REQUESTS_PER_RUN`.
  - Uses stale cap 4 and tiered cap up to 52 (`tier1` first, then `tier2`) for automatic station selection.
  - Stale selection uses `next_due_at <= now() - 24 hours` plus `last_polled_at <= now() - 12 hours` (stations with `next_due_at` null stay in tier1).
  - `tier2` includes all stations with `due_at < now()-3h` (not capped at 24h old) so overdue stations are not trapped in a 24h dead zone before stale cooldown.
  - Applies a per-run OpenAQ request budget (`OPENAQ_MAX_REQUESTS_PER_RUN`, default 56).
  - Applies a gap reserve guard (`OPENAQ_GAP_REQUESTS_REMAINING_MIN`, default 10) so hourly gap calls do not consume the final request budget.
  - Applies a shared DB-backed OpenAQ token budget before each OpenAQ API call via `uk_aq_public.uk_aq_rpc_openaq_token_budget_reserve` (`OPENAQ_SHARED_BUDGET_*` env vars); this is shared across Cloud Run ingest and OpenAQ station scripts.
  - Applies station-count thresholds for run gating:
    - `OPENAQ_MIN_GAP_STATIONS` (default `1`)
    - `OPENAQ_MIN_NON_GAP_STATIONS` (default `10`)
    - If selected stations do not meet either threshold, the run returns `run_status=skipped` with `stations_polled=0`.
  - Response payload includes `gap_stations_total` and `gap_stations_polled` for run diagnostics/UI.
  - Applies strict pre-write exact dedupe on `(connector_id, timeseries_id, observed_at, value, status)` before main observations upsert and before history write enqueue/publish.
  - `request_budget_limited` means the ingest was constrained by the local per-run request budget/gap guard (our budget), not OpenAQ API rate-limit headers.
  - OpenAQ API rate-limit-driven stops are surfaced as `remaining_low`, `rate_limit_429`, or `rate_limit_guard`.
  - Shared-budget stops are surfaced as `shared_budget_minute_limit` / `shared_budget_hour_limit` (or RPC error variants when reservation fails).
- Shared-budget minute/hour stops append structured `openaq_shared_budget_blocked` entries to the run response `warnings` array and normal Dropbox log; they do not create `error_logs` rows or Dropbox `/error_log/` files by themselves.
- OpenAQ fetch warnings observed while shared-budget minute/hour limits are active are classified as shared-budget warnings and remain in `/connectors/openaq/log/...` (not `/error_log/...`).
  - Cloud Run wrapper enforces an hourly OpenAQ request budget using recent `uk_aq_ingest_runs.response_payload.requests_total`; when exhausted before ingest starts, the wrapper records `run_status=skipped` with `run_message=Skipped - Hourly Limit` and defers next run to reset/fallback.
  - In Cloud Run status mapping, `run_status=skipped` is reserved for station-eligibility skips and hourly-cap skips; non-hourly rate-limit/request-budget stops are recorded as `run_status=partial`.
  - Tracks per-station scheduling in `uk_aq_raw.openaq_station_checkpoints` (next due, last observed, sample arrays, last polled). `last_polled_at` only updates for stations where at least one OpenAQ request is issued in the run. When fewer than 10 interval/lag samples exist, `next_due_at` is set to `now() + 5 minutes`. Otherwise it uses the minimum interval (capped at 1 hour) plus lag selected by `OPENAQ_LAG_STAT` (`min` default, `median`, `p25`). If no observations are returned and `next_due_at` is null, it is set to `now() + 5 minutes`. For gap-mode stations with no new observations, `next_due_at` is set to `last_observed_at + min(observ_interval_samples)` capped at `+1 hour` (fallback `+1 hour` when samples are empty).
  - Tracks per-timeseries scheduling in `uk_aq_raw.openaq_timeseries_checkpoints` (next due, last observed, lag samples, last polled); when fewer than 10 lag samples exist, `next_due_at` is set to `now() + 5 minutes`. Otherwise it uses `last_observed_at + 3600s + lag_stat(lag)` (`OPENAQ_LAG_STAT`) and only updates `next_due_at` on new observations or when null.
  - Checkpoint reads are staged for lower egress: the first read loads lightweight per-timeseries `last_observed_at` snapshots from `uk_aq_raw.openaq_timeseries_checkpoints` for gap decisions, then full lag-sample rows are fetched only for timeseries being checkpoint-updated.
  - Station names are prefixed with provider shortnames when configured (e.g., `London Air Quality Network` -> `LAQN`), and append owner when present and not `Unknown*`.
  - Stores OpenAQ owner in `station_metadata.attributes.openaq_owner` when present and not `Unknown*`.
  - Updates `timeseries.last_value` and `timeseries.last_value_at` based on the most recent measurement.
  - Uses public RPCs for database writes (schemas are not exposed via PostgREST).
  - Enforces a runtime budget (default 120s) and returns `partial=true` when exceeded.
  - Runtime-vs-rate-limit stop signaling is explicit: `partial=true` is only set when the runtime deadline is reached; rate-limit/request-budget early stops are reported via `stopped_reason` without forcing `partial=true`.
  - Requires `X-Cron-Secret` when `SB_UK_AQ_CRON_SECRET` is set.
  - Stops issuing new requests when rate-limit remaining drops below the threshold (default 5), on HTTP 429, on OpenAQ HTTP 401/403, or when the per-run request budget is exhausted.
  - Response metadata includes `rate_limit_reset` and `rate_limit_reset_at` so Cloud Run scheduling can defer next run until reset when throttled; if no reset is present, Cloud Run falls back to `OPENAQ_RATE_LIMIT_FALLBACK_SECONDS` (default 300s).
  - Response metadata also includes shared-budget telemetry and reset hints (`shared_budget_*`) so Cloud Run can defer retries to the true shared-budget reset time.
  - Cloud Run auth safety guard can auto-disable OpenAQ polling (`connectors.poll_enabled=false`) on OpenAQ auth 401/403, writes an explicit `openaq_polling_auto_disabled` failure, and clears queued OpenAQ self-tasks to avoid retry loops.
- Logs:
  - Filename prefixes are runtime-specific via `OPENAQ_DROPBOX_UPLOAD_SOURCE`:
    - Edge runtime (default): `uk_aq_log_edge_openaq_`, `uk_aq_raw_edge_openaq_`, `uk_aq_error_edge_openaq_`.
    - Cloud Run runtime: `uk_aq_log_cloud_run_openaq_`, `uk_aq_raw_cloud_run_openaq_`, `uk_aq_error_cloud_run_openaq_`.
  - Writes to Dropbox `/connectors/openaq/log/YYYY-MM-DD/` and `/connectors/openaq/raw_data/YYYY-MM-DD/` when Dropbox config is enabled.
  - Writes diagnostic entries to `error_logs` when Dropbox config is missing/mismatched or log/raw uploads fail.
  - Writes an error log entry when timeseries refs are polled but cannot be mapped to internal `timeseries_id`s (includes sample refs + station ids/refs when available).
  - Buffers error log lines for optional Dropbox error log uploads.
  - Logs timeseries mapping diagnostics (missing refs/station ids samples) to aid checkpoint debugging.
  - Maintenance (2026-02-09): lint cleanup and stricter Dropbox upload byte typing (`Uint8Array.from`) for Deno type compatibility.

### ingest_breathelondon
- Purpose: Poll Breathe London Communities for hourly observations with checkpointing.
- Triggered by:
  - `uk_aq_dispatch_polls` when `connectors.scheduler_backend='supabase_function'`.
  - `workers/uk_aq_blondon_communities_cloud_run` when `connectors.scheduler_backend='google_cloud_run'`.
  - Helper RPCs live in `supabase/uk_aq_polling_helpers.sql`.
- Writes:
  - `connectors` (last_polled_at updates), `stations`, `phenomena`, `timeseries`, `observations`
  - `blondon_communities_station_checkpoints` (per-station checkpoints)
- Notes:
  - Uses connector code `blondon_communities`; public network code and service ref remain `breathelondon`.
  - Requires an existing connector row; the ingest does not create connectors.
  - Uses `BLONDON_COMMUNITIES_API_KEY` for every request.
  - Supports `skip_stations` to avoid station upserts; when set, stations are loaded from Supabase instead of `ListSensors`.
  - Supports `active_only` to limit polling to stations where `stations.removed_at is null`.
  - Supports `station_refs` to limit polling to a specific set of station refs.
  - Phenomena upserts use shared source-service `source_label` keys (for example `breathelondon:pm2.5`) and map to canonical observed-property codes/domains via `uk_aq_rpc_phenomena_upsert`. These labels are not connector identity and intentionally remain stable.
  - Uses `uk_aq_raw.blondon_communities_station_checkpoints` for per-station scheduling (`next_due_at`, `ingest_lag_samples`).
  - `blondon_communities_select_station_refs` stale selection excludes future-due rows (`due_at > now()`).
  - Supports `debug=true` to include a debug block in the response (Dropbox config status, no secrets).
  - Response includes run-level `last_observed_at` (latest observed timestamp across the run scope) for ingest run feed reporting.
  - Cloud Run runner derives `window_hours` from `connectors.poll_window_hours` and batch limit from `connectors.poll_timeseries_batch_size` (fallback defaults apply), then fetches due station refs via `blondon_communities_select_station_refs`.
  - Cloud Run runner marks run `skipped` with `no_station_refs` when no due refs are returned.
  - Cloud Run run-row writes preserve zero-valued metrics (`observations_upserted`, `timeseries_updated`, `series_polled`) as `0` instead of storing `null`.
  - Logs cron secret mismatch diagnostics (presence/length only) when authorization fails.
  - Logs incoming request auth header presence (no secrets) for debugging.
  - Response includes `stations_requested`/`stations_selected` when station refs are supplied.
  - Response includes `series_polled` (timeseries with last-value updates during the run).
  - Runtime budget behavior:
    - Edge runtime (`BLONDON_COMMUNITIES_DROPBOX_UPLOAD_SOURCE=edge`): budget enabled; returns partial progress with `partial=true` when exceeded.
    - Cloud Run runtime (`BLONDON_COMMUNITIES_DROPBOX_UPLOAD_SOURCE=cloud_run`): inner ingest budget disabled by default, but the Cloud Run wrapper enforces a 14-minute child-process timeout so the service releases its in-process run lock before the default 15-minute Cloud Run request timeout.
    - Override with `BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET=true|false`.
  - History dual-write rows are buffered and flushed in batches to reduce History RPC request count (`HISTORY_BUFFER_FLUSH_ROWS`, default `5000`).
    - This applies to both Edge and Cloud Run because Cloud Run reuses `ingest_breathelondon/index.ts`.
  - Applies strict pre-write exact dedupe on `(connector_id, timeseries_id, observed_at, value, status)` before main observations upsert and before history write enqueue/publish.
  - Response payload includes dedupe counters: `observations_rows_input`, `observations_rows_prepared`, `observations_rows_deduped_prewrite`, `history_rows_prepared`, `history_rows_deduped_prewrite`.
  - Response payload includes history write counters: `history_written`, `history_receipts_upserted`, `history_enqueued`, `history_flushes`.
  - Updates `connectors.last_polled_at` on successful non-dry runs.
- Logs:
  - Writes a log file to Dropbox `/connectors/blondon_communities/log/YYYY-MM-DD/` when Dropbox credentials are configured.
  - Raw payload uploads are gated by `BLONDON_COMMUNITIES_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (or `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL`) matching `SUPABASE_URL`.
  - Filename prefixes are runtime-specific: `uk_aq_*_edge_*` for edge runtime and `uk_aq_*_cloud_run_*` for Cloud Run runtime.
  - Writes errors to `error_logs` and `/error_log/YYYY-MM-DD/` when Dropbox error logging is configured.
  - Writes diagnostic entries to `error_logs` when Dropbox config is missing/mismatched or log/raw uploads fail.

### ingest_erg_laqn
- Purpose: Poll ERG LAQN (configurable group, default London) and write observations.
- Triggered by: `uk_aq_dispatch_polls` (external scheduler). Helper RPCs live in `supabase/uk_aq_polling_helpers.sql`.
- Writes:
  - `connectors`, `stations`, `phenomena`, `timeseries`, `observations`
  - `timeseries.last_value` and `timeseries.last_value_at` (update by id)
  - `connectors.last_polled_at` (update by id)
  - `erg_laqn_station_checkpoints` (update by station_id)
- Notes:
  - Requires an existing connector row; the ingest does not create connectors.
  - Request body supports `group`, `station_refs`, `species`, `days`, `start_date`, `end_date`, `batch_size`, `sleep_seconds`, `dry_run`, `csv_station_id`, and `csv_station_ref`.
  - Uses `/Information/MonitoringSites/GroupName={group}/Json` for stations.
  - Uses `/Data/SiteSpecies/SiteCode={code}/SpeciesCode={species}/StartDate={YYYY-MM-DD}/EndDate={YYYY-MM-DD}/Json` for raw data.
  - Phenomena upserts use `source_label` keys (for example `laqn:NO2`) and map to canonical observed-property codes/domains via `uk_aq_rpc_phenomena_upsert`.
  - Dates are treated as UTC/GMT; when `end_date` is omitted, the edge function sets `EndDate` to tomorrow's UTC date so "today" is included.
  - Skips per-site/species ERG responses that return HTTP 400 (logs a warning; continues).
  - Includes zero-valued observations (no zero-value filtering).
  - When `start_from_latest=true`, uses `timeseries.last_value_at` to extend the per-series start date if the latest value is older than the requested start date.
  - Logs a warning when a site/species fetch returns data older than UTC midnight for the current day.
  - When CSV settings are configured, uploads a daily CSV per pollutant to Dropbox using a fixed station (see env vars).
  - Enforces a runtime budget and will return partial progress with `partial=true` when exceeded.
  - History dual-write rows are buffered and flushed in batches to reduce History RPC request count (`HISTORY_BUFFER_FLUSH_ROWS`, default `5000`).
  - Response payload includes history write counters: `history_written`, `history_receipts_upserted`, `history_enqueued`, `history_flushes`.
- Logs:
  - Writes a log file to Dropbox `/connectors/erg_laqn/log/YYYY-MM-DD/` (prefix `uk_aq_log_edge_erg_laqn_`).
  - Writes raw payloads to Dropbox `/connectors/erg_laqn/raw_data/YYYY-MM-DD/` as ZIP (prefix `uk_aq_raw_edge_erg_laqn_`).
  - Writes errors to `error_logs` and `/error_log/YYYY-MM-DD/` when Dropbox error logging is configured.

### uk_aq_latest
- Purpose: Serve the latest values across all stations (optionally filtered by region/station/pollutant).
- Triggered by: Web requests (read-only, no writes).
- Auth mode: deployed with `verify_jwt=false` plus required header `X-UK-AQ-Upstream-Auth` (shared secret checked in-function).
- Returns: contract version 2 with flattened latest rows. Public network identity is scalar `network_id`, `network_code`, and `network_label`; connector ID/code/label remain separate provenance fields.
- Params: `region`, `station_like`, `pollutant`, `network_code`, `limit`, `pcon_code`, `window` (`3h|6h|1d|7d|all`, default `all`), optional `caller` tag (for egress attribution telemetry).
- Notes:
  - The edge response intentionally omits nested `station` / `connector` / `phenomenon` objects to reduce payload size.
  - `window` is applied server-side using `last_value_at`; `all` disables time filtering.
  - `connector`, `connector_id`, and `connector_code` query parameters return `400`; there is no compatibility translation.
- RPC backing: `uk_aq_latest_rpc` via `/rest/v1/rpc/uk_aq_latest_rpc`.
- Cache-Control: success responses use `public, max-age=60, s-maxage=180, stale-while-revalidate=300, stale-if-error=86400`; errors use `no-store`.
- Egress observability: sampled success responses plus all `304`/`4xx`/`5xx`
  are recorded via `_shared/egress_metrics.ts` (console logs + DB metrics RPC).
  PostgREST RPC calls are tagged with window-specific caller labels:
  `uk_aq_latest.window_3h`, `uk_aq_latest.window_6h`, `uk_aq_latest.window_1d`,
  `uk_aq_latest.window_7d`, `uk_aq_latest.window_all`.
- `display_name` logic:
  - Uses `connectors.station_display_name_template` if present, with tokens `{station_name}`, `{station_label}`, `{station_ref}`.
  - Fallback is always `{station_name} - {station_ref}` (or `station_label` if `station_name` is missing).

Curl test example:
```bash
curl "https://YOUR_PROJECT.supabase.co/functions/v1/uk_aq_latest?region=London&pollutant=pm2.5&limit=100"
```

### uk_aq_stations_chart
- Purpose: Serve latest values for station-search chart pages (for example Bristol/Surbiton queries) with one shared endpoint.
- Triggered by: Web requests (read-only, no writes).
- Auth mode: deployed with `verify_jwt=false` plus required header `X-UK-AQ-Upstream-Auth` (shared secret checked in-function).
- Params: `station_like` (or `q`) required, `pollutant`, `network_code`, `window` (`3h|6h|1d|7d|all`, default `all`), `limit`, optional incremental cursor (`since`, `since_id`).
- Returns: contract version 2 flattened rows with scalar network identity, separate connector provenance, and cursor fields (`next_since`, `next_since_id`).
- Legacy connector filter parameters return `400`.
- `display_name` logic matches `uk_aq_latest`.
- Conditional requests: supports `If-None-Match`; returns `304 Not Modified` with `ETag` when payload is unchanged.
- Cache-Control: success responses use `public, max-age=60, s-maxage=300, stale-while-revalidate=300, stale-if-error=86400`; errors use `no-store`.
- Egress observability: sampled success responses plus all `304`/`4xx`/`5xx`
  are recorded via `_shared/egress_metrics.ts`.
- RPC backing: `uk_aq_latest_rpc` via `/rest/v1/rpc/uk_aq_latest_rpc`.

### uk_aq_stations
- Purpose: Serve station geometry for the hex map (bypasses RLS via service role).
- Triggered by: Web requests (read-only, no writes).
- Auth mode: deployed with `verify_jwt=false` plus required header `X-UK-AQ-Upstream-Auth` (shared secret checked in-function).
- Returns: contract version 2 station geometry rows with scalar network identity and separate connector provenance.
- Params: `network_code`, `region`, `station_like`, `limit`, `page_size`.
- Legacy connector filter parameters return `400`.
- RPC backing: `uk_aq_stations_rpc` via `/rest/v1/rpc/uk_aq_stations_rpc`.
- Cache-Control: success responses use `public, max-age=60, s-maxage=300, stale-while-revalidate=300, stale-if-error=86400`; errors use `no-store`.
- Egress observability: sampled success responses plus all `304`/`4xx`/`5xx`
  are recorded via `_shared/egress_metrics.ts`.

### uk_aq_la_hex
- Purpose: Serve LA-level latest PM2.5 summaries (median + mean) for the hex cartogram.
- Triggered by: Web requests (read-only, no writes).
- Auth mode: deployed with `verify_jwt=false` plus required header `X-UK-AQ-Upstream-Auth` (shared secret checked in-function).
- Returns: contract version 2 rows keyed by LA and public network, including scalar network identity, `station_count`, `single_site`, `median_value`, `mean_value`, and `latest_value_at`.
- Params: `region`, `la_version`, `network_code`, `limit`, optional `since` (ISO-8601 timestamp; returns changed LA/network rows only).
- Legacy connector filter parameters return `400`.
- RPC backing: `uk_aq_la_hex_rpc` via `/rest/v1/rpc/uk_aq_la_hex_rpc`.
- Conditional requests: supports `If-None-Match`; returns `304 Not Modified` with `ETag` when payload is unchanged.
- Cache-Control: success responses use `public, max-age=60, s-maxage=180, stale-while-revalidate=300, stale-if-error=86400`; errors use `no-store`.
- Egress observability: sampled success responses plus all `304`/`4xx`/`5xx`
  are recorded via `_shared/egress_metrics.ts`.

### uk_aq_pcon_hex
- Purpose: Serve constituency-level latest PM2.5 summaries (median + mean) for the hex cartogram.
- Triggered by: Web requests (read-only, no writes).
- Auth mode: deployed with `verify_jwt=false` plus required header `X-UK-AQ-Upstream-Auth` (shared secret checked in-function).
- Returns: contract version 2 rows keyed by constituency and public network, including scalar network identity, `station_count`, `single_site`, `median_value`, `mean_value`, and `latest_value_at`.
- Params: `pcon_version`, `network_code`, `limit`, optional `since` (ISO-8601 timestamp; returns changed constituency/network rows only).
- Legacy connector filter parameters return `400`.
- RPC backing: `uk_aq_pcon_hex_rpc` via `/rest/v1/rpc/uk_aq_pcon_hex_rpc`.
- Conditional requests: supports `If-None-Match`; returns `304 Not Modified` with `ETag` when payload is unchanged.
- Cache-Control: success responses use `public, max-age=60, s-maxage=300, stale-while-revalidate=300, stale-if-error=86400`; errors use `no-store`.
- Egress observability: sampled success responses plus all `304`/`4xx`/`5xx`
  are recorded via `_shared/egress_metrics.ts`.

### uk_aq_public_networks
- Purpose: Serve the canonical catalog used to construct public website network filters.
- Triggered by: Web requests (read-only, no writes).
- Auth mode: deployed with `verify_jwt=false` plus required header `X-UK-AQ-Upstream-Auth`.
- Returns: contract version 2 rows containing `network_id`, `network_code`, `network_label`, `network_type`, and `public_display_enabled`.
- Source: the filtered `uk_aq_public.networks` view; disabled networks cannot appear.
- Filtering: the catalog has no public filter parameters. Network and legacy connector filter parameters return `400`.
- Conditional requests: supports `If-None-Match` and `304 Not Modified`.
- Cache-Control: success responses use the shared five-minute metadata cache profile; errors use `no-store`.
- Egress observability: sampled success responses plus all `304`/`4xx`/`5xx` are recorded via `_shared/egress_metrics.ts`.

### uk_aq_timeseries
- Purpose: Serve raw observation points for a single timeseries.
- Triggered by: Web requests (read-only, no writes).
- Auth mode: deployed with `verify_jwt=false` plus required header `X-UK-AQ-Upstream-Auth` (shared secret checked in-function).
- Params:
  - required: `timeseries_id`
  - optional range selector (use one only):
    - `window` (`12h|24h|7d|31d|90d`; default `24h` when no selector is provided)
    - `days` (positive integer, max controlled by `UK_AQ_TIMESERIES_MAX_WINDOW_DAYS`, default `366`)
    - `start` + `end` (ISO-8601 datetimes), or `start_utc` + `end_utc`
  - optional response controls: `limit` (positive integer), `since` (ISO-8601), `format` (`objects|compact`, default `objects`)
- Validation:
  - only one range selector is allowed (`window` or `days` or `start/end`); mixed selectors return `400`.
  - invalid `window`, invalid datetime, or invalid `days` return `400`.
  - for datetime mode, if `end` is in the future, the effective end is clamped to current UTC time.
- Returns:
  - `data_format=objects`: row objects (`observed_at`, `value`)
  - `data_format=compact`: positional arrays with `columns` metadata (for lower payload size)
  - `source`: `recent_only | history_only | recent_history_stitched`
  - plus optional `guideline` (AQG_2021 24h) if found
- Notes: when `limit` is omitted, all rows in the requested window are returned (no default cap).
- Read path:
  - request interval is split by `INGESTDB_RETENTION_DAYS` into three source zones:
    - retention range (`now - INGESTDB_RETENTION_DAYS` to now): ingestdb only
    - one-day overlap (`now - (INGESTDB_RETENTION_DAYS + 1 day)` to retention start): R2 preferred, ingestdb fills only observation hours missing from R2
    - historical range (older than the one-day overlap): Observs History R2 API worker only
  - edge resolves `connector_id` from ingest `uk_aq_core.timeseries` and sends `timeseries_id + connector_id + start_utc/end_utc` to the worker.
  - ingest RPC `uk_aq_timeseries_rpc` is called only when the requested interval includes the retention range or one-day overlap.
  - direct Observs History R2 worker requests retry transient failures (`5xx`, `429`, Cloudflare `1102`, timeout, or request failure) up to three times with bounded backoff before the edge marks that request incomplete.
  - when a wide history-window request still fails with transient upstream errors, edge retries in smaller history chunks (`UK_AQ_OBSERVS_HISTORY_R2_CHUNK_DAYS`, default `7`) with per-chunk retries (`UK_AQ_OBSERVS_HISTORY_R2_CHUNK_MAX_RETRIES`, default `4`); multi-day chunks can be bisected down to daily chunks.
  - historical ranges do not fall back to ingestdb. Connector lookup or historical R2 failures mark the response as incomplete (`response_complete=false`, `has_gap=true`) instead of silently filling old history from ingestdb.
  - rows are merged on `observed_at` with R2 history preferred over ingestdb when both sources contain the same timestamp.
  - response metadata includes `overlap_start_utc`, `retention_start_utc`, source-window coverage, R2 partial reasons, and overlap ingest fill counts.
- Request flow (exact):
  1. Website calls Cloudflare cache proxy route `/api/aq/timeseries` (cache worker code/deploy is owned by `uk-aq-ops`).
  2. Cache proxy maps that route to one upstream edge function: `uk_aq_timeseries`.
  3. `uk_aq_timeseries` calls ingest PostgREST (`SUPABASE_URL/rest/v1`).
  4. Edge function calls ingest `uk_aq_timeseries_rpc` only for retention/overlap windows, and calls the Observs History R2 API worker for historical/overlap history.
  5. Edge function merges rows, returns one payload to Cloudflare, Cloudflare returns one payload to website.
- Important architecture note:
  - Cloudflare worker does not directly call DB or R2 history workers.
  - Cloudflare calls one edge function (`uk_aq_timeseries`), then edge performs ingest RPC reads and R2 history API reads.
  - Missing/misconfigured history worker settings no longer hard-fail the endpoint, but historical coverage is reported as partial rather than being filled from ingestdb.
- Conditional requests: supports `If-None-Match`; returns `304 Not Modified` with `ETag` when payload is unchanged.
- Cache-Control: success responses use `public, max-age=60, s-maxage=300, stale-while-revalidate=300, stale-if-error=86400`; errors use `no-store`.
- Egress observability: sampled success responses plus all `304`/`4xx`/`5xx`
  are recorded via `_shared/egress_metrics.ts`.

Curl test example (shape check):
```bash
curl "https://YOUR_PROJECT.supabase.co/functions/v1/uk_aq_timeseries?timeseries_id=123&window=24h"
curl "https://YOUR_PROJECT.supabase.co/functions/v1/uk_aq_timeseries?timeseries_id=123&window=31d"
curl "https://YOUR_PROJECT.supabase.co/functions/v1/uk_aq_timeseries?timeseries_id=123&days=45"
curl "https://YOUR_PROJECT.supabase.co/functions/v1/uk_aq_timeseries?timeseries_id=123&start=2026-02-01T00:00:00Z&end=2026-03-01T00:00:00Z"
```

Note:
- Browser-to-cache-proxy session auth is now handled inside Cloudflare Worker (`/api/aq/session/start`) and is not implemented as a Supabase Edge Function.

### uk_aq_station_snapshot
- Purpose: Serve raw station snapshot payloads for local debug dashboards via a protected endpoint.
- Triggered by: Web requests (read-only, requires authenticated JWT).
- Params:
  - `station_id` or `station_ref` (one required)
  - `timeseries_id` (optional, int4 / integer id)
  - `window` (`6h|24h|7d|21d|31d|90d`, default `6h`)
  - `obs_limit` (`100|1000`, default `100`)
- ID typing:
  - `station_id` remains bigint.
  - `timeseries_id` is integer.
- Auth:
  - Requires `Authorization: Bearer <JWT>`.
  - Verifies identity with `auth.getUser()`.
  - Uses `SB_PUBLISHABLE_DEFAULT_KEY` + caller JWT (does not use service role key).
- RPC backing: `uk_aq_public.uk_aq_station_snapshot`.
- SQL deploy note: `supabase/uk_aq_station_snapshot.sql` drops legacy bigint and int4 overloads before recreating the int4 signature to avoid PostgREST RPC ambiguity.
- Returns:
  - Raw station row (`stations`)
  - Raw station timeseries rows (`timeseries`)
  - Raw checkpoint rows from `uk_aq_raw.openaq_station_checkpoints` and `uk_aq_raw.openaq_timeseries_checkpoints`
  - Observations for selected timeseries ordered newest-first (`observed_at desc`)
  - `meta` with window bounds, obs limit, and default timeseries selection rule (`lowest_timeseries_id_for_station`)
- Caching / CORS:
  - OPTIONS includes `Access-Control-Max-Age: 86400`.
  - Success responses: `Cache-Control: public, max-age=30`.
  - Error responses: `Cache-Control: no-store`.

## Environment variables (Supabase Edge)

Required:
- `SUPABASE_URL`
- `SB_SECRET_KEY` (preferred; fallback `SB_SECRET_KEY` during migration)
- `UK_AQ_EDGE_UPSTREAM_SECRET` (required by AQ read endpoints; must match worker secret/header)
- `BLONDON_COMMUNITIES_API_KEY` (required for `ingest_breathelondon`)

Dropbox (raw/log/error uploads):
- `DROPBOX_APP_KEY`
- `DROPBOX_APP_SECRET`
- `DROPBOX_REFRESH_TOKEN`

Dropbox folders:
  - `UK_AQ_DROPBOX_ROOT` (e.g., `/CIC-Test` or `/LIVE`)
- `UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (required to enable raw uploads)
- `OPENAQ_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist override for OpenAQ)
- `BLONDON_COMMUNITIES_DROPBOX_ROOT` (optional override for Breathe London)
- `BLONDON_COMMUNITIES_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist override for Breathe London)
- `BLONDON_COMMUNITIES_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist override for Breathe London error uploads)
- `SCOMM_DROPBOX_ROOT` (optional override for Sensor.Community)
- `SCOMM_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist override for Sensor.Community)

Optional:
- `UK_AQ_CORE_SCHEMA` (defaults to `uk_aq_core`; used for PostgREST profile headers)
- `UK_AQ_RAW_SCHEMA` (defaults to `uk_aq_raw`; used for raw tables like `error_logs` and checkpoint tables)
- `UK_AQ_OBSERVS_HISTORY_R2_API_URL` (required for older-window `uk_aq_timeseries` reads; Observs History R2 worker URL)
- `UK_AQ_OBSERVS_HISTORY_R2_API_TIMEOUT_MS` (optional; default `10000`; timeout for edge-to-R2-history API requests)
- `UK_AQ_OBSERVS_HISTORY_R2_CHUNK_DAYS` (optional; default `7`; history retry chunk size for `uk_aq_timeseries` when large-window history reads return transient upstream failures)
- `UK_AQ_OBSERVS_HISTORY_R2_CHUNK_MAX_RETRIES` (optional; default `4`; per-chunk retry attempts during history chunk fallback in `uk_aq_timeseries`)
- `INGESTDB_RETENTION_DAYS` (optional; default `5`; single split source for retention range plus one-day overlap; current TEST config is `4`)
- `OBSERVS_OUTBOX_CLOUD_RUN_MAX_BATCHES` (optional; defaults to `30`; Cloud Run outbox batches per run)
- `OBSERVS_OUTBOX_CLOUD_RUN_CLAIM_BATCH_LIMIT` (optional; defaults to `20`; outbox claim size per batch in Cloud Run)
- `OBSERVS_OUTBOX_CLOUD_RUN_BUDGET_SECONDS` (optional; defaults to `540`; Cloud Run runtime budget)
- `OBSERVS_OUTBOX_CLOUD_RUN_SHUTDOWN_BUFFER_SECONDS` (optional; defaults to `20`; reserved buffer before timeout)
- `OBSERVS_OUTBOX_CLOUD_RUN_RPC_RETRIES` (optional; defaults to `3`; main RPC retry count)
- `OBSERVS_UPSERT_RPC_RETRIES` (optional; defaults to `3`; retry count for history upsert RPC calls)
- `OBSERVS_UPSERT_RETRY_BASE_MS` (optional; defaults to `1000`; base backoff (ms) between history upsert retries)
- `OBSERVS_UPSERT_TIMEOUT_SPLIT_MIN_ROWS` (optional; defaults to `32`; minimum chunk size that can be split on statement timeout)
- `OBSERVS_UPSERT_TIMEOUT_SPLIT_MAX_DEPTH` (optional; defaults to `4`; maximum recursive split depth for timeout fallback)
- `UK_AQ_DB_SIZE_RPC` (optional; defaults to `uk_aq_rpc_database_size_bytes`; Cloud Run DB-size read RPC name)
- `UK_AQ_DB_SIZE_UPSERT_RPC` (optional; defaults to `uk_aq_rpc_db_size_metric_upsert`; ingest DB write RPC name)
- `UK_AQ_DB_SIZE_CLEANUP_RPC` (optional; defaults to `uk_aq_rpc_db_size_metric_cleanup`; ingest DB retention cleanup RPC name)
- `UK_AQ_DB_SIZE_RETENTION_DAYS` (optional; defaults to `120`; DB-size metrics retention)
- `UK_AQ_DB_SIZE_RPC_RETRIES` (optional; defaults to `3`; DB-size logger RPC retry count)
- `UK_AQ_INGEST_DB_LABEL` (optional; defaults to `ingestdb`; label stored for ingest DB points)
- `UK_AQ_OBS_AQIDB_DB_LABEL` (optional; defaults to `obs_aqidb`; label stored for history DB points)
- `OBSERVS_WRITE_MODE` (optional; defaults to `outbox_only`; `outbox_only` queues history rows to main outbox for asynchronous flush, `direct` attempts immediate history upsert then falls back to outbox, `pubsub_only` publishes rows to Pub/Sub and does not use main DB outbox)
- `GCP_PROJECT_ID` (required when `OBSERVS_WRITE_MODE=pubsub_only` unless `GOOGLE_CLOUD_PROJECT` is set)
- `GCP_OBSERVS_PUBSUB_TOPIC` (optional; required when `OBSERVS_WRITE_MODE=pubsub_only`; accepts topic id or full `projects/.../topics/...` path)
- `OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE` (optional; defaults to `500`; number of Pub/Sub messages per publish call)
- `DISPATCH_TIME_BUDGET_MS` (optional; defaults to `150000`; dispatcher per-request runtime budget)
- `DISPATCH_SHUTDOWN_BUFFER_MS` (optional; defaults to `10000`; reserved time before budget to return cleanly)
- `DISPATCH_EDGE_CALL_TIMEOUT_MS` (optional; defaults to `140000`; per-child ingest timeout within dispatcher)
- `DISPATCH_MIN_START_EDGE_CALL_MS` (optional; defaults to `30000`; skip starting a new child ingest call when remaining dispatcher budget is below this threshold)
- `DISPATCH_QUEUE_CLAIM_BATCH_LIMIT` (optional; defaults to `1`; queue jobs claimed per `mode=run_queue` call)
- `DISPATCH_QUEUE_LEASE_SECONDS` (optional; defaults to `900`; queue job lease during processing)
- `LATEST_INGEST_RUNS_LOOKBACK_HOURS` (optional; defaults to `48`; dispatcher lookback window when loading latest ingest run state)
- `LATEST_INGEST_RUNS_FALLBACK_LIMIT` (optional; defaults to `25`; fallback `uk_aq_ingest_runs` read cap if latest-run RPC is unavailable)
- `UK_AQ_EGRESS_LOG_SAMPLE_RATE` (optional; defaults to `0.2`; sample rate for `2xx` endpoint metrics)
- `UK_AQ_EGRESS_METRICS_DB_ENABLED` (optional; defaults to `true`; DB write toggle for endpoint metrics)
- `UK_AQ_EGRESS_METRICS_CLEANUP_SAMPLE_RATE` (optional; defaults to `0.01`; chance to run retention cleanup after write)
- `UK_AQ_EGRESS_METRICS_CLEANUP_MIN_INTERVAL_MS` (optional; defaults to `900000`; minimum interval between cleanup attempts)
- `UK_AQ_EGRESS_METRICS_AGG_RETENTION_DAYS` (optional; defaults to `30`; minute aggregate retention)
- `UK_AQ_EGRESS_METRICS_RAW_RETENTION_DAYS` (optional; defaults to `7`; raw `304`/error event retention)
- `UK_AQ_POSTGREST_EGRESS_CAPTURE_ENABLED` (optional; defaults to `true`; enables `/rest/v1/*` fetch instrumentation in edge functions and Cloud Run workers that import the shared patch)
- `UK_AQ_POSTGREST_EGRESS_CAPTURE_SAMPLE_RATE` (optional; defaults to `1`; sampling for captured PostgREST `2xx` fetch metrics)
- `UK_AQ_POSTGREST_EGRESS_CAPTURE_URLS` (optional; comma-separated Supabase base URLs/origins to track in addition to `SUPABASE_URL` and `OBS_AQIDB_SUPABASE_URL`)
- `UK_AQ_EGRESS_MONITOR_LOOKBACK_MINUTES` (optional; defaults to `60`; monitor lookback window)
- `UK_AQ_EGRESS_MONITOR_TOP_N` (optional; defaults to `20`; monitor top endpoint count)
- `UK_AQ_EGRESS_MONITOR_ALERT_MB` (optional; defaults to `250`; warning threshold for MB in lookback window)
- `UK_AQ_EGRESS_MONITOR_WRITE_ERROR_LOG` (optional; defaults to `true`; write warning rows into `error_logs` when threshold is exceeded)
- `UK_AQ_EGRESS_MONITOR_PAGE_SIZE` (optional; defaults to `1000`; page size used by egress monitor pagination)
- `UK_AQ_EGRESS_MONITOR_MAX_ROWS` (optional; defaults to `100000`; safety cap for monitor row scan)
- `UK_AQ_EGRESS_MONITOR_RUNTIME_BUDGET_MS` (optional; defaults to `120000`; runtime budget for monitor pagination loop)
- `UK_AQ_EGRESS_MONITOR_REQUEST_TIMEOUT_MS` (optional; defaults to `20000`; per-PostgREST request timeout during monitor pagination)
- `UK_AIR_ERROR_DROPBOX_FOLDER` (defaults to `error_log`)
- `BLONDON_COMMUNITIES_ERROR_DROPBOX_FOLDER` (optional override for Breathe London)
- `SCOMM_ERROR_DROPBOX_FOLDER` (optional override for Sensor.Community)
- `SCOMM_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist for Sensor.Community error uploads)
- `SCOMM_INGEST_MET_FIELDS` (defaults to `false`; set `true` to ingest temperature/humidity/pressure)
- `SCOMM_MAX_RUNTIME_SECONDS` (optional; defaults to `130`)
- `SCOMM_RESPONSE_BUFFER_MS` (optional; defaults to `10000`; reserved budget before runtime cutoff to return response cleanly)
- `SCOMM_OBSERVATION_UPSERT_CHUNK_SIZE` (optional; defaults to `1000`; chunk size for Sensor.Community observation upserts to main DB)
- `OPENAQ_BASE_URL` (optional; defaults to `https://api.openaq.org/v3`)
- `OPENAQ_API_KEY` (required for `ingest_openaq`)
- `OPENAQ_CONNECTOR_CODE` (optional; defaults to `openaq`)
- `OPENAQ_SERVICE_REF` (optional; defaults to `OPENAQ_CONNECTOR_CODE`)
- `OPENAQ_SERVICE_LABEL` (optional; defaults to `OpenAQ`)
- `OPENAQ_USER_AGENT` (optional; defaults to `uk-air-quality-networks`)
- `OPENAQ_BBOX` (optional; defaults to `-8.623555,49.863222,1.763337,60.871222`)
- `OPENAQ_PAGE_LIMIT` (optional; defaults to `1000`)
- `OPENAQ_MAX_PAGES` (optional; defaults to `50`)
- `OPENAQ_CONCURRENCY` (optional; defaults to `6`)
- `OPENAQ_MAX_RUNTIME_SECONDS` (optional; defaults to `120`)
- `OPENAQ_RATE_LIMIT_RETRIES` (optional; defaults to `3`)
- `OPENAQ_INGEST_STATION_FETCH` (optional; defaults to `false`)
- `OPENAQ_TIERED_LIMIT` (optional; defaults to `50`)
- `OPENAQ_STALE_LIMIT` (optional; defaults to `4`)
- `OPENAQ_TIER1_RETRY_SECONDS` (optional; defaults to `300`; minimum seconds since last poll for tier1 station selection)
- `OPENAQ_LAG_STAT` (optional; defaults to `min`; options `min|median|p25` for checkpoint lag scheduling)
- `OPENAQ_MIN_GAP_STATIONS` (optional; defaults to `1`; minimum selected gap stations needed to run regardless of non-gap count)
- `OPENAQ_MIN_NON_GAP_STATIONS` (optional; defaults to `10`; skip when no gap stations and selected non-gap stations are below this threshold)
- `OPENAQ_RATE_LIMIT_STOP_THRESHOLD` (optional; defaults to `5`)
- `OPENAQ_MAX_REQUESTS_PER_HOUR` (optional; Cloud Run wrapper default `1900`; hourly OpenAQ budget guard)
- `OPENAQ_SHARED_BUDGET_ENFORCE` (optional; defaults to `true`; enforce shared DB-backed minute/hour token budget)
- `OPENAQ_SHARED_BUDGET_KEY` (optional; defaults to `openaq`; shared bucket key for all OpenAQ callers)
- `OPENAQ_SHARED_BUDGET_CALLER` (optional; defaults to `ingest_openaq`; caller telemetry label)
- `OPENAQ_SHARED_BUDGET_MINUTE_LIMIT` (optional; defaults to `50`; hard shared per-minute cap)
- `OPENAQ_SHARED_BUDGET_HOUR_LIMIT` (optional; defaults to `1500`; hard shared rolling-hour cap)
- `OPENAQ_RATE_LIMIT_FALLBACK_SECONDS` (optional; Cloud Run wrapper default `300`; retry delay when rate-limit reset is unavailable)
- `OPENAQ_AUTH_SAFETY_DISABLE_POLLING` (optional; Cloud Run wrapper default `true`; auto-disable OpenAQ connector polling on auth 401/403)
- `CLEANAIRSURB_ST_ID` (optional; defaults to `189841`; OpenAQ debug station id used in ingest debug logs)
- `BLONDON_COMMUNITIES_BASE_URL` (optional override for Breathe London API base URL)
- `BLONDON_COMMUNITIES_CONNECTOR_CODE` / `BLONDON_COMMUNITIES_SERVICE_REF` (optional override)
- `BLONDON_COMMUNITIES_SERVICE_LABEL` (optional override)
- `BLONDON_COMMUNITIES_USER_AGENT` (optional override)
- `BLONDON_COMMUNITIES_MAX_RUNTIME_SECONDS` (optional; defaults to 120; used when BL runtime budget is enabled)
- `BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET` (optional; defaults to `true` on edge and `false` on Cloud Run)
- `LAQN_BASE_URL` (optional override for ERG LAQN API base URL)
- `LAQN_CONNECTOR_CODE` / `LAQN_SERVICE_REF` (optional override)
- `LAQN_CONNECTOR_LABEL` (optional override, `LAQN_SERVICE_LABEL` also accepted)
- `LAQN_CONNECTOR_DISPLAY_NAME` (optional override)
- `LAQN_USER_AGENT` (optional override)
- `LAQN_DEFAULT_GROUP` (optional override, default `London`)
- `LAQN_CSV_STATION_ID` / `LAQN_CSV_STATION_REF` (optional station selection for daily CSV uploads)
- `LAQN_CSV_DROPBOX_FOLDER` (optional override for ERG LAQN daily CSV folder; default `/connectors/erg_laqn`)
- `LAQN_RAW_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist override for ERG LAQN raw uploads)
- `LAQN_ERROR_DROPBOX_ALLOWED_SUPABASE_URL` (optional allowlist override for ERG LAQN error uploads)
- `LAQN_ERROR_DROPBOX_FOLDER` (optional override for ERG LAQN error folder)
- `LAQN_MAX_RUNTIME_SECONDS` (optional; defaults to 120)
- `SOS_MAX_RUNTIME_SECONDS` (optional; defaults to 120)
- `SB_UK_AQ_CRON_SECRET` (when set, ingest functions require `X-Cron-Secret`)

## Notes

- `ingest_sos` does not discover stations/timeseries; discovery happens in
  the Python ingest script (see `scripts/sos/sos_ingest.py`).
- `ingest_sensorcommunity` and `ingest_breathelondon` both upsert stations and
  timeseries as part of the poll.
- When `SB_UK_AQ_CRON_SECRET` is set, ingest functions require an `X-Cron-Secret`
  header that matches the secret.
- If `timeseries.station_id` is null, joins to stations will not work correctly.
  Run the discovery step to populate station links.
- Edge functions send `Accept-Profile` / `Content-Profile` headers for core/raw
  schemas (core by default; raw for `error_logs` and checkpoint tables). RPC calls
  in `uk_aq_dispatch_polls` target the `public` schema.
# Public network API contract

- `/api/aq/networks` maps to `uk_aq_public_networks` and returns
  `contract_version: 2`.
- The catalogue returns enabled networks only and includes `network_type`.
- Station, latest, chart, LA, and PCON rows expose scalar `network_id`,
  `network_code`, and `network_label`; station/latest rows omit `network_type`.
- `connector_id`, `connector_code`, and `connector_label` remain separate
  provenance where the endpoint already exposes them.
- Public query parameters `connector`, `connector_id`, and `connector_code`
  are unsupported and return HTTP 400.
- Hidden networks, including OpenAQ while disabled, do not appear in public
  responses. Both Breathe London connectors resolve to public code
  `breathelondon` and label `Breathe London`.
