# UK AQ Dispatcher + Ingest Flow

This doc explains the current two-stage dispatcher flow and why the Cloudflare worker calls the dispatcher once for enqueue and then run-queue calls per cron tick.

## Overview

The scheduler is split into selection and execution:

1. `mode=enqueue`:
- Selects due connectors from `uk_aq_core.connectors`.
- Writes queue jobs into `uk_aq_raw.dispatch_connector_queue` (one row per connector).
- Returns quickly.

2. `mode=run_queue`:
- Claims queued jobs from `uk_aq_raw.dispatch_connector_queue`.
- Executes one queued connector ingest job per call (when `run_queue_claim_limit=1`).
- Resolves the queue job as success/failure with retry backoff.

This removes long ingest runtime from due-selection calls.

## Why the Worker Calls Both

The worker must do both actions in sequence:

- If it only calls `mode=enqueue`, jobs accumulate but no ingest runs.
- If it only calls `mode=run_queue`, no new due connectors are added to the queue.

Calling both in one tick guarantees:
- due connectors keep entering the queue, and
- queued jobs can be processed in parallel via run-queue fan-out calls.

## Queue Objects

Queue table:
- `uk_aq_raw.dispatch_connector_queue`

Queue RPCs:
- `uk_aq_core.uk_aq_dispatch_queue_enqueue(p_entries jsonb)`
- `uk_aq_core.uk_aq_dispatch_queue_claim(p_batch_limit int, p_lease_seconds int)`
- `uk_aq_core.uk_aq_dispatch_queue_resolve(p_resolutions jsonb)`

## Dispatcher Modes

`uk_aq_dispatch_polls` supports:
- `mode=legacy` (default when mode is omitted)
- `mode=enqueue`
- `mode=run_queue`

Worker behavior:
- Calls `mode=enqueue`, reads `dispatcher_settings.max_runs_per_dispatch_call`, then calls `mode=run_queue` that many times in parallel.
- If a queue-mode call fails, falls back to `mode=legacy` for that cron tick.

## Runtime + Backoff

Queue retry behavior:
- Failed jobs increment `attempts`.
- `next_attempt_at` uses backoff (30s, 120s, 600s, then 1800s default).
- Claimed jobs have a lease (`lease_expires_at`) so interrupted runs can be recovered.
- Queued jobs for disabled connectors are resolved and dropped (`queue_entry_disabled_connector`).
- Queued jobs for connectors with `scheduler_backend='google_cloud_run'` are resolved and dropped (`queue_entry_external_scheduler`).
- Run-overlap guard: if a connector run is still active, or started within that connector's poll interval, dispatcher does not start another run and requeues the claimed job with retry.

Relevant env vars:
- `DISPATCH_QUEUE_CLAIM_BATCH_LIMIT` (default `1`)
- `DISPATCH_QUEUE_LEASE_SECONDS` (default `900`)
- `DISPATCH_TIME_BUDGET_MS` (default `150000`)
- `DISPATCH_SHUTDOWN_BUFFER_MS` (default `10000`)
- `DISPATCH_EDGE_CALL_TIMEOUT_MS` (default `140000`)
- `DISPATCH_MIN_START_EDGE_CALL_MS` (default `30000`)
- Effective queue claim size per `run_queue` call:
  `run_queue_claim_limit` request payload value (when provided), else `DISPATCH_QUEUE_CLAIM_BATCH_LIMIT`.
- Worker currently sends `run_queue_claim_limit=1` per run-queue call so one claim maps to one ingest run.

## Operational Notes

- Single concurrency dial: `dispatcher_settings.max_runs_per_dispatch_call`.
- Scheduler backend toggle:
  - `connectors.scheduler_backend='supabase_function'`: connector runs via dispatcher.
  - `connectors.scheduler_backend='google_cloud_run'`: dispatcher skips it and expects external scheduling.
  - Current Cloud Run connectors: `sos`, `sensorcommunity`, `blondon_communities`, `openaq`.
  - OpenAQ Cloud Run scheduling is due-driven (one-off Cloud Tasks based on `openaq_station_checkpoints.next_due_at`) with a 15-minute safety cron.
- Use queue mode for normal operation; use `mode=legacy` only as fallback/debug.
- Monitor:
  - `uk_aq_raw.dispatch_connector_queue` row count, attempts, and last_error
  - `uk_aq_core.uk_aq_ingest_runs` for per-connector outcomes
  - `uk_aq_raw.history_observation_outbox` via dedicated Cloud Run flusher job (`workers/uk_aq_observs_outbox_cloud_run`) on a 10-minute schedule
  - History writes default to outbox-first (`OBSERVS_WRITE_MODE=outbox_only`), so lag in history is expected between ingest completion and outbox flush.
  - For connectors configured with `OBSERVS_WRITE_MODE=pubsub_only` (for example OpenAQ Cloud Run), monitor Pub/Sub backlog + `workers/uk_aq_observs_pubsub_cloud_run` hourly drain summaries instead of main DB outbox depth.
