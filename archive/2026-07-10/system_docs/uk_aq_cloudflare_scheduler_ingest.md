# UK AQ Cloudflare Scheduler Ingest

This document covers the ingest-side phase-2 dry-run scheduler worker that evaluates fixed schedules and logs decisions without triggering Cloud Run yet.

## Current phase-2 scope

The canonical job definitions now live in:

- `cloudflare/scheduler/jobs.toml`

Validation and manifest sync are handled by:

- `.github/workflows/uk_aq_cloudflare_scheduler_ingest_config_sync.yml`

That workflow validates the TOML file and checks it against the worker's exported `INGEST_JOBS` table so the manifest and runtime stay in lockstep.

### Ingest scheduler

- Worker: `uk-aq-scheduler-ingest`
- Path: `cloudflare/scheduler/`
- Cron: `*/15 * * * *`
- Jobs:
  - `uk_aq_blondon_communities`
  - `uk_aq_blondon_nodes`
  - `uk_aq_scomm`
  - `uk_aq_sos`
  - `uk_aq_openaq_safety`

## Explicitly deferred

These jobs are intentionally not included in phase 2:

- `uk-aq-db-size-logger`
- `uk-aq-aqilevels-retention-service`
- `uk-aq-timeseries-aqi-hourly`

Keep them out until the state model is ready for a safe trigger path.

## State source

- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `uk_aq_core.uk_aq_ingest_runs`

## Logging

The worker logs one JSON decision record per job and a final summary record for each scheduled invocation.

## Cron

- `*/15 * * * *`

The worker evaluates ingest due-state one minute ahead of wall clock time to
offset Cloudflare scheduler lag.
