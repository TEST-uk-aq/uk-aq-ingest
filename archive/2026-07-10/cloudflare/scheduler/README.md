# UK AQ Scheduler Ingest

Cloudflare Worker dry-run scheduler for ingest services.

This worker is owned by the ingest repo and deploys from
`.github/workflows/uk_aq_cloudflare_scheduler_ingest_deploy.yml`.

## Scope in phase 2

This worker only evaluates and logs decisions. It does not trigger Cloud Run yet.

Tracked jobs:

- `uk_aq_blondon_communities`
- `uk_aq_blondon_nodes`
- `uk_aq_scomm`
- `uk_aq_sos`
- `uk_aq_openaq_safety`

Canonical manifest:

- `cloudflare/scheduler/jobs.toml`

Sync validation:

- `.github/workflows/uk_aq_cloudflare_scheduler_ingest_config_sync.yml`

Excluded for now:

- `uk-aq-db-size-logger`
- `uk-aq-aqilevels-retention-service`
- `uk-aq-timeseries-aqi-hourly`

## State source

- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `uk_aq_core.uk_aq_ingest_runs`

## Logging

The worker logs one JSON decision record per job and a final summary record for each scheduled invocation.

## Cron

- `*/15 * * * *`

The worker evaluates each run one minute ahead of wall clock time to offset
Cloudflare scheduler lag.
