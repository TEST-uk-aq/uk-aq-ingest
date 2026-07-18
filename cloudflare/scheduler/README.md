# UK AQ Ingest Cron Scheduler

This directory contains the generic D1-backed Cloudflare scheduler for ingest
Cloud Run services.

## Runtime

- Worker: `uk-aq-cron-scheduler-ingest`
- D1 database: `uk_aq_cron_scheduler_ingest_db`
- D1 binding: `SCHEDULER_DB`
- Cloudflare cron: `* * * * *`
- Dispatch lead: 30 seconds
- Worker secrets: `UK_AQ_EDGE_UPSTREAM_SECRET` and
  `UK_AQ_SCHEDULER_TRIGGER_SECRET`

The Worker reads enabled jobs from `scheduler_jobs`, claims each `(job_key,
due_at)` once, and records scheduler runs and dispatch results in D1. Cloud Run
requests include `x-uk-aq-dispatch-secret`; secret values are never read from
`jobs.toml` or D1.

Cloudflare cron and authenticated `POST /run-if-due` calls share an atomic D1
UTC-minute claim. A duplicate trigger returns a bounded `already_claimed`
response and does not evaluate jobs or dispatch targets.

Ingest recovers from the latest finished scheduler evaluation window. If that
window contains multiple missed slots for one job, it selects only the latest
slot for that job in the recovery run.

## Configuration

`jobs.toml` is the reviewed desired configuration. D1 is the runtime source.
The config-sync workflow validates pull requests and applies idempotent upserts
after changes reach `main`. Deployment-managed Cloud Run URLs are preserved by
normal config syncs and are reconciled by each service deployment.

The current manifest mixes live dispatches with dry-run safeguards. OpenAQ
safety and SOS remain `dry_run = true`; the other enabled jobs dispatch live.

## Local validation

```bash
python3 cloudflare/scheduler/scripts/sync_jobs.py \
  --jobs-file cloudflare/scheduler/jobs.toml \
  --sql-file /tmp/ingest_scheduler_jobs.sql \
  --json-file /tmp/ingest_scheduler_jobs.json
node --test tests/cloudflare_scheduler_ingest.test.mjs
python3 -m unittest tests/test_cloudflare_scheduler_ingest_jobs_sync.py
```

No local validation command writes to remote D1.
