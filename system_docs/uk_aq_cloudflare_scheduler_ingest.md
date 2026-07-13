# UK AQ Cloudflare Scheduler Ingest

## Architecture

The ingest scheduler uses the same generic runtime model as the ops scheduler:

```text
cloudflare/scheduler/jobs.toml
  -> GitHub Actions validation and sync
  -> D1 scheduler_jobs
  -> minute-triggered Cloudflare Worker
  -> authenticated Cloud Run POST
```

`jobs.toml` is the desired configuration source of truth. D1 remains the runtime
source read by the Worker. The Worker does not query Supabase, inspect ingest-run
state, or contain a hard-coded production job array.

## Resources

- Worker: `uk-aq-cron-scheduler-ingest`
- D1 database: `uk_aq_cron_scheduler_ingest_db`
- Binding: `SCHEDULER_DB`
- Trigger: `* * * * *`
- Dispatch lead: one minute before each configured job schedule
- Config sync: `.github/workflows/uk_aq_cloudflare_scheduler_ingest_config_sync.yml`
- Worker deploy: `.github/workflows/uk_aq_cloudflare_scheduler_ingest_deploy.yml`

The D1 schema uses `scheduler_jobs`, `scheduler_dispatches`, and
`scheduler_runs`. The unique `(job_key, due_at)` constraint prevents duplicate
dispatch of a scheduled slot. Each evaluation claims due slots and records
dry-run decisions first, then starts all claimed live job targets concurrently.
The run summary is recorded only after every live dispatch has settled.

## Jobs

| Job key | Schedule (UTC) | Request body | Initial mode |
|---|---:|---|---|
| `uk_aq_blondon_communities` | `*/15 * * * *` | `{"trigger_mode":"safety"}` | dry run |
| `uk_aq_blondon_nodes` | `*/15 * * * *` | `{}` | dry run |
| `uk_aq_scomm` | `*/15 * * * *` | `{"trigger_mode":"safety"}` | dry run |
| `uk_aq_sos` | `*/15 * * * *` | `{"trigger_mode":"safety"}` | dry run |
| `uk_aq_openaq_safety` | `*/30 * * * *` | `{"trigger_mode":"safety"}` | dry run |

OpenAQ safety semantics remain in the OpenAQ service. A safety request exits
successfully without ingest when a sufficiently recent run exists with status
`succeeded`, `success`, `partial`, or `skipped`; otherwise it starts the normal
OpenAQ work. The current guard does not independently prove that the previous
run created its next Cloud Task. The scheduler does not reproduce or alter that
state decision.

## URL ownership

Cloud Run service URLs are deployment-managed values. `jobs.toml` marks each URL
as `cloud_run_url_managed_by_deploy = true`, so a config sync preserves the
stored URL. Each corresponding Cloud Run deployment resolves `status.url`, calls
`scripts/cloudflare/uk_aq_reconcile_ingest_scheduler_url.sh`, updates only its
own D1 row, and verifies the stored value.

| Job key | Cloud Run service default | Deployment workflow |
|---|---|---|
| `uk_aq_blondon_communities` | `uk-aq-blondon-communities-ingest` | `uk_aq_blondon_communities_cloud_run_deploy.yml` |
| `uk_aq_blondon_nodes` | `uk-aq-blondon-nodes-ingest` | `uk_aq_blondon_nodes_cloud_run_deploy.yml` |
| `uk_aq_scomm` | `uk-aq-scomm-ingest` | `uk_aq_scomm_cloud_run_deploy.yml` |
| `uk_aq_sos` | `uk-aq-sos-ingest` | `uk_aq_sos_cloud_run_deploy.yml` |
| `uk_aq_openaq_safety` | `uk-aq-openaq-ingest` | `uk_aq_openaq_cloud_run_deploy.yml` |

The workflow variables can override these service defaults. Reconciliation uses
the deployed `SERVICE_NAME` and `gcloud run services describe ... status.url`,
not the scheduler job key or a guessed URL.

## Authentication

The Cloud Run services allow unauthenticated transport access because
Cloudflare cannot mint Google IAM identity tokens. POST execution remains
application-authenticated:

- Cloudflare sends `x-uk-aq-dispatch-secret`.
- Existing Google Cloud Scheduler and OpenAQ Cloud Tasks callers send
  `x-uk-aq-upstream-auth`.
- Both are checked against `UK_AQ_EDGE_UPSTREAM_SECRET` before work starts.
- Missing or incorrect credentials return HTTP 403.
- GET health checks remain available.

The same secret must exist as a GitHub Actions secret, a deployed Worker secret,
and a GCP Secret Manager secret mounted into all five services. Do not store it
in `jobs.toml`, D1, request bodies, or logs.

## Initial setup

Create the D1 database once, then replace `__INGEST_D1_DATABASE_ID__` in
`cloudflare/scheduler/wrangler.toml` with the returned UUID:

```bash
npx wrangler@4 d1 create uk_aq_cron_scheduler_ingest_db
npx wrangler@4 d1 migrations apply uk_aq_cron_scheduler_ingest_db \
  --remote \
  --config cloudflare/scheduler/wrangler.toml
```

Set the existing GitHub secrets `CLOUDFLARE_ACCOUNT_ID`,
`CLOUDFLARE_API_TOKEN`, and `UK_AQ_EDGE_UPSTREAM_SECRET`. Then run the config-sync
workflow, deploy the Worker, and redeploy each of the five Cloud Run services so
their actual URLs are reconciled into D1.

```bash
gh workflow run uk_aq_cloudflare_scheduler_ingest_config_sync.yml --ref main
gh workflow run uk_aq_cloudflare_scheduler_ingest_deploy.yml --ref main
gh workflow run uk_aq_blondon_communities_cloud_run_deploy.yml --ref main
gh workflow run uk_aq_blondon_nodes_cloud_run_deploy.yml --ref main
gh workflow run uk_aq_scomm_cloud_run_deploy.yml --ref main
gh workflow run uk_aq_sos_cloud_run_deploy.yml --ref main
gh workflow run uk_aq_openaq_cloud_run_deploy.yml --ref main
```

To rotate the shared secret, update the existing GitHub secret and rerun the
Worker and all five service deployment workflows. The service workflows upsert
the same value into GCP Secret Manager and retain authenticated headers on the
legacy Google Cloud Scheduler jobs. The OpenAQ deployment also mounts the value
used by its self-created Cloud Tasks.

```bash
gh secret set UK_AQ_EDGE_UPSTREAM_SECRET
gh workflow run uk_aq_cloudflare_scheduler_ingest_deploy.yml --ref main
```

## Verification

```bash
npx wrangler@4 d1 execute uk_aq_cron_scheduler_ingest_db \
  --remote \
  --config cloudflare/scheduler/wrangler.toml \
  --command "SELECT job_key, enabled, cron_expr, target_type, cloud_run_url, cloud_run_method, dry_run, updated_at FROM scheduler_jobs ORDER BY job_key;"

npx wrangler@4 d1 execute uk_aq_cron_scheduler_ingest_db \
  --remote \
  --config cloudflare/scheduler/wrangler.toml \
  --command "SELECT id, job_key, due_at, dispatch_status, reason, response_status, response_preview FROM scheduler_dispatches ORDER BY id DESC LIMIT 20;"

npx wrangler@4 d1 execute uk_aq_cron_scheduler_ingest_db \
  --remote \
  --config cloudflare/scheduler/wrangler.toml \
  --command "SELECT id, scheduler_name, started_at, finished_at, status, jobs_checked, jobs_due, jobs_claimed, jobs_dispatched, jobs_failed, error_message FROM scheduler_runs ORDER BY id DESC LIMIT 20;"
```

Expected initial state: five HTTPS Cloud Run URLs, five `dry_run = 1` rows, and
due slots recorded as `dry_run` without outbound Cloud Run requests.

To enable one job, set only that job's `dry_run = false`, merge to `main`, and
verify a successful authenticated dispatch before enabling another job. Rollback
is `dry_run = true`; use `enabled = false` when the job must stop claiming slots.

A scheduler HTTP 2xx means the service request completed successfully. Confirm
the actual ingest outcome separately in the service logs and ingest health/run
records. OpenAQ's 30-minute request is only a safety invocation; its normal
one-off Cloud Tasks self-scheduling remains unchanged.
