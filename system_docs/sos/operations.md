# UK-AIR SOS operations

## Deployment ownership

The authoritative deployment path for the SOS Cloud Run service is:

```text
.github/workflows/uk_aq_sos_cloud_run_deploy.yml
```

Manual workflow dispatch:

```bash
gh workflow run uk_aq_sos_cloud_run_deploy.yml --ref main
```

The workflow also runs automatically on pushes to `main` that affect the SOS Cloud Run worker or its copied ingest runtime dependencies.

Use the workflow rather than a bare `gcloud run deploy` command. The workflow owns:

- image build and Artifact Registry push;
- the configured runtime service account;
- timeout, CPU, memory, concurrency and instance limits;
- environment variables;
- Secret Manager bindings;
- service labels;
- Cloud Scheduler creation or update;
- Cloud Run invocation permissions;
- reconciliation of the deployed service URL into the Cloudflare scheduler D1 configuration.

A direct `gcloud run deploy` command that omits these settings does not reproduce the repository deployment contract.

## Component deployment scope

The child-result contract is packaged into the Cloud Run image from:

- `workers/uk_aq_sos_cloud_run/result_contract.ts`;
- `workers/uk_aq_sos_cloud_run/run_job.ts`;
- `workers/uk_aq_sos_cloud_run/run_service.ts`;
- `workers/uk_aq_sos_cloud_run/Dockerfile`.

Changes limited to these Cloud Run files require the Cloud Run workflow only.

Changes to `supabase/functions/ingest_sos/**` also require the Supabase edge function to be deployed if those changes have not already reached TEST. The repository-wide Supabase deploy workflow is `.github/workflows/supabase_edge_deploy.yml`.

Do not redeploy an unchanged component solely because another component uses the same documented contract.

## Scheduling

The default path is:

```text
Google Cloud Scheduler
  -> Cloud Run service
  -> run_service.ts
  -> run_job.ts
  -> local ingest_sos server
```

Cloud Scheduler may invoke the service more frequently than the connector interval. Effective polling cadence remains controlled by `connectors.poll_interval_minutes` and the worker's due-state and claim-state checks.

The Cloud Run service allows one in-flight child run per container. The connector claim adds database-backed overlap protection.

## Result-file operation

For each accepted Cloud Run POST:

1. `run_service.ts` creates a unique temporary result file.
2. It passes the path to `run_job.ts` as `SOS_RUN_RESULT_PATH`.
3. The child writes one bounded result for every intentional successful exit.
4. The service validates and returns the result after a successful child exit.
5. Missing or invalid results fail closed with HTTP 500.
6. The file is removed in `finally`.

The temporary path requires no persistent environment configuration.

## Run persistence

The Cloud Run worker updates:

- `uk_aq_core.connectors.last_run_start`;
- `uk_aq_core.connectors.last_run_end`;
- `uk_aq_core.connectors.last_run_status`;
- `uk_aq_core.connectors.last_run_message`;
- `uk_aq_core.connectors.last_polled_at` for successful or partial runs;
- `uk_aq_core.uk_aq_ingest_runs` for persisted attempts;
- `uk_aq_raw.sos_station_checkpoints` after successful or partial polling;
- `uk_aq_raw.error_logs` for edge-owned errors and child-job wrapper failures caught inside `run_job.ts`.

Not-due and claim-not-acquired results are scheduler-level skips and do not create new ingest-run rows. `no_station_refs` and `no_timeseries_ids` retain their persisted skipped-run behaviour.

## Error evidence

### Recognised dependency failure

Expected evidence includes:

- the real upstream HTTP status preserved through Cloud Run, including HTTP 502, or HTTP 503 for a recognised timeout/deadline with no upstream response;
- `status: upstream_unavailable`;
- truthful `upstream_status`;
- `upstream_failure_kind`;
- `connector_http_status`;
- one edge-owned upstream probe error record;
- no second generic Cloud Run wrapper error or Dropbox error JSON for the same failure.

### Runtime-budget stop

Expected evidence includes:

- HTTP 207;
- `partial: true`;
- `stopped_reason: runtime_budget_exceeded`;
- one consolidated run-level error record;
- bounded deadline count and timeseries sample;
- normal individual records only for non-deadline timeseries failures.

### Child-job wrapper failure

A process, configuration, persistence or unexpected child-job failure exits non-zero, returns generic HTTP 500 and uses the `run_job.ts` catch path. That path attempts connector failure persistence, a wrapper-owned error row and optional Dropbox error JSON.

### Result-contract failure

A code-zero child with an empty or invalid result file returns HTTP 500 with `missing_child_result` or `invalid_child_result`.

This branch is detected by `run_service.ts` after the child has exited successfully. It is visible in the service response and Cloud Run request logs, but it does not pass through the child-job catch path and therefore does not by itself guarantee a new database error row or Dropbox error JSON.

## Logging

Depending on configured Dropbox credentials and allowlist:

- normal logs are written under `/connectors/sos/log/YYYY-MM-DD/`;
- raw captures are written under `/connectors/sos/raw_data/YYYY-MM-DD/`;
- error JSON is written under `/error_log/YYYY-MM-DD/`;
- Cloud Run filenames use the `uk_aq_*_cloud_run_*` prefixes.

Recognised dependency failures must not produce a duplicate wrapper-owned error JSON.

## Rollback

For a regression in the result-contract change:

1. revert the relevant Cloud Run worker commit;
2. redeploy through `uk_aq_sos_cloud_run_deploy.yml`;
3. confirm the service revision points to the rollback image;
4. run one normal TEST invocation;
5. check connector and ingest-run state for the expected status.

No schema, scheduler cadence, connector interval, timeout, concurrency or environment-variable rollback is required for the result-contract correction itself.

## Cost impact

Healthy-run Supabase egress and database writes are unchanged.

During upstream or runtime incidents, the contract reduces duplicate wrapper error rows and duplicate Dropbox error JSON. It does not introduce a new persistent table, scheduled job or long-lived file.