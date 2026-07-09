# UK AQ UK-AIR SOS Cloud Run

This document covers the Cloud Run path for UK-AIR SOS ingest.

## Scope

- Connector: `sos`
- Worker: `workers/uk_aq_sos_cloud_run`
- Scheduler: Google Cloud Scheduler -> Cloud Run Service
- Default service name: `uk-aq-sos-ingest`

## Connector toggle

Use `connectors.scheduler_backend` in the dashboard:

- `supabase_function`: handled by `uk_aq_dispatch_polls` (edge path)
- `google_cloud_run`: handled by Cloud Run worker

## Cadence model

- Cloud Scheduler can run frequently (for example every 2 minutes).
- Effective run cadence still comes from `connectors.poll_interval_minutes`.
- The worker checks due-state and claim-state before dispatch.
- Station batch size defaults to `connectors.poll_timeseries_batch_size` (dashboard `batch_size`);
  fallback is `SOS_STATION_BATCH_LIMIT` when connector batch size is unset.
- `batch_size` is a hard total cap across tier1, tier2, and stale station picks.

## Checkpoint model

- Edge path (unchanged):
  - selector: `uk_aq_core.sos_select_timeseries_ids`
  - checkpoint table: `uk_aq_raw.sos_timeseries_checkpoints`
- Cloud Run path:
  - selector: `uk_aq_core.sos_select_station_refs`
  - checkpoint table: `uk_aq_raw.sos_station_checkpoints`

Cloud Run picks due stations first, then scopes timeseries to those stations.
The station selector is constrained to stations that have at least one SOS timeseries row,
so worker runs do not churn on `no_timeseries_ids` for orphaned stations.
Rows with `uk_aq_core.timeseries.ended_at` set are excluded from selector/load scopes.

## Run safety

- Service entrypoint (`run_service.ts`) allows only one in-flight run per container.
- Worker claims connector via `uk_aq_public.uk_aq_rpc_dispatch_claim`.
- If claim is not acquired, the run exits without dispatch.
- In-flight guard + claim timeout prevent overlap under normal operation.

## Runtime writes

Per run, worker updates:

- `uk_aq_core.connectors` (`last_run_*`, and `last_polled_at` on success/partial)
- `uk_aq_core.uk_aq_ingest_runs` (dashboard run feed)
  - `last_observed_at` uses ingest payload when present; otherwise falls back to
    `max(timeseries.last_value_at)` across selected timeseries ids.
- `uk_aq_raw.error_logs` on ingest failure
  - When Dropbox error logging is enabled, the wrapper mirrors the inserted failure row into `/error_log/YYYY-MM-DD/` and patches `error_logs.dropbox_path`.
- `uk_aq_raw.sos_station_checkpoints` after successful/partial runs
- History observations via shared history mode (`OBSERVS_WRITE_MODE`, default `pubsub_only`)
- Dropbox artifacts use `uk_aq_*_cloud_run_*` filename prefixes
  (`SOS_DROPBOX_UPLOAD_SOURCE=cloud_run`).

## Deployment

- Workflow: `.github/workflows/uk_aq_sos_cloud_run_deploy.yml`
- Worker README: `workers/uk_aq_sos_cloud_run/README.md`
