# UK-AIR SOS polling contract

## Scope

This document defines the required behaviour of current UK-AIR SOS observation polling for connector code `sos`.

It covers both:

- the Supabase `ingest_sos` handler; and
- the Cloud Run service that selects work, invokes that handler locally, persists run state and returns the bounded result.

Discovery, site-register and archive-mapping behaviour remains in [`../sos.md`](../sos.md) during the gradual documentation migration.

## Shared ingest behaviour

Both scheduler backends use the same `ingest_sos` implementation:

- `scheduler_backend='supabase_function'`: the dispatcher calls the edge function;
- `scheduler_backend='google_cloud_run'`: the Cloud Run worker selects stations and scoped internal `timeseries.id` values, then invokes the same handler locally.

Polling MUST:

- use the existing `sos` connector row;
- select only active timeseries where `ended_at is null`;
- preserve internal `timeseries.id` scoping without falling back to source `timeseries_ref`;
- write observations keyed by `connector_id`, `timeseries_id` and `observed_at`;
- update last-value state only from parsed source data;
- preserve genuine per-timeseries failures as individual errors unless they are runtime-deadline fan-out failures.

## Upstream probe failures

Before polling selected timeseries, `ingest_sos` probes the configured UK-AIR SOS base URL.

Failures are classified as one of:

- `http`
- `request_timeout`
- `runtime_deadline`
- `network`
- `unknown`

The connector-facing HTTP status MUST follow these rules:

- a real upstream HTTP response keeps its actual status, including HTTP 502;
- `request_timeout` or `runtime_deadline` with no upstream HTTP response maps to HTTP 503;
- `network` or `unknown` with no upstream HTTP response remains HTTP 500.

A probe failure response MUST use `status: upstream_unavailable` and include truthful structured evidence:

- `upstream_status`: the real upstream HTTP status, or `null` when no HTTP response was received;
- `upstream_failure_kind`;
- `connector_http_status`;
- bounded `upstream_error` in the edge response and error record context;
- `series_polled: 0`;
- `observations_upserted: 0`.

The system MUST NOT invent an upstream status when the request timed out or was aborted before a response.

## Recognised dependency failures

The Cloud Run wrapper may treat a non-2xx ingest response as a completed dependency result only when the structured response is internally consistent.

A recognised dependency failure requires:

- `status: upstream_unavailable`;
- `connector_http_status` equal to the ingest HTTP status; and
- either:
  - `upstream_failure_kind: http` with `upstream_status` equal to that HTTP status; or
  - `upstream_failure_kind: request_timeout` or `runtime_deadline`, `upstream_status: null`, and HTTP 503.

A recognised dependency failure MUST:

- retain HTTP 502 or 503 through the outer Cloud Run service;
- persist the connector and ingest-run result as `failed` with a meaningful upstream message;
- retain the bounded diagnostic fields in the Cloud Run response;
- avoid the generic Cloud Run wrapper exception path;
- avoid a second wrapper-owned `error_logs` row or Dropbox error JSON for the same dependency incident.

An arbitrary HTTP 502 or 503 response without the validated structured contract MUST NOT be treated as a recognised dependency result.

## Wrapper and local failures

Failures in Cloud Run process startup, local service startup, configuration, malformed ingest output, persistence or unexpected wrapper code are wrapper failures.

Wrapper failures MUST:

- exit the child job non-zero;
- return generic HTTP 500 from the outer service;
- use the existing wrapper error logging path;
- never be reclassified as an upstream availability failure merely because an HTTP status resembles 502 or 503.

## Runtime-budget behaviour

The ingest runtime budget prevents new SOS requests from starting once the budget is exhausted.

A genuine incomplete runtime stop MUST return:

- HTTP 207;
- `partial: true`;
- `stopped_reason: runtime_budget_exceeded`;
- `runtime_deadline_failure_count`;
- a bounded `runtime_deadline_timeseries_sample`.

The system MUST write one run-level runtime-budget error record whenever work stopped before completion, including when the pool stopped before scheduling another request and therefore has zero in-flight deadline failures.

Runtime-deadline fan-out failures MUST NOT create one error record per affected timeseries.

A bare clock check after all selected work completed MUST NOT mark the run partial.

## Individual failure count

`individual_error_count` means the number of timeseries failures that were reported through the individual per-timeseries error path.

It MUST include genuine request, HTTP, parsing, database, observation-write and last-value failures reported individually.

It MUST NOT be derived from the general response `errors` array and MUST NOT include:

- the consolidated runtime-budget token;
- runtime-deadline failures included in the run-level aggregate;
- observation-buffer flush warnings;
- connector metadata update failures;
- other run-level infrastructure errors.

## Intentional skips

Every intentional successful Cloud Run child exit MUST write a valid child result before returning.

Current skip reasons include:

- connector disabled or otherwise not due, including `not_due`, `in_flight` and `scheduler_backend_not_cloud_run`;
- `claim_not_acquired`;
- `no_station_refs`;
- `no_timeseries_ids`.

Skipped child results MUST return HTTP 200 with:

- `ok: true`;
- `status: skipped`;
- `run_status: skipped`;
- bounded `run_message`;
- bounded `reason`;
- `connector_id` when known.

The result-file contract does not change existing database semantics:

- not-due and claim-not-acquired exits remain scheduler-level skips without a new ingest-run row;
- `no_station_refs` and `no_timeseries_ids` retain their existing persisted skipped-run behaviour.

## Behaviour deliberately unchanged

The failure-handling work does not change:

- scheduler cadence;
- connector polling intervals;
- station or timeseries batch size;
- concurrency;
- Cloud Run timeout;
- database schema;
- environment variable names;
- pollutant mappings;
- source discovery or timeseries lifecycle reconciliation.