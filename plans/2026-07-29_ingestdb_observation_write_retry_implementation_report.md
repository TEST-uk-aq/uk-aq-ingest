# IngestDB observation-write retry implementation report

Date: 2026-07-29  
Starting HEAD: `6ab46abddb123befe055719795a494c33dccdff1`

## Scope and incident evidence

This implementation applies `system_docs/ingestdb_observation_write_retry_contract.md` to the active IngestDB observation writers. The supplied incident evidence records Sensor.Community SQLSTATE `57014` statement timeouts on 2026-07-22 and 2026-07-23, with the independent ObsAQIDB write succeeding while the IngestDB write failed.

No historical rows were repaired, no ObsAQIDB rows were deleted or rolled back, and no durable retry queue or cross-database transaction was added.

## Writers inspected and changed

The active or deployable observation writers found were:

- Sensor.Community standalone Cloud Run Node worker;
- Sensor.Community Edge Function fallback;
- Breathe London Communities Edge Function, also packaged for Cloud Run;
- OpenAQ Edge Function implementation, also packaged for Cloud Run;
- ERG LAQN Edge Function;
- UK-AIR SOS Edge Function, also packaged for Cloud Run;
- current Python writers for Breathe London Nodes, ERG LAQN, Sensor.Community and UK-AIR SOS.

All of these writers now use a shared retry implementation. No active writer was deferred. An unused single-attempt observation method in the Breathe London Communities station-listing script was removed so it cannot become a non-compliant bypass.

## Shared design

- JavaScript/Deno/Node helper: `supabase/functions/_shared/ingestdb_observation_writer.mjs`
- Python equivalent: `scripts/uk_aq_ingestdb_observation_writer.py`
- Canonical idempotency key: `(connector_id, timeseries_id, observed_at)`
- Defaults: 3 total attempts, 500 ms base delay, 5000 ms maximum delay, positive jitter, 25-row minimum split size and maximum split depth 5.
- Configuration parsing is bounded. No new environment variables were added.
- Persistent statement timeouts split sequentially into two non-empty ordered children. Successful children are retained if a sibling later fails.
- Retryable classifications cover statement timeout, deadlock, serialization failure, connection failure, HTTP 429 and transient HTTP 500/502/503/504 responses. Clearly permanent database, schema, authentication, payload and constraint errors fail immediately.
- Terminal errors preserve their cause and bounded code/status/chunk/attempt/depth/unresolved-row diagnostics.
- Existing runtime callbacks are passed by Edge callers. The reserved attempt duration now matches each caller's bounded PostgREST request timeout. Paths without an existing runtime callback retain bounded attempts and request timeouts without inventing a new application runtime budget.

## Checkpoint and completion review

- Sensor.Community metadata is created before observation writes, but `timeseries.last_value` and `last_value_at` are now applied only after IngestDB commits. Terminal IngestDB failure prevents successful completion and connector polling-state advancement.
- Breathe London Communities and ERG LAQN build timeseries/checkpoint updates only after their corresponding observation writes complete; terminal shared-writer errors escape to the failed run response.
- OpenAQ performs the IngestDB write before latest-value and checkpoint updates, so a terminal write error stops those updates.
- SOS records successful timeseries IDs separately, advances only those checkpoints, and withholds connector `last_polled_at` after any terminal IngestDB write failure.
- Scheduled Python SOS and Breathe London Nodes paths now re-raise terminal shared-writer errors instead of converting them into a successful process exit.
- ObsAQIDB writes remain independent. Run output reports IngestDB and ObsAQIDB results separately and includes `cross_database_transaction: false` where both stores are involved.

## Reporting

Shared statistics report:

- `input_rows`
- `normal_chunk_size`
- `committed_rows`
- `write_requests`
- `retry_attempts`
- `retried_chunks`
- `split_operations`
- `smallest_attempted_chunk`
- `unresolved_rows`
- `terminal_failure_classification`
- `terminal_reason`
- `stopped_for_runtime_budget`

Connector `observations_upserted` values now derive from confirmed IngestDB committed rows. Retry, split and terminal logs contain bounded operational context and never include observation payloads or secrets.

## Files changed

- Shared helpers and focused tests under `supabase/functions/_shared/`, `scripts/` and `tests/`.
- Observation writers under `supabase/functions/ingest_breathelondon/`, `ingest_erg_laqn/`, `ingest_openaq/`, `ingest_sensorcommunity/` and `ingest_sos/`.
- Python writers under `scripts/blondon_nodes/`, `scripts/erg_laqn/`, `scripts/sensorcommunity/` and `scripts/sos/`.
- Sensor.Community Cloud Run Node worker.
- Relevant Cloud Run Dockerfiles and deployment workflow path filters so shared helpers are included in images and trigger rebuilds.
- The dormant observation writer was removed from `scripts/blondon_communities/blondon_communities_list_stations.py`.

## Focused validation

The following local checks passed without network or database access:

- `node --test tests/ingestdb_observation_writer.test.mjs`: 14 passed.
- `.venv/bin/python -m pytest -q tests/test_ingestdb_observation_writer.py`: 6 passed.
- `deno check` for all five changed Edge Function files.
- `python3 -m py_compile` for the shared Python helper and changed Python writers.
- `node --check` for the shared JavaScript helper and Sensor.Community Cloud Run worker.
- `git diff --check`.

The tests inject sleep and randomness and cover the normal path, bounded backoff and jitter, retry classification, adaptive splitting, successful-child retention, terminal limits, committed/unresolved accounting, canonical-key idempotency, runtime-budget classification, caller adoption, checkpoint ordering and bounded configuration.

## Archive handling

The existing `archive/2026-07-29_ingestdb_observation_retry/` snapshot was reused. No duplicate same-day snapshots were created. The previously unsnapshotted active Breathe London Communities station-listing script was captured once before its dormant method was removed. Tests and `system_docs/` were not archived.

## Deliberately unchanged

- no durable retry queue;
- no ObsAQIDB-to-IngestDB reconciliation;
- no historical missing-row repair;
- no ObsAQIDB rollback or deletion;
- no cross-database transaction;
- no polling-schedule, Prune Daily, R2 history or LIVE changes;
- no deployment or real ingest was run.

## TEST deployment and operational validation

After review and commit, deploy the affected TEST Edge Functions and Cloud Run services. Confirm one ordinary run per active connector, with `retry_attempts=0`, `split_operations=0`, `unresolved_rows=0`, and `observations_upserted=committed_rows`. Confirm ObsAQIDB output remains separately reported and checkpoints advance normally.

When a genuine transient statement timeout next occurs, confirm retries and any splits are logged, committed-row accounting remains accurate, and a terminal failure returns a failed/partial run with the exact classification and unresolved-row count. Do not inject a synthetic database fault solely for this validation.

## System-documentation handover

Repository rules reserve `system_docs/` edits for ChatGPT in Chat mode. Update `system_docs/uk_aq_edge_functions.md`, `system_docs/uk_aq_scripts.md`, the relevant connector pages, and Cloud Run pages to identify the shared helpers, adopted connectors, defaults, retry metrics, checkpoint ordering and explicit lack of a cross-database transaction. Link to the authoritative contract rather than duplicating it. No schema or environment-variable documentation change is required.
