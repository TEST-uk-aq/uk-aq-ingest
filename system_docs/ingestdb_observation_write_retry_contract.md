# IngestDB Observation Write Retry Contract

## Purpose

This contract defines the required behaviour for writing source observations to the authoritative IngestDB observation table when a transient database or transport failure occurs.

It applies to every ingest implementation that writes observations into `uk_aq_core.observations`, including Supabase Edge Functions and Cloud Run workers.

The objective is to prevent genuine source observations from being omitted from IngestDB because one bounded observation upsert encountered a transient failure such as a PostgreSQL statement timeout.

## Authority and scope

IngestDB is the authoritative current observation store used by the website and downstream operational processing.

ObsAQIDB is a separate analytical observation store. A successful ObsAQIDB write does not prove that the corresponding IngestDB write succeeded, and an ObsAQIDB write must not be rolled back merely because the IngestDB write needs to be retried.

This contract covers immediate in-process retries and adaptive chunk splitting only.

It does not introduce:

- a durable IngestDB observation retry queue;
- cross-database transactions;
- deletion or rollback of genuine ObsAQIDB observations;
- reconciliation from ObsAQIDB back into IngestDB;
- changes to connector polling schedules or source checkpoint semantics.

## Observation identity and idempotency

Observation writes must remain idempotent on the canonical observation key:

```text
(connector_id, timeseries_id, observed_at)
```

Retries must use the same upsert conflict target and merge behaviour as the original write.

A retry of a previously committed chunk must update or retain the same logical observations and must not create duplicate observations.

Changed `value` or `status` values at the same canonical key must continue to follow the existing upsert semantics.

## Required write behaviour

Every IngestDB observation writer must use one shared retry-capable write path, or a provably equivalent implementation, with the following behaviour.

### Initial bounded write

1. Prepare and deduplicate the observation rows using the existing connector rules.
2. Divide the prepared rows into the configured normal write chunk size.
3. Attempt each chunk using the existing IngestDB observation upsert operation.
4. Do not advance completion reporting for a chunk until that chunk has succeeded.

### Retryable failures

A failed chunk may be retried only when the failure is classified as transient.

Retryable failures include, where exposed by PostgreSQL, PostgREST or the HTTP client:

- PostgreSQL statement timeout or query cancellation caused by timeout, including SQLSTATE `57014` when the message identifies a statement timeout;
- deadlock detection;
- serialization failure;
- connection termination or reset;
- temporary network failure;
- HTTP `429`;
- HTTP `500`, `502`, `503` or `504` when the response is consistent with a transient service or database failure.

Classification must use structured status or error codes where available and may use bounded message matching only where the calling interface does not expose a structured code.

### Non-retryable failures

The writer must fail immediately for errors that are not safely classified as transient, including:

- malformed observation payloads;
- authentication or authorization failures;
- missing required metadata;
- unknown columns or invalid SQL;
- deterministic constraint failures caused by invalid data;
- invalid connector, timeseries or timestamp values.

A non-retryable failure must not be hidden by a generic retry loop.

## Immediate retry policy

For a retryable failure, the writer must retry the same chunk a bounded number of times using exponential backoff with jitter.

The implementation must provide safe defaults and may allow environment configuration, but configuration must be bounded and validated.

Recommended defaults are:

- maximum direct attempts per chunk: `3` total attempts, including the initial attempt;
- base retry delay: `500` milliseconds;
- maximum retry delay: `5000` milliseconds;
- positive random jitter applied to each retry delay.

The exact defaults may differ where an existing shared runtime contract requires them, but retries must never be unbounded inside one ingest invocation.

## Adaptive chunk splitting

When a retryable statement-timeout failure persists after the direct retry attempts, the writer must split the failed chunk into smaller chunks and retry them independently.

Required behaviour:

1. Split the failed chunk into two non-empty halves.
2. Preserve row order unless the existing writer explicitly treats order as irrelevant.
3. Apply the same retry policy independently to each child chunk.
4. Continue recursive splitting only for retryable statement-timeout failures.
5. Stop splitting at a configured minimum chunk size or configured maximum split depth.
6. If a minimum-size chunk still fails after its allowed attempts, fail the ingest write and report the unresolved rows.

Recommended defaults are:

- minimum split chunk size: `25` rows;
- maximum split depth: `5`.

The implementation must prevent empty child chunks, uncontrolled recursion and unbounded request counts.

A successful child chunk must not be repeated merely because its sibling later fails. The whole ingest run may report failure or partial failure according to the existing connector contract, but the writer must retain accurate committed-row accounting.

## Runtime-budget behaviour

Retry and split processing must respect the existing runtime budget of the Edge Function or Cloud Run worker.

The writer must not start another retry or split child when the remaining runtime budget is insufficient for a safe attempt and orderly shutdown.

When stopped by the runtime budget, the result must distinguish this from:

- exhausted retry attempts;
- minimum-chunk failure;
- non-retryable failure.

This contract does not require a durable queue for unresolved rows. Until such a queue is explicitly designed, an exhausted or budget-stopped write must remain a visible ingest failure requiring operational repair.

## IngestDB and ObsAQIDB independence

IngestDB observation write retries must not delete, undo or invalidate genuine rows that have already reached ObsAQIDB.

Where the current connector writes IngestDB and ObsAQIDB concurrently, the implementation may retain that independence in this change.

However:

- IngestDB success and ObsAQIDB success must be reported separately;
- an ObsAQIDB success must not cause the IngestDB run to be reported as successfully written;
- an IngestDB failure must retain its exact failure classification and unresolved-row count;
- the absence of a cross-database transaction must be explicit in implementation documentation and operational reporting.

## Checkpoint and completion safety

A connector must not advance a source checkpoint, completion marker or equivalent state past observations that the connector contract requires to be present in IngestDB unless that state can safely cause the same source observations to be fetched again.

Codex must inspect each changed connector's checkpoint behaviour before reusing the shared retry helper.

If a connector's source is latest-value-only and cannot replay an omitted timestamp on the next poll, exhausting retries must be treated as a material ingest failure.

No checkpoint behaviour may be changed silently as part of adding retries.

## Reporting and observability

The write result and connector run summary must preserve enough information to diagnose retry behaviour.

At minimum, report or log:

- prepared observation row count;
- normal chunk size;
- successful committed row count;
- number of write requests;
- number of retry attempts;
- number of chunks that required retry;
- number of split operations;
- smallest attempted chunk size;
- unresolved row count;
- terminal failure classification;
- whether processing stopped for runtime budget.

Logs must not include secrets or unbounded observation payloads.

Existing high-level fields such as `observations_upserted` must represent successfully completed IngestDB writes, not merely attempted rows.

## Failure semantics

The writer must fail closed when any prepared IngestDB observation remains unresolved after the allowed retry and split process.

The terminal error must retain the most useful available cause and include bounded context such as:

- PostgreSQL or HTTP status/code;
- original chunk size;
- final child chunk size;
- attempt count;
- split depth;
- unresolved row count.

A retryable failure that later succeeds must not leave the connector run marked as failed, but its retries and splits must remain visible in diagnostics.

## Structural implementation requirements

The preferred implementation is a shared helper used by all relevant IngestDB observation writers.

The helper must:

- accept an explicit operation that performs one idempotent observation upsert;
- classify retryable failures consistently;
- implement bounded retry, backoff and jitter;
- implement statement-timeout-only adaptive splitting;
- return accurate aggregate statistics;
- preserve the original terminal error as the cause when all recovery attempts fail;
- be testable without a real database.

Connector-specific wrappers may translate HTTP/PostgREST errors into the shared error representation, but must not weaken this contract.

## Deployment and validation

Before deployment, validate only that:

- the shared helper is structurally viable;
- all changed callers pass idempotent observation chunks;
- runtime and environment configuration parsing is bounded;
- syntax and directly relevant focused tests pass.

Functional validation must occur after deployment through real operations on the TEST system.

The first TEST validation should confirm:

1. an ordinary ingest succeeds without retries;
2. retry statistics remain zero on the normal path;
3. a real transient statement timeout, when encountered, is retried and split rather than immediately losing the source observations;
4. `observations_upserted` reflects committed IngestDB rows;
5. ObsAQIDB behaviour remains unchanged;
6. no checkpoint advances incorrectly after a terminal IngestDB write failure.

Do not introduce a synthetic production-database fault solely to test retries unless a targeted pre-deployment check is explicitly approved.

## Deferred work

The following remain separate future decisions:

- durable IngestDB observation retry queue;
- automated reconciliation from ObsAQIDB or raw source archives;
- connector-independent cross-database consistency monitoring;
- transactional coupling between IngestDB observation upsert and ObsAQIDB outbox enqueue.
