# Compact observation transport and metadata ownership implementation plan

Date: 2026-08-12
Status: ready for implementation
Primary implementation repository: `TEST-uk-aq/uk-aq-ingest`
Schema repository: `TEST-uk-aq/uk-aq-schema`
Authoritative contract: `uk-aq-system-docs/system_docs/ingest/compact_observation_transport_and_metadata_ownership_contract.md`

This plan implements the promoted compact observation transport and metadata ownership contract. It is an implementation plan, not a second behavioural authority.

## Codex prompts

### Prompt A: schema phase

**Recommended Codex model: Codex with High reasoning.**

Run this prompt in the `TEST-uk-aq/uk-aq-schema` repository before the ingest phase:

```text
Implement the schema phase of the UK AQ compact observation transport plan.

Before editing:
1. Read this repository's AGENTS.md / repository guidance.
2. Read the authoritative system contract from the local uk-aq-system-docs clone:
   system_docs/ingest/compact_observation_transport_and_metadata_ownership_contract.md
3. Read the current canonical IngestDB and ObsAQIDB RPC definitions and the migration conventions used by this repository.
4. Treat system_docs as read-only authority. Do not edit system documentation.

Required behaviour changing:
- Add additive compact typed-array observation RPCs for IngestDB and ObsAQIDB.
- Add a separate compact IngestDB latest-value RPC.
- Add the minimum Sensor.Community station-state resolver and presence-touch database interfaces needed by the authoritative contract.

Behaviour that MUST remain unchanged:
- observation values, timestamps and eligibility;
- logical duplicate identities and update-on-change semantics;
- IngestDB connector-based observation routing;
- ObsAQIDB daily observed_at partition routing and current hot-write window;
- service-role protection and relevant statement-timeout semantics;
- station/timeseries identities;
- all existing object-row RPCs during the additive TEST rollout;
- scheduler, polling, checkpoint, retry, Pub/Sub and R2 behaviour.

Implement all schema tasks in Phase 1 of:
  uk-aq-ingest/plans/2026-08-12 Compact observation transport/2026-08-12_Compact_observation_transport_plan.md

Use typed PostgreSQL arrays exactly as authorised by the contract. Validate all vector lengths and all required timeseries IDs before ambiguous writes. Do not silently discard unresolved IDs.

Do not create a speculative test suite. After implementation, run only the repository's existing targeted structural/static validation that is appropriate for changed SQL and inspect the final diff. Do not deploy to LIVE. Report the canonical files and migration files changed, the exact RPC signatures created, structural checks run, and any apply steps required for TEST.
```

### Prompt B: ingest phase

**Recommended Codex model: Codex with High reasoning.**

Run this prompt in the `TEST-uk-aq/uk-aq-ingest` repository only after the additive schema RPCs from Prompt A have been applied successfully to both TEST databases:

```text
Implement the ingest/runtime phase of the UK AQ compact observation transport plan.

Before editing:
1. Read AGENTS.md, AGENTS_BASE.md and all repository guidance that applies to the files you will touch.
2. Read the authoritative system contract from the local uk-aq-system-docs clone:
   system_docs/ingest/compact_observation_transport_and_metadata_ownership_contract.md
3. Read this plan completely:
   plans/2026-08-12 Compact observation transport/2026-08-12_Compact_observation_transport_plan.md
4. Inspect every active current call site before changing it. Do not assume archived implementations are active.
5. Treat system_docs as read-only authority. Do not edit system documentation.

Required behaviour changing:
- normal IngestDB observation writes use the compact typed-array RPC;
- normal IngestDB latest-value writes use the separate compact typed-array RPC;
- the GCP ObsAQIDB delivery path sends compact arrays to ObsAQIDB and omits status at that external boundary;
- Sensor.Community stops resending unchanged descriptive station metadata while preserving new station/timeseries discovery and independent presence/reappearance updates;
- outbound request-body bytes and request counts remain measurable for the affected external writes.

Behaviour that MUST remain unchanged:
- source APIs, observation filtering, values and timestamps;
- polling cadence, due selection, scheduler ownership and OpenAQ Cloud Tasks behaviour;
- retry classification, timeout splitting, runtime-budget protection, committed-row accounting and checkpoints;
- Pub/Sub message shape, message publishing, pull/dedupe and acknowledgement semantics;
- OpenAQ normal OPENAQ_INGEST_STATION_FETCH=false behaviour;
- connector-specific station retirement rules;
- R2 history and AQI behaviour;
- ObsAQIDB secondary delivery itself.

Implement Phases 2 and 3 of the plan. Keep the current shared retry engine semantics and change only its database write payload/callback boundary where possible. Avoid unrelated refactors.

Do not create a speculative pre-deployment test programme. Run the existing focused static/unit checks that directly cover changed shared helpers if they already exist, plus syntax/type checks normally required by the repository. Do not substitute synthetic functional tests for real TEST operation.

Do not deploy to LIVE. At the end, provide a concise implementation report listing files changed, exact transport changes, checks run, TEST deployment order, rollback route and the real TEST observations that still need to be performed under Phase 4.
```

## Goal

Reduce Cloud Run to Supabase write egress and repeated metadata work without changing the logical data produced by normal successful ingest.

The primary savings are expected from:

- sending observation field names once per batch rather than once per row;
- not sending derivable `connector_id` per observation;
- not sending null-only `status` to ObsAQIDB, which does not persist it;
- not resending unchanged Sensor.Community station metadata;
- not resending complete static timeseries rows merely to update latest values;
- retaining practical batching while reducing request-body bytes per row.

## Already completed structural verification

No additional broad pre-implementation investigation is required. The following were verified directly against current TEST code/schema and the two deployed TEST databases before this plan was written:

- `uk_aq_core.timeseries.id` is `integer` and is the primary internal timeseries identity in both databases;
- the complete TEST `timeseries.id` sets match between IngestDB and ObsAQIDB for every connector;
- IngestDB observations use `connector_id integer`, `timeseries_id integer`, `observed_at timestamptz`, `value double precision`, `status text`;
- ObsAQIDB observations use `connector_id integer`, `timeseries_id integer`, `observed_at timestamptz`, `value double precision` and do not store status;
- ObsAQIDB observations are daily range partitions on `observed_at`;
- existing IngestDB duplicate writes update only when value or status is distinct;
- existing ObsAQIDB duplicate writes update only when value is distinct;
- the existing IngestDB latest-value RPC updates only when `last_value` or `last_value_at` is distinct;
- `uk_aq_core.stations.id` is `bigint`;
- Sensor.Community descriptive station ownership is `label`, effective `station_name`, `station_type`, `station_exposure`, and geometry, while `last_seen_at` and `removed_at` are presence/lifecycle state;
- a seen Sensor.Community station currently refreshes `last_seen_at` and clears `removed_at`;
- normal TEST OpenAQ has `OPENAQ_INGEST_STATION_FETCH=false`;
- the examined OpenAQ station catalogue path does not implement retirement by catalogue absence;
- the existing Pub/Sub message format can remain unchanged because compaction can occur only at the writer-to-ObsAQIDB external boundary.

## Protected non-goals

This work MUST NOT:

- reduce source polling frequency;
- drop, aggregate, downsample or round observations;
- change timestamp precision;
- change source selection/filtering;
- redesign Pub/Sub messages in v1;
- change scheduler topology;
- introduce OpenAQ or Sensor.Community retirement-by-absence;
- change R2 history behaviour;
- change AQI calculation;
- combine observation and latest-value writes into a new behavioural transaction in v1;
- change public website interfaces.

## Phase 1: additive schema implementation in `uk-aq-schema`

### 1.1 IngestDB compact observation RPC

Add the canonical definition and migration for:

```text
uk_aq_public.uk_aq_rpc_observations_compact_upsert_v1(
    timeseries_ids integer[],
    observed_ats timestamptz[],
    values double precision[],
    statuses text[] default null
)
```

Requirements:

- reject null/mismatched required vectors;
- accept an omitted/null `statuses` vector as all-null status values;
- if `statuses` is supplied, require the same cardinality as the other vectors;
- resolve every input timeseries ID against `uk_aq_core.timeseries` before writing;
- derive `connector_id` from that local row;
- fail the RPC if any required timeseries ID is unresolved;
- expand vectors deterministically, using ordinality or equivalent row-position preservation;
- write through the existing parent observation table so current connector partitioning continues to apply;
- preserve `(connector_id, timeseries_id, observed_at)` identity;
- preserve the current `IS DISTINCT FROM` value/status update rule;
- return the same useful minimal upsert count style as the existing RPC;
- keep existing observation RPC metrics, adding the compact endpoint name and meaningful input/payload accounting rather than creating a second metrics system;
- service access must be no broader than the existing write RPC.

Do not remove or alter `uk_aq_rpc_observations_upsert(rows jsonb)` in this phase.

### 1.2 IngestDB compact latest-value RPC

Add:

```text
uk_aq_public.uk_aq_rpc_timeseries_last_values_compact_update_v1(
    timeseries_ids integer[],
    last_values double precision[],
    last_value_ats timestamptz[]
)
```

Requirements:

- equal-length validation;
- update by `timeseries.id`;
- preserve the existing distinct-value/timestamp guard;
- preserve current first-value handling elsewhere rather than silently changing it here;
- return a minimal updated-row count.

Keep the existing JSONB latest-value RPC during rollout.

### 1.3 ObsAQIDB compact observation RPC

Add the canonical ObsAQIDB definition and migration for:

```text
uk_aq_public.uk_aq_rpc_observs_observations_compact_upsert_v1(
    timeseries_ids integer[],
    observed_ats timestamptz[],
    values double precision[]
)
```

Requirements:

- equal-length validation;
- resolve every timeseries ID against mirrored `uk_aq_core.timeseries` and derive `connector_id`;
- fail before ambiguous writes if an ID is missing from the mirror;
- preserve the existing hot-write window exactly;
- preserve daily parent-table partition routing;
- preserve the existing logical duplicate/update semantics;
- preserve current service-role guard, timezone handling, statement timeout and custom-plan behaviour where they are part of the deployed RPC;
- keep endpoint metrics using the existing metrics table/path;
- do not add status to ObsAQIDB.

Do not remove `uk_aq_rpc_observs_observations_upsert(rows jsonb)` in this phase.

### 1.4 Sensor.Community compact station-state interfaces

Implement the minimum additive IngestDB RPCs needed to avoid full station metadata upserts for unchanged stations.

Use a resolver conceptually equivalent to:

```text
sensorcommunity station refs[]
    -> station_id bigint
    -> current descriptive metadata fingerprint
```

The resolver MUST derive the fingerprint from the actual persisted station row, not from an independently stored cache that could drift from the station metadata.

The v1 fingerprint must represent exactly the authorised descriptive fields:

- `label`;
- `station_name` only when the connector `overwrite_station_name` policy makes it source-controlled;
- `station_type`;
- `station_exposure`;
- longitude and latitude from geometry.

Identity and lifecycle fields are excluded.

Use a deterministic cross-runtime encoding that does not round coordinates. Recommended implementation:

- trim text and represent null/empty deterministically;
- encode canonical text as UTF-8 hex before concatenation, avoiding separator/escaping ambiguity;
- encode longitude and latitude using exact float64 bytes/hex, with SQL using the equivalent `float8send` representation;
- SHA-256 the deterministic field sequence;
- preserve case unless the persisted field contract itself normalises case.

If a simpler encoding is chosen, Codex must show that SQL and Node/Deno produce byte-identical canonical input without coordinate rounding.

Add a presence-touch RPC conceptually equivalent to:

```text
station_ids bigint[]
seen_ats timestamptz[]
```

It MUST:

- validate equal lengths;
- update only the addressed stations;
- write `last_seen_at` using the same effective seen time currently supplied by Sensor.Community;
- set `removed_at = NULL` for seen/reappearing stations;
- not overwrite descriptive metadata.

Use existing connector identity checks so a Sensor.Community-only helper cannot accidentally mutate another connector's stations.

### 1.5 Canonical SQL and migrations

Update the canonical schema files, including the current equivalents of:

- `schemas/ingest_db/uk_aq_rpc.sql`;
- `schemas/obs_aqi_db/uk_aq_obs_aqi_db_ops_rpcs.sql`;

and create additive migration file(s) using the repository's current migration convention. Do not rewrite old applied migrations.

The migration must be safe to apply before any runtime switches to the new RPCs.

### 1.6 Structural validation only

Before TEST deployment, validate only that:

- the SQL parses under the repository's established validation path;
- function signatures and grants are internally consistent;
- old RPC definitions remain present;
- no table/partition/identity change has been introduced accidentally.

Do not create a new behavioural test suite.

## Deployment gate A: apply schema to TEST

Apply the additive schema migration to both TEST databases before merging/deploying ingest runtime adoption.

Confirm only structural availability at this gate:

- all new RPC signatures exist in the intended database;
- service-role execution grants match the contract;
- old RPCs still exist;
- IngestDB and ObsAQIDB timeseries mirrors remain structurally present.

This is not the functional validation phase.

## Phase 2: shared ingest transport adoption in `uk-aq-ingest`

### 2.1 Preserve the existing retry engine

`supabase/functions/_shared/ingestdb_observation_writer.mjs` currently owns important behaviour for retry classification, timeout splitting, runtime budget, committed-row accounting and terminal diagnostics.

Preserve those semantics.

Prefer changing the connector `writeChunk` callbacks or a narrowly shared payload adapter so each attempted row chunk is transformed to:

```json
{
  "timeseries_ids": [...],
  "observed_ats": [...],
  "values": [...],
  "statuses": [...]
}
```

rather than replacing the retry engine.

Where the entire chunk has null status, `statuses` may be omitted/null as authorised by the RPC.

### 2.2 Adopt the compact IngestDB observation RPC across active connectors

Trace the active observation write path for all five current connectors and switch normal IngestDB writes to `uk_aq_rpc_observations_compact_upsert_v1`:

- SOS;
- Breathe London Nodes;
- Breathe London Communities;
- OpenAQ;
- Sensor.Community.

Do not edit archived copies.

Each call site must preserve:

- current logical rows presented to the retry engine;
- current normal chunk sizes initially;
- committed-row accounting;
- existing connector run summary fields;
- existing failure classification and checkpoint outcomes.

Do not add `connector_id` to the compact RPC payload merely because it remains present in in-memory row objects.

### 2.3 Adopt compact latest-value updates

Trace all active normal timeseries latest-value update calls and use `uk_aq_rpc_timeseries_last_values_compact_update_v1` while preserving current value/timestamp selection and stale/update guards.

Do not merge this with the observation RPC.

### 2.4 Keep static timeseries metadata out of normal value updates

Where active connectors currently resend complete timeseries reference objects even though a valid `timeseries_ref -> timeseries_id` mapping already exists, narrow the normal path to the existing identity.

Retain connector-specific creation/self-repair for genuinely missing timeseries, particularly:

- the Nodes self-repair guard authorised by the broad ingest contract;
- immediate Sensor.Community new-timeseries discovery.

A missing required timeseries must still fail or repair explicitly, never silently drop its observation.

## Phase 3: Sensor.Community and ObsAQIDB boundary

### 3.1 Sensor.Community station comparison

In the active Sensor.Community Cloud Run worker:

1. continue to normalise each valid feed station using the current source fields and coordinates;
2. deduplicate by station identity as today;
3. call the compact station-state resolver for the unique station refs;
4. compute the source-side v1 fingerprint using exactly the same canonical encoding as the database resolver;
5. classify stations as new, changed or unchanged;
6. send the existing full descriptive station upsert payload only for new/changed stations;
7. independently call the compact presence-touch RPC for all seen known stations;
8. after creating new stations, resolve their IDs and apply presence state before dependent observation handling completes;
9. preserve immediate missing-timeseries creation before observations are written.

When `overwrite_station_name` is false, exclude source station name from the fingerprint so it cannot cause a permanent false change loop.

If the resolver cannot return a trustworthy comparison for a known station, fail safe by treating it as requiring descriptive refresh. Do not skip metadata merely to save bytes.

Do not introduce absence-based station retirement.

### 3.2 ObsAQIDB external write compaction

Update `supabase/functions/_shared/observs_client.ts` and the active GCP ObsAQIDB Pub/Sub writer path so the external Supabase RPC receives:

```json
{
  "timeseries_ids": [...],
  "observed_ats": [...],
  "values": [...]
}
```

instead of one object per observation.

Keep the in-GCP Pub/Sub messages unchanged. Existing `prepareObservsRows`, deduplication, outbox payloads and receipt generation MAY continue using row objects internally where that avoids widening the change.

At the external ObsAQIDB RPC boundary:

- omit `connector_id` because the database derives it;
- omit `status` because ObsAQIDB does not persist it;
- preserve exact `value` and timestamp values;
- preserve the current timeout retry and fallback splitting behaviour;
- do not acknowledge Pub/Sub messages before destination success under the existing contract.

Update the deployment workflow's default ObsAQIDB RPC name to the compact v1 endpoint where required. Do not redesign Pub/Sub topics/subscriptions or scheduler behaviour.

### 3.3 Request-body egress metrics

Use the existing egress instrumentation path where possible. Do not create a parallel metrics subsystem.

For affected external write calls, record aggregate request count and serialized UTF-8 request-body bytes, with caller/destination/endpoint dimensions already supported by the current metrics design.

Measure the actual serialised body being sent, for example using `TextEncoder`/`Buffer.byteLength` around the JSON string already used for the request. Never log the body itself.

Retain existing database RPC metrics and endpoint counters.

### 3.4 OpenAQ protection

Do not turn `OPENAQ_INGEST_STATION_FETCH` back on.

Normal scheduled OpenAQ must continue to resolve work from local reference state. Do not add retirement by catalogue absence as part of this efficiency work.

## Pre-deployment code checks

After implementation, run only the focused checks already appropriate to changed files, such as:

- existing Node/Deno syntax or type checking configured by the repository;
- existing targeted tests for the shared observation retry writer or existing transport helper if those tests already exist and are directly affected;
- diff review for deployment workflow path/env changes.

Do not design a new speculative functional test matrix. Functional behaviour is validated on TEST after deployment.

## Deployment gate B: TEST runtime deployment

Do not deploy the ingest runtime until Gate A is complete.

Once the new RPCs exist in TEST, deploy the affected TEST services/workflows using the repository's normal deployment paths.

Because shared helper changes can trigger several connector deployments, review workflow path filters before push/deployment and ensure the schema gate is already satisfied for every affected service.

Do not deploy to LIVE in this plan.

Rollback during TEST is:

1. return the affected Cloud Run service to the previous known-good revision or revert the runtime commit;
2. use the still-present legacy object-row RPCs through that previous runtime;
3. leave the additive compact RPCs in place until the failure is understood unless they themselves are the cause;
4. do not change source polling or delete data as a rollback mechanism.

## Phase 4: real TEST operational validation

This phase happens after deployment through real TEST operations, not synthetic pre-implementation testing.

### 4.1 All connectors

Observe at least one normal successful run of each affected connector and confirm from source/run/database evidence that:

- cadence and due selection are unchanged;
- observation rows are still written with expected counts, values and timestamps;
- no unresolved-timeseries errors appear for valid existing reference data;
- committed-row accounting and ingest-run status remain coherent;
- latest-value state advances normally;
- checkpoints continue to behave normally.

### 4.2 Sensor.Community

Observe a normal Sensor.Community run and a subsequent run where most station metadata is unchanged.

Confirm:

- new station/timeseries discovery still works when encountered naturally or through an existing known TEST case;
- unchanged descriptive station metadata is not included in full metadata upserts;
- `last_seen_at` continues to update independently;
- `removed_at` is still cleared for a seen/reappearing station under the existing semantics;
- fingerprint comparison is stable across repeated unchanged coordinates/text;
- no station metadata is rounded or degraded.

A deliberately manipulated station is not required unless real TEST operation exposes ambiguity that cannot otherwise be resolved.

### 4.3 ObsAQIDB Pub/Sub writer

Confirm real Pub/Sub delivery continues to:

- pull/decode/dedupe messages as before;
- write the expected logical observations to ObsAQIDB;
- create sync receipts as before;
- acknowledge successful messages only after destination handling;
- avoid a growing subscription backlog or repeated write failures.

### 4.4 OpenAQ

Confirm normal runtime logs/config continue to show station catalogue fetching disabled and normal observation work continues from local reference state.

### 4.5 Egress evidence

Compare baseline and post-deployment request metrics for representative normal runs:

- IngestDB observation request count and body bytes;
- IngestDB latest-value body bytes;
- Sensor.Community station metadata/presence body bytes;
- GCP writer to ObsAQIDB body bytes.

The acceptance question is whether the transport is materially smaller while logical operation remains equivalent. Do not change batching/cadence simply to improve the metric.

## Acceptance criteria

The initial implementation is accepted on TEST when:

- all five connectors have completed normal successful operation on the compact IngestDB path;
- no observation precision, timestamp or eligibility regression is found;
- latest-value/checkpoint/retry behaviour remains intact;
- Sensor.Community no longer resends unchanged full descriptive metadata;
- Sensor.Community presence semantics remain intact;
- ObsAQIDB continues to receive equivalent logical observations through unchanged Pub/Sub messaging;
- OpenAQ full catalogue fetch remains disabled;
- measured external request-body bytes are lower on the intended write boundaries;
- no new sustained error/backlog condition is introduced.

## Phase 5: cleanup only after TEST acceptance

Do not perform this phase in the first implementation pass before real TEST validation.

After the user accepts the TEST evidence:

- remove legacy normal-runtime object-row write code that is no longer used;
- decide whether the old database RPCs should be retired immediately or retained for one additional promotion window;
- remove obsolete configuration only when no rollback revision depends on it;
- update the implementation report/status in this plan;
- hand the final behavioural outcome back to ChatGPT for any necessary system-doc wording adjustments before LIVE promotion.

## Expected primary file scope

Codex must verify the active call sites before editing, but the likely scope includes:

### `uk-aq-schema`

- canonical IngestDB RPC SQL;
- canonical ObsAQIDB operations RPC SQL;
- additive migration file(s) for the compact RPCs and Sensor.Community helpers;
- existing grant/validation definitions associated with those RPCs.

### `uk-aq-ingest`

- `supabase/functions/_shared/ingestdb_observation_writer.mjs` only where an adapter/API change is genuinely required;
- active connector observation write callbacks for SOS, Nodes, Communities, OpenAQ and Sensor.Community;
- active latest-value update helpers/call sites;
- `workers/uk_aq_sensorcommunity_cloud_run/index.mjs`;
- `supabase/functions/_shared/observs_client.ts`;
- `workers/uk_aq_observs_pubsub_cloud_run/run_job.ts`;
- relevant connector/ObsAQIDB deployment workflow defaults and path filters;
- existing egress instrumentation helper only where necessary to measure request bodies.

Archived copies are explicitly out of scope.

## Final implementation report requirements

After each Codex phase, report:

- repository and commit/diff scope;
- exact files changed;
- exact RPC signatures and runtime endpoints adopted;
- confirmation that system docs were not edited by Codex;
- structural/static checks run;
- TEST apply/deploy actions still required;
- any observed deviation between current code and the authoritative contract;
- rollback route.

After Phase 4, add real TEST operational evidence rather than replacing it with synthetic claims.
