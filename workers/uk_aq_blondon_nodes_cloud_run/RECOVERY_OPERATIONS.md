# Nodes recent-hour recovery: implementation handover

Companion PRs: [schema #14](https://github.com/TEST-uk-aq/uk-aq-schema/pull/14), [Ops mapping #116](https://github.com/TEST-uk-aq/uk-aq-ops/pull/116).

Implementation only. No TEST deployment, migration application, source recovery or functional acceptance is established by this change. Authoritative prose remains in `TEST-uk-aq/uk-aq-system-docs`; this file describes the implementation and operator prerequisites.

## Behaviour

Normal polling still acquires PM25, NO2, PM25Index and NO2Index using the existing due selection, window, overlap, duplicate winners, compact-v1 writer, latest-value updates and station/species checkpoints. A later numeric source row can advance those checkpoints beyond an earlier null; the null never becomes an observation, and ordinary overlap polling can then exclude its timestamp.

After normal checkpoint writes, an independently bounded pass scans active, deterministically mapped Nodes timeseries. It also runs when normal station selection is empty. It skips manual/dry runs, disabled polling, incomplete normal runs and insufficient remaining runtime. Its nested `recovery` report never changes normal run status or counters.

The scan reads actual finite observation keys. Three consecutive real hourly keys establish observed hourly cadence; missing hours strictly between the first and last real key are candidates. Off-hour/subhourly cadence, insufficient evidence and series edges remain unconfirmed. A candidate does not prove a source measurement ever existed. Requests group only contiguous candidate hours, up to the configured span, with one source request per selected timeseries per pass. The persisted attempt number rotates between outstanding groups.

Only exact, explicitly offset-bearing source instants and finite numeric ScaledValue winners are accepted. Nulls, omissions, booleans, non-finite values, ambiguous timestamps and failures remain unresolved. Existing `build_rows` chooses the final valid conflicting occurrence. No concentration is reconstructed from an index, adjacent hour, CSV, raw ZIP or another connector.

`blondon_nodes_recovery_state` owns scan order, UTC-day attempt counts, cooldowns, two-minute claims and candidate timestamps. Nothing in recovery updates first/latest timeseries state, station/species checkpoints, due dates or lag samples. Selected scans are claimed before HTTP work, so a killed invocation also observes cooldown on retry.

The separate pooled writer validates Nodes identity, claim membership/expiry, active lifecycle, UTC hour alignment, finite numbers and current bounds. Atomic conflict handling inserts absent keys or fills a null value; every existing non-null value/status/provenance is preserved. Only rows returned after transaction commit go to the existing secondary writer. There is no immediate source retry, database retry or transport fallback. Ambiguous writes and secondary failures/deferrals are reported; secondary delivery is not an atomic cross-database guarantee.

Normal and recovery source responses use the same recorder and at most one existing ZIP/JSONL archive per invocation. Recovery source responses are captured before filtering. No archive format, secret gate or retention change is introduced.

## Configuration and ownership prerequisites

`BLONDON_NODES_RECOVERY_CONFIG` is an external non-secret JSON variable. Missing/empty or `{"enabled":false}` disables recovery. Enabled configuration requires every field; there are no application defaults for recovery limits. One conservative initial configuration is:

```json
{"enabled":true,"horizon_hours":48,"lag_hours":2,"scan_limit":8,"request_limit":8,"request_span_hours":6,"runtime_seconds":90,"cooldown_minutes":180,"max_attempts_per_day":6,"not_before_utc":"<operator-verified UTC midnight as YYYY-MM-DDT00:00:00Z>"}
```

The placeholder must be replaced with a real UTC date after establishing that no Nodes connector/day on or after that date is already canonical historical R2 authority. Do not enable with an assumed date. History can exist without a Prune gate; gate absence alone is insufficient. Before a separately authorised historical operation enters the recovery window, disable recovery, let active invocations finish and move this boundary forward before re-enabling.

Also supply:

- `INGESTDB_RETENTION_DAYS`: exactly the effective TEST Prune policy, including any workflow override. The checked-in Ops fallback is five days; it does not establish the deployed setting. This task could not read Actions variables (HTTP 403) and had no database credentials.
- Existing `BLONDON_NODES_INGESTDB_WRITE_TRANSPORT=database` and the environment's existing shared database secret/binding.
- Existing `BLONDON_NODES_BASE_URL`, `UKAQ_ENV_NAME` and, for Pub/Sub, `GCP_OBSERVS_PUBSUB_TOPIC`. The source endpoint, environment-label and topic literal fallbacks have been removed; deployment checks require explicit settings where needed.

Eligibility begins at the latest of `now - horizon`, the attested midnight, and UTC midnight minus `(retention_days - 1)` days. This stays a full day inside Prune's deletion boundary. The scan and writer exclude every matching history candidate or connector/day gate, including consumed/false gates. These checks supplement the external historical boundary; they do not discover all R2 authority or coordinate an operator's concurrent Integrity operation.

Safety ceilings: horizon 72h; lag 2–24h; scan/request caps 16; contiguous request span 6h; recovery budget 75–90s; cooldown 60–1440min; daily attempts 1–24. Recovery reserves 45s of the existing child budget for finalisation and starts requests only with conservative write/delivery headroom. Shorter remaining time produces a deferral. The outer service timeout must exceed the child budget plus cleanup headroom. Source concurrency is one.

Both ingest and Ops GitHub target mappings list the new variable and reused settings. Per the user's task instruction, the Ops mapping replaces the unavailable ignored master CSV for this change. No remote variables or secrets were changed.

## Manual TEST order

1. Review/merge the additive schema PR and the Ops mapping PR. Neither applies a migration or enables recovery.
2. With an explicitly selected TEST **administrator** DSN, run from the schema checkout:

   ```sh
   psql "$TEST_INGESTDB_ADMIN_DSN" -v ON_ERROR_STOP=1 -f schemas/migrations/20261021_002_ingest_blondon_nodes_recent_recovery.sql
   psql "$TEST_INGESTDB_ADMIN_DSN" -v ON_ERROR_STOP=1 -f schemas/ingest_db/verify_canonical_ingest_db.sql
   ```

   Expected: additive state/helper/scan/writer objects, one extra exact runtime EXECUTE grant, schema reload notification and a successful canonical verifier. The pooled runtime remains without core/raw table access; its explicit public function surface is six.
3. Verify actual retention and historical ownership, current API endpoint, environment label, Pub/Sub topic, shared secret binding and service/child timeout headroom. Supply required external settings. Keep recovery disabled until these prerequisites are satisfied; then set the complete JSON using the normal environment-sync process.
4. Only after separate deployment authorisation, merge the ingest PR. **Its changed paths automatically trigger the Nodes Cloud Run deployment workflow on `main`.** Schema and configuration must be ready first. If deployment is separately approved later, retain disabled JSON until that approval and trigger the same workflow manually when approved:

   ```sh
   gh workflow run uk_aq_blondon_nodes_cloud_run_deploy.yml --repo TEST-uk-aq/uk-aq-ingest --ref main
   ```

5. Accept through real scheduled TEST operations. No migration, merge, deployment, backfill, Integrity run, remote configuration update or LIVE operation was performed by this task.

## Structural checks

The 21 October recovery contract was fetched from TEST system-docs `main` before implementation. Checks covered Python syntax/imports with the pinned Nodes runtime dependencies; workflow YAML and embedded Bash syntax; canonical SQL, PL/pgSQL and migration parsing; canonical/migration definition alignment; CSV target consistency; and unchanged ASTs for normal `build_rows`, duplicate selection and due selection. A single local PostgreSQL-engine safety check installed the additive migration and demonstrated null filling, preservation of a newer primary value/status/provenance on replay, history-gate rejection and the writer-only pooled grant. This is structural/safety evidence, not TEST operational or source acceptance. No live source-capability probe was required or performed. Actual database retention, source availability, concurrency and costs still require real TEST verification.

## Expected overhead and acceptance

With the initial example, each eligible scheduled invocation adds one PostgREST scan (up to eight indexed key reads of at most 74 returned finite keys each), up to eight source requests, at most 48 candidate observations in one pooled write, and normal secondary delivery of committed rows. The hard writer vector ceiling is 96. At 96 invocations/day the absolute example ceilings are 96 scans and 768 source requests/day, plus at most 96 pooled transactions with writes and normal secondary calls. Cooldowns, attempts, real gaps and runtime reduce these ceilings. Catalog/state selection still traverses eligible metadata; there is no full-network observation-history scan.

Small configured secondary batches can require more than one delivery call; remaining-budget checks defer excess work visibly. Unresolved source values are not repeatedly rewritten. Raw capture remains one archive per invocation. Actual log bytes and costs must be measured through TEST, including API Gateway, PostgreSQL and Supavisor, against the organisation's 30 MB/day target; request-count bounds are not measured billing evidence.

Acceptance must demonstrate unchanged normal selection/four-series writes/checkpoints/latest state and duplicate winners; actual missing keys despite newer observations; exact authentic source recovery once; concurrent valid primary data surviving recovery; truthful null/omission/failure/ambiguous-write and secondary diagnostics; cooldown/attempt/fairness/cap behaviour; raw evidence with no secrets; and normal later Prune publication without recreating historical days. Observe complete UTC-day usage and report remaining due timeseries instead of claiming network coverage from one scan.

Verify both UTC midnight and the next real GMT/BST transition (25 October 2026 at 01:00 UTC if deployment precedes it): no shifted, duplicated or invented hours. Offset-free source timestamps deliberately remain unconfirmed. Check UTC-day retry reset independently from local clock changes.

## Rollback and outstanding work

Disable recovery with `{"enabled":false}` and redeploy the same service through separately authorised operations. Alternatively restore the prior known-good image/code revision, ensuring its endpoint/environment settings remain correct. Leave additive schema/state/grants in place for investigation; do not delete authentic recovered observations. If retiring the feature permanently after rollback acceptance, separately review revocation/removal of only the new writer, scan, helper and state, then restore the canonical verifier's five-function expectation. Never remove the shared role or normal compact-v1 grant.

Missing source values that never become numeric, ambiguous timestamps, unsupported/insufficient cadence, unbracketed edges, work expiring outside bounds and already-published historical days require separate assessment. No Integrity invocation or historical repair is automatic, and the source may never supply the measurement.

Chat-mode documentation handover: update `system_docs/ingest/blondon_nodes_recent_hourly_gap_recovery_contract.md` with the exact implemented eligibility/pacing/grouping/retention and external historical-boundary semantics, the new schema interfaces, config and limitations while retaining future/not-accepted status. After real TEST acceptance only, update that status and the ingest README; extend `blondon_nodes_database_transport_contract.md`, `gcp_cloud_run_database_runtime_contract.md` and the canonical apply-order description for the sixth exact grant; clarify in `blondon_nodes_raw_capture_contract.md` that a normal no-due run may capture recovery responses; cross-reference this recovery route from source-normalisation/compact transport docs without changing normal semantics. Record measured costs, authentic recovery evidence (if any), UTC/DST evidence, unresolved historical scopes and rollback evidence. This task did not edit authoritative documentation.
