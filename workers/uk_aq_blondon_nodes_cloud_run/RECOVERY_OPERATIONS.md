# Nodes recent-hour recovery: implementation handover

Companion PRs: [schema #14](https://github.com/TEST-uk-aq/uk-aq-schema/pull/14) and [Ops mapping #116](https://github.com/TEST-uk-aq/uk-aq-ops/pull/116) are merged into TEST `main`. The approved two-day minimum is implemented in TEST Ops `main` commit `fee4089`; its deployment and operational acceptance remain separate prerequisites.

Implementation only. No TEST deployment, migration application, source recovery or functional acceptance is established by this change. Authoritative prose remains in `TEST-uk-aq/uk-aq-system-docs`; this file describes the implementation and operator prerequisites.

## Behaviour

Normal polling still acquires PM25, NO2, PM25Index and NO2Index using the existing due selection, window, overlap, duplicate winners, compact-v1 writer, latest-value updates and station/species checkpoints. A later numeric source row can advance those checkpoints beyond an earlier null; the null never becomes an observation, and ordinary overlap polling can then exclude its timestamp.

After normal checkpoint writes, an independently bounded pass scans active, deterministically mapped Nodes timeseries. It also runs when normal station selection is empty. It skips manual/dry runs, disabled polling, incomplete normal runs and insufficient remaining runtime. Its nested `recovery` report never changes normal run status or counters.

The scan reads actual finite observation keys. Three consecutive real hourly keys establish observed hourly cadence; missing hours strictly between the first and last real key are candidates. Off-hour/subhourly cadence, insufficient evidence and series edges remain unconfirmed. A candidate does not prove a source measurement ever existed. Requests group only contiguous candidate hours, up to the configured span, with one source request per selected timeseries per pass. The persisted attempt number rotates between outstanding groups.

Only exact, explicitly offset-bearing source instants and finite numeric ScaledValue winners are accepted. Nulls, omissions, booleans, non-finite values, ambiguous timestamps and failures remain unresolved. Existing `build_rows` chooses the final valid conflicting occurrence. No concentration is reconstructed from an index, adjacent hour, CSV, raw ZIP or another connector.

`blondon_nodes_recovery_state` owns scan order, UTC-day attempt counts, cooldowns, two-minute claims and candidate timestamps. Nothing in recovery updates first/latest timeseries state, station/species checkpoints, due dates or lag samples. Selected scans are claimed before HTTP work, so a killed invocation also observes cooldown on retry.

The separate pooled writer validates Nodes identity, claim membership/expiry, active lifecycle, UTC hour alignment, finite numbers and current bounds. Atomic conflict handling inserts absent keys or fills a null value; every existing non-null value/status/provenance is preserved. Only rows returned after transaction commit go to the existing secondary writer. There is no immediate source retry, database retry or transport fallback. Ambiguous writes and secondary failures/deferrals are reported; secondary delivery is not an atomic cross-database guarantee.

Normal and recovery source responses use the same recorder and at most one existing ZIP/JSONL archive per invocation. Recovery source responses are captured before filtering. No archive format, secret gate or retention change is introduced.

## Shared upstream endpoint

The user confirms that TEST and LIVE share the Breathe London Nodes upstream `https://breathe-london-7x54d7qf.ew.gateway.dev`. Normal polling and recovery both construct the same `BreatheLondonNodesClient`, whose default base URL is the single `SHARED_BLONDON_NODES_API_BASE_URL` constant in `scripts/blondon_nodes/blondon_nodes_ingest.py`. Neither path reads `BLONDON_NODES_BASE_URL`; there is no replacement variable or environment-dependent fallback.

Repository evidence identifies this URL as the source API: the Nodes station client uses its `/ListSensors` route with `X-API-KEY`, and the ingest client uses `/SensorData` with the same authentication scheme. The Cloud Run deployment destination is separate: the workflow resolves its own service URL through `gcloud run services describe` and reconciles that URL into the scheduler. Together with the user's explicit shared-endpoint confirmation, this supports treating the upstream as an invariant rather than a TEST/LIVE deployment setting. No LIVE inspection or live source probe was performed.

If Breathe London changes the provider endpoint, verify its shared-upstream identity and request/authentication contract, update this constant through a reviewed and archived code change, and update the authoritative source contract separately. Deploy the same complete application files through the authorised TEST-first process and subsequent separately authorised LIVE promotion; do not introduce environment branches or endpoint variables. Review the standalone station-discovery/probe tools' existing source defaults separately if the provider change affects them too. Their optional local overrides are unchanged by this PR and are not needed by their default scheduled use.

All credentials, API keys, database URLs, Cloud Run deployment destinations, project/account identifiers, environment labels, Pub/Sub configuration and other environment-specific settings remain externally supplied. This one common upstream decision does not authorise hard-coding those values.

## Configuration and ownership prerequisites

`BLONDON_NODES_RECOVERY_CONFIG` is an external non-secret JSON variable. Missing/empty or `{"enabled":false}` disables recovery. Enabled configuration requires every field; there are no application defaults for recovery limits. One conservative initial configuration is:

```json
{"enabled":true,"horizon_hours":48,"lag_hours":2,"scan_limit":8,"request_limit":8,"request_span_hours":6,"runtime_seconds":90,"cooldown_minutes":180,"max_attempts_per_day":6}
```

There is no operator-maintained historical cutoff. Both application and deployment validation reject any externally supplied `not_before_utc` key when recovery is enabled, including null or empty values. Remove the obsolete key from the enabled JSON before deployment. Disabled configuration remains accepted without requiring recovery fields.

Nodes uses the fixed approved Prune minimum, rather than the changing `INGESTDB_RETENTION_DAYS` environment variable:

```text
MIN_PRUNE_RETENTION_DAYS = 2
RECOVERY_SQL_RETENTION_ARGUMENT_DAYS = MIN_PRUNE_RETENTION_DAYS + 1 = 3
existing SQL cutoff = UTC midnight - (3 - 1) days = UTC midnight - 2 days
internal not_before_utc = UTC midnight at recovery invocation start - 2 days
```

The existing SQL helper subtracts one day from its retention argument, so both the service-role scan and pooled recovery writer receive **3**, not 2. Passing 2 would truncate the rolling 48-hour lookback at yesterday's UTC midnight. The application and deployment validators reject enabled horizons above **48 hours**, even though the unchanged SQL helper accepts up to 72 hours. No SQL function, signature, privilege, verifier or migration changes are required for this amendment.

For compatibility with that already-applied helper, recovery captures the UTC time once at invocation start, derives UTC midnight minus `MIN_PRUNE_RETENTION_DAYS`, and adds its ISO UTC value as `not_before_utc` to a copy of the validated configuration. Both SQL operations receive the same effective configuration and integer argument **3**. The operator's JSON is unchanged. This internal compatibility field repeats the minimum retention boundary; it provides no independent historical R2 ownership safeguard. If an invocation crosses UTC midnight, the unchanged SQL helper still applies its own current retention boundary at each operation.

Before enabling recovery, separately deploy and validate the TEST Prune safeguard from `fee4089`, or attest that every active Prune path retains at least two UTC days. Include direct invocation overrides and legacy service revisions: a source commit alone does not establish the deployed guarantee. The safeguard rejects retention below two rather than silently clamping it. Actual Prune retention can be 2, 4, 5 or higher without synchronising a Nodes variable or redeploying Nodes for that change.

Also supply:

- Existing `BLONDON_NODES_INGESTDB_WRITE_TRANSPORT=database` and the environment's existing shared database secret/binding.
- Existing `UK_AQ_ENV_NAME` and, for Pub/Sub, `GCP_OBSERVS_PUBSUB_TOPIC`. Environment-label and topic settings remain explicit; the common upstream endpoint needs no GitHub Actions variable.

Eligibility begins at the later of `now - horizon` and the SQL UTC-midnight-minus-**two-days** boundary, obtained by passing 3 to both existing SQL functions. The internal compatibility field does not impose an additional historical cutoff. Prune's earliest permitted deletion cutoff is also UTC midnight minus two days, which is never later than the start of a rolling 48-hour lookback. At exactly 00:00 UTC the cutoffs coincide: **there is no extra safety margin**. The scan and writer still exclude every matching history candidate or connector/day gate, including consumed/false gates.

Older gaps belong to separately authorised History Integrity operations, never automatic Nodes recovery. The existing [History Writer Coordination request-level boundary](https://github.com/TEST-uk-aq/uk-aq-system-docs/blob/main/system_docs/r2_history/history_writer_coordination.md#integrity-request-level-boundary-check) requires every Integrity mode to determine each requested connector's earliest UTC IngestDB day before source acquisition, comparison, planning or R2 mutation. If the requested inclusive end date is on or after that day for any requested connector with IngestDB rows, the **entire request fails closed**; it cannot clip the range, skip the blocking connector or treat an empty later day as historical space. A connector with no IngestDB rows has no boundary for this check. Properly gated Integrity therefore cannot act on connector days still owned by IngestDB. Normal authorised Integrity needs no separate Nodes recovery disable, drain or age-out procedure.

An R2 connector/day need not have a Prune candidate or gate; gate absence never proves a day unpublished. The Integrity ownership gate, two-day Prune minimum, maximum 48-hour recovery window and existing candidate/gate exclusions provide the agreed normal-operation separation. Separately authorised R2 writers that bypass the Integrity ownership gate require their own safety review; do not assume these protections cover them.

Safety ceilings: horizon 48h; lag 2–24h; scan/request caps 16; contiguous request span 6h; recovery budget 75–90s; cooldown 60–1440min; daily attempts 1–24. Recovery reserves 45s of the existing child budget for finalisation and starts requests only with conservative write/delivery headroom. Shorter remaining time produces a deferral. The outer service timeout must exceed the child budget plus cleanup headroom. Source concurrency is one.

Both ingest and Ops GitHub target mappings list the new variable and reused settings. Per the user's task instruction, the Ops mapping replaces the unavailable ignored master CSV for this change. `INGESTDB_RETENTION_DAYS` remains in shared catalogues for legitimate Prune/other consumers, but Nodes neither reads it nor injects it through its Cloud Run deployment workflow. No remote variables or secrets were changed.

The ingest mapping no longer routes `BLONDON_NODES_BASE_URL`: the Nodes deploy workflow was its only Actions consumer, and Daily Stations already uses the station client's common default without injecting that variable. The standalone station/probe scripts still accept optional local overrides, so their local interfaces are preserved. The latest authoritative TEST documentation records the corresponding Ops catalogue row removal in commit `2878c8a`. This cutoff amendment introduces no variable or catalogue change; no other repository or remote variable was changed in this PR.

## Manual TEST order

1. Confirm the existing Schema #14 recovery interfaces, matching canonical verifier and Ops mapping are available. This code-only cutoff amendment requires **no new migration or reapplication of the existing migration**. With an explicitly selected TEST administrator DSN, the existing read-only schema verifier can be run from the schema checkout:

   ```sh
   psql "$TEST_INGESTDB_ADMIN_DSN" -v ON_ERROR_STOP=1 -f schemas/ingest_db/verify_canonical_ingest_db.sql
   ```

   Expected: the already-existing state/helper/scan/writer objects and six-function runtime grant surface pass the verifier. The pooled runtime remains without core/raw table access.
2. Separately deploy and validate the two-day Prune safeguard from TEST Ops `main` commit `fee4089`, or explicitly attest the same minimum across every actively used Prune path, including overrides/legacy revisions. Recovery must remain disabled until this guarantee is established; source merge alone is insufficient.
3. Confirm the existing Integrity request-wide IngestDB ownership gate remains enforced. Verify the environment label, Pub/Sub topic, shared secret binding, source API key and service/child timeout headroom. Supply the complete enabled JSON with `horizon_hours <= 48` and **without** `not_before_utc`. Normal authorised Integrity requires no recovery pause; any separately authorised R2 writer bypassing that gate requires its own safety review. The common upstream comes from the application constant; do not supply or synchronise a Nodes endpoint or retention variable.
4. Only after separate deployment authorisation, merge the ingest PR. **Its changed paths automatically trigger the Nodes Cloud Run deployment workflow on `main`.** Schema and configuration must be ready first. If deployment is separately approved later, retain disabled JSON until that approval and trigger the same workflow manually when approved:

   ```sh
   gh workflow run uk_aq_blondon_nodes_cloud_run_deploy.yml --repo TEST-uk-aq/uk-aq-ingest --ref main
   ```

5. Accept through real scheduled TEST operations. No migration, merge, deployment, backfill, Integrity run, remote configuration update or LIVE operation was performed by this task.

## Structural checks

The original PR preparation checked Python syntax/imports with the pinned Nodes dependencies; workflow YAML/Bash syntax; SQL/PLpgSQL parsing and canonical/migration alignment; CSV consistency; protected normal-function ASTs; and a narrow local PostgreSQL conflict-safety check. Those checks do not establish TEST operational or source acceptance.

This retention-only amendment uses focused Python/workflow syntax and configuration-boundary checks, verifies that both existing SQL calls receive the fixed argument 3, and confirms that Nodes no longer depends on the retention variable. No schema changes, migration application, live source probe or functional recovery test accompanies this amendment. Real TEST verification must establish the deployed Prune floor, source availability, concurrency and costs after separately authorised deployment.

The shared-endpoint amendment uses focused Python/workflow syntax and client-construction checks with no source requests. It verifies that normal and recovery clients resolve the same constant regardless of an absent or stale endpoint variable, checks that request/capture and recovery logic are unchanged, and confirms the obsolete Actions injection/mapping is gone. Same-day archives are reused unchanged. Functional acceptance remains through real TEST operations after separately authorised deployment.

The internal-cutoff amendment uses focused Python/workflow syntax and configuration checks: simplified enabled JSON is valid, enabled external `not_before_utc` is rejected, the maximum horizon remains 48 hours, and both SQL operations use the copied effective configuration with argument 3. It checks UTC-midnight-minus-two-days derivation without source or database requests. Existing same-day source archives are reused unchanged. No functional recovery or SQL execution is part of these checks.

This documentation-only amendment was reviewed for consistency with the latest TEST Nodes recovery, History Writer Coordination and Prune Daily Phase B contracts. Only this Markdown file changes; no application, database or deployment code changes, code archives, new test suite or functional testing are required.

## Expected overhead and acceptance

With the initial example, each eligible scheduled invocation adds one PostgREST scan (up to eight indexed key reads of at most 74 returned finite keys each), up to eight source requests, at most 48 candidate observations in one pooled write, and normal secondary delivery of committed rows. The hard writer vector ceiling is 96. At 96 invocations/day the absolute example ceilings are 96 scans and 768 source requests/day, plus at most 96 pooled transactions with writes and normal secondary calls. Cooldowns, attempts, real gaps and runtime reduce these ceilings. Catalog/state selection still traverses eligible metadata; there is no full-network observation-history scan.

Small configured secondary batches can require more than one delivery call; remaining-budget checks defer excess work visibly. Unresolved source values are not repeatedly rewritten. Raw capture remains one archive per invocation. Actual log bytes and costs must be measured through TEST, including API Gateway, PostgreSQL and Supavisor, against the organisation's 30 MB/day target; request-count bounds are not measured billing evidence.

Acceptance must demonstrate unchanged normal selection/four-series writes/checkpoints/latest state and duplicate winners; actual missing keys despite newer observations; exact authentic source recovery once; concurrent valid primary data surviving recovery; truthful null/omission/failure/ambiguous-write and secondary diagnostics; cooldown/attempt/fairness/cap behaviour; raw evidence with no secrets; and normal later Prune publication without recreating historical days. Observe complete UTC-day usage and report remaining due timeseries instead of claiming network coverage from one scan.

Verify the full 48-hour rolling window remains eligible, subject to source evidence and candidate/gate exclusions, including at UTC midnight when the minimum cutoffs coincide. Verify the internal compatibility field is recalculated per recovery invocation from UTC time and never treated as independent R2 authority. Confirm that Integrity retains its existing fail-closed request-wide IngestDB ownership gate; no additional recovery disable/drain exercise is required for normal Integrity. Also verify the next real GMT/BST transition (25 October 2026 at 01:00 UTC if deployment precedes it): no shifted, duplicated or invented hours. Offset-free source timestamps deliberately remain unconfirmed. Check UTC-day retry reset independently from local clock changes.

## Rollback and outstanding work

Disable recovery with `{"enabled":false}` and redeploy the same service through separately authorised operations. Alternatively restore the prior known-good image/code revision, ensuring its endpoint/environment settings remain correct. Leave additive schema/state/grants in place for investigation; do not delete authentic recovered observations. If retiring the feature permanently after rollback acceptance, separately review revocation/removal of only the new writer, scan, helper and state, then restore the canonical verifier's five-function expectation. Never remove the shared role or normal compact-v1 grant.

Missing source values that never become numeric, ambiguous timestamps, unsupported/insufficient cadence, unbracketed edges, work expiring outside bounds and already-published historical days require separate assessment. No Integrity invocation or historical repair is automatic, and the source may never supply the measurement.

Chat-mode documentation handover: TEST `main` already records the implementation at `fe0ed52` in `system_docs/ingest/blondon_nodes_recent_hourly_gap_recovery_contract.md` and explains the existing Integrity IngestDB ownership gate there and in `system_docs/r2_history/prune_daily_observation_only_phase_b_contract.md`, with the gate owned by `system_docs/r2_history/history_writer_coordination.md`. This handover now matches that authority. No further contract or SQL interface amendment is needed for this correction; retain future/not-operationally-accepted status until real TEST acceptance. No authoritative documentation was edited here.

After real TEST acceptance only, update the recovery status and ingest README; extend `blondon_nodes_database_transport_contract.md`, `gcp_cloud_run_database_runtime_contract.md` and the canonical apply-order description for the sixth exact grant; clarify in `blondon_nodes_raw_capture_contract.md` that a normal no-due run may capture recovery responses; cross-reference this recovery route from source-normalisation/compact transport docs without changing normal semantics. Record measured costs, authentic recovery evidence (if any), UTC/DST evidence, unresolved historical scopes, confirmation of the existing Integrity IngestDB ownership gate and rollback evidence.
