# SOS recent recovery handover — 21 October 2026

Implementation only; no deployment, migration application, operational recovery/backfill, LIVE access or historical R2 mutation was performed. Read [companion schema PR #13](https://github.com/TEST-uk-aq/uk-aq-schema/pull/13) before enabling recovery.

## Authority and source ownership

Reviewed the latest TEST system-docs `main`, commit `8a4bf63e9ffedc1982bf51f3f90fcc6aeb0feead`, including `ingest/sos/recent_observation_gap_recovery_contract.md`, `contract.md` and `README.md`. Recent recovery is future implementation authority, not deployed behaviour. No authoritative documentation was edited.

**CSV-based acquisition and recovery remain exclusively within History Integrity.** This implementation contains no annual/site CSV acquisition, parsing, caching, cache consumer, scheduler, disabled adapter, acquisition method or CSV-related schema/configuration. The existing `uk_aq_rpc_sos_uk_air_flat_file_mappings` name is reused solely as the authorised metadata bridge; it does not acquire source files.

The existing HTML fallback remains `days=0`, and its parser still rejects observations outside the current UTC day. An unverified multi-day HTML extension was removed during scope review. Earlier-day recovery uses only native SOS getData time spans when the primary probe succeeded. When neither native SOS nor current-day HTML provides the reading, it remains unresolved. No alternate source is attempted.

## Root-cause findings

The supplied incident evidence shows configured `/services` requests timing out from 17 October 2026 06:30 UTC. `upstream_status=null` means no received HTTP status; the connector generated its own 503. Both environments being affected supports a shared upstream/path incident but does not distinguish provider failure, routing, filtering or transport. The original outage has not been diagnosed conclusively or claimed fixed. The last checked-in handler change before this task was 1 October, not the incident boundary. No LIVE investigation occurred. Runtime settings and real provider connectivity were not inspected remotely; this workspace does not permit UK-AIR destination probing, and no restriction was bypassed.

A separate wrapper defect converted JavaScript null to numeric zero, producing `HTTP 0`; this is fixed. Primary HTTP errors are classified before decoding their bodies so malformed/slow error bodies cannot erase genuine HTTP statuses. Successful-response body timeout/deadline failures remain classified as such. The connector-level probe makes one bounded 30-second attempt rather than two. Normal getData retains its existing bounded retry limits, including transient network failures. Endpoint precedence and authentication are preserved.

Permanent gaps were possible because current-day HTML could not supply yesterday's readings and newer latest-value/checkpoint state did not prove intervening hourly rows existed. No-chart HTML failures also represent missing source coverage, not proof that no measurement was taken. Recovery now checks actual database keys.

## Files and schema

- `supabase/functions/ingest_sos/index.ts`: primary request/body classification, bounded probe, explicit availability/acquisition route and degraded fallback health; native parser extraction.
- `supabase/functions/ingest_sos/sos_datapoints.ts`: shared existing native parsing used by normal acquisition and recovery. Empty/boolean/non-numeric source values cannot become artificial zero; no source-value logging was added.
- `supabase/functions/ingest_sos/uk_aq_html_fallback.ts`: one-attempt bounded recovery option, retained days=0, and useful body-failure details.
- `supabase/functions/ingest_sos/uk_aq_html_parser.ts`: reports eligible null-source timestamps separately; current-day filter/signature preserved.
- `workers/uk_aq_sos_cloud_run/recovery.ts`: bounded recent gap detection, lifecycle/hourly-cadence evidence, per-day mapping validity, native SOS/current-day HTML acquisition, retry/cursor/known-gap state and recovery counters.
- `workers/uk_aq_sos_cloud_run/run_job.ts`: runs recovery after normal acquisition and on due claimed runs with no normal selected work; uses the shared compact retry/split writer and existing Observs/outbox path; persists state in existing run JSON.
- `workers/uk_aq_sos_cloud_run/result_contract.ts`: null-to-zero fix and bounded availability/recovery result validation.
- `workers/uk_aq_sos_cloud_run/Dockerfile`: packages required helpers/module; production imports unchanged.
- `.github/workflows/uk_aq_sos_cloud_run_deploy.yml`, `config/uk_aq_github_env_targets.csv`: six recovery variables and existing retention-setting binding.
- Companion `uk-aq-schema`: canonical `schemas/ingest_db/uk_aq_rpc.sql` plus additive `schemas/migrations/20261021_001_ingest_sos_recent_recovery.sql`.
- Exact pre-change snapshots are under `archive/2026-10-21/` in each repository. They are rollback reference only, never active inputs.

The new canonical `uk_aq_rpc_sos_recovery_insert_v1(integer[], timestamptz[], double precision[], text)` is required for concurrency safety: a pre-write query alone cannot protect a valid observation inserted concurrently. It atomically inserts absent keys or fills null values; `ON CONFLICT ... WHERE stored.value IS NULL` prevents stale recovery from revising a valid value. Only active SOS identities, finite numeric values, UTC hourly timestamps, at most 72 hours of age, at least two hours of publishing age and established acquisition methods `sos`/`ukair_html` are accepted. It stores null status, never inventing P/R. Service-role function-only access is added; no pooled runtime role/table privilege expansion occurs. Normal compact v1/v2 behaviour, latest-value state, logical identity and observation table shape remain unchanged. This migration is exclusively for SOS/HTML write safety, with no CSV functionality.

## Recovery design

Normal checkpoint-driven native SOS or authorised current-day HTML fallback runs first. Normal acquisition retains its full 600-second internal budget. Recovery runs afterwards for at most its configured 60 seconds within a total 660-second parent envelope, leaving the existing 900-second outer timeout for cleanup and reporting. Recovery never extends that parent deadline. The outer 900-second timeout, due/claim overlap protection, scheduler cadence and normal selected-work caps stay unchanged. Slow normal cleanup can defer recovery visibly.

The recovery cursor walks active SOS internal timeseries IDs independently of station due selection. It reads at most 48 series and their actual recent hourly database keys, using connector/timeseries/time filters, deterministic pages and the existing `(timeseries_id, observed_at)` index. It excludes hours before `first_value_at`, requires at least one consecutive-hour pair in retained observations as hourly-cadence evidence, and reports unconfirmed lifecycle/cadence rather than labelling all absent rows expected measurements. Supported pollutant/unit eligibility uses the existing bridge rules; CO remains disabled. Explicit source nulls are counted separately from source failure/no-chart/missing points.

The configurable 72-hour window excludes the newest two publishing hours and is clamped to one complete UTC day inside the existing `INGESTDB_RETENTION_DAYS` boundary. No out-of-retention observations are reintroduced. Data outside this boundary belongs to the existing authorised History Integrity procedures, including their retained-IngestDB ownership gates.

Bridge validity and ambiguity are checked independently for every observation UTC day. Current mapping is not authority for yesterday; station mismatch and unsafe/ambiguous mappings fail closed. No references are created or remapped.

If the primary probe succeeded, recovery requests the existing `/timeseries/<source-ref>/getData?timespan=...&format=tvp` route for the contiguous envelope of missing hours in that timeseries. The end includes the final missing hour's interval boundary. Returned data uses the same native parser as normal ingestion, is cached within the run, and only exact missing-hour keys are emitted. If primary acquisition is unavailable, only today's gaps can be attempted with the authorised days=0 HTML page, grouped by public site. Earlier-day gaps remain explicit unresolved work. Recovery does not issue another services probe, bypass a failed probe, use local Integrity evidence, or invent a historical HTML route.

At most four source requests are made per run, sequentially, with one acquisition attempt each. Native requests use a bounded 15-second timeout; HTML shares the remaining acquisition budget while reserving database time. Empty/missing source data and failures receive a configurable cooldown (default three hours per native timeseries or HTML site). Request-cap stops leave the next unprocessed row at the cursor so one early subset does not monopolise every sweep. Source page content is cached within the run only; persistent cooldown prevents continuous re-fetching of unavailable hours.

Epoch timestamps preserve the actual hour-ending UTC identity. No subtraction, local timezone correction, textual CSV 24:00 parser or source-year guessing is added. Future, non-hourly, out-of-window, invalid or non-finite candidates are rejected. UTC midnight/year rollover and UK DST use the same represented instant. Current-day HTML cannot recover a prior-day hour, even if removing the filter would expose another date.

Candidates are deduplicated and batched by acquisition method through the shared compact retry/split writer and the conflict-safe canonical RPC. Durable current values are read back for unchanged Observs/outbox delivery, so a concurrent primary observation wins over a stale recovery candidate in both paths. Cross-database delivery remains non-atomic under the existing contract: a deadline/delivery failure after an IngestDB commit can leave conservative recovery counts. Inspect durable keys and existing delivery/outbox state; do not infer rollback.

Recovery does not advance station last-polled/last-observed/next-due/learned-lag or successful-timeseries checkpoints. Recovery-only requests are separate from normal attempted/polled counters. Its cursor, cooldowns and bounded counts of known unresolved gaps persist in `uk_aq_ingest_runs.response_payload.sos_recovery_state`; no recovery table is created. Malformed state fails closed. Run retention must retain at least one state-carrying run over the horizon. Forced process termination or failed run persistence can lose the newest pacing state; request-per-run/due/claim caps still hold.

Run results expose primary availability/real failure, route, current batch missing/recovered/unresolved counts, retained known unresolved counts, cadence uncertainty, retention exclusion, mapping/source absence, requests/retries and request/runtime limits. Known gaps survive a later clean scan batch and expire with recent-window eligibility. Counts can remain conservatively stale until the series is rescanned after normal acquisition fills a gap. `recovery_scope=bounded_scan` and `batch_complete` never assert all-station completeness. Recovery partial/failure remains separate in its fields, makes an otherwise successful run partial/207, and does not hide a failed primary dependency result.

## Configuration and workload

| Setting | Default | Bounds / requirement |
|---|---:|---|
| `SOS_RECOVERY_ENABLED` | true | exactly true/false; normal acquisition retains its full budget in either mode |
| `SOS_RECOVERY_WINDOW_HOURS` | 72 | integer 24–72, additionally clamped by retention |
| `SOS_RECOVERY_SCAN_LIMIT` | 48 | integer 1–96 |
| `SOS_RECOVERY_SITE_LIMIT` | 4 | integer 1–8 total native-timeseries or HTML-site requests/run |
| `SOS_RECOVERY_RETRY_MINUTES` | 180 | integer 60–720 |
| `SOS_RECOVERY_MAX_RUNTIME_SECONDS` | 60 | integer 45–120; outer timeout must exceed normal plus recovery budgets and cleanup time |
| existing `INGESTDB_RETENTION_DAYS` | required when enabled | integer 1–30; must match actual TEST retention policy, no guessed deployment value |

Existing `SUPABASE_URL`, `SB_SECRET_KEY`, SOS endpoint precedence, cron/upstream authentication and Observs/Pub/Sub configuration remain required and environment-specific. No new secret or cross-environment fallback. Normal pooled SOS writes stay pooled. The small recovery-only RPC uses service-role PostgREST with no automatic transport fallback. Missing required retention configuration fails explicitly; apply the schema/configuration before enabling recovery.

At the normal 15-minute cadence, source traffic is capped at 4/run = 384/day before cooldown reductions. Default database scans normally use one state read, one metadata page (occasionally another at cursor wrap), one bridge RPC and up to four observation pages. Nonempty batches add normally one compact write, bounded authoritative reread, coverage verification and existing delivery overhead. Rough additional hosted traffic at default bounds is about 7–17 calls/run (672–1,632/day), plus bounded shared retries/splits and Observs delivery when needed. A recovery run chooses one route, so it normally uses one method batch. Higher configured limits increase these caps. No per-observation success logging is added; one compact summary and existing run persistence are reused. Measure real TEST Supabase organisation usage against the 30 MB/day log-ingestion contract; request estimates are not billable-byte measurements or evidence of compliance.

The operator-local ignored ops `env-vars-master.csv` is absent in this workspace. Its owner must add the six new variable rows and retain the existing retention setting. Ingest's target mapping/deploy workflow are updated. No remote configuration was changed.

## Structural validation

Deno 2.5.0 checks passed for the changed handler and Cloud Run child/service dependency graphs. Production dependency hosts deno.land/esm.sh are blocked in this workspace; a temporary, uncommitted import map supplied the old serve(handler, options) type signature and exact npm `@supabase/supabase-js@2.49.8` for structural checking. TLS verification stayed enabled with the system CA store. Production imports were not rewritten; the unmodified production dependency build remains a TEST deployment check.

The existing `tests/ingest_sos_failure_test.ts` passed all nine checks. SQL and PL/pgSQL parser checks passed for the additive migration, with workflow YAML/canonical-migration matching and whitespace checks included. No SQL was executed, no broad suite or speculative fixtures were created, and no functional ingestion acceptance is claimed.

## Manual TEST deployment — not executed

Review both PRs and the documentation handover first. The schema migration must precede enabling recovery. Merging ingest changes can trigger the existing deploy-on-main workflow, so merge only within a separately authorised TEST deployment.

1. In the reviewed TEST schema checkout, apply only the additive migration using a verified TEST administrative connection:

   ```bash
   : "${TEST_INGESTDB_ADMIN_URL:?Set a verified TEST IngestDB admin connection}"
   psql "$TEST_INGESTDB_ADMIN_URL" -X -v ON_ERROR_STOP=1 \
     -f schemas/migrations/20261021_001_ingest_sos_recent_recovery.sql
   ```

   Expected: one function is created/replaced and a PostgREST schema reload notified. No observation data changes; no whole-schema apply.

2. Record the current TEST Cloud Run revision for rollback. Update the operator master catalogue. Match the actual TEST retention policy, without changing it, and set the six recovery variables:

   ```bash
   : "${TEST_INGESTDB_RETENTION_DAYS:?Use the verified existing TEST retention value}"
   gh variable set INGESTDB_RETENTION_DAYS --body "$TEST_INGESTDB_RETENTION_DAYS" --repo TEST-uk-aq/uk-aq-ingest
   gh variable set SOS_RECOVERY_ENABLED --body true --repo TEST-uk-aq/uk-aq-ingest
   gh variable set SOS_RECOVERY_WINDOW_HOURS --body 72 --repo TEST-uk-aq/uk-aq-ingest
   gh variable set SOS_RECOVERY_SCAN_LIMIT --body 48 --repo TEST-uk-aq/uk-aq-ingest
   gh variable set SOS_RECOVERY_SITE_LIMIT --body 4 --repo TEST-uk-aq/uk-aq-ingest
   gh variable set SOS_RECOVERY_RETRY_MINUTES --body 180 --repo TEST-uk-aq/uk-aq-ingest
   gh variable set SOS_RECOVERY_MAX_RUNTIME_SECONDS --body 60 --repo TEST-uk-aq/uk-aq-ingest
   ```

   Preserve the existing 15-minute cadence, 600-second normal budget, at most 60 seconds additional recovery, 900-second outer timeout, normal pooled transport, authenticated service access and environment-specific identities/secrets.

3. Deploy the reviewed TEST branch through the existing workflow, or merge only as part of the separately authorised deployment process:

   ```bash
   gh workflow run uk_aq_sos_cloud_run_deploy.yml \
     --repo TEST-uk-aq/uk-aq-ingest --ref fix/sos-recent-hourly-recovery
   gh run list --repo TEST-uk-aq/uk-aq-ingest \
     --workflow uk_aq_sos_cloud_run_deploy.yml --limit 1
   ```

   Verify production dependency caching/image build, included helper modules, settings and preserved authentication/outer timeout. The handler remains in the active Cloud Run workflow/image. No separate hosted edge deploy is required for the active Cloud Run backend; legacy edge-only scheduling does not run the new recovery pass.

4. Observe ordinary due TEST runs. Missing RPC/configuration must produce a visible failure/partial result, never a silent alternate writer. Do not run a historical job as part of this deployment.

## Real TEST operational verification

Use existing run dashboards, a bounded log window and read-only queries:

```sql
select run_started_at, run_status, response_status, run_message,
       response_payload->'primary_sos_available' as primary_sos_available,
       response_payload->'upstream_failure_kind' as failure_kind,
       response_payload->'upstream_status' as upstream_status,
       response_payload->'recovery_status' as recovery_status,
       response_payload->'recovery_missing' as missing,
       response_payload->'recovery_recovered' as recovered,
       response_payload->'recovery_unresolved' as unresolved_in_batch,
       response_payload->'recovery_unresolved_known' as unresolved_known,
       response_payload->'recovery_sos_requests' as sos_requests,
       response_payload->'recovery_html_requests' as html_requests,
       response_payload->'recovery_source_unavailable' as source_nulls,
       response_payload->'recovery_cadence_unconfirmed' as cadence_unknown,
       response_payload->'recovery_retention_excluded_hours' as retention_excluded,
       response_payload->'recovery_retry_deferred' as deferred
from uk_aq_core.uk_aq_ingest_runs
where connector_code = 'sos'
order by run_ended_at desc limit 12;
```

Confirm real timeouts retain null upstream status and meaningful request-timeout text, with no HTTP 0 or claim of provider 503. Fallback remains degraded. Native recovery requests must target missing series/time spans and retain `acquisition_method=sos`; HTML requests remain days=0 with `ukair_html`, today-only epochs and null source status. Earlier-day gaps stay unresolved during SOS outage. No measurement is generated for null/missing source data. Missing lifecycle/cadence evidence is explicit, not a fabricated expected sample.

Resolve Bloomsbury's PM2.5 internal identity using the bridge (no guessed site reference):

```sql
select distinct m.timeseries_id, m.site_ref, m.valid_from_day_utc, m.valid_to_day_utc
from uk_aq_public.uk_aq_rpc_sos_uk_air_flat_file_mappings(
  '2026-10-20'::date, '2026-10-21'::date, array['pm25']::text[]
) m
join uk_aq_core.stations s on s.id = m.station_id
where s.label ilike '%Bloomsbury%';
```

For the resolved ID, inspect real rows at `2026-10-20T23:00:00Z` and `2026-10-21T00:00:00Z`, including value/status/method/creation time. These are targets for verification, not claimed recovered rows. The 23:00 previous-day value is unrecoverable by current-day HTML. Either value remains unresolved when native SOS is unavailable and HTML does not supply it. If deployment occurs after these readings leave recent eligibility, inspect an equivalent naturally missing recent overnight pair; do not widen the safety ceiling or claim historical repair.

Across actual recurring TEST runs, verify cursor progress, cooldown survival after failed sources, capped requests, idempotence, unchanged valid values/provenance, no normal checkpoint effects, and durable existing Observs/outbox delivery. Known unresolved counts must not vanish on an unrelated clean batch. Observe a real UTC midnight boundary and the 25 October 2026 Europe/London DST transition without shifted/duplicate/missing UTC hours. At year rollover the native epoch keeps its actual year and observation-day mapping. No CSV timestamp parsing is involved.

Inspect one complete UTC day of TEST Supabase organisation log usage and normal-ingestion latency/runtime; honestly label shorter windows. Acceptance remains pending until real TEST proves authentic recovery when a source provides the reading, truthful unresolved outcomes otherwise, no value downgrade and bounded work without starvation.

## Separately controlled historical recovery

No historical recovery is authorised or performed here. After accepted TEST operation, report exact remaining 17–20 October hourly keys and ownership/retention state for a separately authorised History Integrity investigation. History Integrity exclusively owns annual/site CSV acquisition, cache/evidence, parsing and CSV-based historical repair/R2 reconciliation. Its existing retained-IngestDB/date gates still apply; do not use Integrity to bypass them or stretch ordinary ingest's horizon. No source-cache reads, CSV adapter, automatic Integrity invocation or direct R2 writes belong in this repository.

## Rollback

Set `SOS_RECOVERY_ENABLED=false` and redeploy to stop recovery while retaining the full normal budget and primary diagnostics. Alternatively return TEST traffic to the previously recorded Cloud Run revision using existing TEST project/region/service settings. Revert reviewed commits rather than executing archives. Leave the additive RPC inert during rollback. Never delete recovered readings or revert provenance merely to roll back application code. Once all callers are retired, separately retire only `uk_aq_public.uk_aq_rpc_sos_recovery_insert_v1(integer[], timestamptz[], double precision[], text)` if required; normal compact RPCs/schema/roles remain intact.

## Documentation handover

Chat-mode owners should finalise interfaces/operations around the future recent-recovery contract, then promote observed behaviour only after TEST acceptance:

- SOS interfaces: bounded primary/acquisition/recovery fields, retained known-gap scope and limits; private pacing state is excluded from the child response.
- SOS operations/validation: companion conflict-safe SOS/HTML RPC, six variables plus required existing retention binding, rollout order, normal/recovery budget allocation, lifecycle/cadence uncertainty, real midnight/DST checks and rollback.
- Observation transport/provenance: additive service-role recovery-only conflict semantics, unchanged normal v1/v2/pooler and ObsAQIDB contracts, existing `sos`/`ukair_html` values, null status and no CSV extension.
- Current HTML contract/parser remain current-day-only. No earlier-day HTML capability is claimed. CSV-based recovery remains exclusively History Integrity.
- Record real TEST source coverage, unresolved keys, runtime and organisation log usage before claiming operational acceptance.
