# Bounded ingestion implementation — TEST, 8 November 2026

Implementation handover, not an authoritative system contract or operational acceptance record. No deployment, migration application, merge, LIVE access, historical deletion, Integrity run or IFW run was performed.

## Behaviour implemented

| Area | Result |
| --- | --- |
| Common ownership | Required `INGESTDB_RETENTION_DAYS` (integer >= 2), matching effective Prune. A service-role read-only RPC returns each connector's lower bound: the later of UTC midnight minus retention days and the day after its latest existing prune gate/history candidate. Consumed/false gates also fence history. Physical observation rows do not establish eligibility. Missing configuration/RPC/connector boundary fails explicitly. |
| Communities TypeScript | Existing `(station_id, species)` checkpoints drive independent window selection. Healthy species normally replay six hours; stale/error species replay at least 24 hours; polling interruptions widen to at most 48 hours. No historical window traversal and no high-water exclusion of returned observations. Manual `start_date` selects one retained slice of at most 24 hours. |
| Communities budgets | At most 20 stations and 40 species windows per invocation, each source call allowing at most three existing retry attempts, plus at most one three-attempt station catalogue acquisition. Response processing rejects arrays over 10,000 points. Runtime budgeting is mandatory, capped at 600 seconds, with acquisition cutoff/cleanup headroom; the deployment workflow requires outer timeout >= effective internal budget + 180 seconds. Source retry timeouts respect the acquisition deadline. |
| Communities persistence | Attempts persist before acquisition; completed batch progress persists immediately after primary commit; source/latest/write failures remain visible. Final checkpoint persistence errors are fatal. Incomplete attempts retain `attempt_incomplete` until completion/error reporting. Completed species survive another species' failure. Safe replay repairs uncheckpointed work. Station scheduling and freshness remain separate summaries. |
| Communities Python | Ordinary bootstrap, checkpoint override and manual start options use the same bounded retained windows. Implements the previously missing `SupabaseWriter.upsert_observations` through the existing compact/shared writer. Uses paired atomic latest updates and immediate per-species progress. Retry exhaustion is an error for ingestion; the reference client's default behaviour is unchanged. |
| Communities selector | Stale membership orders by oldest polling attempt, then observation age and station ID. The reserved stale group is returned first so a partial runtime does not routinely consume only healthy capacity. Existing eligibility predicates, quotas and retry intervals remain unchanged. The ingest handler preserves selector order. |
| OpenAQ | Hourly queries replay a recent 6–48-hour interval, clipped to ownership, rather than starting at an old station/species checkpoint. `/latest`, provider request budgets, scheduling and recent-gap detection remain. Invalid/future/stale hourly points are excluded from both gap evidence and writing. Future hourly timestamps are rejected instead of being replaced by ingestion time. Empty requests do not advance station observation progress from companion checkpoint state alone. |
| Nodes | Adds source timestamp/response, configurable/manual request and retry-write guards. Existing species overlap, final-valid-duplicate selection and recent-hour recovery remain; recovery's existing separate missing/null-only SQL writer and its narrower bounds are unchanged. |
| UK-AIR SOS | Guards normal native requests and native/HTML responses, including Python ordinary refresh. Rechecks bounds per request/retry. Preserves station attempt and successful-timeseries checkpoint distinctions and existing recovery. Null-valued observations no longer advance a latest timestamp independently of its value; equal-timestamp numeric HTML corrections can reach the atomic writer. Explicit historical Python backfill remains a separate maintenance entry point. |
| WAQN/SAQN/NI | Clips configurable/manual windows and filters again before primary/latest/secondary delivery. Preserves graph parsing, UK-local timestamp conversion, NI graph-period selection, HTML supplement, precedence and independent failures. Indivisible graph responses may exceed retention; returned observations cannot. |
| Sensor.Community | TypeScript edge, Node Cloud Run and Python reject missing, invalid, future and expired source timestamps. Known source UTC timestamps without offsets retain UTC interpretation. No ingestion-time replacement. Latest state is committed through the atomic paired RPC after primary observations. |
| Black Carbon | No changes; its R2-only observation architecture is excluded. |

All initial observations are filtered before derived latest state. Writers recheck eligibility for each attempt, including midnight changes; a batch that ages out during retry fails explicitly for safe replay rather than claiming filtered rows were committed. Secondary/latest delivery also rechecks eligibility. Published-prefix metadata is read at invocation start; UTC retention is recalculated during the invocation. Prune and ingestion must use the same effective retention value, including any Prune invocation override. Do not change that value inconsistently while ingestion is running.

## Existing state reused and companion SQL

No tables or columns are added. Reused state:

- `uk_aq_raw.blondon_communities_timeseries_checkpoints`: `station_id`, `species`, `timeseries_id`, `last_observed_at`, `last_polled_at`, `last_error`, `updated_at`.
- `uk_aq_raw.blondon_communities_station_checkpoints`: `station_id`, `last_observed_at`, `last_polled_at`, `next_due_at`, `ingest_lag_samples`.
- Existing OpenAQ/SOS/Nodes checkpoint fields and recovery interfaces.
- `uk_aq_core.timeseries.last_value` and `last_value_at`.
- Existing `uk_aq_ops.prune_connector_day_gates` and `history_candidates` connector/day metadata.

Companion schema branch: `TEST-uk-aq/uk-aq-schema`, `codex/bounded-ingestion-checkpoints`.
Migration: `schemas/migrations/2026-11-08_bounded_ingestion_checkpoints.sql`.
Canonical owners are `schemas/ingest_db/uk_aq_rpc.sql`, `uk_aq_raw_schema.sql` and `uk_aq_blondon_communities_polling_helper.sql`.

The existing latest-value compact RPC performs one atomic SQL UPDATE conditioned on incoming timestamp >= stored timestamp. It selects the newest input per timeseries, with the final input winning same-timestamp ties, and ignores non-finite/null timestamp inputs. It keeps the existing signature, return shape and grants. A client read-then-write check could race and is therefore insufficient. Observation insertion remains separate and accepts duplicate-safe retained late readings. All active callers were inspected: Communities, OpenAQ, SOS pooled/REST, Nodes, official networks and Sensor.Community. Python/edge paths previously writing latest state directly now use that RPC.

The new read-only `uk_aq_public.uk_aq_rpc_ingest_retention_boundaries(integer)` is granted only to `service_role`; it does not extend the pooled runtime identity or mutate Prune gates. Two Communities table triggers retain monotonic observation/attempt progress across overlaps and preserve error ownership by the latest polling attempt. They use existing fields only.

## Targeted checks and limitations

Confirmed from TEST GitHub checkouts: checkpoint definitions/readers/writers/identities; canonical grants; Prune's UTC-day calculation (Phase B ends at the complete day before the retention floor) and minimum two-day safeguard; compact write/commit interfaces; Communities 5-minute/1-hour/12-hour retry tiers and stale quota; Cloud Run timeouts/import layout; existing SOS/Nodes recovery interfaces; all affected latest-RPC consumers.

Environment status supplied no TEST database secrets, outbound database identity or runtime variables. No TEST checkpoint rows, live grants, deployed revisions or effective remote retention values were inspected. These operational checks remain required. The operator-owned, ignored `env-vars-master.csv` is absent. Both repository target maps already contain `INGESTDB_RETENTION_DAYS`; no new variable/secret names are introduced and no ops changes are needed. Its private catalogue description and effective value must be confirmed by the operator.

Structural validation:

- Deno 2.5 checks of all four changed edge handlers and their local import graph. This workspace blocks `deno.land`/`esm.sh`; validation used a temporary import map to the same std 0.224.0 GitHub sources and Supabase JS 2.49.8 npm package, with normal system CA trust. Production imports were not redirected.
- Python AST parsing, including the real SOS hash-guarded dynamic station-identity patch and its resulting source.
- Node syntax checks; changed workflow YAML and Bash syntax; existing GitHub environment-target validator.
- SQL and PL/pgSQL parsing of the migration, plus parsing of the changed canonical SQL owners.
- Container COPY/relative-import graph validation for Communities, OpenAQ, SOS and Sensor.Community. Python images already copy `scripts/` and include `requests`.
- One small deterministic retention check for UTC floor, midnight, published prefix, invalid/future timestamp and missing/invalid configuration boundaries. This does not simulate deployed ingestion.
- Updated existing writer structural checks to reflect the atomic latest path and paired null handling; targeted results are recorded in the PR.
- Exact pre-change archive bytes compared with Git HEAD; whitespace checks.

No container build, SQL application, external-source ingest or operational acceptance is claimed.

Schema review: https://github.com/TEST-uk-aq/uk-aq-schema/pull/19. This migration supplements the schema changes that reached `main` during implementation. Revalidate the final SQL order and grants before applying it.

## Required deployment order (separately authorised)

1. Review/merge the schema PR, then apply the migration to the operator-selected **TEST IngestDB administrative connection**:

   ```sh
   psql '<TEST IngestDB administrative connection string>' -v ON_ERROR_STOP=1 -f schemas/migrations/2026-11-08_bounded_ingestion_checkpoints.sql
   psql '<same TEST connection>' -v ON_ERROR_STOP=1 -f schemas/ingest_db/verify_canonical_ingest_db.sql
   ```

   Verify the ownership RPC with the actual retention value; verify service-role execution, denied anonymous/authenticated execution, both checkpoint triggers and the atomic latest function. Check existing rows/identities and the actual deployed Prune configuration/overrides. No data correction or deletion is part of the migration.

2. Confirm the existing `INGESTDB_RETENTION_DAYS` value is present in TEST ingest repository configuration and equals effective Prune. Confirm Communities outer timeout leaves required headroom. Keep existing source credentials, endpoints, pooled database secrets and transport settings.
3. Only after schema/configuration readiness, authorise the ingest merge/deployment. Existing `main` push workflows automatically deploy affected Cloud Run services: Communities, OpenAQ, SOS, Nodes, Sensor.Community and all three official-network services. Do not merge this application PR before applying the schema prerequisite.
4. If edge backends are used, explicitly run the new manual-only `uk_aq_ingest_edge_deploy.yml` workflow after the same prerequisite. It checks the ownership RPC, sets only the existing retention variable and deploys the selected four existing ingest functions. It preserves their existing credentials and cron-auth configuration. Merge alone does not deploy edge backends.
5. Ordinary Python invocations need the same existing database/source environment and retention value. Communities manual retained recovery uses `--start-date <retained UTC timestamp>` with the existing station/species selectors; each invocation covers at most 24 hours per species. Repeat deliberately for another retained slice. Old starts are clipped to ownership; routine `--initial-days`/`--ignore-checkpoints` cannot restore historical traversal.

Rollback: restore the pre-change application revision first if necessary. Prefer retaining the additive ownership RPC and monotonic SQL protections; removing them while new applications run causes explicit failures. Pre-change TEST SQL/code snapshots are under each repository's `archive/2026-11-08/`; they are reference material, not executable rollback scripts. Any SQL rollback requires a separately reviewed migration and must consider already committed progress. No rollback should delete observations or undo historical ownership.

## Real TEST acceptance after deployment

- Inspect independent INO2/IPM25 progress. A source-backed missing PM2.5 point older than NO2 and older than PM2.5's own high-water mark must insert when retained; its newest progress must not regress.
- Observe overlapping normal/recovery acquisitions. Older retained readings must reach observations without replacing a newer latest pair; an actual same-timestamp source correction must update the pair consistently.
- Observe healthy six-hour, stale/error 24-hour and interruption-bounded requests. Old checkpoints must not generate historical traversal. Manual recovery must remain a single eligible slice.
- Confirm stale retries rotate by polling attempts, preserve configured quotas/intervals, and receive capacity during partial runs.
- Observe a genuine source/write failure or deadline: completed species/batches remain, incomplete/error checkpoints are truthful, final persistence failure is visible and replay is safe. Check request/work/runtime counts and cleanup before the outer timeout.
- Inspect OpenAQ hourly query bounds with an old companion checkpoint; verify `/latest`, budgets, recent-gap detection and empty-response progress remain correct.
- Observe ingestion crossing UTC midnight and verify retry/write/secondary/latest/checkpoint evidence excludes newly expired source readings. Confirm invalid, future and missing timestamps never become ingestion timestamps.
- Confirm Nodes/SOS normal overlap, station-attempt versus successful-timeseries semantics, HTML handling and existing recent-hour recovery continue.
- Confirm WAQN/SAQN/NI preserve source-supported response periods, timezone handling, graph/HTML precedence and independent failure handling while excluding expired returned observations.
- Check published/candidate day protection after any retention widening; delayed physical deletion must not reopen historical ownership. Confirm no Prune gate changes, historical deletion, automatic Integrity/IFW activity or Black Carbon effect.

## Chat-mode documentation handover

Authoritative `uk-aq-system-docs/system_docs/` was not edited. Chat-mode maintenance should update the ingest router and applicable Communities/polling, compact latest transport, SOS normal polling/transport, Nodes, official-network and Sensor.Community contracts with the behaviour above. Specifically replace the prior SOS timestamp-only latest fallback and optional Communities runtime-budget descriptions; distinguish per-species progress from station scheduling; document bounded late-arrival replay and retained manual slices; describe indivisible source responses and historical Integrity/IFW ownership; record the additive SQL prerequisite and existing retention variable requirement without claiming deployment/acceptance. Preserve the current history architecture, recovery-only SQL semantics and Black Carbon exclusion.

## Exact changed-file and archive inventory

### `uk-aq-schema`

Changed or added active files:

- `schemas/ingest_db/uk_aq_blondon_communities_polling_helper.sql`
- `schemas/ingest_db/uk_aq_raw_schema.sql`
- `schemas/ingest_db/uk_aq_rpc.sql`
- `schemas/migrations/2026-11-08_bounded_ingestion_checkpoints.sql`

Exact pre-change archives:

- `archive/2026-11-08/schemas/ingest_db/uk_aq_blondon_communities_polling_helper.sql`
- `archive/2026-11-08/schemas/ingest_db/uk_aq_raw_schema.sql`
- `archive/2026-11-08/schemas/ingest_db/uk_aq_rpc.sql`

### `uk-aq-ingest`

Changed or added active files:

- `.github/workflows/uk_aq_blondon_communities_cloud_run_deploy.yml`
- `.github/workflows/uk_aq_blondon_nodes_cloud_run_deploy.yml`
- `.github/workflows/uk_aq_ingest_edge_deploy.yml`
- `.github/workflows/uk_aq_official_networks_cloud_run_deploy.yml`
- `.github/workflows/uk_aq_openaq_cloud_run_deploy.yml`
- `.github/workflows/uk_aq_raw_dropbox.yml`
- `.github/workflows/uk_aq_scomm_cloud_run_deploy.yml`
- `.github/workflows/uk_aq_sos_cloud_run_deploy.yml`
- `scripts/blondon_communities/blondon_communities_ingest.py`
- `scripts/blondon_communities/blondon_communities_list_stations.py`
- `scripts/blondon_nodes/blondon_nodes_ingest.py`
- `scripts/dev/_apply_sos_station_identity_fix.py`
- `scripts/official_networks/official_network_ingest.py`
- `scripts/sensorcommunity/sensorcommunity_ingest.py`
- `scripts/sos/sos_ingest_legacy.py`
- `scripts/uk_aq_retention.py`
- `supabase/functions/_shared/retention.mjs`
- `supabase/functions/ingest_breathelondon/index.ts`
- `supabase/functions/ingest_openaq/index.ts`
- `supabase/functions/ingest_sensorcommunity/index.ts`
- `supabase/functions/ingest_sos/index.ts`
- `supabase/functions/ingest_sos/sos_datapoints.ts`
- `tests/ingestdb_observation_writer.test.mjs`
- `tests/test_ingestdb_observation_writer.py`
- `workers/uk_aq_blondon_communities_cloud_run/Dockerfile`
- `workers/uk_aq_blondon_communities_cloud_run/README.md`
- `workers/uk_aq_openaq_cloud_run/Dockerfile`
- `workers/uk_aq_sensorcommunity_cloud_run/Dockerfile`
- `workers/uk_aq_sensorcommunity_cloud_run/index.mjs`
- `workers/uk_aq_sos_cloud_run/Dockerfile`
- `BOUNDED_INGESTION_HANDOVER.md`

Exact pre-change archives:

- `archive/2026-11-08/.github/workflows/uk_aq_blondon_communities_cloud_run_deploy.yml`
- `archive/2026-11-08/.github/workflows/uk_aq_blondon_nodes_cloud_run_deploy.yml`
- `archive/2026-11-08/.github/workflows/uk_aq_official_networks_cloud_run_deploy.yml`
- `archive/2026-11-08/.github/workflows/uk_aq_openaq_cloud_run_deploy.yml`
- `archive/2026-11-08/.github/workflows/uk_aq_raw_dropbox.yml`
- `archive/2026-11-08/.github/workflows/uk_aq_scomm_cloud_run_deploy.yml`
- `archive/2026-11-08/.github/workflows/uk_aq_sos_cloud_run_deploy.yml`
- `archive/2026-11-08/scripts/blondon_communities/blondon_communities_ingest.py`
- `archive/2026-11-08/scripts/blondon_communities/blondon_communities_list_stations.py`
- `archive/2026-11-08/scripts/blondon_nodes/blondon_nodes_ingest.py`
- `archive/2026-11-08/scripts/dev/_apply_sos_station_identity_fix.py`
- `archive/2026-11-08/scripts/official_networks/official_network_ingest.py`
- `archive/2026-11-08/scripts/sensorcommunity/sensorcommunity_ingest.py`
- `archive/2026-11-08/scripts/sos/sos_ingest_legacy.py`
- `archive/2026-11-08/supabase/functions/ingest_breathelondon/index.ts`
- `archive/2026-11-08/supabase/functions/ingest_openaq/index.ts`
- `archive/2026-11-08/supabase/functions/ingest_sensorcommunity/index.ts`
- `archive/2026-11-08/supabase/functions/ingest_sos/index.ts`
- `archive/2026-11-08/supabase/functions/ingest_sos/sos_datapoints.ts`
- `archive/2026-11-08/workers/uk_aq_blondon_communities_cloud_run/Dockerfile`
- `archive/2026-11-08/workers/uk_aq_openaq_cloud_run/Dockerfile`
- `archive/2026-11-08/workers/uk_aq_sensorcommunity_cloud_run/Dockerfile`
- `archive/2026-11-08/workers/uk_aq_sensorcommunity_cloud_run/index.mjs`
- `archive/2026-11-08/workers/uk_aq_sos_cloud_run/Dockerfile`

