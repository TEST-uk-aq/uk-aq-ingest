# New DB Install

This guide is for bringing up fresh Supabase projects for:
- MAIN Ingest DB (UK AQ ingest + APIs)
- HISTORY DB (history observations store)

## 1. MAIN Ingest DB install

Run SQL in this order.

1. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/ingest_db/uk_aq_core_schema.sql`
2. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/ingest_db/uk_aq_raw_schema.sql`
3. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/ingest_db/uk_aq_pop_schema.sql`
4. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/ingest_db/uk_aq_rpc.sql`
5. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/ingest_db/uk_aq_public_views.sql`
6. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/ingest_db/uk_aq_security.sql`
7. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/ingest_db/main_db_dualwrite_bootstrap.sql`
8. `supabase/uk_aq_polling_helpers.sql`

`main_db_dualwrite_bootstrap.sql` includes Phase B backup ops objects for prune safety and resumable backup exports:
- `uk_aq_ops.backup_candidates`
- `uk_aq_ops.prune_day_gates`
- `uk_aq_ops.uk_aq_phase_b_backup_rows(...)`

Then configure Supabase Data API exposed schemas for the MAIN project:

1. Open Supabase Dashboard -> Settings -> Data API.
2. Ensure exposed schemas include: `public`, `uk_aq_core`, `uk_aq_raw`, `uk_aq_public`.
3. Save changes before running workflows/scripts that use PostgREST.

Notes:

1. Exposed schemas is a Supabase project setting (dashboard/API), not a SQL migration.
2. If `uk_aq_core`/`uk_aq_raw` are not exposed, PostgREST calls can fail with `406 PGRST106` errors.
3. Main DB observation-write payload metrics are available in `uk_aq_public.uk_aq_observation_rpc_metrics_minute`.

Then set MAIN project runtime secrets:

1. `OBS_AQIDB_SUPABASE_URL`
2. `OBS_AQIDB_SECRET_KEY`
3. Optional: `OBSERVS_UPSERT_RPC` (default `uk_aq_rpc_observs_observations_upsert`)
4. Optional: `OBSERVS_OUTBOX_FLUSH_LIMIT` (default `40`)
5. Optional: `OBSERVS_UPSERT_CHUNK_SIZE` (default `5000`)
6. Optional: `OBSERVS_OUTBOX_CLOUD_RUN_MAX_BATCHES` (default `30`)
7. Optional: `OBSERVS_OUTBOX_CLOUD_RUN_CLAIM_BATCH_LIMIT` (default `20`)
8. Optional: `OBSERVS_OUTBOX_CLOUD_RUN_BUDGET_SECONDS` (default `540`)

## 2. HISTORY DB install

Run SQL in this order.

1. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_schema.sql`
2. `../CIC-Test-UK-AQ-Schema/uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_dualwrite_bootstrap.sql`

Notes:

1. History observations uses ID keys: `(connector_id, timeseries_id, observed_at)`.
2. History upsert RPC: `uk_aq_public.uk_aq_rpc_observs_observations_upsert`.
3. History write payload metrics are available in both `uk_aq_public.uk_aq_history_rpc_metrics_minute` and `uk_aq_public.uk_aq_observation_rpc_metrics_minute` (alias view with matching shape to main DB).

## 3. Connector setup actions after install

Connector rows are created/updated by station list scripts. Run the relevant scripts to ensure connector rows exist before polling.

1. `scripts/openaq/openaq_list_stations.py`
2. `scripts/blondon_communities/blondon_communities_list_stations.py`
3. `scripts/erg_laqn/erg_laqn_list_stations.py`
4. `scripts/sos/sos_list_stations.py`
5. `scripts/sensorcommunity/sensorcommunity_list_stations.py`

During runtime, connector rows are also updated by dispatcher/ingest workers (`last_polled_at`, `last_run_start`, `last_run_end`, statuses).

## 4. Sensor.Community first-run order (fresh DB)

Run these in order before expecting Sensor.Community to appear on the hex map.

1. Confirm script schema profile is core (not public view):
   - `UK_AQ_CORE_SCHEMA=uk_aq_core`
2. Upsert Sensor.Community stations + connector row:
   - `python3 scripts/sensorcommunity/sensorcommunity_list_stations.py --to-supabase`
3. Assign PCON/LA codes to stations (required for map inclusion):
   - `python3 scripts/uk_aq_refresh_station_geo_r2.py`
4. Run Sensor.Community ingest to populate timeseries/observations:
   - `python3 scripts/sensorcommunity/sensorcommunity_ingest.py --refresh-recent`
5. Backfill Sensor.Community timeseries phenomena:
   - `python3 scripts/sensorcommunity/sensorcommunity_backfill_timeseries_phenomena.py`
6. Run station geo refresh again to catch any newly inserted stations:
   - `python3 scripts/uk_aq_refresh_station_geo_r2.py`

Notes:

1. `uk_aq_latest` excludes rows with no `pcon_code` and no `la_code`, so Sensor.Community can be missing from map filters until station geo refresh is done.
2. If you see `403 permission denied for view stations` when running geo refresh, `UK_AQ_CORE_SCHEMA` is set incorrectly (usually `uk_aq_public` instead of `uk_aq_core`).

## 5. UK-AQ webpage HTML update after DB switch

When moving to a new Supabase project ref/key, update the UK-AQ static HTML placeholders before deploy.

1. In `../CIC UK-AQ Webpage/CIC-test-uk-aq/.env`, set:
   - `SUPABASE_PROJECT_REF=<new-main-project-ref>`
   - `SB_PUBLISHABLE_DEFAULT_KEY=<new-main-publishable-key>`
2. Run injection script:
   - `cd "../CIC UK-AQ Webpage/CIC-test-uk-aq"`
   - `node scripts/uk_aq_inject_project_ref.mjs`
3. Deploy webpage (GH Pages / Cloudflare) after injection.

Notes:

1. GH Pages workflow (`.github/workflows/pages.yml`) runs injection in CI using repo secrets (`SUPABASE_PROJECT_REF`, `SB_PUBLISHABLE_DEFAULT_KEY`), then deploys the built artifact.
2. GH Pages deploy does not write injected values back to git-tracked files in the repo; local files remain unchanged unless you run the script locally.

## 6. GCP service accounts and IAM roles

Use dedicated service accounts (not default compute SAs) for deploy and runtime.

### 6.1 Core service accounts

1. GitHub deploy SA (`GCP_SERVICE_ACCOUNT`)
   - Purpose: GitHub Actions auth principal that deploys Cloud Run jobs and supporting resources.
   - Project roles needed by current workflows:
     - `roles/run.admin`
     - `roles/pubsub.admin`
     - `roles/cloudscheduler.admin`
     - `roles/cloudtasks.admin` (required for OpenAQ Cloud Tasks setup)
     - `roles/artifactregistry.writer`
     - `roles/secretmanager.admin` (or equivalent create/update + set policy permissions used by workflows)
   - Extra IAM needed:
     - `roles/iam.serviceAccountUser` on each runtime/invoker SA used in `--service-account`.
     - Permission to set IAM policy on OpenAQ task-invoker SA (for `roles/iam.serviceAccountTokenCreator` binding to Cloud Tasks service agent), e.g. `roles/iam.serviceAccountAdmin` on that SA.

2. OpenAQ runtime SA (`GCP_OPENAQ_JOB_SERVICE_ACCOUNT`, e.g. `uk-aq-openaq-job@...`)
   - Required roles/bindings:
     - Secret Manager access to runtime secrets (`roles/secretmanager.secretAccessor`).
     - `roles/cloudtasks.enqueuer` on OpenAQ task queue.
     - `roles/pubsub.publisher` on history topic (`GCP_OBSERVS_PUBSUB_TOPIC`) when `OPENAQ_OBSERVS_WRITE_MODE=pubsub_only`.

3. Sensor.Community runtime SA (`GCP_SCOMM_JOB_SERVICE_ACCOUNT`, e.g. `uk-aq-scomm-job@...`)
   - Required roles/bindings:
     - Secret Manager access to runtime secrets (`roles/secretmanager.secretAccessor`).
     - `roles/pubsub.publisher` on history topic (`GCP_OBSERVS_PUBSUB_TOPIC`) when `SCOMM_OBSERVS_WRITE_MODE=pubsub_only`.

4. UK-AIR SOS runtime SA (`GCP_SOS_JOB_SERVICE_ACCOUNT`, e.g. `uk-aq-sos-job@...`)
   - Required roles/bindings:
     - Secret Manager access to runtime secrets (`roles/secretmanager.secretAccessor`).

5. Breathe London Communities runtime SA (`GCP_BLONDON_COMMUNITIES_JOB_SERVICE_ACCOUNT`, currently `uk-aq-breathelondon-job@...`; the variable name identifies the Communities connector)
   - Required roles/bindings:
     - Secret Manager access to runtime secrets (`roles/secretmanager.secretAccessor`).

6. History outbox runtime SA (`GCP_OBSERVS_OUTBOX_JOB_SERVICE_ACCOUNT`, e.g. `uk-aq-observs-outbox-flusher@...`)
   - Required roles/bindings:
     - Secret Manager access to runtime secrets (`roles/secretmanager.secretAccessor`).

7. History Pub/Sub writer runtime SA (`GCP_OBSERVS_PUBSUB_SERVICE_ACCOUNT` or legacy `GCP_OBSERVS_PUBSUB_JOB_SERVICE_ACCOUNT`, e.g. `uk-aq-observs-pubsub@...`)
   - Required roles/bindings:
     - Secret Manager access to runtime secrets (`roles/secretmanager.secretAccessor`).
     - `roles/pubsub.subscriber` on history subscription (`uk-aq-observs-observations-sub` by default).

8. Scheduler invoker SA(s)
   - Examples:
     - Shared: `uk-aq-scheduler-invoker@...`
     - OpenAQ-specific: `GCP_OPENAQ_SCHEDULER_SERVICE_ACCOUNT`
     - History Pub/Sub specific: `GCP_OBSERVS_PUBSUB_SCHEDULER_SERVICE_ACCOUNT`
  - Required roles/bindings:
     - `roles/run.invoker` on each target Cloud Run service/job.

9. OpenAQ task invoker SA (`GCP_OPENAQ_TASK_INVOKER_SERVICE_ACCOUNT`, defaults to scheduler SA or OpenAQ job SA)
  - Required roles/bindings:
     - `roles/run.invoker` on OpenAQ Cloud Run service.
     - The Google Cloud Tasks service agent must have `roles/iam.serviceAccountTokenCreator` on this SA.

10. Google-managed Cloud Tasks service agent (`service-<PROJECT_NUMBER>@gcp-sa-cloudtasks.iam.gserviceaccount.com`)
   - Required binding:
     - `roles/iam.serviceAccountTokenCreator` on the OpenAQ task invoker SA.

### 6.2 Pub/Sub-specific notes for history flow

1. Topic: `GCP_OBSERVS_PUBSUB_TOPIC` (default `uk-aq-observs-observations`)
   - Publisher SAs: OpenAQ runtime SA and Sensor.Community runtime SA (and any other connector migrated to `pubsub_only`).
2. Subscription: `uk-aq-observs-observations-sub` (default in history Pub/Sub deploy workflow)
   - Subscriber SA: history Pub/Sub writer runtime SA.
3. Recommended subscription settings for this project:
   - Ack deadline: `600` seconds.
   - Message retention: `604800s` (7 days).
