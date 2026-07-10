# UK AQ Stations Daily Workflow

## Purpose

`uk_aq_stations_daily.yml` is the daily stations maintenance workflow in the ingest repo. It refreshes station and timeseries metadata in IngestDB, enriches and exports station data, then mirrors the required `uk_aq_core` reference tables into ObsAQIDB.

The ObsAQIDB mirror step is important because AQI hourly and public ObsAQIDB queries rely on local copies of core reference rows such as stations, timeseries, phenomena, and observed properties.

## Existing documentation found

I found stations-daily information in these existing system docs:

- `system_docs/uk_aq_github_actions.md`
  - Has a `uk_aq_stations_daily.yml` section.
  - Describes the 03:00 UTC schedule, UK-AIR SOS and Breathe London station sync, OpenAQ polling guard, PCON/LA refresh, Dropbox export, and the final ObsAQIDB core mirror step.
  - Some details need updating because the current mirror design is now explicitly ID-preserving and the schema verifier ignores physical column order.

- `system_docs/uk_aq_scripts.md`
  - Has a section for `scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py`.
  - Explains that the script mirrors `uk_aq_core` reference tables from IngestDB to ObsAQIDB.
  - Documents the PostgREST source reads, destination `uk_aq_public` mirror RPCs, transient retry behaviour, timeseries alignment checks, observed-properties alignment checks, and the opt-in repair mode.
  - Some wording should be treated as superseded where it says schema verification checks column order. The current fix compares by table and column name and ignores `ordinal_position`.

- `system_docs/uk_aq_cron_schedules.csv`
  - Lists `uk_aq_stations_daily.yml` as daily at 03:00 UTC.
  - The schedule source is the Cloudflare workflow scheduler in the ops repo, not a live GitHub cron in the workflow file.

## Workflow trigger and schedule

The workflow file has `workflow_dispatch` enabled. The old GitHub cron is commented out. The intended external schedule is:

```text
03:00 UTC daily via Cloudflare Worker
```

The cron schedule document also records:

```text
CIC-test-uk-aq-ingest, GitHub Actions, uk_aq_stations_daily.yml, CF workflow-scheduler, 0 3 * * *, Daily @ 03:00 UTC
```

So in production, the ops repo Cloudflare workflow scheduler triggers the ingest repo workflow at 03:00 UTC.

## Main workflow phases

The workflow runs one job:

```text
sync-stations
```

with a 90 minute timeout.

The main phases are:

1. Checkout repo.
2. Set up Node.js.
3. Report daily task health as `started` to ObsAQIDB.
4. Set up Python 3.11.
5. Install Python dependencies.
6. Validate `SUPABASE_DB_URL` for the Supabase pooler.
7. Pause OpenAQ polling if it is currently enabled.
8. Sync UK-AIR SOS stations into IngestDB.
9. Pause UK-AIR SOS polling if it is currently enabled.
10. Discover UK-AIR SOS timeseries.
11. Resume UK-AIR SOS polling if the workflow paused it.
12. Sync Breathe London stations.
13. Sync OpenAQ stations only if OpenAQ polling was enabled when the workflow started.
14. Resume OpenAQ polling if the workflow paused it.
15. Optionally discover Sensor.Community stations, disabled by default.
16. Backfill Sensor.Community timeseries phenomena.
17. Refresh station PCON/LA codes from R2 shards.
18. Check for null station names.
19. Enrich station names if required.
20. Export stations to Dropbox.
21. Sync `uk_aq_core` reference tables from IngestDB to ObsAQIDB.
22. Report daily task health as `final`.

## The ObsAQIDB core mirror step

The final mirror step is:

```yaml
- name: Sync uk_aq_core reference tables to Obs AQI DB
  env:
    SRC_SUPABASE_URL: ${{ vars.SUPABASE_URL }}
    SRC_SECRET_KEY: ${{ secrets.SB_SECRET_KEY }}
    DST_SUPABASE_URL: ${{ vars.OBS_AQIDB_SUPABASE_URL }}
    DST_SECRET_KEY: ${{ secrets.OBS_AQIDB_SECRET_KEY }}
    SYNC_TARGET_LABEL: obs_aqidb
    SYNC_CALLER_PREFIX: stations_daily_sync_obs_aqidb
    OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS: ${{ vars.OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS }}
  run: |
    python3 scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py
```

The workflow always runs the same Python script:

```bash
python3 scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py
```

Whether it runs in normal mode or repair mode depends only on the `OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS` environment variable.

## Normal mode versus repair mode

### Normal mode

Normal mode is the default.

It is used when this GitHub repo variable is unset, blank, or anything other than `1`:

```text
OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS
```

In normal mode, the script checks for observed-property ID drift and fails fast if it finds any shared `observed_properties.code` with different numeric IDs between IngestDB and ObsAQIDB.

This is intentional. The mirror is ID-preserving, so shared natural keys with different IDs represent database drift that must be repaired deliberately.

### Repair mode

Repair mode is opt-in and should normally be run once, manually, after applying the repair RPC to ObsAQIDB.

Enable it with:

```bash
OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS=1 \
python3 scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py
```

Or in GitHub Actions, temporarily set the repository variable:

```text
OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS=1
```

Then run the workflow from the branch that contains the repair code.

After repair succeeds, remove or blank the GitHub variable before the next normal scheduled run.

## Required environment variables for local runs

The Python mirror script does not read `SUPABASE_URL` and `OBS_AQIDB_SUPABASE_URL` directly.

It expects:

```bash
SRC_SUPABASE_URL="https://<ingest-project-ref>.supabase.co"
SRC_SECRET_KEY="<ingest service role key>"
DST_SUPABASE_URL="https://<obs-aqidb-project-ref>.supabase.co"
DST_SECRET_KEY="<obs-aqidb service role key>"
```

For the current projects:

```bash
SRC_SUPABASE_URL="https://zztjgmdiftqtdcrlfpvc.supabase.co"
DST_SUPABASE_URL="https://waytarxkjprweyifffvq.supabase.co"
```

The DB URL variables are only for direct SQL application with `psql`, for example:

```bash
psql "$OBS_AQIDB_SUPABASE_DB_URL" \
  -v ON_ERROR_STOP=1 \
  -f supabase/sql/20260617_observed_properties_id_drift_repair_rpc.sql
```

## Tables mirrored to ObsAQIDB

The script mirrors the core reference tables needed by ObsAQIDB. The core set includes:

```text
uk_aq_core.connectors
uk_aq_core.observed_properties
uk_aq_core.categories
uk_aq_core.phenomena
uk_aq_core.offerings
uk_aq_core.features
uk_aq_core.procedures
uk_aq_core.stations
uk_aq_core.timeseries
```

This mirror is intended to be an ID-preserving copy from IngestDB into ObsAQIDB. Numeric primary keys are preserved. The script does not treat ObsAQIDB as a natural-key remapped database.

That matters because dependent rows use numeric FKs, for example:

```text
uk_aq_core.phenomena.observed_property_id
uk_aq_core.timeseries.phenomenon_id
uk_aq_core.timeseries.station_id
```

## Schema verification

Before writing, the script verifies that the destination tables are compatible with the source schema.

Current expected behaviour:

- Compare columns by table name and column name.
- Ignore physical column order and `ordinal_position`.
- Still fail for:
  - missing columns
  - extra columns where not allowed
  - type mismatches
  - nullability mismatches
  - default mismatches
  - primary-key mismatches

This avoids false failures where IngestDB and ObsAQIDB have the same usable columns but PostgreSQL physical column order differs.

The script also uses explicit source column selection for sync reads, so active table-copy behaviour should not depend on `select=*` or physical column order.

## Timeseries alignment check

Before table writes, the script runs a timeseries alignment check keyed by:

```text
connector_id
service_ref
timeseries_ref
```

It prints per-connector source and destination counts and hashes:

```text
connector_code
source_row_count
destination_row_count
source_key_only_hash
destination_key_only_hash
source_key_plus_id_hash
destination_key_plus_id_hash
```

The check distinguishes between:

- key-only gaps, such as rows missing in ObsAQIDB before sync
- ID mismatches, where the same natural timeseries key has different numeric IDs

Missing destination rows can be normal sync drift. ID mismatches are a stronger sign of mirror drift.

A successful post-sync state should have the same source and destination row counts for stations and timeseries.

## Observed-properties alignment check

The script checks `uk_aq_core.observed_properties` by natural key:

```text
code
```

If the same `code` exists in both IngestDB and ObsAQIDB with different IDs, the script prints diagnostics such as:

```text
Observed properties pre-sync alignment summary: id_mismatch=3 missing_in_destination=0 extra_in_destination=0
OBSERVED_PROPERTY_ID_MISMATCH code=124c6h3ch33 source_id=42 destination_id=41
OBSERVED_PROPERTY_ID_MISMATCH code=c7h16 source_id=50 destination_id=49
OBSERVED_PROPERTY_ID_MISMATCH code=nox_as_no2 source_id=19 destination_id=18
```

In normal mode, this fails before writes.

In repair mode, it prints proposed repairs and calls the repair RPC.

## Observed-properties repair RPC

Before repair mode can work, apply this SQL to ObsAQIDB:

```bash
psql "$OBS_AQIDB_SUPABASE_DB_URL" \
  -v ON_ERROR_STOP=1 \
  -f supabase/sql/20260617_observed_properties_id_drift_repair_rpc.sql
```

This creates or replaces:

```text
uk_aq_public.uk_aq_rpc_repair_observed_property_id_drift(p_repairs jsonb)
```

The RPC:

- validates each requested repair
- refuses malformed payloads
- refuses `source_id == destination_id`
- confirms the destination stale ID has the expected `code`
- refuses if the source ID is occupied by a different code
- discovers every single-column FK in the destination database that references `uk_aq_core.observed_properties(id)`
- rewires dependent rows from the stale destination ID to the source ID
- refuses composite FKs involving `observed_properties(id)` rather than handling them silently
- deletes the stale duplicate row
- verifies no dependent FK still references the stale ID
- verifies the final source-ID row exists
- advances the identity sequence

The RPC result should show the rewrites, for example:

```json
[
  {
    "code": "nox_as_no2",
    "source_id": 19,
    "destination_id": 18,
    "dependent_rewrites": {
      "uk_aq_core.phenomena.observed_property_id": 1
    },
    "stale_rows_deleted": 1
  }
]
```

## One-off repair workflow

Use this when normal sync fails with `OBSERVED_PROPERTY_ID_MISMATCH`.

1. Switch to the branch containing the repair code.

```bash
git fetch origin
git switch <branch-name>
```

2. Apply the repair RPC to ObsAQIDB.

```bash
psql "$OBS_AQIDB_SUPABASE_DB_URL" \
  -v ON_ERROR_STOP=1 \
  -f supabase/sql/20260617_observed_properties_id_drift_repair_rpc.sql
```

3. Run repair mode locally.

```bash
OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS=1 \
python3 scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py
```

4. Confirm repair success.

Expected lines:

```text
Observed properties repair RPC result: [...]
Observed properties pre-sync alignment summary: id_mismatch=0 missing_in_destination=0 extra_in_destination=0
uk_aq_core sync to destination completed successfully.
```

5. Run normal sync without the flag.

```bash
unset OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS
python3 scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py
```

Expected clean normal run:

```text
Observed properties pre-sync alignment summary: id_mismatch=0
Timeseries pre-sync alignment summary: id_mismatch=0 missing_in_destination=0 extra_in_destination=0
uk_aq_core sync to destination completed successfully.
```

6. Merge the ingest PR only after one successful repair run and one successful normal sync.

7. Rerun AQI hourly after core sync is healthy.

## Running a workflow branch before merging

You can run code from a branch before merging.

Local branch run:

```bash
git fetch origin
git switch <branch-name>
python3 scripts/stations_daily/sync_obs_aqidb_uk_aq_core.py
```

PR branch run:

```bash
git fetch origin pull/<PR_NUMBER>/head:pr-<PR_NUMBER>
git switch pr-<PR_NUMBER>
```

GitHub Actions branch run:

1. Open GitHub Actions.
2. Select `UK-AQ Stations Daily Sync`.
3. Choose `Run workflow`.
4. Select the branch from the branch dropdown.
5. Run it.

This is the correct way to test workflow changes before merging them into `main`.

## Common failure modes

### `ERROR: Missing required environment variable: SRC_SUPABASE_URL`

The local command is missing the script-specific REST variables.

Set:

```bash
export SRC_SUPABASE_URL="$SUPABASE_URL"
export SRC_SECRET_KEY="$SUPABASE_SERVICE_ROLE_KEY"
export DST_SUPABASE_URL="$OBS_AQIDB_SUPABASE_URL"
export DST_SECRET_KEY="$OBS_AQIDB_SUPABASE_SERVICE_ROLE_KEY"
```

or set the four variables directly.

### `Schema verification failed: timeseries column definition mismatch`

If the diff only shows different `ordinal_position` values, you are probably running older branch code. The fixed verifier should ignore physical column order.

Switch to the current repair branch and rerun.

### `column reference "code" is ambiguous`

The repair RPC installed in ObsAQIDB is old or has an unqualified PL/pgSQL variable/column reference.

Reapply the current SQL file to ObsAQIDB:

```bash
psql "$OBS_AQIDB_SUPABASE_DB_URL" \
  -v ON_ERROR_STOP=1 \
  -f supabase/sql/20260617_observed_properties_id_drift_repair_rpc.sql
```

Then rerun repair mode.

## Operational guidance

For normal scheduled operations:

- Keep `OBS_AQIDB_REPAIR_OBSERVED_PROPERTY_IDS` unset or blank.
- Let the workflow run the normal sync.
- Treat repair mode as a deliberate one-off maintenance action.
- After repair mode succeeds, immediately run a normal sync.
- Do not leave the GitHub variable set to `1` permanently.
- If AQI hourly fails after a successful core sync, investigate the AQI hourly worker/RPC separately.
