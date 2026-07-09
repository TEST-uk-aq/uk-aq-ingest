# UK-AIR SOS Rename Inventory

Discovery-only report for the planned `sos*` -> `sos*` rename.

Scope searched:
- `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest`
- `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-schema`
- `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ops`

Archive folders were excluded from the active scan.

## 1. Summary

Summary answer: **No official direct mapping found; current evidence points to matching by name/coordinates**

Key counts from the active repos scanned:
- Total files with `sos`-family references: **150** (`78` ingest, `31` schema, `41` ops)
- Direct rename targets listed below: **30+** individual names, plus family rows for env/config and suffix patterns
- Total likely table names: **7**
- Total likely function/RPC names: **1**
- Total likely script/file/path names: **11**
- Total likely env var/config families: **2** (`SOS_*`, `sos_*`)

Important distinction:
- Mapping A, `UK-AIR ID -> site_ref/site_id`, is supported by official UK-AIR site-info / flat-file discovery.
- Mapping B, `SOS station id/ref -> UK-AIR ID`, was **not** found as an official direct field or endpoint in local files. The local implementation uses a controlled matching step with name + coordinates fallback.

## 2. Rename Inventory

This is the actionable checklist. Rows marked `rename with migration` are code or schema surfaces. Rows marked `docs only` are documentation or generated artifacts. Rows marked `review` are public/runtime config names that may need compatibility aliases.

| Current name | Suggested new name | Category | Confidence | Notes |
|---|---|---|---|---|
| `sos_networks` | `sos_networks` | table | high | Core network lookup table. |
| `sos_network_pollutants` | `sos_network_pollutants` | table | high | Core network-to-pollutant table. |
| `sos_site_register` | `sos_site_register` | table | high | Monthly UK-AIR register table. |
| `sos_station_uk_air_refs` | `sos_station_uk_air_refs` | table | high | Bridge from internal station id to UK-AIR ID. |
| `sos_station_timeseries_site_refs` | `sos_station_timeseries_site_refs` | table | high | Derived site/timeseries mapping table. |
| `sos_station_checkpoints` | `sos_station_checkpoints` | table | high | Daily station checkpoint table. |
| `sos_timeseries_checkpoints` | `sos_timeseries_checkpoints` | table | high | Daily timeseries checkpoint table. |
| `sos_site_timeseries_candidates` | `sos_site_timeseries_candidates` | temp table / helper | medium | Temporary helper name inside the refresh RPC. |
| `sos_station_bindings` | `sos_station_bindings` | table / helper | medium | Appears in reporting and ingest surfaces. |
| `sos_source_change_targets` | `sos_source_change_targets` | table / helper | medium | Appears in reporting and ingest surfaces. |
| `sos_snapshots_no_data` | `sos_snapshots_no_data` | table / helper | medium | Snapshot/reporting surface. |
| `sos_snapshots_successful` | `sos_snapshots_successful` | table / helper | medium | Snapshot/reporting surface. |
| `sos_stations` | `sos_stations` | table / view | medium | Canonical SOS station inventory surface. |
| `sos_days` | `sos_days` | table / helper | medium | Daily ingest/helper surface. |
| `sos_station_day` | `sos_station_day` | table / helper | medium | Daily ingest/helper surface. |
| `sos_select_station_refs` | `sos_select_station_refs` | rpc/function | high | Selection RPC used by worker/runtime code. |
| `sos_select_timeseries_ids` | `sos_select_timeseries_ids` | rpc/function | high | Selection RPC used by worker/runtime code. |
| `sos_lookup_active_stations` | `sos_lookup_active_stations` | rpc/function | medium | Helper/lookup function name. |
| `sos_lookup_active_timeseries` | `sos_lookup_active_timeseries` | rpc/function | medium | Helper/lookup function name. |
| `sos_extract_datapoints` | `sos_extract_datapoints` | rpc/function | medium | Helper/parse function name. |
| `sos_fetch_timeseries_payload` | `sos_fetch_timeseries_payload` | rpc/function | medium | Helper/parse function name. |
| `sos_parse_timestamp` | `sos_parse_timestamp` | rpc/function | medium | Helper/parse function name. |
| `sos_to_finite_number` | `sos_to_finite_number` | rpc/function | medium | Helper/parse function name. |
| `uk_aq_rpc_sos_station_timeseries_site_refs_refresh` | `uk_aq_rpc_sos_station_timeseries_site_refs_refresh` | rpc/function | high | Public RPC called by the monthly register load. |
| `scripts/sos/` | `scripts/sos/` | directory path | high | Script directory rename plus import/path updates. |
| `network_info/sos/` | `network_info/sos/` | directory path | high | Generated site-ref and support data paths. |
| `workers/uk_aq_sos_cloud_run/` | `workers/uk_aq_sos_cloud_run/` | directory path | high | Cloud Run worker directory. |
| `supabase/functions/ingest_sos/` | `supabase/functions/ingest_sos/` | directory path | high | Supabase edge function directory. |
| `.github/workflows/sos_site_register_monthly.yml` | `.github/workflows/sos_site_register_monthly.yml` | workflow file | high | Monthly register workflow. |
| `.github/workflows/uk_aq_sos_cloud_run_deploy.yml` | `.github/workflows/uk_aq_sos_cloud_run_deploy.yml` | workflow file | high | Cloud Run deploy workflow. |
| `.github/workflows/uk_aq_stations_daily.yml` | unchanged filename; update embedded references | workflow file | high | Contains `sos` connector code, env vars, and script paths. |
| `.github/workflows/uk_aq_raw_dropbox.yml` | unchanged filename; update embedded references | workflow file | medium | Contains `sos` script path and dropbox settings. |
| `.github/workflows/supabase_edge_deploy.yml` | unchanged filename; update embedded references | workflow file | medium | Contains `ingest_sos` deploy target. |
| `scripts/sos/sos_site_register.py` | `scripts/sos/sos_site_register.py` | script filename | high | Monthly register loader. |
| `scripts/sos/sos_list_stations.py` | `scripts/sos/sos_list_stations.py` | script filename | high | Daily station discovery utility. |
| `scripts/sos/sos_ingest.py` | `scripts/sos/sos_ingest.py` | script filename | high | Main ingest orchestrator. |
| `scripts/sos/sos_compare.py` | `scripts/sos/sos_compare.py` | script filename | high | Comparison utility. |
| `scripts/sos/sos_network_assignment_report.py` | `scripts/sos/sos_network_assignment_report.py` | script filename | high | Reporting utility. |
| `scripts/sos/sos_timeseries_metadata_sample.py` | `scripts/sos/sos_timeseries_metadata_sample.py` | script filename | high | Sampling utility. |
| `workers/uk_aq_sos_cloud_run/run_job.ts` | `workers/uk_aq_sos_cloud_run/run_job.ts` | script file | high | Worker runtime file. |
| `workers/uk_aq_sos_cloud_run/run_service.ts` | `workers/uk_aq_sos_cloud_run/run_service.ts` | script file | high | Worker runtime file. |
| `workers/uk_aq_sos_cloud_run/Dockerfile` | `workers/uk_aq_sos_cloud_run/Dockerfile` | config file | high | Worker image build file. |
| `workers/uk_aq_sos_cloud_run/README.md` | `workers/uk_aq_sos_cloud_run/README.md` | doc | medium | Worker README path should track the directory rename. |
| `supabase/functions/ingest_sos/index.ts` | `supabase/functions/ingest_sos/index.ts` | script file | high | Edge function entry point. |
| `tests/test_sos_site_register_mapping.py` | `tests/test_sos_site_register_mapping.py` | test file | medium | Rename with import updates. |
| `tests/aq_in_wales/sos_stations.json` | `tests/aq_in_wales/sos_stations.json` | fixture / data file | medium | Generated fixture name. |
| `dbml/sos_connector_membership_subset.dbml` | `dbml/sos_connector_membership_subset.dbml` | dbml / schema doc | medium | Documentation artifact. |
| `system_docs/sos_station_register_linking.md` | `system_docs/sos_station_register_linking.md` | doc | high | Runtime linking note. |
| `system_docs/sos_ingest_flow.md` | `system_docs/sos_ingest_flow.md` | doc | high | Ingest flow note. |
| `system_docs/table_info/sos_networks.md` | `system_docs/table_info/sos_networks.md` | doc | high | Table info doc. |
| `system_docs/table_info/sos_network_pollutants.md` | `system_docs/table_info/sos_network_pollutants.md` | doc | high | Table info doc. |
| `system_docs/table_info/sos_site_register.md` | `system_docs/table_info/sos_site_register.md` | doc | high | Table info doc. |
| `system_docs/table_info/sos_station_uk_air_refs.md` | `system_docs/table_info/sos_station_uk_air_refs.md` | doc | high | Table info doc. |
| `sos` | `sos` | connector code / external identifier | medium | Requires coordinated database, workflow, and dashboard migration. |
| `SOS` | `SOS` or review | public label | medium | Human-facing label; treat as a branding decision. |
| `SOS_*` | `SOS_*` | env var / config family | medium | Includes the runtime and workflow config family; keep aliases during cutover. |
| `sos_*` | `sos_*` | constant / helper family | medium | Includes helper names, metric labels, and internal config keys. |
| `sos_*_idx` / `sos_*_uidx` | `sos_*_idx` / `sos_*_uidx` | index family | high | Rename with the parent table/index migration. |
| `sos_*_select_service_role` / `sos_*_write_service_role` | `sos_*_select_service_role` / `sos_*_write_service_role` | policy family | high | Rename with table/policy migration. |

## 3. Evidence Found
### Official Mapping A: UK-AIR ID -> site_ref/site_id

| File | Snippet | What it proves | Supports |
|---|---|---|---|
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_site_register.py` | `_discover_site_ref_mapping()` calls `site-info?uka_id=<uk_air_ref>` and `_validate_site_ref_mapping()` checks `site-info?site_id=<site_ref>` and `data/flat_files?site_id=<site_ref>` | The code has an official route from `uk_air_ref` to `site_ref` and validates it against UK-AIR pages | Mapping A |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/system_docs/sos_station_register_linking.md` | `uk_air_ref = UKA00591` / `site_ref = EA8` and `Monthly register loading knows official UK-AIR register/site identifiers` | The docs explicitly treat `site_ref` discovery as official and separate from SOS discovery | Mapping A |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/system_docs/site_ref_2_station_id_tseries_id.md` | `sos_site_register -> uk_air_ref -> site_ref` and `site_ref = ... official UK-AIR site-info discovery, or the checked seed map CSV` | Confirms the intended register flow is official UK-AIR ID to `site_ref` | Mapping A |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-schema/schemas/migrations/20260708_001_ingest_sos_site_refs.sql` | Table comment: `Maps DEFRA UK-AIR AURN archive site_ref + pollutant_code to UK AQ station/timeseries rows` | The archive mapping table is driven by `site_ref`, not by a direct SOS-station-to-UK-AIR API field | Mapping A |

### No official direct Mapping B found

| File | Snippet | What it proves | Supports |
|---|---|---|---|
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/tests/aq_in_wales/sos_stations.json` | JSON objects contain `properties.id`, `properties.label`, `properties.timeseries`, `geometry.coordinates` | The sampled SOS payload exposes SOS station IDs and coordinates, but no `UKA...`, `uk_air_ref`, `site_ref`, or `site_id` fields | Mapping B absence |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/archive/2026-02-08/scripts/uk_aq_backfill_station_memberships.py` | `_find_register_match()` computes distance from station coordinates to register coordinates, then falls back to `name+distance` | The actual bridge builder is a controlled matcher, not a direct official field join | Mapping B absence |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/system_docs/sos_station_register_linking.md` | `Daily SOS station discovery does not reliably know the UK-AIR flat-file/Data Selector code` and `should not guess: uk_air_ref, site_ref` | The docs explicitly say the daily SOS layer cannot directly infer the register identifier | Mapping B absence |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_list_stations.py` | `fetch_sos_station_uk_air_refs()` reads `station_id, uk_air_ref, match_method, match_distance_m, source_snapshot_at` | The station bridge is consumed as a matched table, not discovered from a direct SOS field | Mapping B absence |

## 4. Existing Code Paths

### Ingest repo

| File | Source data used | Columns read / written | Matching style |
|---|---|---|---|
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_site_register.py` | UK-AIR monitoring sites CSV, optional seed CSV, official UK-AIR site-info / flat-files pages, Supabase `sos_networks` and `sos_site_register` | Reads `UK-AIR ID`, `Site Name`, `Latitude`, `Longitude`, `Networks`, etc.; writes `sos_networks`, `sos_network_pollutants`, `sos_site_register`; calls RPC refresh | Official `uk_air_ref -> site_ref` discovery; no direct SOS-station-to-UK-AIR join |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_list_stations.py` | Supabase `stations`, `timeseries`, `sos_station_uk_air_refs`, `sos_networks` | Reads `station_id, uk_air_ref, match_method, match_distance_m, source_snapshot_at` from `sos_station_uk_air_refs` | Consumes bridge table; does not create official mapping |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_ingest.py` | Orchestrates daily station/timeseries ingest | References `sos` connector code, cloud-run file paths, and runtime labels | Operational orchestrator; prefix rename candidate |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_compare.py` | UK-AIR vs SOS comparison inputs | Script/file names and URLs use `sos` | Utility / rename candidate |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_network_assignment_report.py` | Connector membership reporting | Uses `sos` connector code | Utility / rename candidate |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_timeseries_metadata_sample.py` | SOS timeseries sample generation | File names, output paths, and labels use `sos` | Utility / rename candidate |

### Schema repo

| File | Source data used | Columns read / written | Matching style |
|---|---|---|---|
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-schema/schemas/migrations/20260708_001_ingest_sos_site_refs.sql` | Existing `sos_site_register`, `sos_station_uk_air_refs`, `stations`, `timeseries` | Creates / indexes / comments on `sos_site_register`, `sos_station_uk_air_refs`, `sos_station_timeseries_site_refs` | Schema migration; no direct SOS-station-to-UK-AIR field |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-schema/schemas/migrations/20260708_002_ingest_sos_station_timeseries_site_refs_refresh.sql` | `sos_site_register`, `sos_station_uk_air_refs`, `stations`, `timeseries`, `observed_properties` | RPC `uk_aq_rpc_sos_station_timeseries_site_refs_refresh(p_source_snapshot_at)` | Refreshes mapping after `station_id -> uk_air_ref` exists |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-schema/dbml/sos_connector_membership_subset.dbml` | DBML inventory of connector membership subset | Defines `sos_station_uk_air_refs` with `station_id`, `uk_air_id`, `match_method`, `match_distance_m`, `source_snapshot_at` | Explicitly documents controlled matching, not an official direct field |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_schema.sql` | Canonical raw/core schema | Defines `sos_site_register`, `sos_station_uk_air_refs`, `sos_station_timeseries_site_refs` and related indexes/policies/comments | Rename target for tables, indexes, policies, comments |

### Ops repo

| File | Source data used | Columns read / written | Matching style |
|---|---|---|---|
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ops/local/dashboard/server/uk_aq_dashboard_api.py` | Dashboard source allowlist | Hard-coded `"sos"` entry in the scheduler-backend connector allowlist | Config/reference rename candidate |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ops/system_docs/uk_aq_scripts.md` | Repository script catalog | Mentions `sos` source keys in history-integrity docs | Docs only |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ops/env-vars-master.csv` | Central environment-variable registry | Includes `SOS_SITE_SEARCH_USER_AGENT` | Env var / config rename candidate, high blast radius |
| `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ops/tests/backfill_sos_integrity_snapshot.test.mjs` | Test name / snapshot fixture | File name uses `sos` | Test rename candidate |

## 5. Data Source Inventory

| Source / file / API | Has SOS station id? | Has UK-AIR ID? | Has site_ref / site_id? | Can it directly map SOS -> UK-AIR? |
|---|---:|---:|---:|---:|
| SOS `/stations` API payload | Yes | No | No | No |
| UK-AIR monitoring sites CSV | No | Yes | No | Not directly |
| UK-AIR `site-info` / `flat_files` pages | No | Yes | Yes | Yes for `uk_air_ref -> site_ref` |
| `sos_site_register` table | No | Yes | Yes | Yes for Mapping A only |
| `sos_station_uk_air_refs` table | Yes | Yes | No | Yes only after controlled matching |
| `sos_station_timeseries_site_refs` table | Yes via `station_id` | Yes via join | Yes | No, it depends on the bridge table |
| `tests/aq_in_wales/sos_stations.json` sample | Yes | No | No | No |
| `plans/*.csv` matching outputs | Yes | Sometimes | Sometimes | Evidence artifact only, not official |

## 6. Recommendation

Safest monthly implementation:

1. Keep the official Mapping A flow intact:
   - `uk_air_ref -> site_ref` via official UK-AIR site-info / flat-files pages.
2. Add or preserve a controlled bridge step for Mapping B:
   - populate `sos_station_uk_air_refs` from SOS station metadata plus register rows.
3. Use explicit match methods:
   - `official-direct` if a future official SOS payload ever exposes the UK-AIR ID directly
   - `name+distance` for the best current controlled match
   - `distance` for unambiguous coordinate matches
   - `manual-seed` for curated overrides
4. Persist match evidence:
   - `station_id`
   - `uk_air_ref`
   - `match_method`
   - `match_distance_m`
   - `source_snapshot_at`
5. Keep `sos_station_timeseries_site_refs` as a derived table refreshed after the station bridge exists.

Implementation note:
- Do **not** rely on an assumed official SOS field for `station_id -> uk_air_ref`. The local evidence does not show one.
- If a rename is executed later, introduce backward-compatible aliases or migration views first so the workflows can be cut over safely.

## 7. Suggested Next Queries or Files to Inspect Manually

If anything still needs confirmation before a rename, inspect:

- `https://uk-air.defra.gov.uk/sos-ukair/api/v1/stations`
- `https://uk-air.defra.gov.uk/sos-ukair/api/v1/timeseries`
- `https://uk-air.defra.gov.uk/data/site-info?uka_id=<UKA...>`
- `https://uk-air.defra.gov.uk/data/flat_files?site_id=<site_ref>`
- `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/scripts/sos/sos_site_register.py`
- `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest/archive/2026-02-08/scripts/uk_aq_backfill_station_memberships.py`
- `/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-schema/schemas/migrations/20260708_001_ingest_sos_site_refs.sql`

## 8. Recommended Phased Rename Plan

1. Schema table / object rename migration
   - rename tables, indexes, policies, comments, and RPCs in the schema repo
2. Code updates
   - update Python imports, script names, worker paths, and RPC calls
3. Workflow updates
   - rename workflow files and step paths
4. Env var / config updates
   - add compatibility aliases where required
5. CSV / `network_info` path updates
   - rename generated outputs only after consumers are updated
6. Docs updates
   - rewrite tables, runbooks, and system docs
7. Compatibility layer
   - keep aliases / views temporarily if the cutover is risky
8. Verification
   - repo grep + schema checks + workflow dry-run validation

## 9. Verification Commands

Use these after any future rename:

```bash
grep -RIn --exclude-dir=archive --exclude-dir=.git --exclude-dir=node_modules --exclude-dir=.venv --exclude-dir=venv --exclude-dir=__pycache__ 'sos\\|SOS\\|Sos\\|sos\\|SOS\\|sos' \
  '/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ingest' \
  '/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-schema' \
  '/Users/mikehinford/Dropbox/Projects/UK-AQ Website & Network/TEST UK-AQ GH Repos/TEST-uk-aq-ops'
```

```sql
select table_schema, table_name
from information_schema.tables
where table_schema in ('uk_aq_raw', 'uk_aq_core', 'uk_aq_public')
  and table_name ilike '%sos%';
```

```sql
select routine_schema, routine_name
from information_schema.routines
where routine_schema in ('uk_aq_raw', 'uk_aq_core', 'uk_aq_public')
  and routine_name ilike '%sos%';
```

```sql
select schemaname, tablename, policyname
from pg_policies
where schemaname in ('uk_aq_raw', 'uk_aq_core', 'uk_aq_public')
  and (tablename ilike '%sos%' or policyname ilike '%sos%');
```

## 10. Things Not to Rename

Do not rename official UK-AIR concepts just because they appear near the connector prefix:

- `uk_air_ref`
- `uk_air_id`
- `UK-AIR ID`
- `UKA00591`
- `site_ref`
- `site_id`

Only the `sos*` naming surface is in scope for the planned rename.
