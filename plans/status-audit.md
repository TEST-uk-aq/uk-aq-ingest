# Status Audit (Cloud Run ingest path)

Date: 2026-02-28

Scope used for this audit
- Active Cloud Run workers under `workers/`.
- Runtime ingest scripts launched by those workers (`/app/runtime/ingest_*`), which are copied from `supabase/functions/ingest_*`:
  - `workers/uk_aq_sos_cloud_run/Dockerfile:9`
  - `workers/uk_aq_openaq_cloud_run/Dockerfile:10`
  - `workers/uk_aq_breathelondon_cloud_run/Dockerfile:10`

Cross-cutting finding on `status_id`
- All current ingest/history writers send `status` (string), not `status_id`:
  - `supabase/functions/_shared/history_client.ts:17`
  - `supabase/functions/_shared/history_client.ts:278`
  - `supabase/functions/_shared/history_client.ts:454`
  - `workers/uk_aq_sensorcommunity_cloud_run/index.mjs:345`
  - `workers/uk_aq_sensorcommunity_cloud_run/index.mjs:1322`
- History RPC expects `status_id` (smallint) in input rows, not `status`:
  - `/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_dualwrite_bootstrap.sql:159`
  - `/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_dualwrite_bootstrap.sql:165`
- So `uk_aq_history.observations.status_id` is effectively unpopulated by current cloud-run ingesters, unless some upstream caller already supplies `status_id` (not found in active ingest code).

## 1) Gov.UK SOS AURN (UK-AIR SOS)

API base URL(s) used
- Default base: `https://uk-air.defra.gov.uk/sos-ukair/api/v1` (`supabase/functions/ingest_sos/index.ts:48`)
- Effective base can be connector override (`connector.service_url`) (`supabase/functions/ingest_sos/index.ts:388`)
- Observations endpoint path: `/timeseries/{timeseries_ref_or_id}/getData` with `format=tvp` (`supabase/functions/ingest_sos/index.ts:497`)

Raw payload status/quality fields
- Explicit status field paths parsed:
  - `values[*][2]` when row is array (`supabase/functions/ingest_sos/index.ts:1683`)
  - `values[*].status` (`supabase/functions/ingest_sos/index.ts:1700`)
  - `values[*].quality` (`supabase/functions/ingest_sos/index.ts:1701`)
  - `values[*].qc` (`supabase/functions/ingest_sos/index.ts:1702`)
- Fixture evidence of array status slot:
  - `tests/fixtures/timeseries_getdata.json:2` (`["2025-01-01T00:00:00Z", 1.0, "ok"]`)

Currently persisted?
- Ingest DB: yes, written to `uk_aq_core.observations.status`:
  - Build row with `status: point.status` (`supabase/functions/ingest_sos/index.ts:527`)
  - Upsert `observations` table (`supabase/functions/ingest_sos/index.ts:531`)
  - Target column exists as `status text` (`/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/main_db/uk_aq_core_schema.sql:640`)
- History DB: not meaningfully persisted to `status_id`.
  - History rows are built with `status` string (`supabase/functions/ingest_sos/index.ts:547`)
  - History RPC input uses `status_id` only (`/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_dualwrite_bootstrap.sql:165`)
  - Result: `uk_aq_history.observations.status_id` receives null from current UK-AIR writer path.
- Conditional ingest raw outbox: if outbox path is used, status string is retained inside JSON payload (`supabase/functions/_shared/history_client.ts:530`) and inserted into `uk_aq_raw.history_observation_outbox.payload` (`/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/main_db/main_db_dualwrite_bootstrap.sql:112`).

`status_id` mapping logic?
- No active mapping from UK-AIR status text to `status_id` found.

## 2) Sensor.Community

API base URL(s) used
- Base: `https://data.sensor.community` (`workers/uk_aq_sensorcommunity_cloud_run/index.mjs:36`)
- Endpoint path: `/airrohr/v1/filter/country=GB` (`workers/uk_aq_sensorcommunity_cloud_run/index.mjs:1731`)

Raw payload status/quality fields
- None found for observation quality/status in fetch payload.
- Worker parses `sensordatavalues[*].value_type` + `sensordatavalues[*].value` only (`workers/uk_aq_sensorcommunity_cloud_run/index.mjs:730`, `workers/uk_aq_sensorcommunity_cloud_run/index.mjs:735`).
- Live sample check (2026-02-28) shows top-level keys `id, location, sampling_rate, sensor, sensordatavalues, timestamp` and sensor value keys `id, value, value_type` (no status/qc/flag keys).

Currently persisted?
- Ingest DB: observation `status` is always null:
  - Raw row sets `status: null` (`workers/uk_aq_sensorcommunity_cloud_run/index.mjs:1825`)
  - Upsert `observations` table (`workers/uk_aq_sensorcommunity_cloud_run/index.mjs:1100`)
- History DB: `status_id` remains null (same global mismatch).
  - History row carries `status` string/null (`workers/uk_aq_sensorcommunity_cloud_run/index.mjs:1195`)
  - History upsert RPC call (`workers/uk_aq_sensorcommunity_cloud_run/index.mjs:1322`) expects `status_id` in SQL (`/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_dualwrite_bootstrap.sql:165`).

`status_id` mapping logic?
- No active mapping.

## 3) OpenAQ

API base URL(s) used
- Base: `https://api.openaq.org/v3` (`supabase/functions/ingest_openaq/index.ts:149`)
- Paths used:
  - `/locations` (`supabase/functions/ingest_openaq/index.ts:1421`)
  - `/locations/{locationId}/latest` (`supabase/functions/ingest_openaq/index.ts:1452`)
  - `/sensors/{sensorId}/measurements/hourly` (`supabase/functions/ingest_openaq/index.ts:1486`)

Raw payload status/quality fields
- Equivalent flag field exists in sample hourly payload: `results[*].flagInfo.hasFlags`:
  - `tests/cleanairsurb/cleanairsurb_openaq_api_1753.json:12`
- In current OpenAQ ingest code, that flag is not consumed for observation status.
  - Observation value comes from `summary.avg|summary.median|summary.q50|value` (`supabase/functions/ingest_openaq/index.ts:3223`)
  - Observation status is forced to null (`supabase/functions/ingest_openaq/index.ts:3391`)

Currently persisted?
- Ingest DB:
  - `uk_aq_core.observations.status` gets null from OpenAQ writes (`supabase/functions/ingest_openaq/index.ts:3391`, `supabase/functions/ingest_openaq/index.ts:2115`)
  - A non-quality flag is persisted: `location.isMobile -> stations.station_type` (`supabase/functions/ingest_openaq/index.ts:1835`, `supabase/functions/ingest_openaq/index.ts:1879`)
- History DB:
  - History rows carry null status (`supabase/functions/ingest_openaq/index.ts:3406`)
  - `uk_aq_history.observations.status_id` not mapped/populated (SQL expects `status_id`, `/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_dualwrite_bootstrap.sql:165`).

`status_id` mapping logic?
- No active mapping from OpenAQ flags to `status_id`.

## 4) Breathe London

API base URL(s) used
- Base: `https://api.breathelondon-communities.org/api` (`supabase/functions/ingest_breathelondon/index.ts:91`)
- Paths used:
  - `/ListSensors` (`supabase/functions/ingest_breathelondon/index.ts:597`)
  - `/getClarityData/{siteCode}/{species}/{start}/{end}/Hourly` (`supabase/functions/ingest_breathelondon/index.ts:615`)

Raw payload status/quality fields
- Status/health-like fields exist on station payload (`ListSensors`), e.g.:
  - `BatteryStatus` (`supabase/functions/ingest_breathelondon/index.ts:378`; sample `breathelondon_stations.json:16`)
  - `SensorsHealthStatus` (`supabase/functions/ingest_breathelondon/index.ts:381`; sample `breathelondon_stations.json:19`)
  - `OverallStatus` (`supabase/functions/ingest_breathelondon/index.ts:382`; sample `breathelondon_stations.json:20`)
- Observation extractor for `getClarityData` uses only `DateTime` and `ScaledValue` (no observation status field):
  - `supabase/functions/ingest_breathelondon/index.ts:1110`
  - `supabase/functions/ingest_breathelondon/index.ts:1111`

Currently persisted?
- Ingest DB:
  - Station health/status fields are persisted to `uk_aq_core.station_metadata.attributes`:
    - field mapping (`supabase/functions/ingest_breathelondon/index.ts:368`)
    - metadata upsert (`supabase/functions/ingest_breathelondon/index.ts:836`)
    - call site (`supabase/functions/ingest_breathelondon/index.ts:1971`)
  - `uk_aq_core.observations.status` is null (rows do not include status):
    - extracted rows include `connector_id, timeseries_id, observed_at, value` only (`supabase/functions/ingest_breathelondon/index.ts:1115`)
    - observations upsert (`supabase/functions/ingest_breathelondon/index.ts:996`)
- History DB:
  - History rows copy `row.status` if present, but it is null in current path (`supabase/functions/ingest_breathelondon/index.ts:1062`)
  - `status_id` not mapped/populated (SQL expects `status_id`, `/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Air Quality Networks/CIC-Test-UK-AQ-Schema/CIC-test-uk-aq-schema/schemas/obs_aqi_db/uk_aq_obs_aqi_db_dualwrite_bootstrap.sql:165`).

`status_id` mapping logic?
- No active mapping.

## Recommendation: keep or remove status from history_db

Recommendation: remove `status_id` from history_db unless you are about to implement a real mapping pipeline.

Why
- Availability is inconsistent for observation-level quality:
  - UK-AIR SOS: yes (explicit status/quality/qc in payload)
  - OpenAQ: has `flagInfo.hasFlags` in sample hourly payload, but current ingest does not consume/map it
  - Breathe London: has station/device health fields, not observation QC status
  - Sensor.Community: no explicit observation QC/status field found
- Current code path does not map any network status/flags to `status_id`.
- Current history RPC contract uses `status_id`, while ingesters send `status` text; this means `status_id` is not being populated now.

Safety note on removal
- From current worker code, removing status tracking from history is low-risk functionally because no active status_id mapping is used.
- But schema/RPC changes must be coordinated: if you drop/alter `status_id`, update `uk_aq_public.uk_aq_rpc_observs_observations_upsert` and dependent writer code in the same change set.
