# sos_station_checkpoints

Cloud Run station-level scheduling checkpoints for UK-AIR SOS polling.

## Columns
- `station_id` (bigint, PK): Internal station id (`uk_aq_core.stations.id`).
- `next_due_at` (timestamptz): Next planned poll time for this station.
- `last_observed_at` (timestamptz): Latest observed timestamp seen for station timeseries.
- `ingest_lag_samples` (integer[]): Recent lag samples (seconds) used to shape next due time.
- `last_polled_at` (timestamptz): Last time the station was attempted by Cloud Run SOS polling.
- `updated_at` (timestamptz): Last checkpoint update timestamp.

## Usage
- `uk_aq_core.sos_select_station_refs(batch_limit, stale_limit)` selects due station refs using this table plus current `timeseries.last_value_at` rollups.
- `workers/uk_aq_sos_cloud_run/run_job.ts` updates rows after each Cloud Run SOS run.
- Edge SOS path remains unchanged and continues to use `sos_timeseries_checkpoints`.
