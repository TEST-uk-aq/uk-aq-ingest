# sos_timeseries_checkpoints

Tracks the last time each UK-AIR SOS timeseries was polled so batches can rotate
even when a station stops sending data.

## Columns
- `timeseries_id` (integer, PK): Internal timeseries id.
- `last_polled_at` (timestamptz): When the timeseries was last attempted.
- `updated_at` (timestamptz): Updated alongside `last_polled_at`.

## Usage
- `uk_aq_dispatch_polls` calls `sos_select_timeseries_ids` to select the
  oldest checkpoints (nulls first) for each batch among active rows
  (`uk_aq_core.timeseries.ended_at is null`).
- `ingest_sos` upserts rows after each batch run.
- Lifecycle end-dating/reactivation is stored on `uk_aq_core.timeseries`
  (`last_catalog_seen_at`, `catalog_missing_runs`, `ended_at`), not in this checkpoint table.
