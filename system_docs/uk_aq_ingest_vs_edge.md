# Ingest Script vs Edge Function

This project uses **two different ingestion paths** that serve different purposes.

Dispatcher/queue runtime flow is documented in `system_docs/uk_aq_cloudflare_scheduler_ingest_flow.md`.

## Ingest script (`scripts/sos/sos_ingest.py`)

**Purpose:** full discovery + data refresh.

- Discovers SOS services, stations, timeseries, phenomena, procedures, offerings.
- Creates/updates `timeseries` rows (including `timeseries_ref`, `station_id`, `phenomenon_id`).
- Can backfill historical data and refresh recent data.
- Writes:
  - `connectors`, `stations`, `timeseries`, `observations`, `phenomena`, `procedures`, `offerings`, `categories`, `features`.
- Can upload raw payloads + logs to Dropbox (optional).
- Runs locally or via GitHub Actions.

## Edge function (`supabase/functions/ingest_sos/index.ts`)

**Purpose:** lightweight polling of existing timeseries rows.

- **Does not discover** stations/timeseries or fix missing links.
- Loads existing `timeseries` rows and polls `timeseries_ref` for recent values.
- Writes:
  - `observations`
  - `timeseries.last_value` + `timeseries.last_value_at` (update by id)
- Logs to Dropbox and `error_logs` when configured.
- Triggered by the scheduler using helper RPCs in `supabase/uk_aq_polling_helpers.sql`.

## Why both exist

- The ingest script keeps the metadata **complete and correct**.
- The edge function keeps **recent values up to date** without running discovery.

## Practical implications

- If `timeseries.station_id` is null, **run the ingest script** (`--discover`) to fix it.
- If the web page shows `—` values, check that the edge function is running and that
  `timeseries.last_value` is being updated.
