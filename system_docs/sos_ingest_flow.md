# UK-AIR SOS Ingest Flow

This page summarises how SOS data lands in tables and how stations map to multiple networks.

It remains the high-level data-flow document during the gradual SOS documentation migration. Current polling, Cloud Run result and failure behaviour is authoritative under [`system_docs/sos/`](sos/README.md).

## Key Tables
- `connectors`: data sources. UK-AIR SOS is one connector.
- `stations`: station metadata ingested from SOS (one row per `station_ref`).
- `timeseries`: per-station, per-phenomenon SOS time series metadata (`timeseries_ref`).
- `phenomena`: pollutant definitions tied to a connector.
- `observations`: time/value pairs keyed by `connector_id` + `timeseries_id` + `observed_at`.
- Placeholder SOS station refs (for example `9999999999`) are skipped during ingest and flagged via `station_metadata.exclude_from_ui=true`.
- `sos_site_register`: UK-AIR site register snapshot (includes `uk_air_ref`, optional DEFRA flat-file `site_ref`, and source network labels).
- `sos_networks`: network lookup (source label -> internal `network_code` + UI display name).
- `sos_network_pollutants`: pollutant matching rules per network.
- `sos_station_uk_air_refs`: map SOS `station_id` to `uk_air_ref`.
- `sos_station_timeseries_site_refs`: map archive `site_ref` + pollutant to UK AQ station/timeseries rows for historical backfill.
- `networks`: canonical network catalog referenced by `stations.network_id`.

## Ingest Steps
1) **SOS metadata ingest (daily)**
   - Fetches SOS stations + timeseries metadata.
   - Upserts into `stations` and `timeseries`.
   - Each `timeseries` row links to `phenomena` and a `station_id`.
2) **UK-AIR register ingest (daily/periodic)**
   - Loads `sos_site_register`.
   - Stores structured AURN pollutant evidence in `sos_site_register.uk_air_pollutants` when the CSV includes `AURN Pollutants Measured`.
   - Discovers DEFRA flat-file `site_ref` values for AURN rows from official `networks/site-info?uka_id=<uk_air_ref>` pages.
   - Uses `network_info/sos/sos_site_refs.csv` as a seed/override map where needed.
   - The monthly workflow validates mapped and discovered `site_ref` values against `networks/site-info?site_id=<site_ref>` and `data/flat_files?site_id=<site_ref>` before loading them.
   - After loading the register snapshot, refreshes `sos_station_uk_air_refs` from SOS stations that carry the archive pollutants (`pm25`, `pm10`, `no2`) using controlled name-and-distance matching.
   - After loading the register snapshot, calls `uk_aq_rpc_sos_station_timeseries_site_refs_refresh` to map each validated `site_ref` and pollutant to the corresponding UK AQ station and timeseries.
   - Successive ended/current timeseries receive non-overlapping validity dates. Multiple active timeseries for one `site_ref` + pollutant, or an invalid derived interval, fails the workflow before mappings are written.
   - Unmapped AURN register sites are counted in the workflow log and are not guessed.
   - Upserts `sos_networks` and `sos_network_pollutants`.
3) **Station-to-register matching**
   - If SOS metadata includes a UK-AIR ID, link directly.
   - Otherwise match by station name + distance (coordinates).
   - The monthly register workflow writes `sos_station_uk_air_refs` with match method + distance and only keeps stations that actually have archive-pollutant timeseries.
4) **Network membership backfill**
   - Collects pollutant keys from station `timeseries` -> `phenomena`.
   - Filters allowed networks via `sos_network_pollutants`.
   - Validates the station's scalar `network_id` against `networks.id`.

## Polling Flow (Observations)

### Edge path

```text
Cloudflare scheduler
  -> uk_aq_dispatch_polls
  -> ingest_sos
  -> observations and timeseries last-value state
```

- `sos_timeseries_checkpoints` records `last_polled_at` so the dispatcher rotates timeseries batches.
- The dispatcher passes internal `timeseries.id` values into `ingest_sos`.

### Cloud Run path

```text
Google Cloud Scheduler or configured external trigger
  -> Cloud Run run_service.ts
  -> unique temporary child-result file
  -> run_job.ts
  -> select due station refs
  -> resolve scoped internal timeseries IDs
  -> local ingest_sos process
  -> connector and ingest-run persistence
  -> bounded child result
  -> run_service.ts returns the declared HTTP status
```

- `sos_station_checkpoints` records station due-state and lag samples.
- The child writes a result for every intentional successful exit, including skips.
- A successful child with an empty or invalid result fails closed as HTTP 500.
- Recognised upstream dependency failures preserve HTTP 502 or 503 without creating a duplicate wrapper error.
- Runtime-budget stops return a partial HTTP 207 result with one consolidated run-level error.

The detailed authority for these rules is:

- [`sos/contract.md`](sos/contract.md)
- [`sos/interfaces.md`](sos/interfaces.md)
- [`sos/operations.md`](sos/operations.md)
- [`sos/validation.md`](sos/validation.md)

## Why Coordinate Matching Exists
- UK-AIR register is keyed by `uk_air_ref`, but SOS metadata does not always include it.
- Station names are not unique and can vary; coordinates are the most stable tie-breaker.
- Name + distance provides a reliable fallback for linking SOS stations to UK-AIR sites.

## Network assignment
- Each station has one canonical `stations.network_id`.
- Public labels and codes come from the referenced `networks` row.

## Notes on Station Granularity
- SOS can emit multiple `timeseries_ref` per station.
- We keep `stations` at the SOS `station_ref` level and `timeseries` at the phenomenon level.
- Use `sos_station_uk_air_refs.uk_air_ref` to group a station across phenomena or networks.
- Use `sos_station_timeseries_site_refs` when the source is an archive flat file keyed by DEFRA `site_ref` and pollutant.