# UK-AIR SOS Ingest Flow

This page summarizes how SOS data lands in tables and how stations map to multiple networks.

## Key Tables
- `connectors`: data sources. UK-AIR SOS is one connector.
- `stations`: station metadata ingested from SOS (one row per `station_ref`).
- `timeseries`: per-station, per-phenomenon SOS time series metadata (`timeseries_ref`).
- `phenomena`: pollutant definitions tied to a connector.
- `observations`: time/value pairs keyed by `connector_id` + `timeseries_id` + `observed_at`.
- Placeholder SOS station refs (for example `9999999999`) are skipped during ingest and flagged via `station_metadata.exclude_from_ui=true`.
- `uk_air_sos_site_register`: UK-AIR site register snapshot (includes `uk_air_ref`, optional DEFRA flat-file `site_ref`, and source network labels).
- `uk_air_sos_networks`: network lookup (source label -> internal `network_code` + UI display name).
- `uk_air_sos_network_pollutants`: pollutant matching rules per network.
- `uk_air_sos_station_refs`: map SOS `station_id` to `uk_air_ref`.
- `uk_air_sos_site_timeseries_refs`: map archive `site_ref` + pollutant to UK AQ station/timeseries rows for historical backfill.
- `networks`: canonical network catalog referenced by `stations.network_id`.

## Ingest Steps
1) **SOS metadata ingest (daily)**
   - Fetches SOS stations + timeseries metadata.
   - Upserts into `stations` and `timeseries`.
   - Each `timeseries` row links to `phenomena` and a `station_id`.
2) **UK-AIR register ingest (daily/periodic)**
   - Loads `uk_air_sos_site_register`.
   - Discovers DEFRA flat-file `site_ref` values for AURN rows from official `networks/site-info?uka_id=<uk_air_ref>` pages.
   - Uses `network_info/uk_air_sos/uk_air_sos_site_refs.csv` as a seed/override map where needed.
   - The monthly workflow validates mapped and discovered `site_ref` values against `networks/site-info?site_id=<site_ref>` and `data/flat_files?site_id=<site_ref>` before loading them.
   - After loading the register snapshot, calls `uk_aq_rpc_uk_air_sos_site_timeseries_refs_refresh` to map each validated `site_ref` and pollutant to the corresponding UK AQ station and timeseries.
   - Successive ended/current timeseries receive non-overlapping validity dates. Multiple active timeseries for one `site_ref` + pollutant, or an invalid derived interval, fails the workflow before mappings are written.
   - Unmapped AURN register sites are counted in the workflow log and are not guessed.
   - Upserts `uk_air_sos_networks` and `uk_air_sos_network_pollutants`.
3) **Station-to-register matching**
   - If SOS metadata includes a UK-AIR ID, link directly.
   - Otherwise match by station name + distance (coordinates).
   - Writes `uk_air_sos_station_refs` with match method + distance.
4) **Network membership backfill**
   - Collects pollutant keys from station `timeseries` -> `phenomena`.
   - Filters allowed networks via `uk_air_sos_network_pollutants`.
   - Validates the station's scalar `network_id` against `networks.id`.

## Polling Flow (Observations)
- 15-minute polling uses `timeseries_ref` to resolve `timeseries.id`.
- Each sample is stored in `observations` keyed by `connector_id` + `timeseries_id` + `observed_at`.
- Edge path: `uk_air_sos_timeseries_checkpoints` records `last_polled_at` so the dispatcher rotates timeseries batches.
- Cloud Run path: `uk_air_sos_station_checkpoints` records station due-state and lag samples; station refs are selected first, then scoped timeseries are polled.

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
- Use `uk_air_sos_station_refs.uk_air_ref` to group a station across phenomena or networks.
- Use `uk_air_sos_site_timeseries_refs` when the source is an archive flat file keyed by DEFRA `site_ref` and pollutant.
