# Feature Fixes

## Backfill DB function
- Starting from the earliest date observations in the DB, request historical data per day/month for all sensors, and populate the DB as if it had been ingested in realtime.

## Geometry fixes
- Test the station geometry workflow from a blank database before promoting to production.

## Timeseries station mapping
- Verify ingest uses label+matching-geometry fallback when station_ref is missing; confirm no ambiguous label matches are applied.
- Applies to `scripts/sos/sos_ingest.py` and `scripts/uk_aq_backfill_timeseries_stations.py`.
