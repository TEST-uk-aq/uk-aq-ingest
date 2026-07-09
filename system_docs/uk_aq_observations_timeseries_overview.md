# Observations & Timeseries Tables (UK Air Quality)

This note describes how the `timeseries` and `observations` tables work, how to query them, and how they relate to `stations`.

## What the tables represent

### `timeseries`
- Stores metadata for each unique measurement series coming from an SOS service_ref (one row per series).
- Each series is tied to a monitoring site via `timeseries.station_id` (FK → `stations.id`).
- Series identify a pollutant/parameter via `timeseries.phenomenon_id` and include units, labels, and raw metadata.
- The pair `(connector_id, service_ref, timeseries_ref)` is unique, so each external SOS series maps to exactly one row.
- UK-AIR lifecycle fields: `last_catalog_seen_at`, `catalog_missing_runs`, `ended_at` (ended rows are not polled).

### `observations`
- Stores the actual time-value measurements for each series.
- Each row is a single reading at a specific time: `(connector_id, timeseries_id, observed_at)` is the primary key.
- `observations.timeseries_id` points back to `timeseries.id`, so observations are always attached to a single series.

## How `timeseries`, `observations`, and `stations` interact

- `stations` defines the monitoring site (name, region, geometry).
- `timeseries` defines a specific pollutant/parameter at that station (e.g., NO2 at “Bristol Centre”).
- `observations` holds the raw measurements for that timeseries over time.

In other words:

`stations (site)` → `timeseries (pollutant series at site)` → `observations (time/value readings)`

Station pollutant coverage is derived by looking at the `timeseries` rows attached to a station (not directly stored on the station itself).

## How to get data (example SQL)

> Below are typical query patterns you can use in Supabase/Postgres. Adjust filters as needed.

### 1) Find all timeseries for a station
```sql
select ts.*
from timeseries ts
join stations st on st.id = ts.station_id
where st.station_ref = 'YOUR_STATION_REF';
```

### 2) Pull observations for a specific timeseries
```sql
select obs.observed_at, obs.value, obs.status
from observations obs
where obs.timeseries_id = 123
order by obs.observed_at desc
limit 100;
```

### 3) Pull latest observation per timeseries for a station
```sql
select distinct on (obs.timeseries_id)
  obs.timeseries_id,
  obs.observed_at,
  obs.value,
  ts.label as timeseries_label,
  st.label as station_label
from observations obs
join timeseries ts on ts.id = obs.timeseries_id
join stations st on st.id = ts.station_id
where st.station_ref = 'YOUR_STATION_REF'
order by obs.timeseries_id, obs.observed_at desc;
```

### 4) Pull observations across all stations for a time window
```sql
select st.label as station_label,
       ts.label as timeseries_label,
       obs.observed_at,
       obs.value
from observations obs
join timeseries ts on ts.id = obs.timeseries_id
join stations st on st.id = ts.station_id
where obs.observed_at >= now() - interval '24 hours'
order by obs.observed_at desc;
```

## Ingestion flow (how data gets there)

The typical ingest sequence is:
1) Discover SOS services and stations.
2) Fetch timeseries metadata (with `expanded=true`).
3) Backfill or poll `/timeseries/{id}/getData` from the SOS API.
4) Upsert into `observations` keyed by `(connector_id, timeseries_id, observed_at)`.

The Edge Function `ingest_sos` also polls recent observations using existing `timeseries` rows.
