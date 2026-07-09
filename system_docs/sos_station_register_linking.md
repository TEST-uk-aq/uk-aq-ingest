# UK-AIR SOS station/register linking

This note explains the simple route from a UK-AIR SOS station row to the official UK-AIR flat-file/Data Selector `site_ref`.

## Short version

The intended identity chain is:

```text
uk_aq_core.stations.id
  -> uk_aq_raw.sos_station_uk_air_refs.station_id
  -> uk_aq_raw.sos_station_uk_air_refs.uk_air_ref
  -> uk_aq_raw.sos_site_register.uk_air_ref
  -> uk_aq_raw.sos_site_register.site_ref
```

For archive/Data Selector links, the final operational table is:

```text
uk_aq_raw.sos_station_timeseries_site_refs
```

That table links:

```text
site_ref + pollutant_code
  -> station_id
  -> timeseries_id
```

## Daily station discovery

The daily UK-AIR SOS station workflow gets data from the UK-AIR SOS REST API.

Source:

```text
https://uk-air.defra.gov.uk/sos-ukair/api/v1
```

Main daily jobs:

```text
scripts/sos/sos_list_stations.py --to-supabase
scripts/sos/sos_ingest.py --discover
```

### What daily knows

Daily discovery knows SOS/API identifiers:

| Concept | Where it comes from | Stored as |
|---|---|---|
| SOS station ref | SOS `/stations` payload | `uk_aq_core.stations.station_ref` |
| SOS service ref | SOS service metadata / station payload | `uk_aq_core.stations.service_ref` |
| Internal station id | Supabase identity column | `uk_aq_core.stations.id` |
| SOS timeseries ref | SOS `/timeseries` payload | `uk_aq_core.timeseries.timeseries_ref` |
| Internal timeseries id | Supabase identity column | `uk_aq_core.timeseries.id` |
| Pollutant | SOS phenomenon metadata | `phenomena -> observed_properties.code` |

### What daily does not reliably know

Daily SOS station discovery does not reliably know the UK-AIR flat-file/Data Selector code. On UK-AIR/GOV.UK pages this appears as the external `site_id` URL parameter; internally UK AQ stores it as `site_ref`.

Examples:

```text
EA8
BDMA
```

It also does not reliably know the official UK-AIR register identifier unless a separate register matching step has populated it.

That means daily can populate:

```text
stations
timeseries
phenomena
observed_properties
```

but daily should not guess:

```text
uk_air_ref
site_ref
```

## Monthly site register load

The monthly UK-AIR SOS site register workflow gets data from the official UK-AIR monitoring sites CSV.

Main monthly job:

```text
scripts/sos/sos_site_register.py --load
```

The current monthly workflow also uses:

```text
--site-ref-map-csv network_info/sos/sos_site_refs.csv
--validate-site-ref-map
--discover-site-refs
```

### What monthly knows

Monthly register loading knows official UK-AIR register/site identifiers:

| Concept | Where it comes from | Stored as |
|---|---|---|
| UK-AIR site identifier | UK-AIR monitoring sites CSV, `UK-AIR ID` | `uk_aq_raw.sos_site_register.uk_air_ref` |
| UK-AIR flat-file/Data Selector code | Seed map and/or official UK-AIR site-info discovery | `uk_aq_raw.sos_site_register.site_ref` |
| Site name | UK-AIR monitoring sites CSV | `uk_aq_raw.sos_site_register.site_name` |
| Coordinates | UK-AIR monitoring sites CSV | `latitude`, `longitude` |
| Networks | UK-AIR monitoring sites CSV | `networks` |

Example:

```text
uk_air_ref = UKA00591
site_ref   = EA8
site_name  = Ealing Horn Lane
```

## The two linking tables

### 1. Station to UK-AIR register link

Table:

```text
uk_aq_raw.sos_station_uk_air_refs
```

Purpose:

```text
station_id -> uk_air_ref
```

This is the missing bridge if `station_ref_rows = 0`.

It should be populated by matching monthly register rows to already-discovered SOS station rows.

Suggested matching inputs:

| From monthly register | From daily SOS stations |
|---|---|
| `uk_air_ref` | `stations.id` |
| `site_name` | `stations.station_name` |
| `latitude`, `longitude` | `stations.geometry` |
| `networks` | `stations.network_id` / connector context |

Suggested output:

```text
station_id
uk_air_ref
match_method
match_distance_m
source_snapshot_at
created_at
updated_at
```

### 2. Site ref to timeseries link

Table:

```text
uk_aq_raw.sos_station_timeseries_site_refs
```

Purpose:

```text
site_ref + pollutant_code -> station_id -> timeseries_id
```

This table is refreshed by:

```text
uk_aq_public.uk_aq_rpc_sos_station_timeseries_site_refs_refresh(...)
```

The RPC expects `sos_station_uk_air_refs` to already contain `station_id -> uk_air_ref`.

If `sos_station_uk_air_refs` is empty, the RPC has no route from the register to stations/timeseries and will insert zero rows.

## Current failure shape

If these checks return:

```text
sos_site_register rows with site_ref > 0
sos_station_uk_air_refs rows = 0
sos_station_timeseries_site_refs rows = 0
```

then the site register load and `site_ref` discovery are working, but the station bridge step is missing.

The route fails here:

```text
sos_site_register.uk_air_ref
  -X-> sos_station_uk_air_refs.uk_air_ref
```

because `sos_station_uk_air_refs` has no rows.

## Intended monthly order

The monthly workflow should run in this order:

```text
1. Download/load UK-AIR monitoring sites CSV.
2. Load/discover `site_ref` values into `sos_site_register`.
3. Populate `sos_station_uk_air_refs`:
     stations.id -> uk_air_ref
4. Refresh `sos_station_timeseries_site_refs`:
     site_ref + pollutant_code -> station_id -> timeseries_id
```

## Minimal route SQL

This is the simplest query shape once the bridge table is populated:

```sql
select
  s.id as station_id,
  s.station_name,
  sr.uk_air_ref,
  reg.site_ref,
  reg.site_name as register_site_name
from uk_aq_core.stations s
join uk_aq_raw.sos_station_uk_air_refs sr
  on sr.station_id = s.id
join uk_aq_raw.sos_site_register reg
  on reg.uk_air_ref = sr.uk_air_ref
where s.connector_id = (
  select id
  from uk_aq_core.connectors
  where connector_code = 'sos'
  limit 1
)
  and reg.snapshot_at = (
    select max(snapshot_at)
    from uk_aq_raw.sos_site_register
  )
order by reg.site_ref, s.station_name;
```

## Practical diagnosis

If the RPC returns:

```json
{
  "mapping_rows_upserted": 0,
  "mapped_site_refs": 0,
  "mapped_sites_without_timeseries": 211
}
```

and this check returns:

```text
aurn_register_rows = 211
aurn_rows_with_site_ref = 211
rows_with_station_ref_bridge = 0
```

then the missing step is not `site_ref` discovery.

The missing step is:

```text
populate uk_aq_raw.sos_station_uk_air_refs
```

before running:

```text
uk_aq_public.uk_aq_rpc_sos_station_timeseries_site_refs_refresh(...)
```
