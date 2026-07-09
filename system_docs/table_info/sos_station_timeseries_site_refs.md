# `uk_aq_raw.sos_station_timeseries_site_refs`

Maps an official DEFRA UK-AIR archive `site_ref` and canonical pollutant code to
the UK AQ station and timeseries that may receive historical flat-file rows.

The monthly UK-AIR site-register workflow refreshes this table through
`uk_aq_public.uk_aq_rpc_sos_station_timeseries_site_refs_refresh`. The RPC joins
the latest AURN register snapshot to `sos_station_uk_air_refs` by `uk_air_ref`,
then joins the station's timeseries to its canonical observed property.

`valid_from_day_utc` and `valid_to_day_utc` make successive internal timeseries
deterministic for a historical day. The refresh fails when a `site_ref` and
pollutant have multiple active timeseries or produce an invalid interval.
Unmapped register sites are reported and never matched by station name.
