UK_AQ_RAW

sos_site_register

	uk_air_ref     -- official UK-AIR ID, e.g. UKA00591
	site_ref       -- UK-AIR flat-file/Data Selector site_id, e.g. EA8


sos_station_uk_air_refs

	station_id     -- internal UK AQ station id
	uk_air_ref     -- official UK-AIR ID


sos_station_timeseries_site_refs

	station_id     -- internal UK AQ station id
	timeseries_id  -- internal UK AQ timeseries id
	site_ref       -- flat-file/Data Selector site_id
	pollutant_code -- pm25, pm10, no2, etc.



	The route is then:

	station_id
	  -> uk_air_ref
	  -> site_ref
	  -> timeseries_id + pollutant_code

	Or as joins:

	sos_station_uk_air_refs.station_id
	  -> sos_station_uk_air_refs.uk_air_ref
	  -> sos_site_register.uk_air_ref
	  -> sos_site_register.site_ref
	  -> sos_station_timeseries_site_refs.site_ref

  sos_site_register = UK-AIR monitoring sites register, enriched with site_ref
  sos_station_uk_air_refs = bridge from internal station_id to UK-AIR ID
  sos_station_timeseries_site_refs = resolved site_ref/pollutant/timeseries mapping
  
 

  UK_AQ_RAW

  sos_site_register
    uk_air_ref
      - Source: UK-AIR monitoring sites CSV
      - Meaning: official UK-AIR site identifier, e.g. UKA00591

    site_ref
      - Source: official UK-AIR site-info discovery, or the checked seed map CSV
      - Meaning: UK-AIR flat-file/Data Selector site_id, e.g. EA8


  sos_station_uk_air_refs
    station_id
      - Source: our Supabase uk_aq_core.stations table
      - Meaning: internal UK AQ station id created by daily SOS station discovery

    uk_air_ref
      - Source: matched from sos_site_register
      - Meaning: official UK-AIR site identifier matched to that internal station
      - How: monthly matching step should match daily SOS station rows to register rows, probably by name + coordinates


  sos_station_timeseries_site_refs
    station_id
      - Source: our Supabase uk_aq_core.stations table, via sos_station_uk_air_refs
      - Meaning: internal UK AQ station id

    timeseries_id
      - Source: our Supabase uk_aq_core.timeseries table
      - Meaning: internal UK AQ timeseries id created by daily SOS timeseries discovery

    site_ref
      - Source: sos_site_register.site_ref, joined via uk_air_ref
      - Meaning: UK-AIR flat-file/Data Selector site_id, e.g. EA8

    pollutant_code
      - Source: our Supabase observed property mapping
      - Meaning: canonical UK AQ pollutant code, e.g. pm25, pm10, no2