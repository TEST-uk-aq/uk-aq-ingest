export const COMMUNITIES_SPECIES = Object.freeze(["IPM25", "INO2"]);

export const COMMUNITIES_SPECIES_CONFIG = Object.freeze({
  IPM25: Object.freeze({ label: "PM2.5", uom: "ug/m3", source_label: "breathelondon:pm2.5", notation: "PM2.5", pollutant_label: "pm2.5", observed_property_code: "pm25", observed_property_domain: "aq", mapping_kind: "raw_observed_property", is_aqi_eligible: true }),
  INO2: Object.freeze({ label: "NO2", uom: "ug/m3", source_label: "breathelondon:no2", notation: "NO2", pollutant_label: "no2", observed_property_code: "no2", observed_property_domain: "aq", mapping_kind: "raw_observed_property", is_aqi_eligible: true }),
});

export function buildCommunitiesPhenomenaRows(connectorId, species = COMMUNITIES_SPECIES) {
  return species.map((name) => {
    const config = COMMUNITIES_SPECIES_CONFIG[name];
    if (!config) throw new Error(`Unsupported Communities species: ${name}`);
    return { connector_id: Number(connectorId), label: config.label, source_label: config.source_label, notation: config.notation, pollutant_label: config.pollutant_label, source_uom: config.uom, mapping_kind: config.mapping_kind, observed_property_code: config.observed_property_code, observed_property_domain: config.observed_property_domain, is_aqi_eligible: config.is_aqi_eligible };
  });
}

export function canonicalCommunitiesMappings(phenomenaRows, diagnostics) {
  const byLabel = new Map((diagnostics ?? []).map((row) => [String(row.source_label), row]));
  const phenomenonIds = {}, observedPropertyIds = {};
  for (const input of phenomenaRows) {
    const row = byLabel.get(input.source_label);
    if (!row?.phenomenon_id || !row?.observed_property_id || row.mapping_warning) throw new Error(`Invalid canonical Communities mapping for ${input.source_label}`);
    if (row.observed_property_code !== input.observed_property_code || row.mapping_kind !== input.mapping_kind) throw new Error(`Canonical Communities mapping mismatch for ${input.source_label}`);
    phenomenonIds[input.source_label] = Number(row.phenomenon_id);
    observedPropertyIds[input.source_label] = Number(row.observed_property_id);
  }
  return { phenomenonIds, observedPropertyIds };
}

export function buildCommunitiesTimeseriesRows(stations, { connectorId, serviceRef = "breathelondon", phenomenonIds, observedPropertyIds, species = COMMUNITIES_SPECIES }) {
  const rows = [], seen = new Set();
  for (const station of stations) for (const name of species) {
    const config = COMMUNITIES_SPECIES_CONFIG[name], stationRef = String(station.station_ref ?? "").trim();
    const ref = `${stationRef}:${name}`;
    if (!stationRef || seen.has(ref)) throw new Error(`Invalid or duplicate Communities timeseries identity: ${ref}`);
    seen.add(ref);
    const phenomenonId = phenomenonIds[config.source_label], observedPropertyId = observedPropertyIds[config.source_label];
    if (!phenomenonId || !observedPropertyId) throw new Error(`Missing canonical IDs for ${config.source_label}`);
    rows.push({ timeseries_ref: ref, label: `${station.station_name || station.label || stationRef} ${config.label}`, uom: config.uom, station_id: Number(station.id), service_ref: serviceRef, connector_id: Number(connectorId), phenomenon_id: phenomenonId, observed_property_id: observedPropertyId, extras: { site_code: stationRef, species: name } });
  }
  return rows;
}
