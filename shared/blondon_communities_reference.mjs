export const COMMUNITIES_SPECIES = Object.freeze(["IPM25", "INO2"]);

export const COMMUNITIES_SPECIES_CONFIG = Object.freeze({
  IPM25: Object.freeze({ label: "PM2.5", uom: "ug/m3", source_label: "breathelondon:pm2.5", notation: "PM2.5", pollutant_label: "pm2.5", observed_property_code: "pm25", observed_property_domain: "aq", mapping_kind: "raw_observed_property", is_aqi_eligible: true }),
  INO2: Object.freeze({ label: "NO2", uom: "ug/m3", source_label: "breathelondon:no2", notation: "NO2", pollutant_label: "no2", observed_property_code: "no2", observed_property_domain: "aq", mapping_kind: "raw_observed_property", is_aqi_eligible: true }),
});

export const COMMUNITIES_TIMESERIES_STATIC_FIELDS = Object.freeze([
  "connector_id",
  "station_id",
  "timeseries_ref",
  "label",
  "uom",
  "service_ref",
  "phenomenon_id",
  "observed_property_id",
]);

export function buildCommunitiesPhenomenaRows(connectorId, species = COMMUNITIES_SPECIES) {
  return species.map((name) => {
    const config = COMMUNITIES_SPECIES_CONFIG[name];
    if (!config) throw new Error(`Unsupported Communities species: ${name}`);
    return { connector_id: Number(connectorId), label: config.label, source_label: config.source_label, notation: config.notation, pollutant_label: config.pollutant_label, source_uom: config.uom, mapping_kind: config.mapping_kind, observed_property_code: config.observed_property_code, observed_property_domain: config.observed_property_domain, is_aqi_eligible: config.is_aqi_eligible };
  });
}

export function canonicalCommunitiesMappings(phenomenaRows, diagnostics) {
  const byLabel = new Map((diagnostics ?? []).map((row) => [String(row.source_label), row]));
  /** @type {Record<string, number>} */
  const phenomenonIds = {};
  /** @type {Record<string, number>} */
  const observedPropertyIds = {};
  for (const input of phenomenaRows) {
    const row = byLabel.get(input.source_label);
    if (!row?.phenomenon_id || !row?.observed_property_id || row.mapping_warning) throw new Error(`Invalid canonical Communities mapping for ${input.source_label}`);
    if (row.observed_property_code !== input.observed_property_code || row.mapping_kind !== input.mapping_kind || row.is_aqi_eligible !== input.is_aqi_eligible) throw new Error(`Canonical Communities mapping mismatch for ${input.source_label}`);
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
    const stationId = Number(station.id), numericConnectorId = Number(connectorId);
    const phenomenonId = phenomenonIds[config.source_label], observedPropertyId = observedPropertyIds[config.source_label];
    if (!Number.isInteger(stationId) || !Number.isInteger(numericConnectorId)) throw new Error(`Invalid Communities station or connector ID for ${ref}`);
    if (!phenomenonId || !observedPropertyId) throw new Error(`Missing canonical IDs for ${config.source_label}`);
    rows.push({ timeseries_ref: ref, label: `${station.station_name || station.label || stationRef} ${config.label}`, uom: config.uom, station_id: stationId, service_ref: serviceRef, connector_id: numericConnectorId, phenomenon_id: phenomenonId, observed_property_id: observedPropertyId, extras: { site_code: stationRef, species: name } });
  }
  return rows;
}

export function communitiesTimeseriesRowMatches(existing, expected) {
  for (const field of COMMUNITIES_TIMESERIES_STATIC_FIELDS) {
    const actual = existing?.[field];
    const wanted = expected?.[field];
    if (field.endsWith("_id")) {
      if (Number(actual) !== Number(wanted)) return false;
    } else if (actual !== wanted) {
      return false;
    }
  }
  const actualExtras = existing?.extras;
  const expectedExtras = expected?.extras;
  return Boolean(
    actualExtras && typeof actualExtras === "object"
      && expectedExtras && typeof expectedExtras === "object"
      && actualExtras.site_code === expectedExtras.site_code
      && actualExtras.species === expectedExtras.species
  );
}

export function classifyCommunitiesTimeseriesRows(expectedRows, actualRows, stableIds = new Map()) {
  const actualByRef = new Map();
  for (const row of actualRows ?? []) {
    const ref = String(row?.timeseries_ref ?? "");
    if (!ref || actualByRef.has(ref)) throw new Error(`Duplicate stored Communities timeseries identity: ${ref || "<missing>"}`);
    actualByRef.set(ref, row);
  }
  const missingRefs = [], mismatchedRefs = [], changedIdRefs = [];
  /** @type {Record<string, number>} */
  const validIds = {};
  for (const expected of expectedRows ?? []) {
    const ref = String(expected.timeseries_ref);
    const actual = actualByRef.get(ref);
    if (!actual) {
      missingRefs.push(ref);
      continue;
    }
    if (!communitiesTimeseriesRowMatches(actual, expected)) {
      mismatchedRefs.push(ref);
      continue;
    }
    if (!Number.isInteger(Number(actual.id)) || Number(actual.id) <= 0) {
      mismatchedRefs.push(ref);
      continue;
    }
    if (stableIds.has(ref) && Number(stableIds.get(ref)) !== Number(actual.id)) {
      changedIdRefs.push(ref);
      continue;
    }
    validIds[ref] = Number(actual.id);
  }
  return { actualByRef, validIds, missingRefs, mismatchedRefs, changedIdRefs };
}
