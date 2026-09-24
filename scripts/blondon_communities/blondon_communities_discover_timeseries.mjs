#!/usr/bin/env node
import {
  buildCommunitiesPhenomenaRows,
  buildCommunitiesTimeseriesRows,
  canonicalCommunitiesMappings,
  classifyCommunitiesTimeseriesRows,
} from "../../shared/blondon_communities_reference.mjs";

const base = (process.env.SUPABASE_URL || "").replace(/\/$/, "");
const key = process.env.SB_SECRET_KEY || "";
if (!base || !key) throw new Error("SUPABASE_URL and SB_SECRET_KEY are required");

const baseHeaders = {
  apikey: key,
  Authorization: `Bearer ${key}`,
  "Content-Type": "application/json",
};
const timeseriesFields = [
  "id",
  "connector_id",
  "station_id",
  "timeseries_ref",
  "label",
  "uom",
  "service_ref",
  "phenomenon_id",
  "observed_property_id",
  "extras",
].join(",");

async function request(path, options = {}, schema = "uk_aq_core") {
  const headers = {
    ...baseHeaders,
    "Accept-Profile": schema,
    "Content-Profile": schema,
    ...(options.headers || {}),
  };
  const response = await fetch(`${base}/rest/v1/${path}`, { ...options, headers });
  const responseText = await response.text();
  if (!response.ok) {
    throw new Error(`${path}: HTTP ${response.status} ${responseText.slice(0, 500)}`);
  }
  return responseText ? JSON.parse(responseText) : [];
}

function postgrestIn(values) {
  return values.map((value) => `"${String(value).replaceAll('"', '\\"')}"`).join(",");
}

async function fetchRequiredTimeseries(connectorId, expectedRows) {
  const output = [];
  for (let index = 0; index < expectedRows.length; index += 150) {
    const refs = postgrestIn(
      expectedRows.slice(index, index + 150).map((row) => row.timeseries_ref),
    );
    output.push(...await request(
      `timeseries?connector_id=eq.${connectorId}&timeseries_ref=in.(${encodeURIComponent(refs)})&select=${timeseriesFields}`,
    ));
  }
  return output;
}

const connectors = await request(
  "connectors?connector_code=eq.blondon_communities&select=id&limit=2",
);
if (connectors.length !== 1) {
  throw new Error(`Expected exactly one blondon_communities connector; found ${connectors.length}`);
}
const connectorId = Number(connectors[0].id);
const stations = await request(
  `stations?connector_id=eq.${connectorId}&service_ref=eq.breathelondon&removed_at=is.null&select=id,station_ref,station_name,label&order=id`,
);
const phenomenaRows = buildCommunitiesPhenomenaRows(connectorId);
const diagnostics = await request(
  "rpc/uk_aq_rpc_phenomena_upsert",
  { method: "POST", body: JSON.stringify({ rows: phenomenaRows }) },
  "uk_aq_public",
);
const mappings = canonicalCommunitiesMappings(phenomenaRows, diagnostics);
const expectedRows = buildCommunitiesTimeseriesRows(stations, {
  connectorId,
  ...mappings,
});
const expectedRefs = new Set(expectedRows.map((row) => row.timeseries_ref));
if (expectedRefs.size !== expectedRows.length || expectedRows.length !== stations.length * 2) {
  throw new Error("Communities builder did not produce exactly two unique references per active station");
}

const beforeRows = await fetchRequiredTimeseries(connectorId, expectedRows);
const before = classifyCommunitiesTimeseriesRows(expectedRows, beforeRows);
const stableIds = new Map(
  beforeRows.map((row) => [String(row.timeseries_ref), Number(row.id)]),
);
const missingBefore = new Set(before.missingRefs);
const mismatchedBefore = new Set(before.mismatchedRefs);
const rowsToRepair = expectedRows.filter((row) =>
  missingBefore.has(row.timeseries_ref) || mismatchedBefore.has(row.timeseries_ref)
);
if (rowsToRepair.length) {
  await request(
    "timeseries?on_conflict=connector_id,timeseries_ref",
    {
      method: "POST",
      headers: { Prefer: "resolution=merge-duplicates,return=minimal" },
      body: JSON.stringify(rowsToRepair),
    },
  );
}

const finalRows = await fetchRequiredTimeseries(connectorId, expectedRows);
const final = classifyCommunitiesTimeseriesRows(expectedRows, finalRows, stableIds);
const summary = {
  connector_id: connectorId,
  active_station_count: stations.length,
  expected_active_timeseries_count: expectedRows.length,
  pre_existing_required_timeseries_count: beforeRows.length,
  upserted_or_repaired_count: rowsToRepair.length,
  final_required_timeseries_count: finalRows.length,
  missing_required_timeseries_count: final.missingRefs.length,
  mismatched_required_timeseries_count: final.mismatchedRefs.length,
  changed_existing_timeseries_id_count: final.changedIdRefs.length,
  missing_required_timeseries_refs: final.missingRefs.slice(0, 200),
  mismatched_required_timeseries_refs: final.mismatchedRefs.slice(0, 200),
  changed_existing_timeseries_id_refs: final.changedIdRefs.slice(0, 200),
  ok: !final.missingRefs.length
    && !final.mismatchedRefs.length
    && !final.changedIdRefs.length
    && finalRows.length === expectedRows.length,
};
console.log(`DISCOVERY_SUMMARY_JSON ${JSON.stringify(summary)}`);
if (!summary.ok) process.exitCode = 1;
