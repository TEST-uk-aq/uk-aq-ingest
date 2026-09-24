#!/usr/bin/env node
import { buildCommunitiesPhenomenaRows, buildCommunitiesTimeseriesRows, canonicalCommunitiesMappings } from "../../shared/blondon_communities_reference.mjs";

const base = (process.env.SUPABASE_URL || "").replace(/\/$/, ""), key = process.env.SB_SECRET_KEY || "";
if (!base || !key) throw new Error("SUPABASE_URL and SB_SECRET_KEY are required");
const headers = { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json", "Accept-Profile": "uk_aq_core", "Content-Profile": "uk_aq_core" };
async function request(path, options = {}, schema = "uk_aq_core") { const h = { ...headers, "Accept-Profile": schema, "Content-Profile": schema, ...(options.headers || {}) }; const response = await fetch(`${base}/rest/v1/${path}`, { ...options, headers: h }); const text = await response.text(); if (!response.ok) throw new Error(`${path}: HTTP ${response.status} ${text.slice(0,500)}`); return text ? JSON.parse(text) : []; }
const [connector] = await request("connectors?connector_code=eq.blondon_communities&select=id");
if (!connector) throw new Error("blondon_communities connector not found");
const stations = await request(`stations?connector_id=eq.${connector.id}&service_ref=eq.breathelondon&removed_at=is.null&select=id,station_ref,station_name,label&order=id`);
const phenomenaRows = buildCommunitiesPhenomenaRows(connector.id);
const diagnostics = await request("rpc/uk_aq_rpc_phenomena_upsert", { method: "POST", body: JSON.stringify({ rows: phenomenaRows }) }, "uk_aq_public");
const mappings = canonicalCommunitiesMappings(phenomenaRows, diagnostics);
const expected = buildCommunitiesTimeseriesRows(stations, { connectorId: connector.id, ...mappings });
const fields = "id,connector_id,station_id,timeseries_ref,label,uom,service_ref,phenomenon_id,observed_property_id,extras";
async function fetchRows() { const out=[]; for(let i=0;i<expected.length;i+=150){ const refs=expected.slice(i,i+150).map(r=>`\"${r.timeseries_ref.replaceAll('"','\\"')}\"`).join(','); out.push(...await request(`timeseries?connector_id=eq.${connector.id}&timeseries_ref=in.(${encodeURIComponent(refs)})&select=${fields}`)); } return out; }
const before = await fetchRows(), beforeMap = new Map(before.map(r=>[r.timeseries_ref,r]));
const matches=(a,b)=>Object.entries(b).every(([k,v])=>JSON.stringify(a[k])===JSON.stringify(v));
const repair = expected.filter(r=>!beforeMap.has(r.timeseries_ref)||!matches(beforeMap.get(r.timeseries_ref),r));
if(repair.length) await request("timeseries?on_conflict=connector_id,timeseries_ref", { method:"POST", headers:{Prefer:"resolution=merge-duplicates,return=minimal"}, body:JSON.stringify(repair) });
const after=await fetchRows(), afterMap=new Map(after.map(r=>[r.timeseries_ref,r]));
const missing=expected.filter(r=>!afterMap.has(r.timeseries_ref)), mismatched=expected.filter(r=>afterMap.has(r.timeseries_ref)&&!matches(afterMap.get(r.timeseries_ref),r));
const changed=[...beforeMap].filter(([ref,row])=>afterMap.has(ref)&&Number(afterMap.get(ref).id)!==Number(row.id));
const summary={connector_id:Number(connector.id),active_station_count:stations.length,expected_active_timeseries_count:expected.length,pre_existing_required_timeseries_count:before.length,upserted_or_repaired_count:repair.length,final_required_timeseries_count:after.length,missing_required_timeseries_count:missing.length,mismatched_required_timeseries_count:mismatched.length,changed_existing_timeseries_id_count:changed.length,ok:!missing.length&&!mismatched.length&&!changed.length};
console.log(`DISCOVERY_SUMMARY_JSON ${JSON.stringify(summary)}`); if(!summary.ok) process.exitCode=1;
