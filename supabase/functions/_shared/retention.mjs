// Prune owns days strictly before UTC midnight minus retention days.
// Published/candidate days remain historical even if retention is later widened.
const DAY = 86_400_000;
let retentionDays;
let ownershipFloor;
const env = (name) => globalThis.Deno?.env.get(name) ?? globalThis.process?.env[name];
export function parseRetentionDays(value) {
  if (!/^[0-9]+$/.test(String(value ?? "")) || !Number.isSafeInteger(Number(value)) || Number(value) < 2) {
    throw new Error("INGESTDB_RETENTION_DAYS is required and must be an integer >= 2");
  }
  return Number(value);
}
export function utcRetentionFloor(days, now = Date.now()) {
  return Math.floor(now / DAY) * DAY - days * DAY;
}
export async function loadRetention(connectorCode) {
  const days = parseRetentionDays(env("INGESTDB_RETENTION_DAYS"));
  const url = env("SUPABASE_URL");
  const key = env("SB_SECRET_KEY");
  if (!url || !key) throw new Error("Retention ownership requires SUPABASE_URL and SB_SECRET_KEY");
  const response = await fetch(`${url.replace(/\/$/, "")}/rest/v1/rpc/uk_aq_rpc_ingest_retention_boundaries`, {
    method: "POST", signal: AbortSignal.timeout(20_000),
    headers: { apikey: key, Authorization: `Bearer ${key}`, "Content-Type": "application/json", "Content-Profile": "uk_aq_public" },
    body: JSON.stringify({ p_retention_days: days }),
  });
  if (!response.ok) throw new Error(`Retention ownership lookup failed: HTTP ${response.status}`);
  const rows = await response.json();
  const row = rows.find((r) => r.connector_code === connectorCode);
  const floor = Date.parse(row?.not_before);
  if (!Number.isFinite(floor)) throw new Error(`Missing retention ownership for ${connectorCode}`);
  retentionDays = days;
  ownershipFloor = floor;
}
export function retentionFloor(now = Date.now()) {
  if (retentionDays === undefined || ownershipFloor === undefined) throw new Error("Retention ownership not loaded");
  return Math.max(ownershipFloor, utcRetentionFloor(retentionDays, now));
}
export function eligibleObservation(value, now = Date.now()) {
  if (typeof value !== "string" || !value.trim()) return false;
  const at = Date.parse(value);
  return Number.isFinite(at) && at >= retentionFloor(now) && at <= now;
}
export function boundedStart(value, now = Date.now()) {
  const at = value instanceof Date ? value.getTime() : Date.parse(value);
  if (!Number.isFinite(at)) throw new Error("Invalid acquisition start timestamp");
  return new Date(Math.max(at, retentionFloor(now)));
}
// Recheck on every write/retry. Abort a changed batch rather than reporting rows
// as committed that were removed at midnight; the next poll safely replays it.
export function assertEligibleRows(rows) {
  if (rows.some((row) => !eligibleObservation(row.observed_at))) {
    throw new Error("Observation batch crossed retention/ownership boundary; replay eligible rows next poll");
  }
}
export function sourceTimestamp(value) {
  if (typeof value !== "string") return null;
  const text = value.trim();
  const match = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$/.exec(text);
  if (!match) return null;
  const [, year, month, day, hour, minute, second] = match;
  const daysInMonth = new Date(Date.UTC(Number(year), Number(month), 0)).getUTCDate();
  if (+month < 1 || +month > 12 || +day < 1 || +day > daysInMonth || +hour > 23 || +minute > 59 || +(second ?? 0) > 59) return null;
  const normalized = text.replace(" ", "T") + (match[8] ? "" : "Z");
  const at = Date.parse(normalized);
  return Number.isFinite(at) ? new Date(at).toISOString() : null;
}
