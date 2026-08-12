// trigger deploy 2026-02-09 12:36 
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import "../_shared/fetch_egress_patch.ts";
import { cacheControlHeaders } from "../_shared/cache.ts";
import {
  type ObservsObservationRow,
  writeObservsWithOutbox,
} from "../_shared/observs_client.ts";
import {
  buildCompactObservationRpcArgs,
  createEmptyIngestDbObservationWriteStats,
  isIngestDbObservationWriteError,
  mergeIngestDbObservationWriteStats,
  serializedJsonUtf8Bytes,
  writeIngestDbObservations,
} from "../_shared/ingestdb_observation_writer.mjs";

type PollRequest = {
  api_key?: string;
  connector_id?: string;
  connector_code?: string;
  connector_label?: string;
  service_ref?: string;
  base_url?: string;
  species?: string[] | string;
  station_refs?: string[] | string;
  initial_days?: number;
  start_date?: string;
  window_hours?: number;
  sleep_seconds?: number;
  batch_size?: number;
  limit?: number;
  skip_stations?: boolean;
  active_only?: boolean;
  dry_run?: boolean;
  debug?: boolean;
};

type ConnectorRow = {
  id: string;
  connector_code: string;
  label: string;
  service_url: string | null;
};

type DropboxConfig = {
  appKey: string;
  appSecret: string;
  refreshToken: string;
};

type DropboxDiagnostics = {
  enabled: boolean;
  reason: string | null;
  raw_enabled: boolean;
  raw_reason: string | null;
  has_app_key: boolean;
  has_app_secret: boolean;
  has_refresh_token: boolean;
  supabase_url: string | null;
  raw_allowed_supabase_url: string | null;
  raw_allowed_match: boolean;
  error_allowed_supabase_url: string | null;
  error_allowed_match: boolean;
  dropbox_root: string | null;
};

type ErrorLogEntry = {
  source: string;
  severity: string;
  message: string;
  stack?: string | null;
  context?: Record<string, unknown> | null;
  connector_code?: string | null;
  connector_id?: string | number | null;
  station_id?: string | number | null;
  timeseries_id?: string | number | null;
};

type LogBuffer = {
  lines: string[];
  info: (message: string, context?: Record<string, unknown>) => void;
  warn: (message: string, context?: Record<string, unknown>) => void;
  error: (message: string, context?: Record<string, unknown>) => void;
};

type RawRecorder = {
  lines: string[];
  responseCount: number;
  recordEvent: (name: string, payload: Record<string, unknown>) => void;
  recordResponse: (
    path: string,
    params: Record<string, string>,
    statusCode: number,
    payload: unknown,
  ) => void;
};

const DEFAULT_BASE_URL = "https://api.breathelondon-communities.org/api";
const DEFAULT_CONNECTOR_CODE = "blondon_communities";
const DEFAULT_SERVICE_REF = "breathelondon";
const DEFAULT_SERVICE_LABEL = "Breathe London";
const DEFAULT_USER_AGENT = "uk-air-quality-networks";
const DEFAULT_INITIAL_DAYS = 7;
const DEFAULT_WINDOW_HOURS = 24;
const DEFAULT_SLEEP_SECONDS = 0.2;
const DEFAULT_BATCH_SIZE = 500;
const DEFAULT_OBSERVS_BUFFER_FLUSH_ROWS = 5000;
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RUNTIME_SECONDS = 120;
const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);

async function postgrestFetch(url: string, init: RequestInit): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error("PostgREST request timed out.");
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

const SPECIES_CONFIG: Record<
  string,
  {
    label: string;
    uom: string;
    source_label: string;
    notation: string;
    pollutant_label: string;
    observed_property_code: string;
    observed_property_domain: "aq" | "met";
  }
> = {
  IPM25: {
    label: "PM2.5",
    uom: "ug/m3",
    source_label: "breathelondon:pm2.5",
    notation: "PM2.5",
    pollutant_label: "pm2.5",
    observed_property_code: "pm25",
    observed_property_domain: "aq",
  },
  INO2: {
    label: "NO2",
    uom: "ug/m3",
    source_label: "breathelondon:no2",
    notation: "NO2",
    pollutant_label: "no2",
    observed_property_code: "no2",
    observed_property_domain: "aq",
  },
};

const UK_BBOX = {
  west: -11.0,
  south: 49.0,
  east: 2.0,
  north: 61.0,
};

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")
  ?? Deno.env.get("SB_SUPABASE_URL")
  ?? "";
const SB_SECRET_KEY = Deno.env.get("SB_SECRET_KEY") ?? "";
const SUPABASE_PRIVILEGED_KEY = SB_SECRET_KEY;
const UK_AQ_CORE_SCHEMA = Deno.env.get("UK_AQ_CORE_SCHEMA")
  ?? "uk_aq_core";
const UK_AQ_RAW_SCHEMA = Deno.env.get("UK_AQ_RAW_SCHEMA")
  ?? "uk_aq_raw";

const BLONDON_COMMUNITIES_API_KEY = Deno.env.get("BLONDON_COMMUNITIES_API_KEY") ?? "";
const BLONDON_COMMUNITIES_BASE_URL = (Deno.env.get("BLONDON_COMMUNITIES_BASE_URL") ?? DEFAULT_BASE_URL)
  .replace(/\/$/, "");
const CONNECTOR_CODE_ERROR =
  "Use connector_code=blondon_communities for Breathe London Communities. network_code/service_ref may remain breathelondon.";
const BLONDON_COMMUNITIES_CONNECTOR_CODE = resolveCommunitiesConnectorCode(
  Deno.env.get("BLONDON_COMMUNITIES_CONNECTOR_CODE"),
);
const BLONDON_COMMUNITIES_SERVICE_REF = Deno.env.get("BLONDON_COMMUNITIES_SERVICE_REF")
  ?? DEFAULT_SERVICE_REF;
const BLONDON_COMMUNITIES_SERVICE_LABEL = Deno.env.get("BLONDON_COMMUNITIES_SERVICE_LABEL")
  ?? DEFAULT_SERVICE_LABEL;
const BLONDON_COMMUNITIES_USER_AGENT = Deno.env.get("BLONDON_COMMUNITIES_USER_AGENT")
  ?? DEFAULT_USER_AGENT;
const BLONDON_COMMUNITIES_MAX_RUNTIME_SECONDS = Number(
  Deno.env.get("BLONDON_COMMUNITIES_MAX_RUNTIME_SECONDS") ?? DEFAULT_MAX_RUNTIME_SECONDS,
);

function resolveCommunitiesConnectorCode(raw: unknown): string {
  const value = typeof raw === "string" ? raw.trim() : "";
  if (value && value !== DEFAULT_CONNECTOR_CODE) {
    throw new Error(CONNECTOR_CODE_ERROR);
  }
  return DEFAULT_CONNECTOR_CODE;
}
const SB_UK_AQ_CRON_SECRET = Deno.env.get("SB_UK_AQ_CRON_SECRET") ?? "";
const DROPBOX_APP_KEY = Deno.env.get("DROPBOX_APP_KEY") ?? "";
const DROPBOX_APP_SECRET = Deno.env.get("DROPBOX_APP_SECRET") ?? "";
const DROPBOX_REFRESH_TOKEN = Deno.env.get("DROPBOX_REFRESH_TOKEN") ?? "";
const DROPBOX_ALLOWED_SUPABASE_URL = Deno.env.get("BLONDON_COMMUNITIES_RAW_DROPBOX_ALLOWED_SUPABASE_URL")
  ?? Deno.env.get("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL")
  ?? "";
const DROPBOX_ERROR_ALLOWED_SUPABASE_URL = Deno.env.get("BLONDON_COMMUNITIES_ERROR_DROPBOX_ALLOWED_SUPABASE_URL")
  ?? Deno.env.get("UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL")
  ?? "";
const DROPBOX_ROOT_FOLDER = (() => {
  const raw = Deno.env.get("UK_AQ_DROPBOX_ROOT") ?? "";
  return normalizeDropboxPath(raw);
})();

const DROPBOX_LOG_FOLDER = dropboxWithRoot("/connectors/blondon_communities/log");
const DROPBOX_RAW_FOLDER = dropboxWithRoot("/connectors/blondon_communities/raw_data");
const DROPBOX_ERROR_FOLDER = dropboxWithRoot(
  Deno.env.get("BLONDON_COMMUNITIES_ERROR_DROPBOX_FOLDER")
    ?? Deno.env.get("UK_AIR_ERROR_DROPBOX_FOLDER")
    ?? "error_log",
);
const DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token";
const DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
const DROPBOX_UPLOAD_SOURCE = (() => {
  const value = (Deno.env.get("BLONDON_COMMUNITIES_DROPBOX_UPLOAD_SOURCE") ?? "edge")
    .trim()
    .toLowerCase();
  return value === "cloud_run" ? "cloud_run" : "edge";
})();
const BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET = (() => {
  const configured = asBoolean(Deno.env.get("BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET"));
  if (configured !== undefined) {
    return configured;
  }
  return DROPBOX_UPLOAD_SOURCE !== "cloud_run";
})();

const REST_BASE_URL = SUPABASE_URL
  ? `${SUPABASE_URL.replace(/\/$/, "")}/rest/v1`
  : "";

function postgrestHeaders(prefer?: string, schema = UK_AQ_CORE_SCHEMA): Record<string, string> {
  const headers: Record<string, string> = {
    apikey: SUPABASE_PRIVILEGED_KEY,
    "Content-Type": "application/json",
    "x-ukaq-egress-caller": "ingest_blondon_communities",
  };
  if (prefer) {
    headers.Prefer = prefer;
  }
  if (schema && schema !== "public") {
    headers["Accept-Profile"] = schema;
    headers["Content-Profile"] = schema;
  }
  return headers;
}

function requireCronSecret(req: Request): Response | null {
  if (!SB_UK_AQ_CRON_SECRET) {
    return null;
  }
  const header = req.headers.get("x-cron-secret");
  if (!header || header !== SB_UK_AQ_CRON_SECRET) {
    console.warn("cron_secret_mismatch", {
      cron_auth_configured: Boolean(SB_UK_AQ_CRON_SECRET),
    });
    return new Response("Unauthorized", { status: 401 });
  }
  return null;
}

async function postgrestRequest<T>(
  method: string,
  table: string,
  params?: Record<string, string>,
  body?: unknown,
  prefer?: string,
  schema?: string,
): Promise<{
  data: T | null;
  error: {
    message: string;
    code?: string | null;
    details?: string | null;
    hint?: string | null;
    http_status?: number | null;
  } | null;
}> {
  if (!REST_BASE_URL || !SUPABASE_PRIVILEGED_KEY) {
    return { data: null, error: { message: "Missing SUPABASE_URL or SB_SECRET_KEY." } };
  }
  const url = new URL(`${REST_BASE_URL}/${table}`);
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  const resp = await postgrestFetch(url.toString(), {
    method,
    headers: postgrestHeaders(prefer, schema),
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  let payload: unknown = null;
  if (resp.status !== 204) {
    const contentType = resp.headers.get("content-type") ?? "";
    payload = contentType.includes("application/json") ? await resp.json() : await resp.text();
  }
  if (!resp.ok) {
    const message = (payload as { message?: string; error_description?: string; error?: string })?.message
      ?? (payload as { error_description?: string })?.error_description
      ?? (payload as { error?: string })?.error
      ?? resp.statusText;
    const errorPayload = payload && typeof payload === "object"
      ? payload as Record<string, unknown>
      : {};
    return {
      data: null,
      error: {
        message: String(message),
        code: errorPayload.code == null ? null : String(errorPayload.code),
        details: errorPayload.details == null
          ? null
          : String(errorPayload.details),
        hint: errorPayload.hint == null ? null : String(errorPayload.hint),
        http_status: resp.status,
      },
    };
  }
  return { data: payload as T, error: null };
}

async function publicRpcRequest<T>(
  fn: string,
  args?: Record<string, unknown>,
): Promise<{ data: T | null; error: { message: string } | null }> {
  return await postgrestRequest<T>(
    "POST",
    `rpc/${fn}`,
    undefined,
    args ?? {},
    undefined,
    "uk_aq_public",
  );
}

function asString(value: unknown, fallback?: string): string | undefined {
  if (value === null || value === undefined) {
    return fallback;
  }
  const trimmed = String(value).trim();
  return trimmed ? trimmed : fallback;
}

function asNumber(value: unknown, fallback?: number): number | undefined {
  if (value === null || value === undefined) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function asBoolean(value: unknown, fallback?: boolean): boolean | undefined {
  if (value === null || value === undefined) {
    return fallback;
  }
  if (typeof value === "boolean") {
    return value;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function normalizeListSensors(payload: unknown): Record<string, unknown>[] {
  if (Array.isArray(payload) && payload.length > 0 && Array.isArray(payload[0])) {
    payload = payload[0];
  }
  if (Array.isArray(payload)) {
    return payload.filter((row) => row && typeof row === "object") as Record<string, unknown>[];
  }
  return [];
}

function coerceFloat(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function maybeSwapCoords(lon: number | null, lat: number | null): [number | null, number | null] {
  if (lon === null || lat === null) {
    return [lon, lat];
  }
  const swapped = (
    UK_BBOX.south <= lon && lon <= UK_BBOX.north
    && UK_BBOX.west <= lat && lat <= UK_BBOX.east
    && !(UK_BBOX.west <= lon && lon <= UK_BBOX.east)
    && !(UK_BBOX.south <= lat && lat <= UK_BBOX.north)
  );
  if (swapped) {
    return [lat, lon];
  }
  return [lon, lat];
}

function stationGeometry(lon: number | null, lat: number | null): string | null {
  if (lon === null || lat === null) {
    return null;
  }
  return `SRID=4326;POINT(${lon} ${lat})`;
}

function stationMetadataAttributes(station: Record<string, unknown>): Record<string, unknown> {
  const attributes: Record<string, unknown> = {};
  const fields: Array<[string, string]> = [
    ["Enabled", "enabled"],
    ["SiteActive", "site_active"],
    ["OrganisationName", "organisation_name"],
    ["SponsorName", "sponsor_name"],
    ["DeviceCode", "device_code"],
    ["SiteDescription", "site_description"],
    ["SitePhotoURL", "site_photo_url"],
    ["BatteryStatus", "battery_status"],
    ["BatteryPercentage", "battery_percentage"],
    ["SignalStrength", "signal_strength"],
    ["SensorsHealthStatus", "sensors_health_status"],
    ["OverallStatus", "overall_status"],
    ["PowerTag", "power_tag"],
    ["OtherTags", "other_tags"],
    ["Indoor", "indoor"],
    ["HeadHeight", "head_height"],
    ["ToRoad", "to_road"],
  ];
  for (const [source, target] of fields) {
    if (station[source] !== undefined && station[source] !== null) {
      attributes[target] = station[source];
    }
  }
  return attributes;
}

function normalizeStationPayload(
  station: Record<string, unknown>,
  connectorId: string,
  serviceRef: string,
): { row: Record<string, unknown>; metadata: Record<string, unknown> } {
  const siteCode = asString(station.SiteCode) ?? null;
  const siteName = asString(station.SiteName);
  const lon = coerceFloat(station.Longitude);
  const lat = coerceFloat(station.Latitude);
  const [lonVal, latVal] = maybeSwapCoords(lon, lat);

  const row = {
    station_ref: siteCode,
    service_ref: serviceRef,
    label: siteName ?? siteCode ?? "Breathe London Station",
    station_name: siteName ?? null,
    station_type: asString(station.SiteClassification) ?? null,
    station_exposure: asString(station.SiteLocationType) ?? null,
    region: asString(station.SiteGroup) ?? null,
    geometry: stationGeometry(lonVal, latVal),
    first_seen_at: asString(station.StartDate) ?? null,
    last_seen_at: asString(station.LastCommunication) ?? null,
    removed_at: asString(station.EndDate) ?? null,
    connector_id: connectorId,
  };
  return { row, metadata: stationMetadataAttributes(station) };
}

function parseSpeciesList(value: string | string[] | undefined | null): string[] {
  const raw = Array.isArray(value) ? value.join(",") : (value ?? "");
  const items = raw.split(",").map((item) => item.trim().toUpperCase()).filter(Boolean);
  return items.filter((item) => Object.hasOwn(SPECIES_CONFIG, item));
}

function parseStationRefs(value: string | string[] | undefined | null): string[] {
  const raw = Array.isArray(value) ? value.join(",") : (value ?? "");
  return raw.split(",").map((item) => item.trim()).filter(Boolean);
}

function parseStartDate(value: string | undefined | null): Date | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const hasTime = trimmed.includes("T") || trimmed.includes(" ");
  if (!hasTime) {
    const parsed = Date.parse(`${trimmed}T00:00:00Z`);
    return Number.isNaN(parsed) ? null : new Date(parsed);
  }
  let candidate = trimmed;
  if (candidate.includes(" ") && !candidate.includes("T")) {
    candidate = candidate.replace(" ", "T");
  }
  if (!candidate.endsWith("Z") && !candidate.includes("+")) {
    candidate = `${candidate}Z`;
  }
  const parsed = Date.parse(candidate);
  return Number.isNaN(parsed) ? null : new Date(parsed);
}

function floorToHour(value: Date): Date {
  return new Date(Date.UTC(
    value.getUTCFullYear(),
    value.getUTCMonth(),
    value.getUTCDate(),
    value.getUTCHours(),
    0,
    0,
    0,
  ));
}

function formatClarityTimestamp(value: Date): string {
  const weekdays = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const pad = (num: number) => String(num).padStart(2, "0");
  return `${weekdays[value.getUTCDay()]} ${pad(value.getUTCDate())} ${months[value.getUTCMonth()]} ${value.getUTCFullYear()} ${pad(value.getUTCHours())}:${pad(value.getUTCMinutes())}:${pad(value.getUTCSeconds())} GMT`;
}

function parseObservationTimestamp(value: unknown): string | null {
  const text = asString(value);
  if (!text) {
    return null;
  }
  const candidate = text.endsWith("Z") || text.includes("+") ? text : text.replace(" ", "T") + "Z";
  const parsed = Date.parse(candidate);
  if (Number.isNaN(parsed)) {
    return null;
  }
  return new Date(parsed).toISOString();
}

function maxTimestampIso(current: string | null, candidate: string | null): string | null {
  if (!candidate) {
    return current;
  }
  if (!current) {
    return candidate;
  }
  const currentMs = Date.parse(current);
  const candidateMs = Date.parse(candidate);
  if (!Number.isFinite(currentMs)) {
    return candidate;
  }
  if (!Number.isFinite(candidateMs)) {
    return current;
  }
  return candidateMs > currentMs ? candidate : current;
}

function quotePostgrestValue(value: string): string {
  const escaped = value
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/,/g, "\\,")
    .replace(/\(/g, "\\(")
    .replace(/\)/g, "\\)")
    .replace(/\r/g, "\\r")
    .replace(/\n/g, "\\n");
  return `"${escaped}"`;
}

function postgrestIn(values: string[]): string {
  return `in.(${values.map(quotePostgrestValue).join(",")})`;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function appendSample(values: number[] | null, value: number, maxSamples = 30): number[] {
  const cleaned = Array.isArray(values) ? values.filter((v) => Number.isFinite(v)) : [];
  const next = [...cleaned, value].slice(-maxSamples);
  return next;
}

function minSeconds(values: number[] | null): number | null {
  if (!Array.isArray(values) || values.length === 0) {
    return null;
  }
  let minValue = Number.POSITIVE_INFINITY;
  for (const value of values) {
    if (!Number.isFinite(value)) {
      continue;
    }
    const rounded = Math.max(0, Math.round(value));
    if (rounded < minValue) {
      minValue = rounded;
    }
  }
  if (!Number.isFinite(minValue)) {
    return null;
  }
  return minValue;
}

async function fetchJson(
  url: string,
  headers: Record<string, string>,
  rawRecorder?: RawRecorder | null,
): Promise<unknown> {
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
    try {
      const resp = await fetch(url, { headers, signal: controller.signal });
      if (RETRYABLE_STATUS.has(resp.status) && attempt < 3) {
        await sleep(Math.min(30_000, 2 ** attempt * 1000));
        continue;
      }
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`HTTP ${resp.status}: ${text}`);
      }
      const payload = await resp.json();
      if (rawRecorder) {
        const parsed = new URL(url);
        const params: Record<string, string> = {};
        parsed.searchParams.forEach((value, key) => {
          const normalized = key.trim().toLowerCase();
          params[key] = (normalized === "key" || normalized === "api_key") ? "redacted" : value;
        });
        rawRecorder.recordResponse(parsed.pathname, params, resp.status, payload);
      }
      return payload;
    } finally {
      clearTimeout(timeout);
    }
  }
  return [];
}

async function listSensors(
  baseUrl: string,
  apiKey: string,
  rawRecorder?: RawRecorder | null,
): Promise<Record<string, unknown>[]> {
  const url = new URL(`${baseUrl}/ListSensors`);
  url.searchParams.set("key", apiKey);
  const payload = await fetchJson(url.toString(), { "User-Agent": BLONDON_COMMUNITIES_USER_AGENT }, rawRecorder);
  return normalizeListSensors(payload);
}

async function getClarityData(
  baseUrl: string,
  apiKey: string,
  siteCode: string,
  species: string,
  startTime: Date,
  endTime: Date,
  rawRecorder?: RawRecorder | null,
): Promise<unknown> {
  const start = encodeURIComponent(formatClarityTimestamp(startTime));
  const end = encodeURIComponent(formatClarityTimestamp(endTime));
  const url = new URL(
    `${baseUrl}/getClarityData/${siteCode}/${species}/${start}/${end}/Hourly`,
  );
  url.searchParams.set("key", apiKey);
  return await fetchJson(url.toString(), { "User-Agent": BLONDON_COMMUNITIES_USER_AGENT }, rawRecorder);
}

async function loadConnector(
  connectorId: string | undefined,
  connectorCode: string,
  _connectorLabel: string,
  _serviceUrl: string,
): Promise<ConnectorRow | null> {
  const select = "id,connector_code,label,service_url";
  if (connectorId) {
    const { data } = await postgrestRequest<ConnectorRow[]>("GET", "connectors", {
      select,
      id: `eq.${connectorId}`,
      limit: "1",
    });
    if (data && data[0]) {
      return data[0];
    }
  }
  const { data: existing } = await postgrestRequest<ConnectorRow[]>("GET", "connectors", {
    select,
    connector_code: `eq.${connectorCode}`,
    limit: "1",
  });
  if (existing && existing[0]) {
    return existing[0];
  }
  return null;
}

async function upsertStations(rows: Record<string, unknown>[]): Promise<number> {
  const payload = rows.filter((row) => row.station_ref);
  if (!payload.length) {
    return 0;
  }
  await postgrestRequest(
    "POST",
    "stations",
    { on_conflict: "connector_id,service_ref,station_ref" },
    payload,
    "resolution=merge-duplicates,return=minimal",
  );
  return payload.length;
}

async function fetchStationsFromDb(
  connectorId: string,
  serviceRef: string,
  limit?: number,
  activeOnly = false,
  stationRefs: string[] = [],
): Promise<Array<{ id: number; station_ref: string; station_name: string | null; label: string | null }>> {
  const rows: Array<{ id: number; station_ref: string; station_name: string | null; label: string | null }> = [];
  const pageSize = 1000;
  const maxRows = limit && Number.isFinite(limit) ? Math.max(0, Math.floor(limit)) : null;
  let offset = 0;

  while (true) {
    const refFilter = stationRefs.length ? postgrestIn(stationRefs) : null;
    if (refFilter && offset > 0) {
      break;
    }
    if (maxRows !== null && rows.length >= maxRows) {
      break;
    }
    const remaining = maxRows !== null ? maxRows - rows.length : pageSize;
    const pageLimit = refFilter
      ? Math.min(pageSize, remaining, stationRefs.length)
      : Math.min(pageSize, remaining);
    if (pageLimit <= 0) {
      break;
    }
    const select = activeOnly
      ? "id,station_ref,station_name,label,removed_at"
      : "id,station_ref,station_name,label";
    const params: Record<string, string> = {
      select,
      connector_id: `eq.${connectorId}`,
      service_ref: `eq.${serviceRef}`,
      order: "station_ref.asc",
      limit: String(pageLimit),
      offset: String(offset),
    };
    if (refFilter) {
      params.station_ref = refFilter;
    }
    const { data, error } = await postgrestRequest<
      Array<{
        id: number;
        station_ref: string;
        station_name: string | null;
        label: string | null;
        removed_at?: string | null;
      }>
    >(
      "GET",
      "stations",
      params,
    );
    if (error) {
      throw new Error(`Failed to load stations from Supabase: ${error.message}`);
    }
    const batch = data ?? [];
    if (activeOnly) {
      for (const row of batch) {
        if (row.removed_at) {
          continue;
        }
        rows.push({
          id: row.id,
          station_ref: row.station_ref,
          station_name: row.station_name ?? null,
          label: row.label ?? null,
        });
      }
    } else {
      rows.push(...batch);
    }
    if (batch.length < pageLimit) {
      break;
    }
    offset += pageLimit;
  }

  return rows;
}

async function fetchStationIdsByRef(
  connectorId: string,
  serviceRef: string,
  stationRefs: string[],
): Promise<Record<string, number>> {
  const refs = stationRefs.filter(Boolean);
  if (!refs.length) {
    return {};
  }
  const mapping: Record<string, number> = {};
  for (let idx = 0; idx < refs.length; idx += 200) {
    const chunk = refs.slice(idx, idx + 200);
    const { data } = await postgrestRequest<Array<{ id: number; station_ref: string }>>(
      "GET",
      "stations",
      {
        select: "id,station_ref",
        connector_id: `eq.${connectorId}`,
        service_ref: `eq.${serviceRef}`,
        station_ref: postgrestIn(chunk),
      },
    );
    for (const row of data ?? []) {
      mapping[String(row.station_ref)] = Number(row.id);
    }
  }
  return mapping;
}

async function fetchStationMetadata(
  stationIds: number[],
): Promise<Record<number, Record<string, unknown>>> {
  if (!stationIds.length) {
    return {};
  }
  const metadata: Record<number, Record<string, unknown>> = {};
  for (let idx = 0; idx < stationIds.length; idx += 200) {
    const chunk = stationIds.slice(idx, idx + 200).map(String);
    const { data } = await postgrestRequest<Array<{ station_id: number; attributes: Record<string, unknown> }>>(
      "GET",
      "station_metadata",
      {
        select: "station_id,attributes",
        station_id: postgrestIn(chunk),
      },
    );
    for (const row of data ?? []) {
      if (row && row.attributes && typeof row.attributes === "object") {
        metadata[Number(row.station_id)] = row.attributes;
      }
    }
  }
  return metadata;
}

async function upsertStationMetadata(
  attributesByStation: Record<number, Record<string, unknown>>,
): Promise<number> {
  const stationIds = Object.keys(attributesByStation).map(Number);
  if (!stationIds.length) {
    return 0;
  }
  const existing = await fetchStationMetadata(stationIds);
  const rows: Record<string, unknown>[] = [];
  const timestamp = new Date().toISOString();
  for (const stationId of stationIds) {
    const merged = { ...(existing[stationId] ?? {}), ...(attributesByStation[stationId] ?? {}) };
    if (Object.keys(merged).length === 0) {
      continue;
    }
    rows.push({ station_id: stationId, attributes: merged, updated_at: timestamp });
  }
  if (!rows.length) {
    return 0;
  }
  await postgrestRequest(
    "POST",
    "station_metadata",
    { on_conflict: "station_id" },
    rows,
    "resolution=merge-duplicates,return=minimal",
  );
  return rows.length;
}

async function fetchPhenomenaIds(
  connectorId: string,
  speciesList: string[],
): Promise<Record<string, number>> {
  if (!speciesList.length) {
    return {};
  }
  const sourceLabels = speciesList.map((species) => SPECIES_CONFIG[species].source_label);
  const { data, error } = await publicRpcRequest<
    Array<{ id: number; source_label?: string; eionet_uri?: string }>
  >(
    "uk_aq_rpc_phenomena_ids",
    {
      connector_id: Number(connectorId),
      eionet_uris: sourceLabels,
    },
  );
  if (error) {
    throw new Error(`Phenomena id lookup failed: ${error.message}`);
  }
  const mapping: Record<string, number> = {};
  for (const row of data ?? []) {
    const sourceLabel = row.source_label ?? row.eionet_uri;
    if (sourceLabel) {
      mapping[String(sourceLabel)] = Number(row.id);
    }
  }
  return mapping;
}

async function upsertPhenomena(connectorId: string, speciesList: string[]): Promise<Record<string, number>> {
  const payload = speciesList.map((species) => {
    const config = SPECIES_CONFIG[species];
    return {
      connector_id: connectorId,
      label: config.label,
      source_label: config.source_label,
      notation: config.notation,
      pollutant_label: config.pollutant_label,
      observed_property_code: config.observed_property_code,
      observed_property_display_name: config.label,
      observed_property_domain: config.observed_property_domain,
      canonical_uom: config.uom,
    };
  });
  const { error } = await publicRpcRequest<Array<{ phenomena_upserted: number }>>(
    "uk_aq_rpc_phenomena_upsert",
    { rows: payload },
  );
  if (error) {
    throw new Error(`Phenomena upsert failed: ${error.message}`);
  }
  return await fetchPhenomenaIds(connectorId, speciesList);
}

async function fetchTimeseriesIds(
  connectorId: string,
  serviceRef: string,
  timeseriesRefs: string[],
): Promise<Record<string, number>> {
  const refs = timeseriesRefs.filter(Boolean);
  if (!refs.length) {
    return {};
  }
  const mapping: Record<string, number> = {};
  for (let idx = 0; idx < refs.length; idx += 200) {
    const chunk = refs.slice(idx, idx + 200);
    const { data } = await postgrestRequest<Array<{ id: number; timeseries_ref: string }>>(
      "GET",
      "timeseries",
      {
        select: "id,timeseries_ref",
        connector_id: `eq.${connectorId}`,
        service_ref: `eq.${serviceRef}`,
        timeseries_ref: postgrestIn(chunk),
      },
    );
    for (const row of data ?? []) {
      mapping[String(row.timeseries_ref)] = Number(row.id);
    }
  }
  return mapping;
}

async function fetchStationCheckpoints(
  stationIds: number[],
): Promise<Record<number, Record<string, unknown>>> {
  if (!stationIds.length) {
    return {};
  }
  const checkpoints: Record<number, Record<string, unknown>> = {};
  for (let idx = 0; idx < stationIds.length; idx += 200) {
    const chunk = stationIds.slice(idx, idx + 200).map(String);
    const { data } = await postgrestRequest<Array<Record<string, unknown>>>(
      "GET",
      "blondon_communities_station_checkpoints",
      {
        select: "station_id,next_due_at,last_observed_at,ingest_lag_samples,last_polled_at",
        station_id: postgrestIn(chunk),
      },
      undefined,
      undefined,
      UK_AQ_RAW_SCHEMA,
    );
    for (const row of data ?? []) {
      const stationId = Number(row.station_id);
      if (!Number.isFinite(stationId)) {
        continue;
      }
      checkpoints[stationId] = row;
    }
  }
  return checkpoints;
}

async function upsertStationCheckpoints(rows: Record<string, unknown>[]): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  await postgrestRequest(
    "POST",
    "blondon_communities_station_checkpoints",
    { on_conflict: "station_id" },
    rows,
    "resolution=merge-duplicates,return=minimal",
    UK_AQ_RAW_SCHEMA,
  );
  return rows.length;
}

async function upsertObservations(
  rows: Record<string, unknown>[],
  runtimeBudget?: { shouldStop: () => boolean; remainingRuntimeMs: () => number },
) {
  return await writeIngestDbObservations({
    rows,
    chunkSize: rows.length || 1,
    connectorCode: BLONDON_COMMUNITIES_CONNECTOR_CODE,
    logger: console,
    runtimeBudget,
    config: { minimumAttemptRuntimeMs: DEFAULT_TIMEOUT_MS },
    requestBodyBytes: (chunk: Record<string, unknown>[]) =>
      serializedJsonUtf8Bytes(buildCompactObservationRpcArgs(chunk)),
    writeChunk: async (chunk: Record<string, unknown>[]) => {
      const { error } = await postgrestRequest(
        "POST",
        "rpc/uk_aq_rpc_observations_compact_upsert_v1",
        {},
        buildCompactObservationRpcArgs(chunk),
        undefined,
        "uk_aq_public",
      );
      if (error) throw error;
    },
  });
}

function observationValueDedupeToken(value: unknown): string {
  if (value === null || value === undefined) {
    return "null";
  }
  const numericValue = Number(value);
  if (Number.isFinite(numericValue)) {
    return `n:${numericValue}`;
  }
  return `s:${String(value)}`;
}

function observationStatusDedupeToken(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function dedupeExactObservationRows(
  rows: Record<string, unknown>[],
): { rows: Record<string, unknown>[]; deduped: number } {
  if (!rows.length) {
    return { rows: [], deduped: 0 };
  }
  const dedup = new Map<string, Record<string, unknown>>();
  for (const row of rows) {
    const connectorId = String(row.connector_id ?? "");
    const timeseriesId = String(row.timeseries_id ?? "");
    const observedAt = String(row.observed_at ?? "").trim();
    const valueToken = observationValueDedupeToken(row.value);
    const statusToken = observationStatusDedupeToken(row.status);
    const key =
      `${connectorId}:${timeseriesId}:${observedAt}:${valueToken}:${statusToken}`;
    if (!dedup.has(key)) {
      dedup.set(key, row);
    }
  }
  const preparedRows = Array.from(dedup.values());
  return { rows: preparedRows, deduped: rows.length - preparedRows.length };
}

function toObservsObservationRows(
  rows: Record<string, unknown>[],
  connectorId: number,
  timeseriesId: number,
): ObservsObservationRow[] {
  const observsRows: ObservsObservationRow[] = [];
  for (const row of rows) {
    const observedAt = asString(row.observed_at);
    if (!observedAt) {
      continue;
    }
    const numericValue = Number(row.value);
    observsRows.push({
      connector_id: connectorId,
      timeseries_id: timeseriesId,
      observed_at: observedAt,
      value: Number.isFinite(numericValue) ? numericValue : null,
      status: asString(row.status) ?? null,
    });
  }
  return observsRows;
}

async function updateTimeseriesLastValues(
  rows: Array<{ id: number; last_value: number; last_value_at: string }>,
  errors: string[],
): Promise<number> {
  if (!rows.length) return 0;
  const { data, error } = await postgrestRequest<Array<{ timeseries_updated: number }>>(
    "POST",
    "rpc/uk_aq_rpc_timeseries_last_values_compact_update_v1",
    {},
    {
      timeseries_ids: rows.map((row) => row.id),
      last_value_ats: rows.map((row) => row.last_value_at),
      last_values: rows.map((row) => row.last_value),
    },
    undefined,
    "uk_aq_public",
  );
  if (error) {
    const message = `timeseries update failed: ${error.message}`;
    errors.push(message);
    console.warn(message);
    return 0;
  }
  return data?.[0]?.timeseries_updated ?? 0;
}

function extractObservations(
  payload: unknown,
  timeseriesId: number,
  connectorId: number,
): { rows: Record<string, unknown>[]; lastObserved: string | null; lastValue: number | null } {
  const rows: Record<string, unknown>[] = [];
  let lastObserved: string | null = null;
  let lastValue: number | null = null;
  if (Array.isArray(payload) && payload.length > 0 && Array.isArray(payload[0])) {
    payload = payload[0];
  }
  if (!Array.isArray(payload)) {
    return { rows, lastObserved, lastValue };
  }
  for (const entry of payload) {
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const observedAt = parseObservationTimestamp(entry.DateTime);
    const value = coerceFloat(entry.ScaledValue);
    if (!observedAt || value === null) {
      continue;
    }
    rows.push({ connector_id: connectorId, timeseries_id: timeseriesId, observed_at: observedAt, value });
    if (!lastObserved || observedAt > lastObserved) {
      lastObserved = observedAt;
      lastValue = value;
    }
  }
  return { rows, lastObserved, lastValue };
}

function createLogBuffer(): LogBuffer {
  const lines: string[] = [];
  const push = (level: string, message: string, context?: Record<string, unknown>) => {
    const timestamp = new Date().toISOString();
    const base = `${timestamp} ${level} ${message}`;
    lines.push(context ? `${base} ${formatContext(context)}` : base);
  };
  return {
    lines,
    info: (message, context) => push("INFO", message, context),
    warn: (message, context) => push("WARN", message, context),
    error: (message, context) => push("ERROR", message, context),
  };
}

function formatContext(context: Record<string, unknown>): string {
  return Object.entries(context)
    .map(([key, value]) => `${key}=${formatLogValue(value)}`)
    .join(" ");
}

function formatLogValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "null";
  }
  if (Array.isArray(value)) {
    return `[${value.map((entry) => formatLogValue(entry)).join(",")}]`;
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function createRawRecorder(): RawRecorder {
  const lines: string[] = [];
  const write = (entry: Record<string, unknown>) => {
    lines.push(JSON.stringify(entry));
  };
  const recorder: RawRecorder = {
    lines,
    responseCount: 0,
    recordEvent: (name, payload) => {
      write({
        type: name,
        recorded_at: new Date().toISOString(),
        payload,
      });
    },
    recordResponse: (path, params, statusCode, payload) => {
      recorder.responseCount += 1;
      write({
        type: "response",
        fetched_at: new Date().toISOString(),
        path,
        params,
        status_code: statusCode,
        payload,
      });
    },
  };
  write({ type: "meta", created_at: new Date().toISOString() });
  return recorder;
}

function loadDropboxConfig(): DropboxConfig | null {
  if (!DROPBOX_APP_KEY || !DROPBOX_APP_SECRET || !DROPBOX_REFRESH_TOKEN) {
    return null;
  }
  return {
    appKey: DROPBOX_APP_KEY,
    appSecret: DROPBOX_APP_SECRET,
    refreshToken: DROPBOX_REFRESH_TOKEN,
  };
}

function rawDropboxUploadsEnabled(): boolean {
  return Boolean(
    SUPABASE_URL &&
      DROPBOX_ALLOWED_SUPABASE_URL &&
      DROPBOX_ALLOWED_SUPABASE_URL === SUPABASE_URL,
  );
}

function loadErrorDropboxConfig(): DropboxConfig | null {
  if (!DROPBOX_APP_KEY || !DROPBOX_APP_SECRET || !DROPBOX_REFRESH_TOKEN) {
    return null;
  }
  if (DROPBOX_ERROR_ALLOWED_SUPABASE_URL && DROPBOX_ERROR_ALLOWED_SUPABASE_URL !== SUPABASE_URL) {
    return null;
  }
  return {
    appKey: DROPBOX_APP_KEY,
    appSecret: DROPBOX_APP_SECRET,
    refreshToken: DROPBOX_REFRESH_TOKEN,
  };
}

function normalizeDropboxPath(raw: string): string {
  const cleaned = raw.trim();
  if (!cleaned) {
    return "";
  }
  const rooted = cleaned.startsWith("/") ? cleaned : `/${cleaned}`;
  return rooted.replace(/\/$/, "");
}

function dropboxWithRoot(path: string): string {
  const cleaned = normalizeDropboxPath(path);
  if (!DROPBOX_ROOT_FOLDER) {
    return cleaned;
  }
  if (!cleaned) {
    return DROPBOX_ROOT_FOLDER;
  }
  if (cleaned === DROPBOX_ROOT_FOLDER || cleaned.startsWith(`${DROPBOX_ROOT_FOLDER}/`)) {
    return cleaned;
  }
  return `${DROPBOX_ROOT_FOLDER}${cleaned}`;
}

function buildDropboxDiagnostics(): DropboxDiagnostics {
  const hasAppKey = Boolean(DROPBOX_APP_KEY);
  const hasAppSecret = Boolean(DROPBOX_APP_SECRET);
  const hasRefreshToken = Boolean(DROPBOX_REFRESH_TOKEN);
  const hasCreds = hasAppKey && hasAppSecret && hasRefreshToken;
  const supabaseUrl = SUPABASE_URL || null;
  const rawAllowed = DROPBOX_ALLOWED_SUPABASE_URL || null;
  const errorAllowed = DROPBOX_ERROR_ALLOWED_SUPABASE_URL || null;
  const rawAllowedMatch = Boolean(rawAllowed) && rawAllowed === SUPABASE_URL;
  const errorAllowedMatch = !errorAllowed || errorAllowed === SUPABASE_URL;

  let logReason: string | null = null;
  let rawReason: string | null = null;
  if (!SUPABASE_URL) {
    logReason = "missing_supabase_url";
  } else if (!hasCreds) {
    logReason = "missing_dropbox_credentials";
  }

  if (!SUPABASE_URL) {
    rawReason = "missing_supabase_url";
  } else if (!hasCreds) {
    rawReason = "missing_dropbox_credentials";
  } else if (!rawAllowed) {
    rawReason = "missing_dropbox_allowed_supabase_url";
  } else if (!rawAllowedMatch) {
    rawReason = "dropbox_allowed_supabase_url_mismatch";
  }

  return {
    enabled: logReason === null,
    reason: logReason,
    raw_enabled: rawReason === null,
    raw_reason: rawReason,
    has_app_key: hasAppKey,
    has_app_secret: hasAppSecret,
    has_refresh_token: hasRefreshToken,
    supabase_url: supabaseUrl,
    raw_allowed_supabase_url: rawAllowed,
    raw_allowed_match: rawAllowedMatch,
    error_allowed_supabase_url: errorAllowed,
    error_allowed_match: errorAllowedMatch,
    dropbox_root: DROPBOX_ROOT_FOLDER || null,
  };
}

function normalizeConnectorPrefix(connectorCode: string | null): string {
  const cleaned = (connectorCode ?? "").trim().toLowerCase();
  const normalized = cleaned.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return normalized || "blondon_communities";
}

function buildDropboxLogPath(
  connectorCode: string | null,
  timestamp: Date,
): string {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_LOG_FOLDER}/${dateFolder}/uk_aq_log_${DROPBOX_UPLOAD_SOURCE}_${prefix}_${stamp}.log`;
}

function buildDropboxRawPath(
  connectorCode: string | null,
  timestamp: Date,
): string {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_RAW_FOLDER}/${dateFolder}/uk_aq_raw_${DROPBOX_UPLOAD_SOURCE}_${prefix}_${stamp}.zip`;
}

function buildDropboxErrorPath(
  errorId: string,
  createdAt: string,
  connectorCode: string | null,
): string {
  const dateFolder = createdAt.slice(0, 10);
  const stamp = formatCompactTimestamp(new Date(createdAt));
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_ERROR_FOLDER}/${dateFolder}/uk_aq_error_${DROPBOX_UPLOAD_SOURCE}_${prefix}_${stamp}_${errorId}.json`;
}

function formatCompactTimestamp(timestamp: Date): string {
  return timestamp.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
}

function formatDateYmd(timestamp: Date): string {
  return timestamp.toISOString().slice(0, 10);
}

class DropboxHttpError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

async function dropboxRefreshAccessToken(config: DropboxConfig): Promise<string> {
  const payload = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: config.refreshToken,
    client_id: config.appKey,
    client_secret: config.appSecret,
  });
  const resp = await fetch(DROPBOX_TOKEN_URL, {
    method: "POST",
    body: payload,
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });
  if (!resp.ok) {
    throw new Error(`Dropbox token request failed (${resp.status})`);
  }
  const data = await resp.json();
  const token = data?.access_token;
  if (!token) {
    throw new Error("Dropbox token response missing access_token.");
  }
  return token;
}

async function dropboxUploadFile(
  accessToken: string,
  path: string,
  contents: string | Uint8Array,
): Promise<void> {
  const body = typeof contents === "string" ? new TextEncoder().encode(contents) : Uint8Array.from(contents);
  const resp = await fetch(DROPBOX_UPLOAD_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Dropbox-API-Arg": JSON.stringify({
        path,
        mode: "add",
        autorename: true,
        mute: false,
      }),
      "Content-Type": "application/octet-stream",
    },
    body,
  });
  if (!resp.ok) {
    throw new DropboxHttpError(`Dropbox upload failed (${resp.status})`, resp.status);
  }
}

async function dropboxUploadFileWithRetry(
  accessToken: string,
  path: string,
  contents: string | Uint8Array,
  refreshToken?: () => Promise<string>,
): Promise<string> {
  try {
    await dropboxUploadFile(accessToken, path, contents);
    return accessToken;
  } catch (err) {
    if (err instanceof DropboxHttpError && err.status === 401 && refreshToken) {
      const refreshed = await refreshToken();
      await dropboxUploadFile(refreshed, path, contents);
      return refreshed;
    }
    throw err;
  }
}

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < table.length; i += 1) {
    let c = i;
    for (let k = 0; k < 8; k += 1) {
      c = (c & 1) ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[i] = c >>> 0;
  }
  return table;
})();

function crc32(data: Uint8Array): number {
  let crc = 0xffffffff;
  for (const byte of data) {
    const idx = (crc ^ byte) & 0xff;
    crc = CRC_TABLE[idx] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function toDosDateTime(date: Date): { dosTime: number; dosDate: number } {
  const year = Math.max(1980, date.getUTCFullYear());
  const month = date.getUTCMonth() + 1;
  const day = date.getUTCDate();
  const hour = date.getUTCHours();
  const minute = date.getUTCMinutes();
  const second = date.getUTCSeconds();
  const dosTime = (hour << 11) | (minute << 5) | Math.floor(second / 2);
  const dosDate = ((year - 1980) << 9) | (month << 5) | day;
  return { dosTime, dosDate };
}

async function zipTextCompressed(filename: string, content: string): Promise<Uint8Array> {
  const encoder = new TextEncoder();
  const data = encoder.encode(content);
  const nameBytes = encoder.encode(filename);
  const crc = crc32(data);
  const fileSize = data.length;
  const compressed = await deflateRaw(data);
  const compressedSize = compressed.length;
  const { dosTime, dosDate } = toDosDateTime(new Date());

  const header: number[] = [];
  const push16 = (value: number) => {
    header.push(value & 0xff, (value >>> 8) & 0xff);
  };
  const push32 = (value: number) => {
    header.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, (value >>> 24) & 0xff);
  };

  push32(0x04034b50);
  push16(20);
  push16(0);
  push16(8);
  push16(dosTime);
  push16(dosDate);
  push32(crc);
  push32(compressedSize);
  push32(fileSize);
  push16(nameBytes.length);
  push16(0);

  const localHeader = new Uint8Array([...header, ...nameBytes]);
  const localOffset = 0;
  const centralOffset = localHeader.length + compressedSize;

  const central: number[] = [];
  const c16 = (value: number) => {
    central.push(value & 0xff, (value >>> 8) & 0xff);
  };
  const c32 = (value: number) => {
    central.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, (value >>> 24) & 0xff);
  };

  c32(0x02014b50);
  c16(20);
  c16(20);
  c16(0);
  c16(8);
  c16(dosTime);
  c16(dosDate);
  c32(crc);
  c32(compressedSize);
  c32(fileSize);
  c16(nameBytes.length);
  c16(0);
  c16(0);
  c16(0);
  c16(0);
  c32(0);
  c32(localOffset);

  const centralHeader = new Uint8Array([...central, ...nameBytes]);
  const centralSize = centralHeader.length;

  const end: number[] = [];
  const e16 = (value: number) => {
    end.push(value & 0xff, (value >>> 8) & 0xff);
  };
  const e32 = (value: number) => {
    end.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, (value >>> 24) & 0xff);
  };

  e32(0x06054b50);
  e16(0);
  e16(0);
  e16(1);
  e16(1);
  e32(centralSize);
  e32(centralOffset);
  e16(0);

  const endHeader = new Uint8Array(end);
  const output = new Uint8Array(
    localHeader.length + compressedSize + centralHeader.length + endHeader.length,
  );
  output.set(localHeader, 0);
  output.set(compressed, localHeader.length);
  output.set(centralHeader, localHeader.length + compressedSize);
  output.set(endHeader, localHeader.length + compressedSize + centralHeader.length);
  return output;
}

async function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([Uint8Array.from(data)]).stream().pipeThrough(new CompressionStream("deflate-raw"));
  const buffer = await new Response(stream).arrayBuffer();
  return new Uint8Array(buffer);
}

async function uploadDropboxLog(
  accessToken: string,
  log: LogBuffer,
  connectorId: string | null,
  connectorCode: string | null,
  errorLogger: { logError: (entry: ErrorLogEntry) => Promise<void> },
  refreshToken?: () => Promise<string>,
): Promise<string> {
  if (!accessToken || log.lines.length === 0) {
    return accessToken;
  }
  const content = log.lines.join("\n") + "\n";
  if (!content.trim()) {
    return accessToken;
  }
  const logPath = buildDropboxLogPath(connectorCode, new Date());
  try {
    return await dropboxUploadFileWithRetry(accessToken, logPath, content, refreshToken);
  } catch (err) {
    console.warn("Dropbox log upload failed:", err);
    await errorLogger.logError({
      source: "edge",
      severity: "error",
      message: "Dropbox log upload failed.",
      stack: err instanceof Error ? err.stack : undefined,
      context: {
        connector_id: connectorId,
        connector_code: connectorCode,
        dropbox_path: logPath,
        dropbox_status: err instanceof DropboxHttpError ? err.status : null,
        error: err instanceof Error ? err.message : String(err),
      },
      connector_code: connectorCode ?? null,
      connector_id: connectorId ?? null,
    });
  }
  return accessToken;
}

async function uploadDropboxRaw(
  accessToken: string,
  recorder: RawRecorder | null,
  connectorId: string | null,
  connectorCode: string | null,
  errorLogger: { logError: (entry: ErrorLogEntry) => Promise<void> },
  refreshToken?: () => Promise<string>,
): Promise<string> {
  if (!accessToken || !recorder || recorder.responseCount === 0) {
    return accessToken;
  }
  const content = recorder.lines.join("\n") + "\n";
  if (!content.trim()) {
    return accessToken;
  }
  const rawPath = buildDropboxRawPath(connectorCode, new Date());
  try {
    const filename = rawPath.split("/").pop() ?? "uk_aq_raw_edge.jsonl";
    const jsonlName = filename.replace(/\.zip$/i, ".jsonl");
    const zipped = await zipTextCompressed(jsonlName, content);
    return await dropboxUploadFileWithRetry(accessToken, rawPath, zipped, refreshToken);
  } catch (err) {
    console.warn("Dropbox raw upload failed:", err);
    await errorLogger.logError({
      source: "edge",
      severity: "error",
      message: "Dropbox raw upload failed.",
      stack: err instanceof Error ? err.stack : undefined,
      context: {
        connector_id: connectorId,
        connector_code: connectorCode,
        dropbox_path: rawPath,
        dropbox_status: err instanceof DropboxHttpError ? err.status : null,
        error: err instanceof Error ? err.message : String(err),
      },
      connector_code: connectorCode ?? null,
      connector_id: connectorId ?? null,
    });
  }
  return accessToken;
}

function createErrorLogger(config: DropboxConfig | null, enabled: boolean) {
  let accessToken: string | null = null;
  return {
    async logError(entry: ErrorLogEntry): Promise<void> {
      if (!enabled) {
        return;
      }
      const errorId = crypto.randomUUID();
      const createdAt = new Date().toISOString();
      const row = {
        id: errorId,
        source: entry.source,
        severity: entry.severity,
        message: entry.message,
        stack: entry.stack ?? null,
        context: entry.context ?? null,
        connector_id: entry.connector_id ?? null,
        station_id: entry.station_id ?? null,
        timeseries_id: entry.timeseries_id ?? null,
      };
      const { error } = await postgrestRequest(
        "POST",
        "error_logs",
        undefined,
        row,
        "return=minimal",
        UK_AQ_RAW_SCHEMA,
      );
      if (error) {
        console.warn("error_logs insert failed:", error.message);
        return;
      }
      if (!config) {
        return;
      }
      try {
        if (!accessToken) {
          accessToken = await dropboxRefreshAccessToken(config);
        }
        const dropboxPath = buildDropboxErrorPath(
          errorId,
          createdAt,
          entry.connector_code ?? BLONDON_COMMUNITIES_CONNECTOR_CODE,
        );
        const payload = {
          ...row,
          connector_code: entry.connector_code ?? null,
          created_at: createdAt,
          dropbox_path: dropboxPath,
        };
        accessToken = await dropboxUploadFileWithRetry(
          accessToken,
          dropboxPath,
          JSON.stringify(payload, null, 2),
          () => dropboxRefreshAccessToken(config),
        );
        await postgrestRequest(
          "PATCH",
          "error_logs",
          { id: `eq.${errorId}` },
          { dropbox_path: dropboxPath },
          "return=minimal",
          UK_AQ_RAW_SCHEMA,
        );
      } catch (err) {
        console.warn("Dropbox error log upload failed:", err);
      }
    },
  };
}

function chunk<T>(values: T[], size: number): T[][] {
  if (size <= 0) {
    return [values];
  }
  const chunks: T[][] = [];
  for (let idx = 0; idx < values.length; idx += size) {
    chunks.push(values.slice(idx, idx + size));
  }
  return chunks;
}

const MAX_LOG_STATION_REFS = 50;

function summarizeStationRefs(refs: string[]): { count: number; refs: string[]; truncated: boolean } {
  if (!refs.length) {
    return { count: 0, refs: [], truncated: false };
  }
  if (refs.length <= MAX_LOG_STATION_REFS) {
    return { count: refs.length, refs, truncated: false };
  }
  return {
    count: refs.length,
    refs: refs.slice(0, MAX_LOG_STATION_REFS),
    truncated: true,
  };
}

serve(async (req) => {
  console.log("ingest_blondon_communities request", {
    method: req.method,
    cron_auth_configured: Boolean(SB_UK_AQ_CRON_SECRET),
  });
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }
  const authResponse = requireCronSecret(req);
  if (authResponse) {
    return authResponse;
  }

  const log = createLogBuffer();
  const dropboxConfig = loadDropboxConfig();
  const dropboxDiagnostics = buildDropboxDiagnostics();
  const rawRecorder = rawDropboxUploadsEnabled() ? createRawRecorder() : null;
  const errorLogger = createErrorLogger(
    loadErrorDropboxConfig(),
    Boolean(SUPABASE_URL && SUPABASE_PRIVILEGED_KEY),
  );

  const errors: string[] = [];
  let status = 200;
  let responsePayload: Record<string, unknown> = {};
  let debug = false;
  let debugInfo: Record<string, unknown> | null = null;
  let connector: ConnectorRow | null = null;
  let resolvedConnectorCode: string | null = null;
  let observationsUpserted = 0;
  let seriesPolled = 0;
  let timeseriesUpdated = 0;
  let checkpointsUpserted = 0;
  let observsWritten = 0;
  let observsReceiptsUpserted = 0;
  let observsEnqueued = 0;
  let observsFlushes = 0;
  let stationsSelected = 0;
  let stationsRequested: number | null = null;
  let runLastObservedAt: string | null = null;
  let stationFetchEnabled: boolean | null = null;
  let observationsRowsInput = 0;
  let observationsRowsPrepared = 0;
  let observationsRowsDedupedPrewrite = 0;
  const ingestDbObservationWriteStats =
    createEmptyIngestDbObservationWriteStats();
  let observsRowsPrepared = 0;
  let observsRowsDedupedPrewrite = 0;
  const runStartedAt = Date.now();
  const maxRuntimeSeconds = Number.isFinite(BLONDON_COMMUNITIES_MAX_RUNTIME_SECONDS)
    ? Math.max(30, BLONDON_COMMUNITIES_MAX_RUNTIME_SECONDS)
    : DEFAULT_MAX_RUNTIME_SECONDS;
  const runtimeDeadline = BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET
    ? runStartedAt + maxRuntimeSeconds * 1000
    : Number.POSITIVE_INFINITY;
  const shouldStop = () =>
    BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET && Date.now() >= runtimeDeadline;

  try {
    if (!SUPABASE_URL || !SUPABASE_PRIVILEGED_KEY) {
      status = 500;
      responsePayload = { error: "Missing SUPABASE_URL or SB_SECRET_KEY." };
      log.error("Missing Supabase configuration.");
    } else if (!BLONDON_COMMUNITIES_API_KEY) {
      status = 500;
      responsePayload = { error: "Missing BLONDON_COMMUNITIES_API_KEY." };
      log.error("Missing Breathe London API key.");
    } else {
      const payload = await req.json().catch(() => ({}));
      const request = payload as PollRequest;

      const connectorId = asString(request.connector_id);
      const connectorCode = resolveCommunitiesConnectorCode(
        asString(request.connector_code) ?? BLONDON_COMMUNITIES_CONNECTOR_CODE,
      );
      const connectorLabel = asString(request.connector_label) ?? BLONDON_COMMUNITIES_SERVICE_LABEL;
      const serviceRef = asString(request.service_ref) ?? BLONDON_COMMUNITIES_SERVICE_REF;
      const baseUrl = asString(request.base_url) ?? BLONDON_COMMUNITIES_BASE_URL;
      const speciesList = parseSpeciesList(request.species ?? "IPM25,INO2");
      const stationRefs = parseStationRefs(request.station_refs ?? []);
      const stationRefLookup = new Set(
        stationRefs.map((ref) => ref.trim().toLowerCase()).filter(Boolean),
      );
      const initialDays = asNumber(request.initial_days, DEFAULT_INITIAL_DAYS) ?? DEFAULT_INITIAL_DAYS;
      const windowHours = asNumber(request.window_hours, DEFAULT_WINDOW_HOURS) ?? DEFAULT_WINDOW_HOURS;
      const sleepSeconds = asNumber(request.sleep_seconds, DEFAULT_SLEEP_SECONDS) ?? DEFAULT_SLEEP_SECONDS;
      const batchSize = asNumber(request.batch_size, DEFAULT_BATCH_SIZE) ?? DEFAULT_BATCH_SIZE;
      const observsBufferFlushRows = Math.max(
        1,
        Math.trunc(
          asNumber(
            Deno.env.get("OBSERVS_BUFFER_FLUSH_ROWS"),
            DEFAULT_OBSERVS_BUFFER_FLUSH_ROWS,
          ) ?? DEFAULT_OBSERVS_BUFFER_FLUSH_ROWS,
        ),
      );
      const limit = asNumber(request.limit);
      const skipStations = asBoolean(request.skip_stations, false) ?? false;
      const activeOnly = asBoolean(request.active_only, false) ?? false;
      const dryRun = asBoolean(request.dry_run, false) ?? false;
      stationFetchEnabled = !skipStations;
      debug = asBoolean(request.debug, false) ?? false;
      const apiKey = asString(request.api_key) ?? BLONDON_COMMUNITIES_API_KEY;
      const startDateOverride = parseStartDate(asString(request.start_date));

      log.info("Poll request", {
        connector_id: connectorId ?? null,
        connector_code: connectorCode,
        connector_label: connectorLabel,
        service_ref: serviceRef,
        skip_stations: skipStations,
        active_only: activeOnly,
        station_refs: stationRefs.length || null,
        station_refs_preview: stationRefs.length ? summarizeStationRefs(stationRefs) : null,
        species: speciesList,
        window_hours: windowHours,
        initial_days: initialDays,
        start_date: startDateOverride ? startDateOverride.toISOString() : null,
        dry_run: dryRun,
        runtime_budget_enabled: BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET,
        max_runtime_seconds: BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET ? maxRuntimeSeconds : null,
        debug,
      });
      if (!dropboxConfig && dropboxDiagnostics.reason) {
        await errorLogger.logError({
          source: "edge",
          severity: "warn",
          message: "Dropbox log uploads disabled.",
          context: {
            reason: dropboxDiagnostics.reason,
            dropbox: dropboxDiagnostics,
          },
          connector_code: connectorCode,
          connector_id: connectorId ?? null,
        });
      }
      if (debug) {
        debugInfo = {
          request: {
            connector_id: connectorId ?? null,
            connector_code: connectorCode,
            connector_label: connectorLabel,
            service_ref: serviceRef,
            base_url: baseUrl,
            skip_stations: skipStations,
            active_only: activeOnly,
            station_refs: stationRefs.length ? stationRefs : null,
            species: speciesList,
            window_hours: windowHours,
            initial_days: initialDays,
            start_date: startDateOverride ? startDateOverride.toISOString() : null,
            dry_run: dryRun,
          },
          dropbox: dropboxDiagnostics,
        };
      }

      if (!speciesList.length) {
        status = 400;
        responsePayload = { error: "No valid species specified." };
        log.warn("No valid species specified.");
      } else {
        connector = await loadConnector(connectorId, connectorCode, connectorLabel, baseUrl);
        resolvedConnectorCode = connector?.connector_code ?? connectorCode;
        if (!connector) {
          status = 404;
          responsePayload = { error: "Connector not found." };
          log.warn("Connector not found.", {
            connector_id: connectorId ?? null,
            connector_code: connectorCode,
          });
        } else {
          if (rawRecorder) {
            rawRecorder.recordEvent("context", {
              connector_id: connector.id,
              connector_code: connector.connector_code,
              connector_label: connector.label,
              base_url: baseUrl,
              skip_stations: skipStations,
              active_only: activeOnly,
              station_refs: stationRefs,
              species: speciesList,
              window_hours: windowHours,
              initial_days: initialDays,
              start_date: startDateOverride ? startDateOverride.toISOString() : null,
            });
          }
          const stationRows: Record<string, unknown>[] = [];
          let stationIdMap: Record<string, number> = {};
          stationsRequested = stationRefs.length ? stationRefs.length : null;

          if (skipStations) {
            const stations = await fetchStationsFromDb(
              connector.id,
              serviceRef,
              limit,
              activeOnly,
              stationRefs,
            );
            for (const station of stations) {
              const stationRef = asString(station.station_ref);
              if (!stationRef) {
                continue;
              }
              stationRows.push({
                station_ref: stationRef,
                station_name: asString(station.station_name) ?? null,
                label: asString(station.label) ?? stationRef,
              });
              const stationId = Number(station.id);
              if (Number.isFinite(stationId)) {
                stationIdMap[stationRef] = stationId;
              }
            }
          } else {
            const sensors = await listSensors(baseUrl, apiKey, rawRecorder);
            const trimmedSensors = limit ? sensors.slice(0, Math.max(0, limit)) : sensors;
            if (!trimmedSensors.length) {
              responsePayload = { warning: "No sensors returned from Breathe London." };
              log.warn("No sensors returned from Breathe London.");
            } else {
              const metadataByRef: Record<string, Record<string, unknown>> = {};
              for (const sensor of trimmedSensors) {
                const { row, metadata } = normalizeStationPayload(sensor, connector.id, serviceRef);
                if (!row.station_ref) {
                  continue;
                }
                if (
                  stationRefLookup.size &&
                  !stationRefLookup.has(String(row.station_ref).trim().toLowerCase())
                ) {
                  continue;
                }
                stationRows.push(row);
                if (metadata && Object.keys(metadata).length > 0) {
                  metadataByRef[String(row.station_ref)] = metadata;
                }
              }

              if (!dryRun) {
                await upsertStations(stationRows);
                if (Object.keys(metadataByRef).length > 0) {
                  const metadataStationIds = await fetchStationIdsByRef(
                    connector.id,
                    serviceRef,
                    Object.keys(metadataByRef),
                  );
                  const attributesByStation: Record<number, Record<string, unknown>> = {};
                  for (const [ref, attrs] of Object.entries(metadataByRef)) {
                    const stationId = metadataStationIds[ref];
                    if (stationId) {
                      attributesByStation[stationId] = attrs;
                    }
                  }
                  await upsertStationMetadata(attributesByStation);
                }
              }
            }
          }

          stationsSelected = stationRows.length;
          if (stationRows.length) {
            const selectedRefs = stationRows
              .map((row) => asString(row.station_ref))
              .filter((value): value is string => Boolean(value));
            log.info("Stations selected", {
              stations_selected: stationsSelected,
              station_refs: summarizeStationRefs(selectedRefs),
            });
          }
          if (!stationRows.length) {
            responsePayload = {
              warning: skipStations
                ? "No Breathe London stations found in Supabase."
                : "No sensors returned from Breathe London.",
              stations_requested: stationsRequested,
              stations_selected: stationRows.length,
            };
            log.warn("No Breathe London stations available after filtering.", {
              skip_stations: skipStations,
              stations_selected: stationRows.length,
            });
          } else if (skipStations) {
            if (!Object.keys(stationIdMap).length) {
              responsePayload = {
                warning: "No station ids resolved for Breathe London.",
                stations_requested: stationsRequested,
                stations_selected: stationRows.length,
              };
            }
          } else {
            stationIdMap = await fetchStationIdsByRef(
              connector.id,
              serviceRef,
              stationRows.map((row) => String(row.station_ref)),
            );
          }

          if (!Object.keys(stationIdMap).length) {
            if (!responsePayload.warning) {
              responsePayload = {
                warning: "No station ids resolved for Breathe London.",
                stations_requested: stationsRequested,
                stations_selected: stationRows.length,
              };
            }
          } else {
              const phenomenonIds = dryRun
                ? await fetchPhenomenaIds(connector.id, speciesList)
                : await upsertPhenomena(connector.id, speciesList);
              const observsRowsPending: ObservsObservationRow[] = [];
              const flushPendingObservsRows = async (
                force = false,
                reason = "threshold",
              ) => {
                if (!observsRowsPending.length) {
                  return;
                }
                if (!force && observsRowsPending.length < observsBufferFlushRows) {
                  return;
                }
                const rows = observsRowsPending.splice(0, observsRowsPending.length);
                try {
                  const stats = await writeObservsWithOutbox(
                    publicRpcRequest,
                    rows,
                    (message) => {
                      log.warn("Observs dual-write warning.", {
                        message,
                        rows: rows.length,
                        reason,
                      });
                      errors.push("observs_dual_write");
                    },
                  );
                  observsFlushes += 1;
                  observsWritten += stats.written;
                  observsReceiptsUpserted += stats.receipts_upserted;
                  observsEnqueued += stats.enqueued;
                } catch (error) {
                  const message = error instanceof Error ? error.message : String(error);
                  log.warn("Observs dual-write flush failed.", {
                    message,
                    rows: rows.length,
                    reason,
                  });
                  errors.push("observs_dual_write_flush");
                }
              };

              const timeseriesRows: Record<string, unknown>[] = [];
              for (const row of stationRows) {
                const stationRef = String(row.station_ref);
                const stationId = stationIdMap[stationRef];
                if (!stationId) {
                  continue;
                }
                const stationName = asString(row.station_name) ?? asString(row.label) ?? stationRef;
                for (const species of speciesList) {
                  const config = SPECIES_CONFIG[species];
                  timeseriesRows.push({
                    timeseries_ref: `${stationRef}:${species}`,
                    label: `${stationName} ${config.label}`,
                    uom: config.uom,
                    station_id: stationId,
                    service_ref: serviceRef,
                    connector_id: connector.id,
                    phenomenon_id: phenomenonIds[config.source_label],
                    extras: { site_code: stationRef, species },
                  });
                }
              }
              const timeseriesIdMap = await fetchTimeseriesIds(
                connector.id,
                serviceRef,
                timeseriesRows.map((row) => String(row.timeseries_ref)),
              );
              const missingTimeseriesRefs = timeseriesRows
                .map((row) => String(row.timeseries_ref))
                .filter((ref) => !timeseriesIdMap[ref]);
              if (missingTimeseriesRefs.length) {
                throw new Error(
                  `Missing Communities timeseries identities: ${missingTimeseriesRefs.slice(0, 10).join(",")}`,
                );
              }

              const stationIds = Array.from(new Set(Object.values(stationIdMap)));
              const checkpoints = await fetchStationCheckpoints(stationIds);

              const now = floorToHour(new Date());
              const timeseriesUpdates: Array<{ id: number; last_value: number; last_value_at: string }> = [];
              const checkpointRows: Record<string, unknown>[] = [];
              observationsUpserted = 0;
              let timeBudgetHit = false;
              let stationsProcessed = 0;
              const nowIso = new Date().toISOString();
              const nowMsForLag = Date.now();

              const flushUpdates = async () => {
                if (dryRun) {
                  return;
                }
                if (timeseriesUpdates.length) {
                  timeseriesUpdated += await updateTimeseriesLastValues(timeseriesUpdates.splice(0), errors);
                }
                if (checkpointRows.length) {
                  checkpointsUpserted += await upsertStationCheckpoints(checkpointRows.splice(0));
                }
              };

              for (const row of stationRows) {
                const stationRef = String(row.station_ref);
                const stationId = stationIdMap[stationRef];
                if (!stationId) {
                  continue;
                }
                const checkpoint = checkpoints[stationId] ?? {};
                const previousLastObserved = parseObservationTimestamp(checkpoint.last_observed_at);
                const previousNextDue = asString(checkpoint.next_due_at);
                let updatedLastObserved = previousLastObserved ?? null;
                let nextDueAt = previousNextDue ?? null;
                let lagSamples = Array.isArray(checkpoint.ingest_lag_samples)
                  ? checkpoint.ingest_lag_samples.filter((value) => Number.isFinite(value))
                  : [];
                let stationLatestObserved = previousLastObserved ?? null;
                let stationHasNewObservation = false;

                for (const species of speciesList) {
                  const timeseriesRef = `${stationRef}:${species}`;
                  const timeseriesId = timeseriesIdMap[timeseriesRef];
                  if (!timeseriesId) {
                    continue;
                  }
                  const checkpointDate = previousLastObserved ? new Date(previousLastObserved) : null;
                  let checkpointCutoffMs = checkpointDate ? checkpointDate.getTime() : null;
                  if (checkpointCutoffMs !== null && !Number.isFinite(checkpointCutoffMs)) {
                    checkpointCutoffMs = null;
                  }
                  let lastObserved = previousLastObserved ?? null;
                  let lastValue: number | null = null;
                  let startTime: Date;
                  if (checkpointDate) {
                    startTime = checkpointDate;
                  } else if (startDateOverride) {
                    startTime = startDateOverride;
                  } else {
                    startTime = new Date(now.getTime() - Math.max(initialDays, 1) * 24 * 60 * 60 * 1000);
                  }
                  startTime = floorToHour(startTime);
                  if (startTime >= now) {
                    continue;
                  }
                  const windowMs = Math.max(windowHours, 1) * 60 * 60 * 1000;
                  let cursor = startTime;

                  while (cursor < now) {
                    if (shouldStop()) {
                      timeBudgetHit = true;
                      break;
                    }
                    const endTime = new Date(Math.min(cursor.getTime() + windowMs, now.getTime()));
                    try {
                      const payload = await getClarityData(
                        baseUrl,
                        apiKey,
                        stationRef,
                        species,
                        cursor,
                        endTime,
                        rawRecorder,
                      );
                      const { rows, lastObserved: windowLast, lastValue: windowValue } = extractObservations(
                        payload,
                        timeseriesId,
                        Number(connector.id),
                      );
                      const freshRows = checkpointCutoffMs === null
                        ? rows
                        : rows.filter((point) => {
                          const observedAt = asString(point.observed_at);
                          if (!observedAt) {
                            return false;
                          }
                          const observedMs = Date.parse(observedAt);
                          return Number.isFinite(observedMs) && observedMs > checkpointCutoffMs!;
                        });
                      if (freshRows.length) {
                        const observationDedupe = dedupeExactObservationRows(freshRows);
                        const preparedRows = observationDedupe.rows;
                        observationsRowsInput += freshRows.length;
                        observationsRowsPrepared += preparedRows.length;
                        observationsRowsDedupedPrewrite += observationDedupe.deduped;
                        observsRowsDedupedPrewrite += observationDedupe.deduped;
                        if (dryRun) {
                          observsRowsPrepared += toObservsObservationRows(
                            preparedRows,
                            Number(connector.id),
                            timeseriesId,
                          ).length;
                        } else {
                          for (const batch of chunk(preparedRows, batchSize)) {
                            const writeStats = await upsertObservations(
                              batch,
                              BLONDON_COMMUNITIES_ENFORCE_RUNTIME_BUDGET
                                ? {
                                  shouldStop,
                                  remainingRuntimeMs: () =>
                                    Math.max(0, runtimeDeadline - Date.now()),
                                }
                                : undefined,
                            );
                            mergeIngestDbObservationWriteStats(
                              ingestDbObservationWriteStats,
                              writeStats,
                            );
                            observationsUpserted =
                              ingestDbObservationWriteStats.committed_rows;
                            const observsRows = toObservsObservationRows(
                              batch,
                              Number(connector.id),
                              timeseriesId,
                            );
                            observsRowsPrepared += observsRows.length;
                            observsRowsPending.push(...observsRows);
                            await flushPendingObservsRows(false, "batch_threshold");
                          }
                        }
                      }
                      if (windowLast && (!lastObserved || windowLast > lastObserved)) {
                        lastObserved = windowLast;
                        lastValue = windowValue;
                        const windowLastMs = Date.parse(windowLast);
                        if (Number.isFinite(windowLastMs)) {
                          checkpointCutoffMs = windowLastMs;
                        }
                      }
                    } catch (error) {
                      if (isIngestDbObservationWriteError(error)) throw error;
                      break;
                    }
                    cursor = endTime;
                    if (sleepSeconds && sleepSeconds > 0 && !shouldStop()) {
                      await sleep(sleepSeconds * 1000);
                    }
                  }

                  if (timeBudgetHit) {
                    break;
                  }

                  if (lastObserved && lastValue !== null) {
                    timeseriesUpdates.push({ id: timeseriesId, last_value: lastValue, last_value_at: lastObserved });
                    seriesPolled += 1;
                  }

                  if (lastObserved && (!stationLatestObserved || lastObserved > stationLatestObserved)) {
                    stationLatestObserved = lastObserved;
                    stationHasNewObservation = true;
                  }
                }

                if (timeBudgetHit) {
                  break;
                }

                if (stationLatestObserved && (!updatedLastObserved || stationLatestObserved > updatedLastObserved)) {
                  updatedLastObserved = stationLatestObserved;
                }
                runLastObservedAt = maxTimestampIso(runLastObservedAt, updatedLastObserved);

                if (stationHasNewObservation && updatedLastObserved) {
                  const lagSeconds = Math.max(
                    0,
                    Math.round((nowMsForLag - Date.parse(updatedLastObserved)) / 1000),
                  );
                  if (Number.isFinite(lagSeconds)) {
                    lagSamples = appendSample(lagSamples, lagSeconds);
                  }
                  if (lagSamples.length < 10) {
                    nextDueAt = new Date(nowMsForLag + 5 * 60 * 1000).toISOString();
                  } else {
                    const lagSecondsMin = minSeconds(lagSamples) ?? 5 * 60;
                    const baseMs = Date.parse(updatedLastObserved);
                    if (Number.isFinite(baseMs)) {
                      nextDueAt = new Date(baseMs + (3600 + lagSecondsMin) * 1000).toISOString();
                    } else {
                      nextDueAt = nowIso;
                    }
                  }
                } else if (!previousNextDue) {
                  nextDueAt = new Date(nowMsForLag + 5 * 60 * 1000).toISOString();
                }

                checkpointRows.push({
                  station_id: stationId,
                  next_due_at: nextDueAt,
                  last_observed_at: updatedLastObserved,
                  ingest_lag_samples: lagSamples,
                  last_polled_at: nowIso,
                });

                stationsProcessed += 1;

                if (shouldStop()) {
                  timeBudgetHit = true;
                  await flushUpdates();
                  break;
                }
              }

              await flushPendingObservsRows(true, "run_complete");
              await flushUpdates();

              responsePayload = {
                connector_id: connector.id,
                stations: stationRows.length,
                stations_requested: stationsRequested,
                stations_selected: stationsSelected,
                stations_processed: stationsProcessed,
                last_observed_at: runLastObservedAt,
                species: speciesList,
                observations_upserted: observationsUpserted,
                ingestdb_observation_write: ingestDbObservationWriteStats,
                cross_database_transaction: false,
                observations_rows_input: observationsRowsInput,
                observations_rows_prepared: observationsRowsPrepared,
                observations_rows_deduped_prewrite: observationsRowsDedupedPrewrite,
                observs_rows_prepared: observsRowsPrepared,
                observs_rows_deduped_prewrite: observsRowsDedupedPrewrite,
                observs_written: observsWritten,
                observs_receipts_upserted: observsReceiptsUpserted,
                observs_enqueued: observsEnqueued,
                observs_flushes: observsFlushes,
                timeseries_updated: timeseriesUpdated,
                series_polled: seriesPolled,
                checkpoints_upserted: checkpointsUpserted,
                dry_run: dryRun,
                partial: timeBudgetHit,
                stopped_reason: timeBudgetHit ? "runtime_budget_exceeded" : null,
                errors,
              };
              log.info("Stations polled.", {
                stations_selected: stationsSelected,
                stations_processed: stationsProcessed,
                partial: timeBudgetHit,
              });
              if (!dryRun) {
                const { error: pollUpdateError } = await postgrestRequest(
                  "PATCH",
                  "connectors",
                  { id: `eq.${connector.id}` },
                  { last_polled_at: new Date().toISOString() },
                  "return=minimal",
                );
                if (pollUpdateError) {
                  log.warn("Failed to update connectors.last_polled_at.", {
                    error: pollUpdateError.message,
                  });
                  await errorLogger.logError({
                    source: "edge",
                    severity: "error",
                    message: "Failed to update connectors.last_polled_at.",
                    context: {
                      connector_id: connector.id,
                      error: pollUpdateError.message,
                    },
                    connector_code: connector.connector_code ?? connectorCode,
                    connector_id: connector.id,
                  });
                }
              }
            }
          }
        }
      }
  } catch (error) {
    status = 500;
    const message = error instanceof Error ? error.message : String(error);
    if (isIngestDbObservationWriteError(error)) {
      const writeError = error as {
        stats?: Record<string, unknown>;
        classification?: string;
        terminalReason?: string;
      };
      if (writeError.stats) {
        mergeIngestDbObservationWriteStats(
          ingestDbObservationWriteStats,
          writeError.stats,
        );
      }
      observationsUpserted = ingestDbObservationWriteStats.committed_rows;
      responsePayload = {
        error: "IngestDB observation write failed.",
        observations_upserted: observationsUpserted,
        ingestdb_observation_write: ingestDbObservationWriteStats,
        cross_database_transaction: false,
        observs_written: observsWritten,
        observs_receipts_upserted: observsReceiptsUpserted,
        observs_enqueued: observsEnqueued,
        failure_classification: writeError.classification ?? null,
        terminal_reason: writeError.terminalReason ?? null,
      };
    } else {
      responsePayload = {
        error: "Internal server error.",
        observations_upserted: observationsUpserted,
        ingestdb_observation_write: ingestDbObservationWriteStats,
        cross_database_transaction: false,
        observs_written: observsWritten,
        observs_receipts_upserted: observsReceiptsUpserted,
        observs_enqueued: observsEnqueued,
      };
    }
    log.error("Breathe London ingest failed.", { error: message });
    await errorLogger.logError({
      source: "edge",
      severity: "error",
      message: "Breathe London ingest failed.",
      stack: error instanceof Error ? error.stack : undefined,
      context: {
        error: message,
      },
      connector_code: connector?.connector_code ?? BLONDON_COMMUNITIES_CONNECTOR_CODE,
      connector_id: connector?.id ?? null,
    });
  }

  log.info("Poll summary", {
    connector_id: connector?.id ?? null,
    stations_selected: stationsSelected,
    observations_upserted: observationsUpserted,
    observations_rows_input: observationsRowsInput,
    observations_rows_prepared: observationsRowsPrepared,
    observations_rows_deduped_prewrite: observationsRowsDedupedPrewrite,
    observs_rows_prepared: observsRowsPrepared,
    observs_rows_deduped_prewrite: observsRowsDedupedPrewrite,
    observs_written: observsWritten,
    observs_receipts_upserted: observsReceiptsUpserted,
    observs_enqueued: observsEnqueued,
    observs_flushes: observsFlushes,
    timeseries_updated: timeseriesUpdated,
    series_polled: seriesPolled,
    checkpoints_upserted: checkpointsUpserted,
    errors: errors.length,
  });
  if (errors.length) {
    log.warn("Poll warnings", { sample: errors.slice(0, 25) });
  }

  if (errors.length) {
    await errorLogger.logError({
      source: "edge",
      severity: "warn",
      message: "Breathe London ingest warnings.",
      context: { warnings: errors },
      connector_code: connector?.connector_code ?? BLONDON_COMMUNITIES_CONNECTOR_CODE,
      connector_id: connector?.id ?? null,
    });
  }

  if (dropboxConfig) {
    let accessToken: string | null = null;
    const refreshDropbox = () => dropboxRefreshAccessToken(dropboxConfig);
    try {
      accessToken = await dropboxRefreshAccessToken(dropboxConfig);
    } catch (err) {
      console.warn("Dropbox token request failed:", err);
      await errorLogger.logError({
        source: "edge",
        severity: "error",
        message: "Dropbox token request failed.",
        stack: err instanceof Error ? err.stack : undefined,
        context: {
          error: err instanceof Error ? err.message : String(err),
          dropbox: dropboxDiagnostics,
        },
        connector_code: resolvedConnectorCode ?? BLONDON_COMMUNITIES_CONNECTOR_CODE,
        connector_id: connector?.id ?? null,
      });
    }
    if (accessToken) {
      const rawConnectorCode = resolvedConnectorCode
        ?? connector?.connector_code
        ?? BLONDON_COMMUNITIES_CONNECTOR_CODE;
      const connectorId = connector?.id ?? null;
      accessToken = await uploadDropboxLog(
        accessToken,
        log,
        connectorId,
        rawConnectorCode,
        errorLogger,
        refreshDropbox,
      );
      if (dropboxDiagnostics.raw_enabled) {
        await uploadDropboxRaw(
          accessToken,
          rawRecorder,
          connectorId,
          rawConnectorCode,
          errorLogger,
          refreshDropbox,
        );
      }
    }
  }

  if (debug) {
    responsePayload = {
      ...responsePayload,
      debug: debugInfo ?? { dropbox: dropboxDiagnostics },
    };
  }
  if (stationFetchEnabled !== null) {
    responsePayload = {
      ...responsePayload,
      station_fetch_enabled: stationFetchEnabled,
    };
  }
  return new Response(JSON.stringify(responsePayload), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...cacheControlHeaders(status),
    },
  });
});
