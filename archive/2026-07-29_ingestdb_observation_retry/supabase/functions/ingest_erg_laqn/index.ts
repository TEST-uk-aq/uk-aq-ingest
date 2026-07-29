// trigger deploy 2026-02-09 12:36
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import "../_shared/fetch_egress_patch.ts";
import { cacheControlHeaders } from "../_shared/cache.ts";
import {
  type ObservsObservationRow,
  writeObservsWithOutbox,
} from "../_shared/observs_client.ts";
import {
  addUtcDays,
  buildErgDateRange,
  formatUtcDate,
  utcDayStart,
} from "./uk_aq_erg_laqn_date_range.ts";

type PollRequest = {
  connector_id?: string;
  connector_code?: string;
  connector_label?: string;
  connector_display_name?: string;
  service_ref?: string;
  base_url?: string;
  group?: string;
  species?: string[] | string;
  station_refs?: string[] | string;
  days?: number;
  start_date?: string;
  end_date?: string;
  start_from_latest?: boolean;
  batch_size?: number;
  sleep_seconds?: number;
  dry_run?: boolean;
  csv_station_id?: string;
  csv_station_ref?: string;
};

type ConnectorRow = {
  id: string;
  connector_code: string;
  label: string;
  display_name: string | null;
  service_url: string | null;
};

type DropboxConfig = {
  appKey: string;
  appSecret: string;
  refreshToken: string;
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

const DEFAULT_BASE_URL = "https://api.erg.ic.ac.uk/AirQuality";
const DEFAULT_CONNECTOR_CODE = "erg_laqn";
const DEFAULT_CONNECTOR_LABEL = "ERG London Air";
const DEFAULT_CONNECTOR_DISPLAY_NAME = "London Air LAQN";
const DEFAULT_USER_AGENT = "uk-air-quality-networks";
const DEFAULT_GROUP = "London";
const DEFAULT_SPECIES = ["NO2", "PM10", "PM25", "O3"];
const DEFAULT_DAYS = 1;
const DEFAULT_BATCH_SIZE = 500;
const DEFAULT_OBSERVS_BUFFER_FLUSH_ROWS = 5000;
const DEFAULT_SLEEP_SECONDS = 0.2;
const DEFAULT_MAX_RUNTIME_SECONDS = 120;
const DEFAULT_TIMEOUT_MS = 30_000;
const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);
const UPSERT_PREFER = "resolution=merge-duplicates,return=minimal";

const SPECIES_CONFIG: Record<
  string,
  {
    label: string;
    uom: string;
    pollutant_label: string;
    observed_property_code: string;
    observed_property_domain: "aq" | "met";
  }
> = {
  NO2: { label: "NO2", uom: "ug/m3", pollutant_label: "no2", observed_property_code: "no2", observed_property_domain: "aq" },
  PM10: { label: "PM10", uom: "ug/m3", pollutant_label: "pm10", observed_property_code: "pm10", observed_property_domain: "aq" },
  PM25: { label: "PM2.5", uom: "ug/m3", pollutant_label: "pm2.5", observed_property_code: "pm25", observed_property_domain: "aq" },
  O3: { label: "O3", uom: "ug/m3", pollutant_label: "o3", observed_property_code: "o3", observed_property_domain: "aq" },
  SO2: { label: "SO2", uom: "ug/m3", pollutant_label: "so2", observed_property_code: "so2", observed_property_domain: "aq" },
  CO: { label: "CO", uom: "mg/m3", pollutant_label: "co", observed_property_code: "co", observed_property_domain: "aq" },
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
const SB_UK_AQ_CRON_SECRET = Deno.env.get("SB_UK_AQ_CRON_SECRET") ?? "";

const LAQN_BASE_URL = (Deno.env.get("LAQN_BASE_URL") ?? DEFAULT_BASE_URL)
  .replace(/\/$/, "");
const LAQN_CONNECTOR_CODE = Deno.env.get("LAQN_CONNECTOR_CODE")
  ?? DEFAULT_CONNECTOR_CODE;
const LAQN_SERVICE_REF = Deno.env.get("LAQN_SERVICE_REF")
  ?? LAQN_CONNECTOR_CODE;
const LAQN_CONNECTOR_LABEL = Deno.env.get("LAQN_CONNECTOR_LABEL")
  ?? Deno.env.get("LAQN_SERVICE_LABEL")
  ?? DEFAULT_CONNECTOR_LABEL;
const LAQN_CONNECTOR_DISPLAY_NAME = Deno.env.get("LAQN_CONNECTOR_DISPLAY_NAME")
  ?? DEFAULT_CONNECTOR_DISPLAY_NAME;
const LAQN_USER_AGENT = Deno.env.get("LAQN_USER_AGENT")
  ?? DEFAULT_USER_AGENT;
const LAQN_DEFAULT_GROUP = Deno.env.get("LAQN_DEFAULT_GROUP") ?? DEFAULT_GROUP;
const LAQN_CSV_STATION_ID = Deno.env.get("LAQN_CSV_STATION_ID") ?? "";
const LAQN_CSV_STATION_REF = Deno.env.get("LAQN_CSV_STATION_REF") ?? "";
const LAQN_MAX_RUNTIME_SECONDS = Number(
  Deno.env.get("LAQN_MAX_RUNTIME_SECONDS") ?? DEFAULT_MAX_RUNTIME_SECONDS,
);
const UK_AQ_DROPBOX_ROOT = (() => {
  const raw = Deno.env.get("UK_AQ_DROPBOX_ROOT") ?? "";
  return normalizeDropboxPath(raw);
})();
const LAQN_CSV_DROPBOX_FOLDER = dropboxWithRoot(
  Deno.env.get("LAQN_CSV_DROPBOX_FOLDER") ?? "/connectors/erg_laqn",
);

const DROPBOX_APP_KEY = Deno.env.get("DROPBOX_APP_KEY") ?? "";
const DROPBOX_APP_SECRET = Deno.env.get("DROPBOX_APP_SECRET") ?? "";
const DROPBOX_REFRESH_TOKEN = Deno.env.get("DROPBOX_REFRESH_TOKEN") ?? "";
const DROPBOX_ALLOWED_SUPABASE_URL = Deno.env.get("LAQN_RAW_DROPBOX_ALLOWED_SUPABASE_URL")
  ?? Deno.env.get("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL")
  ?? "";
const DROPBOX_ERROR_ALLOWED_SUPABASE_URL =
  Deno.env.get("LAQN_ERROR_DROPBOX_ALLOWED_SUPABASE_URL")
    ?? Deno.env.get("UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL")
    ?? "";
const DROPBOX_LOG_FOLDER = dropboxWithRoot("/connectors/erg_laqn/log");
const DROPBOX_RAW_FOLDER = dropboxWithRoot("/connectors/erg_laqn/raw_data");
const DROPBOX_ERROR_FOLDER = dropboxWithRoot(
  Deno.env.get("LAQN_ERROR_DROPBOX_FOLDER")
    ?? Deno.env.get("UK_AIR_ERROR_DROPBOX_FOLDER")
    ?? "error_log",
);
const DROPBOX_LOG_RETENTION_DAYS = 31;
const DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token";
const DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
const DROPBOX_LIST_FOLDER_URL = "https://api.dropboxapi.com/2/files/list_folder";
const DROPBOX_DOWNLOAD_ZIP_URL = "https://content.dropboxapi.com/2/files/download_zip";
const DROPBOX_DELETE_URL = "https://api.dropboxapi.com/2/files/delete_v2";

const REST_BASE_URL = SUPABASE_URL
  ? `${SUPABASE_URL.replace(/\/$/, "")}/rest/v1`
  : "";

function postgrestHeaders(prefer?: string, schema = UK_AQ_CORE_SCHEMA): Record<string, string> {
  const headers: Record<string, string> = {
    apikey: SUPABASE_PRIVILEGED_KEY,
    "Content-Type": "application/json",
    "x-ukaq-egress-caller": "ingest_erg_laqn",
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
): Promise<{ data: T | null; error: { message: string } | null }> {
  if (!REST_BASE_URL || !SUPABASE_PRIVILEGED_KEY) {
    return {
      data: null,
      error: { message: "Missing REST_BASE_URL or SB_SECRET_KEY." },
    };
  }
  const url = new URL(`${REST_BASE_URL}/${table}`);
  if (params) {
    Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, value));
  }
  const resp = await fetch(url.toString(), {
    method,
    headers: postgrestHeaders(prefer, schema),
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!resp.ok) {
    let message = `${resp.status} ${resp.statusText}`;
    try {
      const errorPayload = await resp.json();
      if (errorPayload?.message) {
        message = errorPayload.message;
      }
    } catch {
      // ignore
    }
    return { data: null, error: { message } };
  }
  if (resp.status === 204 || resp.status === 201) {
    return { data: null, error: null };
  }
  const data = (await resp.json().catch(() => null)) as T | null;
  return { data, error: null };
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

function parseTimestamp(value?: string | null): Date | null {
  if (!value) {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
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

const ERROR_LOGGER = createErrorLogger(
  loadErrorDropboxConfig(),
  Boolean(SUPABASE_URL && SUPABASE_PRIVILEGED_KEY),
);

async function fetchJson(
  url: string,
  params?: Record<string, string>,
  log?: LogBuffer,
  rawRecorder?: RawRecorder | null,
): Promise<unknown> {
  const requestUrl = new URL(url);
  if (params) {
    Object.entries(params).forEach(([key, value]) =>
      requestUrl.searchParams.set(key, value)
    );
  }
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
    try {
      const resp = await fetch(requestUrl.toString(), {
        headers: { "User-Agent": LAQN_USER_AGENT },
        signal: controller.signal,
      });
      clearTimeout(timeoutId);
      const contentType = resp.headers.get("content-type") ?? "";
      const payload = resp.status === 204
        ? null
        : (contentType.includes("application/json") ? await resp.json() : await resp.text());
      rawRecorder?.recordResponse(
        requestUrl.pathname,
        Object.fromEntries(requestUrl.searchParams.entries()),
        resp.status,
        payload,
      );
      if (resp.status === 404) {
        throw new Error(`HTTP 404 for ${requestUrl}`);
      }
      if (RETRYABLE_STATUS.has(resp.status)) {
        log?.warn("Retryable response received.", {
          attempt,
          status: resp.status,
          url: requestUrl.toString(),
        });
        await sleep(2 ** attempt);
        continue;
      }
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status} for ${requestUrl}`);
      }
      return payload;
    } catch (err) {
      clearTimeout(timeoutId);
      if (attempt === 3) {
        throw err;
      }
      await sleep(2 ** attempt);
    }
  }
  return null;
}

function sleep(seconds: number): Promise<void> {
  const ms = Math.max(0, seconds) * 1000;
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseSpeciesList(value: string[] | string | undefined): string[] {
  if (!value) {
    return [...DEFAULT_SPECIES];
  }
  const items = Array.isArray(value) ? value : value.split(",");
  const normalized = items.map((item) => String(item).trim().toUpperCase())
    .map((item) => item.replace(".", ""));
  return normalized.filter((item) => item.length > 0);
}

function parseStationRefs(value: string[] | string | undefined): string[] {
  if (!value) {
    return [];
  }
  const items = Array.isArray(value) ? value : value.split(",");
  return items.map((item) => String(item).trim().toUpperCase()).filter(Boolean);
}

function parseDate(value?: string | null): Date | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  let candidate = trimmed;
  if (!candidate.includes("T") && candidate.includes(" ")) {
    candidate = candidate.replace(" ", "T");
  }
  if (!candidate.endsWith("Z") && !candidate.includes("+")) {
    candidate = `${candidate}Z`;
  }
  const parsed = new Date(candidate);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function normalizeTimestampValue(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  return null;
}

function pickValue(payload: Record<string, unknown>, keys: string[]): unknown {
  for (const key of keys) {
    if (key in payload) {
      return payload[key];
    }
    const atKey = `@${key}`;
    if (atKey in payload) {
      return payload[atKey];
    }
    const lowerKey = key.toLowerCase();
    for (const [entryKey, entryValue] of Object.entries(payload)) {
      if (entryKey.toLowerCase() === lowerKey) {
        return entryValue;
      }
    }
  }
  return null;
}

function extractStations(payload: unknown): Record<string, unknown>[] {
  if (Array.isArray(payload)) {
    return payload.filter((row) => typeof row === "object" && row !== null) as Record<string, unknown>[];
  }
  if (payload && typeof payload === "object") {
    const dict = payload as Record<string, unknown>;
    for (const key of ["Sites", "sites", "MonitoringSites", "monitoringSites", "data"]) {
      const value = dict[key];
      if (Array.isArray(value)) {
        return value.filter((row) => typeof row === "object" && row !== null) as Record<string, unknown>[];
      }
      if (value && typeof value === "object") {
        const nested = value as Record<string, unknown>;
        const sites = nested["Site"] ?? nested["site"];
        if (Array.isArray(sites)) {
          return sites.filter((row) => typeof row === "object" && row !== null) as Record<string, unknown>[];
        }
        if (sites && typeof sites === "object") {
          return [sites as Record<string, unknown>];
        }
      }
    }
    const sites = dict["Site"] ?? dict["site"];
    if (Array.isArray(sites)) {
      return sites.filter((row) => typeof row === "object" && row !== null) as Record<string, unknown>[];
    }
    if (sites && typeof sites === "object") {
      return [sites as Record<string, unknown>];
    }
  }
  return [];
}

function normalizeStation(
  station: Record<string, unknown>,
  connectorId: number,
): Record<string, unknown> | null {
  const stationRef = String(
    pickValue(station, ["SiteCode", "SiteID", "SiteId", "Site"]) ?? ""
  ).trim();
  if (!stationRef) {
    return null;
  }
  const label = String(pickValue(station, ["SiteName", "Label", "Name"]) ?? "")
    .trim();
  const latRaw = pickValue(station, ["Latitude", "Lat", "Northing"]);
  const lonRaw = pickValue(station, ["Longitude", "Lon", "Lng", "Easting"]);
  const latitude = latRaw !== null ? Number(latRaw) : null;
  const longitude = lonRaw !== null ? Number(lonRaw) : null;
  const geometry = Number.isFinite(latitude) && Number.isFinite(longitude)
    ? `SRID=4326;POINT(${longitude} ${latitude})`
    : null;

  return {
    station_ref: stationRef,
    service_ref: LAQN_SERVICE_REF,
    label: label || stationRef,
    station_name: label || stationRef,
    station_type: pickValue(station, ["SiteType", "SiteClassification", "Type"]) ?? null,
    station_exposure: pickValue(station, ["LocationType", "SiteLocation", "SiteLocationType"]) ??
      null,
    region: pickValue(station, ["LocalAuthority", "Borough", "Region", "LocalAuthorityName"]) ??
      null,
    geometry,
    first_seen_at: normalizeTimestampValue(
      pickValue(station, ["StartDate", "SiteStartDate", "SiteSetupDate"]),
    ),
    last_seen_at: normalizeTimestampValue(
      pickValue(station, ["LastUpdated", "LastCommunication", "LastSeen"]),
    ),
    removed_at: normalizeTimestampValue(
      pickValue(station, ["EndDate", "SiteEndDate", "DateClosed"]),
    ),
    connector_id: connectorId,
  };
}

function extractObservations(payload: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(payload)) {
    return payload.filter((row) => typeof row === "object" && row !== null) as Array<Record<string, unknown>>;
  }
  if (payload && typeof payload === "object") {
    const dict = payload as Record<string, unknown>;
    const raw = dict["RawAQData"] ?? dict["rawAQData"];
    const container = raw && typeof raw === "object" ? raw as Record<string, unknown> : dict;
    for (const key of ["RawData", "rawData", "Data", "data", "Measurements", "measurements"]) {
      const value = container[key];
      if (Array.isArray(value)) {
        return value.filter((row) => typeof row === "object" && row !== null) as Array<Record<string, unknown>>;
      }
    }
  }
  return [];
}

function parseObservationDate(value: unknown): Date | null {
  if (!value) {
    return null;
  }
  const text = String(value).trim();
  if (!text) {
    return null;
  }
  let candidate = text;
  if (!candidate.includes("T") && candidate.includes(" ")) {
    candidate = candidate.replace(" ", "T");
  }
  if (!candidate.endsWith("Z") && !candidate.includes("+")) {
    candidate = `${candidate}Z`;
  }
  const parsed = new Date(candidate);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function chunk<T>(values: T[], size: number): T[][] {
  if (size <= 0) {
    return [values];
  }
  const result: T[][] = [];
  for (let idx = 0; idx < values.length; idx += size) {
    result.push(values.slice(idx, idx + size));
  }
  return result;
}

function normalizePollutant(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }
  return value.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function csvEscape(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  const text = String(value);
  if (!/[",\n]/.test(text)) {
    return text;
  }
  return `"${text.replace(/"/g, '""')}"`;
}

async function loadCsvStation(
  connectorId: string,
  stationIdRaw: string | null,
  stationRefRaw: string | null,
): Promise<{ id: string; station_ref: string; label: string } | null> {
  if (!connectorId) {
    return null;
  }
  if (stationIdRaw) {
    const { data, error } = await postgrestRequest<
      Array<{ id: number; station_ref: string; label: string }>
    >(
      "GET",
      "stations",
      {
        select: "id,station_ref,label",
        id: `eq.${stationIdRaw}`,
        connector_id: `eq.${connectorId}`,
        limit: "1",
      },
    );
    if (error) {
      throw new Error(`CSV station lookup failed: ${error.message}`);
    }
    if (data && data.length) {
      const row = data[0];
      return {
        id: String(row.id),
        station_ref: String(row.station_ref),
        label: String(row.label),
      };
    }
  }
  if (stationRefRaw) {
    const { data, error } = await postgrestRequest<
      Array<{ id: number; station_ref: string; label: string }>
    >(
      "GET",
      "stations",
      {
        select: "id,station_ref,label",
        connector_id: `eq.${connectorId}`,
        station_ref: `eq.${stationRefRaw}`,
        limit: "1",
      },
    );
    if (error) {
      throw new Error(`CSV station lookup failed: ${error.message}`);
    }
    if (data && data.length) {
      const row = data[0];
      return {
        id: String(row.id),
        station_ref: String(row.station_ref),
        label: String(row.label),
      };
    }
  }
  return null;
}

async function loadTimeseriesByStation(
  connectorId: string,
  stationId: string,
): Promise<Array<{ id: string; pollutant: string | null }>> {
  const { data, error } = await postgrestRequest<
    Array<{ id: number; phenomenon: { pollutant_label?: string; notation?: string; label?: string } | null }>
  >(
    "GET",
    "timeseries",
    {
      select: "id,phenomenon:phenomena(pollutant_label,notation,label)",
      connector_id: `eq.${connectorId}`,
      station_id: `eq.${stationId}`,
      limit: "500",
    },
  );
  if (error) {
    throw new Error(`CSV timeseries lookup failed: ${error.message}`);
  }
  return (data ?? []).map((row) => ({
    id: String(row.id),
    pollutant: normalizePollutant(
      row.phenomenon?.pollutant_label ?? row.phenomenon?.notation ?? row.phenomenon?.label ?? null,
    ),
  }));
}

async function loadObservationsForToday(
  timeseriesId: string,
  startIso: string,
  endIso: string,
): Promise<Array<{ observed_at: string; value: number | null }>> {
  const { data, error } = await postgrestRequest<Array<{ observed_at: string; value: number | null }>>(
    "GET",
    "observations",
    {
      select: "observed_at,value",
      timeseries_id: `eq.${timeseriesId}`,
      and: `(observed_at.gte.${startIso},observed_at.lt.${endIso})`,
      order: "observed_at.asc",
      limit: "2000",
    },
  );
  if (error) {
    throw new Error(`CSV observations fetch failed: ${error.message}`);
  }
  return data ?? [];
}

function buildErgCsvPath(pollutant: string, dateStamp: string): string {
  return `${LAQN_CSV_DROPBOX_FOLDER}/erg_laqn_${pollutant}_${dateStamp}.csv`;
}

async function loadConnector(
  connectorCode: string,
): Promise<ConnectorRow | null> {
  const { data, error } = await postgrestRequest<ConnectorRow[]>(
    "GET",
    "connectors",
    {
      select: "id,connector_code,label,display_name,service_url",
      connector_code: `eq.${connectorCode}`,
      limit: "1",
    },
  );
  if (error) {
    throw new Error(`Connector fetch failed: ${error.message}`);
  }
  return data && data[0] ? data[0] : null;
}

async function upsertStations(rows: Record<string, unknown>[]): Promise<void> {
  if (!rows.length) {
    return;
  }
  const { error } = await postgrestRequest(
    "POST",
    "stations",
    { on_conflict: "connector_id,service_ref,station_ref" },
    rows,
    UPSERT_PREFER,
  );
  if (error) {
    throw new Error(`Stations upsert failed: ${error.message}`);
  }
}

async function fetchStationIds(
  connectorId: number,
  stationRefs: string[],
): Promise<Record<string, number>> {
  const mapping: Record<string, number> = {};
  for (const chunkRefs of chunk(stationRefs, 200)) {
    const { data, error } = await postgrestRequest<Array<{ id: number; station_ref: string }>>(
      "GET",
      "stations",
      {
        select: "id,station_ref",
        connector_id: `eq.${connectorId}`,
        service_ref: `eq.${LAQN_SERVICE_REF}`,
        station_ref: `in.(${chunkRefs.join(",")})`,
      },
    );
    if (error) {
      throw new Error(`Station id fetch failed: ${error.message}`);
    }
    for (const row of data ?? []) {
      mapping[String(row.station_ref)] = Number(row.id);
    }
  }
  return mapping;
}

async function upsertPhenomena(
  connectorId: number,
  speciesList: string[],
): Promise<void> {
  const payload = speciesList.map((species) => {
    const config = SPECIES_CONFIG[species] ?? {
      label: species,
      uom: null,
      pollutant_label: species.toLowerCase(),
      observed_property_code: species.toLowerCase().replace(/[^a-z0-9]+/g, "_"),
      observed_property_domain: "aq" as const,
    };
    return {
      connector_id: connectorId,
      label: config.label,
      source_label: `laqn:${species}`,
      notation: species,
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
}

async function fetchPhenomenaIds(
  connectorId: number,
  speciesList: string[],
): Promise<Record<string, number>> {
  const mapping: Record<string, number> = {};
  const sourceLabels = speciesList.map((species) => `laqn:${species}`);
  const { data, error } = await publicRpcRequest<
    Array<{ id: number; source_label?: string; eionet_uri?: string }>
  >(
    "uk_aq_rpc_phenomena_ids",
    {
      connector_id: connectorId,
      eionet_uris: sourceLabels,
    },
  );
  if (error) {
    throw new Error(`Phenomena fetch failed: ${error.message}`);
  }
  for (const row of data ?? []) {
    const sourceLabel = row.source_label ?? row.eionet_uri;
    if (sourceLabel) {
      mapping[String(sourceLabel)] = Number(row.id);
    }
  }
  return mapping;
}

async function upsertTimeseries(
  rows: Record<string, unknown>[],
): Promise<void> {
  if (!rows.length) {
    return;
  }
  const { error } = await postgrestRequest(
    "POST",
    "timeseries",
    { on_conflict: "connector_id,service_ref,timeseries_ref" },
    rows,
    UPSERT_PREFER,
  );
  if (error) {
    throw new Error(`Timeseries upsert failed: ${error.message}`);
  }
}

type TimeseriesMeta = {
  id: number;
  last_value_at: string | null;
};

async function fetchTimeseriesMeta(
  connectorId: number,
  timeseriesRefs: string[],
): Promise<Record<string, TimeseriesMeta>> {
  const mapping: Record<string, TimeseriesMeta> = {};
  for (const chunkRefs of chunk(timeseriesRefs, 200)) {
    const quoted = chunkRefs.map((value) => `"${value}"`);
    const { data, error } = await postgrestRequest<
      Array<{ id: number; timeseries_ref: string; last_value_at: string | null }>
    >(
      "GET",
      "timeseries",
      {
        select: "id,timeseries_ref,last_value_at",
        connector_id: `eq.${connectorId}`,
        service_ref: `eq.${LAQN_SERVICE_REF}`,
        timeseries_ref: `in.(${quoted.join(",")})`,
      },
    );
    if (error) {
      throw new Error(`Timeseries id fetch failed: ${error.message}`);
    }
    for (const row of data ?? []) {
      mapping[String(row.timeseries_ref)] = {
        id: Number(row.id),
        last_value_at: row.last_value_at ? String(row.last_value_at) : null,
      };
    }
  }
  return mapping;
}

async function upsertObservations(rows: Record<string, unknown>[]): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  const { error } = await postgrestRequest(
    "POST",
    "observations",
    { on_conflict: "connector_id,timeseries_id,observed_at" },
    rows,
    UPSERT_PREFER,
  );
  if (error) {
    throw new Error(`Observations upsert failed: ${error.message}`);
  }
  return rows.length;
}

function toObservsObservationRows(
  rows: Record<string, unknown>[],
  connectorId: number,
  timeseriesId: number,
): ObservsObservationRow[] {
  const observsRows: ObservsObservationRow[] = [];
  for (const row of rows) {
    const observedAt = String(row.observed_at ?? "").trim();
    if (!observedAt) {
      continue;
    }
    const numericValue = Number(row.value);
    observsRows.push({
      connector_id: connectorId,
      timeseries_id: timeseriesId,
      observed_at: observedAt,
      value: Number.isFinite(numericValue) ? numericValue : null,
      status: row.status == null ? null : String(row.status),
    });
  }
  return observsRows;
}

async function updateTimeseriesLastValues(
  rows: Array<{ id: number; last_value: number; last_value_at: string }>,
): Promise<number> {
  let updated = 0;
  for (const row of rows) {
    const { error } = await postgrestRequest(
      "PATCH",
      "timeseries",
      { id: `eq.${row.id}` },
      { last_value: row.last_value, last_value_at: row.last_value_at },
      "return=minimal",
    );
    if (!error) {
      updated += 1;
    }
  }
  return updated;
}

async function updateConnectorLastPolled(connectorId: string): Promise<void> {
  const { error } = await postgrestRequest(
    "PATCH",
    "connectors",
    { id: `eq.${connectorId}` },
    { last_polled_at: new Date().toISOString() },
    "return=minimal",
  );
  if (error) {
    throw new Error(`Failed to update connectors.last_polled_at: ${error.message}`);
  }
}

async function upsertErgLaqnStationCheckpoints(
  rows: Array<{ station_id: number; last_polled_at: string; updated_at: string }>,
): Promise<void> {
  if (!rows.length) {
    return;
  }
  const { error } = await postgrestRequest(
    "POST",
    "erg_laqn_station_checkpoints",
    { on_conflict: "station_id" },
    rows,
    UPSERT_PREFER,
    UK_AQ_RAW_SCHEMA,
  );
  if (error) {
    throw new Error(`ERG LAQN checkpoint upsert failed: ${error.message}`);
  }
}

function loadDropboxConfig(): DropboxConfig | null {
  if (!DROPBOX_APP_KEY || !DROPBOX_APP_SECRET || !DROPBOX_REFRESH_TOKEN) {
    return null;
  }
  if (!DROPBOX_ALLOWED_SUPABASE_URL || DROPBOX_ALLOWED_SUPABASE_URL !== SUPABASE_URL) {
    return null;
  }
  return {
    appKey: DROPBOX_APP_KEY,
    appSecret: DROPBOX_APP_SECRET,
    refreshToken: DROPBOX_REFRESH_TOKEN,
  };
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
  if (!UK_AQ_DROPBOX_ROOT) {
    return cleaned;
  }
  if (!cleaned) {
    return UK_AQ_DROPBOX_ROOT;
  }
  if (
    cleaned === UK_AQ_DROPBOX_ROOT ||
    cleaned.startsWith(`${UK_AQ_DROPBOX_ROOT}/`)
  ) {
    return cleaned;
  }
  return `${UK_AQ_DROPBOX_ROOT}${cleaned}`;
}

function normalizeConnectorPrefix(connectorCode: string | null): string {
  const cleaned = (connectorCode ?? "").trim().toLowerCase();
  const normalized = cleaned.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
  return normalized || "unknown";
}

function buildDropboxLogPath(
  connectorCode: string | null,
  timestamp: Date,
): string {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_LOG_FOLDER}/${dateFolder}/uk_aq_log_edge_${prefix}_${stamp}.log`;
}

function buildDropboxRawPath(
  connectorCode: string | null,
  timestamp: Date,
): string {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_RAW_FOLDER}/${dateFolder}/uk_aq_raw_edge_${prefix}_${stamp}.zip`;
}

function buildDropboxErrorPath(
  errorId: string,
  createdAt: string,
  connectorCode: string | null,
): string {
  const dateFolder = createdAt.slice(0, 10);
  const stamp = formatCompactTimestamp(new Date(createdAt));
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_ERROR_FOLDER}/${dateFolder}/uk_aq_error_edge_${prefix}_${stamp}_${errorId}.json`;
}

function formatCompactTimestamp(timestamp: Date): string {
  return timestamp.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
}

function formatDateYmd(timestamp: Date): string {
  return timestamp.toISOString().slice(0, 10);
}

async function uploadDropboxLog(
  accessToken: string,
  log: LogBuffer,
  connectorId: string | null,
  connectorCode: string | null,
  errorLogger: { logError: (entry: ErrorLogEntry) => Promise<void> },
  refreshToken?: () => Promise<string>,
): Promise<string> {
  if (!accessToken) {
    return accessToken;
  }
  const content = log.lines.join("\n") + "\n";
  if (!content.trim()) {
    return accessToken;
  }
  try {
    const logPath = buildDropboxLogPath(connectorCode, new Date());
    accessToken = await dropboxUploadFileWithRetry(
      accessToken,
      logPath,
      content,
      refreshToken,
    );
    await dropboxArchiveLogs(accessToken, DROPBOX_LOG_FOLDER, DROPBOX_LOG_RETENTION_DAYS, 365);
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
  try {
    const rawPath = buildDropboxRawPath(connectorCode, new Date());
    const filename = rawPath.split("/").pop() ?? "uk_aq_raw_edge.jsonl";
    const jsonlName = filename.replace(/\.zip$/i, ".jsonl");
    const zipped = await zipTextCompressed(jsonlName, content);
    accessToken = await dropboxUploadFileWithRetry(
      accessToken,
      rawPath,
      zipped,
      refreshToken,
    );
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
        error: err instanceof Error ? err.message : String(err),
      },
      connector_code: connectorCode ?? null,
      connector_id: connectorId ?? null,
    });
  }
  return accessToken;
}

async function uploadErgDailyCsvs(
  accessToken: string,
  connectorId: string | null,
  stationIdRaw: string | null,
  stationRefRaw: string | null,
  speciesList: string[],
  log: LogBuffer,
  errorLogger: { logError: (entry: ErrorLogEntry) => Promise<void> },
  refreshToken?: () => Promise<string>,
): Promise<string> {
  if (!accessToken || !connectorId) {
    return accessToken;
  }
  const station = await loadCsvStation(connectorId, stationIdRaw, stationRefRaw);
  if (!station) {
    log.warn("CSV station not resolved; skipping ERG daily CSV upload.", {
      station_id: stationIdRaw,
      station_ref: stationRefRaw,
    });
    return accessToken;
  }
  const todayStart = utcDayStart(new Date());
  const tomorrowStart = addUtcDays(todayStart, 1);
  const dateStamp = formatUtcDate(todayStart).replace(/-/g, "");
  const startIso = todayStart.toISOString();
  const endIso = tomorrowStart.toISOString();
  const seriesList = await loadTimeseriesByStation(connectorId, station.id);
  const seriesByPollutant = new Map<string, string>();
  for (const series of seriesList) {
    if (!series.pollutant) {
      continue;
    }
    if (!seriesByPollutant.has(series.pollutant)) {
      seriesByPollutant.set(series.pollutant, series.id);
    }
  }
  for (const species of speciesList) {
    const normalized = normalizePollutant(
      SPECIES_CONFIG[species]?.pollutant_label ?? species,
    );
    if (!normalized) {
      continue;
    }
    const timeseriesId = seriesByPollutant.get(normalized);
    if (!timeseriesId) {
      log.warn("CSV timeseries missing for pollutant.", { pollutant: normalized });
      continue;
    }
    const observations = await loadObservationsForToday(timeseriesId, startIso, endIso);
    const header = "station.id,station_ref,label,timeseries_id,value,observed_at";
    const rows = observations.map((obs) =>
      [
        csvEscape(station.id),
        csvEscape(station.station_ref),
        csvEscape(station.label),
        csvEscape(timeseriesId),
        csvEscape(obs.value),
        csvEscape(obs.observed_at),
      ].join(",")
    );
    const contents = [header, ...rows].join("\n");
    const dropboxPath = buildErgCsvPath(normalized, dateStamp);
    try {
      accessToken = await dropboxUploadOverwriteWithRetry(
        accessToken,
        dropboxPath,
        contents,
        refreshToken,
      );
      log.info("ERG daily CSV uploaded.", {
        pollutant: normalized,
        dropbox_path: dropboxPath,
        row_count: observations.length,
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      log.warn("ERG daily CSV upload failed.", { pollutant: normalized, error: message });
      await errorLogger.logError({
        source: "edge",
        severity: "warn",
        message: "ERG daily CSV upload failed.",
        context: {
          connector_id: connectorId,
          pollutant: normalized,
          dropbox_path: dropboxPath,
          error: message,
        },
        connector_id: connectorId,
      });
    }
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
          entry.connector_code ?? LAQN_CONNECTOR_CODE,
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

async function dropboxUploadFileOverwrite(
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
        mode: "overwrite",
        autorename: false,
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

async function dropboxUploadOverwriteWithRetry(
  accessToken: string,
  path: string,
  contents: string | Uint8Array,
  refreshToken?: () => Promise<string>,
): Promise<string> {
  try {
    await dropboxUploadFileOverwrite(accessToken, path, contents);
    return accessToken;
  } catch (err) {
    if (err instanceof DropboxHttpError && err.status === 401 && refreshToken) {
      const refreshed = await refreshToken();
      await dropboxUploadFileOverwrite(refreshed, path, contents);
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

  // Local file header.
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

  // Central directory header.
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
  e32(centralHeader.length);
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

async function dropboxArchiveLogs(
  accessToken: string,
  folder: string,
  days: number,
  archiveDays: number,
): Promise<void> {
  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
  const archiveCutoff = Date.now() - archiveDays * 24 * 60 * 60 * 1000;
  const archiveFolder = `${folder}/archive`;
  const listFolder = async (path: string): Promise<Array<Record<string, unknown>>> => {
    let payload: Record<string, unknown> = { path };
    const entries: Array<Record<string, unknown>> = [];
    while (true) {
      const resp = await fetch(DROPBOX_LIST_FOLDER_URL, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      });
      if (resp.status === 409) {
        return [];
      }
      if (!resp.ok) {
        throw new Error(`Dropbox list_folder failed (${resp.status})`);
      }
      const data = await resp.json();
      entries.push(...(Array.isArray(data?.entries) ? data.entries : []));
      if (!data?.has_more) {
        return entries;
      }
      payload = { cursor: data.cursor };
    }
  };

  const [rootEntries, archiveEntries] = await Promise.all([
    listFolder(folder),
    listFolder(archiveFolder),
  ]);
  const archiveNames = new Set(
    archiveEntries
      .filter((entry) => entry?.[".tag"] === "file")
      .map((entry) => String(entry?.name ?? "")),
  );

  for (const entry of rootEntries) {
    if (entry?.[".tag"] !== "folder") {
      continue;
    }
    const folderName = String(entry?.name ?? "");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(folderName)) {
      continue;
    }
    const timestamp = Date.parse(`${folderName}T00:00:00Z`);
    if (!Number.isFinite(timestamp)) {
      continue;
    }
    if (timestamp > cutoff) {
      continue;
    }
    const folderPath = `${folder}/${folderName}`;
    if (timestamp < archiveCutoff) {
      await dropboxDeletePath(accessToken, folderPath);
      continue;
    }
    const archiveName = `${folderName}.zip`;
    if (archiveNames.has(archiveName)) {
      await dropboxDeletePath(accessToken, folderPath);
      continue;
    }
    const zipPayload = await dropboxDownloadZip(accessToken, folderPath);
    await dropboxUploadFile(accessToken, `${archiveFolder}/${archiveName}`, new Uint8Array(zipPayload));
    await dropboxDeletePath(accessToken, folderPath);
  }
}

async function dropboxDownloadZip(accessToken: string, path: string): Promise<ArrayBuffer> {
  const resp = await fetch(DROPBOX_DOWNLOAD_ZIP_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Dropbox-API-Arg": JSON.stringify({ path }),
    },
  });
  if (!resp.ok) {
    throw new Error(`Dropbox download_zip failed (${resp.status})`);
  }
  return await resp.arrayBuffer();
}

async function dropboxDeletePath(accessToken: string, path: string): Promise<void> {
  const resp = await fetch(DROPBOX_DELETE_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ path }),
  });
  if (!resp.ok) {
    throw new Error(`Dropbox delete failed (${resp.status})`);
  }
}

serve(async (req) => {
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }
  const authResponse = requireCronSecret(req);
  if (authResponse) {
    return authResponse;
  }

  const log = createLogBuffer();
  const dropboxConfig = loadDropboxConfig();
  const errorLogger = ERROR_LOGGER;
  const rawRecorder = dropboxConfig ? createRawRecorder() : null;
  let status = 200;
  let responsePayload: Record<string, unknown> = {};
  let connectorId: string | null = null;
  let connectorCodeForLog: string;
  let skippedHttp400 = 0;
  let observsWritten = 0;
  let observsReceiptsUpserted = 0;
  let observsEnqueued = 0;
  let observsFlushes = 0;
  const runStartedAt = Date.now();
  const maxRuntimeSeconds = Number.isFinite(LAQN_MAX_RUNTIME_SECONDS)
    ? Math.max(30, LAQN_MAX_RUNTIME_SECONDS)
    : DEFAULT_MAX_RUNTIME_SECONDS;
  const runtimeDeadline = runStartedAt + maxRuntimeSeconds * 1000;
  const shouldStop = () => Date.now() >= runtimeDeadline;

  let payload: PollRequest = {};
  try {
    payload = await req.json();
  } catch {
    payload = {};
  }

  const connectorCode = payload.connector_code ?? LAQN_CONNECTOR_CODE;
  const connectorLabel = payload.connector_label ?? LAQN_CONNECTOR_LABEL;
  const connectorDisplayName = payload.connector_display_name ?? LAQN_CONNECTOR_DISPLAY_NAME;
  const serviceRef = payload.service_ref ?? LAQN_SERVICE_REF;
  const baseUrl = (payload.base_url ?? LAQN_BASE_URL).replace(/\/$/, "");
  const groupName = payload.group ?? LAQN_DEFAULT_GROUP;
  const stationRefs = parseStationRefs(payload.station_refs);
  const speciesList = parseSpeciesList(payload.species);
  const days = payload.days ?? DEFAULT_DAYS;
  const batchSize = payload.batch_size ?? DEFAULT_BATCH_SIZE;
  const observsBufferFlushRows = (() => {
    const parsed = Number(Deno.env.get("OBSERVS_BUFFER_FLUSH_ROWS") ?? "");
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return DEFAULT_OBSERVS_BUFFER_FLUSH_ROWS;
    }
    return Math.max(1, Math.trunc(parsed));
  })();
  const sleepSeconds = payload.sleep_seconds ?? DEFAULT_SLEEP_SECONDS;
  const dryRun = Boolean(payload.dry_run);
  const startFromLatest = Boolean(payload.start_from_latest);
  const csvStationIdRaw = String(payload.csv_station_id ?? LAQN_CSV_STATION_ID).trim() || null;
  const csvStationRefRaw = String(payload.csv_station_ref ?? LAQN_CSV_STATION_REF)
    .trim()
    .toUpperCase() || null;
  connectorCodeForLog = connectorCode;

  const now = new Date();
  const {
    startDate,
    endDate: _endDate,
    startDateStr: startStr,
    endDateStr: endStr,
    utcTodayStart,
  } = buildErgDateRange({
    now,
    startDateOverride: parseDate(payload.start_date),
    endDateOverride: parseDate(payload.end_date),
    days,
  });

  try {
    if (!SUPABASE_URL || !SUPABASE_PRIVILEGED_KEY) {
      status = 500;
      responsePayload = { error: "Missing SUPABASE_URL or SB_SECRET_KEY." };
      log.error("Missing Supabase configuration.");
    } else if (!speciesList.length) {
      status = 400;
      responsePayload = { error: "No species specified." };
      log.warn("No species specified.");
    } else {
      log.info("Poll request", {
        connector_code: connectorCode,
        connector_label: connectorLabel,
        connector_display_name: connectorDisplayName,
        service_ref: serviceRef,
        group: groupName,
        days,
        start_date: startStr,
        end_date: endStr,
        start_from_latest: startFromLatest,
        station_refs: stationRefs.length ? stationRefs : null,
        species: speciesList,
        dry_run: dryRun,
      });
      if (rawRecorder) {
        rawRecorder.recordEvent("context", {
          connector_code: connectorCode,
          connector_label: connectorLabel,
          connector_display_name: connectorDisplayName,
          service_ref: serviceRef,
          group: groupName,
          days,
          start_date: startStr,
          end_date: endStr,
          start_from_latest: startFromLatest,
          station_refs: stationRefs.length ? stationRefs : null,
          species: speciesList,
          dry_run: dryRun,
        });
      }
    const connector = await loadConnector(connectorCode);
    connectorId = connector?.id ?? null;
    if (!connectorId) {
      throw new Error("Connector not found.");
    }

    const stationsPayload = await fetchJson(
      `${baseUrl}/Information/MonitoringSites/GroupName=${groupName}/Json`,
      undefined,
      log,
      rawRecorder,
    );
    const rawStations = extractStations(stationsPayload);
    const filteredStations = stationRefs.length
      ? rawStations.filter((station) => {
        const ref = String(
          pickValue(station, ["SiteCode", "SiteID", "SiteId", "Site"]) ?? ""
        ).trim().toUpperCase();
        return stationRefs.includes(ref);
      })
      : rawStations;

    const stationRows = filteredStations.map((station) =>
      normalizeStation(station, Number(connectorId))
    ).filter((row) => row) as Record<string, unknown>[];
    if (!stationRows.length) {
      status = 200;
      responsePayload = { warning: "No stations selected." };
      log.warn("No stations selected.");
    } else {
      if (!dryRun) {
        await upsertStations(stationRows);
      }
      const stationIdMap = await fetchStationIds(
        Number(connectorId),
        stationRows.map((row) => String(row.station_ref)),
      );
      if (!Object.keys(stationIdMap).length) {
        status = 200;
        responsePayload = { warning: "No station ids resolved." };
        log.warn("No station ids resolved.");
      } else {
        if (!dryRun) {
          await upsertPhenomena(Number(connectorId), speciesList);
        }
        const phenomenonIds = await fetchPhenomenaIds(Number(connectorId), speciesList);

        const timeseriesRows: Record<string, unknown>[] = [];
        for (const row of stationRows) {
          const stationRef = String(row.station_ref);
          const stationId = stationIdMap[stationRef];
          if (!stationId) {
            continue;
          }
          const stationName = String(row.station_name ?? row.label ?? stationRef);
          for (const species of speciesList) {
            const config = SPECIES_CONFIG[species] ?? { label: species, uom: null };
            timeseriesRows.push({
              timeseries_ref: `${stationRef}:${species}`,
              label: `${stationName} ${config.label}`,
              uom: config.uom ?? null,
              station_id: stationId,
              service_ref: serviceRef,
              connector_id: Number(connectorId),
              phenomenon_id: phenomenonIds[`laqn:${species}`] ?? null,
              extras: { site_code: stationRef, species },
            });
          }
        }
        if (!dryRun) {
          await upsertTimeseries(timeseriesRows);
        }
        const timeseriesMetaMap = await fetchTimeseriesMeta(
          Number(connectorId),
          timeseriesRows.map((row) => String(row.timeseries_ref)),
        );

        let observationsUpserted = 0;
        const latestObservedSummary: Record<string, Record<string, string | null>> = {};
        const timeseriesUpdates: Array<{ id: number; last_value: number; last_value_at: string }> = [];
        const observsRowsPending: ObservsObservationRow[] = [];
        const pollTimestamp = new Date().toISOString();
        const checkpointRows: Array<{ station_id: number; last_polled_at: string; updated_at: string }> = [];
        let backfillSeries = 0;
        let backfillEarliest: Date | null = null;
        let timeBudgetHit = false;
        let stationsProcessed = 0;
        const flushPendingObservsRows = async (
          force = false,
          reason = "threshold",
        ) => {
          if (!observsRowsPending.length || dryRun) {
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
          }
        };

        for (const row of stationRows) {
          if (shouldStop()) {
            timeBudgetHit = true;
            break;
          }
          const stationRef = String(row.station_ref);
          const stationId = stationIdMap[stationRef];
          if (!stationId) {
            continue;
          }
          for (const species of speciesList) {
            if (shouldStop()) {
              timeBudgetHit = true;
              break;
            }
            const timeseriesRef = `${stationRef}:${species}`;
            const timeseriesMeta = timeseriesMetaMap[timeseriesRef];
            const timeseriesId = timeseriesMeta?.id;
            if (!timeseriesId) {
              continue;
            }
            let seriesStartDate = startDate;
            if (startFromLatest) {
              const lastValueAt = parseTimestamp(timeseriesMeta?.last_value_at ?? null);
              if (lastValueAt && lastValueAt < seriesStartDate) {
                seriesStartDate = lastValueAt;
                backfillSeries += 1;
                if (!backfillEarliest || lastValueAt < backfillEarliest) {
                  backfillEarliest = lastValueAt;
                }
              }
            }
            const seriesStartStr = formatUtcDate(seriesStartDate);
            const url =
              `${baseUrl}/Data/SiteSpecies/SiteCode=${stationRef}/SpeciesCode=${species}` +
              `/StartDate=${seriesStartStr}/EndDate=${endStr}/Json`;
            let payloadData: unknown;
            try {
              payloadData = await fetchJson(url, undefined, log, rawRecorder);
            } catch (err) {
              const message = err instanceof Error ? err.message : String(err);
              if (message.startsWith("HTTP 400 ")) {
                skippedHttp400 += 1;
                if (skippedHttp400 <= 5) {
                  log.warn("Skipping site/species due to HTTP 400.", {
                    station_ref: stationRef,
                    species,
                    url,
                  });
                  await errorLogger.logError({
                    source: "edge",
                    severity: "warn",
                    message: "ERG LAQN 400 for site/species.",
                    stack: err instanceof Error ? err.stack : undefined,
                    context: {
                      connector_code: connectorCodeForLog,
                      station_ref: stationRef,
                      species,
                      url,
                      error: message,
                    },
                    connector_code: connectorCodeForLog,
                    connector_id: connectorId,
                    station_id: stationId,
                    timeseries_id: timeseriesId,
                  });
                }
                continue;
              }
              throw err;
            }
            const rawRows = extractObservations(payloadData);
            const observations: Record<string, unknown>[] = [];
            let lastObserved: Date | null = null;
            let lastValue: number | null = null;
            for (const entry of rawRows) {
              const observedAt = parseObservationDate(
                entry["@MeasurementDateGMT"]
                  ?? entry["@MeasurementDate"]
                  ?? entry["DateTimeGMT"]
                  ?? entry["DateTime"]
                  ?? entry["Date"]
              );
              const value = Number(
                entry["@Value"] ?? entry["Value"] ?? entry["ScaledValue"] ?? entry["RawValue"]
              );
              if (!observedAt || Number.isNaN(value)) {
                continue;
              }
              observations.push({
                connector_id: Number(connectorId),
                timeseries_id: timeseriesId,
                observed_at: observedAt.toISOString(),
                value,
              });
              if (!lastObserved || observedAt > lastObserved) {
                lastObserved = observedAt;
                lastValue = value;
              }
            }
            if (lastObserved && lastObserved < utcTodayStart) {
              log.warn("ERG LAQN observations missing today.", {
                station_ref: stationRef,
                species,
                start_date: seriesStartStr,
                end_date: endStr,
                last_observed_at: lastObserved.toISOString(),
              });
            }
            if (observations.length && !dryRun) {
              for (const batch of chunk(observations, batchSize)) {
                observationsUpserted += await upsertObservations(batch);
                observsRowsPending.push(
                  ...toObservsObservationRows(
                    batch,
                    Number(connectorId),
                    timeseriesId,
                  ),
                );
                await flushPendingObservsRows(false, "batch_threshold");
              }
            }
            if (lastObserved && lastValue !== null) {
              const currentLast = parseTimestamp(timeseriesMeta?.last_value_at ?? null);
              if (!currentLast || lastObserved > currentLast) {
                timeseriesUpdates.push({
                  id: timeseriesId,
                  last_value: lastValue,
                  last_value_at: lastObserved.toISOString(),
                });
              }
            }
            if (!latestObservedSummary[stationRef]) {
              latestObservedSummary[stationRef] = {};
            }
            latestObservedSummary[stationRef][species] = lastObserved
              ? lastObserved.toISOString()
              : null;
            if (sleepSeconds) {
              await sleep(sleepSeconds);
            }
          }
          if (timeBudgetHit) {
            break;
          }
          checkpointRows.push({
            station_id: stationId,
            last_polled_at: pollTimestamp,
            updated_at: pollTimestamp,
          });
          stationsProcessed += 1;
        }

        if (timeBudgetHit) {
          log.warn("Stopping ERG LAQN poll early (runtime budget exceeded).", {
            max_runtime_seconds: maxRuntimeSeconds,
          });
        }
        await flushPendingObservsRows(true, "run_complete");

        let timeseriesUpdated = 0;
        let checkpointsUpserted = 0;
        if (!dryRun && timeseriesUpdates.length) {
          timeseriesUpdated = await updateTimeseriesLastValues(timeseriesUpdates);
        }
        if (!dryRun && checkpointRows.length) {
          await upsertErgLaqnStationCheckpoints(checkpointRows);
          checkpointsUpserted = checkpointRows.length;
        }
        if (!dryRun) {
          await updateConnectorLastPolled(connectorId);
        }

        responsePayload = {
          connector_id: connectorId,
          group: groupName,
          stations: stationRows.length,
          stations_processed: stationsProcessed,
          species: speciesList,
          observations_upserted: observationsUpserted,
          observs_written: observsWritten,
          observs_receipts_upserted: observsReceiptsUpserted,
          observs_enqueued: observsEnqueued,
          observs_flushes: observsFlushes,
          timeseries_updated: timeseriesUpdated,
          checkpoints_upserted: checkpointsUpserted,
          skipped_http_400: skippedHttp400,
          start_from_latest: startFromLatest,
          backfill_series: startFromLatest ? backfillSeries : null,
          backfill_earliest: startFromLatest && backfillEarliest
            ? backfillEarliest.toISOString()
            : null,
          latest_observed_by_station_species: latestObservedSummary,
          dry_run: dryRun,
          partial: timeBudgetHit,
          stopped_reason: timeBudgetHit ? "runtime_budget_exceeded" : null,
        };
        log.info("Stations polled.", {
          stations_selected: stationRows.length,
          stations_processed: stationsProcessed,
          partial: timeBudgetHit,
        });
        log.info("Poll complete.", {
          connector_id: connectorId,
          stations: stationRows.length,
          observations_upserted: observationsUpserted,
          observs_written: observsWritten,
          observs_receipts_upserted: observsReceiptsUpserted,
          observs_enqueued: observsEnqueued,
          observs_flushes: observsFlushes,
          timeseries_updated: timeseriesUpdated,
          skipped_http_400: skippedHttp400,
          start_from_latest: startFromLatest,
          backfill_series: startFromLatest ? backfillSeries : null,
          backfill_earliest: startFromLatest && backfillEarliest
            ? backfillEarliest.toISOString()
            : null,
          dry_run: dryRun,
          partial: timeBudgetHit,
          stopped_reason: timeBudgetHit ? "runtime_budget_exceeded" : null,
        });
      }
    }
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    status = 500;
    responsePayload = { error: "Internal server error." };
    log.error("Poll failed.", { error: message });
    await errorLogger.logError({
      source: "edge",
      severity: "error",
      message: "ERG LAQN ingest failed.",
      stack: err instanceof Error ? err.stack : undefined,
      context: {
        connector_code: connectorCodeForLog,
        group: groupName,
        error: message,
      },
      connector_code: connectorCodeForLog,
      connector_id: connectorId,
    });
  }

  if (dropboxConfig) {
    const refreshDropbox = () => dropboxRefreshAccessToken(dropboxConfig);
    try {
      let accessToken = await dropboxRefreshAccessToken(dropboxConfig);
      accessToken = await uploadDropboxLog(
        accessToken,
        log,
        connectorId,
        connectorCodeForLog,
        errorLogger,
        refreshDropbox,
      );
      accessToken = await uploadDropboxRaw(
        accessToken,
        rawRecorder,
        connectorId,
        connectorCodeForLog,
        errorLogger,
        refreshDropbox,
      );
      if (!dryRun) {
        await uploadErgDailyCsvs(
          accessToken,
          connectorId,
          csvStationIdRaw,
          csvStationRefRaw,
          speciesList,
          log,
          errorLogger,
          refreshDropbox,
        );
      }
    } catch (err) {
      log.warn("Dropbox logging skipped due to token error.", {
        error: err instanceof Error ? err.message : String(err),
      });
      await errorLogger.logError({
        source: "edge",
        severity: "error",
        message: "Dropbox token request failed.",
        stack: err instanceof Error ? err.stack : undefined,
        context: {
          connector_id: connectorId,
          connector_code: connectorCodeForLog,
        },
        connector_code: connectorCodeForLog,
        connector_id: connectorId,
      });
    }
  }

  return new Response(JSON.stringify(responsePayload), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...cacheControlHeaders(status),
    },
  });
});
