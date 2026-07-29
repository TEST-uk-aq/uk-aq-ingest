// trigger deploy 2026-02-09 12:36
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import "../_shared/fetch_egress_patch.ts";
import { cacheControlHeaders } from "../_shared/cache.ts";
import {
  type ObservsObservationRow,
  writeObservsWithOutbox,
} from "../_shared/observs_client.ts";

type PollRequest = {
  connector_id?: string;
  connector_code?: string;
  connector_label?: string;
  service_ref?: string;
  country?: string;
  base_url?: string;
  no_filter?: boolean;
};

type ConnectorRow = {
  id: string;
  connector_code: string;
  label: string;
  service_url: string | null;
  overwrite_station_name?: boolean | null;
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

const DEFAULT_BASE_URL = "https://data.sensor.community";
const DEFAULT_CONNECTOR_CODE = "sensorcommunity";
const DEFAULT_SERVICE_LABEL = "Sensor.Community";
const DEFAULT_COUNTRY = "GB";
const DEFAULT_USER_AGENT = "uk-air-quality-networks";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RUNTIME_SECONDS = 130;
const DEFAULT_RESPONSE_BUFFER_MS = 10_000;
const DEFAULT_OBSERVATION_UPSERT_CHUNK_SIZE = 1000;
const MIN_FETCH_TIMEOUT_MS = 1_000;
const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);

const UK_BBOX = {
  west: -11.0,
  south: 49.0,
  east: 2.0,
  north: 61.0,
};

const BASE_VALUE_TYPE_MAP: Record<
  string,
  { pollutant: string; label: string; uom: string }
> = {
  P1: { pollutant: "pm10", label: "PM10", uom: "ug/m3" },
  P2: { pollutant: "pm2.5", label: "PM2.5", uom: "ug/m3" },
};

const BASE_SCOMM_PHENOMENA: Record<
  string,
  {
    source_label: string;
    label: string;
    notation: string;
    pollutant_label: string;
    observed_property_code: string;
    observed_property_domain: "aq" | "met";
  }
> = {
  pm10: {
    source_label: "sensorcommunity:pm10",
    label: "PM10",
    notation: "PM10",
    pollutant_label: "pm10",
    observed_property_code: "pm10",
    observed_property_domain: "aq",
  },
  "pm2.5": {
    source_label: "sensorcommunity:pm2.5",
    label: "PM2.5",
    notation: "PM2.5",
    pollutant_label: "pm2.5",
    observed_property_code: "pm25",
    observed_property_domain: "aq",
  },
};

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ??
  Deno.env.get("SB_SUPABASE_URL") ??
  "";
const SB_SECRET_KEY = Deno.env.get("SB_SECRET_KEY") ?? "";
const SUPABASE_PRIVILEGED_KEY = SB_SECRET_KEY;
const UK_AQ_CORE_SCHEMA = Deno.env.get("UK_AQ_CORE_SCHEMA") ??
  "uk_aq_core";
const UK_AQ_RAW_SCHEMA = Deno.env.get("UK_AQ_RAW_SCHEMA") ??
  "uk_aq_raw";

const SCOMM_BASE_URL = (Deno.env.get("SCOMM_BASE_URL") ?? DEFAULT_BASE_URL)
  .replace(/\/$/, "");
const SCOMM_CONNECTOR_CODE = Deno.env.get("SCOMM_CONNECTOR_CODE") ??
  Deno.env.get("SCOMM_CONNECTOR_REF") ??
  Deno.env.get("SCOMM_SERVICE_REF") ??
  DEFAULT_CONNECTOR_CODE;
const SCOMM_SERVICE_REF = Deno.env.get("SCOMM_SERVICE_REF") ??
  SCOMM_CONNECTOR_CODE;
const SCOMM_SERVICE_LABEL = Deno.env.get("SCOMM_SERVICE_LABEL") ??
  Deno.env.get("SCOMM_CONNECTOR_LABEL") ??
  DEFAULT_SERVICE_LABEL;
const SCOMM_COUNTRY = Deno.env.get("SCOMM_COUNTRY") ?? DEFAULT_COUNTRY;
const SCOMM_USER_AGENT = Deno.env.get("SCOMM_USER_AGENT") ?? DEFAULT_USER_AGENT;
const SCOMM_INGEST_MET_FIELDS = parseBool(
  Deno.env.get("SCOMM_INGEST_MET_FIELDS"),
  false,
);
const SCOMM_MAX_RUNTIME_SECONDS = Number(
  Deno.env.get("SCOMM_MAX_RUNTIME_SECONDS") ?? DEFAULT_MAX_RUNTIME_SECONDS,
);
const SCOMM_RESPONSE_BUFFER_MS = Number(
  Deno.env.get("SCOMM_RESPONSE_BUFFER_MS") ?? DEFAULT_RESPONSE_BUFFER_MS,
);
const SCOMM_OBSERVATION_UPSERT_CHUNK_SIZE = parsePositiveInt(
  Deno.env.get("SCOMM_OBSERVATION_UPSERT_CHUNK_SIZE"),
  DEFAULT_OBSERVATION_UPSERT_CHUNK_SIZE,
);
const SB_UK_AQ_CRON_SECRET = Deno.env.get("SB_UK_AQ_CRON_SECRET") ?? "";

const VALUE_TYPE_MAP: Record<
  string,
  { pollutant: string; label: string; uom: string }
> = {
  ...BASE_VALUE_TYPE_MAP,
  ...(SCOMM_INGEST_MET_FIELDS
    ? {
      temperature: {
        pollutant: "temperature",
        label: "Temperature",
        uom: "degC",
      },
      humidity: { pollutant: "humidity", label: "Humidity", uom: "%" },
      pressure: { pollutant: "pressure", label: "Pressure", uom: "hPa" },
    }
    : {}),
};

const SCOMM_PHENOMENA: Record<
  string,
  {
    source_label: string;
    label: string;
    notation: string;
    pollutant_label: string;
    observed_property_code: string;
    observed_property_domain: "aq" | "met";
  }
> = {
  ...BASE_SCOMM_PHENOMENA,
  ...(SCOMM_INGEST_MET_FIELDS
    ? {
      temperature: {
        source_label: "sensorcommunity:temperature",
        label: "Temperature",
        notation: "temperature",
        pollutant_label: "temperature",
        observed_property_code: "temperature",
        observed_property_domain: "met",
      },
      humidity: {
        source_label: "sensorcommunity:humidity",
        label: "Humidity",
        notation: "humidity",
        pollutant_label: "humidity",
        observed_property_code: "humidity",
        observed_property_domain: "met",
      },
      pressure: {
        source_label: "sensorcommunity:pressure",
        label: "Pressure",
        notation: "pressure",
        pollutant_label: "pressure",
        observed_property_code: "pressure",
        observed_property_domain: "met",
      },
    }
    : {}),
};

const DROPBOX_APP_KEY = Deno.env.get("DROPBOX_APP_KEY") ?? "";
const DROPBOX_APP_SECRET = Deno.env.get("DROPBOX_APP_SECRET") ?? "";
const DROPBOX_REFRESH_TOKEN = Deno.env.get("DROPBOX_REFRESH_TOKEN") ?? "";
const DROPBOX_ALLOWED_SUPABASE_URL =
  Deno.env.get("SCOMM_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ??
    Deno.env.get("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ??
    "";
const DROPBOX_ERROR_ALLOWED_SUPABASE_URL =
  Deno.env.get("SCOMM_ERROR_DROPBOX_ALLOWED_SUPABASE_URL") ??
    Deno.env.get("UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL") ??
    "";
const DROPBOX_ROOT_FOLDER = (() => {
  const raw = Deno.env.get("UK_AQ_DROPBOX_ROOT") ?? "";
  return normalizeDropboxPath(raw);
})();

const DROPBOX_LOG_FOLDER = dropboxWithRoot("/connectors/sensorcommunity/log");
const DROPBOX_RAW_FOLDER = dropboxWithRoot(
  "/connectors/sensorcommunity/raw_data",
);
const DROPBOX_ERROR_FOLDER = dropboxWithRoot(
  Deno.env.get("SCOMM_ERROR_DROPBOX_FOLDER") ??
    Deno.env.get("UK_AIR_ERROR_DROPBOX_FOLDER") ??
    "error_log",
);
const DROPBOX_LOG_RETENTION_DAYS = 31;
const DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token";
const DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
const DROPBOX_LIST_FOLDER_URL =
  "https://api.dropboxapi.com/2/files/list_folder";
const DROPBOX_DOWNLOAD_ZIP_URL =
  "https://content.dropboxapi.com/2/files/download_zip";
const DROPBOX_DELETE_URL = "https://api.dropboxapi.com/2/files/delete_v2";

const REST_BASE_URL = SUPABASE_URL
  ? `${SUPABASE_URL.replace(/\/$/, "")}/rest/v1`
  : "";

function parseBool(
  value: string | null | undefined,
  defaultValue = false,
): boolean {
  if (value === null || value === undefined) {
    return defaultValue;
  }
  const normalized = value.trim().toLowerCase();
  return ["1", "true", "yes", "y", "on"].includes(normalized);
}

function parsePositiveInt(
  value: string | null | undefined,
  fallback: number,
): number {
  const parsed = Number(value ?? "");
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.max(1, Math.trunc(parsed));
}

function postgrestHeaders(
  prefer?: string,
  schema = UK_AQ_CORE_SCHEMA,
): Record<string, string> {
  const headers: Record<string, string> = {
    apikey: SUPABASE_PRIVILEGED_KEY,
    "Content-Type": "application/json",
    "x-ukaq-egress-caller": "ingest_sensorcommunity",
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
      error: { message: "Missing SUPABASE_URL or SB_SECRET_KEY." },
    };
  }
  const url = new URL(`${REST_BASE_URL}/${table}`);
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  try {
    const resp = await fetch(url.toString(), {
      method,
      headers: postgrestHeaders(prefer, schema),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal,
    });
    let payload: unknown = null;
    if (resp.status !== 204) {
      const contentType = resp.headers.get("content-type") ?? "";
      payload = contentType.includes("application/json")
        ? await resp.json()
        : await resp.text();
    }
    if (!resp.ok) {
      const message = (payload as {
        message?: string;
        error_description?: string;
        error?: string;
      })?.message ??
        (payload as { error_description?: string })?.error_description ??
        (payload as { error?: string })?.error ??
        resp.statusText;
      return { data: null, error: { message: String(message) } };
    }
    return { data: payload as T, error: null };
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") {
      throw new Error("PostgREST request timed out.");
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
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
  const push = (
    level: string,
    message: string,
    context?: Record<string, unknown>,
  ) => {
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

function asNonEmptyString(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed || null;
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithRetry(
  url: string,
  attempts = 3,
  rawRecorder?: RawRecorder | null,
  deadlineMs = Number.POSITIVE_INFINITY,
): Promise<unknown> {
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const remainingBudgetMs = deadlineMs - Date.now();
    if (remainingBudgetMs <= MIN_FETCH_TIMEOUT_MS) {
      throw new Error(
        "Runtime budget exhausted before Sensor.Community fetch completed.",
      );
    }

    const timeoutMs = Math.max(
      MIN_FETCH_TIMEOUT_MS,
      Math.min(DEFAULT_TIMEOUT_MS, deadlineMs - Date.now() - 250),
    );

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const resp = await fetch(url, {
        headers: {
          "User-Agent": SCOMM_USER_AGENT,
          Accept: "application/json",
        },
        signal: controller.signal,
      });
      if (RETRYABLE_STATUS.has(resp.status)) {
        throw new Error(`Sensor.Community retryable status: ${resp.status}`);
      }
      if (!resp.ok) {
        throw new Error(`Sensor.Community error: ${resp.status}`);
      }
      const payload = await resp.json();
      if (rawRecorder) {
        const parsed = new URL(url);
        const params: Record<string, string> = {};
        parsed.searchParams.forEach((value, key) => {
          params[key] = value;
        });
        rawRecorder.recordResponse(
          parsed.pathname,
          params,
          resp.status,
          payload,
        );
      }
      return payload;
    } catch (err) {
      if (
        err instanceof DOMException &&
        err.name === "AbortError" &&
        deadlineMs - Date.now() <= MIN_FETCH_TIMEOUT_MS
      ) {
        throw new Error(
          "Runtime budget exhausted before Sensor.Community fetch completed.",
        );
      }
      if (attempt >= attempts) {
        throw err;
      }
      const retryDelayMs = Math.min(30_000, 2 ** attempt * 1000);
      const remainingBudgetMs = deadlineMs - Date.now();
      if (remainingBudgetMs <= retryDelayMs + MIN_FETCH_TIMEOUT_MS) {
        throw err;
      }
      await sleep(retryDelayMs);
    } finally {
      clearTimeout(timeout);
    }
  }
  return [];
}

function coerceNumber(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const num = typeof value === "number"
    ? value
    : Number.parseFloat(String(value));
  return Number.isFinite(num) ? num : null;
}

function maybeSwapCoords(
  lon: number | null,
  lat: number | null,
  bbox: { west: number; south: number; east: number; north: number } | null,
): [number | null, number | null] {
  if (lon === null || lat === null || !bbox) {
    return [lon, lat];
  }
  const lonLooksLat = bbox.south <= lon && lon <= bbox.north;
  const latLooksLon = bbox.west <= lat && lat <= bbox.east;
  const lonIsLon = bbox.west <= lon && lon <= bbox.east;
  const latIsLat = bbox.south <= lat && lat <= bbox.north;
  if (lonLooksLat && latLooksLon && !lonIsLon && !latIsLat) {
    return [lat, lon];
  }
  return [lon, lat];
}

function stationCoords(
  station: Record<string, unknown>,
  bbox: { west: number; south: number; east: number; north: number } | null =
    null,
): [number | null, number | null] {
  let coords: unknown = null;
  if (station.geometry && typeof station.geometry === "object") {
    coords = (station.geometry as Record<string, unknown>).coordinates;
  }
  if (!coords && station.properties && typeof station.properties === "object") {
    const geometry = (station.properties as Record<string, unknown>).geometry;
    if (geometry && typeof geometry === "object") {
      coords = (geometry as Record<string, unknown>).coordinates;
    }
  }
  if (Array.isArray(coords) && coords.length >= 2) {
    const lon = coerceNumber(coords[0]);
    const lat = coerceNumber(coords[1]);
    return maybeSwapCoords(lon, lat, bbox);
  }
  const props = (station.properties && typeof station.properties === "object")
    ? station.properties as Record<string, unknown>
    : {};
  const lon = coerceNumber(props.longitude ?? props.lon ?? props.lng);
  const lat = coerceNumber(props.latitude ?? props.lat);
  return maybeSwapCoords(lon, lat, bbox);
}

function stationInBboxOrMissingCoords(
  station: Record<string, unknown>,
  bbox: { west: number; south: number; east: number; north: number },
): boolean {
  const [lon, lat] = stationCoords(station, bbox);
  if (lon === null || lat === null) {
    return true;
  }
  if (!(lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90)) {
    return false;
  }
  return lon >= bbox.west && lon <= bbox.east && lat >= bbox.south &&
    lat <= bbox.north;
}

function stationStub(record: Record<string, unknown>): Record<string, unknown> {
  const location = (record.location && typeof record.location === "object")
    ? record.location as Record<string, unknown>
    : {};
  return {
    properties: {
      longitude: location.longitude,
      latitude: location.latitude,
    },
  };
}

function normalizeStationPayload(record: Record<string, unknown>): {
  station_ref: string | null;
  label: string | null;
  station_name: string | null;
  station_type: string | null;
  station_exposure: string | null;
  longitude: number | null;
  latitude: number | null;
} {
  const location = (record.location && typeof record.location === "object")
    ? record.location as Record<string, unknown>
    : {};
  const sensor = (record.sensor && typeof record.sensor === "object")
    ? record.sensor as Record<string, unknown>
    : {};
  const sensorType =
    (record.sensor_type && typeof record.sensor_type === "object")
      ? record.sensor_type as Record<string, unknown>
      : {};
  const lon = coerceNumber(location.longitude);
  const lat = coerceNumber(location.latitude);
  const [lonVal, latVal] = maybeSwapCoords(lon, lat, UK_BBOX);
  const stationRef = sensor.id ?? record.sensor_id ?? record.id;
  const label = (location.name ?? record.location_name) as string | null ??
    null;
  const stationType = (sensorType.name ?? sensorType.id) as string | null ??
    null;
  const exposure = stationExposure(location);
  return {
    station_ref: stationRef !== undefined && stationRef !== null
      ? String(stationRef)
      : null,
    label,
    station_name: label,
    station_type: stationType !== undefined && stationType !== null
      ? String(stationType)
      : null,
    station_exposure: exposure,
    longitude: lonVal,
    latitude: latVal,
  };
}

function stationExposure(location: Record<string, unknown>): string | null {
  const indoor = location.indoor;
  if (indoor === null || indoor === undefined) {
    return null;
  }
  if (typeof indoor === "boolean") {
    return indoor ? "indoor" : "outdoor";
  }
  if (typeof indoor === "number") {
    if (indoor === 1) {
      return "indoor";
    }
    if (indoor === 0) {
      return "outdoor";
    }
    return null;
  }
  if (typeof indoor === "string") {
    const value = indoor.trim().toLowerCase();
    if (["1", "true", "yes", "y"].includes(value)) {
      return "indoor";
    }
    if (["0", "false", "no", "n"].includes(value)) {
      return "outdoor";
    }
  }
  return null;
}

function mergeStationRow(
  existing: Record<string, unknown>,
  candidate: Record<string, unknown>,
): Record<string, unknown> {
  const merged: Record<string, unknown> = { ...existing };
  for (const [key, value] of Object.entries(candidate)) {
    if (value === null || value === undefined) {
      continue;
    }
    if (typeof value === "string" && value.trim() === "") {
      continue;
    }
    merged[key] = value;
  }
  return merged;
}

function parseTimestamp(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  let candidate = trimmed;
  if (
    !candidate.endsWith("Z") && !candidate.includes("+") &&
    !candidate.includes("T")
  ) {
    candidate = candidate.replace(" ", "T") + "Z";
  } else if (
    !candidate.endsWith("Z") && candidate.includes("T") &&
    !candidate.includes("+")
  ) {
    candidate = candidate + "Z";
  }
  const ms = Date.parse(candidate);
  if (Number.isNaN(ms)) {
    return null;
  }
  return new Date(ms).toISOString();
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

async function loadConnector(
  connectorId: string | undefined,
  connectorCode: string,
  _connectorLabel: string,
  _serviceUrl: string,
): Promise<ConnectorRow | null> {
  const select =
    "id,connector_code,label,display_name,service_url,overwrite_station_name";
  if (connectorId) {
    const { data } = await postgrestRequest<ConnectorRow[]>(
      "GET",
      "connectors",
      {
        select,
        id: `eq.${connectorId}`,
        limit: "1",
      },
    );
    if (data && data[0]) {
      return data[0];
    }
  }

  const { data: existing } = await postgrestRequest<ConnectorRow[]>(
    "GET",
    "connectors",
    {
      select,
      connector_code: `eq.${connectorCode}`,
      limit: "1",
    },
  );
  if (existing && existing[0]) {
    return existing[0];
  }
  return null;
}

async function upsertPhenomena(
  connectorId: string,
): Promise<Record<string, number>> {
  const payload = Object.values(SCOMM_PHENOMENA).map((meta) => ({
    connector_id: connectorId,
    source_label: meta.source_label,
    label: meta.label,
    notation: meta.notation,
    pollutant_label: meta.pollutant_label,
    observed_property_code: meta.observed_property_code,
    observed_property_display_name: meta.label,
    observed_property_domain: meta.observed_property_domain,
  }));
  const { error: upsertError } = await publicRpcRequest<
    Array<{ phenomena_upserted: number }>
  >(
    "uk_aq_rpc_phenomena_upsert",
    { rows: payload },
  );
  if (upsertError) {
    throw new Error(`Phenomena upsert failed: ${upsertError.message}`);
  }
  const { data, error: idsError } = await publicRpcRequest<
    Array<{ id: number; source_label?: string; eionet_uri?: string }>
  >(
    "uk_aq_rpc_phenomena_ids",
    {
      connector_id: Number(connectorId),
      eionet_uris: Object.values(SCOMM_PHENOMENA).map((meta) => meta.source_label),
    },
  );
  if (idsError) {
    throw new Error(`Phenomena id lookup failed: ${idsError.message}`);
  }
  const idsByUri: Record<string, number> = {};
  for (const row of data ?? []) {
    const sourceLabel = row?.source_label ?? row?.eionet_uri;
    if (sourceLabel) {
      idsByUri[sourceLabel] = Number(row.id);
    }
  }
  const idsByPollutant: Record<string, number> = {};
  for (const [pollutant, meta] of Object.entries(SCOMM_PHENOMENA)) {
    const phenId = idsByUri[meta.source_label];
    if (phenId) {
      idsByPollutant[pollutant] = phenId;
    }
  }
  return idsByPollutant;
}

async function upsertStations(
  stations: Array<Record<string, unknown>>,
  connectorId: string,
  serviceRef: string,
  overwriteStationName: boolean,
): Promise<number> {
  const rowsByRef: Record<string, Record<string, unknown>> = {};
  for (const station of stations) {
    const payload = normalizeStationPayload(station);
    if (!payload.station_ref) {
      continue;
    }
    const stationRef = payload.station_ref;
    const row: Record<string, unknown> = {
      station_ref: stationRef,
      service_ref: String(serviceRef),
      label: payload.label ?? `Sensor.Community ${stationRef}`,
      station_name: payload.station_name,
      station_type: payload.station_type,
      station_exposure: payload.station_exposure,
      geometry: payload.longitude !== null && payload.latitude !== null
        ? `SRID=4326;POINT(${payload.longitude} ${payload.latitude})`
        : null,
      connector_id: connectorId,
      last_seen_at: new Date().toISOString(),
      removed_at: null,
    };
    const existing = rowsByRef[stationRef];
    rowsByRef[stationRef] = existing ? mergeStationRow(existing, row) : row;
  }
  const rows = Object.values(rowsByRef);
  if (!rows.length) {
    return 0;
  }
  if (!overwriteStationName) {
    const existingNames = await fetchStationNames(
      connectorId,
      serviceRef,
      rows.map((row) => String(row.station_ref ?? "")).filter((ref) => ref),
    );
    for (const row of rows) {
      const stationRef = String(row.station_ref ?? "");
      if (!stationRef) {
        continue;
      }
      const existingName = existingNames[stationRef];
      if (
        existingName && typeof existingName === "string" && existingName.trim()
      ) {
        if ("station_name" in row) {
          delete row.station_name;
        }
      }
    }
  }
  await postgrestRequest(
    "POST",
    "stations",
    { on_conflict: "connector_id,service_ref,station_ref" },
    rows,
    "resolution=merge-duplicates,return=minimal",
  );
  return rows.length;
}

async function fetchStationNames(
  connectorId: string,
  serviceRef: string,
  stationRefs: string[],
): Promise<Record<string, string | null>> {
  const mapping: Record<string, string | null> = {};
  for (let idx = 0; idx < stationRefs.length; idx += 200) {
    const chunk = stationRefs.slice(idx, idx + 200);
    const { data } = await postgrestRequest<
      Array<{ station_ref: string; station_name: string | null }>
    >(
      "GET",
      "stations",
      {
        select: "station_ref,station_name",
        connector_id: `eq.${connectorId}`,
        service_ref: `eq.${serviceRef}`,
        station_ref: postgrestIn(chunk),
      },
    );
    for (const row of data ?? []) {
      mapping[String(row.station_ref)] = row.station_name ?? null;
    }
  }
  return mapping;
}

async function fetchStationIds(
  connectorId: string,
  serviceRef: string,
  stationRefs: string[],
): Promise<Record<string, number>> {
  const mapping: Record<string, number> = {};
  for (let idx = 0; idx < stationRefs.length; idx += 200) {
    const chunk = stationRefs.slice(idx, idx + 200);
    const { data } = await postgrestRequest<
      Array<{ id: number; station_ref: string }>
    >(
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

async function upsertTimeseries(
  rows: Array<Record<string, unknown>>,
): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  await postgrestRequest(
    "POST",
    "timeseries",
    { on_conflict: "connector_id,service_ref,timeseries_ref" },
    rows,
    "resolution=merge-duplicates,return=minimal",
  );
  return rows.length;
}

async function fetchTimeseriesIds(
  connectorId: string,
  serviceRef: string,
  timeseriesRefs: string[],
): Promise<Record<string, number>> {
  const mapping: Record<string, number> = {};
  for (let idx = 0; idx < timeseriesRefs.length; idx += 200) {
    const chunk = timeseriesRefs.slice(idx, idx + 200);
    const { data } = await postgrestRequest<
      Array<{ id: number; timeseries_ref: string }>
    >(
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

async function upsertObservations(
  rows: Array<Record<string, unknown>>,
): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  let written = 0;
  for (
    let idx = 0;
    idx < rows.length;
    idx += SCOMM_OBSERVATION_UPSERT_CHUNK_SIZE
  ) {
    const chunk = rows.slice(idx, idx + SCOMM_OBSERVATION_UPSERT_CHUNK_SIZE);
    await postgrestRequest(
      "POST",
      "observations",
      { on_conflict: "connector_id,timeseries_id,observed_at" },
      chunk,
      "resolution=merge-duplicates,return=minimal",
    );
    written += chunk.length;
  }
  return written;
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
  rows: Array<Record<string, unknown>>,
): { rows: Array<Record<string, unknown>>; deduped: number } {
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

function toObservsObservationRow(
  observationRow: Record<string, unknown>,
): ObservsObservationRow | null {
  const observedAt = String(observationRow.observed_at ?? "").trim();
  if (!observedAt) {
    return null;
  }
  const numericValue = Number(observationRow.value);
  return {
    connector_id: Number(observationRow.connector_id),
    timeseries_id: Number(observationRow.timeseries_id),
    observed_at: observedAt,
    value: Number.isFinite(numericValue) ? numericValue : null,
    status: observationRow.status == null
      ? null
      : String(observationRow.status),
  };
}

function loadDropboxConfig(): DropboxConfig | null {
  if (!DROPBOX_APP_KEY || !DROPBOX_APP_SECRET || !DROPBOX_REFRESH_TOKEN) {
    return null;
  }
  if (
    !DROPBOX_ALLOWED_SUPABASE_URL ||
    DROPBOX_ALLOWED_SUPABASE_URL !== SUPABASE_URL
  ) {
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
  if (
    DROPBOX_ERROR_ALLOWED_SUPABASE_URL &&
    DROPBOX_ERROR_ALLOWED_SUPABASE_URL !== SUPABASE_URL
  ) {
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
  if (
    cleaned === DROPBOX_ROOT_FOLDER ||
    cleaned.startsWith(`${DROPBOX_ROOT_FOLDER}/`)
  ) {
    return cleaned;
  }
  return `${DROPBOX_ROOT_FOLDER}${cleaned}`;
}

function normalizeConnectorPrefix(connectorCode: string | null): string {
  const cleaned = (connectorCode ?? "").trim().toLowerCase();
  if (cleaned === "sensorcommunity") {
    return "scomm";
  }
  const normalized = cleaned.replace(/[^a-z0-9]+/g, "_").replace(
    /^_+|_+$/g,
    "",
  );
  return normalized || "scomm";
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
    await dropboxArchiveLogs(
      accessToken,
      DROPBOX_LOG_FOLDER,
      DROPBOX_LOG_RETENTION_DAYS,
      365,
    );
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
          entry.connector_code ?? SCOMM_CONNECTOR_CODE,
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

async function dropboxRefreshAccessToken(
  config: DropboxConfig,
): Promise<string> {
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
  const body = typeof contents === "string"
    ? new TextEncoder().encode(contents)
    : Uint8Array.from(contents);
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
    throw new DropboxHttpError(
      `Dropbox upload failed (${resp.status})`,
      resp.status,
    );
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

async function zipTextCompressed(
  filename: string,
  content: string,
): Promise<Uint8Array> {
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
    header.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
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
    central.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
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
    end.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
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
    localHeader.length + compressedSize + centralHeader.length +
      endHeader.length,
  );
  output.set(localHeader, 0);
  output.set(compressed, localHeader.length);
  output.set(centralHeader, localHeader.length + compressedSize);
  output.set(
    endHeader,
    localHeader.length + compressedSize + centralHeader.length,
  );
  return output;
}

async function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
  const stream = new Blob([Uint8Array.from(data)]).stream().pipeThrough(
    new CompressionStream("deflate-raw"),
  );
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
  const listFolder = async (
    path: string,
  ): Promise<Array<Record<string, unknown>>> => {
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
      payload = { cursor: data?.cursor };
    }
  };

  const entries = await listFolder(folder);
  const archiveEntries = await listFolder(archiveFolder);
  const existingArchives = new Set(
    archiveEntries
      .filter((entry) => entry?.[".tag"] === "file" && entry?.name)
      .map((entry) => asNonEmptyString(entry.name))
      .filter((name): name is string => Boolean(name)),
  );

  for (const entry of entries) {
    if (entry?.[".tag"] !== "folder") {
      continue;
    }
    const name = asNonEmptyString(entry?.name);
    if (!name || name === "archive") {
      continue;
    }
    const folderDate = Date.parse(`${name}T00:00:00Z`);
    if (!Number.isFinite(folderDate) || folderDate >= cutoff) {
      continue;
    }
    const archiveName = `${name}.zip`;
    const archivePath = `${archiveFolder}/${archiveName}`;
    const folderPath = asNonEmptyString(entry?.path_lower) ??
      asNonEmptyString(entry?.path_display);
    if (!folderPath) {
      continue;
    }
    if (!existingArchives.has(archiveName)) {
      const zipPayload = await dropboxDownloadZip(accessToken, folderPath);
      await dropboxUploadFile(accessToken, archivePath, zipPayload);
    }
    await dropboxDeletePath(accessToken, folderPath);
  }

  for (const entry of archiveEntries) {
    if (entry?.[".tag"] !== "file") {
      continue;
    }
    const name = asNonEmptyString(entry?.name) ?? "";
    if (!name.endsWith(".zip")) {
      continue;
    }
    const datePart = name.slice(0, -4);
    const archiveDate = Date.parse(`${datePart}T00:00:00Z`);
    if (!Number.isFinite(archiveDate) || archiveDate >= archiveCutoff) {
      continue;
    }
    const path = asNonEmptyString(entry?.path_lower) ??
      asNonEmptyString(entry?.path_display);
    if (!path) {
      continue;
    }
    await dropboxDeletePath(accessToken, path);
  }
}

async function dropboxDownloadZip(
  accessToken: string,
  path: string,
): Promise<Uint8Array> {
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
  const buffer = await resp.arrayBuffer();
  return new Uint8Array(buffer);
}

async function dropboxDeletePath(
  accessToken: string,
  path: string,
): Promise<void> {
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

const ERROR_LOGGER = createErrorLogger(
  loadErrorDropboxConfig(),
  Boolean(SUPABASE_URL && SUPABASE_PRIVILEGED_KEY),
);

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
  let connector: ConnectorRow | null = null;
  let requestedConnectorId: string | undefined;
  let requestedConnectorCode = SCOMM_CONNECTOR_CODE;
  let requestedConnectorLabel = SCOMM_SERVICE_LABEL;
  let requestedNoFilter = false;
  let fetchedCount = 0;
  let filteredCount = 0;
  let stationsCount = 0;
  let timeseriesCount = 0;
  let observationsUpserted = 0;
  let observationsRowsInput = 0;
  let observationsRowsPrepared = 0;
  let observationsRowsDedupedPrewrite = 0;
  let observsRowsPrepared = 0;
  let observsRowsDedupedPrewrite = 0;
  let stationsProcessed = 0;
  const runStartedAt = Date.now();
  const maxRuntimeSeconds = Number.isFinite(SCOMM_MAX_RUNTIME_SECONDS)
    ? Math.max(30, SCOMM_MAX_RUNTIME_SECONDS)
    : DEFAULT_MAX_RUNTIME_SECONDS;
  const runtimeDeadline = runStartedAt + maxRuntimeSeconds * 1000;
  const responseBufferMs = Number.isFinite(SCOMM_RESPONSE_BUFFER_MS)
    ? Math.max(1_000, Math.floor(SCOMM_RESPONSE_BUFFER_MS))
    : DEFAULT_RESPONSE_BUFFER_MS;
  const processingDeadline = runtimeDeadline - responseBufferMs;
  const shouldStop = () => Date.now() >= processingDeadline;
  let timeBudgetHit = false;

  const noteTimeBudgetHit = (phase: string) => {
    timeBudgetHit = true;
    log.warn(
      "Stopping Sensor.Community poll early (runtime budget exceeded).",
      {
        phase,
        max_runtime_seconds: maxRuntimeSeconds,
        response_buffer_ms: responseBufferMs,
        elapsed_ms: Date.now() - runStartedAt,
      },
    );
  };

  const guardRuntimeBudget = (phase: string): boolean => {
    if (!shouldStop()) {
      return true;
    }
    noteTimeBudgetHit(phase);
    return false;
  };

  try {
    if (!SUPABASE_URL || !SUPABASE_PRIVILEGED_KEY) {
      status = 500;
      responsePayload = {
        ok: false,
        error: "Missing SUPABASE_URL or SB_SECRET_KEY.",
      };
      log.error("Missing Supabase configuration.");
    } else {
      let payload: PollRequest = {};
      try {
        payload = await req.json();
      } catch {
        payload = {};
      }
      requestedConnectorId = payload.connector_id;
      requestedConnectorCode = payload.connector_code ?? SCOMM_CONNECTOR_CODE;
      requestedConnectorLabel = payload.connector_label ?? SCOMM_SERVICE_LABEL;
      const requestedServiceRef =
        typeof payload.service_ref === "string" && payload.service_ref.trim()
          ? payload.service_ref
          : SCOMM_SERVICE_REF;
      const requestedCountry =
        typeof payload.country === "string" && payload.country.trim()
          ? payload.country
          : SCOMM_COUNTRY;
      const requestedBaseUrl =
        typeof payload.base_url === "string" && payload.base_url.trim()
          ? payload.base_url.replace(/\/$/, "")
          : SCOMM_BASE_URL.replace(/\/$/, "");
      requestedNoFilter = Boolean(payload.no_filter);

      log.info("Poll request", {
        connector_id: requestedConnectorId ?? null,
        connector_code: requestedConnectorCode,
        connector_label: requestedConnectorLabel,
        country: requestedCountry,
        no_filter: requestedNoFilter,
        base_url: requestedBaseUrl,
      });

      if (!requestedConnectorCode) {
        status = 400;
        responsePayload = { ok: false, error: "Missing connector_code." };
        log.error("Missing connector_code.");
      } else {
        try {
          connector = await loadConnector(
            requestedConnectorId,
            requestedConnectorCode,
            requestedConnectorLabel,
            requestedBaseUrl,
          );
        } catch (err) {
          status = 500;
          responsePayload = { ok: false, error: "Failed to load connector." };
          log.error("Failed to load connector.", {
            message: err instanceof Error ? err.message : String(err),
          });
          await errorLogger.logError({
            source: "edge",
            severity: "error",
            message: "Failed to load connector.",
            stack: err instanceof Error ? err.stack : undefined,
            context: {
              connector_id: requestedConnectorId ?? null,
              connector_code: requestedConnectorCode,
            },
            connector_code: requestedConnectorCode,
            connector_id: requestedConnectorId ?? null,
          });
        }

        if (status === 200 && !connector) {
          status = 404;
          responsePayload = { ok: false, error: "Connector not found." };
          log.error("Connector not found.");
        }

        let records: Array<Record<string, unknown>> = [];
        if (status === 200 && connector) {
          if (!guardRuntimeBudget("before_source_fetch")) {
            responsePayload = {
              ok: true,
              count: 0,
              partial: true,
              stopped_reason: "runtime_budget_exceeded",
              message: "Stopped before source fetch.",
            };
          } else {
            try {
              const url = `${requestedBaseUrl}/airrohr/v1/filter/country=${
                encodeURIComponent(requestedCountry)
              }`;
              if (rawRecorder) {
                rawRecorder.recordEvent("context", {
                  connector_id: connector.id,
                  connector_code: connector.connector_code,
                  connector_label: connector.label,
                  country: requestedCountry,
                  no_filter: requestedNoFilter,
                });
              }
              const raw = await fetchJsonWithRetry(
                url,
                3,
                rawRecorder,
                processingDeadline,
              );
              if (Array.isArray(raw)) {
                records = raw as Array<Record<string, unknown>>;
              }
            } catch (err) {
              const message = err instanceof Error ? err.message : String(err);
              if (message.includes("Runtime budget exhausted")) {
                noteTimeBudgetHit("during_source_fetch");
                responsePayload = {
                  ok: true,
                  count: 0,
                  partial: true,
                  stopped_reason: "runtime_budget_exceeded",
                  message: "Stopped during source fetch.",
                };
              } else {
                status = 502;
                responsePayload = { ok: false, error: "Sensor.Community fetch failed." };
                log.error("Sensor.Community fetch failed.", { message });
                await errorLogger.logError({
                  source: "edge",
                  severity: "error",
                  message: "Sensor.Community fetch failed.",
                  stack: err instanceof Error ? err.stack : undefined,
                  context: {
                    connector_id: connector.id,
                    connector_code: connector.connector_code,
                    country: requestedCountry,
                  },
                  connector_code: connector.connector_code ??
                    requestedConnectorCode,
                  connector_id: connector.id,
                });
              }
            }
          }
        }

        fetchedCount = records.length;
        if (status === 200 && connector) {
          if (!records.length) {
            responsePayload = {
              ok: true,
              count: 0,
              message: "No records returned.",
            };
            log.warn("No records returned.");
          } else {
            const filtered = requestedNoFilter
              ? records
              : records.filter((record) =>
                stationInBboxOrMissingCoords(stationStub(record), UK_BBOX)
              );
            filteredCount = filtered.length;

            const processedRecords: Array<Record<string, unknown>> = [];
            const stationRefSet = new Set<string>();
            const timeseriesRefSet = new Set<string>();
            const observationsByTimeseries: Map<
              string,
              {
                station_ref: string;
                pollutant: string;
                value: number;
                observed_at: string;
                observed_ms: number;
              }
            > = new Map();

            for (const record of filtered) {
              if (!guardRuntimeBudget("during_record_normalization")) {
                break;
              }
              const normalized = normalizeStationPayload(record);
              if (!normalized.station_ref) {
                continue;
              }
              processedRecords.push(record);
              const stationRef = normalized.station_ref;
              stationRefSet.add(stationRef);
              const observedAt = parseTimestamp(record.timestamp) ??
                new Date().toISOString();
              const observedMs = Date.parse(observedAt);
              const sensorValues = Array.isArray(record.sensordatavalues)
                ? record.sensordatavalues
                : [];
              for (const entry of sensorValues) {
                if (!entry || typeof entry !== "object") {
                  continue;
                }
                const valueType = (entry as Record<string, unknown>).value_type;
                const mapped = VALUE_TYPE_MAP[String(valueType)];
                if (!mapped) {
                  continue;
                }
                const value = coerceNumber(
                  (entry as Record<string, unknown>).value,
                );
                if (value === null) {
                  continue;
                }
                const pollutant = mapped.pollutant;
                const timeseriesRef = `${stationRef}:${pollutant}`;
                timeseriesRefSet.add(timeseriesRef);
                const existing = observationsByTimeseries.get(timeseriesRef);
                if (!existing || observedMs > existing.observed_ms) {
                  observationsByTimeseries.set(timeseriesRef, {
                    station_ref: stationRef,
                    pollutant,
                    value,
                    observed_at: observedAt,
                    observed_ms: observedMs,
                  });
                }
              }
            }

            if (!processedRecords.length) {
              responsePayload = {
                ok: true,
                count: 0,
                message: "No records processed.",
                partial: timeBudgetHit,
                stopped_reason: timeBudgetHit
                  ? "runtime_budget_exceeded"
                  : null,
              };
            } else {
              const stationRefs = Array.from(stationRefSet);
              stationsCount = stationRefs.length;
              stationsProcessed = stationsCount;

              let budgetStopPhase: string | null = null;
              const checkBudget = (phase: string): boolean => {
                if (guardRuntimeBudget(phase)) {
                  return true;
                }
                if (!budgetStopPhase) {
                  budgetStopPhase = phase;
                }
                return false;
              };

              let phenomenonIds: Record<string, number> = {};
              let stationIdMap: Record<string, number> = {};
              let timeseriesIdMap: Record<string, number> = {};

              if (checkBudget("before_upsert_phenomena")) {
                phenomenonIds = await upsertPhenomena(connector.id);
              }
              if (!budgetStopPhase && checkBudget("before_upsert_stations")) {
                await upsertStations(
                  processedRecords,
                  connector.id,
                  requestedServiceRef,
                  connector.overwrite_station_name ?? true,
                );
              }
              if (!budgetStopPhase && checkBudget("before_fetch_station_ids")) {
                stationIdMap = await fetchStationIds(
                  connector.id,
                  requestedServiceRef,
                  stationRefs,
                );
              }

              const timeseriesPayload: Array<Record<string, unknown>> = [];
              if (!budgetStopPhase) {
                for (const entry of observationsByTimeseries.values()) {
                  if (!checkBudget("during_timeseries_build")) {
                    break;
                  }
                  const stationId = stationIdMap[entry.station_ref];
                  if (!stationId) {
                    continue;
                  }
                  const meta = Object.values(VALUE_TYPE_MAP).find((item) =>
                    item.pollutant === entry.pollutant
                  );
                  const label = meta
                    ? `${entry.station_ref} ${meta.label}`
                    : entry.pollutant;
                  timeseriesPayload.push({
                    timeseries_ref: `${entry.station_ref}:${entry.pollutant}`,
                    label,
                    uom: meta ? meta.uom : null,
                    station_id: stationId,
                    connector_id: connector.id,
                    service_ref: String(requestedServiceRef),
                    phenomenon_id: phenomenonIds[entry.pollutant],
                    last_value_at: entry.observed_at,
                    last_value: entry.value,
                  });
                }
              }

              timeseriesCount = timeseriesPayload.length;
              if (!budgetStopPhase && checkBudget("before_upsert_timeseries")) {
                await upsertTimeseries(timeseriesPayload);
              }
              const timeseriesRefs = Array.from(timeseriesRefSet);
              if (
                !budgetStopPhase && checkBudget("before_fetch_timeseries_ids")
              ) {
                timeseriesIdMap = await fetchTimeseriesIds(
                  connector.id,
                  requestedServiceRef,
                  timeseriesRefs,
                );
              }

              const rawObservationRows: Array<Record<string, unknown>> = [];
              let observsRows: ObservsObservationRow[] = [];
              if (!budgetStopPhase) {
                for (const entry of observationsByTimeseries.values()) {
                  if (!checkBudget("during_observation_build")) {
                    break;
                  }
                  const timeseriesRef =
                    `${entry.station_ref}:${entry.pollutant}`;
                  const timeseriesId = timeseriesIdMap[timeseriesRef];
                  if (!timeseriesId) {
                    continue;
                  }
                  rawObservationRows.push({
                    connector_id: connector.id,
                    timeseries_id: timeseriesId,
                    observed_at: entry.observed_at,
                    value: entry.value,
                    status: null,
                  });
                }
              }

              observationsRowsInput = rawObservationRows.length;
              const observationDedupe = dedupeExactObservationRows(
                rawObservationRows,
              );
              const observationRows = observationDedupe.rows;
              observationsRowsPrepared = observationRows.length;
              observationsRowsDedupedPrewrite = observationDedupe.deduped;
              observsRows = observationRows.map(toObservsObservationRow)
                .filter((row): row is ObservsObservationRow => row !== null);
              observsRowsPrepared = observsRows.length;
              observsRowsDedupedPrewrite = observationsRowsDedupedPrewrite;

              if (!budgetStopPhase && checkBudget("before_dual_write")) {
                const [observationWriteResult] = await Promise.all([
                  upsertObservations(observationRows),
                  writeObservsWithOutbox(
                    publicRpcRequest,
                    observsRows,
                    (message) => {
                      log.warn("Observs dual-write warning.", {
                        message,
                        rows: observsRows.length,
                      });
                    },
                  ),
                ]);
                observationsUpserted = observationWriteResult;
              }

              if (
                !budgetStopPhase && checkBudget("before_update_connector_poll")
              ) {
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
                  if (!shouldStop()) {
                    await errorLogger.logError({
                      source: "edge",
                      severity: "error",
                      message: "Failed to update connectors.last_polled_at.",
                      context: {
                        connector_id: connector.id,
                        error: pollUpdateError.message,
                      },
                      connector_code: connector.connector_code ??
                        requestedConnectorCode,
                      connector_id: connector.id,
                    });
                  }
                }
              }

              let lastObservedAt: string | null = null;
              let lastObservedMs = Number.NEGATIVE_INFINITY;
              for (const entry of observationsByTimeseries.values()) {
                if (
                  Number.isFinite(entry.observed_ms) &&
                  entry.observed_ms > lastObservedMs
                ) {
                  lastObservedMs = entry.observed_ms;
                  lastObservedAt = entry.observed_at;
                }
              }
              const seriesPolled = observationsByTimeseries.size;

              responsePayload = {
                ok: true,
                fetched: records.length,
                filtered: filtered.length,
                stations: stationsCount,
                stations_processed: stationsProcessed,
                stations_updated: stationsProcessed,
                timeseries: timeseriesCount,
                timeseries_updated: timeseriesCount,
                observations: observationsUpserted,
                observations_upserted: observationsUpserted,
                observations_rows_input: observationsRowsInput,
                observations_rows_prepared: observationsRowsPrepared,
                observations_rows_deduped_prewrite:
                  observationsRowsDedupedPrewrite,
                observs_rows_prepared: observsRowsPrepared,
                observs_rows_deduped_prewrite:
                  observsRowsDedupedPrewrite,
                series_polled: seriesPolled,
                last_observed_at: lastObservedAt,
                partial: timeBudgetHit || Boolean(budgetStopPhase),
                stopped_reason: (timeBudgetHit || budgetStopPhase)
                  ? "runtime_budget_exceeded"
                  : null,
                stopped_phase: budgetStopPhase,
              };
              log.info("Stations polled.", {
                stations_selected: stationsCount,
                stations_processed: stationsProcessed,
                partial: timeBudgetHit || Boolean(budgetStopPhase),
                stopped_phase: budgetStopPhase,
              });
            }
          }
        }
      }
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    status = 500;
    responsePayload = { ok: false, error: "Internal server error." };
    log.error("Unhandled error during poll.", { message });
    if (!shouldStop()) {
      await errorLogger.logError({
        source: "edge",
        severity: "error",
        message: "Unhandled error during poll.",
        stack: err instanceof Error ? err.stack : undefined,
        context: {
          connector_id: requestedConnectorId ?? null,
          connector_code: requestedConnectorCode,
          connector_label: requestedConnectorLabel,
        },
        connector_code: requestedConnectorCode,
        connector_id: requestedConnectorId ?? null,
      });
    } else {
      console.warn("Skipping error logger write (runtime budget exceeded).");
    }
  } finally {
    log.info("Poll summary", {
      connector_id: connector?.id ?? requestedConnectorId ?? null,
      fetched: fetchedCount,
      filtered: filteredCount,
      stations: stationsCount,
      timeseries: timeseriesCount,
      observations: observationsUpserted,
    });
    const canRunDropboxUploads = !shouldStop();
    if (!canRunDropboxUploads) {
      log.warn("Skipping Dropbox uploads (runtime budget exceeded).", {
        elapsed_ms: Date.now() - runStartedAt,
        response_buffer_ms: responseBufferMs,
      });
    }

    let accessToken: string | null = null;
    const resolvedConnectorCode = connector?.connector_code ??
      requestedConnectorCode;
    const refreshDropbox = dropboxConfig
      ? () => dropboxRefreshAccessToken(dropboxConfig)
      : undefined;
    if (dropboxConfig && canRunDropboxUploads) {
      try {
        accessToken = await dropboxRefreshAccessToken(dropboxConfig);
      } catch (err) {
        console.warn("Dropbox token request failed:", err);
      }
    }
    if (accessToken && canRunDropboxUploads) {
      accessToken = await uploadDropboxLog(
        accessToken,
        log,
        connector?.id ?? requestedConnectorId ?? null,
        resolvedConnectorCode,
        errorLogger,
        refreshDropbox,
      );
      if (!shouldStop()) {
        await uploadDropboxRaw(
          accessToken,
          rawRecorder,
          connector?.id ?? requestedConnectorId ?? null,
          resolvedConnectorCode,
          errorLogger,
          refreshDropbox,
        );
      } else {
        log.warn(
          "Skipping Dropbox raw upload (runtime budget exceeded after log upload).",
        );
      }
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
