import { createHash } from "node:crypto";
import { deflateRawSync } from "node:zlib";
import {
  buildCompactObservationRpcArgs,
  serializedJsonUtf8Bytes,
  writeIngestDbObservations,
} from "../../supabase/functions/_shared/ingestdb_observation_writer.mjs";

const CONNECTOR_CODE = "sensorcommunity";
const SCHEDULER_BACKEND_SUPABASE_FUNCTION = "supabase_function";
const SCHEDULER_BACKEND_GOOGLE_CLOUD_RUN = "google_cloud_run";

const DEFAULT_INTERVAL_MINUTES = parsePositiveInt(
  process.env.SCOMM_DEFAULT_INTERVAL_MINUTES,
  15,
);
const IN_FLIGHT_TIMEOUT_MINUTES = parsePositiveInt(
  process.env.SCOMM_IN_FLIGHT_TIMEOUT_MINUTES,
  30,
);
const CLAIM_TIMEOUT_MINUTES = parsePositiveInt(
  process.env.SCOMM_CLAIM_TIMEOUT_MINUTES,
  30,
);
const HTTP_TIMEOUT_MS = parsePositiveInt(
  process.env.SCOMM_HTTP_TIMEOUT_MS,
  60_000,
);
const SOURCE_FETCH_TIMEOUT_MS = parsePositiveInt(
  process.env.SCOMM_SOURCE_TIMEOUT_MS,
  90_000,
);
const SOURCE_FETCH_RETRIES = parsePositiveInt(
  process.env.SCOMM_SOURCE_RETRIES,
  3,
);
const UPSERT_CHUNK_SIZE = parsePositiveInt(
  process.env.SCOMM_UPSERT_CHUNK_SIZE,
  500,
);
const SCOMM_COUNTRY = process.env.SCOMM_COUNTRY || "GB";
const SCOMM_BASE_URL = (process.env.SCOMM_BASE_URL || "https://data.sensor.community").replace(/\/$/, "");
const SCOMM_SERVICE_REF = process.env.SCOMM_SERVICE_REF || CONNECTOR_CODE;
const SCOMM_USER_AGENT = process.env.SCOMM_USER_AGENT || "uk-air-quality-networks";
const SCOMM_INGEST_MET_FIELDS = parseBool(process.env.SCOMM_INGEST_MET_FIELDS, false);
const SCOMM_TRIGGER_MODE = parseTriggerMode(process.env.SCOMM_TRIGGER_MODE);

const SUPABASE_URL = requiredEnv("SUPABASE_URL");
const SUPABASE_PRIVILEGED_KEY = requiredEnvAny(["SB_SECRET_KEY"]);
const UK_AQ_CORE_SCHEMA = process.env.UK_AQ_CORE_SCHEMA || "uk_aq_core";
const UK_AQ_RAW_SCHEMA = process.env.UK_AQ_RAW_SCHEMA || "uk_aq_raw";
const REST_BASE_URL = buildRestBaseUrl(SUPABASE_URL);

const OBS_AQIDB_SUPABASE_URL = (process.env.OBS_AQIDB_SUPABASE_URL || "").trim();
const OBS_AQIDB_SECRET_KEY = (
  process.env.OBS_AQIDB_SECRET_KEY || ""
).trim();
const OBS_AQIDB_RPC_SCHEMA = normalizeObservsRpcSchema(
  (process.env.OBS_AQIDB_RPC_SCHEMA || "uk_aq_public").trim(),
);
const OBSERVS_UPSERT_RPC = (
  process.env.OBSERVS_UPSERT_RPC ||
  "uk_aq_rpc_observs_observations_compact_upsert_v1"
).trim();
const OBSERVS_UPSERT_CHUNK_SIZE = parsePositiveInt(
  process.env.OBSERVS_UPSERT_CHUNK_SIZE,
  5000,
);
const OBSERVS_WRITE_MODE = normalizeObservsWriteMode(
  process.env.OBSERVS_WRITE_MODE,
);
const GCP_PROJECT_ID = (
  process.env.GCP_PROJECT_ID ||
  process.env.GOOGLE_CLOUD_PROJECT ||
  ""
).trim();
const GCP_OBSERVS_PUBSUB_TOPIC = (
  process.env.GCP_OBSERVS_PUBSUB_TOPIC ||
  ""
).trim();
const OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE = parsePositiveInt(
  process.env.OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE,
  500,
);
const OBSERVS_REST_BASE_URL = OBS_AQIDB_SUPABASE_URL
  ? buildRestBaseUrl(OBS_AQIDB_SUPABASE_URL)
  : "";

const DROPBOX_APP_KEY = (process.env.DROPBOX_APP_KEY || "").trim();
const DROPBOX_APP_SECRET = (process.env.DROPBOX_APP_SECRET || "").trim();
const DROPBOX_REFRESH_TOKEN = (process.env.DROPBOX_REFRESH_TOKEN || "").trim();
const DROPBOX_ALLOWED_SUPABASE_URL = (
  process.env.SCOMM_RAW_DROPBOX_ALLOWED_SUPABASE_URL ||
  process.env.UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL ||
  ""
).trim();
const DROPBOX_ERROR_ALLOWED_SUPABASE_URL = (
  process.env.SCOMM_ERROR_DROPBOX_ALLOWED_SUPABASE_URL ||
  process.env.UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL ||
  DROPBOX_ALLOWED_SUPABASE_URL
).trim();
const DROPBOX_ROOT_FOLDER = (() => {
  const raw = process.env.UK_AQ_DROPBOX_ROOT || "";
  return normalizeDropboxPath(raw);
})();
const DROPBOX_LOG_FOLDER = dropboxWithRoot(
  process.env.SCOMM_LOG_DROPBOX_FOLDER ||
  process.env.UK_AIR_LOG_DROPBOX_FOLDER ||
  "/connectors/sensorcommunity/log",
);
const DROPBOX_RAW_FOLDER = dropboxWithRoot(
  process.env.SCOMM_RAW_DROPBOX_FOLDER ||
  process.env.UK_AIR_RAW_DROPBOX_FOLDER ||
  "/connectors/sensorcommunity/raw_data",
);
const DROPBOX_ERROR_FOLDER = dropboxWithRoot(
  process.env.SCOMM_ERROR_DROPBOX_FOLDER ||
  process.env.UK_AIR_ERROR_DROPBOX_FOLDER ||
  "/error_log",
);
const DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token";
const DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
const SCOMM_ALERT_CONSECUTIVE_500_THRESHOLD = parsePositiveInt(
  process.env.SCOMM_ALERT_CONSECUTIVE_500_THRESHOLD,
  3,
);
const SCOMM_ALERT_FAILURE_RATE_LOOKBACK_MINUTES = parsePositiveInt(
  process.env.SCOMM_ALERT_FAILURE_RATE_LOOKBACK_MINUTES,
  60,
);
const SCOMM_ALERT_FAILURE_RATE_THRESHOLD = parseProbability(
  process.env.SCOMM_ALERT_FAILURE_RATE_THRESHOLD,
  0.5,
);
const SCOMM_ALERT_FAILURE_RATE_MIN_RUNS = parsePositiveInt(
  process.env.SCOMM_ALERT_FAILURE_RATE_MIN_RUNS,
  3,
);
const SCOMM_ALERT_RUN_SAMPLE_LIMIT = parsePositiveInt(
  process.env.SCOMM_ALERT_RUN_SAMPLE_LIMIT,
  240,
);

const UK_BBOX = {
  west: -11.0,
  south: 49.0,
  east: 2.0,
  north: 61.0,
};

const BASE_VALUE_TYPE_MAP = {
  P1: { pollutant: "pm10", label: "PM10", uom: "ug/m3" },
  P2: { pollutant: "pm2.5", label: "PM2.5", uom: "ug/m3" },
};

const VALUE_TYPE_MAP = {
  ...BASE_VALUE_TYPE_MAP,
  ...(SCOMM_INGEST_MET_FIELDS
    ? {
      temperature: {
        pollutant: "temperature",
        label: "Temperature",
        uom: "degC",
      },
      humidity: {
        pollutant: "humidity",
        label: "Humidity",
        uom: "%",
      },
      pressure: {
        pollutant: "pressure",
        label: "Pressure",
        uom: "hPa",
      },
    }
    : {}),
};

const BASE_SCOMM_PHENOMENA = {
  pm10: {
    source_label: "sensorcommunity:pm10",
    label: "PM10",
    notation: "PM10",
    pollutant_label: "pm10",
  },
  "pm2.5": {
    source_label: "sensorcommunity:pm2.5",
    label: "PM2.5",
    notation: "PM2.5",
    pollutant_label: "pm2.5",
  },
};

const SCOMM_PHENOMENA = {
  ...BASE_SCOMM_PHENOMENA,
  ...(SCOMM_INGEST_MET_FIELDS
    ? {
      temperature: {
        source_label: "sensorcommunity:temperature",
        label: "Temperature",
        notation: "temperature",
        pollutant_label: "temperature",
      },
      humidity: {
        source_label: "sensorcommunity:humidity",
        label: "Humidity",
        notation: "humidity",
        pollutant_label: "humidity",
      },
      pressure: {
        source_label: "sensorcommunity:pressure",
        label: "Pressure",
        notation: "pressure",
        pollutant_label: "pressure",
      },
    }
    : {}),
};

const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);

function requiredEnv(name) {
  const value = (process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function requiredEnvAny(names) {
  for (const name of names) {
    const value = (process.env[name] || "").trim();
    if (value) {
      return value;
    }
  }
  throw new Error(
    `Missing required environment variable: one of ${names.join(", ")}`,
  );
}

function parsePositiveInt(raw, fallback) {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.trunc(value);
}

function parseProbability(raw, fallback) {
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0 || value >= 1) {
    return fallback;
  }
  return value;
}

function parseBool(raw, fallback = false) {
  if (raw === undefined || raw === null) {
    return fallback;
  }
  const value = String(raw).trim().toLowerCase();
  if (!value) {
    return fallback;
  }
  return ["1", "true", "yes", "y", "on"].includes(value);
}

function parseTriggerMode(raw) {
  const value = String(raw || "").trim().toLowerCase();
  if (value === "safety" || value === "task" || value === "manual") {
    return value;
  }
  return "manual";
}

function normalizeObservsRpcSchema(raw) {
  const normalized = String(raw || "").trim().toLowerCase();
  if (!normalized || normalized === "uk_aq_observs" || normalized === "public") {
    return "uk_aq_public";
  }
  return String(raw).trim();
}

function normalizeObservsWriteMode(raw) {
  const normalized = String(raw || "").trim().toLowerCase();
  if (normalized === "direct") {
    return "direct";
  }
  if (normalized === "pubsub_only") {
    return "pubsub_only";
  }
  return "outbox_only";
}

function toIntegerOrNull(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return Math.trunc(numeric);
}

function toStringOrNull(value) {
  return typeof value === "string" && value.trim() ? value : null;
}

function toObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value;
}

function coerceNumber(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return numeric;
}

function parseTimestamp(value) {
  if (!value || typeof value !== "string") {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function parseObservedAt(value) {
  if (!value || typeof value !== "string") {
    return new Date().toISOString();
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return new Date().toISOString();
  }

  let normalized = trimmed;
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(trimmed)) {
    normalized = `${trimmed.replace(" ", "T")}Z`;
  } else if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(trimmed)) {
    normalized = `${trimmed}Z`;
  }

  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return new Date().toISOString();
  }
  return parsed.toISOString();
}

function buildRestBaseUrl(url) {
  return `${String(url || "").replace(/\/$/, "")}/rest/v1`;
}

function observsConfigured() {
  return Boolean(OBS_AQIDB_SUPABASE_URL && OBS_AQIDB_SECRET_KEY);
}

function observsPubsubTopicPath() {
  if (!GCP_OBSERVS_PUBSUB_TOPIC) {
    return "";
  }
  if (GCP_OBSERVS_PUBSUB_TOPIC.startsWith("projects/")) {
    return GCP_OBSERVS_PUBSUB_TOPIC;
  }
  if (!GCP_PROJECT_ID) {
    return "";
  }
  return `projects/${GCP_PROJECT_ID}/topics/${GCP_OBSERVS_PUBSUB_TOPIC}`;
}

function observsPubsubConfigured() {
  return Boolean(observsPubsubTopicPath());
}

async function fetchGoogleAccessToken() {
  const response = await fetch(
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
    {
      headers: { "Metadata-Flavor": "Google" },
    },
  );
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(
      `Metadata token request failed (${response.status}): ${text}`,
    );
  }
  const payload = await response.json().catch(() => null);
  const token = typeof payload?.access_token === "string"
    ? payload.access_token.trim()
    : "";
  if (!token) {
    throw new Error("Metadata token response missing access_token");
  }
  return token;
}

async function publishObservsRowsToPubsub(preparedRows) {
  if (!preparedRows.length) {
    return 0;
  }
  const topicPath = observsPubsubTopicPath();
  if (!topicPath) {
    throw new Error(
      "Observs Pub/Sub is not configured (missing GCP_OBSERVS_PUBSUB_TOPIC or GCP_PROJECT_ID).",
    );
  }

  const token = await fetchGoogleAccessToken();
  let published = 0;

  for (const rowsChunk of chunk(preparedRows, OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE)) {
    const messages = rowsChunk.map((row) => ({
      data: Buffer.from(JSON.stringify(row), "utf8").toString("base64"),
      attributes: {
        connector_id: String(row.connector_id),
        timeseries_id: String(row.timeseries_id),
        observed_at: row.observed_at,
      },
    }));

    const response = await fetch(
      `https://pubsub.googleapis.com/v1/${topicPath}:publish`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ messages }),
      },
    );
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      const message = payload && typeof payload === "object"
        ? JSON.stringify(payload)
        : `HTTP ${response.status}`;
      throw new Error(`Observs Pub/Sub publish failed: ${message}`);
    }
    const messageIds = Array.isArray(payload?.messageIds)
      ? payload.messageIds
      : null;
    published += messageIds ? messageIds.length : rowsChunk.length;
  }

  return published;
}

function postgrestHeaders(schema, apiKey, write = false) {
  const headers = {
    apikey: apiKey,
    Accept: "application/json",
    "Accept-Profile": schema,
  };
  if (isLikelyJwt(apiKey)) {
    headers.Authorization = `Bearer ${apiKey}`;
  }
  if (write) {
    headers["Content-Type"] = "application/json";
    headers["Content-Profile"] = schema;
  }
  return headers;
}

function withQuery(restBaseUrl, path, query) {
  const url = new URL(`${restBaseUrl}/${path}`);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null) {
        continue;
      }
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

async function fetchWithTimeout(url, init, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function postgrestRequest(method, path, options = {}) {
  const schema = options.schema || UK_AQ_CORE_SCHEMA;
  const timeoutMs = options.timeoutMs || HTTP_TIMEOUT_MS;
  const apiKey = options.apiKey || SUPABASE_PRIVILEGED_KEY;
  const restBaseUrl = options.restBaseUrl || REST_BASE_URL;
  const url = withQuery(restBaseUrl, path, options.query);
  const write = method !== "GET";
  const headers = postgrestHeaders(schema, apiKey, write);
  if (options.prefer) {
    headers.Prefer = options.prefer;
  }

  const init = {
    method,
    headers,
  };
  if (options.body !== undefined) {
    init.body = JSON.stringify(options.body);
  }

  const startedAt = Date.now();
  const response = await fetchWithTimeout(url, init, timeoutMs);
  const text = await response.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }

  if (init.body !== undefined) {
    console.log(JSON.stringify({
      metric: "uk_aq_endpoint_egress",
      endpoint: `postgrest:${path}`,
      destination: new URL(restBaseUrl).origin,
      caller: "uk_aq_sensorcommunity_cloud_run",
      method,
      status: response.status,
      duration_ms: Date.now() - startedAt,
      request_count: 1,
      request_body_bytes: Buffer.byteLength(init.body, "utf8"),
      ts: new Date().toISOString(),
    }));
  }

  return {
    ok: response.ok,
    status: response.status,
    data,
    text,
  };
}

async function observsPostgrestRequest(method, path, options = {}) {
  if (!observsConfigured()) {
    throw new Error(
      "Observs DB is not configured (missing OBS_AQIDB_SUPABASE_URL or OBS_AQIDB_SECRET_KEY).",
    );
  }
  return postgrestRequest(method, path, {
    ...options,
    schema: options.schema || OBS_AQIDB_RPC_SCHEMA,
    apiKey: OBS_AQIDB_SECRET_KEY,
    restBaseUrl: OBSERVS_REST_BASE_URL,
  });
}

async function mainRpcRequest(fn, args = {}) {
  return postgrestRequest("POST", `rpc/${fn}`, {
    schema: "uk_aq_public",
    body: args,
  });
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildStructuredError(message, details) {
  const error = new Error(message);
  error.details = details;
  return error;
}

function extractErrorDetails(error) {
  if (!error || typeof error !== "object" || Array.isArray(error)) {
    return null;
  }
  const details = error.details;
  if (!details || typeof details !== "object" || Array.isArray(details)) {
    return null;
  }
  return { ...details };
}

function isLikelyJwt(value) {
  return typeof value === "string" &&
    value.startsWith("eyJ") &&
    value.split(".").length === 3;
}

async function fetchJsonWithRetry(url, retries = SOURCE_FETCH_RETRIES) {
  const baseDetails = {
    stage: "source_fetch",
    source_url: url,
    timeout_ms: SOURCE_FETCH_TIMEOUT_MS,
    retries,
  };
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    let response;
    try {
      response = await fetchWithTimeout(
        url,
        {
          method: "GET",
          headers: {
            Accept: "application/json",
            "User-Agent": SCOMM_USER_AGENT,
          },
        },
        SOURCE_FETCH_TIMEOUT_MS,
      );
    } catch (error) {
      const causeMessage = error instanceof Error ? error.message : String(error);
      if (attempt === retries) {
        throw buildStructuredError(
          `Sensor.Community source fetch failed after ${attempt} attempts for ${url}: ${causeMessage}`,
          {
            ...baseDetails,
            attempt,
            cause_message: causeMessage,
          },
        );
      }
      await wait(Math.min(30_000, 2 ** attempt * 1_000));
      continue;
    }

    const text = await response.text();
    if (response.ok) {
      let payload;
      try {
        payload = JSON.parse(text);
      } catch {
        throw buildStructuredError(
          `Sensor.Community response was not valid JSON for ${url}`,
          {
            ...baseDetails,
            attempt,
            http_status: response.status,
            response_excerpt: text.length > 500 ? `${text.slice(0, 497)}...` : text,
          },
        );
      }
      if (!Array.isArray(payload)) {
        throw buildStructuredError(
          `Sensor.Community response was not an array for ${url}`,
          {
            ...baseDetails,
            attempt,
            http_status: response.status,
          },
        );
      }
      return payload;
    }

    if (!RETRYABLE_STATUS.has(response.status) || attempt === retries) {
      throw buildStructuredError(
        `Sensor.Community request failed (${response.status}) after ${attempt} attempts for ${url}`,
        {
          ...baseDetails,
          attempt,
          http_status: response.status,
          response_excerpt: text.length > 500 ? `${text.slice(0, 497)}...` : text,
        },
      );
    }

    await wait(Math.min(30_000, 2 ** attempt * 1_000));
  }

  return [];
}

function chunk(values, size) {
  const chunkSize = Math.max(1, Number(size) || 1);
  const result = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    result.push(values.slice(index, index + chunkSize));
  }
  return result;
}

function encodeInFilter(values) {
  return `(${values
    .map((value) => `"${String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`)
    .join(",")})`;
}

function maybeSwapCoords(lon, lat, bbox) {
  if (lon === null || lat === null || !bbox) {
    return [lon, lat];
  }
  const lonLooksLikeLat = lon >= bbox.south && lon <= bbox.north;
  const latLooksLikeLon = lat >= bbox.west && lat <= bbox.east;
  const lonInLonRange = lon >= bbox.west && lon <= bbox.east;
  const latInLatRange = lat >= bbox.south && lat <= bbox.north;

  if (lonLooksLikeLat && latLooksLikeLon && !lonInLonRange && !latInLatRange) {
    return [lat, lon];
  }
  return [lon, lat];
}

function stationCoords(record) {
  const location = toObject(record?.location) || {};
  let lon =
    coerceNumber(location.longitude) ??
    coerceNumber(location.lon) ??
    coerceNumber(location.lng);
  let lat = coerceNumber(location.latitude) ?? coerceNumber(location.lat);

  [lon, lat] = maybeSwapCoords(lon, lat, UK_BBOX);

  return [lon, lat];
}

function stationInBboxOrMissingCoords(record) {
  const [lon, lat] = stationCoords(record);
  if (lon === null || lat === null) {
    return true;
  }
  if (lon < -180 || lon > 180 || lat < -90 || lat > 90) {
    return false;
  }
  return (
    lon >= UK_BBOX.west &&
    lon <= UK_BBOX.east &&
    lat >= UK_BBOX.south &&
    lat <= UK_BBOX.north
  );
}

function stationExposure(location) {
  if (!location || typeof location !== "object") {
    return null;
  }
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

function normalizeStationPayload(record) {
  const location = toObject(record?.location) || {};
  const sensor = toObject(record?.sensor) || {};
  const sensorType = toObject(record?.sensor_type) || {};
  const [lon, lat] = stationCoords(record);

  const stationRefRaw = sensor.id ?? record?.sensor_id ?? record?.id;
  const stationRef =
    stationRefRaw !== undefined && stationRefRaw !== null
      ? String(stationRefRaw)
      : null;
  const label =
    toStringOrNull(location.name) || toStringOrNull(record?.location_name);

  return {
    station_ref: stationRef,
    label,
    station_name: label,
    station_type:
      toStringOrNull(sensorType.name) || toStringOrNull(sensorType.id),
    station_exposure: stationExposure(location),
    longitude: lon,
    latitude: lat,
  };
}

function mergeStationRow(existing, candidate) {
  const merged = { ...existing };
  for (const [key, value] of Object.entries(candidate)) {
    if (value === null || value === undefined) {
      continue;
    }
    if (typeof value === "string" && !value.trim()) {
      continue;
    }
    merged[key] = value;
  }
  return merged;
}

function canonicalTextHex(value) {
  if (typeof value !== "string") return "~";
  const text = value.replace(/^ +| +$/g, "");
  return text ? Buffer.from(text, "utf8").toString("hex") : "~";
}

function canonicalFloat64Hex(value) {
  return typeof value === "number" && Number.isFinite(value)
    ? float64ToHex(value)
    : "~";
}

function stationDescriptiveFingerprint(row, overwriteStationName) {
  const canonical = [
    `label=${canonicalTextHex(row.label)}`,
    overwriteStationName
      ? `|station_name=${canonicalTextHex(row.station_name)}`
      : "",
    `|station_type=${canonicalTextHex(row.station_type)}`,
    `|station_exposure=${canonicalTextHex(row.station_exposure)}`,
    `|longitude=${canonicalFloat64Hex(row.longitude)}`,
    `|latitude=${canonicalFloat64Hex(row.latitude)}`,
  ].join("");
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}

function buildObservationMap(records) {
  const observationsByTimeseries = new Map();
  const stationRefs = new Set();
  const timeseriesRefs = new Set();

  for (const record of records) {
    const normalized = normalizeStationPayload(record);
    const stationRef = normalized.station_ref;
    if (!stationRef) {
      continue;
    }
    stationRefs.add(stationRef);

    const observedAt = parseObservedAt(record?.timestamp);
    const observedMs = Date.parse(observedAt);

    const sensorValues = Array.isArray(record?.sensordatavalues)
      ? record.sensordatavalues
      : [];

    for (const entry of sensorValues) {
      if (!entry || typeof entry !== "object") {
        continue;
      }
      const valueType = entry.value_type;
      const mapped = VALUE_TYPE_MAP[String(valueType)];
      if (!mapped) {
        continue;
      }
      const value = coerceNumber(entry.value);
      if (value === null) {
        continue;
      }

      const pollutant = mapped.pollutant;
      const timeseriesRef = `${stationRef}:${pollutant}`;
      timeseriesRefs.add(timeseriesRef);
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

  return {
    stationRefs: Array.from(stationRefs),
    timeseriesRefs: Array.from(timeseriesRefs),
    observationsByTimeseries,
  };
}

function evaluateDue(connector, now) {
  if (connector?.poll_enabled !== true) {
    return {
      due: false,
      reason: "poll_disabled",
      intervalMinutes: DEFAULT_INTERVAL_MINUTES,
    };
  }

  const schedulerBackend =
    connector.scheduler_backend || SCHEDULER_BACKEND_SUPABASE_FUNCTION;
  if (schedulerBackend !== SCHEDULER_BACKEND_GOOGLE_CLOUD_RUN) {
    return {
      due: false,
      reason: "scheduler_backend_not_cloud_run",
      intervalMinutes: DEFAULT_INTERVAL_MINUTES,
    };
  }

  const intervalMinutes =
    toIntegerOrNull(connector.poll_interval_minutes) || DEFAULT_INTERVAL_MINUTES;

  const runStartedAt = parseTimestamp(connector.last_run_start);
  const runEndedAt = parseTimestamp(connector.last_run_end);
  if (runStartedAt && !runEndedAt) {
    const runningGuardMs =
      Math.max(intervalMinutes, IN_FLIGHT_TIMEOUT_MINUTES) * 60 * 1000;
    const ageMs = now.getTime() - runStartedAt.getTime();
    if (Number.isFinite(ageMs) && ageMs >= 0 && ageMs < runningGuardMs) {
      return {
        due: false,
        reason: "in_flight",
        intervalMinutes,
      };
    }
  }

  const anchor = runStartedAt || parseTimestamp(connector.last_polled_at);
  if (!anchor) {
    return { due: true, reason: "first_run", intervalMinutes };
  }

  const elapsedMs = now.getTime() - anchor.getTime();
  if (elapsedMs < intervalMinutes * 60 * 1000) {
    return { due: false, reason: "not_due", intervalMinutes };
  }

  return { due: true, reason: "due", intervalMinutes };
}

async function loadConnector() {
  const response = await postgrestRequest("GET", "connectors", {
    query: {
      select:
        "id,connector_code,poll_enabled,poll_interval_minutes,scheduler_backend,last_polled_at,last_run_start,last_run_end,last_run_status,overwrite_station_name",
      connector_code: `eq.${CONNECTOR_CODE}`,
      limit: 1,
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to load connector (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data) ? response.data : [];
  return rows[0] || null;
}

async function claimConnector(runStartedAtIso) {
  const response = await postgrestRequest("POST", "rpc/uk_aq_rpc_dispatch_claim", {
    schema: "uk_aq_public",
    body: {
      p_connector_code: CONNECTOR_CODE,
      p_run_started_at: runStartedAtIso,
      p_timeout_minutes: CLAIM_TIMEOUT_MINUTES,
    },
  });
  if (!response.ok) {
    throw new Error(
      `Dispatch claim failed (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data) ? response.data : [];
  return rows[0] || null;
}

async function upsertRows(table, rows, onConflict, schema = UK_AQ_CORE_SCHEMA) {
  if (!rows.length) {
    return;
  }

  for (const rowsChunk of chunk(rows, UPSERT_CHUNK_SIZE)) {
    const response = await postgrestRequest("POST", table, {
      schema,
      query: { on_conflict: onConflict },
      body: rowsChunk,
      prefer: "resolution=merge-duplicates,return=minimal",
    });
    if (!response.ok) {
      throw new Error(
        `Failed to upsert ${table} (${response.status}): ${response.text}`,
      );
    }
  }
}

async function fetchStationNames(connectorId, serviceRef, stationRefs) {
  const mapping = {};
  if (!stationRefs.length) {
    return mapping;
  }

  for (const refsChunk of chunk(stationRefs, 200)) {
    const response = await postgrestRequest("GET", "stations", {
      query: {
        select: "station_ref,station_name",
        connector_id: `eq.${connectorId}`,
        service_ref: `eq.${serviceRef}`,
        station_ref: `in.${encodeInFilter(refsChunk)}`,
      },
    });
    if (!response.ok) {
      throw new Error(
        `Failed to fetch station names (${response.status}): ${response.text}`,
      );
    }
    const rows = Array.isArray(response.data) ? response.data : [];
    for (const row of rows) {
      const stationRef = toStringOrNull(row?.station_ref);
      if (!stationRef) {
        continue;
      }
      mapping[stationRef] = toStringOrNull(row?.station_name);
    }
  }

  return mapping;
}

async function upsertStations(records, connectorId, serviceRef, overwriteStationName) {
  const rowsByRef = new Map();

  for (const record of records) {
    const normalized = normalizeStationPayload(record);
    if (!normalized.station_ref) {
      continue;
    }

    const row = {
      station_ref: normalized.station_ref,
      service_ref: String(serviceRef),
      label: normalized.label || `Sensor.Community ${normalized.station_ref}`,
      station_name: normalized.station_name,
      station_type: normalized.station_type,
      station_exposure: normalized.station_exposure,
      longitude: normalized.longitude,
      latitude: normalized.latitude,
      connector_id: connectorId,
    };

    const existing = rowsByRef.get(normalized.station_ref);
    if (existing) {
      rowsByRef.set(normalized.station_ref, mergeStationRow(existing, row));
    } else {
      rowsByRef.set(normalized.station_ref, row);
    }
  }

  const rows = Array.from(rowsByRef.values());
  const seenAt = new Date().toISOString();
  let stateByRef = new Map();
  try {
    for (const refsChunk of chunk(rows.map((row) => row.station_ref), 200)) {
      const response = await mainRpcRequest(
        "uk_aq_rpc_sensorcommunity_station_states_v1",
        { connector_id: connectorId, service_ref: String(serviceRef), station_refs: refsChunk },
      );
      if (!response.ok || !Array.isArray(response.data)) {
        throw new Error(`station state resolver failed (${response.status}): ${response.text}`);
      }
      for (const state of response.data) {
        const stationRef = toStringOrNull(state?.station_ref);
        const stationId = toIntegerOrNull(state?.station_id);
        const fingerprint = toStringOrNull(state?.descriptive_fingerprint);
        if (stationRef && stationId !== null && fingerprint) {
          stateByRef.set(stationRef, { station_id: stationId, descriptive_fingerprint: fingerprint });
        }
      }
    }
  } catch (error) {
    stateByRef = new Map();
    logSummary("station_state_resolver_warning", {
      message: shortError(error),
      action: "refresh_all_descriptive_metadata",
    });
  }

  const changedRows = rows.filter((row) => {
    const state = stateByRef.get(row.station_ref);
    return !state || state.descriptive_fingerprint !== stationDescriptiveFingerprint(row, overwriteStationName);
  });

  if (!overwriteStationName && changedRows.length) {
    const stationRefs = changedRows
      .map((row) => toStringOrNull(row.station_ref))
      .filter((value) => Boolean(value));
    const existingNames = await fetchStationNames(connectorId, serviceRef, stationRefs);
    for (const row of changedRows) {
      const stationRef = toStringOrNull(row.station_ref);
      if (!stationRef) {
        continue;
      }
      const existingName = existingNames[stationRef];
      if (existingName) {
        row.station_name = existingName;
      }
    }
  }

  const descriptiveRows = changedRows.map((row) => ({
    station_ref: row.station_ref,
    service_ref: row.service_ref,
    label: row.label,
    station_name: row.station_name,
    station_type: row.station_type,
    station_exposure: row.station_exposure,
    geometry: row.longitude !== null && row.latitude !== null
      ? `SRID=4326;POINT(${row.longitude} ${row.latitude})`
      : null,
    connector_id: row.connector_id,
  }));
  await upsertRows("stations", descriptiveRows, "connector_id,service_ref,station_ref", UK_AQ_CORE_SCHEMA);

  const stationIds = await fetchStationIds(
    connectorId,
    serviceRef,
    rows.map((row) => row.station_ref),
  );
  const unresolved = rows.filter((row) => !stationIds[row.station_ref]);
  if (unresolved.length) {
    throw new Error(`Missing Sensor.Community station identities after upsert: ${unresolved.slice(0, 10).map((row) => row.station_ref).join(",")}`);
  }
  for (const rowsChunk of chunk(rows, UPSERT_CHUNK_SIZE)) {
    const presenceResponse = await mainRpcRequest(
      "uk_aq_rpc_sensorcommunity_station_presence_touch_v1",
      {
        connector_id: connectorId,
        station_ids: rowsChunk.map((row) => stationIds[row.station_ref]),
        seen_ats: rowsChunk.map(() => seenAt),
      },
    );
    if (!presenceResponse.ok) {
      throw new Error(`Station presence touch failed (${presenceResponse.status}): ${presenceResponse.text}`);
    }
  }

  return { updated: descriptiveRows.length, stationIds };
}

async function fetchStationIds(connectorId, serviceRef, stationRefs) {
  const mapping = {};
  if (!stationRefs.length) {
    return mapping;
  }

  for (const refsChunk of chunk(stationRefs, 200)) {
    const response = await postgrestRequest("GET", "stations", {
      query: {
        select: "id,station_ref",
        connector_id: `eq.${connectorId}`,
        service_ref: `eq.${serviceRef}`,
        station_ref: `in.${encodeInFilter(refsChunk)}`,
      },
    });
    if (!response.ok) {
      throw new Error(
        `Failed to fetch station ids (${response.status}): ${response.text}`,
      );
    }
    const rows = Array.isArray(response.data) ? response.data : [];
    for (const row of rows) {
      const stationRef = toStringOrNull(row?.station_ref);
      const stationId = toIntegerOrNull(row?.id);
      if (!stationRef || stationId === null) {
        continue;
      }
      mapping[stationRef] = stationId;
    }
  }

  return mapping;
}

async function upsertPhenomena(connectorId) {
  const payload = Object.values(SCOMM_PHENOMENA).map((meta) => ({
    connector_id: connectorId,
    source_label: meta.source_label,
    label: meta.label,
    notation: meta.notation,
    pollutant_label: meta.pollutant_label,
  }));

  await upsertRows(
    "phenomena",
    payload,
    "connector_id,source_label",
    UK_AQ_CORE_SCHEMA,
  );

  const sourceLabels = payload.map((row) => row.source_label);
  const response = await postgrestRequest("GET", "phenomena", {
    query: {
      select: "id,source_label",
      connector_id: `eq.${connectorId}`,
      source_label: `in.${encodeInFilter(sourceLabels)}`,
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to fetch phenomena ids (${response.status}): ${response.text}`,
    );
  }

  const rows = Array.isArray(response.data) ? response.data : [];
  const idsBySourceLabel = {};
  for (const row of rows) {
    const sourceLabel = toStringOrNull(row?.source_label);
    const id = toIntegerOrNull(row?.id);
    if (!sourceLabel || id === null) {
      continue;
    }
    idsBySourceLabel[sourceLabel] = id;
  }

  const idsByPollutant = {};
  for (const [pollutant, meta] of Object.entries(SCOMM_PHENOMENA)) {
    const id = idsBySourceLabel[meta.source_label];
    if (id !== undefined) {
      idsByPollutant[pollutant] = id;
    }
  }

  return idsByPollutant;
}

async function upsertTimeseries(rows) {
  await upsertRows(
    "timeseries",
    rows,
    "connector_id,service_ref,timeseries_ref",
    UK_AQ_CORE_SCHEMA,
  );
}

async function fetchTimeseriesIds(connectorId, serviceRef, timeseriesRefs) {
  const mapping = {};
  if (!timeseriesRefs.length) {
    return mapping;
  }

  for (const refsChunk of chunk(timeseriesRefs, 200)) {
    const response = await postgrestRequest("GET", "timeseries", {
      query: {
        select: "id,timeseries_ref",
        connector_id: `eq.${connectorId}`,
        service_ref: `eq.${serviceRef}`,
        timeseries_ref: `in.${encodeInFilter(refsChunk)}`,
      },
    });
    if (!response.ok) {
      throw new Error(
        `Failed to fetch timeseries ids (${response.status}): ${response.text}`,
      );
    }
    const rows = Array.isArray(response.data) ? response.data : [];
    for (const row of rows) {
      const ref = toStringOrNull(row?.timeseries_ref);
      const id = toIntegerOrNull(row?.id);
      if (!ref || id === null) {
        continue;
      }
      mapping[ref] = id;
    }
  }

  return mapping;
}

async function upsertObservations(rows) {
  return await writeIngestDbObservations({
    rows,
    chunkSize: UPSERT_CHUNK_SIZE,
    connectorCode: CONNECTOR_CODE,
    logger: console,
    config: { minimumAttemptRuntimeMs: HTTP_TIMEOUT_MS },
    requestBodyBytes: (rowsChunk) =>
      serializedJsonUtf8Bytes(buildCompactObservationRpcArgs(rowsChunk)),
    writeChunk: async (rowsChunk) => {
      const response = await mainRpcRequest(
        "uk_aq_rpc_observations_compact_upsert_v1",
        buildCompactObservationRpcArgs(rowsChunk),
      );
      if (!response.ok) {
        const error = new Error(
          `Failed to upsert observations (${response.status}): ${response.text}`,
        );
        error.httpStatus = response.status;
        error.response = response.data;
        throw error;
      }
    },
  });
}

function observationValueDedupeToken(value) {
  if (value === null || value === undefined) {
    return "null";
  }
  const numericValue = Number(value);
  if (Number.isFinite(numericValue)) {
    return `n:${numericValue}`;
  }
  return `s:${String(value)}`;
}

function observationStatusDedupeToken(value) {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function dedupeExactObservationRows(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return { rows: [], deduped: 0 };
  }
  const dedup = new Map();
  for (const row of rows) {
    const connectorId = String(row?.connector_id ?? "");
    const timeseriesId = String(row?.timeseries_id ?? "");
    const observedAt = String(row?.observed_at ?? "").trim();
    const valueToken = observationValueDedupeToken(row?.value);
    const statusToken = observationStatusDedupeToken(row?.status);
    const key =
      `${connectorId}:${timeseriesId}:${observedAt}:${valueToken}:${statusToken}`;
    if (!dedup.has(key)) {
      dedup.set(key, row);
    }
  }
  const preparedRows = Array.from(dedup.values());
  return { rows: preparedRows, deduped: rows.length - preparedRows.length };
}

function toObservedDay(value) {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length >= 10) {
      return trimmed.slice(0, 10);
    }
  }
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString().slice(0, 10);
}

function countRowsFromPayload(payload, field, fallback) {
  const value = Number(payload?.[0]?.[field] ?? fallback);
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return value;
}

function shortError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 400 ? `${message.slice(0, 397)}...` : message;
}

function toObservsObservationRow(
  observationRow,
) {
  const observedAt = String(observationRow?.observed_at || "").trim();
  if (!observedAt) {
    return null;
  }
  const numericValue = Number(observationRow?.value);
  return {
    connector_id: Number(observationRow?.connector_id),
    timeseries_id: Number(observationRow?.timeseries_id),
    observed_at: observedAt,
    value: Number.isFinite(numericValue) ? numericValue : null,
    status: observationRow?.status == null ? null : String(observationRow.status),
  };
}

function normalizeObservsStatus(value) {
  if (value === null || value === undefined) {
    return null;
  }
  const status = String(value).trim();
  return status ? status : null;
}

function normalizeObservedAtIso(value) {
  const parsed = new Date(String(value || ""));
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

function float64ToHex(value) {
  const buffer = new ArrayBuffer(8);
  const view = new DataView(buffer);
  view.setFloat64(0, value, false);
  const bytes = new Uint8Array(buffer);
  return Array.from(bytes).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function float64FromHex(value) {
  if (typeof value !== "string") {
    return null;
  }
  const hex = value.trim().toLowerCase();
  if (!/^[0-9a-f]{16}$/.test(hex)) {
    return null;
  }
  const bytes = new Uint8Array(8);
  for (let i = 0; i < 8; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  const parsed = new DataView(bytes.buffer).getFloat64(0, false);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeObservsValue(value, valueFloat8Hex) {
  const fromHex = float64FromHex(valueFloat8Hex);
  if (fromHex !== null) {
    return {
      value: fromHex,
      value_float8_hex: float64ToHex(fromHex),
    };
  }
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return {
      value: null,
      value_float8_hex: null,
    };
  }
  return {
    value: numeric,
    value_float8_hex: float64ToHex(numeric),
  };
}

function prepareObservsRows(observsRows) {
  if (!Array.isArray(observsRows) || observsRows.length === 0) {
    return [];
  }
  const dedup = new Map();
  for (const row of observsRows) {
    const connectorId = toIntegerOrNull(row?.connector_id);
    const timeseriesId = toIntegerOrNull(row?.timeseries_id);
    const observedAt = normalizeObservedAtIso(row?.observed_at);
    if (
      connectorId === null || connectorId <= 0 ||
      timeseriesId === null || timeseriesId <= 0 ||
      !observedAt
    ) {
      continue;
    }
    const normalizedValue = normalizeObservsValue(
      row?.value,
      row?.value_float8_hex,
    );
    const key = `${connectorId}:${timeseriesId}:${observedAt}`;
    dedup.set(key, {
      connector_id: connectorId,
      timeseries_id: timeseriesId,
      observed_at: observedAt,
      value: normalizedValue.value,
      value_float8_hex: normalizedValue.value_float8_hex,
      status: normalizeObservsStatus(row?.status),
    });
  }
  return Array.from(dedup.values());
}

function buildObservsSyncReceipts(rows) {
  const dedup = new Map();
  for (const row of rows) {
    const connectorId = Number(row.connector_id);
    const timeseriesId = Number(row.timeseries_id);
    const observedDay = toObservedDay(row.observed_at);
    if (!Number.isFinite(connectorId) || !Number.isFinite(timeseriesId) || !observedDay) {
      continue;
    }
    const key = `${connectorId}:${timeseriesId}:${observedDay}`;
    dedup.set(key, {
      connector_id: connectorId,
      timeseries_id: timeseriesId,
      observed_day: observedDay,
    });
  }
  return Array.from(dedup.values());
}

async function observsUpsertObservations(observsRows) {
  const preparedRows = prepareObservsRows(observsRows);
  if (!preparedRows.length) {
    return 0;
  }

  let written = 0;
  for (const rowsChunk of chunk(preparedRows, OBSERVS_UPSERT_CHUNK_SIZE)) {
    const response = await observsPostgrestRequest(
      "POST",
      `rpc/${OBSERVS_UPSERT_RPC}`,
      {
        body: {
          timeseries_ids: rowsChunk.map((row) => row.timeseries_id),
          observed_ats: rowsChunk.map((row) => row.observed_at),
          values: rowsChunk.map((row) => row.value),
        },
      },
    );
    if (!response.ok) {
      throw new Error(
        `Observs upsert failed (${response.status}): ${response.text}`,
      );
    }
    written += countRowsFromPayload(
      Array.isArray(response.data) ? response.data : null,
      "observations_upserted",
      rowsChunk.length,
    );
  }
  return written;
}

async function upsertObservsSyncReceipts(rows) {
  if (!rows.length) {
    return 0;
  }
  const response = await mainRpcRequest(
    "uk_aq_rpc_observs_sync_receipt_daily_upsert",
    { rows },
  );
  if (!response.ok) {
    throw new Error(
      `Observs receipt upsert failed (${response.status}): ${response.text}`,
    );
  }
  return countRowsFromPayload(
    Array.isArray(response.data) ? response.data : null,
    "rows_upserted",
    rows.length,
  );
}

async function enqueueObservsOutbox(observsRows) {
  const preparedRows = prepareObservsRows(observsRows);
  if (!preparedRows.length) {
    return 0;
  }
  const response = await mainRpcRequest("uk_aq_rpc_observs_outbox_enqueue", {
    entries: [{ payload: preparedRows }],
  });
  if (!response.ok) {
    throw new Error(
      `Observs outbox enqueue failed (${response.status}): ${response.text}`,
    );
  }
  return countRowsFromPayload(
    Array.isArray(response.data) ? response.data : null,
    "rows_enqueued",
    1,
  );
}

async function writeObservsWithOutbox(observsRows) {
  const preparedRows = prepareObservsRows(observsRows);
  if (!preparedRows.length) {
    return { written: 0, receipts_upserted: 0, enqueued: 0 };
  }

  if (OBSERVS_WRITE_MODE === "outbox_only") {
    const enqueued = await enqueueObservsOutbox(preparedRows);
    return {
      written: 0,
      receipts_upserted: 0,
      enqueued,
    };
  }

  if (OBSERVS_WRITE_MODE === "pubsub_only") {
    if (!observsPubsubConfigured()) {
      throw new Error(
        "OBSERVS_WRITE_MODE=pubsub_only but Pub/Sub is not configured.",
      );
    }
    const enqueued = await publishObservsRowsToPubsub(preparedRows);
    return {
      written: 0,
      receipts_upserted: 0,
      enqueued,
    };
  }

  if (!observsConfigured()) {
    return { written: 0, receipts_upserted: 0, enqueued: 0 };
  }

  try {
    const written = await observsUpsertObservations(preparedRows);
    const receipts = buildObservsSyncReceipts(preparedRows);
    const receiptsUpserted = await upsertObservsSyncReceipts(receipts);
    return {
      written,
      receipts_upserted: receiptsUpserted,
      enqueued: 0,
    };
  } catch (error) {
    const enqueued = await enqueueObservsOutbox(preparedRows);
    logSummary("observs_dual_write_warning", {
      rows: preparedRows.length,
      message: shortError(error),
      enqueued,
    });
    return {
      written: 0,
      receipts_upserted: 0,
      enqueued,
    };
  }
}

function normalizeDropboxPath(raw) {
  const cleaned = String(raw || "").trim();
  if (!cleaned) {
    return "";
  }
  const rooted = cleaned.startsWith("/") ? cleaned : `/${cleaned}`;
  return rooted.replace(/\/$/, "");
}

function dropboxWithRoot(path) {
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

function normalizeConnectorPrefix(connectorCode) {
  const cleaned = String(connectorCode || "").trim().toLowerCase();
  if (cleaned === "sensorcommunity") {
    return "scomm";
  }
  const normalized = cleaned.replace(/[^a-z0-9]+/g, "_").replace(
    /^_+|_+$/g,
    "",
  );
  return normalized || "scomm";
}

function formatCompactTimestamp(timestamp) {
  return timestamp.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
}

function formatDateYmd(timestamp) {
  return timestamp.toISOString().slice(0, 10);
}

function buildDropboxLogPath(connectorCode, timestamp) {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_LOG_FOLDER}/${dateFolder}/uk_aq_log_cloud_run_${prefix}_${stamp}.json`;
}

function buildDropboxRawPath(connectorCode, timestamp) {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_RAW_FOLDER}/${dateFolder}/uk_aq_raw_cloud_run_${prefix}_${stamp}.zip`;
}

function buildDropboxErrorPath(errorId, createdAtIso, connectorCode) {
  const createdAt = parseTimestamp(createdAtIso) || new Date();
  const stamp = formatCompactTimestamp(createdAt);
  const dateFolder = createdAtIso.slice(0, 10);
  const prefix = normalizeConnectorPrefix(connectorCode);
  return `${DROPBOX_ERROR_FOLDER}/${dateFolder}/uk_aq_error_cloud_run_${prefix}_${stamp}_${errorId}.json`;
}

function loadDropboxConfigWithAllowlist(allowedSupabaseUrl) {
  if (!DROPBOX_APP_KEY || !DROPBOX_APP_SECRET || !DROPBOX_REFRESH_TOKEN) {
    return null;
  }
  if (!allowedSupabaseUrl || allowedSupabaseUrl !== SUPABASE_URL) {
    return null;
  }
  return {
    appKey: DROPBOX_APP_KEY,
    appSecret: DROPBOX_APP_SECRET,
    refreshToken: DROPBOX_REFRESH_TOKEN,
  };
}

function loadDropboxConfig() {
  return loadDropboxConfigWithAllowlist(DROPBOX_ALLOWED_SUPABASE_URL);
}

function loadDropboxErrorConfig() {
  return loadDropboxConfigWithAllowlist(DROPBOX_ERROR_ALLOWED_SUPABASE_URL);
}

async function dropboxRefreshAccessToken(config) {
  const credentials = Buffer.from(`${config.appKey}:${config.appSecret}`).toString(
    "base64",
  );
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: config.refreshToken,
  });
  const response = await fetch(DROPBOX_TOKEN_URL, {
    method: "POST",
    headers: {
      Authorization: `Basic ${credentials}`,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body,
  });
  if (!response.ok) {
    throw new Error(`Dropbox token request failed (${response.status})`);
  }
  const payload = await response.json();
  const accessToken = String(payload?.access_token || "").trim();
  if (!accessToken) {
    throw new Error("Dropbox token response missing access_token.");
  }
  return accessToken;
}

async function dropboxUploadFile(accessToken, path, contents) {
  const response = await fetch(DROPBOX_UPLOAD_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/octet-stream",
      "Dropbox-API-Arg": JSON.stringify({
        path,
        mode: "overwrite",
        mute: true,
      }),
    },
    body: contents,
  });
  if (!response.ok) {
    const text = await response.text();
    const error = new Error(`Dropbox upload failed (${response.status}): ${text}`);
    error.status = response.status;
    throw error;
  }
}

async function dropboxUploadFileWithRetry(
  accessToken,
  path,
  contents,
  refreshToken,
) {
  try {
    await dropboxUploadFile(accessToken, path, contents);
    return accessToken;
  } catch (error) {
    if (Number(error?.status) === 401 && typeof refreshToken === "function") {
      const refreshed = await refreshToken();
      await dropboxUploadFile(refreshed, path, contents);
      return refreshed;
    }
    throw error;
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

function crc32(data) {
  let crc = 0xffffffff;
  for (const byte of data) {
    const idx = (crc ^ byte) & 0xff;
    crc = CRC_TABLE[idx] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function toDosDateTime(date) {
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

function zipTextCompressed(filename, content) {
  const encoder = new TextEncoder();
  const data = encoder.encode(content);
  const nameBytes = encoder.encode(filename);
  const compressed = deflateRawSync(data);
  const checksum = crc32(data);
  const { dosTime, dosDate } = toDosDateTime(new Date());

  const localHeader = Buffer.alloc(30 + nameBytes.length);
  localHeader.writeUInt32LE(0x04034b50, 0);
  localHeader.writeUInt16LE(20, 4);
  localHeader.writeUInt16LE(0, 6);
  localHeader.writeUInt16LE(8, 8);
  localHeader.writeUInt16LE(dosTime, 10);
  localHeader.writeUInt16LE(dosDate, 12);
  localHeader.writeUInt32LE(checksum, 14);
  localHeader.writeUInt32LE(compressed.length, 18);
  localHeader.writeUInt32LE(data.length, 22);
  localHeader.writeUInt16LE(nameBytes.length, 26);
  localHeader.writeUInt16LE(0, 28);
  Buffer.from(nameBytes).copy(localHeader, 30);

  const centralHeader = Buffer.alloc(46 + nameBytes.length);
  centralHeader.writeUInt32LE(0x02014b50, 0);
  centralHeader.writeUInt16LE(20, 4);
  centralHeader.writeUInt16LE(20, 6);
  centralHeader.writeUInt16LE(0, 8);
  centralHeader.writeUInt16LE(8, 10);
  centralHeader.writeUInt16LE(dosTime, 12);
  centralHeader.writeUInt16LE(dosDate, 14);
  centralHeader.writeUInt32LE(checksum, 16);
  centralHeader.writeUInt32LE(compressed.length, 20);
  centralHeader.writeUInt32LE(data.length, 24);
  centralHeader.writeUInt16LE(nameBytes.length, 28);
  centralHeader.writeUInt16LE(0, 30);
  centralHeader.writeUInt16LE(0, 32);
  centralHeader.writeUInt16LE(0, 34);
  centralHeader.writeUInt16LE(0, 36);
  centralHeader.writeUInt32LE(0, 38);
  centralHeader.writeUInt32LE(0, 42);
  Buffer.from(nameBytes).copy(centralHeader, 46);

  const centralOffset = localHeader.length + compressed.length;
  const endHeader = Buffer.alloc(22);
  endHeader.writeUInt32LE(0x06054b50, 0);
  endHeader.writeUInt16LE(0, 4);
  endHeader.writeUInt16LE(0, 6);
  endHeader.writeUInt16LE(1, 8);
  endHeader.writeUInt16LE(1, 10);
  endHeader.writeUInt32LE(centralHeader.length, 12);
  endHeader.writeUInt32LE(centralOffset, 16);
  endHeader.writeUInt16LE(0, 20);

  return Buffer.concat([localHeader, compressed, centralHeader, endHeader]);
}

async function uploadDropboxArtifacts(
  connectorCode,
  logPayload,
  rawPayload,
) {
  const dropboxConfig = loadDropboxConfig();
  if (!dropboxConfig) {
    return;
  }

  try {
    let accessToken = await dropboxRefreshAccessToken(dropboxConfig);
    const refreshToken = () => dropboxRefreshAccessToken(dropboxConfig);
    let uploadedLogPath = null;
    let uploadedRawPath = null;

    if (logPayload) {
      const logPath = buildDropboxLogPath(connectorCode, new Date());
      const logBytes = new TextEncoder().encode(
        `${JSON.stringify(logPayload, null, 2)}\n`,
      );
      accessToken = await dropboxUploadFileWithRetry(
        accessToken,
        logPath,
        logBytes,
        refreshToken,
      );
      uploadedLogPath = logPath;
    }

    if (rawPayload) {
      const timestamp = new Date();
      const rawPath = buildDropboxRawPath(connectorCode, timestamp);
      const rawText = `${JSON.stringify(rawPayload)}\n`;
      const entryName = `uk_aq_raw_cloud_run_${normalizeConnectorPrefix(connectorCode)
        }_${formatCompactTimestamp(timestamp)}.json`;
      const rawBytes = zipTextCompressed(entryName, rawText);
      await dropboxUploadFileWithRetry(
        accessToken,
        rawPath,
        rawBytes,
        refreshToken,
      );
      uploadedRawPath = rawPath;
    }

    logSummary("dropbox_upload_success", {
      connector_code: connectorCode,
      uploaded_log_path: uploadedLogPath,
      uploaded_raw_path: uploadedRawPath,
    });
  } catch (error) {
    logSummary("dropbox_upload_warning", {
      connector_code: connectorCode,
      message: shortError(error),
    });
  }
}

async function runDirectIngest(connectorId, overwriteStationName, dropboxCapture) {
  const sourceUrl = `${SCOMM_BASE_URL}/airrohr/v1/filter/country=${encodeURIComponent(SCOMM_COUNTRY)}`;
  if (dropboxCapture) {
    dropboxCapture.raw = {
      connector_code: CONNECTOR_CODE,
      service_ref: SCOMM_SERVICE_REF,
      source_url: sourceUrl,
    };
  }
  let sourceRows;
  try {
    sourceRows = await fetchJsonWithRetry(sourceUrl, SOURCE_FETCH_RETRIES);
  } catch (error) {
    const errorDetails = extractErrorDetails(error);
    if (dropboxCapture) {
      dropboxCapture.raw = {
        ...dropboxCapture.raw,
        fetch_error: {
          message: shortError(error),
          details: errorDetails,
        },
      };
    }
    throw error;
  }
  const filteredRows = sourceRows.filter((row) => stationInBboxOrMissingCoords(row));
  if (dropboxCapture) {
    dropboxCapture.raw = {
      ...dropboxCapture.raw,
      fetched: sourceRows.length,
      filtered: filteredRows.length,
      records: filteredRows,
    };
  }

  if (!filteredRows.length) {
    return {
      run_status: "success",
      run_message: "No Sensor.Community rows matched ingest filters.",
      count: 0,
      stations_updated: 0,
      timeseries_updated: 0,
      observations_upserted: 0,
      series_polled: 0,
      last_observed_at: null,
      observs_written: 0,
      observs_receipts_upserted: 0,
      observs_enqueued: 0,
      cross_database_transaction: false,
    };
  }

  const phenomenonIds = await upsertPhenomena(connectorId);
  const stationResult = await upsertStations(
    filteredRows,
    connectorId,
    SCOMM_SERVICE_REF,
    Boolean(overwriteStationName),
  );

  const { timeseriesRefs, observationsByTimeseries } =
    buildObservationMap(filteredRows);
  const stationIdMap = stationResult.stationIds;

  const timeseriesPayload = [];
  for (const [timeseriesRef, observation] of observationsByTimeseries.entries()) {
    const stationId = stationIdMap[observation.station_ref];
    if (!stationId) {
      throw new Error(`Missing Sensor.Community station identity: ${observation.station_ref}`);
    }

    const valueMeta = Object.values(VALUE_TYPE_MAP).find(
      (entry) => entry.pollutant === observation.pollutant,
    );

    timeseriesPayload.push({
      timeseries_ref: timeseriesRef,
      label: valueMeta
        ? `${observation.station_ref} ${valueMeta.label}`
        : observation.pollutant,
      uom: valueMeta ? valueMeta.uom : null,
      station_id: stationId,
      connector_id: connectorId,
      service_ref: String(SCOMM_SERVICE_REF),
      phenomenon_id: phenomenonIds[observation.pollutant] ?? null,
      last_value_at: observation.observed_at,
      last_value: observation.value,
    });
  }

  // Create missing metadata before observations, but do not advance the
  // latest-value marker until the authoritative IngestDB write has committed.
  const timeseriesMetadataPayload = timeseriesPayload.map(
    ({ last_value: _lastValue, last_value_at: _lastValueAt, ...metadata }) => metadata,
  );
  let timeseriesIdMap = await fetchTimeseriesIds(
    connectorId,
    SCOMM_SERVICE_REF,
    timeseriesRefs,
  );
  const missingTimeseriesMetadata = timeseriesMetadataPayload.filter(
    (row) => !timeseriesIdMap[row.timeseries_ref],
  );
  if (missingTimeseriesMetadata.length) {
    await upsertTimeseries(missingTimeseriesMetadata);
    timeseriesIdMap = await fetchTimeseriesIds(
      connectorId,
      SCOMM_SERVICE_REF,
      timeseriesRefs,
    );
  }
  const unresolvedTimeseries = timeseriesRefs.filter((ref) => !timeseriesIdMap[ref]);
  if (unresolvedTimeseries.length) {
    throw new Error(`Missing Sensor.Community timeseries identities after creation: ${unresolvedTimeseries.slice(0, 10).join(",")}`);
  }

  const rawObservationRows = [];
  let lastObservedMs = Number.NEGATIVE_INFINITY;
  let lastObservedAt = null;

  for (const [timeseriesRef, observation] of observationsByTimeseries.entries()) {
    const timeseriesId = timeseriesIdMap[timeseriesRef];
    if (!timeseriesId) {
      throw new Error(`Missing Sensor.Community timeseries identity: ${timeseriesRef}`);
    }

    rawObservationRows.push({
      connector_id: connectorId,
      timeseries_id: timeseriesId,
      observed_at: observation.observed_at,
      value: observation.value,
      status: null,
    });

    if (observation.observed_ms > lastObservedMs) {
      lastObservedMs = observation.observed_ms;
      lastObservedAt = observation.observed_at;
    }
  }

  const observationDedupe = dedupeExactObservationRows(rawObservationRows);
  const observationRows = observationDedupe.rows;
  const observsRows = observationRows.map((row) => toObservsObservationRow(row))
    .filter((row) => row !== null);

  // IngestDB and ObsAQIDB are intentionally independent; no cross-database
  // transaction exists.
  const [ingestDbResult, observsResult] = await Promise.allSettled([
    upsertObservations(observationRows),
    writeObservsWithOutbox(observsRows),
  ]);
  if (ingestDbResult.status === "rejected") {
    const error = ingestDbResult.reason instanceof Error
      ? ingestDbResult.reason
      : new Error(String(ingestDbResult.reason));
    error.details = {
      stage: "ingestdb_observation_write",
      ingestdb_observation_write: error.stats ?? null,
      cross_database_transaction: false,
      obsaqidb_write: observsResult.status === "fulfilled"
        ? { status: "succeeded", ...observsResult.value }
        : { status: "failed", message: shortError(observsResult.reason) },
    };
    throw error;
  }
  const latestResponse = await mainRpcRequest(
    "uk_aq_rpc_timeseries_last_values_compact_update_v1",
    {
      timeseries_ids: timeseriesPayload.map((row) => timeseriesIdMap[row.timeseries_ref]),
      last_values: timeseriesPayload.map((row) => row.last_value),
      last_value_ats: timeseriesPayload.map((row) => row.last_value_at),
    },
  );
  if (!latestResponse.ok) {
    throw new Error(`Timeseries latest-value update failed (${latestResponse.status}): ${latestResponse.text}`);
  }
  if (observsResult.status === "rejected") {
    const error = observsResult.reason instanceof Error
      ? observsResult.reason
      : new Error(String(observsResult.reason));
    error.details = {
      stage: "obsaqidb_write",
      ingestdb_observation_write: {
        status: "succeeded",
        ...ingestDbResult.value,
      },
      cross_database_transaction: false,
      obsaqidb_write: { status: "failed", message: shortError(error) },
    };
    throw error;
  }
  const ingestDbWriteStats = ingestDbResult.value;
  const observsWriteStats = observsResult.value;

  return {
    run_status: "success",
    run_message: "Sensor.Community direct ingest completed via Cloud Run.",
    count: filteredRows.length,
    stations_updated: stationResult.updated,
    timeseries_updated: timeseriesPayload.length,
    observations_upserted: ingestDbWriteStats.committed_rows,
    ingestdb_observation_write: ingestDbWriteStats,
    cross_database_transaction: false,
    observations_rows_input: rawObservationRows.length,
    observations_rows_prepared: observationRows.length,
    observations_rows_deduped_prewrite: observationDedupe.deduped,
    observs_rows_prepared: observsRows.length,
    observs_rows_deduped_prewrite: observationDedupe.deduped,
    series_polled: timeseriesPayload.length,
    last_observed_at: lastObservedAt,
    observs_written: observsWriteStats.written,
    observs_receipts_upserted: observsWriteStats.receipts_upserted,
    observs_enqueued: observsWriteStats.enqueued,
  };
}

function deriveRunSummary(ingestResponse) {
  const payload = toObject(ingestResponse.body);
  const rawRunStatus =
    toStringOrNull(payload?.run_status) ||
    (ingestResponse.ok ? "success" : "failed");
  const runStatus = rawRunStatus === "success" ? "succeeded" : rawRunStatus;

  let runMessage = toStringOrNull(payload?.run_message);
  if (!runMessage) {
    if (ingestResponse.ok) {
      runMessage = "ingest_sensorcommunity completed via google_cloud_run";
    } else {
      runMessage = `ingest_sensorcommunity failed with status ${ingestResponse.status}`;
    }
  }

  return {
    runStatus,
    runMessage,
    payload,
  };
}

async function updateConnectorRun(
  connectorId,
  runEndedAtIso,
  runStatus,
  runMessage,
  runStartedAtIso,
) {
  const payload = {
    last_run_end: runEndedAtIso,
    last_run_status: runStatus,
    last_run_message: runMessage,
  };
  if (runStatus === "succeeded" || runStatus === "success") {
    payload.last_polled_at = runStartedAtIso;
  }

  const response = await postgrestRequest("PATCH", "connectors", {
    query: { id: `eq.${connectorId}` },
    body: payload,
    prefer: "return=minimal",
  });
  if (!response.ok) {
    throw new Error(
      `Failed to update connector run (${response.status}): ${response.text}`,
    );
  }
}

async function insertRunRow(
  connectorId,
  runStartedAtIso,
  runEndedAtIso,
  runStatus,
  runMessage,
  ingestResponse,
  payload,
) {
  const stationsUpdated =
    toIntegerOrNull(payload?.stations_updated) ??
    toIntegerOrNull(payload?.stations) ??
    toIntegerOrNull(payload?.stations_processed);
  const observationsUpserted =
    toIntegerOrNull(payload?.observations_upserted) ??
    toIntegerOrNull(payload?.observations);
  const timeseriesUpdated =
    toIntegerOrNull(payload?.timeseries_updated) ??
    toIntegerOrNull(payload?.timeseries);
  const seriesPolled =
    toIntegerOrNull(payload?.series_polled) ??
    toIntegerOrNull(payload?.timeseries) ??
    toIntegerOrNull(payload?.timeseries_updated);

  const row = {
    connector_id: connectorId,
    connector_code: CONNECTOR_CODE,
    run_started_at: runStartedAtIso,
    run_ended_at: runEndedAtIso,
    run_status: runStatus,
    run_message: runMessage,
    last_observed_at:
      toStringOrNull(payload?.last_observed_at) ??
      toStringOrNull(payload?.last_observed),
    stations_updated: stationsUpdated,
    observations_upserted: observationsUpserted,
    timeseries_updated: timeseriesUpdated,
    series_polled: seriesPolled,
    response_status: ingestResponse.status,
  };

  const response = await postgrestRequest("POST", "uk_aq_ingest_runs", {
    body: row,
    prefer: "return=minimal",
  });
  if (!response.ok) {
    throw new Error(
      `Failed to insert uk_aq_ingest_runs row (${response.status}): ${response.text}`,
    );
  }
}

async function insertErrorLog(connectorId, ingestResponse) {
  const errorId = crypto.randomUUID();
  const createdAtIso = new Date().toISOString();
  const entry = {
    id: errorId,
    created_at: createdAtIso,
    source: "cloud_run",
    severity: "error",
    message: "ingest_sensorcommunity dispatch failed",
    stack: null,
    context: {
      connector_code: CONNECTOR_CODE,
      response_status: ingestResponse.status,
      response_body: ingestResponse.body,
    },
    connector_id: connectorId,
    station_id: null,
    timeseries_id: null,
    dropbox_path: null,
  };

  const response = await postgrestRequest("POST", "error_logs", {
    schema: UK_AQ_RAW_SCHEMA,
    body: entry,
    prefer: "return=minimal",
  });
  if (!response.ok) {
    throw new Error(
      `Failed to insert error_logs row (${response.status}): ${response.text}`,
    );
  }
  return { errorId, createdAtIso, row: entry };
}

async function patchErrorLogDropboxPath(errorId, dropboxPath) {
  const response = await postgrestRequest("PATCH", "error_logs", {
    schema: UK_AQ_RAW_SCHEMA,
    query: { id: `eq.${errorId}` },
    body: { dropbox_path: dropboxPath },
    prefer: "return=minimal",
  });
  if (!response.ok) {
    throw new Error(
      `Failed to patch error_logs.dropbox_path (${response.status}): ${response.text}`,
    );
  }
}

async function insertFailureMonitorAlert(connectorId, alertType, details) {
  const errorId = crypto.randomUUID();
  const createdAtIso = new Date().toISOString();
  const row = {
    id: errorId,
    created_at: createdAtIso,
    source: "cloud_run",
    severity: "warning",
    message: "sensorcommunity_failure_monitor_alert",
    stack: null,
    context: {
      connector_code: CONNECTOR_CODE,
      alert_type: alertType,
      ...details,
    },
    connector_id: connectorId,
    station_id: null,
    timeseries_id: null,
    dropbox_path: null,
  };
  const response = await postgrestRequest("POST", "error_logs", {
    schema: UK_AQ_RAW_SCHEMA,
    body: row,
    prefer: "return=minimal",
  });
  if (!response.ok) {
    throw new Error(
      `Failed to insert failure monitor alert (${response.status}): ${response.text}`,
    );
  }
  return { errorId, createdAtIso, row };
}

async function uploadErrorLogRowToDropbox(errorId, createdAtIso, row) {
  const dropboxConfig = loadDropboxErrorConfig();
  if (!dropboxConfig) {
    return null;
  }
  let accessToken = await dropboxRefreshAccessToken(dropboxConfig);
  const refreshToken = () => dropboxRefreshAccessToken(dropboxConfig);
  const dropboxPath = buildDropboxErrorPath(errorId, createdAtIso, CONNECTOR_CODE);
  const payload = {
    ...row,
    connector_code: CONNECTOR_CODE,
    created_at: createdAtIso,
    dropbox_path: dropboxPath,
  };
  const bytes = new TextEncoder().encode(`${JSON.stringify(payload, null, 2)}\n`);
  await dropboxUploadFileWithRetry(
    accessToken,
    dropboxPath,
    bytes,
    refreshToken,
  );
  await patchErrorLogDropboxPath(errorId, dropboxPath);
  return dropboxPath;
}

function isServerErrorRun(row) {
  const responseStatus = toIntegerOrNull(row?.response_status);
  if (responseStatus !== null) {
    return responseStatus >= 500;
  }
  const runStatus = String(row?.run_status || "").trim().toLowerCase();
  return runStatus === "failed" || runStatus === "error";
}

function countLeadingServerErrors(rows) {
  let streak = 0;
  for (const row of rows) {
    if (!isServerErrorRun(row)) {
      break;
    }
    streak += 1;
  }
  return streak;
}

function rowEndedAtMs(row) {
  const runEndedAt = parseTimestamp(row?.run_ended_at);
  if (runEndedAt) {
    return runEndedAt.getTime();
  }
  const createdAt = parseTimestamp(row?.created_at);
  if (createdAt) {
    return createdAt.getTime();
  }
  return null;
}

function evaluateFailureRate(rows, now) {
  const lookbackMs = SCOMM_ALERT_FAILURE_RATE_LOOKBACK_MINUTES * 60 * 1000;
  const cutoffMs = now.getTime() - lookbackMs;
  const windowRows = rows.filter((row) => {
    const endedAtMs = rowEndedAtMs(row);
    return endedAtMs !== null && endedAtMs >= cutoffMs;
  });
  if (windowRows.length < SCOMM_ALERT_FAILURE_RATE_MIN_RUNS) {
    return {
      triggered: false,
      failures: 0,
      runs: windowRows.length,
      failureRate: 0,
      previousFailureRate: null,
      previousRuns: 0,
    };
  }
  const failures = windowRows.filter((row) => isServerErrorRun(row)).length;
  const failureRate = failures / windowRows.length;

  const previousWindowRows = windowRows.slice(1);
  let previousFailureRate = null;
  if (previousWindowRows.length >= SCOMM_ALERT_FAILURE_RATE_MIN_RUNS) {
    const previousFailures = previousWindowRows.filter((row) =>
      isServerErrorRun(row)
    ).length;
    previousFailureRate = previousFailures / previousWindowRows.length;
  }
  const crossedThreshold =
    failureRate > SCOMM_ALERT_FAILURE_RATE_THRESHOLD &&
    (
      previousFailureRate === null ||
      previousFailureRate <= SCOMM_ALERT_FAILURE_RATE_THRESHOLD
    );
  return {
    triggered: crossedThreshold,
    failures,
    runs: windowRows.length,
    failureRate,
    previousFailureRate,
    previousRuns: previousWindowRows.length,
  };
}

async function loadRecentIngestRunsForAlerts() {
  const response = await postgrestRequest("GET", "uk_aq_ingest_runs", {
    query: {
      select: "id,run_ended_at,run_status,response_status,created_at",
      connector_code: `eq.${CONNECTOR_CODE}`,
      order: "run_ended_at.desc,id.desc",
      limit: String(SCOMM_ALERT_RUN_SAMPLE_LIMIT),
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to load recent ingest runs for alerts (${response.status}): ${response.text}`,
    );
  }
  return Array.isArray(response.data) ? response.data : [];
}

async function evaluateAndWriteFailureAlerts(connectorId) {
  const rows = await loadRecentIngestRunsForAlerts();
  if (!rows.length) {
    return;
  }
  const now = new Date();

  const consecutiveFailures = countLeadingServerErrors(rows);
  const previousConsecutiveFailures = countLeadingServerErrors(rows.slice(1));
  const consecutiveTriggered =
    consecutiveFailures >= SCOMM_ALERT_CONSECUTIVE_500_THRESHOLD &&
    previousConsecutiveFailures < SCOMM_ALERT_CONSECUTIVE_500_THRESHOLD;
  if (consecutiveTriggered) {
    const details = {
      rule: "consecutive_500",
      threshold: SCOMM_ALERT_CONSECUTIVE_500_THRESHOLD,
      consecutive_failures: consecutiveFailures,
      evaluated_runs: rows.length,
    };
    const inserted = await insertFailureMonitorAlert(
      connectorId,
      "consecutive_500",
      details,
    );
    let dropboxPath = null;
    try {
      dropboxPath = await uploadErrorLogRowToDropbox(
        inserted.errorId,
        inserted.createdAtIso,
        inserted.row,
      );
    } catch (error) {
      logSummary("dropbox_error_upload_warning", {
        alert_type: "consecutive_500",
        message: shortError(error),
      });
    }
    logSummary("failure_monitor_alert", {
      alert_type: "consecutive_500",
      threshold: SCOMM_ALERT_CONSECUTIVE_500_THRESHOLD,
      consecutive_failures: consecutiveFailures,
      dropbox_path: dropboxPath,
    });
  }

  const failureRateSummary = evaluateFailureRate(rows, now);
  if (failureRateSummary.triggered) {
    const details = {
      rule: "failure_rate_1h",
      lookback_minutes: SCOMM_ALERT_FAILURE_RATE_LOOKBACK_MINUTES,
      threshold: SCOMM_ALERT_FAILURE_RATE_THRESHOLD,
      min_runs: SCOMM_ALERT_FAILURE_RATE_MIN_RUNS,
      failures: failureRateSummary.failures,
      runs: failureRateSummary.runs,
      failure_rate: Number(failureRateSummary.failureRate.toFixed(6)),
      previous_failure_rate: failureRateSummary.previousFailureRate === null
        ? null
        : Number(failureRateSummary.previousFailureRate.toFixed(6)),
      previous_runs: failureRateSummary.previousRuns,
    };
    const inserted = await insertFailureMonitorAlert(
      connectorId,
      "failure_rate_1h",
      details,
    );
    let dropboxPath = null;
    try {
      dropboxPath = await uploadErrorLogRowToDropbox(
        inserted.errorId,
        inserted.createdAtIso,
        inserted.row,
      );
    } catch (error) {
      logSummary("dropbox_error_upload_warning", {
        alert_type: "failure_rate_1h",
        message: shortError(error),
      });
    }
    logSummary("failure_monitor_alert", {
      alert_type: "failure_rate_1h",
      lookback_minutes: SCOMM_ALERT_FAILURE_RATE_LOOKBACK_MINUTES,
      failures: failureRateSummary.failures,
      runs: failureRateSummary.runs,
      failure_rate: Number(failureRateSummary.failureRate.toFixed(6)),
      threshold: SCOMM_ALERT_FAILURE_RATE_THRESHOLD,
      dropbox_path: dropboxPath,
    });
  }
}

function logSummary(message, details) {
  console.log(
    JSON.stringify({
      ts: new Date().toISOString(),
      connector_code: CONNECTOR_CODE,
      trigger_mode: SCOMM_TRIGGER_MODE,
      message,
      ...details,
    }),
  );
}

async function main() {
  const connector = await loadConnector();
  if (!connector) {
    logSummary("connector_missing", {});
    return;
  }

  const now = new Date();
  const dueCheck = evaluateDue(connector, now);
  if (!dueCheck.due) {
    logSummary("skip", {
      reason: dueCheck.reason,
      poll_enabled: connector.poll_enabled,
      scheduler_backend:
        connector.scheduler_backend || SCHEDULER_BACKEND_SUPABASE_FUNCTION,
      interval_minutes: dueCheck.intervalMinutes,
    });
    return;
  }

  const runStartedAtIso = now.toISOString();
  const claim = await claimConnector(runStartedAtIso);
  if (!claim || claim.claimed !== true) {
    logSummary("skip", {
      reason: "claim_not_acquired",
      claim,
    });
    return;
  }

  const connectorId = Number(claim.connector_id || connector.id);
  let ingestResponse;
  const dropboxCapture = {};
  let runFailed = false;

  try {
    try {
      const payload = await runDirectIngest(
        connectorId,
        connector.overwrite_station_name,
        dropboxCapture,
      );
      ingestResponse = {
        ok: true,
        status: 200,
        body: payload,
        raw: JSON.stringify(payload),
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const errorDetails = extractErrorDetails(error);
      const runEndedAtIso = new Date().toISOString();
      await updateConnectorRun(
        connectorId,
        runEndedAtIso,
        "failed",
        message,
        runStartedAtIso,
      );
      logSummary("direct_ingest_error", {
        error: message,
        ...(errorDetails ? { error_details: errorDetails } : {}),
      });
      runFailed = true;
      ingestResponse = {
        ok: false,
        status: 500,
        body: {
          error: "direct_ingest_failed",
          message,
          details: errorDetails,
        },
        raw: errorDetails ? `${message} | details=${JSON.stringify(errorDetails)}` : message,
      };
      throw error;
    }
  } catch (outerError) {
    // If we threw from the inner block, we still want to finish logging
  }

  const runEndedAtIso = new Date().toISOString();
  const { runStatus, runMessage, payload } = deriveRunSummary(ingestResponse);

  if (!runFailed) {
    runFailed = !ingestResponse.ok || runStatus === "failed" || runStatus === "error";
  }

  await updateConnectorRun(
    connectorId,
    runEndedAtIso,
    runStatus,
    runMessage,
    runStartedAtIso,
  );
  await insertRunRow(
    connectorId,
    runStartedAtIso,
    runEndedAtIso,
    runStatus,
    runMessage,
    ingestResponse,
    payload,
  );

  await uploadDropboxArtifacts(
    CONNECTOR_CODE,
    {
      connector_id: connectorId,
      connector_code: CONNECTOR_CODE,
      run_started_at: runStartedAtIso,
      run_ended_at: runEndedAtIso,
      run_status: runStatus,
      run_message: runMessage,
      response_status: ingestResponse.status,
      payload,
    },
    dropboxCapture.raw || null,
  );

  try {
    await evaluateAndWriteFailureAlerts(connectorId);
  } catch (error) {
    logSummary("failure_monitor_warning", {
      message: shortError(error),
    });
  }

  if (runFailed) {
    const inserted = await insertErrorLog(connectorId, ingestResponse);
    try {
      await uploadErrorLogRowToDropbox(
        inserted.errorId,
        inserted.createdAtIso,
        inserted.row,
      );
    } catch (error) {
      logSummary("dropbox_error_upload_warning", {
        alert_type: "direct_ingest_failure",
        message: shortError(error),
      });
    }
    throw new Error(
      `ingest_sensorcommunity failed (${ingestResponse.status}): ${ingestResponse.raw || runMessage
      }`,
    );
  }

  logSummary("success", {
    run_status: runStatus,
    response_status: ingestResponse.status,
    interval_minutes: dueCheck.intervalMinutes,
    observations_upserted: payload?.observations_upserted ?? null,
    observations_rows_input: payload?.observations_rows_input ?? null,
    observations_rows_prepared: payload?.observations_rows_prepared ?? null,
    observations_rows_deduped_prewrite:
      payload?.observations_rows_deduped_prewrite ?? null,
    observs_rows_prepared: payload?.observs_rows_prepared ?? null,
    observs_rows_deduped_prewrite:
      payload?.observs_rows_deduped_prewrite ?? null,
    observs_written: payload?.observs_written ?? null,
    observs_receipts_upserted: payload?.observs_receipts_upserted ?? null,
    observs_enqueued: payload?.observs_enqueued ?? null,
  });
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logSummary("failure", { error: message });
  process.exitCode = 1;
});
