// trigger deploy 2026-02-09 12:36
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import "../_shared/fetch_egress_patch.ts";
import { cacheControlHeaders } from "../_shared/cache.ts";
import {
  type ObservsObservationRow,
  writeObservsWithOutbox,
} from "../_shared/observs_client.ts";
import {
  createEmptyIngestDbObservationWriteStats,
  isIngestDbObservationWriteError,
  mergeIngestDbObservationWriteStats,
  writeIngestDbObservations,
} from "../_shared/ingestdb_observation_writer.mjs";
import { writeOpenAqIngestDbObservations } from "./ingestdb_observation_write.mjs";

type PollRequest = {
  connector_code?: string;
  station_refs?: string[];
  window_hours?: number;
  batch_size?: number;
  tier1_retry_seconds?: number;
  dry_run?: boolean;
};

type ConnectorRow = {
  id: string;
  connector_code: string;
  label: string;
  service_url: string | null;
  overwrite_station_name?: boolean | null;
};

// Shared rate limit tracking state for OpenAQ requests.
const rateLimitState: {
  remaining: number | null;
  stop: boolean;
  stopReason: string | null;
  limit: number | null;
  firstRemaining: number | null;
  reset: number | null;
  resetAt: string | null;
} = {
  remaining: null,
  stop: false,
  stopReason: null,
  limit: null,
  firstRemaining: null,
  reset: null,
  resetAt: null,
};

type ErrorLogEntry = {
  severity: "error" | "warn";
  message: string;
  context?: Record<string, unknown> | null;
  connector_id?: string | number | null;
};

type RunWarningEntry = Record<string, unknown> & {
  type: string;
  reason: string | null;
  message: string;
  occurred_at: string;
};

type DropboxConfig = {
  appKey: string;
  appSecret: string;
  refreshToken: string;
};

type DropboxDiagnostics = {
  enabled: boolean;
  reason: string | null;
  has_app_key: boolean;
  has_app_secret: boolean;
  has_refresh_token: boolean;
  supabase_url: string | null;
  raw_allowed_supabase_url: string | null;
  raw_allowed_match: boolean;
  dropbox_root: string | null;
};

type RawRecorder = {
  lines: string[];
  responseCount: number;
  recordEvent: (name: string, payload: Record<string, unknown>) => void;
  recordResponse: (
    path: string,
    params: Record<string, string | number>,
    statusCode: number,
    payload: unknown,
  ) => void;
};

type OpenAQLocation = {
  id?: number;
  name?: string | null;
  locality?: string | null;
  isMobile?: boolean | null;
  isMonitor?: boolean | null;
  coordinates?: { latitude?: number | null; longitude?: number | null } | null;
  country?: { code?: string | null; name?: string | null } | null;
  provider?: { name?: string | null } | null;
  owner?: { name?: string | null } | string | null;
  // OpenAQ payload uses "sensors"; we map sensor.id to timeseries_ref internally.
  sensors?: Array<{
    id?: number;
    name?: string | null;
    parameter?: {
      name?: string | null;
      units?: string | null;
      displayName?: string | null;
    } | null;
  }>;
};

type OpenAQLatestRecord = {
  datetime?: { utc?: string | null } | null;
  value?: number | null;
  // OpenAQ payload exposes a sensor id; resolve to timeseries_ref via mapping.
  sensorsId?: number | null;
  locationsId?: number | null;
  coordinates?: { latitude?: number | null; longitude?: number | null } | null;
};

type OpenAQStationCheckpoint = {
  station_id: number;
  next_due_at: string | null;
  last_observed_at: string | null;
  observ_interval_samples: number[] | null;
  ingest_lag_samples: number[] | null;
  last_polled_at: string | null;
};

type OpenAQTimeseriesCheckpoint = {
  station_id: number;
  timeseries_id: number;
  next_due_at: string | null;
  last_observed_at: string | null;
  ingest_lag_samples: number[] | null;
};

type OpenAQTimeseriesCheckpointSnapshot = {
  station_id: number;
  timeseries_id: number;
  last_observed_at: string | null;
};

type OpenAQHourlyRecord = {
  datetime?: { utc?: string | null } | null;
  value?: number | null;
  summary?: {
    avg?: number | null;
    median?: number | null;
    q50?: number | null;
  } | null;
  // OpenAQ payload exposes a sensor id; resolve to timeseries_ref via mapping.
  sensorsId?: number | null;
};

type OpenAQSharedBudgetReserveRow = {
  granted?: boolean | null;
  reason?: string | null;
  budget_key?: string | null;
  caller?: string | null;
  requested_tokens?: number | null;
  minute_bucket?: string | null;
  minute_limit?: number | null;
  minute_used_before?: number | null;
  minute_used_after?: number | null;
  minute_remaining?: number | null;
  minute_reset_at?: string | null;
  hour_window_start?: string | null;
  hour_limit?: number | null;
  hour_used_before?: number | null;
  hour_used_after?: number | null;
  hour_remaining?: number | null;
  hour_reset_at?: string | null;
  retry_after_seconds?: number | null;
};

const DEFAULT_BASE_URL = "https://api.openaq.org/v3";
const DEFAULT_CONNECTOR_CODE = "openaq";
//const DEFAULT_SERVICE_LABEL = "OpenAQ";
const DEFAULT_USER_AGENT = "uk-air-quality-networks";
const DEFAULT_WINDOW_HOURS = 6;
const DEFAULT_BBOX = "-8.623555,49.863222,1.763337,60.871222";
const DEFAULT_PAGE_LIMIT = 1000;
const DEFAULT_MAX_PAGES = 50;
const DEFAULT_CONCURRENCY = 6;
const DEFAULT_MAX_RUNTIME_SECONDS = 120;
const DEFAULT_RATE_LIMIT_RETRIES = 3;
const DEFAULT_MAX_REQUESTS_PER_RUN = 56;
const DEFAULT_STALE_LIMIT = 4;
const DEFAULT_TIER1_RETRY_SECONDS = 300;
const DEFAULT_RATE_LIMIT_STOP_THRESHOLD = 5;
const DEFAULT_GAP_REQUESTS_REMAINING_MIN = 10;
const DEFAULT_MIN_GAP_STATIONS = 1;
const DEFAULT_MIN_NON_GAP_STATIONS = 10;
const DEFAULT_SHARED_BUDGET_MINUTE_LIMIT = 40;
const DEFAULT_SHARED_BUDGET_HOUR_LIMIT = 1500;
const DEFAULT_SHARED_BUDGET_CALLER = "ingest_openaq";
const DEFAULT_POSTGREST_TIMEOUT_MS = 30_000;
type LagStat = "min" | "median" | "p25";
const DEFAULT_LAG_STAT: LagStat = "min";
const PROVIDER_SHORTNAMES: Record<string, string> = {
  "London Air Quality Network": "LAQN",
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
const SB_UK_AQ_CRON_SECRET = Deno.env.get("SB_UK_AQ_CRON_SECRET") ?? "";

const OPENAQ_BASE_URL = (Deno.env.get("OPENAQ_BASE_URL") ?? DEFAULT_BASE_URL)
  .replace(/\/$/, "");
const OPENAQ_CONNECTOR_CODE = Deno.env.get("OPENAQ_CONNECTOR_CODE") ??
  DEFAULT_CONNECTOR_CODE;
const OPENAQ_SERVICE_REF = Deno.env.get("OPENAQ_SERVICE_REF") ??
  OPENAQ_CONNECTOR_CODE;
//const OPENAQ_SERVICE_LABEL = Deno.env.get("OPENAQ_SERVICE_LABEL") ??
//  DEFAULT_SERVICE_LABEL;
const OPENAQ_USER_AGENT = Deno.env.get("OPENAQ_USER_AGENT") ??
  DEFAULT_USER_AGENT;
const OPENAQ_API_KEY = (Deno.env.get("OPENAQ_API_KEY") ?? "").trim();
const OPENAQ_BBOX = Deno.env.get("OPENAQ_BBOX") ?? DEFAULT_BBOX;
const OPENAQ_PAGE_LIMIT = Number(
  Deno.env.get("OPENAQ_PAGE_LIMIT") ?? DEFAULT_PAGE_LIMIT,
);
const OPENAQ_MAX_PAGES = Number(
  Deno.env.get("OPENAQ_MAX_PAGES") ?? DEFAULT_MAX_PAGES,
);
const OPENAQ_CONCURRENCY = Number(
  Deno.env.get("OPENAQ_CONCURRENCY") ?? DEFAULT_CONCURRENCY,
);
const OPENAQ_MAX_RUNTIME_SECONDS = Number(
  Deno.env.get("OPENAQ_MAX_RUNTIME_SECONDS") ?? DEFAULT_MAX_RUNTIME_SECONDS,
);
const OPENAQ_RATE_LIMIT_RETRIES = Number(
  Deno.env.get("OPENAQ_RATE_LIMIT_RETRIES") ?? DEFAULT_RATE_LIMIT_RETRIES,
);
const OPENAQ_MAX_REQUESTS_PER_RUN = Number(
  Deno.env.get("OPENAQ_MAX_REQUESTS_PER_RUN") ?? DEFAULT_MAX_REQUESTS_PER_RUN,
);
const OPENAQ_STALE_LIMIT = Number(
  Deno.env.get("OPENAQ_STALE_LIMIT") ?? DEFAULT_STALE_LIMIT,
);
const OPENAQ_TIER1_RETRY_SECONDS = Number(
  Deno.env.get("OPENAQ_TIER1_RETRY_SECONDS") ?? DEFAULT_TIER1_RETRY_SECONDS,
);
const OPENAQ_RATE_LIMIT_STOP_THRESHOLD = Number(
  Deno.env.get("OPENAQ_RATE_LIMIT_STOP_THRESHOLD") ??
    DEFAULT_RATE_LIMIT_STOP_THRESHOLD,
);
const OPENAQ_GAP_REQUESTS_REMAINING_MIN = Number(
  Deno.env.get("OPENAQ_GAP_REQUESTS_REMAINING_MIN") ??
    DEFAULT_GAP_REQUESTS_REMAINING_MIN,
);
const OPENAQ_MIN_GAP_STATIONS = Number(
  Deno.env.get("OPENAQ_MIN_GAP_STATIONS") ??
    DEFAULT_MIN_GAP_STATIONS,
);
const OPENAQ_MIN_NON_GAP_STATIONS = Number(
  Deno.env.get("OPENAQ_MIN_NON_GAP_STATIONS") ??
    DEFAULT_MIN_NON_GAP_STATIONS,
);
const OPENAQ_SHARED_BUDGET_ENFORCE = parseEnvBoolean(
  Deno.env.get("OPENAQ_SHARED_BUDGET_ENFORCE"),
  true,
);
const OPENAQ_SHARED_BUDGET_KEY = (
  Deno.env.get("OPENAQ_SHARED_BUDGET_KEY") ?? OPENAQ_CONNECTOR_CODE
).trim() || OPENAQ_CONNECTOR_CODE;
const OPENAQ_SHARED_BUDGET_CALLER = (
  Deno.env.get("OPENAQ_SHARED_BUDGET_CALLER") ?? DEFAULT_SHARED_BUDGET_CALLER
).trim() || DEFAULT_SHARED_BUDGET_CALLER;
const OPENAQ_SHARED_BUDGET_MINUTE_LIMIT = Number(
  Deno.env.get("OPENAQ_SHARED_BUDGET_MINUTE_LIMIT") ??
    DEFAULT_SHARED_BUDGET_MINUTE_LIMIT,
);
const OPENAQ_SHARED_BUDGET_HOUR_LIMIT = Number(
  Deno.env.get("OPENAQ_SHARED_BUDGET_HOUR_LIMIT") ??
    DEFAULT_SHARED_BUDGET_HOUR_LIMIT,
);
const OPENAQ_LAG_STAT: LagStat = parseLagStat(
  Deno.env.get("OPENAQ_LAG_STAT"),
);
const OPENAQ_DEBUG_STATION_ID = Number(
  Deno.env.get("CLEANAIRSURB_ST_ID") ?? 189841,
);
const OPENAQ_INGEST_STATION_FETCH = ["1", "true", "yes"].includes(
  String(Deno.env.get("OPENAQ_INGEST_STATION_FETCH") ?? "").toLowerCase(),
);
const UK_AQ_DROPBOX_ROOT = normalizeDropboxPath(
  Deno.env.get("UK_AQ_DROPBOX_ROOT") ?? "",
);
const DROPBOX_APP_KEY = Deno.env.get("DROPBOX_APP_KEY") ?? "";
const DROPBOX_APP_SECRET = Deno.env.get("DROPBOX_APP_SECRET") ?? "";
const DROPBOX_REFRESH_TOKEN = Deno.env.get("DROPBOX_REFRESH_TOKEN") ?? "";
const DROPBOX_ALLOWED_SUPABASE_URL =
  Deno.env.get("OPENAQ_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ??
    Deno.env.get("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ??
    "";
const DROPBOX_LOG_FOLDER = "/connectors/openaq/log";
const DROPBOX_ERROR_FOLDER = "/error_log";
const DROPBOX_RAW_FOLDER = "/connectors/openaq/raw_data";
const DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token";
const DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
const DROPBOX_UPLOAD_SOURCE = (() => {
  const value = (Deno.env.get("OPENAQ_DROPBOX_UPLOAD_SOURCE") ?? "edge")
    .trim()
    .toLowerCase();
  return value === "cloud_run" ? "cloud_run" : "edge";
})();

const requestBudgetState: {
  maxPerRun: number;
  total: number;
  gapReserveMin: number;
  gapPlannedRequests: number;
  gapExecutedRequests: number;
  gapSkippedBudgetRequests: number;
  gapZeroYieldTimeseries: number;
} = {
  maxPerRun: DEFAULT_MAX_REQUESTS_PER_RUN,
  total: 0,
  gapReserveMin: DEFAULT_GAP_REQUESTS_REMAINING_MIN,
  gapPlannedRequests: 0,
  gapExecutedRequests: 0,
  gapSkippedBudgetRequests: 0,
  gapZeroYieldTimeseries: 0,
};

const sharedBudgetState: {
  enabled: boolean;
  key: string;
  caller: string;
  minuteLimit: number;
  hourLimit: number;
  granted: boolean | null;
  reason: string | null;
  requestedTokens: number | null;
  minuteUsedBefore: number | null;
  minuteUsedAfter: number | null;
  minuteRemaining: number | null;
  hourUsedBefore: number | null;
  hourUsedAfter: number | null;
  hourRemaining: number | null;
  minuteResetAt: string | null;
  hourResetAt: string | null;
  retryAfterSeconds: number | null;
} = {
  enabled: OPENAQ_SHARED_BUDGET_ENFORCE,
  key: OPENAQ_SHARED_BUDGET_KEY,
  caller: OPENAQ_SHARED_BUDGET_CALLER,
  minuteLimit: Math.max(
    1,
    positiveInt(
      OPENAQ_SHARED_BUDGET_MINUTE_LIMIT,
      DEFAULT_SHARED_BUDGET_MINUTE_LIMIT,
    ),
  ),
  hourLimit: Math.max(
    1,
    positiveInt(
      OPENAQ_SHARED_BUDGET_HOUR_LIMIT,
      DEFAULT_SHARED_BUDGET_HOUR_LIMIT,
    ),
  ),
  granted: null,
  reason: null,
  requestedTokens: null,
  minuteUsedBefore: null,
  minuteUsedAfter: null,
  minuteRemaining: null,
  hourUsedBefore: null,
  hourUsedAfter: null,
  hourRemaining: null,
  minuteResetAt: null,
  hourResetAt: null,
  retryAfterSeconds: null,
};

let errorLogLines: string[] | null = null;

const REST_BASE_URL = SUPABASE_URL
  ? `${SUPABASE_URL.replace(/\/$/, "")}/rest/v1`
  : "";

async function postgrestFetch(url: string, init: RequestInit): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(
    () => controller.abort(),
    DEFAULT_POSTGREST_TIMEOUT_MS,
  );
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

function postgrestHeaders(
  prefer?: string,
  schema = UK_AQ_CORE_SCHEMA,
): Record<string, string> {
  const headers: Record<string, string> = {
    apikey: SUPABASE_PRIVILEGED_KEY,
    "Content-Type": "application/json",
    "x-ukaq-egress-caller": "ingest_openaq",
  };
  if (prefer) {
    headers.Prefer = prefer;
  }
  if (schema) {
    headers["Accept-Profile"] = schema;
    headers["Content-Profile"] = schema;
  }
  return headers;
}

function openaqHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    "User-Agent": OPENAQ_USER_AGENT,
    "Accept": "application/json",
  };
  if (OPENAQ_API_KEY) {
    headers["X-API-Key"] = OPENAQ_API_KEY;
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

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...cacheControlHeaders(status),
    },
  });
}

async function postgrestRequest<T>(
  method: "GET" | "POST" | "PATCH" | "DELETE",
  path: string,
  params?: Record<string, string>,
  body?: unknown,
  schema = UK_AQ_CORE_SCHEMA,
  prefer?: string,
): Promise<{ data: T | null; error: { message: string } | null }> {
  if (!REST_BASE_URL || !SUPABASE_PRIVILEGED_KEY) {
    return {
      data: null,
      error: { message: "Missing SUPABASE_URL or SB_SECRET_KEY." },
    };
  }
  try {
    const url = new URL(`${REST_BASE_URL}/${path.replace(/^\//, "")}`);
    if (params) {
      for (const [key, value] of Object.entries(params)) {
        url.searchParams.set(key, value);
      }
    }
    const headers = postgrestHeaders(prefer, schema);
    const resp = await postgrestFetch(url.toString(), {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    let payload: unknown = null;
    if (resp.status !== 204) {
      const contentType = resp.headers.get("content-type") ?? "";
      payload = contentType.includes("application/json")
        ? await resp.json()
        : await resp.text();
    }
    if (!resp.ok) {
      const message = typeof payload === "string"
        ? payload
        : JSON.stringify(payload);
      return { data: null, error: { message } };
    }
    return { data: payload as T, error: null };
  } catch (err) {
    return { data: null, error: { message: String(err) } };
  }
}

async function rpcRequest<T>(
  fn: string,
  args?: Record<string, unknown>,
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
    return {
      data: null,
      error: { message: "Missing SUPABASE_URL or SB_SECRET_KEY." },
    };
  }
  try {
    const url = new URL(`${REST_BASE_URL}/rpc/${fn}`);
    const schema = "uk_aq_public";
    const headers = postgrestHeaders(undefined, schema);
    headers["Accept-Profile"] = schema;
    headers["Content-Profile"] = schema;
    const resp = await postgrestFetch(url.toString(), {
      method: "POST",
      headers,
      body: JSON.stringify(args ?? {}),
    });
    let payload: unknown = null;
    if (resp.status !== 204) {
      const contentType = resp.headers.get("content-type") ?? "";
      payload = contentType.includes("application/json")
        ? await resp.json()
        : await resp.text();
    }
    if (!resp.ok) {
      const message = typeof payload === "string"
        ? payload
        : JSON.stringify(payload);
      const errorPayload = payload && typeof payload === "object"
        ? payload as Record<string, unknown>
        : {};
      return {
        data: null,
        error: {
          message,
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
  } catch (err) {
    return { data: null, error: { message: String(err) } };
  }
}

async function loadOpenaqStationRefs(
  batchLimit: number,
  staleLimit: number,
  tier1RetrySeconds: number,
): Promise<Array<{ station_ref: string; station_id: number | null }>> {
  let { data, error } = await rpcRequest<
    Array<{ station_ref: string; station_id: number | null }>
  >(
    "uk_aq_rpc_openaq_select_station_refs",
    {
      batch_limit: batchLimit,
      stale_limit: staleLimit,
      tier1_retry_seconds: tier1RetrySeconds,
    },
  );
  if (error) {
    const fallback = await rpcRequest<
      Array<{ station_ref: string; station_id: number | null }>
    >(
      "uk_aq_rpc_openaq_select_station_refs",
      {
        batch_limit: batchLimit,
        stale_limit: staleLimit,
      },
    );
    if (!fallback.error) {
      data = fallback.data;
      error = null;
    }
  }
  if (error) {
    throw new Error(`OpenAQ station selection failed: ${error.message}`);
  }
  return (data ?? []).map((row) => ({
    station_ref: String(row.station_ref),
    station_id: row.station_id === null ? null : Number(row.station_id),
  }));
}

async function fetchOpenaqStationCheckpoints(
  stationIds: number[],
): Promise<Record<number, OpenAQStationCheckpoint>> {
  if (!stationIds.length) {
    return {};
  }
  const { data, error } = await rpcRequest<OpenAQStationCheckpoint[]>(
    "uk_aq_rpc_openaq_station_checkpoints_select",
    {
      station_ids: stationIds,
    },
  );
  if (error) {
    throw new Error(`OpenAQ checkpoints fetch failed: ${error.message}`);
  }
  const mapping: Record<number, OpenAQStationCheckpoint> = {};
  for (const row of data ?? []) {
    mapping[Number(row.station_id)] = row;
  }
  return mapping;
}

async function upsertOpenaqStationCheckpoints(
  rows: Array<Record<string, unknown>>,
): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  const { data, error } = await rpcRequest<Array<{ rows_upserted: number }>>(
    "uk_aq_rpc_openaq_station_checkpoints_upsert",
    { rows },
  );
  if (error) {
    throw new Error(`OpenAQ checkpoints upsert failed: ${error.message}`);
  }
  return data && data[0] ? Number(data[0].rows_upserted) : 0;
}

type OpenAQTimeseriesCheckpointMaps = {
  byTimeseriesId: Record<number, OpenAQTimeseriesCheckpointSnapshot>;
  byStationId: Record<number, OpenAQTimeseriesCheckpointSnapshot[]>;
};

function postgrestNumericIn(values: number[]): string {
  const cleaned = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .map((value) => String(Math.trunc(value)));
  if (!cleaned.length) {
    return "in.(-1)";
  }
  return `in.(${cleaned.join(",")})`;
}

async function fetchOpenaqTimeseriesCheckpointRows<T>(
  filterColumn: "station_id" | "timeseries_id",
  ids: number[],
  select: string,
): Promise<T[]> {
  if (!ids.length) {
    return [];
  }
  const rows: T[] = [];
  const chunkSize = 200;
  const pageSize = 1000;
  for (let idx = 0; idx < ids.length; idx += chunkSize) {
    const chunk = ids.slice(idx, idx + chunkSize);
    let offset = 0;
    while (true) {
      const { data, error } = await postgrestRequest<T[]>(
        "GET",
        "openaq_timeseries_checkpoints",
        {
          select,
          [filterColumn]: postgrestNumericIn(chunk),
          limit: String(pageSize),
          offset: String(offset),
        },
        undefined,
        UK_AQ_RAW_SCHEMA,
      );
      if (error) {
        throw new Error(
          `OpenAQ timeseries checkpoints fetch failed: ${error.message}`,
        );
      }
      const batch = data ?? [];
      rows.push(...batch);
      if (batch.length < pageSize) {
        break;
      }
      offset += pageSize;
    }
  }
  return rows;
}

async function fetchOpenaqTimeseriesCheckpointSnapshots(
  stationIds: number[],
): Promise<OpenAQTimeseriesCheckpointMaps> {
  if (!stationIds.length) {
    return { byTimeseriesId: {}, byStationId: {} };
  }
  const rows = await fetchOpenaqTimeseriesCheckpointRows<
    OpenAQTimeseriesCheckpointSnapshot
  >(
    "station_id",
    stationIds,
    "station_id,timeseries_id,last_observed_at",
  );
  const byTimeseriesId: Record<number, OpenAQTimeseriesCheckpointSnapshot> = {};
  const byStationId: Record<number, OpenAQTimeseriesCheckpointSnapshot[]> = {};
  for (const row of rows) {
    const timeseriesId = Number(row.timeseries_id);
    const stationId = Number(row.station_id);
    if (Number.isFinite(timeseriesId)) {
      byTimeseriesId[timeseriesId] = row;
    }
    if (Number.isFinite(stationId)) {
      const existing = byStationId[stationId];
      if (existing) {
        existing.push(row);
      } else {
        byStationId[stationId] = [row];
      }
    }
  }
  return { byTimeseriesId, byStationId };
}

async function fetchOpenaqTimeseriesCheckpointDetails(
  timeseriesIds: number[],
): Promise<Record<number, OpenAQTimeseriesCheckpoint>> {
  if (!timeseriesIds.length) {
    return {};
  }
  const rows = await fetchOpenaqTimeseriesCheckpointRows<
    OpenAQTimeseriesCheckpoint
  >(
    "timeseries_id",
    timeseriesIds,
    "station_id,timeseries_id,next_due_at,last_observed_at,ingest_lag_samples",
  );
  const byTimeseriesId: Record<number, OpenAQTimeseriesCheckpoint> = {};
  for (const row of rows) {
    const timeseriesId = Number(row.timeseries_id);
    if (Number.isFinite(timeseriesId)) {
      byTimeseriesId[timeseriesId] = row;
    }
  }
  return byTimeseriesId;
}

async function fetchOpenaqTimeseriesRefsByStationIds(
  connectorId: string,
  serviceRef: string,
  stationIds: number[],
): Promise<Record<number, string[]>> {
  if (!stationIds.length) {
    return {};
  }
  const mapping: Record<number, string[]> = {};
  for (let idx = 0; idx < stationIds.length; idx += 200) {
    const chunk = stationIds.slice(idx, idx + 200);
    const { data, error } = await rpcRequest<
      Array<{ station_id: number; timeseries_ref: string }>
    >(
      "uk_aq_rpc_timeseries_refs_by_station_ids",
      {
        connector_id: Number(connectorId),
        service_ref: serviceRef,
        station_ids: chunk,
      },
    );
    if (error) {
      throw new Error(`OpenAQ timeseries refs fetch failed: ${error.message}`);
    }
    for (const row of data ?? []) {
      const stationId = Number(row.station_id);
      const ref = String(row.timeseries_ref);
      if (!mapping[stationId]) {
        mapping[stationId] = [ref];
      } else {
        mapping[stationId].push(ref);
      }
    }
  }
  return mapping;
}

async function upsertOpenaqTimeseriesCheckpoints(
  rows: Array<Record<string, unknown>>,
): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  const { data, error } = await rpcRequest<Array<{ rows_upserted: number }>>(
    "uk_aq_rpc_openaq_timeseries_checkpoints_upsert",
    { rows },
  );
  if (error) {
    throw new Error(
      `OpenAQ timeseries checkpoints upsert failed: ${error.message}`,
    );
  }
  return data && data[0] ? Number(data[0].rows_upserted) : 0;
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

function postgrestIn(values: string[]): string {
  const cleaned = values
    .map((value) => String(value).replace(/[,()]/g, "").trim())
    .filter(Boolean);
  return `in.(${cleaned.join(",")})`;
}

function formatCompactTimestamp(timestamp: Date): string {
  return timestamp.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
}

function formatDateYmd(timestamp: Date): string {
  return timestamp.toISOString().slice(0, 10);
}

function normalizeConnectorPrefix(connectorCode: string | null): string {
  const cleaned = (connectorCode ?? "").trim().toLowerCase();
  const normalized = cleaned.replace(/[^a-z0-9]+/g, "_").replace(
    /^_+|_+$/g,
    "",
  );
  return normalized || "openaq";
}

function buildDropboxLogPath(
  connectorCode: string | null,
  timestamp: Date,
): string {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  const base = dropboxWithRoot(DROPBOX_LOG_FOLDER);
  return `${base}/${dateFolder}/uk_aq_log_${DROPBOX_UPLOAD_SOURCE}_${prefix}_${stamp}.log`;
}

function buildDropboxRawPath(
  connectorCode: string | null,
  timestamp: Date,
): string {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  const base = dropboxWithRoot(DROPBOX_RAW_FOLDER);
  return `${base}/${dateFolder}/uk_aq_raw_${DROPBOX_UPLOAD_SOURCE}_${prefix}_${stamp}.zip`;
}

function buildDropboxErrorPath(
  connectorCode: string | null,
  timestamp: Date,
): string {
  const stamp = formatCompactTimestamp(timestamp);
  const dateFolder = formatDateYmd(timestamp);
  const prefix = normalizeConnectorPrefix(connectorCode);
  const base = dropboxWithRoot(DROPBOX_ERROR_FOLDER);
  return `${base}/${dateFolder}/uk_aq_error_${DROPBOX_UPLOAD_SOURCE}_${prefix}_${stamp}.log`;
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

function buildDropboxDiagnostics(): DropboxDiagnostics {
  const hasAppKey = Boolean(DROPBOX_APP_KEY);
  const hasAppSecret = Boolean(DROPBOX_APP_SECRET);
  const hasRefreshToken = Boolean(DROPBOX_REFRESH_TOKEN);
  const supabaseUrl = SUPABASE_URL || null;
  const rawAllowed = DROPBOX_ALLOWED_SUPABASE_URL || null;
  const rawAllowedMatch = Boolean(rawAllowed) && rawAllowed === SUPABASE_URL;

  let reason: string | null = null;
  if (!SUPABASE_URL) {
    reason = "missing_supabase_url";
  } else if (!hasAppKey || !hasAppSecret || !hasRefreshToken) {
    reason = "missing_dropbox_credentials";
  } else if (!rawAllowed) {
    reason = "missing_dropbox_allowed_supabase_url";
  } else if (!rawAllowedMatch) {
    reason = "dropbox_allowed_supabase_url_mismatch";
  }

  return {
    enabled: reason === null,
    reason,
    has_app_key: hasAppKey,
    has_app_secret: hasAppSecret,
    has_refresh_token: hasRefreshToken,
    supabase_url: supabaseUrl,
    raw_allowed_supabase_url: rawAllowed,
    raw_allowed_match: rawAllowedMatch,
    dropbox_root: UK_AQ_DROPBOX_ROOT || null,
  };
}

async function dropboxRefreshAccessToken(
  config: DropboxConfig,
): Promise<string> {
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    refresh_token: config.refreshToken,
    client_id: config.appKey,
    client_secret: config.appSecret,
  });
  const resp = await fetch(DROPBOX_TOKEN_URL, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });
  if (!resp.ok) {
    throw new Error(`Dropbox token request failed (${resp.status})`);
  }
  const payload = await resp.json();
  const token = payload?.access_token;
  if (!token) {
    throw new Error("Dropbox token response missing access_token.");
  }
  return String(token);
}

async function dropboxUploadFile(
  accessToken: string,
  path: string,
  contents: Uint8Array | string,
): Promise<void> {
  const args = { path, mode: "add", autorename: true, mute: false };
  const resp = await fetch(DROPBOX_UPLOAD_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Dropbox-API-Arg": JSON.stringify(args),
      "Content-Type": "application/octet-stream",
    },
    body: typeof contents === "string"
      ? new TextEncoder().encode(contents)
      : Uint8Array.from(contents),
  });
  if (!resp.ok) {
    throw new Error(`Dropbox upload failed (${resp.status})`);
  }
}

async function dropboxUploadFileWithRetry(
  config: DropboxConfig,
  path: string,
  contents: Uint8Array | string,
): Promise<void> {
  let token = await dropboxRefreshAccessToken(config);
  try {
    await dropboxUploadFile(token, path, contents);
  } catch (err) {
    if (String(err).includes("401")) {
      token = await dropboxRefreshAccessToken(config);
      await dropboxUploadFile(token, path, contents);
      return;
    }
    throw err;
  }
}

function recordErrorLogLine(
  errorLogLines: string[] | null,
  entry: ErrorLogEntry,
): void {
  if (!errorLogLines) {
    return;
  }
  const stamp = new Date().toISOString();
  let context = entry.context ? { ...entry.context } : undefined;
  if (entry.connector_id !== undefined && entry.connector_id !== null) {
    if (context) {
      context.connector_id = entry.connector_id;
    } else {
      context = { connector_id: entry.connector_id };
    }
  }
  const ctx = context ? ` ${JSON.stringify(context)}` : "";
  const severity = entry.severity || "error";
  errorLogLines.push(
    `[${stamp}] ${severity.toUpperCase()} ${entry.message}${ctx}`,
  );
}

async function logError(entry: ErrorLogEntry): Promise<void> {
  recordErrorLogLine(errorLogLines, entry);
  try {
    await rpcRequest<Array<{ id: string }>>("uk_aq_rpc_error_log_insert", {
      entry: {
        source: "ingest_openaq",
        severity: entry.severity,
        message: entry.message,
        context: entry.context ?? null,
        connector_id: entry.connector_id ?? null,
      },
    });
  } catch (_err) {
    // Best-effort logging; never throw from logError.
  }
}

function parseBbox(value: string): string {
  const parts = value.split(",").map((part) => part.trim()).filter((part) =>
    part
  );
  if (parts.length !== 4) {
    throw new Error("OPENAQ_BBOX must have 4 comma-delimited values.");
  }
  const numbers = parts.map((part) => Number(part));
  if (numbers.some((num) => !Number.isFinite(num))) {
    throw new Error("OPENAQ_BBOX contains invalid numbers.");
  }
  return numbers.join(",");
}

function sleep(ms: number): Promise<void> {
  if (!Number.isFinite(ms) || ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function appendSample(
  values: number[] | null,
  value: number,
  maxSamples = 30,
): number[] {
  const cleaned = Array.isArray(values)
    ? values.filter((v) => Number.isFinite(v))
    : [];
  const next = [...cleaned, value].slice(-maxSamples);
  return next;
}

function parseEnvBoolean(raw: string | undefined, fallback: boolean): boolean {
  const value = String(raw ?? "")
    .trim()
    .toLowerCase();
  if (!value) {
    return fallback;
  }
  if (["1", "true", "yes", "on"].includes(value)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(value)) {
    return false;
  }
  return fallback;
}

function parseLagStat(raw: string | undefined): LagStat {
  const value = String(raw ?? "")
    .trim()
    .toLowerCase();
  if (value === "median" || value === "p25" || value === "min") {
    return value;
  }
  return DEFAULT_LAG_STAT;
}

function medianSeconds(values: number[] | null): number | null {
  if (!Array.isArray(values) || values.length === 0) {
    return null;
  }
  const sorted = values
    .filter((v) => Number.isFinite(v))
    .map((v) => Math.max(0, Math.round(v)))
    .sort((a, b) => a - b);
  if (!sorted.length) {
    return null;
  }
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[mid];
  }
  return Math.round((sorted[mid - 1] + sorted[mid]) / 2);
}

function percentileSeconds(
  values: number[] | null,
  percentile: number,
): number | null {
  if (
    !Array.isArray(values) ||
    values.length === 0 ||
    !Number.isFinite(percentile)
  ) {
    return null;
  }
  const sorted = values
    .filter((v) => Number.isFinite(v))
    .map((v) => Math.max(0, Math.round(v)))
    .sort((a, b) => a - b);
  if (!sorted.length) {
    return null;
  }
  const clamped = Math.min(1, Math.max(0, percentile));
  const index = Math.max(
    0,
    Math.min(
      sorted.length - 1,
      Math.ceil(clamped * sorted.length) - 1,
    ),
  );
  return sorted[index];
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

function lagSecondsByStat(values: number[] | null): number | null {
  if (OPENAQ_LAG_STAT === "median") {
    return medianSeconds(values);
  }
  if (OPENAQ_LAG_STAT === "p25") {
    return percentileSeconds(values, 0.25);
  }
  return minSeconds(values);
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

async function deflateRaw(data: Uint8Array): Promise<Uint8Array> {
  const normalized = Uint8Array.from(data);
  const stream = new Blob([normalized]).stream().pipeThrough(
    new CompressionStream("deflate-raw"),
  );
  return new Uint8Array(await new Response(stream).arrayBuffer());
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
    central.push(
      value & 0xff,
      (value >>> 8) & 0xff,
      (value >>> 16) & 0xff,
      (value >>> 24) & 0xff,
    );
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
  e32(centralHeader.length);
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

function parseRateLimitHeaders(headers: Headers): {
  limit: number | null;
  remaining: number | null;
  reset: number | null;
  used: number | null;
} {
  const toNumber = (value: string | null): number | null => {
    if (!value) {
      return null;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  };
  return {
    limit: toNumber(headers.get("x-ratelimit-limit")),
    remaining: toNumber(headers.get("x-ratelimit-remaining")),
    reset: toNumber(headers.get("x-ratelimit-reset")),
    used: toNumber(headers.get("x-ratelimit-used")),
  };
}

function positiveInt(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.floor(value);
}

function nonNegativeInt(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value < 0) {
    return fallback;
  }
  return Math.floor(value);
}

type RateLimitState = {
  remaining: number | null;
  stop: boolean;
  stopReason: string | null;
  limit: number | null;
  firstRemaining: number | null;
  reset: number | null;
  resetAt: string | null;
};

const UNIX_TIMESTAMP_SECONDS_THRESHOLD = 1e9;
const UNIX_TIMESTAMP_MILLISECONDS_THRESHOLD = 1e12;

function rateLimitResetToEpochMs(reset: number | null): number | null {
  if (reset === null || !Number.isFinite(reset)) {
    return null;
  }
  if (reset > UNIX_TIMESTAMP_MILLISECONDS_THRESHOLD) {
    return reset;
  }
  if (reset > UNIX_TIMESTAMP_SECONDS_THRESHOLD) {
    return reset * 1000;
  }
  return Date.now() + Math.max(0, reset * 1000);
}

function rateLimitDelayMs(reset: number | null): number {
  const resetMs = rateLimitResetToEpochMs(reset);
  if (resetMs === null) {
    return 0;
  }
  return Math.max(0, resetMs - Date.now());
}

async function maybeSleepForRateLimit(
  headers: Headers,
  rawRecorder?: RawRecorder | null,
  status?: number,
): Promise<void> {
  const info = parseRateLimitHeaders(headers);
  if (info.remaining === null || info.reset === null) {
    return;
  }
  if (info.remaining <= 1) {
    const delayMs = rateLimitDelayMs(info.reset);
    if (rawRecorder) {
      rawRecorder.recordEvent("rate_limit", {
        status: status ?? null,
        remaining: info.remaining,
        limit: info.limit,
        used: info.used,
        reset: info.reset,
        sleep_ms: delayMs,
      });
    }
    await sleep(delayMs);
  }
}

function toNumberOrNull(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return parsed;
}

function applySharedBudgetRow(row: OpenAQSharedBudgetReserveRow): void {
  sharedBudgetState.granted = row.granted === true;
  sharedBudgetState.reason = row.reason ?? null;
  sharedBudgetState.requestedTokens = toNumberOrNull(row.requested_tokens);
  sharedBudgetState.minuteUsedBefore = toNumberOrNull(row.minute_used_before);
  sharedBudgetState.minuteUsedAfter = toNumberOrNull(row.minute_used_after);
  sharedBudgetState.minuteRemaining = toNumberOrNull(row.minute_remaining);
  sharedBudgetState.hourUsedBefore = toNumberOrNull(row.hour_used_before);
  sharedBudgetState.hourUsedAfter = toNumberOrNull(row.hour_used_after);
  sharedBudgetState.hourRemaining = toNumberOrNull(row.hour_remaining);
  sharedBudgetState.minuteResetAt = row.minute_reset_at ?? null;
  sharedBudgetState.hourResetAt = row.hour_reset_at ?? null;
  sharedBudgetState.retryAfterSeconds = toNumberOrNull(
    row.retry_after_seconds,
  );
}

function sharedBudgetResponseFields(): Record<string, unknown> {
  return {
    shared_budget_enabled: sharedBudgetState.enabled,
    shared_budget_key: sharedBudgetState.key,
    shared_budget_caller: sharedBudgetState.caller,
    shared_budget_minute_limit: sharedBudgetState.minuteLimit,
    shared_budget_hour_limit: sharedBudgetState.hourLimit,
    shared_budget_granted: sharedBudgetState.granted,
    shared_budget_reason: sharedBudgetState.reason,
    shared_budget_requested_tokens: sharedBudgetState.requestedTokens,
    shared_budget_minute_used_before: sharedBudgetState.minuteUsedBefore,
    shared_budget_minute_used_after: sharedBudgetState.minuteUsedAfter,
    shared_budget_minute_remaining: sharedBudgetState.minuteRemaining,
    shared_budget_minute_reset_at: sharedBudgetState.minuteResetAt,
    shared_budget_hour_used_before: sharedBudgetState.hourUsedBefore,
    shared_budget_hour_used_after: sharedBudgetState.hourUsedAfter,
    shared_budget_hour_remaining: sharedBudgetState.hourRemaining,
    shared_budget_hour_reset_at: sharedBudgetState.hourResetAt,
    shared_budget_retry_after_seconds: sharedBudgetState.retryAfterSeconds,
  };
}

function isSharedBudgetLimitReason(reason: string | null): boolean {
  return reason === "shared_budget_minute_limit" ||
    reason === "shared_budget_hour_limit";
}

function buildSharedBudgetWarning(
  context: Record<string, unknown>,
  connectorId: string | number | null,
): RunWarningEntry {
  return {
    type: "openaq_shared_budget_blocked",
    reason: rateLimitState.stopReason,
    message: "OpenAQ shared budget blocked request",
    connector_id: connectorId,
    shared_budget_key: sharedBudgetState.key,
    shared_budget_caller: sharedBudgetState.caller,
    retry_after_seconds: sharedBudgetState.retryAfterSeconds,
    occurred_at: new Date().toISOString(),
    ...sharedBudgetResponseFields(),
    ...context,
  };
}

function shouldTreatAsSharedBudgetWarning(error: unknown): boolean {
  if (isSharedBudgetLimitReason(rateLimitState.stopReason)) {
    return true;
  }
  const sharedReason = String(sharedBudgetState.reason ?? "").trim()
    .toLowerCase();
  if (sharedReason === "minute_limit" || sharedReason === "hour_limit") {
    return true;
  }
  const errorMessage = String(error ?? "").toLowerCase();
  return errorMessage.includes("shared budget blocked request");
}

async function reserveSharedOpenaqBudget(
  tokens: number,
  rawRecorder?: RawRecorder | null,
): Promise<boolean> {
  const requestedTokens = Math.max(0, Math.trunc(tokens));
  sharedBudgetState.requestedTokens = requestedTokens;
  if (!sharedBudgetState.enabled) {
    sharedBudgetState.granted = true;
    sharedBudgetState.reason = "disabled";
    return true;
  }

  const budgetResponse = await rpcRequest<OpenAQSharedBudgetReserveRow[]>(
    "uk_aq_rpc_openaq_token_budget_reserve",
    {
      p_budget_key: sharedBudgetState.key,
      p_tokens: requestedTokens,
      p_minute_limit: sharedBudgetState.minuteLimit,
      p_hour_limit: sharedBudgetState.hourLimit,
      p_caller: sharedBudgetState.caller,
    },
  );

  if (budgetResponse.error || !Array.isArray(budgetResponse.data)) {
    sharedBudgetState.granted = false;
    sharedBudgetState.reason = "shared_budget_rpc_error";
    if (rawRecorder) {
      rawRecorder.recordEvent("shared_budget", {
        granted: false,
        reason: "shared_budget_rpc_error",
        error: budgetResponse.error?.message ?? "missing budget response",
        requested_tokens: requestedTokens,
      });
    }
    return false;
  }

  const row = budgetResponse.data[0];
  if (!row || typeof row !== "object") {
    sharedBudgetState.granted = false;
    sharedBudgetState.reason = "shared_budget_rpc_empty";
    if (rawRecorder) {
      rawRecorder.recordEvent("shared_budget", {
        granted: false,
        reason: "shared_budget_rpc_empty",
        requested_tokens: requestedTokens,
      });
    }
    return false;
  }

  applySharedBudgetRow(row);
  if (rawRecorder) {
    rawRecorder.recordEvent("shared_budget", {
      granted: sharedBudgetState.granted,
      reason: sharedBudgetState.reason,
      requested_tokens: sharedBudgetState.requestedTokens,
      minute_limit: sharedBudgetState.minuteLimit,
      minute_used_before: sharedBudgetState.minuteUsedBefore,
      minute_used_after: sharedBudgetState.minuteUsedAfter,
      minute_remaining: sharedBudgetState.minuteRemaining,
      minute_reset_at: sharedBudgetState.minuteResetAt,
      hour_limit: sharedBudgetState.hourLimit,
      hour_used_before: sharedBudgetState.hourUsedBefore,
      hour_used_after: sharedBudgetState.hourUsedAfter,
      hour_remaining: sharedBudgetState.hourRemaining,
      hour_reset_at: sharedBudgetState.hourResetAt,
      retry_after_seconds: sharedBudgetState.retryAfterSeconds,
      budget_key: sharedBudgetState.key,
      caller: sharedBudgetState.caller,
    });
  }
  return sharedBudgetState.granted === true;
}

async function openaqRequest(
  path: string,
  params?: Record<string, string | number>,
  rawRecorder?: RawRecorder | null,
): Promise<unknown> {
  const url = new URL(`${OPENAQ_BASE_URL}/${path.replace(/^\//, "")}`);
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  const retries = Number.isFinite(OPENAQ_RATE_LIMIT_RETRIES)
    ? Math.max(1, OPENAQ_RATE_LIMIT_RETRIES)
    : DEFAULT_RATE_LIMIT_RETRIES;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    if (requestBudgetState.total >= requestBudgetState.maxPerRun) {
      rateLimitState.stop = true;
      rateLimitState.stopReason = "max_requests_per_run";
      throw new Error(
        `OpenAQ request budget exceeded (${requestBudgetState.maxPerRun}); skipped ${path}`,
      );
    }

    const sharedBudgetGranted = await reserveSharedOpenaqBudget(1, rawRecorder);
    if (!sharedBudgetGranted) {
      const sharedReason = sharedBudgetState.reason ?? "shared_budget_denied";
      rateLimitState.stop = true;
      if (sharedReason === "minute_limit") {
        rateLimitState.stopReason = "shared_budget_minute_limit";
      } else if (sharedReason === "hour_limit") {
        rateLimitState.stopReason = "shared_budget_hour_limit";
      } else {
        rateLimitState.stopReason = sharedReason;
      }
      throw new Error(
        `OpenAQ shared budget blocked request (${rateLimitState.stopReason})`,
      );
    }

    requestBudgetState.total += 1;

    let resp: Response;
    try {
      resp = await fetch(url.toString(), { headers: openaqHeaders() });
    } catch (err) {
      if (attempt < retries && !rateLimitState.stop) {
        await sleep(Math.min(5000, attempt * 1000));
        continue;
      }
      throw new Error(`OpenAQ request failed (network): ${String(err)}`);
    }

    const contentType = resp.headers.get("content-type") ?? "";
    const payload = contentType.includes("application/json")
      ? await resp.json()
      : await resp.text();
    const info = parseRateLimitHeaders(resp.headers);
    if (info.remaining !== null && Number.isFinite(info.remaining)) {
      rateLimitState.remaining = info.remaining;
      if (rateLimitState.firstRemaining === null) {
        rateLimitState.firstRemaining = info.remaining;
      }
      if (info.limit !== null && Number.isFinite(info.limit)) {
        rateLimitState.limit = info.limit;
      }
      if (info.reset !== null && Number.isFinite(info.reset)) {
        rateLimitState.reset = info.reset;
        const resetMs = rateLimitResetToEpochMs(info.reset);
        rateLimitState.resetAt = resetMs === null
          ? null
          : new Date(resetMs).toISOString();
      }
      if (info.remaining <= OPENAQ_RATE_LIMIT_STOP_THRESHOLD) {
        rateLimitState.stop = true;
        rateLimitState.stopReason = "remaining_low";
      }
    }
    if (rawRecorder) {
      rawRecorder.recordResponse(path, params ?? {}, resp.status, payload);
    }
    if (resp.status === 401) {
      rateLimitState.stop = true;
      rateLimitState.stopReason = "auth_401";
      const message = typeof payload === "string"
        ? payload
        : JSON.stringify(payload);
      throw new Error(`OpenAQ request failed (401): ${message}`);
    }
    if (resp.status === 403) {
      rateLimitState.stop = true;
      rateLimitState.stopReason = "auth_403";
      const message = typeof payload === "string"
        ? payload
        : JSON.stringify(payload);
      throw new Error(`OpenAQ request failed (403): ${message}`);
    }
    if (resp.status === 429) {
      rateLimitState.stop = true;
      rateLimitState.stopReason = "rate_limit_429";
      if (rawRecorder) {
        rawRecorder.recordEvent("rate_limit", {
          status: resp.status,
          remaining: info.remaining,
          limit: info.limit,
          used: info.used,
          reset: info.reset,
          sleep_ms: 0,
        });
      }
      throw new Error("OpenAQ request failed (429): rate limit exceeded");
    }
    if (!resp.ok) {
      const message = typeof payload === "string"
        ? payload
        : JSON.stringify(payload);
      throw new Error(`OpenAQ request failed (${resp.status}): ${message}`);
    }
    await maybeSleepForRateLimit(resp.headers, rawRecorder, resp.status);
    return payload;
  }
  throw new Error("OpenAQ request failed: retries exhausted");
}

async function listLocations(
  bbox: string,
  rawRecorder?: RawRecorder | null,
): Promise<OpenAQLocation[]> {
  const results: OpenAQLocation[] = [];
  const limit = Number.isFinite(OPENAQ_PAGE_LIMIT) && OPENAQ_PAGE_LIMIT > 0
    ? Math.min(OPENAQ_PAGE_LIMIT, 1000)
    : DEFAULT_PAGE_LIMIT;
  let page = 1;
  while (true) {
    const payload = await openaqRequest(
      "locations",
      { bbox, limit, page },
      rawRecorder,
    );
    const payloadResults = (payload as { results?: unknown } | null)?.results;
    const pageResults = Array.isArray(payloadResults)
      ? payloadResults as OpenAQLocation[]
      : [];
    results.push(...pageResults);
    if (!pageResults.length) {
      break;
    }
    if (pageResults.length < limit) {
      break;
    }
    page += 1;
    if (
      Number.isFinite(OPENAQ_MAX_PAGES) && OPENAQ_MAX_PAGES > 0 &&
      page > OPENAQ_MAX_PAGES
    ) {
      break;
    }
  }
  return results;
}

async function listLatestForLocation(
  locationId: string,
  rawRecorder?: RawRecorder | null,
): Promise<OpenAQLatestRecord[]> {
  const payload = await openaqRequest(
    `locations/${locationId}/latest`,
    { limit: 1000 },
    rawRecorder,
  );
  const payloadResults = (payload as { results?: unknown } | null)?.results;
  return Array.isArray(payloadResults)
    ? payloadResults as OpenAQLatestRecord[]
    : [];
}

async function listHourlyMeasurements(
  timeseriesRef: string,
  datetimeFrom: string | null,
  datetimeTo: string | null,
  rawRecorder?: RawRecorder | null,
): Promise<{ records: OpenAQHourlyRecord[]; pages: number; limit: number }> {
  const results: OpenAQHourlyRecord[] = [];
  const limit = Number.isFinite(OPENAQ_PAGE_LIMIT) && OPENAQ_PAGE_LIMIT > 0
    ? Math.min(OPENAQ_PAGE_LIMIT, 1000)
    : DEFAULT_PAGE_LIMIT;
  let page = 1;
  let pages = 0;
  while (true) {
    if (rateLimitState.stop) {
      break;
    }
    const params: Record<string, string | number> = { limit, page };
    if (datetimeFrom) {
      params.datetime_from = datetimeFrom;
    }
    if (datetimeTo) {
      params.datetime_to = datetimeTo;
    }
    const payload = await openaqRequest(
      `sensors/${timeseriesRef}/measurements/hourly`,
      params,
      rawRecorder,
    );
    pages += 1;
    const payloadResults = (payload as { results?: unknown } | null)?.results;
    const pageResults = Array.isArray(payloadResults)
      ? payloadResults as OpenAQHourlyRecord[]
      : [];
    results.push(...pageResults);
    if (!pageResults.length || pageResults.length < limit) {
      break;
    }
    page += 1;
    if (
      Number.isFinite(OPENAQ_MAX_PAGES) && OPENAQ_MAX_PAGES > 0 &&
      page > OPENAQ_MAX_PAGES
    ) {
      break;
    }
  }
  return { records: results, pages, limit };
}

async function runPool<T>(
  items: T[],
  concurrency: number,
  worker: (item: T) => Promise<void>,
  shouldStop?: () => boolean,
): Promise<void> {
  const pool = new Set<Promise<void>>();
  const limit = Number.isFinite(concurrency) && concurrency > 0
    ? Math.floor(concurrency)
    : 1;
  for (const item of items) {
    if (shouldStop && shouldStop()) {
      break;
    }
    const task = worker(item);
    pool.add(task);
    task.finally(() => pool.delete(task));
    if (pool.size >= limit) {
      await Promise.race(pool);
    }
  }
  await Promise.all(pool);
}

function recordObservation(
  observationsByTimeseriesRef: Map<string, Map<string, number | null>>,
  latestByTimeseriesRef: Map<
    string,
    { observed_at: string; value: number | null }
  >,
  latestObservedByStationId: Map<number, string>,
  timeseriesRef: string,
  observedAt: string,
  value: number | null,
  stationId: number | null,
  nowMs: number,
  windowMs: number | null,
  stationIdByObservedRef?: Map<string, number>,
): void {
  const observedMs = Date.parse(observedAt);
  if (!Number.isFinite(observedMs)) {
    return;
  }
  if (windowMs && observedMs < nowMs - windowMs) {
    return;
  }
  let timeseriesObservations = observationsByTimeseriesRef.get(timeseriesRef);
  if (!timeseriesObservations) {
    timeseriesObservations = new Map();
    observationsByTimeseriesRef.set(timeseriesRef, timeseriesObservations);
  }
  if (!timeseriesObservations.has(observedAt)) {
    timeseriesObservations.set(observedAt, value);
  } else if (
    timeseriesObservations.get(observedAt) === null && value !== null
  ) {
    timeseriesObservations.set(observedAt, value);
  }
  const existing = latestByTimeseriesRef.get(timeseriesRef);
  if (!existing || observedAt > existing.observed_at) {
    latestByTimeseriesRef.set(timeseriesRef, {
      observed_at: observedAt,
      value,
    });
  }
  if (stationId !== null) {
    const current = latestObservedByStationId.get(stationId);
    if (!current || observedAt > current) {
      latestObservedByStationId.set(stationId, observedAt);
    }
    if (stationIdByObservedRef && !stationIdByObservedRef.has(timeseriesRef)) {
      stationIdByObservedRef.set(timeseriesRef, stationId);
    }
  }
}

function resolveLocationId(location: OpenAQLocation): string | null {
  if (location?.id === null || location?.id === undefined) {
    return null;
  }
  return String(location.id);
}

function resolveLocationName(location: OpenAQLocation): string | null {
  if (location?.name && String(location.name).trim()) {
    return String(location.name).trim();
  }
  if (location?.locality && String(location.locality).trim()) {
    return String(location.locality).trim();
  }
  return null;
}

function resolveProviderName(location: OpenAQLocation): string | null {
  if (location?.provider?.name && String(location.provider.name).trim()) {
    const raw = String(location.provider.name).trim();
    return PROVIDER_SHORTNAMES[raw] ?? raw;
  }
  return null;
}

function resolveOwnerName(location: OpenAQLocation): string | null {
  const owner = location?.owner;
  if (typeof owner === "string") {
    const raw = owner.trim();
    return raw ? raw : null;
  }
  if (owner && typeof owner === "object" && "name" in owner) {
    const raw = String((owner as { name?: string | null }).name ?? "").trim();
    return raw ? raw : null;
  }
  return null;
}

function normalizeOwnerName(ownerName: string | null): string | null {
  if (!ownerName) {
    return null;
  }
  const trimmed = ownerName.trim();
  if (!trimmed) {
    return null;
  }
  if (trimmed.toLowerCase().startsWith("unknown")) {
    return null;
  }
  return trimmed;
}

function buildStationName(
  rawName: string | null,
  providerName: string | null,
  ownerName: string | null,
): string | null {
  const baseName = rawName && providerName
    ? `${providerName} ${rawName}`
    : rawName;
  if (!baseName) {
    return baseName;
  }
  const providerToken = providerName ? providerName.trim().toLowerCase() : null;
  const ownerToken = ownerName ? ownerName.trim().toLowerCase() : null;
  if (
    ownerName && ownerToken && providerToken && ownerToken === providerToken
  ) {
    return baseName;
  }
  if (ownerName) {
    return `${baseName} - ${ownerName}`;
  }
  return baseName;
}

function resolveTimeseriesRefFromLatest(
  record: OpenAQLatestRecord,
): string | null {
  const raw = record?.sensorsId;
  if (raw === null || raw === undefined) {
    return null;
  }
  return String(raw);
}

function resolveHourlyObservedAt(
  record: OpenAQHourlyRecord,
  nowMs = Date.now(),
): string | null {
  const period = (record as {
    period?: {
      datetimeTo?: { utc?: string | null } | null;
      datetimeFrom?: { utc?: string | null } | null;
    } | null;
    coverage?: {
      datetimeTo?: { utc?: string | null } | null;
      datetimeFrom?: { utc?: string | null } | null;
    } | null;
  })?.period;
  const coverage = (record as {
    coverage?: {
      datetimeTo?: { utc?: string | null } | null;
      datetimeFrom?: { utc?: string | null } | null;
    } | null;
  })?.coverage;
  const coverageTo = coverage?.datetimeTo?.utc ?? null;
  if (coverageTo) {
    return String(coverageTo);
  }
  const recordUtc = record?.datetime?.utc ?? null;
  const periodTo = period?.datetimeTo?.utc ?? null;
  const periodFrom = period?.datetimeFrom?.utc ?? null;
  const observedAt = recordUtc ?? periodTo ?? periodFrom ?? null;
  if (!observedAt) {
    return null;
  }
  const observedMs = Date.parse(observedAt);
  if (Number.isFinite(observedMs) && observedMs > nowMs) {
    return periodFrom
      ? String(periodFrom)
      : String(new Date(nowMs).toISOString());
  }
  return String(observedAt);
}

function resolveCoordinates(
  location: OpenAQLocation,
): { longitude: number | null; latitude: number | null } {
  const longitude = location?.coordinates?.longitude;
  const latitude = location?.coordinates?.latitude;
  return {
    longitude: Number.isFinite(longitude) ? Number(longitude) : null,
    latitude: Number.isFinite(latitude) ? Number(latitude) : null,
  };
}

async function loadConnector(
  connectorCode: string,
): Promise<ConnectorRow | null> {
  const { data, error } = await rpcRequest<ConnectorRow[]>(
    "uk_aq_rpc_connector_select",
    {
      connector_code: connectorCode,
    },
  );
  if (error) {
    throw new Error(`Connector fetch failed: ${error.message}`);
  }
  return data && data[0] ? data[0] : null;
}

async function fetchStationNames(
  connectorId: string,
  serviceRef: string,
  stationRefs: string[],
): Promise<Record<string, string | null>> {
  const mapping: Record<string, string | null> = {};
  for (let idx = 0; idx < stationRefs.length; idx += 200) {
    const chunk = stationRefs.slice(idx, idx + 200);
    const { data } = await rpcRequest<
      Array<{ station_ref: string; station_name: string | null }>
    >(
      "uk_aq_rpc_station_names",
      {
        connector_id: Number(connectorId),
        service_ref: serviceRef,
        station_refs: chunk,
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
    const { data } = await rpcRequest<
      Array<{ id: number; station_ref: string }>
    >(
      "uk_aq_rpc_station_ids",
      {
        connector_id: Number(connectorId),
        service_ref: serviceRef,
        station_refs: chunk,
      },
    );
    for (const row of data ?? []) {
      mapping[String(row.station_ref)] = Number(row.id);
    }
  }
  return mapping;
}

async function upsertStationMetadata(
  attributesByStation: Record<number, Record<string, unknown>>,
): Promise<number> {
  const stationIds = Object.keys(attributesByStation).map(Number);
  if (!stationIds.length) {
    return 0;
  }
  const rows = stationIds.map((stationId) => ({
    station_id: stationId,
    attributes: attributesByStation[stationId],
    updated_at: new Date().toISOString(),
  }));
  const { data, error } = await rpcRequest<
    Array<{ station_metadata_upserted: number }>
  >(
    "uk_aq_rpc_station_metadata_upsert",
    { rows },
  );
  if (error) {
    throw new Error(`Station metadata upsert failed: ${error.message}`);
  }
  return data?.[0]?.station_metadata_upserted ?? 0;
}

async function upsertStations(
  locations: OpenAQLocation[],
  connectorId: string,
  serviceRef: string,
  overwriteStationName: boolean,
): Promise<number> {
  const rowsByRef: Record<string, Record<string, unknown>> = {};
  const ownerByRef: Record<string, string> = {};
  for (const location of locations) {
    const stationRef = resolveLocationId(location);
    if (!stationRef) {
      continue;
    }
    const { longitude, latitude } = resolveCoordinates(location);
    const rawName = resolveLocationName(location);
    const providerName = resolveProviderName(location);
    const ownerName = normalizeOwnerName(resolveOwnerName(location));
    const stationName = buildStationName(rawName, providerName, ownerName);
    const row: Record<string, unknown> = {
      station_ref: stationRef,
      service_ref: String(serviceRef),
      label: rawName ?? `OpenAQ ${stationRef}`,
      station_name: stationName,
      station_type: location?.isMobile ? "mobile" : "fixed",
      region: location?.locality ?? location?.country?.name ?? null,
      geometry: longitude !== null && latitude !== null
        ? `SRID=4326;POINT(${longitude} ${latitude})`
        : null,
      connector_id: connectorId,
      last_seen_at: new Date().toISOString(),
      removed_at: null,
    };
    rowsByRef[stationRef] = rowsByRef[stationRef]
      ? { ...rowsByRef[stationRef], ...row }
      : row;
    if (ownerName) {
      ownerByRef[stationRef] = ownerName;
    }
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
  const { data, error } = await rpcRequest<
    Array<{ stations_upserted: number }>
  >(
    "uk_aq_rpc_stations_upsert",
    { rows },
  );
  if (error) {
    throw new Error(`Stations upsert failed: ${error.message}`);
  }
  if (Object.keys(ownerByRef).length) {
    const stationIds = await fetchStationIds(
      connectorId,
      serviceRef,
      Object.keys(ownerByRef),
    );
    const attributesByStation: Record<number, Record<string, unknown>> = {};
    for (const [stationRef, ownerName] of Object.entries(ownerByRef)) {
      const stationId = stationIds[stationRef];
      if (!stationId) {
        continue;
      }
      attributesByStation[stationId] = { openaq_owner: ownerName };
    }
    await upsertStationMetadata(attributesByStation);
  }
  return data?.[0]?.stations_upserted ?? 0;
}

type ParameterMeta = {
  name: string;
  displayName: string | null;
  units: string | null;
};

function canonicalObservedPropertyCode(raw: string): string {
  const normalized = raw.trim().toLowerCase();
  const compact = normalized.replace(/[^a-z0-9]+/g, "");
  if (compact === "pm25") {
    return "pm25";
  }
  if (compact === "pm10") {
    return "pm10";
  }
  if (compact === "no2") {
    return "no2";
  }
  if (compact === "o3") {
    return "o3";
  }
  if (compact === "so2") {
    return "so2";
  }
  if (compact === "co") {
    return "co";
  }
  if (compact === "temperature" || compact === "temp") {
    return "temperature";
  }
  if (compact === "humidity" || compact === "relativehumidity") {
    return "humidity";
  }
  if (compact === "pressure" || compact === "airpressure") {
    return "pressure";
  }
  return normalized.replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
}

function observedPropertyDomain(code: string): "aq" | "met" {
  if (["temperature", "humidity", "pressure"].includes(code)) {
    return "met";
  }
  return "aq";
}

async function upsertPhenomena(
  connectorId: string,
  parameters: Record<string, ParameterMeta>,
): Promise<Record<string, number>> {
  const payload = Object.values(parameters).map((meta) => {
    const code = canonicalObservedPropertyCode(meta.name);
    return {
      connector_id: connectorId,
      source_label: `openaq:${meta.name}`,
      label: meta.displayName ?? meta.name,
      notation: meta.displayName ?? meta.name,
      pollutant_label: meta.name,
      observed_property_code: code,
      observed_property_display_name: meta.displayName ?? meta.name,
      observed_property_domain: observedPropertyDomain(code),
      canonical_uom: meta.units ?? null,
    };
  });
  if (payload.length) {
    const { error } = await rpcRequest<Array<{ phenomena_upserted: number }>>(
      "uk_aq_rpc_phenomena_upsert",
      { rows: payload },
    );
    if (error) {
      throw new Error(`Phenomena upsert failed: ${error.message}`);
    }
  }
  const sourceLabels = Object.values(parameters).map((meta) =>
    `openaq:${meta.name}`
  );
  if (!sourceLabels.length) {
    return {};
  }
  const { data } = await rpcRequest<
    Array<{ id: number; source_label?: string; eionet_uri?: string }>
  >(
    "uk_aq_rpc_phenomena_ids",
    {
      connector_id: Number(connectorId),
      eionet_uris: sourceLabels,
    },
  );
  const idsByUri: Record<string, number> = {};
  for (const row of data ?? []) {
    const sourceLabel = row?.source_label ?? row?.eionet_uri;
    if (sourceLabel) {
      idsByUri[sourceLabel] = Number(row.id);
    }
  }
  const idsByName: Record<string, number> = {};
  for (const meta of Object.values(parameters)) {
    const phenId = idsByUri[`openaq:${meta.name}`];
    if (phenId) {
      idsByName[meta.name] = phenId;
    }
  }
  return idsByName;
}

async function upsertTimeseries(
  rows: Array<Record<string, unknown>>,
): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  const { data, error } = await rpcRequest<
    Array<{ timeseries_upserted: number }>
  >(
    "uk_aq_rpc_timeseries_upsert",
    { rows },
  );
  if (error) {
    throw new Error(`Timeseries upsert failed: ${error.message}`);
  }
  return data?.[0]?.timeseries_upserted ?? 0;
}

async function updateTimeseriesLastValues(
  rows: Array<{ id: number; last_value: number; last_value_at: string }>,
  errors: string[],
): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  const { data, error } = await rpcRequest<
    Array<{ timeseries_updated: number }>
  >(
    "uk_aq_rpc_timeseries_last_values_update",
    { rows },
  );
  if (error) {
    const message = `timeseries update failed: ${error.message}`;
    errors.push(message);
    console.warn(message);
    return 0;
  }
  return data?.[0]?.timeseries_updated ?? 0;
}

async function fetchTimeseriesIds(
  connectorId: string,
  serviceRef: string,
  timeseriesRefs: string[],
): Promise<Record<string, number>> {
  const mapping: Record<string, number> = {};
  for (let idx = 0; idx < timeseriesRefs.length; idx += 200) {
    const chunk = timeseriesRefs.slice(idx, idx + 200);
    const { data } = await rpcRequest<
      Array<{ id: number; timeseries_ref: string }>
    >(
      "uk_aq_rpc_timeseries_ids",
      {
        connector_id: Number(connectorId),
        service_ref: serviceRef,
        timeseries_refs: chunk,
      },
    );
    for (const row of data ?? []) {
      mapping[String(row.timeseries_ref)] = Number(row.id);
    }
  }
  return mapping;
}

async function fetchTimeseriesStationIds(
  timeseriesIds: number[],
): Promise<Record<number, number>> {
  const ids = timeseriesIds.filter((id) => Number.isFinite(id));
  if (!ids.length) {
    return {};
  }
  const mapping: Record<number, number> = {};
  for (let idx = 0; idx < ids.length; idx += 200) {
    const chunk = ids.slice(idx, idx + 200);
    const { data, error } = await postgrestRequest<
      Array<{ id: number; station_id: number | null }>
    >(
      "GET",
      "timeseries",
      {
        select: "id,station_id",
        id: postgrestIn(chunk.map((value) => String(value))),
      },
    );
    if (error) {
      throw new Error(
        `Failed to load timeseries station ids: ${error.message}`,
      );
    }
    for (const row of data ?? []) {
      if (row.station_id) {
        mapping[Number(row.id)] = Number(row.station_id);
      }
    }
  }
  return mapping;
}

async function upsertObservations(
  rows: Array<Record<string, unknown>>,
  runtimeBudget?: { shouldStop: () => boolean; remainingRuntimeMs: () => number },
) {
  return await writeIngestDbObservations({
    rows,
    chunkSize: rows.length || 1,
    connectorCode: "openaq",
    logger: console,
    runtimeBudget,
    config: { minimumAttemptRuntimeMs: DEFAULT_POSTGREST_TIMEOUT_MS },
    writeChunk: async (chunk: Array<Record<string, unknown>>) => {
      const { error } = await rpcRequest<
        Array<{ observations_upserted: number }>
      >(
        "uk_aq_rpc_observations_upsert",
        { rows: chunk },
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

function collectParameters(
  locations: OpenAQLocation[],
): Record<string, ParameterMeta> {
  const parameters: Record<string, ParameterMeta> = {};
  for (const location of locations) {
    for (const timeseries of location?.sensors ?? []) {
      const paramName = timeseries?.parameter?.name;
      if (!paramName || !String(paramName).trim()) {
        continue;
      }
      const name = String(paramName).trim();
      if (!parameters[name]) {
        parameters[name] = {
          name,
          displayName: timeseries?.parameter?.displayName
            ? String(timeseries.parameter.displayName)
            : null,
          units: timeseries?.parameter?.units
            ? String(timeseries.parameter.units)
            : null,
        };
      }
    }
  }
  return parameters;
}

function collectTimeseriesRefs(
  locations: OpenAQLocation[],
): Map<string, { locationId: string; parameter: ParameterMeta }> {
  const timeseriesRefs = new Map<
    string,
    { locationId: string; parameter: ParameterMeta }
  >();
  for (const location of locations) {
    const locationId = resolveLocationId(location);
    if (!locationId) {
      continue;
    }
    for (const timeseries of location?.sensors ?? []) {
      const timeseriesRef = timeseries?.id;
      const paramName = timeseries?.parameter?.name;
      if (!timeseriesRef || !paramName) {
        continue;
      }
      const name = String(paramName).trim();
      if (!name) {
        continue;
      }
      const parameter: ParameterMeta = {
        name,
        displayName: timeseries?.parameter?.displayName
          ? String(timeseries.parameter.displayName)
          : null,
        units: timeseries?.parameter?.units
          ? String(timeseries.parameter.units)
          : null,
      };
      timeseriesRefs.set(String(timeseriesRef), { locationId, parameter });
    }
  }
  return timeseriesRefs;
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204 });
  }
  rateLimitState.remaining = null;
  rateLimitState.stop = false;
  rateLimitState.stopReason = null;
  rateLimitState.limit = null;
  rateLimitState.firstRemaining = null;
  rateLimitState.reset = null;
  rateLimitState.resetAt = null;
  const authResponse = requireCronSecret(req);
  if (authResponse) {
    return authResponse;
  }
  if (!OPENAQ_API_KEY) {
    return jsonResponse({ error: "OPENAQ_API_KEY is required." }, 500);
  }

  let payload: PollRequest = {};
  if (req.method === "POST") {
    try {
      payload = await req.json();
    } catch (_err) {
      payload = {};
    }
  }

  const connectorCode = payload.connector_code ?? OPENAQ_CONNECTOR_CODE;
  const hasRequestedRefs = Array.isArray(payload.station_refs) &&
    payload.station_refs.length > 0;
  let stationRefs = Array.isArray(payload.station_refs)
    ? payload.station_refs.map((ref) => String(ref))
    : [];
  let stationIdByRef: Record<string, number> = {};
  let stationRefById: Record<number, string> = {};
  let selectedStations: Array<
    { station_ref: string; station_id: number | null }
  > = [];
  const stationsRequested = hasRequestedRefs ? stationRefs.length : 0;
  const windowHours = Number(payload.window_hours ?? DEFAULT_WINDOW_HOURS);
  const dryRun = payload.dry_run ?? false;
  const requestedBatchSize = Number(payload.batch_size);
  const maxRequestsPerRun = positiveInt(
    Number.isFinite(requestedBatchSize)
      ? requestedBatchSize
      : OPENAQ_MAX_REQUESTS_PER_RUN,
    DEFAULT_MAX_REQUESTS_PER_RUN,
  );
  const staleLimitConfigured = positiveInt(
    OPENAQ_STALE_LIMIT,
    DEFAULT_STALE_LIMIT,
  );
  const staleLimit = Math.min(
    staleLimitConfigured,
    DEFAULT_STALE_LIMIT,
    Math.max(0, maxRequestsPerRun),
  );
  const tier1RetrySeconds = positiveInt(
    Number(payload.tier1_retry_seconds ?? OPENAQ_TIER1_RETRY_SECONDS),
    DEFAULT_TIER1_RETRY_SECONDS,
  );
  const tieredLimit = Math.max(0, maxRequestsPerRun - staleLimit);
  const gapReserveMin = nonNegativeInt(
    OPENAQ_GAP_REQUESTS_REMAINING_MIN,
    DEFAULT_GAP_REQUESTS_REMAINING_MIN,
  );
  const minNonGapStations = positiveInt(
    OPENAQ_MIN_NON_GAP_STATIONS,
    DEFAULT_MIN_NON_GAP_STATIONS,
  );
  const minGapStations = positiveInt(
    OPENAQ_MIN_GAP_STATIONS,
    DEFAULT_MIN_GAP_STATIONS,
  );
  requestBudgetState.maxPerRun = maxRequestsPerRun;
  requestBudgetState.total = 0;
  requestBudgetState.gapReserveMin = gapReserveMin;
  requestBudgetState.gapPlannedRequests = 0;
  requestBudgetState.gapExecutedRequests = 0;
  requestBudgetState.gapSkippedBudgetRequests = 0;
  requestBudgetState.gapZeroYieldTimeseries = 0;
  const runStartedAt = Date.now();
  const maxRuntimeSeconds = Number.isFinite(OPENAQ_MAX_RUNTIME_SECONDS)
    ? Math.max(30, OPENAQ_MAX_RUNTIME_SECONDS)
    : DEFAULT_MAX_RUNTIME_SECONDS;
  const runtimeDeadline = runStartedAt + maxRuntimeSeconds * 1000;
  const runtimeDeadlineReached = () => Date.now() >= runtimeDeadline;
  const shouldStop = () => runtimeDeadlineReached() || rateLimitState.stop;
  let timeBudgetHit = false;
  const logLines: string[] = [];
  const runWarnings: RunWarningEntry[] = [];
  errorLogLines = [];
  const logLine = (
    level: string,
    message: string,
    context?: Record<string, unknown>,
  ) => {
    const stamp = new Date().toISOString();
    const ctx = context ? ` ${JSON.stringify(context)}` : "";
    logLines.push(`[${stamp}] ${level} ${message}${ctx}`);
  };
  const logTimeseriesRefMapping = (
    refs: string[],
    mapping: Record<string, number>,
    extra?: Record<string, unknown>,
  ) => {
    if (!refs.length) {
      return;
    }
    const uniqueRefs = Array.from(new Set(refs));
    const missingSample: string[] = [];
    let missingCount = 0;
    for (const ref of uniqueRefs) {
      if (mapping[ref] === undefined) {
        missingCount += 1;
        if (missingSample.length < 10) {
          missingSample.push(ref);
        }
      }
    }
    logLine("INFO", "OpenAQ timeseries ref mapping", {
      timeseries_refs_total: uniqueRefs.length,
      timeseries_ids_mapped: Object.keys(mapping).length,
      timeseries_refs_missing: missingCount,
      timeseries_refs_missing_sample: missingSample,
      ...extra,
    });
  };
  const summarizeMissingTimeseriesRefs = (
    refs: string[],
    mapping: Record<string, number>,
    stationMapping?: Map<string, number>,
    stationRefsById?: Record<number, string>,
    observedStationMapping?: Map<string, number>,
  ) => {
    const uniqueRefs = Array.from(new Set(refs));
    const missingSample: string[] = [];
    const missingDetails: Array<
      { timeseries_ref: string; station_id?: number; station_ref?: string }
    > = [];
    let missingCount = 0;
    for (const ref of uniqueRefs) {
      if (mapping[ref] === undefined) {
        missingCount += 1;
        if (missingSample.length < 10) {
          missingSample.push(ref);
        }
        if (
          (stationMapping || observedStationMapping) &&
          missingDetails.length < 10
        ) {
          const stationId = stationMapping?.get(ref) ??
            observedStationMapping?.get(ref);
          missingDetails.push(
            stationId
              ? {
                timeseries_ref: ref,
                station_id: stationId,
                station_ref: stationRefsById?.[stationId],
              }
              : { timeseries_ref: ref },
          );
        }
      }
    }
    return {
      missingCount,
      missingSample,
      missingDetails,
      total: uniqueRefs.length,
    };
  };
  const logTimeseriesStationMapping = (
    refMapping: Record<string, number>,
    stationMapping: Record<number, number>,
    extra?: Record<string, unknown>,
  ) => {
    const entries = Object.entries(refMapping);
    if (!entries.length) {
      return;
    }
    const missingSample: Array<
      { timeseries_ref: string; timeseries_id: number }
    > = [];
    let missingCount = 0;
    for (const [ref, id] of entries) {
      if (stationMapping[id] === undefined) {
        missingCount += 1;
        if (missingSample.length < 10) {
          missingSample.push({ timeseries_ref: ref, timeseries_id: id });
        }
      }
    }
    logLine("INFO", "OpenAQ timeseries station mapping", {
      timeseries_ids_total: entries.length,
      station_ids_mapped: Object.keys(stationMapping).length,
      station_ids_missing: missingCount,
      station_ids_missing_sample: missingSample,
      ...extra,
    });
  };
  const populateStationIdByTimeseriesIdFromRefs = (
    refMapping: Record<string, number>,
    stationMapping: Map<string, number>,
    target: Record<number, number>,
  ) => {
    for (const [timeseriesRef, timeseriesId] of Object.entries(refMapping)) {
      const stationId = stationMapping.get(timeseriesRef);
      if (stationId) {
        target[Number(timeseriesId)] = stationId;
      }
    }
  };
  const dropboxConfig = loadDropboxConfig();
  const dropboxDiagnostics = buildDropboxDiagnostics();
  const rawRecorder = dropboxConfig ? createRawRecorder() : null;
  logLine("INFO", "OpenAQ ingest started", {
    connector_code: connectorCode,
    window_hours: windowHours,
    dry_run: dryRun,
    station_refs: stationRefs.length ? stationRefs.length : 0,
    max_requests_per_run: maxRequestsPerRun,
    tiered_limit: tieredLimit,
    stale_limit: staleLimit,
    tier1_retry_seconds: tier1RetrySeconds,
    min_gap_stations: minGapStations,
    min_non_gap_stations: minNonGapStations,
    gap_requests_remaining_min: gapReserveMin,
    max_runtime_seconds: maxRuntimeSeconds,
  });

  const connector = await loadConnector(connectorCode);
  if (!connector) {
    return jsonResponse({ error: "Connector not found." }, 404);
  }

  let bbox: string;
  try {
    bbox = parseBbox(OPENAQ_BBOX);
  } catch (err) {
    logLine("ERROR", "Invalid OPENAQ_BBOX value", {
      error: err instanceof Error ? err.message : String(err),
    });
    return jsonResponse({ error: "Invalid OPENAQ_BBOX configuration." }, 500);
  }
  rawRecorder?.recordEvent("context", {
    connector_code: connectorCode,
    bbox,
    window_hours: windowHours,
    dry_run: dryRun,
    station_refs: stationRefs,
  });

  if (!stationRefs.length) {
    try {
      selectedStations = await loadOpenaqStationRefs(
        tieredLimit,
        staleLimit,
        tier1RetrySeconds,
      );
      stationRefs = selectedStations.map((row) => row.station_ref);
      for (const row of selectedStations) {
        if (row.station_id !== null && Number.isFinite(row.station_id)) {
          stationIdByRef[row.station_ref] = row.station_id;
        }
      }
    } catch (err) {
      await logError({
        severity: "error",
        message: "OpenAQ station selection failed",
        connector_id: connector.id,
        context: { error: String(err) },
      });
      return jsonResponse({ error: "OpenAQ station selection failed." }, 502);
    }
    rawRecorder?.recordEvent("selection", {
      tiered_limit: tieredLimit,
      stale_limit: staleLimit,
      tier1_retry_seconds: tier1RetrySeconds,
      station_refs: stationRefs,
    });
    if (!stationRefs.length) {
      logLine("INFO", "No OpenAQ station refs selected", {
        tiered_limit: tieredLimit,
        stale_limit: staleLimit,
      });
      return jsonResponse({ status: "no_station_refs_selected" }, 200);
    }
  }
  const stationsSelected = stationRefs.length;

  const locationsFetched = OPENAQ_INGEST_STATION_FETCH;
  let locations: OpenAQLocation[] = [];
  if (locationsFetched) {
    try {
      locations = await listLocations(bbox, rawRecorder);
    } catch (err) {
      if (isSharedBudgetLimitReason(rateLimitState.stopReason)) {
        const warning = buildSharedBudgetWarning(
          { phase: "location_fetch", error: String(err) },
          connector.id,
        );
        runWarnings.push(warning);
        logLine("WARN", "OpenAQ location fetch failed", warning);
        return jsonResponse({
          connector_code: connectorCode,
          stations_requested: stationsRequested,
          stations_selected: stationRefs.length,
          stations_polled: 0,
          stations_updated: 0,
          timeseries_updated: 0,
          observations_upserted: 0,
          observations_rows_input: 0,
          observations_rows_prepared: 0,
          observations_rows_deduped_prewrite: 0,
          observs_rows_prepared: 0,
          observs_rows_deduped_prewrite: 0,
          series_polled: 0,
          window_hours: windowHours,
          station_fetch_enabled: locationsFetched,
          partial: true,
          stopped_reason: rateLimitState.stopReason,
          rate_limit_stop: true,
          rate_limit_stop_reason: rateLimitState.stopReason,
          requests_total: requestBudgetState.total,
          max_requests_per_run: requestBudgetState.maxPerRun,
          dry_run: dryRun,
          warnings: runWarnings,
          ...sharedBudgetResponseFields(),
        });
      }
      await logError({
        severity: "error",
        message: "OpenAQ location fetch failed",
        connector_id: connector.id,
        context: { error: String(err) },
      });
      return jsonResponse({ error: "OpenAQ location fetch failed." }, 502);
    }
    logLine("INFO", "Fetched OpenAQ locations", { count: locations.length });
  }

  if (locationsFetched && stationRefs.length) {
    locations = locations.filter((loc) => {
      const locationId = resolveLocationId(loc);
      return locationId ? stationRefs.includes(locationId) : false;
    });
  }

  const connectorId = String(connector.id);
  const overwriteStationName = connector.overwrite_station_name ?? false;
  const stationsUpdated = locationsFetched && !dryRun
    ? await upsertStations(
      locations,
      connectorId,
      OPENAQ_SERVICE_REF,
      overwriteStationName,
    )
    : locationsFetched && dryRun
    ? locations.length
    : 0;

  const stationRefsForIds = locationsFetched
    ? locations
      .map((location) => resolveLocationId(location))
      .filter((id): id is string => Boolean(id))
    : stationRefs;
  const missingRefsForIds = stationRefsForIds.filter(
    (ref) => stationIdByRef[ref] === undefined,
  );
  if (missingRefsForIds.length) {
    const fetchedIds = await fetchStationIds(
      connectorId,
      OPENAQ_SERVICE_REF,
      missingRefsForIds,
    );
    stationIdByRef = { ...stationIdByRef, ...fetchedIds };
  }
  stationRefById = {};
  for (const [stationRef, stationId] of Object.entries(stationIdByRef)) {
    const idValue = Number(stationId);
    if (Number.isFinite(idValue)) {
      stationRefById[idValue] = stationRef;
    }
  }
  {
    const missingStationRefs = stationRefsForIds.filter(
      (ref) => stationIdByRef[ref] === undefined,
    );
    logLine("INFO", "OpenAQ station ref mapping", {
      station_refs_total: stationRefsForIds.length,
      station_ids_mapped: Object.keys(stationIdByRef).length,
      station_refs_missing: missingStationRefs.length,
      station_refs_missing_sample: missingStationRefs.slice(0, 10),
      locations_fetched: locationsFetched,
    });
  }

  const stationIds = Object.values(stationIdByRef).map((id) => Number(id));
  let checkpointByStationId: Record<number, OpenAQStationCheckpoint> = {};
  try {
    checkpointByStationId = await fetchOpenaqStationCheckpoints(stationIds);
    logLine("INFO", "OpenAQ station checkpoints fetched", {
      station_ids: stationIds.length,
      checkpoints: Object.keys(checkpointByStationId).length,
    });
  } catch (err) {
    await logError({
      severity: "warn",
      message: "OpenAQ checkpoints fetch failed",
      connector_id: connector.id,
      context: { error: String(err) },
    });
    checkpointByStationId = {};
  }

  const nowMs = Date.now();
  const debugStationId = OPENAQ_DEBUG_STATION_ID;
  if (stationIds.includes(debugStationId)) {
    logLine("INFO", "OpenAQ debug station present", {
      station_id: debugStationId,
    });
  }

  const parameters = locationsFetched ? collectParameters(locations) : {};
  const timeseriesRefMap = locationsFetched
    ? collectTimeseriesRefs(locations)
    : new Map();
  const phenomenonIds = locationsFetched
    ? await upsertPhenomena(connectorId, parameters)
    : {};

  const timeseriesRefsByStationId = new Map<number, string[]>();
  const stationIdByTimeseriesRef = new Map<string, number>();
  if (locationsFetched) {
    for (const [timeseriesRef, meta] of timeseriesRefMap.entries()) {
      const stationId = Number(stationIdByRef[meta.locationId]);
      if (!Number.isFinite(stationId)) {
        continue;
      }
      stationIdByTimeseriesRef.set(timeseriesRef, stationId);
      const existing = timeseriesRefsByStationId.get(stationId);
      if (existing) {
        existing.push(timeseriesRef);
      } else {
        timeseriesRefsByStationId.set(stationId, [timeseriesRef]);
      }
    }
    logLine("INFO", "OpenAQ timeseries mapping", {
      timeseries_total: timeseriesRefMap.size,
      station_ids_mapped: stationIdByTimeseriesRef.size,
      stations_with_timeseries: timeseriesRefsByStationId.size,
    });
  } else if (stationIds.length) {
    try {
      const refsByStation = await fetchOpenaqTimeseriesRefsByStationIds(
        connectorId,
        OPENAQ_SERVICE_REF,
        stationIds,
      );
      const perStationCounts: Record<string, number> = {};
      for (const [stationIdRaw, refs] of Object.entries(refsByStation)) {
        const stationId = Number(stationIdRaw);
        if (!Number.isFinite(stationId) || !refs.length) {
          continue;
        }
        const normalizedRefs = refs.map((ref) => String(ref));
        timeseriesRefsByStationId.set(stationId, normalizedRefs);
        for (const ref of normalizedRefs) {
          stationIdByTimeseriesRef.set(ref, stationId);
        }
        perStationCounts[stationIdRaw] = refs.length;
      }
      logLine("INFO", "OpenAQ timeseries refs loaded", {
        station_ids: stationIds.length,
        stations_with_timeseries: timeseriesRefsByStationId.size,
        timeseries_per_station_sample: Object.entries(perStationCounts)
          .slice(0, 10)
          .map(([station_id, count]) => ({
            station_id: Number(station_id),
            count,
          })),
      });
    } catch (err) {
      await logError({
        severity: "warn",
        message: "OpenAQ timeseries refs fetch failed",
        connector_id: connector.id,
        context: { error: String(err) },
      });
    }
  }

  const timeseriesRows: Array<Record<string, unknown>> = [];
  const timeseriesRefs: string[] = [];
  let timeseriesIdByRef: Record<string, number> = {};
  let stationIdByTimeseriesId: Record<number, number> = {};
  if (locationsFetched) {
    for (const [timeseriesRef, meta] of timeseriesRefMap.entries()) {
      const stationId = stationIdByRef[meta.locationId];
      if (!stationId) {
        continue;
      }
      const phenomenonId = phenomenonIds[meta.parameter.name];
      if (!phenomenonId) {
        continue;
      }
      const label = `${meta.locationId} ${
        meta.parameter.displayName ?? meta.parameter.name
      }`;
      timeseriesRows.push({
        timeseries_ref: timeseriesRef,
        label,
        uom: meta.parameter.units ?? null,
        station_id: stationId,
        connector_id: connectorId,
        service_ref: OPENAQ_SERVICE_REF,
        phenomenon_id: phenomenonId,
      });
      timeseriesRefs.push(timeseriesRef);
    }
    if (!dryRun) {
      await upsertTimeseries(timeseriesRows);
    }
    if (timeseriesRefs.length) {
      timeseriesIdByRef = await fetchTimeseriesIds(
        connectorId,
        OPENAQ_SERVICE_REF,
        timeseriesRefs,
      );
    }
    populateStationIdByTimeseriesIdFromRefs(
      timeseriesIdByRef,
      stationIdByTimeseriesRef,
      stationIdByTimeseriesId,
    );
    logTimeseriesRefMapping(timeseriesRefs, timeseriesIdByRef, {
      locations_fetched: locationsFetched,
      dry_run: dryRun,
    });
  }

  let timeseriesCheckpointById: Record<
    number,
    OpenAQTimeseriesCheckpointSnapshot
  > = {};
  let timeseriesCheckpointsByStationId: Record<
    number,
    OpenAQTimeseriesCheckpointSnapshot[]
  > = {};
  if (stationIds.length) {
    try {
      const timeseriesCheckpointMaps =
        await fetchOpenaqTimeseriesCheckpointSnapshots(
          stationIds,
        );
      timeseriesCheckpointById = timeseriesCheckpointMaps.byTimeseriesId;
      timeseriesCheckpointsByStationId = timeseriesCheckpointMaps.byStationId;
      logLine("INFO", "OpenAQ timeseries checkpoints fetched", {
        station_ids: stationIds.length,
        checkpoints: Object.keys(timeseriesCheckpointById).length,
      });
    } catch (err) {
      await logError({
        severity: "warn",
        message: "OpenAQ timeseries checkpoints fetch failed",
        connector_id: connector.id,
        context: { error: String(err) },
      });
      timeseriesCheckpointById = {};
      timeseriesCheckpointsByStationId = {};
    }
  }

  const gapStationIds = new Set<number>();
  const gapMinAgeMs = 2 * 60 * 60 * 1000;
  const gapMaxAgeMs = 24 * 60 * 60 * 1000;
  for (const stationId of stationIds) {
    const timeseriesRefs = timeseriesRefsByStationId.get(stationId) ?? [];
    if (!timeseriesRefs.length) {
      continue;
    }
    const stationCheckpoints = timeseriesCheckpointsByStationId[stationId] ??
      [];
    let checkpointsSeen = 0;
    let gapFlagged = false;
    for (const tsCheckpoint of stationCheckpoints) {
      const lastObservedAt = tsCheckpoint?.last_observed_at ?? null;
      if (!lastObservedAt) {
        continue;
      }
      checkpointsSeen += 1;
      const lastObservedMs = Date.parse(lastObservedAt);
      if (!Number.isFinite(lastObservedMs)) {
        continue;
      }
      const ageMs = nowMs - lastObservedMs;
      if (ageMs >= gapMinAgeMs && ageMs < gapMaxAgeMs) {
        gapFlagged = true;
        break;
      }
    }
    if (gapFlagged) {
      gapStationIds.add(stationId);
    }
    if (stationId === debugStationId) {
      logLine("INFO", "OpenAQ gap precheck debug", {
        station_id: stationId,
        timeseries_refs_count: timeseriesRefs.length,
        timeseries_checkpoints_total: stationCheckpoints.length,
        timeseries_checkpoints_seen: checkpointsSeen,
        gap_flagged: gapFlagged,
      });
    }
  }
  if (stationIds.includes(debugStationId)) {
    logLine("INFO", "OpenAQ gap precheck summary", {
      station_id: debugStationId,
      gap_flagged: gapStationIds.has(debugStationId),
    });
  }
  logLine("INFO", "OpenAQ gap precheck", {
    station_ids: stationIds.length,
    gap_station_ids: gapStationIds.size,
  });

  const latestByTimeseries = new Map<
    string,
    { observed_at: string; value: number | null }
  >();
  const observationsByTimeseries = new Map<
    string,
    Map<string, number | null>
  >();
  const stationIdByObservedTimeseriesRef = new Map<string, number>();
  const latestObservedByStationId = new Map<number, string>();
  const gapContigEndByTimeseriesRef = new Map<string, string>();
  const gapHasRecentGapByTimeseriesRef = new Map<string, boolean>();
  const seenStationIds = new Set<number>();
  const polledStationIds = new Set<number>();
  const windowMs = Number.isFinite(windowHours) && windowHours > 0
    ? windowHours * 60 * 60 * 1000
    : null;
  const recentGapThresholdMs = nowMs - 24 * 60 * 60 * 1000;

  const locationIds = locationsFetched
    ? locations
      .map((location) => resolveLocationId(location))
      .filter((id): id is string => Boolean(id))
    : stationRefs;

  const nonGapLocationCount = locationIds.reduce((count, locationId) => {
    const stationId = stationIdByRef[locationId];
    if (!stationId || !gapStationIds.has(Number(stationId))) {
      return count + 1;
    }
    return count;
  }, 0);
  if (
    gapStationIds.size < minGapStations &&
    nonGapLocationCount < minNonGapStations
  ) {
    const skippedReason = gapStationIds.size === 0
      ? "insufficient_non_gap_stations_no_gap"
      : "insufficient_gap_and_non_gap_stations";
    logLine("INFO", "OpenAQ run skipped by non-gap threshold", {
      skipped_reason: skippedReason,
      stations_selected: stationsSelected,
      gap_stations_total: gapStationIds.size,
      min_gap_stations: minGapStations,
      non_gap_stations_selected: nonGapLocationCount,
      min_non_gap_stations: minNonGapStations,
      stations_polled: 0,
    });
    return jsonResponse({
      run_status: "skipped",
      run_message: "skipped",
      skipped_reason: skippedReason,
      connector_code: connectorCode,
      stations_requested: stationsRequested,
      stations_selected: stationsSelected,
      gap_stations_total: gapStationIds.size,
      gap_stations_polled: 0,
      min_gap_stations: minGapStations,
      non_gap_stations_selected: nonGapLocationCount,
      min_non_gap_stations: minNonGapStations,
      stations_polled: 0,
      stations_updated: 0,
      timeseries_updated: 0,
      observations_upserted: 0,
      observations_rows_input: 0,
      observations_rows_prepared: 0,
      observations_rows_deduped_prewrite: 0,
      observs_rows_prepared: 0,
      observs_rows_deduped_prewrite: 0,
      series_polled: 0,
      window_hours: windowHours,
      last_observed_at: null,
      station_fetch_enabled: locationsFetched,
      partial: false,
      stopped_reason: skippedReason,
      rate_limit_remaining: rateLimitState.remaining,
      rate_limit_limit: rateLimitState.limit,
      rate_limit_reset: rateLimitState.reset,
      rate_limit_reset_at: rateLimitState.resetAt,
      rate_limit_remaining_first: rateLimitState.firstRemaining,
      rate_limit_stop: false,
      rate_limit_stop_reason: null,
      requests_total: requestBudgetState.total,
      max_requests_per_run: requestBudgetState.maxPerRun,
      tiered_limit: tieredLimit,
      stale_limit: staleLimit,
      tier1_retry_seconds: tier1RetrySeconds,
      gap_requests_remaining_min: requestBudgetState.gapReserveMin,
      gap_requests_planned: 0,
      gap_requests_executed: 0,
      gap_requests_skipped_budget: 0,
      gap_zero_yield_timeseries: 0,
      dry_run: dryRun,
      warnings: runWarnings,
      ...sharedBudgetResponseFields(),
    });
  }
  const gapStationPlan = Array.from(gapStationIds).map((stationId) => {
    const stationCheckpoint = checkpointByStationId[stationId];
    return {
      stationId,
      estimatedRequests:
        (timeseriesRefsByStationId.get(stationId) ?? []).length,
      dueAt: stationCheckpoint?.next_due_at ?? null,
      lastPolledAt: stationCheckpoint?.last_polled_at ?? null,
    };
  }).sort((a, b) => {
    const aLastPolled = a.lastPolledAt
      ? Date.parse(a.lastPolledAt)
      : Number.NEGATIVE_INFINITY;
    const bLastPolled = b.lastPolledAt
      ? Date.parse(b.lastPolledAt)
      : Number.NEGATIVE_INFINITY;
    if (aLastPolled !== bLastPolled) {
      return aLastPolled - bLastPolled;
    }
    const aDue = a.dueAt ? Date.parse(a.dueAt) : Number.POSITIVE_INFINITY;
    const bDue = b.dueAt ? Date.parse(b.dueAt) : Number.POSITIVE_INFINITY;
    return aDue - bDue;
  });
  requestBudgetState.gapPlannedRequests = gapStationPlan.reduce(
    (sum, row) => sum + row.estimatedRequests,
    0,
  );

  const requestsRemainingBeforePolling = Math.max(
    0,
    requestBudgetState.maxPerRun - requestBudgetState.total,
  );
  let gapBudgetRemaining = Math.max(
    0,
    requestsRemainingBeforePolling - nonGapLocationCount -
      requestBudgetState.gapReserveMin,
  );
  const scheduledGapStationIds = new Set<number>();
  let scheduledGapEstimatedRequests = 0;
  for (const entry of gapStationPlan) {
    if (entry.estimatedRequests <= gapBudgetRemaining) {
      scheduledGapStationIds.add(entry.stationId);
      scheduledGapEstimatedRequests += entry.estimatedRequests;
      gapBudgetRemaining -= entry.estimatedRequests;
    } else {
      requestBudgetState.gapSkippedBudgetRequests += entry.estimatedRequests;
    }
  }
  logLine("INFO", "OpenAQ gap budget plan", {
    max_requests_per_run: requestBudgetState.maxPerRun,
    requests_remaining_before_polling: requestsRemainingBeforePolling,
    min_gap_stations: minGapStations,
    min_non_gap_stations: minNonGapStations,
    reserve_for_non_gap_requests: nonGapLocationCount,
    gap_requests_remaining_min: requestBudgetState.gapReserveMin,
    gap_requests_planned: requestBudgetState.gapPlannedRequests,
    gap_requests_scheduled_estimate: scheduledGapEstimatedRequests,
    gap_requests_skipped_estimate: requestBudgetState.gapSkippedBudgetRequests,
    gap_stations_total: gapStationIds.size,
    gap_stations_scheduled: scheduledGapStationIds.size,
  });

  await runPool(locationIds, OPENAQ_CONCURRENCY, async (locationId) => {
    if (shouldStop()) {
      timeBudgetHit = runtimeDeadlineReached();
      return;
    }
    const stationIdValue = stationIdByRef[locationId];
    const stationId = stationIdValue ? Number(stationIdValue) : null;
    if (stationId !== null && Number.isFinite(stationId)) {
      seenStationIds.add(stationId);
    }
    if (stationId === debugStationId) {
      logLine("INFO", "OpenAQ debug station selected", {
        station_id: stationId,
        gap_flagged: gapStationIds.has(stationId),
        station_checkpoint: checkpointByStationId[stationId] ?? null,
      });
    }

    if (
      stationId !== null && gapStationIds.has(stationId) &&
      !scheduledGapStationIds.has(stationId)
    ) {
      if (stationId === debugStationId) {
        logLine("INFO", "OpenAQ debug station gap skipped by budget", {
          station_id: stationId,
        });
      }
      return;
    }

    if (stationId !== null && gapStationIds.has(stationId)) {
      const stationCheckpoint = checkpointByStationId[stationId];
      const timeseriesRefs = timeseriesRefsByStationId.get(stationId) ?? [];
      if (stationId === debugStationId) {
        logLine("INFO", "OpenAQ debug station gap path", {
          station_id: stationId,
          timeseries_refs_count: timeseriesRefs.length,
          timeseries_refs_sample: timeseriesRefs.slice(0, 10),
        });
      }
      for (const timeseriesRef of timeseriesRefs) {
        const requestsRemaining = requestBudgetState.maxPerRun -
          requestBudgetState.total;
        if (requestsRemaining <= requestBudgetState.gapReserveMin) {
          requestBudgetState.gapSkippedBudgetRequests += 1;
          if (stationId === debugStationId) {
            logLine("INFO", "OpenAQ debug timeseries skipped (gap reserve)", {
              station_id: stationId,
              timeseries_ref: timeseriesRef,
              requests_remaining: requestsRemaining,
              gap_requests_remaining_min: requestBudgetState.gapReserveMin,
            });
          }
          break;
        }
        if (shouldStop()) {
          timeBudgetHit = runtimeDeadlineReached();
          return;
        }
        const timeseriesId = timeseriesIdByRef[timeseriesRef];
        const tsCheckpoint = timeseriesId
          ? timeseriesCheckpointById[timeseriesId]
          : null;
        if (tsCheckpoint?.last_observed_at) {
          const tsObservedMs = Date.parse(tsCheckpoint.last_observed_at);
          if (
            Number.isFinite(tsObservedMs) &&
            nowMs - tsObservedMs < 60 * 60 * 1000
          ) {
            if (stationId === debugStationId) {
              logLine("INFO", "OpenAQ debug timeseries skipped (recent)", {
                station_id: stationId,
                timeseries_ref: timeseriesRef,
                timeseries_id: timeseriesId ?? null,
                last_observed_at: tsCheckpoint.last_observed_at,
              });
            }
            continue;
          }
        }
        if (typeof stationId === "number" && Number.isFinite(stationId)) {
          polledStationIds.add(stationId);
        }
        const baseObservedAt = tsCheckpoint?.last_observed_at ??
          stationCheckpoint?.last_observed_at ??
          null;
        const datetimeFrom = baseObservedAt ??
          (windowMs ? new Date(nowMs - windowMs).toISOString() : null);
        let datetimeTo = new Date(nowMs).toISOString();
        if (datetimeFrom && windowMs) {
          const fromMs = Date.parse(datetimeFrom);
          if (Number.isFinite(fromMs)) {
            const cappedMs = Math.min(nowMs, fromMs + windowMs);
            datetimeTo = new Date(cappedMs).toISOString();
          }
        }
        if (stationId === debugStationId) {
          logLine("INFO", "OpenAQ debug timeseries fetch", {
            station_id: stationId,
            timeseries_ref: timeseriesRef,
            timeseries_id: timeseriesId ?? null,
            datetime_from: datetimeFrom,
            datetime_to: datetimeTo,
            ts_checkpoint: tsCheckpoint ?? null,
          });
        }
        let hourly: OpenAQHourlyRecord[] = [];
        const requestsBeforeGapQuery = requestBudgetState.total;
        try {
          const hourlyResult = await listHourlyMeasurements(
            timeseriesRef,
            datetimeFrom,
            datetimeTo,
            rawRecorder,
          );
          hourly = hourlyResult.records;
          if (hourlyResult.pages > 1 || stationId === debugStationId) {
            logLine("INFO", "OpenAQ hourly paging info", {
              station_id: stationId,
              timeseries_ref: timeseriesRef,
              datetime_from: datetimeFrom,
              datetime_to: datetimeTo,
              pages: hourlyResult.pages,
              page_limit: hourlyResult.limit,
              records_count: hourly.length,
            });
          }
        } catch (err) {
          requestBudgetState.gapExecutedRequests += Math.max(
            0,
            requestBudgetState.total - requestsBeforeGapQuery,
          );
          const warningContext = {
            station_id: stationId,
            timeseries_ref: timeseriesRef,
            error: String(err),
          };
          if (shouldTreatAsSharedBudgetWarning(err)) {
            const warning = buildSharedBudgetWarning(
              warningContext,
              connector.id,
            );
            runWarnings.push(warning);
            logLine("WARN", "OpenAQ hourly measurements fetch failed", warning);
          } else {
            await logError({
              severity: "warn",
              message: "OpenAQ hourly measurements fetch failed",
              connector_id: connector.id,
              context: warningContext,
            });
          }
          if (rateLimitState.stop) {
            return;
          }
          continue;
        }
        requestBudgetState.gapExecutedRequests += Math.max(
          0,
          requestBudgetState.total - requestsBeforeGapQuery,
        );
        if (stationId === debugStationId) {
          logLine("INFO", "OpenAQ debug timeseries fetched", {
            station_id: stationId,
            timeseries_ref: timeseriesRef,
            hourly_count: hourly.length,
          });
        }
        if (!hourly.length) {
          requestBudgetState.gapZeroYieldTimeseries += 1;
        }
        if (datetimeFrom && datetimeTo) {
          const start = new Date(datetimeFrom);
          const end = new Date(datetimeTo);
          if (
            Number.isFinite(start.getTime()) &&
            Number.isFinite(end.getTime()) && start < end
          ) {
            start.setUTCMinutes(0, 0, 0);
            end.setUTCMinutes(0, 0, 0);
            const expected: string[] = [];
            const expectedStart = new Date(start);
            expectedStart.setUTCHours(expectedStart.getUTCHours() + 1);
            const endExclusive = new Date(end);
            endExclusive.setUTCHours(endExclusive.getUTCHours() + 1);
            const cursor = new Date(expectedStart);
            while (cursor < endExclusive) {
              expected.push(cursor.toISOString());
              cursor.setUTCHours(cursor.getUTCHours() + 1);
            }
            const returned = new Set<string>();
            for (const record of hourly) {
              const observedAt = resolveHourlyObservedAt(record, nowMs);
              if (!observedAt) {
                continue;
              }
              const observed = new Date(observedAt);
              if (!Number.isFinite(observed.getTime())) {
                continue;
              }
              observed.setUTCMinutes(0, 0, 0);
              returned.add(observed.toISOString());
            }
            const missing = expected.filter((hour) => !returned.has(hour));
            if (expected.length > 0) {
              let contigEnd: string | null = null;
              if (missing.length > 0) {
                const firstMissing = missing[0];
                const firstIndex = expected.indexOf(firstMissing);
                if (firstIndex > 0) {
                  contigEnd = expected[firstIndex - 1];
                }
              } else {
                contigEnd = expected[expected.length - 1];
              }
              if (contigEnd) {
                gapContigEndByTimeseriesRef.set(timeseriesRef, contigEnd);
              }
              if (missing.length > 0) {
                const hasRecentGap = missing.some((hour) => {
                  const hourMs = Date.parse(hour);
                  return Number.isFinite(hourMs) &&
                    hourMs >= recentGapThresholdMs;
                });
                if (hasRecentGap) {
                  gapHasRecentGapByTimeseriesRef.set(timeseriesRef, true);
                }
              }
            }
            if (missing.length > 0) {
              logLine("INFO", "OpenAQ hourly gap detected", {
                station_id: stationId,
                timeseries_ref: timeseriesRef,
                datetime_from: datetimeFrom,
                datetime_to: datetimeTo,
                expected_hours: expected.length,
                returned_hours: returned.size,
                missing_hours: missing.slice(0, 12),
                missing_hours_count: missing.length,
              });
            }
          }
        }
        for (const record of hourly) {
          const observedAt = resolveHourlyObservedAt(record, nowMs);
          if (!observedAt) {
            continue;
          }
          recordObservation(
            observationsByTimeseries,
            latestByTimeseries,
            latestObservedByStationId,
            String(timeseriesRef),
            observedAt,
            record?.summary?.avg ??
              record?.summary?.median ??
              record?.summary?.q50 ??
              record?.value ??
              null,
            stationId,
            nowMs,
            null,
            stationIdByObservedTimeseriesRef,
          );
        }
      }
      return;
    }
    if (stationId === debugStationId) {
      logLine("INFO", "OpenAQ debug station latest path", {
        station_id: stationId,
        gap_flagged: gapStationIds.has(stationId),
      });
    }

    let latest: OpenAQLatestRecord[] = [];
    try {
      if (stationId !== null && Number.isFinite(stationId)) {
        polledStationIds.add(stationId);
      }
      latest = await listLatestForLocation(locationId, rawRecorder);
    } catch (err) {
      const warningContext = {
        station_id: stationId,
        location_id: locationId,
        error: String(err),
      };
      if (shouldTreatAsSharedBudgetWarning(err)) {
        const warning = buildSharedBudgetWarning(warningContext, connector.id);
        runWarnings.push(warning);
        logLine("WARN", "OpenAQ latest fetch failed", warning);
      } else {
        await logError({
          severity: "warn",
          message: "OpenAQ latest fetch failed",
          connector_id: connector.id,
          context: warningContext,
        });
      }
      return;
    }
    for (const record of latest) {
      const timeseriesRef = resolveTimeseriesRefFromLatest(record);
      const observedAt = record?.datetime?.utc;
      if (!timeseriesRef || !observedAt) {
        continue;
      }
      recordObservation(
        observationsByTimeseries,
        latestByTimeseries,
        latestObservedByStationId,
        String(timeseriesRef),
        observedAt,
        record?.value ?? null,
        stationId,
        nowMs,
        windowMs,
        stationIdByObservedTimeseriesRef,
      );
    }
  }, () => {
    if (shouldStop()) {
      timeBudgetHit = runtimeDeadlineReached();
      return true;
    }
    return false;
  });

  if (!locationsFetched) {
    for (const timeseriesRef of observationsByTimeseries.keys()) {
      timeseriesRefs.push(timeseriesRef);
    }
    if (!dryRun && timeseriesRefs.length) {
      timeseriesIdByRef = await fetchTimeseriesIds(
        connectorId,
        OPENAQ_SERVICE_REF,
        timeseriesRefs,
      );
    }
    populateStationIdByTimeseriesIdFromRefs(
      timeseriesIdByRef,
      stationIdByTimeseriesRef,
      stationIdByTimeseriesId,
    );
    logTimeseriesRefMapping(timeseriesRefs, timeseriesIdByRef, {
      locations_fetched: locationsFetched,
      dry_run: dryRun,
    });
    if (!dryRun && timeseriesRefs.length) {
      const missingSummary = summarizeMissingTimeseriesRefs(
        timeseriesRefs,
        timeseriesIdByRef,
        stationIdByTimeseriesRef,
        stationRefById,
        stationIdByObservedTimeseriesRef,
      );
      if (missingSummary.missingCount > 0) {
        await logError({
          severity: "error",
          message: "OpenAQ timeseries refs missing from mapping",
          connector_id: connector.id,
          context: {
            timeseries_refs_total: missingSummary.total,
            timeseries_refs_missing: missingSummary.missingCount,
            timeseries_refs_missing_sample: missingSummary.missingSample,
            station_sample: missingSummary.missingDetails,
            observations_timeseries: observationsByTimeseries.size,
            stations_selected: stationsSelected,
            locations_fetched: locationsFetched,
          },
        });
      }
    }
  }
  if (!locationsFetched && Object.keys(timeseriesIdByRef).length) {
    try {
      stationIdByTimeseriesId = await fetchTimeseriesStationIds(
        Object.values(timeseriesIdByRef),
      );
      logTimeseriesStationMapping(timeseriesIdByRef, stationIdByTimeseriesId, {
        locations_fetched: locationsFetched,
      });
    } catch (err) {
      await logError({
        severity: "warn",
        message: "OpenAQ timeseries station lookup failed",
        connector_id: connector.id,
        context: { error: String(err) },
      });
      stationIdByTimeseriesId = {};
    }
  }

  logLine("INFO", "OpenAQ polling summary", {
    stations_selected: stationsSelected,
    stations_polled: polledStationIds.size,
    gap_stations_total: gapStationIds.size,
    gap_stations_polled: Array.from(polledStationIds).filter((stationId) =>
      gapStationIds.has(stationId)
    ).length,
    latest_timeseries: latestByTimeseries.size,
    observations_timeseries: observationsByTimeseries.size,
    timeseries_refs: timeseriesRefs.length,
    timeseries_ids: Object.keys(timeseriesIdByRef).length,
    timeseries_station_ids: Object.keys(stationIdByTimeseriesId).length,
  });

  let observationsUpserted = 0;
  let ingestDbObservationWriteStats =
    createEmptyIngestDbObservationWriteStats();
  let observationsRowsInput = 0;
  let observationsRowsPrepared = 0;
  let observationsRowsDedupedPrewrite = 0;
  let observsRowsPrepared = 0;
  let observsRowsDedupedPrewrite = 0;
  let observsWritten = 0;
  let observsReceiptsUpserted = 0;
  let observsEnqueued = 0;
  const seriesPolled = observationsByTimeseries.size;
  let lastObservedAt: string | null = null;
  let timeseriesLastUpdated = 0;
  const timeseriesErrors: string[] = [];

  if (!dryRun) {
    const rawObservationRows: Array<Record<string, unknown>> = [];
    for (
      const [timeseriesRef, observations] of observationsByTimeseries.entries()
    ) {
      const timeseriesId = timeseriesIdByRef[timeseriesRef];
      if (!timeseriesId) {
        continue;
      }
      for (const [observedAt, value] of observations.entries()) {
        rawObservationRows.push({
          connector_id: connectorId,
          timeseries_id: timeseriesId,
          observed_at: observedAt,
          value,
          status: null,
        });
      }
    }
    observationsRowsInput = rawObservationRows.length;
    const observationDedupe = dedupeExactObservationRows(rawObservationRows);
    const observationRows = observationDedupe.rows;
    observationsRowsPrepared = observationRows.length;
    observationsRowsDedupedPrewrite = observationDedupe.deduped;

    const observsRows: ObservsObservationRow[] = observationRows.map((row) => ({
      connector_id: Number(row.connector_id),
      timeseries_id: Number(row.timeseries_id),
      observed_at: String(row.observed_at),
      value: typeof row.value === "number" ? row.value : null,
      status: row.status == null ? null : String(row.status),
    }));
    observsRowsPrepared = observsRows.length;
    observsRowsDedupedPrewrite = observationsRowsDedupedPrewrite;

    await writeOpenAqIngestDbObservations({
      write: () =>
        upsertObservations(
          observationRows,
          {
            shouldStop: runtimeDeadlineReached,
            remainingRuntimeMs: () =>
              Math.max(0, runtimeDeadline - Date.now()),
          },
        ),
      aggregateStats: ingestDbObservationWriteStats,
      isWriteError: isIngestDbObservationWriteError,
      mergeStats: mergeIngestDbObservationWriteStats,
      onObservationsUpserted: (committedRows: number) => {
        observationsUpserted = committedRows;
      },
      onTerminalError: (error: {
        classification?: string;
        terminalReason?: string;
      }) => {
        logLine("ERROR", "OpenAQ IngestDB observation write failed", {
          connector_id: connector.id,
          observations_upserted: observationsUpserted,
          ingestdb_observation_write: ingestDbObservationWriteStats,
          cross_database_transaction: false,
          failure_classification: error.classification ??
            ingestDbObservationWriteStats.terminal_failure_classification,
          terminal_reason: error.terminalReason ??
            ingestDbObservationWriteStats.terminal_reason,
        });
      },
    });
    if (observsRows.length) {
      // The stores are deliberately independent: this ObsAQIDB operation is
      // not part of a cross-database transaction with IngestDB.
      try {
        const observsStats = await writeObservsWithOutbox(
          rpcRequest,
          observsRows,
          (message) => {
            logLine("WARN", "OpenAQ observs write warning", {
              connector_id: connector.id,
              message,
              rows: observsRows.length,
            });
            void logError({
              severity: "warn",
              message: "OpenAQ observs dual-write warning",
              connector_id: connector.id,
              context: {
                warning: message,
                rows: observsRows.length,
              },
            });
          },
        );
        observsWritten = observsStats.written;
        observsReceiptsUpserted = observsStats.receipts_upserted;
        observsEnqueued = observsStats.enqueued;
      } catch (error) {
        logLine("ERROR", "OpenAQ ObsAQIDB write failed after IngestDB commit", {
          connector_id: connector.id,
          observations_upserted: observationsUpserted,
          ingestdb_observation_write: ingestDbObservationWriteStats,
          cross_database_transaction: false,
          obsaqidb_write: { status: "failed", message: String(error) },
        });
        throw error;
      }
    }
    const timeseriesUpdates: Array<
      { id: number; last_value: number; last_value_at: string }
    > = [];
    for (const [timeseriesRef, latest] of latestByTimeseries.entries()) {
      const timeseriesId = timeseriesIdByRef[timeseriesRef];
      if (!timeseriesId) {
        continue;
      }
      if (typeof latest.value !== "number") {
        continue;
      }
      timeseriesUpdates.push({
        id: timeseriesId,
        last_value: latest.value,
        last_value_at: latest.observed_at,
      });
    }
    timeseriesLastUpdated = await updateTimeseriesLastValues(
      timeseriesUpdates,
      timeseriesErrors,
    );
  }

  for (const latest of latestByTimeseries.values()) {
    if (!lastObservedAt || latest.observed_at > lastObservedAt) {
      lastObservedAt = latest.observed_at;
    }
  }

  if (timeseriesErrors.length) {
    await logError({
      severity: "warn",
      message: "Timeseries last_value updates failed",
      connector_id: connector.id,
      context: { errors: timeseriesErrors.slice(0, 10) },
    });
  }

  if (!dryRun && polledStationIds.size) {
    const checkpointRows: Array<Record<string, unknown>> = [];
    const nowIso = new Date().toISOString();
    const nowMsForLag = Date.now();
    const stationObservedSample: Array<{
      station_id: number;
      min_observed_at: string | null;
      recent_gap_min_observed_at: string | null;
      station_last_observed_at: string | null;
      latest_observed_at: string | null;
    }> = [];
    const gapSchedulingSample: Array<Record<string, unknown>> = [];
    const resolveStationObservedForCheckpoint = (stationId: number): {
      minObserved: string | null;
      recentGapMinObserved: string | null;
    } => {
      const timeseriesRefs = timeseriesRefsByStationId.get(stationId);
      if (!timeseriesRefs?.length) {
        return { minObserved: null, recentGapMinObserved: null };
      }
      let minObserved: string | null = null;
      let recentGapMinObserved: string | null = null;
      let hasRecentGap = false;
      for (const timeseriesRef of timeseriesRefs) {
        const latestObserved =
          latestByTimeseries.get(timeseriesRef)?.observed_at ?? null;
        const timeseriesId = timeseriesIdByRef[timeseriesRef];
        const checkpointObserved = timeseriesId
          ? timeseriesCheckpointById[timeseriesId]?.last_observed_at ?? null
          : null;
        const candidate = latestObserved ?? checkpointObserved;
        if (!candidate) {
          continue;
        }
        if (!minObserved || candidate < minObserved) {
          minObserved = candidate;
        }
        if (gapHasRecentGapByTimeseriesRef.get(timeseriesRef)) {
          hasRecentGap = true;
          const contigEnd = gapContigEndByTimeseriesRef.get(timeseriesRef) ??
            candidate;
          if (
            contigEnd &&
            (!recentGapMinObserved || contigEnd < recentGapMinObserved)
          ) {
            recentGapMinObserved = contigEnd;
          }
        }
      }
      if (!hasRecentGap) {
        return { minObserved, recentGapMinObserved: null };
      }
      return { minObserved, recentGapMinObserved };
    };
    for (const stationId of polledStationIds) {
      const checkpoint = checkpointByStationId[stationId];
      const _isNewCheckpoint = checkpoint === undefined;
      const previousLastObserved = checkpoint?.last_observed_at ?? null;
      const previousNextDue = checkpoint?.next_due_at ?? null;
      let observSamples = checkpoint?.observ_interval_samples ?? [];
      let lagSamples = checkpoint?.ingest_lag_samples ?? [];
      let updatedLastObserved = previousLastObserved;
      let nextDueAt = previousNextDue;
      const latestObservedForScheduling =
        latestObservedByStationId.get(stationId) ?? null;
      const { minObserved, recentGapMinObserved } =
        resolveStationObservedForCheckpoint(stationId);
      const minObservedForStation = recentGapMinObserved ?? minObserved;

      if (recentGapMinObserved) {
        logLine("INFO", "OpenAQ recent gap clamp applied", {
          station_id: stationId,
          min_observed_at: minObserved,
          recent_gap_min_observed_at: recentGapMinObserved,
          station_last_observed_at: minObservedForStation,
          latest_observed_at: latestObservedForScheduling,
        });
      }

      if (minObservedForStation) {
        updatedLastObserved = minObservedForStation;
      }
      if (
        stationObservedSample.length < 10 &&
        (minObservedForStation || latestObservedForScheduling)
      ) {
        stationObservedSample.push({
          station_id: stationId,
          min_observed_at: minObserved,
          recent_gap_min_observed_at: recentGapMinObserved,
          station_last_observed_at: minObservedForStation,
          latest_observed_at: latestObservedForScheduling,
        });
      }

      const isGapStation = gapStationIds.has(stationId);
      const hasNewObservation = Boolean(
        latestObservedForScheduling &&
          (!previousLastObserved ||
            latestObservedForScheduling > previousLastObserved),
      );
      const latestObservedForDecision = latestObservedForScheduling ??
        updatedLastObserved;
      const latestObservedMs = latestObservedForDecision
        ? Date.parse(latestObservedForDecision)
        : null;
      const isRecentObserved = latestObservedMs !== null &&
        Number.isFinite(latestObservedMs) &&
        latestObservedMs >= nowMsForLag - 24 * 60 * 60 * 1000;
      const minStationIntervalSeconds = minSeconds(observSamples);

      if (isGapStation) {
        if (!latestObservedForDecision || !Number.isFinite(latestObservedMs)) {
          nextDueAt = new Date(nowMsForLag - 24 * 60 * 60 * 1000).toISOString();
        } else if (hasNewObservation) {
          nextDueAt = isRecentObserved
            ? new Date(nowMsForLag + 60 * 60 * 1000).toISOString()
            : nowIso;
        } else {
          const intervalSeconds = Math.min(
            60 * 60,
            Math.max(0, minStationIntervalSeconds ?? 60 * 60),
          );
          const baseObservedMs = Date.parse(latestObservedForDecision);
          nextDueAt = Number.isFinite(baseObservedMs)
            ? new Date(baseObservedMs + intervalSeconds * 1000).toISOString()
            : latestObservedForDecision;
        }
        if (gapSchedulingSample.length < 10) {
          gapSchedulingSample.push({
            station_id: stationId,
            latest_observed_at: latestObservedForDecision,
            has_new_observation: hasNewObservation,
            is_recent_observed: isRecentObserved,
            min_station_interval_seconds: minStationIntervalSeconds,
            next_due_at: nextDueAt,
          });
        }
      } else if (hasNewObservation && latestObservedForScheduling) {
        const latestObservedForNonGap = latestObservedForScheduling;
        let intervalSampleAdded = false;
        if (previousLastObserved) {
          const intervalSeconds = Math.max(
            0,
            Math.round(
              (Date.parse(latestObservedForNonGap) -
                Date.parse(previousLastObserved)) / 1000,
            ),
          );
          if (Number.isFinite(intervalSeconds) && intervalSeconds > 0) {
            observSamples = appendSample(observSamples, intervalSeconds);
            intervalSampleAdded = true;
          }
        }
        if (intervalSampleAdded) {
          const lagSeconds = Math.max(
            0,
            Math.round(
              (nowMsForLag - Date.parse(latestObservedForNonGap)) / 1000,
            ),
          );
          if (Number.isFinite(lagSeconds)) {
            lagSamples = appendSample(lagSamples, lagSeconds);
          }
        }
        if (observSamples.length < 10 || lagSamples.length < 10) {
          nextDueAt = new Date(nowMsForLag + 5 * 60 * 1000).toISOString();
        } else {
          const intervalSeconds = Math.min(
            minSeconds(observSamples) ?? 5 * 60,
            60 * 60,
          );
          const lagSeconds = lagSecondsByStat(lagSamples) ?? 5 * 60;
          const baseMs = Date.parse(latestObservedForNonGap);
          if (Number.isFinite(baseMs)) {
            nextDueAt = new Date(baseMs + (intervalSeconds + lagSeconds) * 1000)
              .toISOString();
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
        observ_interval_samples: observSamples,
        ingest_lag_samples: lagSamples,
        last_polled_at: nowIso,
      });
    }

    try {
      const rowsUpserted = await upsertOpenaqStationCheckpoints(checkpointRows);
      logLine("INFO", "OpenAQ station checkpoints upserted", {
        rows_prepared: checkpointRows.length,
        rows_upserted: rowsUpserted,
        lag_stat: OPENAQ_LAG_STAT,
        station_observed_sample: stationObservedSample,
        gap_scheduling_sample: gapSchedulingSample,
      });
    } catch (err) {
      await logError({
        severity: "warn",
        message: "OpenAQ checkpoints upsert failed",
        connector_id: connector.id,
        context: { error: String(err) },
      });
    }

    if (latestByTimeseries.size) {
      const checkpointTimeseriesIds = Array.from(
        new Set(
          Array.from(latestByTimeseries.keys())
            .map((timeseriesRef) => timeseriesIdByRef[timeseriesRef])
            .filter((timeseriesId): timeseriesId is number =>
              Number.isFinite(Number(timeseriesId))
            )
            .map((timeseriesId) => Number(timeseriesId)),
        ),
      );
      let timeseriesCheckpointDetailsById: Record<
        number,
        OpenAQTimeseriesCheckpoint
      > = {};
      if (checkpointTimeseriesIds.length) {
        try {
          timeseriesCheckpointDetailsById =
            await fetchOpenaqTimeseriesCheckpointDetails(
              checkpointTimeseriesIds,
            );
          logLine("INFO", "OpenAQ timeseries checkpoint details fetched", {
            requested_timeseries_ids: checkpointTimeseriesIds.length,
            checkpoints: Object.keys(timeseriesCheckpointDetailsById).length,
          });
        } catch (err) {
          await logError({
            severity: "warn",
            message: "OpenAQ timeseries checkpoint details fetch failed",
            connector_id: connector.id,
            context: { error: String(err) },
          });
          timeseriesCheckpointDetailsById = {};
        }
      }
      const timeseriesCheckpointRows: Array<Record<string, unknown>> = [];
      const timeseriesCheckpointStats = {
        latest_timeseries: latestByTimeseries.size,
        rows_prepared: 0,
        skipped_missing_timeseries_id: 0,
        skipped_missing_station_id: 0,
        missing_timeseries_id_sample: [] as string[],
        missing_station_id_sample: [] as Array<
          { timeseries_ref: string; timeseries_id: number }
        >,
        new_checkpoints: 0,
        existing_checkpoints: 0,
        new_observations: 0,
        next_due_updated: 0,
      };
      for (const [timeseriesRef, latest] of latestByTimeseries.entries()) {
        const timeseriesId = timeseriesIdByRef[timeseriesRef];
        if (!timeseriesId) {
          timeseriesCheckpointStats.skipped_missing_timeseries_id += 1;
          if (
            timeseriesCheckpointStats.missing_timeseries_id_sample.length < 10
          ) {
            timeseriesCheckpointStats.missing_timeseries_id_sample.push(
              timeseriesRef,
            );
          }
          continue;
        }
        const stationId = stationIdByTimeseriesRef.get(timeseriesRef) ??
          stationIdByTimeseriesId[timeseriesId];
        if (!stationId) {
          timeseriesCheckpointStats.skipped_missing_station_id += 1;
          if (timeseriesCheckpointStats.missing_station_id_sample.length < 10) {
            timeseriesCheckpointStats.missing_station_id_sample.push({
              timeseries_ref: timeseriesRef,
              timeseries_id: timeseriesId,
            });
          }
          continue;
        }
        const checkpoint = timeseriesCheckpointDetailsById[timeseriesId];
        if (checkpoint) {
          timeseriesCheckpointStats.existing_checkpoints += 1;
        } else {
          timeseriesCheckpointStats.new_checkpoints += 1;
        }
        const previousLastObserved = checkpoint?.last_observed_at ?? null;
        const previousNextDue = checkpoint?.next_due_at ?? null;
        let lagSamples = checkpoint?.ingest_lag_samples ?? [];
        let updatedLastObserved = previousLastObserved;
        let nextDueAt = previousNextDue;
        const latestObserved = latest?.observed_at ?? null;
        let hasNewObservation = false;

        if (
          latestObserved &&
          (!previousLastObserved || latestObserved > previousLastObserved)
        ) {
          updatedLastObserved = latestObserved;
          hasNewObservation = true;
          timeseriesCheckpointStats.new_observations += 1;
          const lagSeconds = Math.max(
            0,
            Math.round((nowMsForLag - Date.parse(latestObserved)) / 1000),
          );
          if (Number.isFinite(lagSeconds)) {
            lagSamples = appendSample(lagSamples, lagSeconds);
          }
        }

        if (hasNewObservation || !previousNextDue) {
          timeseriesCheckpointStats.next_due_updated += 1;
          if (lagSamples.length < 10) {
            nextDueAt = new Date(nowMsForLag + 5 * 60 * 1000).toISOString();
          } else {
            const lagSeconds = lagSecondsByStat(lagSamples) ?? 5 * 60;
            const baseMs = Date.parse(
              updatedLastObserved ?? latestObserved ?? "",
            );
            if (Number.isFinite(baseMs)) {
              nextDueAt = new Date(baseMs + (60 * 60 + lagSeconds) * 1000)
                .toISOString();
            } else {
              nextDueAt = nowIso;
            }
          }
        }

        timeseriesCheckpointRows.push({
          station_id: stationId,
          timeseries_id: timeseriesId,
          next_due_at: nextDueAt,
          last_observed_at: updatedLastObserved,
          ingest_lag_samples: lagSamples,
          last_polled_at: nowIso,
        });
        timeseriesCheckpointStats.rows_prepared += 1;
      }

      if (timeseriesCheckpointRows.length) {
        try {
          const rowsUpserted = await upsertOpenaqTimeseriesCheckpoints(
            timeseriesCheckpointRows,
          );
          logLine("INFO", "OpenAQ timeseries checkpoints upserted", {
            ...timeseriesCheckpointStats,
            lag_stat: OPENAQ_LAG_STAT,
            rows_upserted: rowsUpserted,
          });
        } catch (err) {
          await logError({
            severity: "warn",
            message: "OpenAQ timeseries checkpoints upsert failed",
            connector_id: connector.id,
            context: { error: String(err) },
          });
        }
      } else {
        logLine("INFO", "OpenAQ timeseries checkpoints skipped (no rows)", {
          ...timeseriesCheckpointStats,
        });
      }
    }
  } else if (!dryRun) {
    logLine("INFO", "OpenAQ checkpoint updates skipped", {
      stations_polled: polledStationIds.size,
      latest_timeseries: latestByTimeseries.size,
      dry_run: dryRun,
    });
  }

  const stoppedReason = timeBudgetHit
    ? "runtime_budget_exceeded"
    : rateLimitState.stop
    ? (rateLimitState.stopReason ?? "rate_limit_guard")
    : null;
  const rateLimitUsedEstimate =
    rateLimitState.limit !== null && rateLimitState.remaining !== null
      ? Math.max(0, rateLimitState.limit - rateLimitState.remaining)
      : null;
  const gapStationsPolled = Array.from(polledStationIds).filter((stationId) =>
    gapStationIds.has(stationId)
  ).length;

  logLine("INFO", "OpenAQ ingest complete", {
    locations: locations.length,
    station_fetch_enabled: locationsFetched,
    stations_selected: stationsSelected,
    stations_polled: polledStationIds.size,
    lag_stat: OPENAQ_LAG_STAT,
    gap_stations_total: gapStationIds.size,
    gap_stations_polled: gapStationsPolled,
    min_gap_stations: minGapStations,
    min_non_gap_stations: minNonGapStations,
    stations_updated: stationsUpdated,
    timeseries_updated: timeseriesRows.length,
    timeseries_last_updated: timeseriesLastUpdated,
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
    series_polled: seriesPolled,
    last_observed_at: lastObservedAt,
    rate_limit_remaining: rateLimitState.remaining,
    rate_limit_reset: rateLimitState.reset,
    rate_limit_reset_at: rateLimitState.resetAt,
    rate_limit_stop: rateLimitState.stop,
    rate_limit_stop_reason: rateLimitState.stopReason,
    partial: timeBudgetHit,
    stopped_reason: stoppedReason,
    requests_total: requestBudgetState.total,
    max_requests_per_run: requestBudgetState.maxPerRun,
    gap_requests_planned: requestBudgetState.gapPlannedRequests,
    gap_requests_executed: requestBudgetState.gapExecutedRequests,
    gap_requests_skipped_budget: requestBudgetState.gapSkippedBudgetRequests,
    gap_zero_yield_timeseries: requestBudgetState.gapZeroYieldTimeseries,
    raw_responses: rawRecorder?.responseCount ?? requestBudgetState.total,
    shared_budget_enabled: sharedBudgetState.enabled,
    shared_budget_key: sharedBudgetState.key,
    shared_budget_caller: sharedBudgetState.caller,
    shared_budget_minute_limit: sharedBudgetState.minuteLimit,
    shared_budget_hour_limit: sharedBudgetState.hourLimit,
    shared_budget_granted: sharedBudgetState.granted,
    shared_budget_reason: sharedBudgetState.reason,
    shared_budget_requested_tokens: sharedBudgetState.requestedTokens,
    shared_budget_minute_used_after: sharedBudgetState.minuteUsedAfter,
    shared_budget_minute_remaining: sharedBudgetState.minuteRemaining,
    shared_budget_hour_used_after: sharedBudgetState.hourUsedAfter,
    shared_budget_hour_remaining: sharedBudgetState.hourRemaining,
    shared_budget_retry_after_seconds: sharedBudgetState.retryAfterSeconds,
    warnings: runWarnings,
  });

  logLine("INFO", "OpenAQ rate limit summary", {
    rate_limit_limit: rateLimitState.limit,
    rate_limit_reset: rateLimitState.reset,
    rate_limit_reset_at: rateLimitState.resetAt,
    rate_limit_remaining_first: rateLimitState.firstRemaining,
    rate_limit_remaining_last: rateLimitState.remaining,
    rate_limit_used_estimate: rateLimitUsedEstimate,
    requests_total: requestBudgetState.total,
    max_requests_per_run: requestBudgetState.maxPerRun,
    gap_requests_remaining_min: requestBudgetState.gapReserveMin,
    gap_requests_planned: requestBudgetState.gapPlannedRequests,
    gap_requests_executed: requestBudgetState.gapExecutedRequests,
    gap_requests_skipped_budget: requestBudgetState.gapSkippedBudgetRequests,
    gap_zero_yield_timeseries: requestBudgetState.gapZeroYieldTimeseries,
    stations_selected: stationsSelected,
    stations_polled: polledStationIds.size,
    gap_stations_total: gapStationIds.size,
    gap_stations_polled: gapStationsPolled,
    min_gap_stations: minGapStations,
    min_non_gap_stations: minNonGapStations,
    stopped_reason: stoppedReason,
    shared_budget_enabled: sharedBudgetState.enabled,
    shared_budget_key: sharedBudgetState.key,
    shared_budget_caller: sharedBudgetState.caller,
    shared_budget_minute_limit: sharedBudgetState.minuteLimit,
    shared_budget_hour_limit: sharedBudgetState.hourLimit,
    shared_budget_granted: sharedBudgetState.granted,
    shared_budget_reason: sharedBudgetState.reason,
    shared_budget_requested_tokens: sharedBudgetState.requestedTokens,
    shared_budget_minute_used_before: sharedBudgetState.minuteUsedBefore,
    shared_budget_minute_used_after: sharedBudgetState.minuteUsedAfter,
    shared_budget_minute_remaining: sharedBudgetState.minuteRemaining,
    shared_budget_minute_reset_at: sharedBudgetState.minuteResetAt,
    shared_budget_hour_used_before: sharedBudgetState.hourUsedBefore,
    shared_budget_hour_used_after: sharedBudgetState.hourUsedAfter,
    shared_budget_hour_remaining: sharedBudgetState.hourRemaining,
    shared_budget_hour_reset_at: sharedBudgetState.hourResetAt,
    shared_budget_retry_after_seconds: sharedBudgetState.retryAfterSeconds,
  });

  if (dropboxConfig) {
    try {
      if (rawRecorder) {
        const rawPayload = rawRecorder.lines.join("\n") + "\n";
        const jsonlName = buildDropboxRawPath(connectorCode, new Date())
          .replace(/\.zip$/i, ".jsonl");
        const zipped = await zipTextCompressed(
          jsonlName.split("/").slice(-1)[0],
          rawPayload,
        );
        await dropboxUploadFileWithRetry(
          dropboxConfig,
          buildDropboxRawPath(connectorCode, new Date()),
          zipped,
        );
      }
      if (errorLogLines && errorLogLines.length) {
        await dropboxUploadFileWithRetry(
          dropboxConfig,
          buildDropboxErrorPath(connectorCode, new Date()),
          errorLogLines.join("\n") + "\n",
        );
      }
      await dropboxUploadFileWithRetry(
        dropboxConfig,
        buildDropboxLogPath(connectorCode, new Date()),
        logLines.join("\n") + "\n",
      );
    } catch (err) {
      await logError({
        severity: "warn",
        message: "Dropbox log/raw upload failed.",
        connector_id: connector.id,
        context: { error: String(err), dropbox: dropboxDiagnostics },
      });
    }
  } else if (dropboxDiagnostics.reason) {
    await logError({
      severity: "warn",
      message: "Dropbox log/raw uploads disabled.",
      connector_id: connector.id,
      context: { dropbox: dropboxDiagnostics },
    });
  }

  return jsonResponse({
    connector_code: connectorCode,
    stations_requested: stationsRequested,
    stations_selected: stationsSelected,
    stations_polled: polledStationIds.size,
    gap_stations_total: gapStationIds.size,
    gap_stations_polled: gapStationsPolled,
    min_gap_stations: minGapStations,
    min_non_gap_stations: minNonGapStations,
    stations_updated: stationsUpdated,
    timeseries_updated: timeseriesRows.length,
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
    series_polled: seriesPolled,
    window_hours: windowHours,
    last_observed_at: lastObservedAt,
    station_fetch_enabled: locationsFetched,
    partial: timeBudgetHit,
    stopped_reason: stoppedReason,
    rate_limit_remaining: rateLimitState.remaining,
    rate_limit_limit: rateLimitState.limit,
    rate_limit_reset: rateLimitState.reset,
    rate_limit_reset_at: rateLimitState.resetAt,
    rate_limit_remaining_first: rateLimitState.firstRemaining,
    rate_limit_stop: rateLimitState.stop,
    rate_limit_stop_reason: rateLimitState.stopReason,
    rate_limit_used_estimate: rateLimitUsedEstimate,
    requests_total: requestBudgetState.total,
    max_requests_per_run: requestBudgetState.maxPerRun,
    tiered_limit: tieredLimit,
    stale_limit: staleLimit,
    lag_stat: OPENAQ_LAG_STAT,
    tier1_retry_seconds: tier1RetrySeconds,
    gap_requests_remaining_min: requestBudgetState.gapReserveMin,
    gap_requests_planned: requestBudgetState.gapPlannedRequests,
    gap_requests_executed: requestBudgetState.gapExecutedRequests,
    gap_requests_skipped_budget: requestBudgetState.gapSkippedBudgetRequests,
    gap_zero_yield_timeseries: requestBudgetState.gapZeroYieldTimeseries,
    dry_run: dryRun,
    warnings: runWarnings,
    ...sharedBudgetResponseFields(),
  });
});
