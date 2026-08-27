import "../../supabase/functions/_shared/fetch_egress_patch.ts";
import {
  normalizeDropboxPath,
  uploadErrorLogJsonToDropbox,
} from "../shared/dropbox_error_log.ts";
import {
  buildSosCloudRunChildResult,
  buildSosCloudRunSkippedResult,
  describeSosDependencyFailure,
  isCompletedSosChildResponse,
  isRecognizedSosDependencyFailure,
  type SosCloudRunChildResult,
} from "./result_contract.ts";
import { UK_AIR_HTML_BRIDGE_POLLUTANT_CODES } from "../../supabase/functions/ingest_sos/uk_aq_html_parser.ts";
import {
  normalizeSosSelectedWorkRpcResponse,
  type SosSelectedStationRow,
  type SosSelectedTimeseriesDispatchRow,
  type SosSelectedWorkPlan,
} from "../../supabase/functions/ingest_sos/selected_work.ts";
import {
  type PostgrestResponse,
  requestWithTransientJwtFutureRetry,
} from "./postgrest_auth_retry.ts";

const CONNECTOR_CODE =
  (Deno.env.get("SOS_CONNECTOR_CODE") || "sos").trim();
const SCHEDULER_BACKEND_SUPABASE_FUNCTION = "supabase_function";
const SCHEDULER_BACKEND_GOOGLE_CLOUD_RUN = "google_cloud_run";

const DEFAULT_INTERVAL_MINUTES = parsePositiveInt(
  Deno.env.get("SOS_DEFAULT_INTERVAL_MINUTES"),
  60,
);
const IN_FLIGHT_TIMEOUT_MINUTES = parsePositiveInt(
  Deno.env.get("SOS_IN_FLIGHT_TIMEOUT_MINUTES"),
  30,
);
const CLAIM_TIMEOUT_MINUTES = parsePositiveInt(
  Deno.env.get("SOS_CLAIM_TIMEOUT_MINUTES"),
  30,
);
const DEFAULT_WINDOW_HOURS = parsePositiveInt(
  Deno.env.get("SOS_DEFAULT_WINDOW_HOURS"),
  6,
);
const DEFAULT_TIMESERIES_LIMIT = parsePositiveInt(
  Deno.env.get("SOS_DEFAULT_TIMESERIES_LIMIT"),
  100,
);
const DEFAULT_STATION_BATCH_LIMIT = parsePositiveInt(
  Deno.env.get("SOS_STATION_BATCH_LIMIT"),
  100,
);
const DEFAULT_STALE_LIMIT = parsePositiveInt(
  Deno.env.get("SOS_STALE_LIMIT"),
  4,
);
const PORT = parsePositiveInt(
  Deno.env.get("SOS_LOCAL_PORT") || Deno.env.get("PORT"),
  8000,
);
const REQUEST_PAYLOAD_RAW = (Deno.env.get("SOS_REQUEST_PAYLOAD") ||
  "{}").trim();
const REQUEST_PAYLOAD_OVERRIDES = parseRequestPayload(REQUEST_PAYLOAD_RAW);
const CRON_SECRET = (Deno.env.get("SB_UK_AQ_CRON_SECRET") || "").trim();
const SOS_INGEST_SCRIPT_PATH =
  (Deno.env.get("SOS_INGEST_SCRIPT_PATH") ||
    "/app/runtime/ingest_sos/index.ts").trim();

const SUPABASE_URL = requiredEnv("SUPABASE_URL");
const SUPABASE_PRIVILEGED_KEY = requiredEnvAny(["SB_SECRET_KEY"]);
const UK_AQ_CORE_SCHEMA = (Deno.env.get("UK_AQ_CORE_SCHEMA") || "uk_aq_core")
  .trim();
const UK_AQ_RAW_SCHEMA = (Deno.env.get("UK_AQ_RAW_SCHEMA") || "uk_aq_raw")
  .trim();
const UK_AQ_PUBLIC_SCHEMA = "uk_aq_public";
const REST_BASE_URL = `${SUPABASE_URL.replace(/\/$/, "")}/rest/v1`;
const UK_AQ_DROPBOX_ROOT = normalizeDropboxPath(
  Deno.env.get("UK_AQ_DROPBOX_ROOT") ?? "",
);
const DROPBOX_APP_KEY = (Deno.env.get("DROPBOX_APP_KEY") || "").trim();
const DROPBOX_APP_SECRET = (Deno.env.get("DROPBOX_APP_SECRET") || "").trim();
const DROPBOX_REFRESH_TOKEN = (Deno.env.get("DROPBOX_REFRESH_TOKEN") || "")
  .trim();
const DROPBOX_ERROR_ALLOWED_SUPABASE_URL = (
  Deno.env.get("SOS_ERROR_DROPBOX_ALLOWED_SUPABASE_URL") ??
  Deno.env.get("UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL") ??
  Deno.env.get("SOS_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ??
  Deno.env.get("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ??
  ""
).trim();
const DROPBOX_ERROR_FOLDER = (
  Deno.env.get("SOS_ERROR_DROPBOX_FOLDER") ??
  Deno.env.get("UK_AIR_ERROR_DROPBOX_FOLDER") ??
  "/error_log"
).trim();

const CHECKPOINT_WARMUP_SECONDS = 5 * 60;
const CHECKPOINT_BASE_PERIOD_SECONDS = 60 * 60;
const CHECKPOINT_LAG_SAMPLE_MAX = 50;
const SOS_RUN_RESULT_PATH = (Deno.env.get("SOS_RUN_RESULT_PATH") ?? "").trim();

type IngestResponse = {
  ok: boolean;
  status: number;
  body: unknown;
  raw: string;
};

type ConnectorConfig = {
  id: unknown;
  connector_code: unknown;
  poll_enabled: unknown;
  poll_interval_minutes: unknown;
  poll_window_hours: unknown;
  poll_timeseries_batch_size: unknown;
  scheduler_backend: unknown;
  last_polled_at: unknown;
  last_run_start: unknown;
  last_run_end: unknown;
  last_run_status: unknown;
};

type DispatchClaimRow = {
  claimed: unknown;
  connector_id: unknown;
  last_run_start: unknown;
  last_run_end: unknown;
};

type StationRow = SosSelectedStationRow;
type SelectedTimeseriesRow = SosSelectedTimeseriesDispatchRow;

type StationCheckpointRow = {
  station_id: number;
  next_due_at: string | null;
  last_observed_at: string | null;
  ingest_lag_samples: number[];
  last_polled_at: string | null;
};

function requiredEnv(name: string): string {
  const value = (Deno.env.get(name) || "").trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function requiredEnvAny(names: string[]): string {
  for (const name of names) {
    const value = (Deno.env.get(name) || "").trim();
    if (value) {
      return value;
    }
  }
  throw new Error(
    `Missing required environment variable: one of ${names.join(", ")}`,
  );
}

function parsePositiveInt(raw: string | undefined, fallback: number): number {
  const value = Number(raw || "");
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.trunc(value);
}

function parseRequestPayload(raw: string): Record<string, unknown> {
  if (!raw) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (_error) {
    throw new Error("SOS_REQUEST_PAYLOAD must be valid JSON.");
  }
  const payload = toObject(parsed);
  if (!payload) {
    throw new Error("SOS_REQUEST_PAYLOAD must be a JSON object.");
  }
  return payload;
}

function toObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function toStringOrNull(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function toIntegerOrNull(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return Math.trunc(parsed);
}

function toPositiveIntegerOrNull(value: unknown): number | null {
  const parsed = toIntegerOrNull(value);
  if (parsed === null || parsed <= 0) {
    return null;
  }
  return parsed;
}

function parseTimestamp(value: unknown): Date | null {
  const text = toStringOrNull(value);
  if (!text) {
    return null;
  }
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date;
}

function shortError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
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

function postgrestIn(values: Array<string | number>): string {
  return `in.(${values.map((value) => quotePostgrestValue(String(value))).join(",")})`;
}

function evaluateDue(connector: ConnectorConfig | null, now: Date): {
  due: boolean;
  reason: string;
  intervalMinutes: number;
} {
  if (!connector) {
    return {
      due: false,
      reason: "connector_not_found",
      intervalMinutes: DEFAULT_INTERVAL_MINUTES,
    };
  }

  if (connector.poll_enabled !== true) {
    return {
      due: false,
      reason: "poll_disabled",
      intervalMinutes: DEFAULT_INTERVAL_MINUTES,
    };
  }

  const schedulerBackend = toStringOrNull(connector.scheduler_backend) ||
    SCHEDULER_BACKEND_SUPABASE_FUNCTION;
  if (schedulerBackend !== SCHEDULER_BACKEND_GOOGLE_CLOUD_RUN) {
    return {
      due: false,
      reason: "scheduler_backend_not_cloud_run",
      intervalMinutes: DEFAULT_INTERVAL_MINUTES,
    };
  }

  const intervalMinutes = toIntegerOrNull(connector.poll_interval_minutes) ||
    DEFAULT_INTERVAL_MINUTES;

  const runStartedAt = parseTimestamp(connector.last_run_start);
  const runEndedAt = parseTimestamp(connector.last_run_end);
  if (runStartedAt && !runEndedAt) {
    const runningGuardMs =
      Math.max(intervalMinutes, IN_FLIGHT_TIMEOUT_MINUTES) * 60 * 1000;
    const ageMs = now.getTime() - runStartedAt.getTime();
    if (Number.isFinite(ageMs) && ageMs >= 0 && ageMs < runningGuardMs) {
      return { due: false, reason: "in_flight", intervalMinutes };
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

function getWindowHours(connector: ConnectorConfig | null): number {
  const value = toPositiveIntegerOrNull(connector?.poll_window_hours);
  if (value !== null) {
    return value;
  }
  return DEFAULT_WINDOW_HOURS;
}

function getTimeseriesLimit(connector: ConnectorConfig | null): number {
  const value = toPositiveIntegerOrNull(connector?.poll_timeseries_batch_size);
  if (value !== null) {
    return value;
  }
  return DEFAULT_TIMESERIES_LIMIT;
}

function getStationBatchLimit(connector: ConnectorConfig | null): number {
  const value = toPositiveIntegerOrNull(connector?.poll_timeseries_batch_size);
  if (value !== null) {
    return value;
  }
  return DEFAULT_STATION_BATCH_LIMIT;
}

function postgrestHeaders(
  schema: string,
  write = false,
): Record<string, string> {
  const headers: Record<string, string> = {
    apikey: SUPABASE_PRIVILEGED_KEY,
    Accept: "application/json",
    "Accept-Profile": schema,
  };
  if (write) {
    headers["Content-Type"] = "application/json";
    headers["Content-Profile"] = schema;
  }
  return headers;
}

function withQuery(path: string, query?: Record<string, string>): string {
  const url = new URL(`${REST_BASE_URL}/${path}`);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (!value) {
        continue;
      }
      url.searchParams.set(key, value);
    }
  }
  return url.toString();
}

async function postgrestRequest(
  method: string,
  path: string,
  options: {
    schema?: string;
    query?: Record<string, string>;
    body?: unknown;
    prefer?: string;
    transientJwtFutureRetry?: {
      operation: string;
    };
  } = {},
): Promise<PostgrestResponse> {
  const schema = options.schema || UK_AQ_CORE_SCHEMA;
  const write = method !== "GET";
  const headers = postgrestHeaders(schema, write);
  if (options.prefer) {
    headers.Prefer = options.prefer;
  }
  const url = withQuery(path, options.query);
  const body = options.body === undefined
    ? undefined
    : JSON.stringify(options.body);
  const request = async (): Promise<PostgrestResponse> => {
    const response = await fetch(url, {
      method,
      headers,
      body,
    });
    const text = await response.text();
    let data: unknown = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = text;
      }
    }
    return { ok: response.ok, status: response.status, text, data };
  };

  if (!options.transientJwtFutureRetry) {
    return await request();
  }
  return await requestWithTransientJwtFutureRetry(request, {
    operation: options.transientJwtFutureRetry.operation,
    target: `${method} ${path}`,
    logRetry: (details) =>
      logSummary("postgrest_transient_jwt_future_retry", details),
  });
}

async function loadSelectedWork(params: {
  batchLimit: number;
  staleLimit: number;
  timeseriesLimit: number;
  dayUtc: string;
}): Promise<SosSelectedWorkPlan> {
  const response = await postgrestRequest(
    "POST",
    "rpc/uk_aq_rpc_sos_selected_work_v1",
    {
      body: {
        p_batch_limit: Math.max(1, Math.trunc(params.batchLimit)),
        p_stale_limit: Math.max(0, Math.trunc(params.staleLimit)),
        p_timeseries_limit: Math.max(1, Math.trunc(params.timeseriesLimit)),
        p_day_utc: params.dayUtc,
        p_pollutant_codes: [...UK_AIR_HTML_BRIDGE_POLLUTANT_CODES],
      },
      schema: UK_AQ_PUBLIC_SCHEMA,
      transientJwtFutureRetry: {
        operation: "load_sos_selected_work",
      },
    },
  );
  if (!response.ok) {
    throw new Error(
      `Failed to load SOS selected work (${response.status}): ${response.text}`,
    );
  }
  return normalizeSosSelectedWorkRpcResponse(response.data);
}

async function recordStationAttempts(
  timeseriesRows: SelectedTimeseriesRow[],
  attemptedAtIso: string,
): Promise<number> {
  const stationIds = [...new Set(timeseriesRows.map((row) => row.station_id))]
    .sort((a, b) => a - b);
  if (!stationIds.length) {
    return 0;
  }

  const response = await postgrestRequest(
    "POST",
    "rpc/sos_record_station_attempts",
    {
      schema: UK_AQ_CORE_SCHEMA,
      body: {
        p_station_ids: stationIds,
        p_attempted_at: attemptedAtIso,
      },
    },
  );
  if (!response.ok) {
    throw new Error(
      `Failed to record SOS station attempts (${response.status}): ${response.text}`,
    );
  }

  const recorded = toIntegerOrNull(response.data);
  if (recorded !== stationIds.length) {
    throw new Error(
      `SOS station attempt count mismatch: expected ${stationIds.length}, recorded ${recorded}`,
    );
  }
  return recorded;
}

async function loadSuccessfullyPolledTimeseriesIds(
  timeseriesIds: string[],
  runStartedAtIso: string,
): Promise<Set<number>> {
  const successfullyPolled = new Set<number>();
  if (!timeseriesIds.length) return successfullyPolled;

  const runStartedAtMs = Date.parse(runStartedAtIso);
  let offset = 0;
  const limit = 1000;
  while (true) {
    const response = await postgrestRequest(
      "GET",
      "sos_timeseries_checkpoints",
      {
        schema: UK_AQ_RAW_SCHEMA,
        query: {
          select: "timeseries_id,last_polled_at",
          timeseries_id: postgrestIn(timeseriesIds),
          last_polled_at: `gte.${runStartedAtIso}`,
          order: "timeseries_id.asc",
          limit: String(limit),
          offset: String(offset),
        },
      },
    );
    if (!response.ok) {
      throw new Error(
        `Failed to load SOS timeseries checkpoints (${response.status}): ${response.text}`,
      );
    }
    const rows = Array.isArray(response.data) ? response.data : [];
    for (const row of rows) {
      const record = toObject(row);
      const timeseriesId = toIntegerOrNull(record?.timeseries_id);
      const lastPolledAt = toStringOrNull(record?.last_polled_at);
      if (
        timeseriesId !== null && lastPolledAt &&
        Number.isFinite(runStartedAtMs) &&
        Date.parse(lastPolledAt) >= runStartedAtMs
      ) {
        successfullyPolled.add(timeseriesId);
      }
    }
    if (rows.length < limit) break;
    offset += limit;
  }
  return successfullyPolled;
}

async function fetchMaxTimeseriesLastValueAt(
  timeseriesIds: string[],
): Promise<string | null> {
  if (!timeseriesIds.length) {
    return null;
  }
  const response = await postgrestRequest("GET", "timeseries", {
    query: {
      select: "last_value_at",
      id: postgrestIn(timeseriesIds),
      last_value_at: "not.is.null",
      order: "last_value_at.desc",
      limit: "1",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to load max timeseries.last_value_at (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data) ? response.data : [];
  return toStringOrNull(toObject(rows[0])?.last_value_at);
}

async function loadStationLatestObserved(
  connectorId: number,
  stationIds: number[],
): Promise<Map<number, string>> {
  const latestByStation = new Map<number, string>();
  if (!stationIds.length) {
    return latestByStation;
  }

  let offset = 0;
  const limit = 1000;
  while (true) {
    const response = await postgrestRequest("GET", "timeseries", {
      query: {
        select: "station_id,last_value_at",
        connector_id: `eq.${connectorId}`,
        station_id: postgrestIn(stationIds),
        ended_at: "is.null",
        last_value_at: "not.is.null",
        limit: String(limit),
        offset: String(offset),
      },
    });
    if (!response.ok) {
      throw new Error(
        `Failed to load SOS station latest observed (${response.status}): ${response.text}`,
      );
    }
    const rows = Array.isArray(response.data) ? response.data : [];
    for (const row of rows) {
      const obj = toObject(row);
      const stationId = toIntegerOrNull(obj?.station_id);
      const observedAt = toStringOrNull(obj?.last_value_at);
      if (stationId === null || !observedAt) {
        continue;
      }
      const current = latestByStation.get(stationId);
      if (!current || Date.parse(observedAt) > Date.parse(current)) {
        latestByStation.set(stationId, observedAt);
      }
    }
    if (rows.length < limit) {
      break;
    }
    offset += limit;
  }

  return latestByStation;
}

async function loadStationCheckpointRows(
  stationIds: number[],
): Promise<Map<number, StationCheckpointRow>> {
  const checkpoints = new Map<number, StationCheckpointRow>();
  if (!stationIds.length) {
    return checkpoints;
  }

  let offset = 0;
  const limit = 1000;
  while (true) {
    const response = await postgrestRequest(
      "GET",
      "sos_station_checkpoints",
      {
        schema: UK_AQ_RAW_SCHEMA,
        query: {
          select:
            "station_id,next_due_at,last_observed_at,ingest_lag_samples,last_polled_at",
          station_id: postgrestIn(stationIds),
          order: "station_id.asc",
          limit: String(limit),
          offset: String(offset),
        },
      },
    );
    if (!response.ok) {
      throw new Error(
        `Failed to load SOS station checkpoints (${response.status}): ${response.text}`,
      );
    }
    const rows = Array.isArray(response.data) ? response.data : [];
    for (const row of rows) {
      const obj = toObject(row);
      const stationId = toIntegerOrNull(obj?.station_id);
      if (stationId === null) {
        continue;
      }
      const rawSamples = Array.isArray(obj?.ingest_lag_samples)
        ? obj?.ingest_lag_samples
        : [];
      const lagSamples = rawSamples
        .map((sample) => toIntegerOrNull(sample))
        .filter(
          (sample): sample is number => sample !== null && sample >= 0,
        );
      checkpoints.set(stationId, {
        station_id: stationId,
        next_due_at: toStringOrNull(obj?.next_due_at),
        last_observed_at: toStringOrNull(obj?.last_observed_at),
        ingest_lag_samples: lagSamples,
        last_polled_at: toStringOrNull(obj?.last_polled_at),
      });
    }
    if (rows.length < limit) {
      break;
    }
    offset += limit;
  }

  return checkpoints;
}

function appendSample(samples: number[], sample: number): number[] {
  const next = [...samples, sample];
  if (next.length <= CHECKPOINT_LAG_SAMPLE_MAX) {
    return next;
  }
  return next.slice(next.length - CHECKPOINT_LAG_SAMPLE_MAX);
}

function minSeconds(samples: number[]): number | null {
  if (!samples.length) {
    return null;
  }
  return samples.reduce((acc, current) => Math.min(acc, current), samples[0]);
}

function maxTimestampIso(a: string | null, b: string | null): string | null {
  if (!a) return b;
  if (!b) return a;
  const aMs = Date.parse(a);
  const bMs = Date.parse(b);
  if (!Number.isFinite(aMs)) return b;
  if (!Number.isFinite(bMs)) return a;
  return bMs > aMs ? b : a;
}

function buildStationCheckpointRows(
  stationRows: StationRow[],
  latestByStation: Map<number, string>,
  checkpointByStation: Map<number, StationCheckpointRow>,
  now: Date,
): Array<Record<string, unknown>> {
  const nowIso = now.toISOString();
  return stationRows.map((station) => {
    const current = checkpointByStation.get(station.id);
    const previousLastObserved = current?.last_observed_at ?? null;
    const latestObserved = latestByStation.get(station.id) ?? null;
    const updatedLastObserved = maxTimestampIso(previousLastObserved, latestObserved);
    const stationHasNewObservation = Boolean(
      latestObserved &&
      (!previousLastObserved || Date.parse(latestObserved) > Date.parse(previousLastObserved)),
    );

    let lagSamples = current?.ingest_lag_samples ?? [];
    let nextDueAt = current?.next_due_at ?? null;

    if (stationHasNewObservation && updatedLastObserved) {
      const lagSeconds = Math.max(
        0,
        Math.round((now.getTime() - Date.parse(updatedLastObserved)) / 1000),
      );
      if (Number.isFinite(lagSeconds)) {
        lagSamples = appendSample(lagSamples, lagSeconds);
      }
      if (lagSamples.length < 10) {
        nextDueAt = new Date(now.getTime() + CHECKPOINT_WARMUP_SECONDS * 1000).toISOString();
      } else {
        const lagSecondsMin = minSeconds(lagSamples) ?? CHECKPOINT_WARMUP_SECONDS;
        const baseMs = Date.parse(updatedLastObserved);
        if (Number.isFinite(baseMs)) {
          nextDueAt = new Date(
            baseMs + (CHECKPOINT_BASE_PERIOD_SECONDS + lagSecondsMin) * 1000,
          ).toISOString();
        } else {
          nextDueAt = nowIso;
        }
      }
    } else if (!nextDueAt) {
      if (updatedLastObserved && Number.isFinite(Date.parse(updatedLastObserved))) {
        nextDueAt = new Date(
          Date.parse(updatedLastObserved) + CHECKPOINT_BASE_PERIOD_SECONDS * 1000,
        ).toISOString();
      } else {
        nextDueAt = new Date(now.getTime() + CHECKPOINT_WARMUP_SECONDS * 1000).toISOString();
      }
    }

    return {
      station_id: station.id,
      next_due_at: nextDueAt,
      last_observed_at: updatedLastObserved,
      ingest_lag_samples: lagSamples,
      last_polled_at: nowIso,
      updated_at: nowIso,
    };
  });
}

async function upsertStationCheckpoints(
  rows: Array<Record<string, unknown>>,
): Promise<void> {
  if (!rows.length) {
    return;
  }
  const response = await postgrestRequest(
    "POST",
    "sos_station_checkpoints",
    {
      schema: UK_AQ_RAW_SCHEMA,
      query: { on_conflict: "station_id" },
      body: rows,
      prefer: "resolution=merge-duplicates,return=minimal",
    },
  );
  if (!response.ok) {
    throw new Error(
      `Failed to upsert SOS station checkpoints (${response.status}): ${response.text}`,
    );
  }
}

async function buildIngestPayload(
  connector: ConnectorConfig | null,
): Promise<{
  payload: Record<string, unknown>;
  windowHours: number;
  stationBatchLimit: number;
  staleLimit: number;
  timeseriesLimit: number;
  stationRows: StationRow[];
  timeseriesRows: SelectedTimeseriesRow[];
  timeseriesIds: string[];
}> {
  const payload: Record<string, unknown> = {
    ...REQUEST_PAYLOAD_OVERRIDES,
  };
  const connectorCode = toStringOrNull(payload.connector_code) || CONNECTOR_CODE;
  const windowHours = getWindowHours(connector);
  const timeseriesLimit = getTimeseriesLimit(connector);
  const stationBatchLimit =
    toPositiveIntegerOrNull(payload.station_batch_limit) ?? getStationBatchLimit(connector);
  const staleLimit = toPositiveIntegerOrNull(payload.stale_limit) ?? DEFAULT_STALE_LIMIT;
  const bridgeDayUtc = new Date().toISOString().slice(0, 10);
  const selectedWork = await loadSelectedWork({
    batchLimit: stationBatchLimit,
    staleLimit,
    timeseriesLimit,
    dayUtc: bridgeDayUtc,
  });
  const stationRows = selectedWork.stationRows;
  const timeseriesRows = selectedWork.timeseriesRows;
  const timeseriesIds = timeseriesRows.map((row) => String(row.id));

  payload.connector_code = connectorCode;
  payload.window_hours = windowHours;
  payload.timeseries_limit = timeseriesLimit;
  payload.timeseries_ids = timeseriesIds;
  payload.selected_timeseries = selectedWork.selectedTimeseries;
  payload.uk_air_html_bridge_rows = selectedWork.bridgeRows;
  payload.uk_air_html_bridge_day_utc = bridgeDayUtc;

  return {
    payload,
    windowHours,
    stationBatchLimit,
    staleLimit,
    timeseriesLimit,
    stationRows,
    timeseriesRows,
    timeseriesIds,
  };
}

function deriveRunSummary(ingestResponse: IngestResponse): {
  runStatus: string;
  runMessage: string;
  payload: Record<string, unknown> | null;
} {
  const payload = toObject(ingestResponse.body);
  const rawStatus = toStringOrNull(payload?.run_status) ||
    (ingestResponse.ok ? "success" : "failed");
  let runStatus = rawStatus === "success" ? "succeeded" : rawStatus;

  const partial = payload?.partial === true;
  const stoppedReason = toStringOrNull(payload?.stopped_reason);
  if (partial || stoppedReason === "runtime_budget_exceeded") {
    runStatus = "partial";
  }

  if (isRecognizedSosDependencyFailure(ingestResponse)) {
    return {
      runStatus: "failed",
      runMessage: describeSosDependencyFailure(ingestResponse),
      payload,
    };
  }

  let runMessage = toStringOrNull(payload?.run_message);
  if (!runMessage && stoppedReason) {
    runMessage = stoppedReason;
  }
  if (!runMessage) {
    if (ingestResponse.ok) {
      runMessage = "ingest_sos completed via google_cloud_run";
    } else {
      runMessage =
        `ingest_sos failed with status ${ingestResponse.status}`;
    }
  }

  return { runStatus, runMessage, payload };
}

async function resolveConnectorId(
  payload: Record<string, unknown> | null,
): Promise<number> {
  const payloadConnectorId = toIntegerOrNull(payload?.connector_id);
  if (payloadConnectorId !== null) {
    return payloadConnectorId;
  }

  const response = await postgrestRequest("GET", "connectors", {
    query: {
      connector_code: `eq.${CONNECTOR_CODE}`,
      select: "id",
      limit: "1",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to resolve connector id (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data) ? response.data : [];
  const id = toIntegerOrNull(toObject(rows[0])?.id);
  if (id === null) {
    throw new Error(`Connector not found: ${CONNECTOR_CODE}`);
  }
  return id;
}

async function loadConnector(): Promise<ConnectorConfig | null> {
  const response = await postgrestRequest("GET", "connectors", {
    query: {
      select:
        "id,connector_code,poll_enabled,poll_interval_minutes,poll_window_hours,poll_timeseries_batch_size,scheduler_backend,last_polled_at,last_run_start,last_run_end,last_run_status",
      connector_code: `eq.${CONNECTOR_CODE}`,
      limit: "1",
    },
    transientJwtFutureRetry: {
      operation: "load_connector",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to load connector (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data) ? response.data : [];
  const row = toObject(rows[0]);
  return row as ConnectorConfig | null;
}

async function claimConnector(
  runStartedAtIso: string,
): Promise<DispatchClaimRow | null> {
  const response = await postgrestRequest(
    "POST",
    "rpc/uk_aq_rpc_dispatch_claim",
    {
      schema: "uk_aq_public",
      body: {
        p_connector_code: CONNECTOR_CODE,
        p_run_started_at: runStartedAtIso,
        p_timeout_minutes: CLAIM_TIMEOUT_MINUTES,
      },
    },
  );
  if (!response.ok) {
    throw new Error(
      `Dispatch claim failed (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data) ? response.data : [];
  const row = toObject(rows[0]);
  return row as DispatchClaimRow | null;
}

async function updateConnectorRun(
  connectorId: number,
  runStartedAtIso: string,
  runEndedAtIso: string,
  runStatus: string,
  runMessage: string,
): Promise<void> {
  const body: Record<string, unknown> = {
    last_run_start: runStartedAtIso,
    last_run_end: runEndedAtIso,
    last_run_status: runStatus,
    last_run_message: runMessage,
  };
  if (runStatus === "succeeded" || runStatus === "success" || runStatus === "partial") {
    body.last_polled_at = runStartedAtIso;
  }
  const response = await postgrestRequest("PATCH", "connectors", {
    query: { id: `eq.${connectorId}` },
    body,
    prefer: "return=minimal",
  });
  if (!response.ok) {
    throw new Error(
      `Failed to update connector run (${response.status}): ${response.text}`,
    );
  }
}

async function insertRunRow(
  connectorId: number,
  runStartedAtIso: string,
  runEndedAtIso: string,
  runStatus: string,
  runMessage: string,
  ingestResponse: IngestResponse,
  payload: Record<string, unknown> | null,
  stationsFallback: number | null,
  lastObservedFallback: string | null,
): Promise<void> {
  const row = {
    connector_id: connectorId,
    connector_code: CONNECTOR_CODE,
    run_started_at: runStartedAtIso,
    run_ended_at: runEndedAtIso,
    run_status: runStatus,
    run_message: runMessage,
    last_observed_at: toStringOrNull(payload?.last_observed_at) ||
      toStringOrNull(payload?.last_observed) ||
      lastObservedFallback,
    stations_updated: toIntegerOrNull(payload?.stations_updated) ??
      toIntegerOrNull(payload?.stations) ??
      stationsFallback,
    observations_upserted: toIntegerOrNull(payload?.observations_upserted) ??
      toIntegerOrNull(payload?.observations),
    timeseries_updated: toIntegerOrNull(payload?.timeseries_updated) ??
      toIntegerOrNull(payload?.timeseries),
    series_polled: toIntegerOrNull(payload?.series_polled) ??
      toIntegerOrNull(payload?.timeseries) ??
      toIntegerOrNull(payload?.timeseries_updated),
    response_status: ingestResponse.status,
    response_payload: payload,
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

async function insertErrorLog(
  connectorId: number,
  ingestResponse: IngestResponse,
): Promise<{ errorId: string; createdAtIso: string; row: Record<string, unknown> }> {
  const errorId = crypto.randomUUID();
  const createdAtIso = new Date().toISOString();
  const entry = {
    id: errorId,
    created_at: createdAtIso,
    source: "cloud_run",
    severity: "error",
    message: "ingest_sos dispatch failed",
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

function buildWrapperFailureResponse(errorMessage: string): IngestResponse {
  return {
    ok: false,
    status: 500,
    body: {
      error: "cloud_run_wrapper_failed",
      message: errorMessage,
    },
    raw: errorMessage,
  };
}

async function patchErrorLogDropboxPath(
  errorId: string,
  dropboxPath: string,
): Promise<void> {
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

async function uploadErrorLogRowToDropbox(
  errorId: string,
  createdAtIso: string,
  row: Record<string, unknown>,
): Promise<string | null> {
  const dropboxPath = await uploadErrorLogJsonToDropbox({
    appKey: DROPBOX_APP_KEY,
    appSecret: DROPBOX_APP_SECRET,
    refreshToken: DROPBOX_REFRESH_TOKEN,
    allowedSupabaseUrl: DROPBOX_ERROR_ALLOWED_SUPABASE_URL,
    supabaseUrl: SUPABASE_URL,
    dropboxRoot: UK_AQ_DROPBOX_ROOT,
    errorFolder: DROPBOX_ERROR_FOLDER,
    errorId,
    createdAtIso,
    connectorCode: CONNECTOR_CODE,
    payload: {
      ...row,
      connector_code: CONNECTOR_CODE,
    },
  });
  if (!dropboxPath) {
    return null;
  }
  await patchErrorLogDropboxPath(errorId, dropboxPath);
  return dropboxPath;
}

async function waitForServer(url: string, maxWaitMs = 30_000): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < maxWaitMs) {
    try {
      const response = await fetch(url, { method: "GET" });
      if (response.status > 0) {
        return;
      }
    } catch {
      // wait and retry
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("Timed out waiting for local SOS server startup.");
}

function logSummary(message: string, details: Record<string, unknown>): void {
  console.log(
    JSON.stringify({
      ts: new Date().toISOString(),
      connector_code: CONNECTOR_CODE,
      message,
      ...details,
    }),
  );
}

async function writeChildResult(result: SosCloudRunChildResult): Promise<void> {
  if (!SOS_RUN_RESULT_PATH) {
    return;
  }
  await Deno.writeTextFile(SOS_RUN_RESULT_PATH, JSON.stringify(result));
}

async function runIngestOnce(
  payload: Record<string, unknown>,
): Promise<IngestResponse> {
  const headers: HeadersInit = {
    "content-type": "application/json",
  };
  if (CRON_SECRET) {
    headers["x-cron-secret"] = CRON_SECRET;
  }
  const response = await fetch(`http://127.0.0.1:${PORT}/`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
  const raw = await response.text();
  let body: unknown = raw;
  if (raw) {
    try {
      body = JSON.parse(raw);
    } catch {
      body = raw;
    }
  }
  return {
    ok: response.ok,
    status: response.status,
    body,
    raw,
  };
}

async function recordSkippedRun(
  connectorId: number,
  runStartedAtIso: string,
  runMessage: string,
): Promise<void> {
  const runEndedAtIso = new Date().toISOString();
  const runStatus = "skipped";
  const ingestResponse: IngestResponse = {
    ok: true,
    status: 204,
    body: {
      run_status: runStatus,
      run_message: runMessage,
      connector_code: CONNECTOR_CODE,
    },
    raw: "",
  };
  await updateConnectorRun(
    connectorId,
    runStartedAtIso,
    runEndedAtIso,
    runStatus,
    runMessage,
  );
  await insertRunRow(
    connectorId,
    runStartedAtIso,
    runEndedAtIso,
    runStatus,
    runMessage,
    ingestResponse,
    toObject(ingestResponse.body),
    null,
    null,
  );
}

async function main(): Promise<void> {
  const now = new Date();
  const runStartedAtIso = now.toISOString();
  const connector = await loadConnector();
  const due = evaluateDue(connector, now);
  if (!due.due) {
    await writeChildResult(
      buildSosCloudRunSkippedResult(
        due.reason,
        toIntegerOrNull(connector?.id),
      ),
    );
    logSummary("skipped", {
      reason: due.reason,
      interval_minutes: due.intervalMinutes,
      poll_enabled: connector?.poll_enabled,
      scheduler_backend: toStringOrNull(connector?.scheduler_backend) ||
        SCHEDULER_BACKEND_SUPABASE_FUNCTION,
    });
    return;
  }

  const claim = await claimConnector(runStartedAtIso);
  if (!claim || claim.claimed !== true) {
    await writeChildResult(
      buildSosCloudRunSkippedResult(
        "claim_not_acquired",
        toIntegerOrNull(claim?.connector_id) ??
          toIntegerOrNull(connector?.id),
      ),
    );
    logSummary("skipped", {
      reason: "claim_not_acquired",
      claim,
    });
    return;
  }

  const claimedConnectorId = toIntegerOrNull(claim.connector_id);
  let connectorId: number | null = claimedConnectorId ??
    toIntegerOrNull(connector?.id);
  if (connectorId === null) {
    connectorId = await resolveConnectorId(null);
  }

  let server: Deno.ChildProcess | null = null;
  let ingestResponse: IngestResponse | null = null;

  try {
    const payloadPlan = await buildIngestPayload(connector);

    if (!payloadPlan.stationRows.length) {
      await recordSkippedRun(connectorId, runStartedAtIso, "no_station_refs");
      await writeChildResult(
        buildSosCloudRunSkippedResult("no_station_refs", connectorId),
      );
      logSummary("skipped", {
        reason: "no_station_refs",
        connector_id: connectorId,
        station_batch_limit: payloadPlan.stationBatchLimit,
        stale_limit: payloadPlan.staleLimit,
      });
      return;
    }

    if (!payloadPlan.timeseriesIds.length) {
      await recordSkippedRun(connectorId, runStartedAtIso, "no_timeseries_ids");
      await writeChildResult(
        buildSosCloudRunSkippedResult("no_timeseries_ids", connectorId),
      );
      logSummary("skipped", {
        reason: "no_timeseries_ids",
        connector_id: connectorId,
        stations_selected: payloadPlan.stationRows.length,
        timeseries_limit: payloadPlan.timeseriesLimit,
      });
      return;
    }

    logSummary("dispatching", {
      connector_id: connectorId,
      window_hours: payloadPlan.windowHours,
      station_batch_limit: payloadPlan.stationBatchLimit,
      stale_limit: payloadPlan.staleLimit,
      stations_selected: payloadPlan.stationRows.length,
      timeseries_limit: payloadPlan.timeseriesLimit,
      timeseries_selected: payloadPlan.timeseriesIds.length,
    });

    server = new Deno.Command("deno", {
      args: [
        "run",
        "--allow-env",
        "--allow-net",
        "--allow-read",
        "--allow-write",
        SOS_INGEST_SCRIPT_PATH,
      ],
      env: {
        ...Deno.env.toObject(),
        SOS_DROPBOX_UPLOAD_SOURCE: "cloud_run",
      },
      stdout: "inherit",
      stderr: "inherit",
    }).spawn();

    await waitForServer(`http://127.0.0.1:${PORT}/`);
    const attemptedAtIso = new Date().toISOString();
    const stationsAttempted = await recordStationAttempts(
      payloadPlan.timeseriesRows,
      attemptedAtIso,
    );
    logSummary("attempts_recorded", {
      connector_id: connectorId,
      stations_attempted: stationsAttempted,
      attempted_at: attemptedAtIso,
    });
    ingestResponse = await runIngestOnce(payloadPlan.payload);

    const { runStatus, runMessage, payload } = deriveRunSummary(ingestResponse);
    const childResult = buildSosCloudRunChildResult(
      ingestResponse,
      runStatus,
      runMessage,
    );
    if (!childResult) {
      throw new Error("Malformed SOS ingest response.");
    }
    const recognizedDependencyFailure = isRecognizedSosDependencyFailure(
      ingestResponse,
    );
    const runEndedAtIso = new Date().toISOString();
    const maxTimeseriesLastObservedAt = await fetchMaxTimeseriesLastValueAt(
      payloadPlan.timeseriesIds,
    );

    if ((runStatus === "succeeded" || runStatus === "partial") && payloadPlan.stationRows.length) {
      const checkpointNow = new Date();
      const latestByStation = await loadStationLatestObserved(
        connectorId,
        payloadPlan.stationRows.map((row) => row.id),
      );
      const checkpointByStation = await loadStationCheckpointRows(
        payloadPlan.stationRows.map((row) => row.id),
      );
      const recoveredFallback =
        toStringOrNull(payload?.upstream_failure_kind) !== null;
      let checkpointStationRows = payloadPlan.stationRows;
      if (recoveredFallback) {
        const successfullyPolledTimeseriesIds =
          await loadSuccessfullyPolledTimeseriesIds(
            payloadPlan.timeseriesIds,
            runStartedAtIso,
          );
        const successfullyPolledStationIds = new Set(
          payloadPlan.timeseriesRows
            .filter((row) => successfullyPolledTimeseriesIds.has(row.id))
            .map((row) => row.station_id),
        );
        checkpointStationRows = payloadPlan.stationRows.filter((station) =>
          successfullyPolledStationIds.has(station.id)
        );
      }
      const checkpointRows = buildStationCheckpointRows(
        checkpointStationRows,
        latestByStation,
        checkpointByStation,
        checkpointNow,
      );
      await upsertStationCheckpoints(checkpointRows);
    }

    await updateConnectorRun(
      connectorId,
      runStartedAtIso,
      runEndedAtIso,
      runStatus,
      runMessage,
    );

    await insertRunRow(
      connectorId,
      runStartedAtIso,
      runEndedAtIso,
      runStatus,
      runMessage,
      ingestResponse,
      payload,
      payloadPlan.stationRows.length,
      maxTimeseriesLastObservedAt,
    );

    if (!isCompletedSosChildResponse(ingestResponse) ||
      (!recognizedDependencyFailure &&
        (runStatus === "failed" || runStatus === "error"))) {
      throw new Error(
        `ingest_sos failed (${ingestResponse.status}): ${ingestResponse.raw}`,
      );
    }

    await writeChildResult(childResult);

    logSummary("success", {
      run_status: runStatus,
      response_status: ingestResponse.status,
      connector_id: connectorId,
      stations_selected: payloadPlan.stationRows.length,
      series_polled: toIntegerOrNull(payload?.series_polled),
      observations_upserted: toIntegerOrNull(payload?.observations_upserted),
      partial: payload?.partial === true,
      stopped_reason: toStringOrNull(payload?.stopped_reason),
      recognized_dependency_failure: recognizedDependencyFailure,
    });
  } catch (error) {
    const errorMessage = shortError(error);
    const runEndedAtIso = new Date().toISOString();
    try {
      await updateConnectorRun(
        connectorId,
        runStartedAtIso,
        runEndedAtIso,
        "failed",
        errorMessage,
      );
    } catch (updateError) {
      logSummary("connector_update_failed", {
        error: shortError(updateError),
      });
    }

    try {
      const inserted = await insertErrorLog(
        connectorId,
        ingestResponse ?? buildWrapperFailureResponse(errorMessage),
      );
      await uploadErrorLogRowToDropbox(
        inserted.errorId,
        inserted.createdAtIso,
        inserted.row,
      );
    } catch (loggingError) {
      logSummary("dropbox_error_upload_warning", {
        error: shortError(loggingError),
      });
    }

    throw error;
  } finally {
    try {
      server?.kill("SIGTERM");
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logSummary("server_kill_failed", { error: message });
    }
    try {
      await Promise.race([
        server?.status ?? Promise.resolve(undefined),
        new Promise((resolve) => setTimeout(resolve, 5000)),
      ]);
    } catch {
      // Ignore shutdown race.
    }
  }

  if (!ingestResponse) {
    throw new Error("No SOS ingest response received.");
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logSummary("failure", { error: message });
  Deno.exit(1);
});
