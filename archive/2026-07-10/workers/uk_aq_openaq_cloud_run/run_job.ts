import "../../supabase/functions/_shared/fetch_egress_patch.ts";
import {
  normalizeDropboxPath,
  uploadErrorLogJsonToDropbox,
} from "../shared/dropbox_error_log.ts";

const CONNECTOR_CODE = (Deno.env.get("OPENAQ_CONNECTOR_CODE") || "openaq")
  .trim();
const OPENAQ_SERVICE_REF =
  (Deno.env.get("OPENAQ_SERVICE_REF") || CONNECTOR_CODE).trim();
const SCHEDULER_BACKEND_SUPABASE_FUNCTION = "supabase_function";
const SCHEDULER_BACKEND_GOOGLE_CLOUD_RUN = "google_cloud_run";

const IN_FLIGHT_TIMEOUT_MINUTES = parsePositiveInt(
  Deno.env.get("OPENAQ_IN_FLIGHT_TIMEOUT_MINUTES"),
  30,
);
const CLAIM_TIMEOUT_MINUTES = parsePositiveInt(
  Deno.env.get("OPENAQ_CLAIM_TIMEOUT_MINUTES"),
  30,
);
const DEFAULT_WINDOW_HOURS = parsePositiveInt(
  Deno.env.get("OPENAQ_DEFAULT_WINDOW_HOURS"),
  6,
);
const DEFAULT_BATCH_LIMIT = parsePositiveInt(
  Deno.env.get("OPENAQ_DEFAULT_BATCH_LIMIT"),
  56,
);
const DEFAULT_STALE_LIMIT = parsePositiveInt(
  Deno.env.get("OPENAQ_STALE_LIMIT"),
  4,
);
const DEFAULT_TIER1_RETRY_SECONDS = parsePositiveInt(
  Deno.env.get("OPENAQ_TIER1_RETRY_SECONDS"),
  300,
);
const PORT = parsePositiveInt(
  Deno.env.get("OPENAQ_LOCAL_PORT") || Deno.env.get("PORT"),
  8000,
);
const REQUEST_PAYLOAD_RAW = (Deno.env.get("OPENAQ_REQUEST_PAYLOAD") || "{}")
  .trim();
const REQUEST_PAYLOAD_OVERRIDES = parseRequestPayload(REQUEST_PAYLOAD_RAW);
const CRON_SECRET = (Deno.env.get("SB_UK_AQ_CRON_SECRET") || "").trim();

const OPENAQ_TASKS_ENABLED = parseBool(
  Deno.env.get("OPENAQ_TASKS_ENABLED"),
  true,
);
const OPENAQ_TRIGGER_MODE = parseTriggerMode(
  Deno.env.get("OPENAQ_TRIGGER_MODE"),
);
const OPENAQ_SAFETY_SUCCESS_LOOKBACK_MINUTES = parsePositiveInt(
  Deno.env.get("OPENAQ_SAFETY_SUCCESS_LOOKBACK_MINUTES"),
  10,
);
const OPENAQ_NEXT_CHECK_MIN_SECONDS = parsePositiveInt(
  Deno.env.get("OPENAQ_NEXT_CHECK_MIN_SECONDS"),
  60,
);
const OPENAQ_NEXT_CHECK_PARTIAL_MIN_SECONDS = parsePositiveInt(
  Deno.env.get("OPENAQ_NEXT_CHECK_PARTIAL_MIN_SECONDS"),
  OPENAQ_NEXT_CHECK_MIN_SECONDS,
);
const OPENAQ_NEXT_CHECK_SKIPPED_MIN_SECONDS = parsePositiveInt(
  Deno.env.get("OPENAQ_NEXT_CHECK_SKIPPED_MIN_SECONDS"),
  OPENAQ_NEXT_CHECK_MIN_SECONDS,
);
const OPENAQ_RATE_LIMIT_FALLBACK_SECONDS = parsePositiveInt(
  Deno.env.get("OPENAQ_RATE_LIMIT_FALLBACK_SECONDS"),
  300,
);
const OPENAQ_MAX_REQUESTS_PER_HOUR = parsePositiveInt(
  Deno.env.get("OPENAQ_MAX_REQUESTS_PER_HOUR"),
  1900,
);
const OPENAQ_AUTH_SAFETY_DISABLE_POLLING = parseBool(
  Deno.env.get("OPENAQ_AUTH_SAFETY_DISABLE_POLLING"),
  true,
);
const OPENAQ_FAILURE_RETRY_SECONDS = parsePositiveInt(
  Deno.env.get("OPENAQ_FAILURE_RETRY_SECONDS"),
  120,
);
const OPENAQ_TASK_NAME_PREFIX =
  (Deno.env.get("OPENAQ_TASK_NAME_PREFIX") || "uk-aq-openaq-next").trim();
const OPENAQ_CLOUD_RUN_TARGET = parseRunTarget(
  Deno.env.get("OPENAQ_CLOUD_RUN_TARGET"),
);
const OPENAQ_GCP_PROJECT_ID = (
  Deno.env.get("OPENAQ_GCP_PROJECT_ID") ||
  Deno.env.get("GCP_PROJECT_ID") ||
  Deno.env.get("GOOGLE_CLOUD_PROJECT") ||
  ""
).trim();
const OPENAQ_GCP_REGION = (
  Deno.env.get("OPENAQ_GCP_REGION") ||
  Deno.env.get("GCP_REGION") ||
  "europe-west2"
).trim();
const OPENAQ_CLOUD_RUN_JOB_NAME = (
  Deno.env.get("OPENAQ_CLOUD_RUN_JOB_NAME") ||
  Deno.env.get("GCP_OPENAQ_JOB_NAME") ||
  "uk-aq-openaq-ingest"
).trim();
const OPENAQ_CLOUD_RUN_SERVICE_NAME = (
  Deno.env.get("OPENAQ_CLOUD_RUN_SERVICE_NAME") ||
  Deno.env.get("GCP_OPENAQ_SERVICE_NAME") ||
  OPENAQ_CLOUD_RUN_JOB_NAME
).trim();
const OPENAQ_CLOUD_RUN_SERVICE_URL = (
  Deno.env.get("OPENAQ_CLOUD_RUN_SERVICE_URL") || ""
).trim();
const OPENAQ_TASK_QUEUE_ID = (
  Deno.env.get("OPENAQ_TASK_QUEUE_ID") ||
  Deno.env.get("GCP_OPENAQ_TASK_QUEUE_ID") ||
  "uk-aq-openaq-trigger-queue"
).trim();
const OPENAQ_TASK_INVOKER_SERVICE_ACCOUNT = (
  Deno.env.get("OPENAQ_TASK_INVOKER_SERVICE_ACCOUNT") ||
  Deno.env.get("GCP_OPENAQ_TASK_INVOKER_SERVICE_ACCOUNT") ||
  Deno.env.get("GCP_OPENAQ_SCHEDULER_SERVICE_ACCOUNT") ||
  ""
).trim();
const OPENAQ_CURRENT_TASK_NAME =
  (Deno.env.get("OPENAQ_CURRENT_TASK_NAME") || "").trim() || null;
const OPENAQ_INGEST_SCRIPT_PATH = (Deno.env.get("OPENAQ_INGEST_SCRIPT_PATH") ||
  "/app/runtime/ingest_openaq/index.ts").trim();

const SUPABASE_URL = requiredEnv("SUPABASE_URL");
const SUPABASE_PRIVILEGED_KEY = requiredEnvAny(["SB_SECRET_KEY"]);
const UK_AQ_CORE_SCHEMA = (Deno.env.get("UK_AQ_CORE_SCHEMA") || "uk_aq_core")
  .trim();
const UK_AQ_RAW_SCHEMA = (Deno.env.get("UK_AQ_RAW_SCHEMA") || "uk_aq_raw")
  .trim();
const REST_BASE_URL = `${SUPABASE_URL.replace(/\/$/, "")}/rest/v1`;
const UK_AQ_DROPBOX_ROOT = normalizeDropboxPath(
  Deno.env.get("UK_AQ_DROPBOX_ROOT") ?? "",
);
const DROPBOX_APP_KEY = (Deno.env.get("DROPBOX_APP_KEY") || "").trim();
const DROPBOX_APP_SECRET = (Deno.env.get("DROPBOX_APP_SECRET") || "").trim();
const DROPBOX_REFRESH_TOKEN = (Deno.env.get("DROPBOX_REFRESH_TOKEN") || "")
  .trim();
const DROPBOX_ERROR_ALLOWED_SUPABASE_URL = (
  Deno.env.get("OPENAQ_ERROR_DROPBOX_ALLOWED_SUPABASE_URL") ??
    Deno.env.get("OPENAQ_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ??
    Deno.env.get("UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL") ??
    Deno.env.get("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ??
    ""
).trim();
const DROPBOX_ERROR_FOLDER = (
  Deno.env.get("OPENAQ_ERROR_DROPBOX_FOLDER") ??
    "/error_log"
).trim();

type IngestResponse = {
  ok: boolean;
  status: number;
  body: unknown;
  raw: string;
};

type IngestPayloadSummary = Record<string, unknown> & {
  connector_id?: number | string | null;
  run_status?: string;
  run_message?: string;
  partial?: boolean;
  stopped_reason?: string;
  rate_limit_stop?: boolean;
  rate_limit_stop_reason?: string;
  rate_limit_reset?: number | string | null;
  rate_limit_reset_at?: string;
};

type RunSummary = {
  runStatus: string;
  runMessage: string;
  payload: IngestPayloadSummary | null;
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

type OpenAQStationRefRow = {
  station_ref: unknown;
  station_id: unknown;
};

type OpenAQHourlyUsageRow = {
  run_started_at: unknown;
  response_payload: unknown;
};

type PendingOpenAQTask = {
  name: string;
  scheduleAt: string;
  scheduleAtMs: number;
};

type OpenAQHourlyBudgetState = {
  cap: number;
  used: number;
  remaining: number;
  resetAt: string | null;
  rowsConsidered: number;
  windowStartIso: string;
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

function parseBool(raw: string | undefined, fallback = false): boolean {
  if (raw === undefined || raw === null) {
    return fallback;
  }
  const value = String(raw).trim().toLowerCase();
  if (!value) {
    return fallback;
  }
  return ["1", "true", "yes", "y", "on"].includes(value);
}

function parseTriggerMode(
  raw: string | undefined,
): "safety" | "task" | "manual" | "unknown" {
  const value = (raw || "").trim().toLowerCase();
  if (value === "safety" || value === "task" || value === "manual") {
    return value;
  }
  return "unknown";
}

function parseRunTarget(raw: string | undefined): "job" | "service" {
  const value = (raw || "").trim().toLowerCase();
  if (value === "service") {
    return "service";
  }
  return "job";
}

function shortError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function parseRequestPayload(raw: string): Record<string, unknown> {
  if (!raw) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new Error("OPENAQ_REQUEST_PAYLOAD must be valid JSON.");
  }
  const payload = toObject(parsed);
  if (!payload) {
    throw new Error("OPENAQ_REQUEST_PAYLOAD must be a JSON object.");
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

function isRateLimitReason(reason: string | null): boolean {
  if (!reason) {
    return false;
  }
  return [
    "remaining_low",
    "rate_limit_429",
    "rate_limit_guard",
    "request_budget_limited",
    "max_requests_per_run",
    "hourly_rate_limit_guard",
    "shared_budget_minute_limit",
    "shared_budget_hour_limit",
    "shared_budget_rpc_error",
    "shared_budget_rpc_empty",
  ].includes(reason);
}

function isAuthStopReason(reason: string | null): boolean {
  if (!reason) {
    return false;
  }
  return reason === "auth_401" || reason === "auth_403";
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

function evaluateEligibility(connector: ConnectorConfig | null, now: Date): {
  eligible: boolean;
  reason: string;
} {
  if (!connector) {
    return { eligible: false, reason: "connector_not_found" };
  }
  if (connector.poll_enabled !== true) {
    return { eligible: false, reason: "poll_disabled" };
  }
  const schedulerBackend = toStringOrNull(connector.scheduler_backend) ||
    SCHEDULER_BACKEND_SUPABASE_FUNCTION;
  if (schedulerBackend !== SCHEDULER_BACKEND_GOOGLE_CLOUD_RUN) {
    return { eligible: false, reason: "scheduler_backend_not_cloud_run" };
  }
  const runStartedAt = parseTimestamp(connector.last_run_start);
  const runEndedAt = parseTimestamp(connector.last_run_end);
  if (runStartedAt && !runEndedAt) {
    const runningGuardMs = IN_FLIGHT_TIMEOUT_MINUTES * 60 * 1000;
    const ageMs = now.getTime() - runStartedAt.getTime();
    if (Number.isFinite(ageMs) && ageMs >= 0 && ageMs < runningGuardMs) {
      return { eligible: false, reason: "in_flight" };
    }
  }
  return { eligible: true, reason: "eligible" };
}

function getWindowHours(connector: ConnectorConfig | null): number {
  const value = toPositiveIntegerOrNull(connector?.poll_window_hours);
  if (value !== null) {
    return value;
  }
  return DEFAULT_WINDOW_HOURS;
}

function getBatchLimit(connector: ConnectorConfig | null): number {
  const value = toPositiveIntegerOrNull(connector?.poll_timeseries_batch_size);
  if (value !== null) {
    return value;
  }
  return DEFAULT_BATCH_LIMIT;
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
  } = {},
): Promise<{ ok: boolean; status: number; text: string; data: unknown }> {
  const schema = options.schema || UK_AQ_CORE_SCHEMA;
  const write = method !== "GET";
  const headers = postgrestHeaders(schema, write);
  if (options.prefer) {
    headers.Prefer = options.prefer;
  }
  const response = await fetch(withQuery(path, options.query), {
    method,
    headers,
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
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
}

async function loadStationRefs(params: {
  tieredLimit: number;
  staleLimit: number;
  tier1RetrySeconds: number;
}): Promise<Array<{ station_ref: string; station_id: number | null }>> {
  const bodyWithTier1 = {
    batch_limit: Math.max(0, Math.trunc(params.tieredLimit)),
    stale_limit: Math.max(0, Math.trunc(params.staleLimit)),
    tier1_retry_seconds: Math.max(0, Math.trunc(params.tier1RetrySeconds)),
  };
  const bodyLegacy = {
    batch_limit: Math.max(0, Math.trunc(params.tieredLimit)),
    stale_limit: Math.max(0, Math.trunc(params.staleLimit)),
  };
  let response = await postgrestRequest(
    "POST",
    "rpc/uk_aq_rpc_openaq_select_station_refs",
    {
      schema: "uk_aq_public",
      body: bodyWithTier1,
    },
  );
  if (!response.ok) {
    const fallback = await postgrestRequest(
      "POST",
      "rpc/uk_aq_rpc_openaq_select_station_refs",
      {
        schema: "uk_aq_public",
        body: bodyLegacy,
      },
    );
    if (fallback.ok) {
      response = fallback;
      console.warn(
        "OpenAQ station selection fallback to legacy RPC signature (missing tier1_retry_seconds parameter).",
      );
    }
  }
  if (!response.ok) {
    throw new Error(
      `Failed to load OpenAQ station refs (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data) ? response.data : [];
  return rows
    .map((row) => {
      const obj = toObject(row) as OpenAQStationRefRow | null;
      const stationRef = toStringOrNull(obj?.station_ref);
      const stationId = toIntegerOrNull(obj?.station_id);
      if (!stationRef) {
        return null;
      }
      return {
        station_ref: stationRef,
        station_id: stationId,
      };
    })
    .filter((row): row is { station_ref: string; station_id: number | null } =>
      row !== null
    );
}

async function loadEarliestNextDueAt(
  connectorId: number,
): Promise<string | null> {
  const primary = await postgrestRequest(
    "GET",
    "openaq_station_checkpoints",
    {
      schema: UK_AQ_RAW_SCHEMA,
      query: {
        select:
          "next_due_at,station_id,stations!inner(connector_id,service_ref,removed_at)",
        "stations.connector_id": `eq.${connectorId}`,
        "stations.service_ref": `eq.${OPENAQ_SERVICE_REF}`,
        "stations.removed_at": "is.null",
        next_due_at: "not.is.null",
        order: "next_due_at.asc",
        limit: "1",
      },
    },
  );
  if (primary.ok) {
    const rows = Array.isArray(primary.data) ? primary.data : [];
    const first = toObject(rows[0]);
    const nextDueAt = toStringOrNull(first?.next_due_at);
    if (nextDueAt) {
      return nextDueAt;
    }
  }

  const fallback = await postgrestRequest(
    "GET",
    "openaq_station_checkpoints",
    {
      schema: UK_AQ_RAW_SCHEMA,
      query: {
        select: "next_due_at",
        next_due_at: "not.is.null",
        order: "next_due_at.asc",
        limit: "1",
      },
    },
  );
  if (!fallback.ok) {
    throw new Error(
      `Failed to load OpenAQ next_due_at (${fallback.status}): ${fallback.text}`,
    );
  }
  const rows = Array.isArray(fallback.data) ? fallback.data : [];
  return toStringOrNull(toObject(rows[0])?.next_due_at);
}

async function buildIngestPayload(
  connector: ConnectorConfig | null,
  options: { batchLimitOverride?: number } = {},
): Promise<{
  payload: Record<string, unknown>;
  configuredBatchLimit: number;
  batchLimit: number;
  windowHours: number;
  staleLimit: number;
  tieredLimit: number;
  tier1RetrySeconds: number;
  stationRows: Array<{ station_ref: string; station_id: number | null }>;
}> {
  const payload: Record<string, unknown> = {
    ...REQUEST_PAYLOAD_OVERRIDES,
  };
  const connectorCode = toStringOrNull(payload.connector_code) ||
    CONNECTOR_CODE;
  const windowHours = getWindowHours(connector);
  const configuredBatchLimit = getBatchLimit(connector);
  const requestedOverride = toIntegerOrNull(options.batchLimitOverride);
  const batchLimit = requestedOverride === null
    ? configuredBatchLimit
    : Math.max(0, Math.min(configuredBatchLimit, requestedOverride));
  const staleLimitConfigured = toPositiveIntegerOrNull(payload.stale_limit) ??
    DEFAULT_STALE_LIMIT;
  const staleLimit = Math.min(staleLimitConfigured, Math.max(0, batchLimit));
  const tieredLimit = Math.max(0, batchLimit - staleLimit);
  const tier1RetrySeconds =
    toPositiveIntegerOrNull(payload.tier1_retry_seconds) ??
      DEFAULT_TIER1_RETRY_SECONDS;
  const stationRows = await loadStationRefs({
    tieredLimit,
    staleLimit,
    tier1RetrySeconds,
  });

  payload.connector_code = connectorCode;
  payload.window_hours = windowHours;
  payload.batch_size = batchLimit;
  payload.tier1_retry_seconds = tier1RetrySeconds;
  payload.station_refs = stationRows.map((row) => row.station_ref);

  return {
    payload,
    configuredBatchLimit,
    batchLimit,
    windowHours,
    staleLimit,
    tieredLimit,
    tier1RetrySeconds,
    stationRows,
  };
}

function deriveRunSummary(ingestResponse: IngestResponse): RunSummary {
  const payload = toObject(ingestResponse.body) as IngestPayloadSummary | null;
  if (!ingestResponse.ok) {
    return {
      runStatus: "failed",
      runMessage: `HTTP ${ingestResponse.status}`,
      payload,
    };
  }

  const explicitRunStatus = toStringOrNull(payload?.run_status)?.toLowerCase();
  if (explicitRunStatus === "skipped") {
    return {
      runStatus: "skipped",
      runMessage: toStringOrNull(payload?.run_message) ?? "skipped",
      payload,
    };
  }

  const partial = payload?.partial === true;
  const stoppedReason =
    toStringOrNull(payload?.stopped_reason)?.toLowerCase() ??
      null;
  const rateLimitStopReason = toStringOrNull(payload?.rate_limit_stop_reason)
    ?.toLowerCase() ?? null;
  const hourlyRateLimitGuard = stoppedReason === "hourly_rate_limit_guard" ||
    rateLimitStopReason === "hourly_rate_limit_guard";
  const rateLimitStop = stoppedReason === "remaining_low" ||
    stoppedReason === "rate_limit_429" ||
    stoppedReason === "rate_limit_guard" ||
    stoppedReason === "shared_budget_minute_limit" ||
    stoppedReason === "shared_budget_hour_limit" ||
    rateLimitStopReason === "remaining_low" ||
    rateLimitStopReason === "rate_limit_429" ||
    rateLimitStopReason === "rate_limit_guard" ||
    rateLimitStopReason === "shared_budget_minute_limit" ||
    rateLimitStopReason === "shared_budget_hour_limit";
  const requestsTotal = toIntegerOrNull(payload?.requests_total);
  const maxRequestsPerRun = toIntegerOrNull(payload?.max_requests_per_run);
  const gapRequestsSkippedBudget = toIntegerOrNull(
    payload?.gap_requests_skipped_budget,
  );
  const requestBudgetLimited =
    (gapRequestsSkippedBudget !== null && gapRequestsSkippedBudget > 0) ||
    (
      requestsTotal !== null &&
      maxRequestsPerRun !== null &&
      maxRequestsPerRun > 0 &&
      requestsTotal >= maxRequestsPerRun
    ) ||
    stoppedReason === "request_budget_limited" ||
    stoppedReason === "max_requests_per_run";

  if (hourlyRateLimitGuard) {
    return {
      runStatus: "skipped",
      runMessage: "Skipped - Hourly Limit",
      payload,
    };
  }

  if (rateLimitStop) {
    const reason = rateLimitStopReason || stoppedReason || "rate_limit_guard";
    return {
      runStatus: "partial",
      runMessage: reason,
      payload,
    };
  }

  if (requestBudgetLimited) {
    return {
      runStatus: "partial",
      runMessage: "request_budget_limited",
      payload,
    };
  }

  if (partial || stoppedReason !== null) {
    const reason = stoppedReason ||
      (isAuthStopReason(rateLimitStopReason) ? rateLimitStopReason : null) ||
      "partial_run";
    return {
      runStatus: "partial",
      runMessage: reason,
      payload,
    };
  }

  return {
    runStatus: "succeeded",
    runMessage: "ingest_openaq completed via google_cloud_run",
    payload,
  };
}

const STORED_RESPONSE_PAYLOAD_KEYS = [
  "partial",
  "stopped_reason",
  "rate_limit_stop",
  "rate_limit_stop_reason",
  "rate_limit_remaining",
  "rate_limit_limit",
  "rate_limit_reset",
  "rate_limit_reset_at",
  "shared_budget_enabled",
  "shared_budget_key",
  "shared_budget_caller",
  "shared_budget_minute_limit",
  "shared_budget_hour_limit",
  "shared_budget_granted",
  "shared_budget_reason",
  "shared_budget_requested_tokens",
  "shared_budget_minute_used_before",
  "shared_budget_minute_used_after",
  "shared_budget_minute_remaining",
  "shared_budget_minute_reset_at",
  "shared_budget_hour_used_before",
  "shared_budget_hour_used_after",
  "shared_budget_hour_remaining",
  "shared_budget_hour_reset_at",
  "shared_budget_retry_after_seconds",
  "requests_total",
  "max_requests_per_run",
  "lag_stat",
  "gap_requests_remaining_min",
  "gap_requests_planned",
  "gap_requests_executed",
  "gap_requests_skipped_budget",
  "gap_stations_total",
  "gap_stations_polled",
  "min_gap_stations",
  "non_gap_stations_selected",
  "min_non_gap_stations",
  "stations_selected",
  "stations_polled",
  "stations_updated",
  "observations_upserted",
  "observations_rows_input",
  "observations_rows_prepared",
  "observations_rows_deduped_prewrite",
  "observs_rows_prepared",
  "observs_rows_deduped_prewrite",
  "series_polled",
  "warnings",
  "event",
  "severity",
  "reason",
  "poll_enabled",
  "scheduler_backend",
  "self_reschedule_suppressed",
] as const;

function compactRunResponsePayload(
  payload: Record<string, unknown> | null,
): Record<string, unknown> | null {
  if (!payload) {
    return null;
  }
  const compact: Record<string, unknown> = {};
  for (const key of STORED_RESPONSE_PAYLOAD_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(payload, key)) {
      continue;
    }
    const value = payload[key];
    if (value !== undefined) {
      compact[key] = value;
    }
  }
  return Object.keys(compact).length ? compact : null;
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

async function loadRecentHourlyRequestBudget(
  now: Date,
): Promise<OpenAQHourlyBudgetState> {
  const windowStart = new Date(now.getTime() - 60 * 60 * 1000);
  const windowStartIso = windowStart.toISOString();
  const response = await postgrestRequest("GET", "uk_aq_ingest_runs", {
    query: {
      connector_code: `eq.${CONNECTOR_CODE}`,
      run_started_at: `gte.${windowStartIso}`,
      select: "run_started_at,response_payload",
      order: "run_started_at.asc",
      limit: "1000",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to load OpenAQ hourly request usage (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data)
    ? response.data as OpenAQHourlyUsageRow[]
    : [];

  const usageRows: Array<{ runStartedAtMs: number; requests: number }> = [];
  let used = 0;
  for (const row of rows) {
    const runStartedAt = parseTimestamp(row.run_started_at);
    if (!runStartedAt) {
      continue;
    }
    const payload = toObject(row.response_payload);
    const requestsTotal = toIntegerOrNull(payload?.requests_total) ?? 0;
    if (requestsTotal <= 0) {
      continue;
    }
    const runStartedAtMs = runStartedAt.getTime();
    if (!Number.isFinite(runStartedAtMs)) {
      continue;
    }
    usageRows.push({ runStartedAtMs, requests: requestsTotal });
    used += requestsTotal;
  }

  const cap = Math.max(1, OPENAQ_MAX_REQUESTS_PER_HOUR);
  const remaining = Math.max(0, cap - used);
  let resetAt: string | null = null;
  if (remaining <= 0) {
    let rollingUsed = used;
    for (const row of usageRows) {
      rollingUsed -= row.requests;
      if (rollingUsed < cap) {
        resetAt = new Date(row.runStartedAtMs + 60 * 60 * 1000).toISOString();
        break;
      }
    }
    if (!resetAt) {
      resetAt = new Date(
        now.getTime() + OPENAQ_RATE_LIMIT_FALLBACK_SECONDS * 1000,
      ).toISOString();
    }
  }

  return {
    cap,
    used,
    remaining,
    resetAt,
    rowsConsidered: usageRows.length,
    windowStartIso,
  };
}

async function loadConnector(): Promise<ConnectorConfig | null> {
  const response = await postgrestRequest("GET", "connectors", {
    query: {
      select:
        "id,connector_code,poll_enabled,poll_interval_minutes,poll_window_hours,poll_timeseries_batch_size,scheduler_backend,last_polled_at,last_run_start,last_run_end,last_run_status",
      connector_code: `eq.${CONNECTOR_CODE}`,
      limit: "1",
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

async function loadLatestSafetyRunStartedAt(): Promise<{
  runStartedAt: Date | null;
  runStatus: string | null;
}> {
  const response = await postgrestRequest("GET", "uk_aq_ingest_runs", {
    query: {
      select: "run_started_at,run_status",
      connector_code: `eq.${CONNECTOR_CODE}`,
      run_status: "in.(succeeded,success,partial,skipped)",
      order: "run_started_at.desc",
      limit: "1",
    },
  });
  if (!response.ok) {
    throw new Error(
      `Failed to load latest safety run (${response.status}): ${response.text}`,
    );
  }
  const rows = Array.isArray(response.data) ? response.data : [];
  const row = toObject(rows[0]);
  const runStartedAt = toStringOrNull(row?.run_started_at);
  return {
    runStartedAt: parseTimestamp(runStartedAt),
    runStatus: toStringOrNull(row?.run_status),
  };
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
  if (
    runStatus === "succeeded" ||
    runStatus === "success" ||
    runStatus === "partial"
  ) {
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

async function disableConnectorPollingForAuthStop(
  connectorId: number,
  runStartedAtIso: string,
  runEndedAtIso: string,
  authReason: string,
): Promise<void> {
  const message = `OpenAQ polling auto-disabled (${authReason})`;
  const response = await postgrestRequest("PATCH", "connectors", {
    query: { id: `eq.${connectorId}` },
    body: {
      poll_enabled: false,
      last_run_start: runStartedAtIso,
      last_run_end: runEndedAtIso,
      last_run_status: "failed",
      last_run_message: message,
    },
    prefer: "return=minimal",
  });
  if (!response.ok) {
    throw new Error(
      `Failed to disable OpenAQ polling after ${authReason} (${response.status}): ${response.text}`,
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
  stationRowsCount: number | null,
): Promise<void> {
  const row = {
    connector_id: connectorId,
    connector_code: CONNECTOR_CODE,
    run_started_at: runStartedAtIso,
    run_ended_at: runEndedAtIso,
    run_status: runStatus,
    run_message: runMessage,
    last_observed_at: toStringOrNull(payload?.last_observed_at) ||
      toStringOrNull(payload?.last_observed),
    stations_updated: toIntegerOrNull(payload?.stations_polled) ??
      toIntegerOrNull(payload?.stations_processed) ??
      toIntegerOrNull(payload?.stations_selected) ??
      toIntegerOrNull(payload?.stations_updated) ??
      toIntegerOrNull(payload?.stations) ??
      stationRowsCount,
    observations_upserted: toIntegerOrNull(payload?.observations_upserted) ??
      toIntegerOrNull(payload?.observations),
    timeseries_updated: toIntegerOrNull(payload?.timeseries_updated) ??
      toIntegerOrNull(payload?.timeseries),
    series_polled: toIntegerOrNull(payload?.series_polled) ??
      toIntegerOrNull(payload?.timeseries) ??
      toIntegerOrNull(payload?.timeseries_updated),
    response_status: ingestResponse.status,
    response_payload: compactRunResponsePayload(payload),
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
): Promise<
  { errorId: string; createdAtIso: string; row: Record<string, unknown> }
> {
  const errorId = crypto.randomUUID();
  const createdAtIso = new Date().toISOString();
  const entry = {
    id: errorId,
    created_at: createdAtIso,
    source: "cloud_run",
    severity: "error",
    message: "ingest_openaq dispatch failed",
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
  throw new Error("Timed out waiting for local OpenAQ server startup.");
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

async function fetchGoogleAccessToken(): Promise<string> {
  const response = await fetch(
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
    {
      headers: { "Metadata-Flavor": "Google" },
    },
  );
  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Metadata token request failed (${response.status}): ${text}`,
    );
  }
  const payload = await response.json();
  const token = toStringOrNull(toObject(payload)?.access_token);
  if (!token) {
    throw new Error("Metadata token response missing access_token");
  }
  return token;
}

function effectiveTasksEnabled(): {
  enabled: boolean;
  reason: string;
} {
  if (!OPENAQ_TASKS_ENABLED) {
    return { enabled: false, reason: "tasks_disabled" };
  }
  if (!OPENAQ_GCP_PROJECT_ID) {
    return { enabled: false, reason: "missing_project_id" };
  }
  if (!OPENAQ_GCP_REGION) {
    return { enabled: false, reason: "missing_region" };
  }
  if (
    OPENAQ_CLOUD_RUN_TARGET === "job" &&
    !OPENAQ_CLOUD_RUN_JOB_NAME
  ) {
    return { enabled: false, reason: "missing_job_name" };
  }
  if (
    OPENAQ_CLOUD_RUN_TARGET === "service" &&
    !OPENAQ_CLOUD_RUN_SERVICE_NAME &&
    !OPENAQ_CLOUD_RUN_SERVICE_URL
  ) {
    return { enabled: false, reason: "missing_service_name_or_url" };
  }
  if (!OPENAQ_TASK_QUEUE_ID) {
    return { enabled: false, reason: "missing_queue_id" };
  }
  if (!OPENAQ_TASK_INVOKER_SERVICE_ACCOUNT) {
    return { enabled: false, reason: "missing_task_invoker_service_account" };
  }
  return { enabled: true, reason: "enabled" };
}

async function resolveCloudRunServiceUrl(token: string): Promise<string> {
  if (OPENAQ_CLOUD_RUN_SERVICE_URL) {
    return OPENAQ_CLOUD_RUN_SERVICE_URL;
  }
  const response = await fetch(
    `https://run.googleapis.com/v2/projects/${OPENAQ_GCP_PROJECT_ID}/locations/${OPENAQ_GCP_REGION}/services/${OPENAQ_CLOUD_RUN_SERVICE_NAME}`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
    },
  );
  const text = await response.text();
  if (!response.ok) {
    throw new Error(
      `Cloud Run service lookup failed (${response.status}): ${text}`,
    );
  }
  let payload: unknown = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = null;
    }
  }
  const uri = toStringOrNull(toObject(payload)?.uri);
  if (!uri) {
    throw new Error("Cloud Run service URI missing in service lookup response");
  }
  return uri;
}

async function resolveTaskTarget(
  token: string,
): Promise<{
  url: string;
  body: Record<string, unknown>;
  auth: "oauth" | "oidc";
}> {
  if (OPENAQ_CLOUD_RUN_TARGET === "service") {
    const serviceUrl = await resolveCloudRunServiceUrl(token);
    return {
      url: serviceUrl,
      body: {
        trigger_mode: "task",
      },
      auth: "oidc",
    };
  }
  return {
    url:
      `https://run.googleapis.com/v2/projects/${OPENAQ_GCP_PROJECT_ID}/locations/${OPENAQ_GCP_REGION}/jobs/${OPENAQ_CLOUD_RUN_JOB_NAME}:run`,
    body: {
      overrides: {
        containerOverrides: [{
          env: [{
            name: "OPENAQ_TRIGGER_MODE",
            value: "task",
          }],
        }],
      },
    },
    auth: "oauth",
  };
}

function computeNextCheckTime(
  now: Date,
  earliestNextDueAt: string | null,
  rateLimitResetAt: string | null,
  runStatus: string,
  failure: boolean,
): Date {
  const minDelaySeconds = failure
    ? OPENAQ_FAILURE_RETRY_SECONDS
    : runStatus === "partial"
    ? OPENAQ_NEXT_CHECK_PARTIAL_MIN_SECONDS
    : runStatus === "skipped"
    ? OPENAQ_NEXT_CHECK_SKIPPED_MIN_SECONDS
    : OPENAQ_NEXT_CHECK_MIN_SECONDS;
  const minDelayMs = minDelaySeconds * 1000;
  let notBeforeMs = now.getTime() + minDelayMs;
  if (rateLimitResetAt) {
    const resetMs = Date.parse(rateLimitResetAt);
    if (Number.isFinite(resetMs)) {
      notBeforeMs = Math.max(notBeforeMs, resetMs);
    }
  }
  if (!earliestNextDueAt) {
    return new Date(notBeforeMs);
  }
  const dueMs = Date.parse(earliestNextDueAt);
  if (!Number.isFinite(dueMs)) {
    return new Date(notBeforeMs);
  }
  return new Date(Math.max(dueMs, notBeforeMs));
}

function buildTaskResourceName(taskId: string): string {
  return `projects/${OPENAQ_GCP_PROJECT_ID}/locations/${OPENAQ_GCP_REGION}/queues/${OPENAQ_TASK_QUEUE_ID}/tasks/${taskId}`;
}

function isOpenaqSelfTaskName(taskName: string): boolean {
  return taskName.includes(
    `/tasks/${OPENAQ_TASK_NAME_PREFIX}-${CONNECTOR_CODE}-`,
  );
}

function isCurrentOpenaqTaskName(taskName: string): boolean {
  if (!OPENAQ_CURRENT_TASK_NAME) {
    return false;
  }
  if (taskName === OPENAQ_CURRENT_TASK_NAME) {
    return true;
  }
  if (OPENAQ_CURRENT_TASK_NAME.includes("/tasks/")) {
    const currentTaskId = OPENAQ_CURRENT_TASK_NAME.split("/tasks/").pop();
    return Boolean(
      currentTaskId && taskName.endsWith(`/tasks/${currentTaskId}`),
    );
  }
  return taskName.endsWith(`/tasks/${OPENAQ_CURRENT_TASK_NAME}`);
}

async function listPendingOpenaqTasks(
  queueUri: string,
  token: string,
): Promise<PendingOpenAQTask[]> {
  const tasks: PendingOpenAQTask[] = [];
  let pageToken: string | null = null;
  do {
    const url = new URL(queueUri);
    url.searchParams.set("responseView", "BASIC");
    url.searchParams.set("pageSize", "100");
    if (pageToken) {
      url.searchParams.set("pageToken", pageToken);
    }
    const response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`Cloud Tasks list failed (${response.status}): ${text}`);
    }
    let payload: unknown = null;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch {
        payload = null;
      }
    }
    const root = toObject(payload);
    const rows = Array.isArray(root?.tasks) ? root?.tasks : [];
    for (const row of rows) {
      const obj = toObject(row);
      const name = toStringOrNull(obj?.name);
      if (!name) {
        continue;
      }
      if (isCurrentOpenaqTaskName(name)) {
        continue;
      }
      if (!isOpenaqSelfTaskName(name)) {
        continue;
      }
      const scheduleAt = toStringOrNull(obj?.scheduleTime);
      if (!scheduleAt) {
        continue;
      }
      const scheduleAtMs = Date.parse(scheduleAt);
      if (!Number.isFinite(scheduleAtMs)) {
        continue;
      }
      tasks.push({
        name,
        scheduleAt,
        scheduleAtMs,
      });
    }
    pageToken = toStringOrNull(root?.nextPageToken);
  } while (pageToken);
  tasks.sort((a, b) => a.scheduleAtMs - b.scheduleAtMs);
  return tasks;
}

async function deleteCloudTask(taskName: string, token: string): Promise<void> {
  const response = await fetch(
    `https://cloudtasks.googleapis.com/v2/${taskName}`,
    {
      method: "DELETE",
      headers: {
        Authorization: `Bearer ${token}`,
      },
    },
  );
  if (response.ok || response.status === 404) {
    return;
  }
  const text = await response.text();
  throw new Error(`Cloud Tasks delete failed (${response.status}): ${text}`);
}

async function deletePendingTasks(
  pendingTasks: PendingOpenAQTask[],
  token: string,
  reason: string,
): Promise<{ deletedCount: number; deleteErrors: number }> {
  let deletedCount = 0;
  let deleteErrors = 0;
  for (const task of pendingTasks) {
    try {
      await deleteCloudTask(task.name, token);
      deletedCount += 1;
    } catch (error) {
      deleteErrors += 1;
      logSummary("task_delete_failed", {
        reason,
        task_name: task.name,
        task_schedule_at: task.scheduleAt,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return { deletedCount, deleteErrors };
}

async function clearPendingOpenaqTasks(
  reason: string,
): Promise<void> {
  const tasksState = effectiveTasksEnabled();
  if (!tasksState.enabled) {
    logSummary("task_clear_skipped", {
      reason,
      tasks_state: tasksState.reason,
    });
    return;
  }
  const queueUri =
    `https://cloudtasks.googleapis.com/v2/projects/${OPENAQ_GCP_PROJECT_ID}/locations/${OPENAQ_GCP_REGION}/queues/${OPENAQ_TASK_QUEUE_ID}/tasks`;
  const token = await fetchGoogleAccessToken();
  const pendingTasks = await listPendingOpenaqTasks(queueUri, token);
  if (!pendingTasks.length) {
    logSummary("task_clear_skipped", {
      reason,
      tasks_state: "no_pending_tasks",
    });
    return;
  }
  const { deletedCount, deleteErrors } = await deletePendingTasks(
    pendingTasks,
    token,
    reason,
  );
  logSummary("task_cleared_for_safety", {
    reason,
    pending_count: pendingTasks.length,
    deleted_count: deletedCount,
    delete_errors: deleteErrors,
  });
}

function hasEarlierOrEqualPendingTask(
  pendingTasks: PendingOpenAQTask[],
  requestedScheduleMs: number,
): boolean {
  return pendingTasks.some((task) => task.scheduleAtMs <= requestedScheduleMs);
}

function logTaskEnqueueSkippedExistingEarlier(
  reason: string,
  requestedScheduleAtIso: string,
  pendingTasks: PendingOpenAQTask[],
  rateLimitResetAt: string | null = null,
): void {
  const earliestTask = pendingTasks[0];
  logSummary("task_enqueue_skipped_existing_earlier", {
    reason,
    requested_schedule_at: requestedScheduleAtIso,
    rate_limit_reset_at: rateLimitResetAt,
    pending_count: pendingTasks.length,
    earliest_pending_task_name: earliestTask?.name ?? null,
    earliest_pending_schedule_at: earliestTask?.scheduleAt ?? null,
  });
}

async function enqueueSelfRunTask(
  scheduleAt: Date,
  reason: string,
  rateLimitResetAt: string | null,
): Promise<void> {
  const tasksState = effectiveTasksEnabled();
  if (!tasksState.enabled) {
    logSummary("tasks_not_scheduled", {
      reason: tasksState.reason,
      schedule_at: scheduleAt.toISOString(),
    });
    return;
  }

  const queueUri =
    `https://cloudtasks.googleapis.com/v2/projects/${OPENAQ_GCP_PROJECT_ID}/locations/${OPENAQ_GCP_REGION}/queues/${OPENAQ_TASK_QUEUE_ID}/tasks`;

  const taskMinuteBucket = Math.floor(scheduleAt.getTime() / 60_000);
  const taskId =
    `${OPENAQ_TASK_NAME_PREFIX}-${CONNECTOR_CODE}-${taskMinuteBucket}`;
  const taskName = buildTaskResourceName(taskId);
  const token = await fetchGoogleAccessToken();
  const target = await resolveTaskTarget(token);

  let pendingTasks: PendingOpenAQTask[] = [];
  try {
    pendingTasks = await listPendingOpenaqTasks(queueUri, token);
  } catch (error) {
    logSummary("task_probe_failed", {
      reason,
      requested_schedule_at: scheduleAt.toISOString(),
      error: error instanceof Error ? error.message : String(error),
    });
  }

  if (pendingTasks.length) {
    const requestedScheduleMs = scheduleAt.getTime();
    const rateLimitResetMs = rateLimitResetAt
      ? Date.parse(rateLimitResetAt)
      : Number.NaN;
    const hasRateLimitGuard = Number.isFinite(rateLimitResetMs);
    if (hasRateLimitGuard) {
      const hasBeforeResetTask = pendingTasks.some((task) =>
        task.scheduleAtMs < rateLimitResetMs
      );
      if (hasBeforeResetTask) {
        const { deletedCount, deleteErrors } = await deletePendingTasks(
          pendingTasks,
          token,
          reason,
        );
        logSummary("task_reconciled_rate_limit_guard", {
          reason,
          requested_schedule_at: scheduleAt.toISOString(),
          rate_limit_reset_at: rateLimitResetAt,
          pending_count: pendingTasks.length,
          deleted_count: deletedCount,
          delete_errors: deleteErrors,
        });
        pendingTasks = [];
      } else {
        const hasEarlierOrEqualTask = hasEarlierOrEqualPendingTask(
          pendingTasks,
          requestedScheduleMs,
        );
        if (hasEarlierOrEqualTask) {
          logTaskEnqueueSkippedExistingEarlier(
            reason,
            scheduleAt.toISOString(),
            pendingTasks,
            rateLimitResetAt,
          );
          return;
        }
      }
    } else {
      const hasEarlierOrEqualTask = hasEarlierOrEqualPendingTask(
        pendingTasks,
        requestedScheduleMs,
      );
      if (hasEarlierOrEqualTask) {
        logTaskEnqueueSkippedExistingEarlier(
          reason,
          scheduleAt.toISOString(),
          pendingTasks,
        );
        return;
      }
    }
  }

  if (pendingTasks.length) {
    const requestedScheduleMs = scheduleAt.getTime();
    const hasEarlierOrEqualTask = hasEarlierOrEqualPendingTask(
      pendingTasks,
      requestedScheduleMs,
    );
    if (hasEarlierOrEqualTask) {
      logTaskEnqueueSkippedExistingEarlier(
        reason,
        scheduleAt.toISOString(),
        pendingTasks,
      );
      return;
    }

    const { deletedCount, deleteErrors } = await deletePendingTasks(
      pendingTasks,
      token,
      reason,
    );
    logSummary("task_reconciled_later_tasks", {
      reason,
      requested_schedule_at: scheduleAt.toISOString(),
      pending_count: pendingTasks.length,
      deleted_count: deletedCount,
      delete_errors: deleteErrors,
    });
  }

  const body = {
    task: {
      name: taskName,
      scheduleTime: scheduleAt.toISOString(),
      httpRequest: {
        httpMethod: "POST",
        url: target.url,
        headers: {
          "Content-Type": "application/json",
        },
        body: btoa(JSON.stringify(target.body)),
        ...(target.auth === "oidc"
          ? {
            oidcToken: {
              serviceAccountEmail: OPENAQ_TASK_INVOKER_SERVICE_ACCOUNT,
              audience: target.url,
            },
          }
          : {
            oauthToken: {
              serviceAccountEmail: OPENAQ_TASK_INVOKER_SERVICE_ACCOUNT,
              scope: "https://www.googleapis.com/auth/cloud-platform",
            },
          }),
      },
    },
  };

  const response = await fetch(queueUri, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) {
    if (response.status === 409) {
      logSummary("task_already_scheduled", {
        reason,
        task_name: taskName,
        schedule_at: scheduleAt.toISOString(),
      });
      return;
    }
    throw new Error(
      `Cloud Tasks create failed (${response.status}): ${text}`,
    );
  }

  logSummary("task_scheduled", {
    reason,
    task_name: taskName,
    schedule_at: scheduleAt.toISOString(),
  });
}

async function scheduleNextCheck(
  connectorId: number,
  reason: string,
  runStatus: string,
  failure: boolean,
  rateLimitResetAt: string | null,
): Promise<void> {
  let earliestNextDueAt: string | null = null;
  try {
    earliestNextDueAt = await loadEarliestNextDueAt(connectorId);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logSummary("next_due_lookup_failed", {
      reason,
      connector_id: connectorId,
      error: message,
    });
  }

  const now = new Date();
  const scheduleAt = computeNextCheckTime(
    now,
    earliestNextDueAt,
    rateLimitResetAt,
    runStatus,
    failure,
  );
  await enqueueSelfRunTask(scheduleAt, reason, rateLimitResetAt);
}

function deriveRateLimitResetAt(
  payload: Record<string, unknown> | null,
): string | null {
  if (!payload) {
    return null;
  }
  const stopReason = toStringOrNull(payload.stopped_reason)?.toLowerCase() ??
    null;
  const rateLimitStopReason =
    toStringOrNull(payload.rate_limit_stop_reason)?.toLowerCase() ??
      null;
  const requestsTotal = toIntegerOrNull(payload.requests_total);
  const maxRequestsPerRun = toIntegerOrNull(payload.max_requests_per_run);
  const gapRequestsSkippedBudget = toIntegerOrNull(
    payload.gap_requests_skipped_budget,
  );
  const requestBudgetLimited =
    (gapRequestsSkippedBudget !== null && gapRequestsSkippedBudget > 0) ||
    (
      requestsTotal !== null &&
      maxRequestsPerRun !== null &&
      maxRequestsPerRun > 0 &&
      requestsTotal >= maxRequestsPerRun
    );
  const rateLimitStop = isRateLimitReason(rateLimitStopReason) ||
    isRateLimitReason(stopReason) ||
    requestBudgetLimited;
  if (!rateLimitStop) {
    return null;
  }
  const explicitResetAt = toStringOrNull(payload.rate_limit_reset_at);
  if (explicitResetAt) {
    return explicitResetAt;
  }
  const sharedResetAt = toStringOrNull(payload.shared_budget_hour_reset_at) ||
    toStringOrNull(payload.shared_budget_minute_reset_at);
  if (sharedResetAt) {
    return sharedResetAt;
  }
  const sharedRetrySeconds = toIntegerOrNull(
    payload.shared_budget_retry_after_seconds,
  );
  if (sharedRetrySeconds !== null && sharedRetrySeconds > 0) {
    return new Date(Date.now() + sharedRetrySeconds * 1000).toISOString();
  }
  const numericReset = toIntegerOrNull(payload.rate_limit_reset);
  if (numericReset !== null) {
    const nowMs = Date.now();
    const resetMs = numericReset > 1e12
      ? numericReset
      : numericReset > 1e9
      ? numericReset * 1000
      : nowMs + Math.max(0, numericReset * 1000);
    return new Date(resetMs).toISOString();
  }
  return new Date(
    Date.now() + OPENAQ_RATE_LIMIT_FALLBACK_SECONDS * 1000,
  ).toISOString();
}

async function recordSkippedRun(
  connectorId: number,
  runStartedAtIso: string,
  runMessage: string,
  payloadExtras: Record<string, unknown> | null = null,
): Promise<void> {
  const runEndedAtIso = new Date().toISOString();
  const runStatus = "skipped";
  const payload = {
    run_status: runStatus,
    run_message: runMessage,
    connector_code: CONNECTOR_CODE,
    ...(payloadExtras ?? {}),
  };
  const ingestResponse: IngestResponse = {
    ok: true,
    status: 204,
    body: payload,
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
    payload,
    null,
  );
}

async function main(): Promise<void> {
  const now = new Date();
  const runStartedAtIso = now.toISOString();
  const connector = await loadConnector();
  const eligibility = evaluateEligibility(connector, now);
  if (!eligibility.eligible) {
    const connectorId = toIntegerOrNull(connector?.id);
    if (eligibility.reason === "poll_disabled" && connectorId !== null) {
      await recordSkippedRun(
        connectorId,
        runStartedAtIso,
        "poll_disabled",
        {
          event: "openaq_polling_disabled",
          severity: "warning",
          reason: "poll_disabled",
          connector_code: CONNECTOR_CODE,
          connector_id: connectorId,
          poll_enabled: false,
          scheduler_backend: toStringOrNull(connector?.scheduler_backend) ||
            SCHEDULER_BACKEND_SUPABASE_FUNCTION,
          warnings: [
            {
              event: "openaq_polling_disabled",
              severity: "warning",
              connector_code: CONNECTOR_CODE,
              connector_id: connectorId,
              poll_enabled: false,
              scheduler_backend: toStringOrNull(connector?.scheduler_backend) ||
                SCHEDULER_BACKEND_SUPABASE_FUNCTION,
              reason: "poll_disabled",
              occurred_at: runStartedAtIso,
            },
          ],
        },
      );
    }
    logSummary("skipped", {
      reason: eligibility.reason,
      trigger_mode: OPENAQ_TRIGGER_MODE,
      poll_enabled: connector?.poll_enabled,
      scheduler_backend: toStringOrNull(connector?.scheduler_backend) ||
        SCHEDULER_BACKEND_SUPABASE_FUNCTION,
    });
    return;
  }

  if (OPENAQ_TRIGGER_MODE === "safety") {
    const latestRun = await loadLatestSafetyRunStartedAt();
    const latestRunAt = latestRun.runStartedAt;
    const lookbackMs = OPENAQ_SAFETY_SUCCESS_LOOKBACK_MINUTES * 60 * 1000;
    if (
      latestRunAt &&
      now.getTime() - latestRunAt.getTime() <= lookbackMs
    ) {
      logSummary("safety_noop_recent_run", {
        trigger_mode: OPENAQ_TRIGGER_MODE,
        lookback_minutes: OPENAQ_SAFETY_SUCCESS_LOOKBACK_MINUTES,
        last_run_at: latestRunAt.toISOString(),
        last_run_status: latestRun.runStatus,
        minutes_since_last_run: Math.max(
          0,
          Math.round((now.getTime() - latestRunAt.getTime()) / 60000),
        ),
      });
      return;
    }
    logSummary("safety_trigger_run", {
      trigger_mode: OPENAQ_TRIGGER_MODE,
      lookback_minutes: OPENAQ_SAFETY_SUCCESS_LOOKBACK_MINUTES,
      last_run_at: latestRunAt ? latestRunAt.toISOString() : null,
      last_run_status: latestRun.runStatus,
    });
  }

  const claim = await claimConnector(runStartedAtIso);
  if (!claim || claim.claimed !== true) {
    logSummary("skipped", {
      reason: "claim_not_acquired",
      trigger_mode: OPENAQ_TRIGGER_MODE,
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

  let hourlyBudget: OpenAQHourlyBudgetState = {
    cap: OPENAQ_MAX_REQUESTS_PER_HOUR,
    used: 0,
    remaining: OPENAQ_MAX_REQUESTS_PER_HOUR,
    resetAt: null,
    rowsConsidered: 0,
    windowStartIso: new Date(now.getTime() - 60 * 60 * 1000).toISOString(),
  };
  try {
    hourlyBudget = await loadRecentHourlyRequestBudget(now);
  } catch (error) {
    logSummary("hourly_budget_lookup_failed", {
      trigger_mode: OPENAQ_TRIGGER_MODE,
      connector_id: connectorId,
      error: shortError(error),
      fallback_cap: OPENAQ_MAX_REQUESTS_PER_HOUR,
    });
  }
  if (hourlyBudget.remaining <= 0) {
    await recordSkippedRun(
      connectorId,
      runStartedAtIso,
      "Skipped - Hourly Limit",
      {
        stopped_reason: "hourly_rate_limit_guard",
        rate_limit_stop: true,
        rate_limit_stop_reason: "hourly_rate_limit_guard",
        rate_limit_reset_at: hourlyBudget.resetAt,
        requests_total: 0,
        max_requests_per_run: 0,
      },
    );
    await scheduleNextCheck(
      connectorId,
      "rate_limit_hourly_guard",
      "skipped",
      false,
      hourlyBudget.resetAt,
    );
    logSummary("skipped", {
      reason: "rate_limit_hourly_guard",
      trigger_mode: OPENAQ_TRIGGER_MODE,
      connector_id: connectorId,
      max_requests_per_hour: hourlyBudget.cap,
      hourly_requests_used: hourlyBudget.used,
      hourly_requests_remaining: hourlyBudget.remaining,
      hourly_window_start: hourlyBudget.windowStartIso,
      hourly_rows_considered: hourlyBudget.rowsConsidered,
      next_retry_at: hourlyBudget.resetAt,
    });
    return;
  }

  const payloadPlan = await buildIngestPayload(connector, {
    batchLimitOverride: hourlyBudget.remaining,
  });
  if (!payloadPlan.stationRows.length) {
    await recordSkippedRun(connectorId, runStartedAtIso, "no_station_refs");
    await scheduleNextCheck(
      connectorId,
      "no_station_refs",
      "skipped",
      false,
      null,
    );
    logSummary("skipped", {
      reason: "no_station_refs",
      trigger_mode: OPENAQ_TRIGGER_MODE,
      connector_id: connectorId,
      max_requests_per_hour: hourlyBudget.cap,
      hourly_requests_used: hourlyBudget.used,
      hourly_requests_remaining: hourlyBudget.remaining,
      hourly_window_start: hourlyBudget.windowStartIso,
      configured_batch_limit: payloadPlan.configuredBatchLimit,
      batch_limit: payloadPlan.batchLimit,
      tiered_limit: payloadPlan.tieredLimit,
      stale_limit: payloadPlan.staleLimit,
      tier1_retry_seconds: payloadPlan.tier1RetrySeconds,
    });
    return;
  }

  logSummary("dispatching", {
    trigger_mode: OPENAQ_TRIGGER_MODE,
    connector_id: connectorId,
    max_requests_per_hour: hourlyBudget.cap,
    hourly_requests_used: hourlyBudget.used,
    hourly_requests_remaining: hourlyBudget.remaining,
    configured_batch_limit: payloadPlan.configuredBatchLimit,
    batch_limit: payloadPlan.batchLimit,
    tiered_limit: payloadPlan.tieredLimit,
    stale_limit: payloadPlan.staleLimit,
    tier1_retry_seconds: payloadPlan.tier1RetrySeconds,
    window_hours: payloadPlan.windowHours,
    station_refs_count: payloadPlan.stationRows.length,
  });

  const server = new Deno.Command("deno", {
    args: [
      "run",
      "--allow-env",
      "--allow-net",
      "--allow-read",
      "--allow-write",
      OPENAQ_INGEST_SCRIPT_PATH,
    ],
    env: {
      ...Deno.env.toObject(),
    },
    stdout: "inherit",
    stderr: "inherit",
  }).spawn();

  let ingestResponse: IngestResponse | null = null;
  let runFailed = false;
  let runStatus = "failed";
  let rateLimitResetAt: string | null = null;
  let suppressScheduling = false;
  let suppressSchedulingReason: string | null = null;
  let summary: RunSummary;

  try {
    try {
      await waitForServer(`http://127.0.0.1:${PORT}/`);
      ingestResponse = await runIngestOnce(payloadPlan.payload);
    } catch (error) {
      const errorMessage = shortError(error);
      const runEndedAtIso = new Date().toISOString();
      if (connectorId === null) {
        connectorId = await resolveConnectorId(null);
      }
      await updateConnectorRun(
        connectorId,
        runStartedAtIso,
        runEndedAtIso,
        "failed",
        errorMessage,
      );
      runFailed = true;
      try {
        const inserted = await insertErrorLog(connectorId, {
          ok: false,
          status: 500,
          body: {
            error: "cloud_run_wrapper_failed",
            message: errorMessage,
          },
          raw: errorMessage,
        });
        await uploadErrorLogRowToDropbox(
          inserted.errorId,
          inserted.createdAtIso,
          inserted.row,
        );
      } catch (loggingError) {
        logSummary("dropbox_error_upload_warning", {
          trigger_mode: OPENAQ_TRIGGER_MODE,
          error: shortError(loggingError),
        });
      }
      throw error;
    }

    summary = deriveRunSummary(ingestResponse);
    runStatus = summary.runStatus;
    runFailed = !ingestResponse.ok || runStatus === "failed" ||
      runStatus === "error";

    const runEndedAtIso = new Date().toISOString();
    if (connectorId === null) {
      connectorId = await resolveConnectorId(summary.payload);
    }

    rateLimitResetAt = deriveRateLimitResetAt(summary.payload);
    const stoppedReason =
      toStringOrNull(summary.payload?.stopped_reason)?.toLowerCase() ?? null;
    const rateLimitStopReason = toStringOrNull(
      summary.payload?.rate_limit_stop_reason,
    )?.toLowerCase() ?? null;
    const authReason = isAuthStopReason(stoppedReason)
      ? stoppedReason
      : (isAuthStopReason(rateLimitStopReason) ? rateLimitStopReason : null);
    if (OPENAQ_AUTH_SAFETY_DISABLE_POLLING && authReason) {
      suppressScheduling = true;
      suppressSchedulingReason = `auth_safety_${authReason}`;
      runStatus = "failed";
      runFailed = true;
      summary = {
        runStatus: "failed",
        runMessage: `OpenAQ polling auto-disabled (${authReason})`,
        payload: {
          ...(summary.payload ?? {}),
          event: "openaq_polling_auto_disabled",
          severity: "error",
          run_status: "failed",
          run_message: `OpenAQ polling auto-disabled (${authReason})`,
          reason: "openaq_auth_401_or_403",
          poll_enabled: false,
          self_reschedule_suppressed: true,
          stopped_reason: authReason,
          rate_limit_stop_reason: authReason,
        },
      };
      ingestResponse = {
        ...ingestResponse,
        body: summary.payload,
        raw: JSON.stringify(summary.payload),
      };
      await disableConnectorPollingForAuthStop(
        connectorId,
        runStartedAtIso,
        runEndedAtIso,
        authReason,
      );
    } else {
      await updateConnectorRun(
        connectorId,
        runStartedAtIso,
        runEndedAtIso,
        runStatus,
        summary.runMessage,
      );
    }

    await insertRunRow(
      connectorId,
      runStartedAtIso,
      runEndedAtIso,
      runStatus,
      summary.runMessage,
      ingestResponse,
      summary.payload,
      payloadPlan.stationRows.length,
    );

    if (suppressScheduling) {
      try {
        await clearPendingOpenaqTasks(
          suppressSchedulingReason ?? "auth_safety",
        );
      } catch (error) {
        logSummary("task_clear_failed", {
          reason: suppressSchedulingReason ?? "auth_safety",
          error: shortError(error),
        });
      }
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
          trigger_mode: OPENAQ_TRIGGER_MODE,
          error: shortError(error),
        });
      }
      throw new Error(
        `ingest_openaq failed (${ingestResponse.status}): ${ingestResponse.raw}`,
      );
    }

    logSummary("success", {
      trigger_mode: OPENAQ_TRIGGER_MODE,
      run_status: runStatus,
      response_status: ingestResponse.status,
      connector_id: connectorId,
      stations_selected: payloadPlan.stationRows.length,
      observations_upserted: toIntegerOrNull(
        summary.payload?.observations_upserted,
      ),
      observations_rows_input: toIntegerOrNull(
        summary.payload?.observations_rows_input,
      ),
      observations_rows_prepared: toIntegerOrNull(
        summary.payload?.observations_rows_prepared,
      ),
      observations_rows_deduped_prewrite: toIntegerOrNull(
        summary.payload?.observations_rows_deduped_prewrite,
      ),
      observs_rows_prepared: toIntegerOrNull(
        summary.payload?.observs_rows_prepared,
      ),
      observs_rows_deduped_prewrite: toIntegerOrNull(
        summary.payload?.observs_rows_deduped_prewrite,
      ),
      series_polled: toIntegerOrNull(summary.payload?.series_polled),
      partial: summary.payload?.partial === true,
      stopped_reason: toStringOrNull(summary.payload?.stopped_reason),
      shared_budget_enabled: summary.payload?.shared_budget_enabled === true,
      shared_budget_key: toStringOrNull(summary.payload?.shared_budget_key),
      shared_budget_caller: toStringOrNull(
        summary.payload?.shared_budget_caller,
      ),
      shared_budget_reason: toStringOrNull(
        summary.payload?.shared_budget_reason,
      ),
      shared_budget_granted: summary.payload?.shared_budget_granted === true,
      shared_budget_minute_limit: toIntegerOrNull(
        summary.payload?.shared_budget_minute_limit,
      ),
      shared_budget_minute_used_after: toIntegerOrNull(
        summary.payload?.shared_budget_minute_used_after,
      ),
      shared_budget_minute_remaining: toIntegerOrNull(
        summary.payload?.shared_budget_minute_remaining,
      ),
      shared_budget_hour_limit: toIntegerOrNull(
        summary.payload?.shared_budget_hour_limit,
      ),
      shared_budget_hour_used_after: toIntegerOrNull(
        summary.payload?.shared_budget_hour_used_after,
      ),
      shared_budget_hour_remaining: toIntegerOrNull(
        summary.payload?.shared_budget_hour_remaining,
      ),
      shared_budget_retry_after_seconds: toIntegerOrNull(
        summary.payload?.shared_budget_retry_after_seconds,
      ),
      suppress_scheduling: suppressScheduling,
      suppress_scheduling_reason: suppressSchedulingReason,
    });
  } finally {
    try {
      server.kill("SIGTERM");
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logSummary("server_kill_failed", {
        trigger_mode: OPENAQ_TRIGGER_MODE,
        error: message,
      });
    }
    try {
      await Promise.race([
        server.status,
        new Promise((resolve) => setTimeout(resolve, 5000)),
      ]);
    } catch {
      // Ignore shutdown race.
    }

    if (suppressScheduling) {
      logSummary("task_schedule_skipped", {
        trigger_mode: OPENAQ_TRIGGER_MODE,
        connector_id: connectorId,
        reason: suppressSchedulingReason ?? "suppressed",
      });
    } else {
      try {
        await scheduleNextCheck(
          connectorId,
          runFailed ? "failed_retry" : runStatus,
          runStatus,
          runFailed,
          rateLimitResetAt,
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logSummary("task_schedule_failed", {
          trigger_mode: OPENAQ_TRIGGER_MODE,
          connector_id: connectorId,
          error: message,
        });
      }
    }
  }

  if (!ingestResponse) {
    throw new Error("No OpenAQ ingest response received.");
  }
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  logSummary("failure", { error: message, trigger_mode: OPENAQ_TRIGGER_MODE });
  Deno.exit(1);
});
