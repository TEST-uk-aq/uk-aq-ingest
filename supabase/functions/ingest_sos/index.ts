// trigger deploy 2026-02-09 12:36
import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import "../_shared/fetch_egress_patch.ts";
import { cacheControlHeaders } from "../_shared/cache.ts";
import {
  type ObservsObservationRow,
  writeObservsWithOutbox,
} from "../_shared/observs_client.ts";
import {
  buildCompactObservationRpcArgsV2,
  createEmptyIngestDbObservationWriteStats,
  isIngestDbObservationWriteError,
  mergeIngestDbObservationWriteStats,
  serializedJsonUtf8Bytes,
  writeIngestDbObservations,
} from "../_shared/ingestdb_observation_writer.mjs";
import {
  addRuntimeDeadlineFailure,
  asSosFetchFailure,
  boundMessage,
  connectorHttpStatusForProbe,
  isIndividuallyReportedTimeseriesFailure,
  isRuntimeDeadlineFailure,
  runtimeBudgetStopObserved,
  type RuntimeDeadlineFailureSummary,
  SosFetchFailure,
} from "./failure.ts";
import {
  fetchUkAirHtmlPage,
  isUkAirHtmlFallbackProbeFailure,
  resolveUkAirHtmlMappings,
  type UkAirHtmlBridgeRow,
  type UkAirHtmlMappedWork,
} from "./uk_aq_html_fallback.ts";
import {
  parseUkAirHtmlChart,
  UK_AIR_HTML_BRIDGE_POLLUTANT_CODES,
  UkAirHtmlParseError,
} from "./uk_aq_html_parser.ts";
import { recordSosObservationChanges } from "./run_metrics.ts";

type PollRequest = {
  connector_id?: string;
  connector_code?: string;
  connector_label?: string;
  window_hours?: number;
  pollutants?: string[] | string;
  timeseries_ids?: string[] | string;
  timeseries_limit?: number;
};

type ConnectorRow = {
  id: string;
  connector_code: string;
  label: string;
  service_url: string | null;
  poll_enabled: boolean | null;
  poll_window_hours: number | null;
  poll_timeseries_batch_size: number | null;
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

type SosTimeseriesRow = {
  id: number;
  timeseries_ref: string | null;
  service_ref: string | null;
  phenomenon_id: string | null;
  last_value_at: string | null;
  uom: string | null;
};

const DEFAULT_BASE_URL = "https://uk-air.defra.gov.uk/sos-ukair/api/v1";
const DEFAULT_SERVICE_LABEL = "SOS";
const DEFAULT_CONNECTOR_CODE = "sos";
const SOS_PRIMARY_ACQUISITION_METHOD = "sos";
const SOS_HTML_FALLBACK_ACQUISITION_METHOD = "ukair_html";
const DEFAULT_WINDOW_HOURS = 6;
const DEFAULT_MAX_RUNTIME_SECONDS = 240;
const DEFAULT_TIMEOUT_MS = 30_000;
const MIN_FETCH_TIMEOUT_MS = 4_000;
const FETCH_RETRY_ATTEMPTS = 3;
const FETCH_RETRY_BACKOFF_BASE_MS = 1_000;
const FETCH_RETRY_BACKOFF_MAX_MS = 30_000;
const RETRYABLE_FETCH_STATUSES = new Set([429, 500, 502, 503, 504]);
const DEFAULT_BOOTSTRAP_NULL_LAST_VALUE_BATCH = 50;
const DEFAULT_OBSERVS_BUFFER_FLUSH_ROWS = 5000;
const PAGE_SIZE = 1000;
const CONCURRENCY_LIMIT = 5;
const RUNTIME_DEADLINE_TIMESERIES_SAMPLE_LIMIT = 10;

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
const SOS_BASE_URL = (Deno.env.get("SOS_BASE_URL")
  ?? Deno.env.get("UK_AIR_BASE_URL")
  ?? DEFAULT_BASE_URL).replace(/\/$/, "");
const SOS_SERVICE_LABEL = Deno.env.get("SOS_SERVICE_LABEL")
  ?? Deno.env.get("UK_AIR_SERVICE_LABEL")
  ?? DEFAULT_SERVICE_LABEL;
const SOS_CONNECTOR_CODE = Deno.env.get("SOS_CONNECTOR_CODE")
  ?? DEFAULT_CONNECTOR_CODE;
const SOS_MAX_RUNTIME_SECONDS = Number(
  Deno.env.get("SOS_MAX_RUNTIME_SECONDS") ?? DEFAULT_MAX_RUNTIME_SECONDS,
);
const SOS_BOOTSTRAP_NULL_LAST_VALUE_BATCH = Number(
  Deno.env.get("SOS_BOOTSTRAP_NULL_LAST_VALUE_BATCH")
    ?? DEFAULT_BOOTSTRAP_NULL_LAST_VALUE_BATCH,
);
const DROPBOX_APP_KEY = Deno.env.get("DROPBOX_APP_KEY") ?? "";
const DROPBOX_APP_SECRET = Deno.env.get("DROPBOX_APP_SECRET") ?? "";
const DROPBOX_REFRESH_TOKEN = Deno.env.get("DROPBOX_REFRESH_TOKEN") ?? "";
const DROPBOX_ALLOWED_SUPABASE_URL = Deno.env.get("UK_AIR_RAW_DROPBOX_ALLOWED_SUPABASE_URL") ?? "";
const _DROPBOX_ERROR_ALLOWED_SUPABASE_URL =
  Deno.env.get("UK_AIR_ERROR_DROPBOX_ALLOWED_SUPABASE_URL") ?? "";
const DROPBOX_ROOT_FOLDER = (() => {
  const raw = Deno.env.get("UK_AQ_DROPBOX_ROOT") ?? "";
  return normalizeDropboxPath(raw);
})();

const DROPBOX_LOG_FOLDER = dropboxWithRoot("/connectors/sos/log");
const DROPBOX_RAW_FOLDER = dropboxWithRoot("/connectors/sos/raw_data");
const DROPBOX_ERROR_FOLDER = dropboxWithRoot(
  Deno.env.get("UK_AIR_ERROR_DROPBOX_FOLDER") ?? "error_log",
);
const DROPBOX_LOG_RETENTION_DAYS = 31;
const DROPBOX_TOKEN_URL = "https://api.dropbox.com/oauth2/token";
const DROPBOX_UPLOAD_URL = "https://content.dropboxapi.com/2/files/upload";
const DROPBOX_UPLOAD_SOURCE = (() => {
  const value = (Deno.env.get("SOS_DROPBOX_UPLOAD_SOURCE") ?? "edge")
    .trim()
    .toLowerCase();
  return value === "cloud_run" ? "cloud_run" : "edge";
})();
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
    "x-ukaq-egress-caller": "ingest_sos",
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
      has_cron_secret: Boolean(SB_UK_AQ_CRON_SECRET),
      header_present: Boolean(header),
      header_length: header ? header.length : 0,
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

const ERROR_LOGGER = createErrorLogger(
  loadErrorDropboxConfig(),
  Boolean(SUPABASE_URL && SUPABASE_PRIVILEGED_KEY),
);

function logUnhandledError(
  message: string,
  err: unknown,
  context: Record<string, unknown>,
): void {
  const error = err instanceof Error ? err : new Error(String(err));
  void ERROR_LOGGER.logError({
    source: "edge",
    severity: "error",
    message,
    stack: error.stack,
    context,
    connector_code: SOS_CONNECTOR_CODE,
  });
}

addEventListener("error", (event) => {
  if (typeof event.preventDefault === "function") {
    event.preventDefault();
  }
  logUnhandledError(
    "Unhandled error event.",
    event.error ?? event.message,
    {
      message: event.message,
      filename: event.filename,
      lineno: event.lineno,
      colno: event.colno,
    },
  );
});

addEventListener("unhandledrejection", (event) => {
  if (typeof event.preventDefault === "function") {
    event.preventDefault();
  }
  logUnhandledError(
    "Unhandled promise rejection.",
    event.reason,
    {
      reason: event.reason instanceof Error ? event.reason.message : String(event.reason),
    },
  );
});

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
  const errors: string[] = [];
  let status = 200;
  let polled = 0;
  let observationsUpserted = 0;
  const changedTimeseriesIds = new Set<number>();
  const ingestDbObservationWriteStats =
    createEmptyIngestDbObservationWriteStats();
  let ingestDbObservationWriteFailed = false;
  let gateway502Failures = 0;
  let skippedNoLastValueAt = 0;
  let skippedStaleLastValueAt = 0;
  let responsePayload: Record<string, unknown> = {};
  let connector: ConnectorRow | null = null;
  let requestedConnectorId: string | undefined;
  let requestedConnectorCode = SOS_CONNECTOR_CODE;
  let requestedConnectorLabel = SOS_SERVICE_LABEL;
  let requestedWindowHours: number | undefined;
  let requestedPollutants: string[] | undefined;
  let requestedLimit: number | undefined;
  let requestedTimeseriesIds: number[] | undefined;
  const runStartedAt = Date.now();
  const maxRuntimeSeconds = Number.isFinite(SOS_MAX_RUNTIME_SECONDS)
    ? Math.max(30, SOS_MAX_RUNTIME_SECONDS)
    : DEFAULT_MAX_RUNTIME_SECONDS;
  const runtimeDeadline = runStartedAt + maxRuntimeSeconds * 1000;
  const shouldStop = () => Date.now() >= runtimeDeadline;
  let timeBudgetHit = false;
  let individualTimeseriesErrorCount = 0;
  const runtimeDeadlineFailures: RuntimeDeadlineFailureSummary = {
    count: 0,
    timeseriesSample: [],
  };
  const logFallbackDiagnostic = (
    level: "info" | "warn",
    message: string,
    context: Record<string, unknown>,
  ) => {
    log[level](message, context);
    const structuredEntry = JSON.stringify({
      ts: new Date().toISOString(),
      connector_code: requestedConnectorCode,
      message,
      ...context,
    });
    if (level === "warn") {
      console.warn(structuredEntry);
    } else {
      console.info(structuredEntry);
    }
  };

  try {
    if (!SUPABASE_URL || !SUPABASE_PRIVILEGED_KEY) {
      status = 500;
      responsePayload = { error: "Missing SUPABASE_URL or SB_SECRET_KEY." };
      log.error("Missing Supabase configuration.");
    } else {
      const payload = await readJson(req);
      requestedConnectorId = asString(payload?.connector_id);
      requestedConnectorCode = asString(payload?.connector_code) || SOS_CONNECTOR_CODE;
      requestedConnectorLabel = asString(payload?.connector_label) || SOS_SERVICE_LABEL;
      requestedWindowHours = asNumber(payload?.window_hours, undefined);
      requestedPollutants = parseList(payload?.pollutants);
      requestedLimit = asNumber(payload?.timeseries_limit, undefined);
      const requestedTimeseriesTokens = parseList(payload?.timeseries_ids);
      requestedTimeseriesIds = parsePositiveIntList(payload?.timeseries_ids);
      if (
        requestedTimeseriesTokens?.length &&
        (!requestedTimeseriesIds || requestedTimeseriesIds.length < requestedTimeseriesTokens.length)
      ) {
        log.warn("Ignored non-numeric timeseries_ids tokens.", {
          requested_tokens: requestedTimeseriesTokens.length,
          accepted_ids: requestedTimeseriesIds?.length ?? 0,
        });
      }

      log.info("Poll request", {
        connector_id: requestedConnectorId ?? null,
        connector_code: requestedConnectorCode,
        connector_label: requestedConnectorLabel,
        window_hours: requestedWindowHours ?? null,
        pollutants: requestedPollutants?.length ?? null,
        timeseries_ids: requestedTimeseriesIds?.length ?? null,
        timeseries_limit: requestedLimit ?? null,
      });

      connector = await loadConnector(
        requestedConnectorId,
        requestedConnectorCode,
        requestedConnectorLabel,
      );
      if (!connector) {
        status = 404;
        responsePayload = { error: "Connector not found." };
        log.warn("Connector not found.");
      } else if (connector.poll_enabled === false) {
        status = 200;
        responsePayload = { status: "poll_disabled", connector_id: connector.id };
        log.info("Polling disabled for connector.", { connector_id: connector.id });
      } else {
        const observsBufferFlushRows = clampPositiveInt(
          Number(Deno.env.get("OBSERVS_BUFFER_FLUSH_ROWS") ?? ""),
          DEFAULT_OBSERVS_BUFFER_FLUSH_ROWS,
        );
        const observsRowsPending: ObservsObservationRow[] = [];
        let observsFlushes = 0;
        let observsWritten = 0;
        let observsReceiptsUpserted = 0;
        let observsEnqueued = 0;
        const flushPendingObservsRows = async (
          reason: string,
          force = false,
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
                log.warn("Observs dual-write warning", {
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
            errors.push("observs_flush_failed");
            log.warn("Observs dual-write flush failed.", {
              message,
              rows: rows.length,
              reason,
            });
            await errorLogger.logError({
              source: "edge",
              severity: "error",
              message: "Observs dual-write flush failed.",
              context: {
                connector_id: connector?.id ?? requestedConnectorId ?? null,
                rows: rows.length,
                reason,
                error: message,
              },
              connector_code: connector?.connector_code ??
                requestedConnectorCode ?? SOS_CONNECTOR_CODE,
              connector_id: connector?.id ?? requestedConnectorId ?? null,
            });
          }
        };

        let shouldPoll = true;
        const pollWindow = requestedWindowHours ?? connector.poll_window_hours ?? DEFAULT_WINDOW_HOURS;
        const effectiveLimit = requestedLimit ?? connector.poll_timeseries_batch_size ?? undefined;
        const baseUrl = (connector.service_url || SOS_BASE_URL).replace(/\/$/, "");
        const now = new Date();
        const windowStart = new Date(now.getTime() - pollWindow * 60 * 60 * 1000);
        const connectorForRun = connector;
        let htmlFallbackProbeFailure: SosFetchFailure | null = null;

        const recordUnrecoveredProbeFailure = async (
          failure: SosFetchFailure,
        ) => {
          const upstreamStatus = failure.upstreamStatus;
          const connectorHttpStatus = connectorHttpStatusForProbe(failure);
          errors.push(`upstream_probe_failed:${upstreamStatus ?? "unknown"}`);
          log.warn("UK-AIR SOS upstream probe failure was not recovered.", {
            connector_id: connectorForRun.id,
            upstream_status: upstreamStatus,
            upstream_failure_kind: failure.kind,
            connector_http_status: connectorHttpStatus,
            upstream_error: failure.message,
          });
          await errorLogger.logError({
            source: "edge",
            severity: "error",
            message: "UK-AIR SOS upstream probe failure was not recovered.",
            context: {
              connector_id: connectorForRun.id,
              upstream_status: upstreamStatus,
              upstream_failure_kind: failure.kind,
              connector_http_status: connectorHttpStatus,
              upstream_error: failure.message,
            },
            connector_code: connectorForRun.connector_code ??
              requestedConnectorCode ?? SOS_CONNECTOR_CODE,
            connector_id: connectorForRun.id,
          });
          status = connectorHttpStatus;
          responsePayload = {
            status: "upstream_unavailable",
            run_message: upstreamStatus === 502
              ? "HTTP 502 Gateway Failure"
              : "Upstream unavailable",
            connector_id: connectorForRun.id,
            series_polled: 0,
            observations_upserted: 0,
            errors,
            upstream_status: upstreamStatus,
            upstream_failure_kind: failure.kind,
            connector_http_status: connectorHttpStatus,
            upstream_error: failure.message,
          };
        };

        let series = await loadTimeseries(connector.id);
        if (requestedTimeseriesIds?.length) {
          const requestedSet = new Set(requestedTimeseriesIds);
          series = series.filter((row) => requestedSet.has(row.id));
        }

        if (requestedPollutants?.length) {
          const allowedPhenomena = await loadPhenomena(connector.id, requestedPollutants);
          if (allowedPhenomena.size === 0) {
            status = 200;
            responsePayload = { status: "no_matching_pollutants", connector_id: connector.id };
            log.warn("No matching pollutants for connector.", { connector_id: connector.id });
            shouldPoll = false;
          } else {
            series = series.filter((row) =>
              row.phenomenon_id && allowedPhenomena.has(row.phenomenon_id)
            );
          }
        }

        if (shouldPoll) {
          const probe = await probeSosUpstream(baseUrl, rawRecorder, runtimeDeadline);
          if (!probe.ok) {
            if (isUkAirHtmlFallbackProbeFailure(probe.failure)) {
              htmlFallbackProbeFailure = probe.failure;
              logFallbackDiagnostic(
                "warn",
                "UK-AIR SOS upstream probe failed; starting HTML fallback.",
                {
                  connector_id: connector.id,
                  upstream_status: probe.failure.upstreamStatus,
                  upstream_failure_kind: probe.failure.kind,
                  connector_http_status: connectorHttpStatusForProbe(
                    probe.failure,
                  ),
                  upstream_error: probe.failure.message,
                },
              );
            } else {
              shouldPoll = false;
              log.warn("UK-AIR SOS probe failure is not eligible for HTML fallback.", {
                connector_id: connector.id,
                upstream_status: probe.failure.upstreamStatus,
                upstream_failure_kind: probe.failure.kind,
                connector_http_status: connectorHttpStatusForProbe(
                  probe.failure,
                ),
                upstream_error: probe.failure.message,
              });
              await recordUnrecoveredProbeFailure(probe.failure);
            }
          }
        }

        if (shouldPoll) {
          const checkpointCandidates = requestedTimeseriesIds?.length ? series.slice() : [];
          const successfullyPolledTimeseriesIds = new Set<number>();
          let htmlFallbackIncomplete = false;
          const beforeRecencyFilter = series.length;
          const withRecentLastValue = series.filter((row) => {
            if (!row.last_value_at) {
              skippedNoLastValueAt += 1;
              return false;
            }
            const parsed = new Date(row.last_value_at);
            if (Number.isNaN(parsed.getTime())) {
              skippedNoLastValueAt += 1;
              return false;
            }
            if (parsed < windowStart) {
              skippedStaleLastValueAt += 1;
              return false;
            }
            return true;
          });
          const staleLastValueCandidates = series.filter((row) => {
            if (!row.last_value_at) {
              return false;
            }
            const parsed = new Date(row.last_value_at);
            if (Number.isNaN(parsed.getTime())) {
              return false;
            }
            return parsed < windowStart;
          });

          const nullLastValueCandidates = series.filter((row) => !row.last_value_at);
          const bootstrapBatchSize = clampPositiveInt(
            SOS_BOOTSTRAP_NULL_LAST_VALUE_BATCH,
            DEFAULT_BOOTSTRAP_NULL_LAST_VALUE_BATCH,
          );
          let bootstrapTake = Math.min(
            bootstrapBatchSize,
            nullLastValueCandidates.length,
          );
          if (typeof effectiveLimit === "number" && effectiveLimit > 0) {
            bootstrapTake = Math.min(bootstrapTake, Math.max(1, effectiveLimit));
          }
          const nowBucket = Math.floor(Date.now() / (2 * 60 * 1000));
          const bootstrapRows = takeCircular(
            nullLastValueCandidates,
            nowBucket * Math.max(1, bootstrapTake),
            bootstrapTake,
          );

          if (typeof effectiveLimit === "number" && effectiveLimit > 0) {
            const recentTake = Math.max(0, effectiveLimit - bootstrapRows.length);
            series = withRecentLastValue.slice(0, recentTake).concat(bootstrapRows);
          } else {
            series = withRecentLastValue.concat(bootstrapRows);
          }

          let staleRecoveryApplied = false;
          let staleRecoverySelected = 0;
          if (requestedTimeseriesIds?.length && staleLastValueCandidates.length > 0) {
            if (typeof effectiveLimit === "number" && effectiveLimit > 0) {
              const remainingBudget = Math.max(0, effectiveLimit - series.length);
              if (remainingBudget > 0) {
                const selectedIds = new Set(series.map((row) => row.id));
                const stalePool = staleLastValueCandidates.filter((row) =>
                  !selectedIds.has(row.id)
                );
                const staleFill = takeCircular(
                  stalePool,
                  nowBucket * Math.max(1, remainingBudget),
                  remainingBudget,
                );
                if (staleFill.length > 0) {
                  series = series.concat(staleFill);
                  staleRecoveryApplied = true;
                  staleRecoverySelected = staleFill.length;
                }
              }
            } else if (series.length === 0) {
              series = takeCircular(
                staleLastValueCandidates,
                nowBucket * Math.max(1, staleLastValueCandidates.length),
                staleLastValueCandidates.length,
              );
              staleRecoveryApplied = true;
              staleRecoverySelected = series.length;
            }
          }

          if (beforeRecencyFilter !== series.length || bootstrapRows.length > 0 || staleRecoveryApplied) {
            log.info("Timeseries recency filter applied", {
              total: beforeRecencyFilter,
              remaining_after_filter: withRecentLastValue.length,
              remaining_after_bootstrap: series.length,
              skipped_no_last_value_at: skippedNoLastValueAt,
              skipped_stale_last_value_at: skippedStaleLastValueAt,
              bootstrap_null_last_value_candidates: nullLastValueCandidates.length,
              bootstrap_null_last_value_selected: bootstrapRows.length,
              stale_last_value_candidates: staleLastValueCandidates.length,
              stale_recovery_applied: staleRecoveryApplied,
              stale_recovery_selected: staleRecoverySelected,
              window_hours: pollWindow,
            });
          }

          const timespan = `${windowStart.toISOString()}/${now.toISOString()}`;
          if (rawRecorder) {
            rawRecorder.recordEvent("context", {
              connector_id: connector.id,
              connector_code: connector.connector_code,
              connector_label: connector.label,
              timespan,
              window_hours: pollWindow,
              timeseries_limit: typeof effectiveLimit === "number" ? effectiveLimit : null,
              pollutants: requestedPollutants?.length ? requestedPollutants : "all",
            });
          }

          const writeObservationPoints = async (
            timeseriesId: number,
            points: Array<{
              observed_at: string;
              value: number | null;
              status: string | null;
            }>,
            acquisitionMethod: string,
          ) => {
            if (!points.length) return;
            const connectorIdForObs = connector?.id ??
              requestedConnectorId ?? null;
            if (connectorIdForObs == null) {
              throw new Error(
                "observations upsert failed: connector_id is required",
              );
            }
            let droppedDuplicates = 0;
            const deduped = new Map<string, {
              observed_at: string;
              value: number | null;
              status: string | null;
            }>();
            for (const point of points) {
              if (deduped.has(point.observed_at)) droppedDuplicates += 1;
              deduped.set(point.observed_at, point);
            }
            if (droppedDuplicates) {
              console.warn("Dropping duplicate observations", {
                timeseries_id: timeseriesId,
                dropped: droppedDuplicates,
                total: points.length,
              });
            }
            const observationRows = Array.from(deduped.values()).map((point) => ({
              connector_id: connectorIdForObs,
              timeseries_id: timeseriesId,
              observed_at: point.observed_at,
              value: point.value,
              status: point.status,
            }));
            const writeStats = await writeIngestDbObservations({
              rows: observationRows,
              chunkSize: observationRows.length,
              connectorCode: SOS_CONNECTOR_CODE,
              logger: console,
              config: { minimumAttemptRuntimeMs: DEFAULT_TIMEOUT_MS },
              runtimeBudget: {
                shouldStop,
                remainingRuntimeMs: () =>
                  Math.max(0, runtimeDeadline - Date.now()),
              },
              requestBodyBytes: (chunk: Record<string, unknown>[]) =>
                serializedJsonUtf8Bytes(
                  buildCompactObservationRpcArgsV2(chunk, acquisitionMethod),
                ),
              writeChunk: async (chunk: Record<string, unknown>[]) => {
                const { data, error } = await postgrestRequest<
                  Array<{ observations_upserted: number }>
                >(
                  "POST",
                  "rpc/uk_aq_rpc_observations_compact_upsert_v2",
                  {},
                  buildCompactObservationRpcArgsV2(chunk, acquisitionMethod),
                  undefined,
                  "uk_aq_public",
                );
                if (error) throw error;
                observationsUpserted += recordSosObservationChanges(
                  data,
                  timeseriesId,
                  changedTimeseriesIds,
                );
              },
            });
            mergeIngestDbObservationWriteStats(
              ingestDbObservationWriteStats,
              writeStats,
            );
            const observsRows = observationRows.map((point) => {
              const numericValue = Number(point.value);
              return {
                connector_id: Number(point.connector_id),
                timeseries_id: Number(point.timeseries_id),
                observed_at: String(point.observed_at),
                value: Number.isFinite(numericValue) ? numericValue : null,
                status: point.status == null ? null : String(point.status),
              } satisfies ObservsObservationRow;
            });
            observsRowsPending.push(...observsRows);
          };

          if (!htmlFallbackProbeFailure) {
            timeBudgetHit = await runPool(
              series,
              CONCURRENCY_LIMIT,
              async (row) => {
                if (shouldStop()) {
                  return;
                }
                try {
                  const sourceId = row.timeseries_ref || String(row.id);
                  const data = await fetchJson(
                    baseUrl,
                    `/timeseries/${encodeURIComponent(sourceId)}/getData`,
                    { timespan, format: "tvp" },
                    rawRecorder,
                    { deadlineMs: runtimeDeadline },
                  );
                  const points = parseDatapoints(data?.values, row.id);
                  await writeObservationPoints(
                    row.id,
                    points,
                    SOS_PRIMARY_ACQUISITION_METHOD,
                  );
                  await upsertLastValue(
                    row.id,
                    data,
                    points,
                    errorLogger,
                    connector?.id ?? requestedConnectorId ?? null,
                    connector?.connector_code ??
                      requestedConnectorCode ?? SOS_CONNECTOR_CODE,
                  );
                  polled += 1;
                  successfullyPolledTimeseriesIds.add(Number(row.id));
                } catch (err) {
                  if (isIngestDbObservationWriteError(err)) {
                    const writeError = err as {
                      stats?: Record<string, unknown>;
                    };
                    if (writeError.stats) {
                      mergeIngestDbObservationWriteStats(
                        ingestDbObservationWriteStats,
                        writeError.stats,
                      );
                    }
                    ingestDbObservationWriteFailed = true;
                  }
                  const failure = asSosFetchFailure(err);
                  if (isRuntimeDeadlineFailure(failure)) {
                    addRuntimeDeadlineFailure(
                      runtimeDeadlineFailures,
                      row.id,
                      RUNTIME_DEADLINE_TIMESERIES_SAMPLE_LIMIT,
                    );
                    return;
                  }
                  if (failure.upstreamStatus === 502) {
                    gateway502Failures += 1;
                  }
                  if (isIndividuallyReportedTimeseriesFailure(failure)) {
                    individualTimeseriesErrorCount += 1;
                  }
                  errors.push(`${row.id}: upsert_failed`);
                  console.warn(`Poll failed for ${row.id}: ${failure.message}`);
                  await errorLogger.logError({
                    source: "edge",
                    severity: "error",
                    message: `Poll failed for timeseries ${row.id}.`,
                    stack: err instanceof Error ? err.stack : undefined,
                    context: {
                      timeseries_ref: row.timeseries_ref,
                      timespan,
                      connector_id: connector?.id ?? requestedConnectorId ?? null,
                      upstream_status: failure.upstreamStatus,
                      upstream_failure_kind: failure.kind,
                      upstream_error: failure.message,
                    },
                    connector_code: connector?.connector_code ??
                      requestedConnectorCode ?? SOS_CONNECTOR_CODE,
                    connector_id: connector?.id ?? requestedConnectorId ?? null,
                    timeseries_id: row.id,
                  });
                }
              },
              shouldStop,
            );
          } else {
            const currentUtcDay = new Date().toISOString().slice(0, 10);
            const fallbackFailedTimeseriesIds = new Set<number>();
            const recordFallbackTimeseriesFailure = (
              timeseriesId: number,
              reason: string,
              individual = true,
            ) => {
              if (fallbackFailedTimeseriesIds.has(timeseriesId)) return;
              fallbackFailedTimeseriesIds.add(timeseriesId);
              errors.push(`${timeseriesId}:html_fallback_${reason}`);
              if (individual) individualTimeseriesErrorCount += 1;
            };

            try {
              const bridgeRows = await loadUkAirHtmlBridgeRows(currentUtcDay);
              const mapping = resolveUkAirHtmlMappings(series, bridgeRows);
              for (const id of mapping.unmappedTimeseriesIds) {
                recordFallbackTimeseriesFailure(id, "unmapped");
              }
              for (const id of mapping.ambiguousTimeseriesIds) {
                recordFallbackTimeseriesFailure(id, "ambiguous_mapping");
              }
              for (const id of mapping.coWriteDisabledTimeseriesIds) {
                recordFallbackTimeseriesFailure(id, "co_write_disabled");
              }
              for (const id of mapping.unsafeUnitTimeseriesIds) {
                recordFallbackTimeseriesFailure(id, "unsafe_unit");
              }

              const workBySite = new Map<string, UkAirHtmlMappedWork[]>();
              for (const work of mapping.mappedWork) {
                const siteWork = workBySite.get(work.siteRef) ?? [];
                siteWork.push(work);
                workBySite.set(work.siteRef, siteWork);
              }
              logFallbackDiagnostic("info", "UK-AIR HTML fallback mapping resolved.", {
                current_utc_day: currentUtcDay,
                selected_timeseries: series.length,
                bridge_rows: bridgeRows.length,
                mapped_timeseries: mapping.mappedWork.length,
                unmapped_timeseries: mapping.unmappedTimeseriesIds.length,
                ambiguous_timeseries: mapping.ambiguousTimeseriesIds.length,
                co_write_disabled_timeseries:
                  mapping.coWriteDisabledTimeseriesIds.length,
                unsafe_unit_timeseries: mapping.unsafeUnitTimeseriesIds.length,
                selected_sites: workBySite.size,
                unmapped_sample: mapping.unmappedTimeseriesIds.slice(0, 10),
                ambiguous_sample: mapping.ambiguousTimeseriesIds.slice(0, 10),
                co_write_disabled_sample:
                  mapping.coWriteDisabledTimeseriesIds.slice(0, 10),
                unsafe_unit_sample:
                  mapping.unsafeUnitTimeseriesIds.slice(0, 10),
              });

              const siteEntries = Array.from(workBySite.entries());
              timeBudgetHit = await runPool(
                siteEntries,
                CONCURRENCY_LIMIT,
                async ([siteRef, siteWork]) => {
                  if (shouldStop()) return;
                  const siteStartedAt = Date.now();
                  try {
                    const page = await fetchUkAirHtmlPage(
                      siteRef,
                      runtimeDeadline,
                    );
                    const parsed = parseUkAirHtmlChart(
                      page.html,
                      currentUtcDay,
                      Date.now(),
                    );
                    const ambiguousPageCodes = new Set(
                      parsed.ambiguousPollutantCodes,
                    );
                    const parsedByPollutant = new Map(
                      parsed.series
                        .filter((item) =>
                          !ambiguousPageCodes.has(item.pollutantCode)
                        )
                        .map((item) => [item.pollutantCode, item] as const),
                    );
                    const selectedCodes = new Set(
                      siteWork.map((work) => work.pollutantCode),
                    );
                    const ignoredUnselectedSeries = parsed.series.filter(
                      (item) => !selectedCodes.has(item.pollutantCode),
                    ).length;
                    logFallbackDiagnostic("info", "UK-AIR HTML fallback page parsed.", {
                      site_ref: siteRef,
                      http_status: page.status,
                      duration_ms: page.durationMs,
                      chart_found: true,
                      chart_invocations: parsed.chartInvocationCount,
                      chart_candidates: parsed.chartCandidateCount,
                      selected_series: siteWork.length,
                      recognised_series: parsed.series.length,
                      ignored_unselected_series: ignoredUnselectedSeries,
                      unknown_series: parsed.unknownSeriesNames.slice(0, 10),
                      rejected_series: parsed.rejectedSeriesCount,
                      ambiguous_series_codes:
                        parsed.ambiguousPollutantCodes.slice(0, 10),
                    });

                    for (const work of siteWork) {
                      if (ambiguousPageCodes.has(work.pollutantCode)) {
                        recordFallbackTimeseriesFailure(
                          work.timeseries.id,
                          "ambiguous_page_series",
                        );
                        continue;
                      }
                      const htmlSeries = parsedByPollutant.get(
                        work.pollutantCode,
                      );
                      if (!htmlSeries) {
                        recordFallbackTimeseriesFailure(
                          work.timeseries.id,
                          "series_missing",
                        );
                        continue;
                      }
                      logFallbackDiagnostic("info", "UK-AIR HTML fallback series parsed.", {
                        site_ref: siteRef,
                        timeseries_id: work.timeseries.id,
                        pollutant_code: work.pollutantCode,
                        html_series: htmlSeries.htmlSeriesName,
                        destination_unit: work.timeseries.uom,
                        total_points: htmlSeries.totalPointCount,
                        valid_points: htmlSeries.points.length,
                        null_points: htmlSeries.nullPointCount,
                        rejected_points: htmlSeries.rejectedPointCount,
                        future_points: htmlSeries.futurePointCount,
                        out_of_day_points: htmlSeries.outOfDayPointCount,
                        newest_eligible_timestamp:
                          htmlSeries.newestEligibleTimestamp,
                      });
                      if (!htmlSeries.writeEnabled || !htmlSeries.points.length) {
                        recordFallbackTimeseriesFailure(
                          work.timeseries.id,
                          htmlSeries.writeEnabled
                            ? "no_eligible_points"
                            : "write_disabled",
                        );
                        continue;
                      }
                      const observationPoints = htmlSeries.points.map((point) => ({
                        observed_at: point.observed_at,
                        value: point.value,
                        status: null,
                      }));
                      try {
                        await writeObservationPoints(
                          work.timeseries.id,
                          observationPoints,
                          SOS_HTML_FALLBACK_ACQUISITION_METHOD,
                        );
                        const existingLastValueAt = work.timeseries.last_value_at
                          ? Date.parse(work.timeseries.last_value_at)
                          : Number.NaN;
                        const newestPointAt = Date.parse(
                          observationPoints[observationPoints.length - 1]
                            .observed_at,
                        );
                        const latestPoints = Number.isFinite(existingLastValueAt) &&
                            newestPointAt <= existingLastValueAt
                          ? []
                          : observationPoints;
                        await upsertLastValue(
                          work.timeseries.id,
                          {},
                          latestPoints,
                          errorLogger,
                          connector?.id ?? requestedConnectorId ?? null,
                          connector?.connector_code ??
                            requestedConnectorCode ?? SOS_CONNECTOR_CODE,
                        );
                        polled += 1;
                        successfullyPolledTimeseriesIds.add(
                          work.timeseries.id,
                        );
                      } catch (error) {
                        if (isIngestDbObservationWriteError(error)) {
                          const writeError = error as {
                            stats?: Record<string, unknown>;
                          };
                          if (writeError.stats) {
                            mergeIngestDbObservationWriteStats(
                              ingestDbObservationWriteStats,
                              writeError.stats,
                            );
                          }
                          ingestDbObservationWriteFailed = true;
                        }
                        const failure = asSosFetchFailure(error);
                        if (isRuntimeDeadlineFailure(failure)) {
                          addRuntimeDeadlineFailure(
                            runtimeDeadlineFailures,
                            work.timeseries.id,
                            RUNTIME_DEADLINE_TIMESERIES_SAMPLE_LIMIT,
                          );
                          recordFallbackTimeseriesFailure(
                            work.timeseries.id,
                            "runtime_deadline",
                            false,
                          );
                          continue;
                        }
                        recordFallbackTimeseriesFailure(
                          work.timeseries.id,
                          "observation_write_failed",
                        );
                        logFallbackDiagnostic("warn", "UK-AIR HTML fallback series write failed.", {
                          site_ref: siteRef,
                          timeseries_id: work.timeseries.id,
                          pollutant_code: work.pollutantCode,
                          error: boundMessage(error),
                        });
                        await errorLogger.logError({
                          source: "edge",
                          severity: "error",
                          message:
                            "UK-AIR HTML fallback observation write failed.",
                          stack: error instanceof Error
                            ? error.stack
                            : undefined,
                          context: {
                            site_ref: siteRef,
                            pollutant_code: work.pollutantCode,
                            timeseries_id: work.timeseries.id,
                            error: boundMessage(error),
                          },
                          connector_code: connector?.connector_code ??
                            requestedConnectorCode ?? SOS_CONNECTOR_CODE,
                          connector_id: connector?.id ??
                            requestedConnectorId ?? null,
                          timeseries_id: work.timeseries.id,
                        });
                      }
                    }
                  } catch (error) {
                    const failure = asSosFetchFailure(error);
                    const parserCode = error instanceof UkAirHtmlParseError
                      ? error.code
                      : null;
                    const isRuntimeDeadline = isRuntimeDeadlineFailure(failure);
                    for (const work of siteWork) {
                      if (isRuntimeDeadline) {
                        addRuntimeDeadlineFailure(
                          runtimeDeadlineFailures,
                          work.timeseries.id,
                          RUNTIME_DEADLINE_TIMESERIES_SAMPLE_LIMIT,
                        );
                      }
                      recordFallbackTimeseriesFailure(
                        work.timeseries.id,
                        parserCode ?? (isRuntimeDeadline
                          ? "runtime_deadline"
                          : "site_failed"),
                        !isRuntimeDeadline,
                      );
                    }
                    logFallbackDiagnostic("warn", "UK-AIR HTML fallback site failed.", {
                      site_ref: siteRef,
                      affected_timeseries: siteWork.length,
                      duration_ms: Math.max(0, Date.now() - siteStartedAt),
                      chart_found: parserCode === "chart_not_found" ? false : null,
                      parser_error: parserCode,
                      upstream_status: failure.upstreamStatus,
                      failure_kind: failure.kind,
                      error: boundMessage(error),
                    });
                    if (!isRuntimeDeadline) {
                      await errorLogger.logError({
                        source: "edge",
                        severity: "error",
                        message: "UK-AIR HTML fallback site failed.",
                        context: {
                          site_ref: siteRef,
                          affected_timeseries: siteWork.length,
                          parser_error: parserCode,
                          upstream_status: failure.upstreamStatus,
                          failure_kind: failure.kind,
                          error: boundMessage(error),
                        },
                        connector_code: connector?.connector_code ??
                          requestedConnectorCode ?? SOS_CONNECTOR_CODE,
                        connector_id: connector?.id ??
                          requestedConnectorId ?? null,
                      });
                    }
                  }
                },
                shouldStop,
              );
            } catch (error) {
              for (const row of series) {
                recordFallbackTimeseriesFailure(
                  row.id,
                  "bridge_lookup_failed",
                );
              }
              logFallbackDiagnostic(
                "warn",
                "UK-AIR HTML fallback could not resolve bridge mappings.",
                {
                  current_utc_day: currentUtcDay,
                  selected_timeseries: series.length,
                  error: boundMessage(error),
                },
              );
            }
            htmlFallbackIncomplete = successfullyPolledTimeseriesIds.size <
              series.length;
            logFallbackDiagnostic("info", "UK-AIR HTML fallback completed.", {
              current_utc_day: currentUtcDay,
              selected_timeseries: series.length,
              recovered_timeseries: successfullyPolledTimeseriesIds.size,
              failed_timeseries: fallbackFailedTimeseriesIds.size,
              observations_upserted: observationsUpserted,
              timeseries_updated: changedTimeseriesIds.size,
              outcome: successfullyPolledTimeseriesIds.size === 0
                ? "unrecovered"
                : htmlFallbackIncomplete
                ? "partial"
                : "recovered",
            });
          }

          const runtimeBudgetExceeded = runtimeBudgetStopObserved(
            timeBudgetHit,
            runtimeDeadlineFailures.count,
          );
          if (runtimeBudgetExceeded) {
            log.warn("Stopping UK-AIR SOS poll early (runtime budget exceeded).", {
              max_runtime_seconds: maxRuntimeSeconds,
              runtime_deadline_failure_count: runtimeDeadlineFailures.count,
              runtime_deadline_timeseries_sample: runtimeDeadlineFailures.timeseriesSample,
            });
          }
          await flushPendingObservsRows("run_complete", true);

          if (runtimeBudgetExceeded) {
            errors.push("runtime_budget_exceeded");
            await errorLogger.logError({
              source: "edge",
              severity: "error",
              message: "UK-AIR SOS runtime budget exceeded while polling timeseries.",
              context: {
                connector_id: connector.id,
                max_runtime_seconds: maxRuntimeSeconds,
                runtime_deadline_failure_count: runtimeDeadlineFailures.count,
                runtime_deadline_timeseries_sample: runtimeDeadlineFailures.timeseriesSample,
                series_selected: series.length,
                series_polled: polled,
              },
              connector_code: connector.connector_code ?? requestedConnectorCode ?? SOS_CONNECTOR_CODE,
              connector_id: connector.id,
            });
          }

          const htmlFallbackUnrecovered = Boolean(
            htmlFallbackProbeFailure &&
              successfullyPolledTimeseriesIds.size === 0 &&
              observationsUpserted === 0,
          );
          if (htmlFallbackUnrecovered && htmlFallbackProbeFailure) {
            await recordUnrecoveredProbeFailure(htmlFallbackProbeFailure);
          } else {
            const successfulCheckpointCandidates = checkpointCandidates.filter(
              (row) => successfullyPolledTimeseriesIds.has(Number(row.id)),
            );
            if (successfulCheckpointCandidates.length) {
              await upsertSosTimeseriesCheckpoints(
                successfulCheckpointCandidates.map((row) => ({ id: row.id })),
                now.toISOString(),
                errorLogger,
                connector?.id ?? requestedConnectorId ?? null,
                connector?.connector_code ??
                  requestedConnectorCode ?? SOS_CONNECTOR_CODE,
              );
            }

            if (!ingestDbObservationWriteFailed) {
              const { error: pollUpdateError } = await postgrestRequest(
                "PATCH",
                "connectors",
                { id: `eq.${connector.id}` },
                { last_polled_at: now.toISOString() },
                "return=minimal",
              );
              if (pollUpdateError) {
                errors.push("connector last_polled_at update failed");
                await errorLogger.logError({
                  source: "edge",
                  severity: "error",
                  message: "Failed to update connectors.last_polled_at.",
                  context: {
                    connector_id: connector.id,
                    error: pollUpdateError.message,
                  },
                  connector_code: connector.connector_code ??
                    requestedConnectorCode ?? SOS_CONNECTOR_CODE,
                  connector_id: connector.id,
                });
              }
            }

            const hardGateway502Failure = gateway502Failures > 0 && polled === 0;
            const partial = runtimeBudgetExceeded || htmlFallbackIncomplete;
            status = hardGateway502Failure ? 502 : errors.length ? 207 : 200;
            responsePayload = {
              status: hardGateway502Failure ? "gateway_failure" : "ok",
              run_message: hardGateway502Failure
                ? "HTTP 502 Gateway Failure"
                : htmlFallbackProbeFailure
                ? htmlFallbackIncomplete
                  ? "UK-AIR HTML fallback partially recovered SOS probe failure"
                  : "UK-AIR HTML fallback recovered SOS probe failure"
                : null,
              connector_id: connector.id,
              series_polled: polled,
              observations_upserted: observationsUpserted,
              timeseries_updated: changedTimeseriesIds.size,
              ingestdb_observation_write: ingestDbObservationWriteStats,
              cross_database_transaction: false,
              observs_written: observsWritten,
              observs_receipts_upserted: observsReceiptsUpserted,
              observs_enqueued: observsEnqueued,
              observs_flushes: observsFlushes,
              http_502_failures: gateway502Failures,
              errors,
              individual_error_count: individualTimeseriesErrorCount,
              runtime_deadline_failure_count: runtimeDeadlineFailures.count,
              runtime_deadline_timeseries_sample:
                runtimeDeadlineFailures.timeseriesSample,
              partial,
              stopped_reason: runtimeBudgetExceeded
                ? "runtime_budget_exceeded"
                : null,
              ...(htmlFallbackProbeFailure
                ? {
                  upstream_status: htmlFallbackProbeFailure.upstreamStatus,
                  upstream_failure_kind: htmlFallbackProbeFailure.kind,
                  connector_http_status: connectorHttpStatusForProbe(
                    htmlFallbackProbeFailure,
                  ),
                }
                : {}),
            };
          }
        }
      }
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    errors.push(message);
    status = 500;
    responsePayload = { error: "Internal server error." };
    log.error("Unhandled error during poll.", { message });
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
      connector_code: requestedConnectorCode ?? SOS_CONNECTOR_CODE,
      connector_id: requestedConnectorId ?? null,
    });
  } finally {
    log.info("Poll summary", {
      connector_id: connector?.id ?? requestedConnectorId ?? null,
      series_polled: polled,
      observations_upserted: observationsUpserted,
      timeseries_updated: changedTimeseriesIds.size,
      errors: errors.length,
    });
    if (errors.length) {
      log.warn("Poll errors", { sample: errors.slice(0, 25) });
    }
    let accessToken: string | null = null;
    const resolvedConnectorCode = connector?.connector_code
      ?? requestedConnectorCode
      ?? SOS_CONNECTOR_CODE;
    const refreshDropbox = dropboxConfig
      ? () => dropboxRefreshAccessToken(dropboxConfig)
      : undefined;
    if (dropboxConfig) {
      try {
        accessToken = await dropboxRefreshAccessToken(dropboxConfig);
      } catch (err) {
        console.warn("Dropbox token request failed:", err);
      }
    }
    if (accessToken) {
      accessToken = await uploadDropboxLog(
        accessToken,
        log,
        connector?.id ?? requestedConnectorId ?? null,
        resolvedConnectorCode,
        errorLogger,
        refreshDropbox,
      );
      await uploadDropboxRaw(
        accessToken,
        rawRecorder,
        connector?.id ?? requestedConnectorId ?? null,
        resolvedConnectorCode,
        errorLogger,
        refreshDropbox,
      );
    }
  }

  return json(responsePayload, status);
});

async function readJson(req: Request): Promise<PollRequest | null> {
  try {
    return await req.json();
  } catch {
    return null;
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...cacheControlHeaders(status),
    },
  });
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

type FetchJsonOptions = {
  attempts?: number;
  deadlineMs?: number;
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

function errorMessage(error: unknown): string {
  return boundMessage(error);
}

function isRetryableFetchFailure(error: unknown): boolean {
  return asSosFetchFailure(error).retryable;
}

function asString(value: unknown): string | undefined {
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }
  return undefined;
}

function asNumber(value: unknown, fallback: number | undefined): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function parseList(value: unknown): string[] | undefined {
  if (Array.isArray(value)) {
    return value.map((item) => String(item).trim()).filter(Boolean);
  }
  if (typeof value === "string") {
    const parts = value.split(",").map((item) => item.trim()).filter(Boolean);
    return parts.length ? parts : undefined;
  }
  return undefined;
}

function parsePositiveIntList(value: unknown): number[] | undefined {
  const tokens = parseList(value);
  if (!tokens?.length) {
    return undefined;
  }
  const parsed = tokens
    .map((token) => Number(token))
    .filter((candidate) => Number.isFinite(candidate) && candidate > 0)
    .map((candidate) => Math.trunc(candidate));
  if (!parsed.length) {
    return undefined;
  }
  return Array.from(new Set(parsed));
}

function clampPositiveInt(value: number, fallback: number): number {
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.max(1, Math.trunc(value));
}

function takeCircular<T>(rows: T[], start: number, count: number): T[] {
  if (!rows.length || count <= 0) {
    return [];
  }
  if (count >= rows.length) {
    return rows.slice();
  }
  const normalizedStart = ((start % rows.length) + rows.length) % rows.length;
  const result: T[] = [];
  for (let idx = 0; idx < count; idx += 1) {
    result.push(rows[(normalizedStart + idx) % rows.length]);
  }
  return result;
}

function _normalizeServiceLabel(label: string | undefined): string {
  if (!label) {
    return SOS_SERVICE_LABEL;
  }
  const trimmed = label.trim();
  if (!trimmed) {
    return SOS_SERVICE_LABEL;
  }
  if (trimmed.toLowerCase().startsWith("my timeseries service")) {
    return SOS_SERVICE_LABEL;
  }
  return trimmed;
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
    const filename = rawPath.split("/").pop() ??
      `uk_aq_raw_${DROPBOX_UPLOAD_SOURCE}.jsonl`;
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
          entry.connector_code ?? SOS_CONNECTOR_CODE,
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
    const name = String(entry?.name ?? "");
    if (!name || name === "archive") {
      continue;
    }
    const parsed = parseYmd(name);
    if (!parsed || parsed.getTime() >= cutoff) {
      continue;
    }
    const archiveName = `${name}.zip`;
    const folderPath = String(entry?.path_lower || entry?.path_display || "");
    if (!folderPath) {
      continue;
    }
    if (!archiveNames.has(archiveName)) {
      const zipped = await dropboxDownloadZip(accessToken, folderPath);
      await dropboxUploadFile(accessToken, `${archiveFolder}/${archiveName}`, new Uint8Array(zipped));
      archiveNames.add(archiveName);
    }
    await fetch(DROPBOX_DELETE_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path: folderPath }),
    });
  }

  for (const entry of archiveEntries) {
    if (entry?.[".tag"] !== "file") {
      continue;
    }
    const name = String(entry?.name ?? "");
    if (!name.endsWith(".zip")) {
      continue;
    }
    const parsed = parseYmd(name.slice(0, -4));
    if (!parsed || parsed.getTime() >= archiveCutoff) {
      continue;
    }
    const path = String(entry?.path_lower || entry?.path_display || "");
    if (!path) {
      continue;
    }
    await fetch(DROPBOX_DELETE_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ path }),
    });
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

function parseYmd(value: string): Date | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return null;
  }
  return new Date(Date.UTC(year, month - 1, day));
}

async function loadConnector(
  connectorId: string | undefined,
  connectorCode: string,
  _connectorLabel: string,
): Promise<ConnectorRow | null> {
  const select =
    "id,connector_code,label,display_name,service_url,poll_enabled,poll_window_hours,poll_timeseries_batch_size";
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

  if (connectorCode) {
    const { data } = await postgrestRequest<ConnectorRow[]>("GET", "connectors", {
      select,
      connector_code: `eq.${connectorCode}`,
      limit: "1",
    });
    if (data && data[0]) {
      return data[0];
    }
  }
  return null;
}

async function loadTimeseries(
  connectorId: string,
): Promise<SosTimeseriesRow[]> {
  const rows: SosTimeseriesRow[] = [];
  let offset = 0;
  while (true) {
    const { data, error } = await postgrestRequest<SosTimeseriesRow[]>(
      "GET",
      "timeseries",
      {
        select: "id,timeseries_ref,service_ref,phenomenon_id,last_value_at,uom",
        connector_id: `eq.${connectorId}`,
        ended_at: "is.null",
        limit: String(PAGE_SIZE),
        offset: String(offset),
      },
    );
    if (error) {
      throw new Error(`Failed to load timeseries: ${error.message}`);
    }
    if (!data || data.length === 0) {
      break;
    }
    rows.push(...data.map((row) => ({
      id: Number(row.id),
      timeseries_ref: row.timeseries_ref ? String(row.timeseries_ref) : null,
      service_ref: row.service_ref ? String(row.service_ref) : null,
      phenomenon_id: row.phenomenon_id ? String(row.phenomenon_id) : null,
      last_value_at: row.last_value_at ? String(row.last_value_at) : null,
      uom: row.uom ? String(row.uom) : null,
    })));
    if (data.length < PAGE_SIZE) {
      break;
    }
    offset += PAGE_SIZE;
  }
  return rows;
}

async function loadPhenomena(connectorId: string, filters: string[]): Promise<Set<string>> {
  const needle = new Set(filters.map((value) => value.toLowerCase()));
  const canonicalNeedle = new Set(
    filters
      .map((value) => value.toLowerCase().replace(/[^a-z0-9]+/g, ""))
      .filter(Boolean),
  );
  const { data, error } = await postgrestRequest<
    Array<{
      id: string;
      label: string | null;
      notation: string | null;
      source_label: string | null;
      observed_property?: { code?: string | null; display_name?: string | null } | null;
    }>
  >("GET", "phenomena", {
    select: "id,label,notation,source_label,observed_property:observed_properties(code,display_name)",
    connector_id: `eq.${connectorId}`,
  });
  if (error) {
    throw new Error(`Failed to load phenomena: ${error.message}`);
  }
  const matches = new Set<string>();
  for (const row of data || []) {
    const id = row.id ? String(row.id) : "";
    const label = row.label ? String(row.label) : "";
    const notation = row.notation ? String(row.notation) : "";
    const sourceLabel = row.source_label ? String(row.source_label) : "";
    const observedPropertyCode = row.observed_property?.code
      ? String(row.observed_property.code)
      : "";
    const observedPropertyDisplay = row.observed_property?.display_name
      ? String(row.observed_property.display_name)
      : "";
    const observedPropertyCanonical = observedPropertyCode
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "");
    if (
      (id && needle.has(id.toLowerCase())) ||
      (label && needle.has(label.toLowerCase())) ||
      (notation && needle.has(notation.toLowerCase())) ||
      (sourceLabel && needle.has(sourceLabel.toLowerCase())) ||
      (observedPropertyDisplay && needle.has(observedPropertyDisplay.toLowerCase())) ||
      (observedPropertyCode && needle.has(observedPropertyCode.toLowerCase())) ||
      (observedPropertyCanonical && canonicalNeedle.has(observedPropertyCanonical))
    ) {
      if (id) {
        matches.add(id);
      }
    }
  }
  return matches;
}

function _extractList(payload: unknown, keys: string[]): Array<Record<string, unknown>> {
  if (Array.isArray(payload)) {
    return payload as Array<Record<string, unknown>>;
  }
  if (payload && typeof payload === "object") {
    for (const key of keys) {
      const items = (payload as Record<string, unknown>)[key];
      if (Array.isArray(items)) {
        return items as Array<Record<string, unknown>>;
      }
    }
  }
  return [];
}

async function fetchJson(
  baseUrl: string,
  path: string,
  params: Record<string, string>,
  recorder?: RawRecorder | null,
  options?: FetchJsonOptions,
): Promise<any> {
  const url = new URL(`${baseUrl.replace(/\/$/, "")}/${path.replace(/^\//, "")}`);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  const attempts = clampPositiveInt(options?.attempts ?? FETCH_RETRY_ATTEMPTS, FETCH_RETRY_ATTEMPTS);
  const deadlineMs = options?.deadlineMs;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const remainingBudgetMs = deadlineMs == null
      ? Number.POSITIVE_INFINITY
      : deadlineMs - Date.now();
    if (remainingBudgetMs <= MIN_FETCH_TIMEOUT_MS) {
      throw new SosFetchFailure({
        kind: "runtime_deadline",
        message: "Runtime budget exhausted before UK-AIR SOS fetch completed.",
      });
    }
    const timeoutMs = Number.isFinite(remainingBudgetMs)
      ? Math.max(MIN_FETCH_TIMEOUT_MS, Math.min(DEFAULT_TIMEOUT_MS, remainingBudgetMs - 250))
      : DEFAULT_TIMEOUT_MS;
    const timeoutKind = timeoutMs < DEFAULT_TIMEOUT_MS
      ? "runtime_deadline"
      : "request_timeout";
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      let resp: Response;
      try {
        resp = await fetch(url.toString(), { signal: controller.signal });
      } catch (err) {
        if (controller.signal.aborted) {
          throw new SosFetchFailure({
            kind: timeoutKind,
            message: timeoutKind === "runtime_deadline"
              ? "Runtime budget exhausted during UK-AIR SOS fetch."
              : "UK-AIR SOS request timed out.",
            retryable: timeoutKind === "request_timeout",
          });
        }
        throw new SosFetchFailure({
          kind: "network",
          message: boundMessage(err),
        });
      }
      const contentType = resp.headers.get("content-type") || "";
      const payload = contentType.includes("application/json")
        ? await resp.json()
        : await resp.text();
      if (recorder) {
        recorder.recordResponse(path, params, resp.status, payload);
      }
      if (!resp.ok) {
        throw new SosFetchFailure({
          kind: "http",
          message: `HTTP ${resp.status} ${resp.statusText}`,
          upstreamStatus: resp.status,
          retryable: RETRYABLE_FETCH_STATUSES.has(resp.status),
        });
      }
      return payload;
    } catch (err) {
      const failure = asSosFetchFailure(err);
      const shouldRetry = attempt < attempts && isRetryableFetchFailure(failure);
      if (!shouldRetry) {
        throw failure;
      }
      const retryDelayMs = Math.min(
        FETCH_RETRY_BACKOFF_MAX_MS,
        FETCH_RETRY_BACKOFF_BASE_MS * (2 ** (attempt - 1)),
      );
      const remainingAfterCatchMs = deadlineMs == null
        ? Number.POSITIVE_INFINITY
        : deadlineMs - Date.now();
      if (remainingAfterCatchMs <= retryDelayMs + MIN_FETCH_TIMEOUT_MS) {
        throw failure;
      }
      if (recorder) {
        recorder.recordEvent("retry", {
          path,
          params,
          attempt,
          delay_ms: retryDelayMs,
          reason: failure.message,
        });
      }
      await sleep(retryDelayMs);
    } finally {
      clearTimeout(timeout);
    }
  }
  throw new Error(`UK-AIR SOS fetch exhausted retries for ${path}.`);
}

async function probeSosUpstream(
  baseUrl: string,
  recorder: RawRecorder | null | undefined,
  runtimeDeadline: number,
): Promise<
  | { ok: true; status: 200 }
  | { ok: false; failure: SosFetchFailure }
> {
  try {
    await fetchJson(baseUrl, "/services", {}, recorder, { attempts: 2, deadlineMs: runtimeDeadline });
    return { ok: true, status: 200 };
  } catch (err) {
    return {
      ok: false,
      failure: asSosFetchFailure(err),
    };
  }
}

async function loadUkAirHtmlBridgeRows(
  currentUtcDay: string,
): Promise<UkAirHtmlBridgeRow[]> {
  const { data, error } = await postgrestRequest<UkAirHtmlBridgeRow[]>(
    "POST",
    "rpc/uk_aq_rpc_sos_uk_air_flat_file_mappings",
    {},
    {
      p_from_day: currentUtcDay,
      p_to_day: currentUtcDay,
      p_pollutant_codes: [...UK_AIR_HTML_BRIDGE_POLLUTANT_CODES],
    },
    undefined,
    "uk_aq_public",
  );
  if (error) {
    throw new Error(`UK-AIR HTML bridge lookup failed: ${error.message}`);
  }
  return Array.isArray(data) ? data : [];
}

let emptySeriesLogs = 0;

function parseDatapoints(
  values: unknown,
  seriesId?: number,
): Array<{ observed_at: string; value: number | null; status: string | null }> {
  let rows = values;
  if (!Array.isArray(rows) && rows && typeof rows === "object") {
    const nested = (rows as Record<string, unknown>).values
      ?? (rows as Record<string, unknown>).data;
    if (Array.isArray(nested)) {
      rows = nested;
    }
  }
  if (!Array.isArray(rows)) {
    logEmptySeries(seriesId, rows, "values not array");
    return [];
  }
  if (rows.length === 0) {
    logEmptySeries(seriesId, { row_count: 0 }, "no rows");
    return [];
  }
  const points: Array<{ observed_at: string; value: number | null; status: string | null }> = [];
  for (const row of rows) {
    if (Array.isArray(row)) {
      if (row.length < 2) {
        continue;
      }
      const observedAt = parseTimestamp(row[0]);
      if (!observedAt) {
        continue;
      }
      const value = toNumber(row[1]);
      const status = row.length > 2 && row[2] != null ? String(row[2]) : null;
      points.push({
        observed_at: observedAt.toISOString(),
        value,
        status,
      });
      continue;
    }
    if (row && typeof row === "object") {
      const record = row as Record<string, unknown>;
      const observedAt = parseTimestamp(
        record.timestamp ?? record.time ?? record.phenomenonTime ?? record.dateTime ?? record.datetime,
      );
      if (!observedAt) {
        continue;
      }
      const value = toNumber(record.value ?? record.result ?? record.v);
      const status = record.status != null ? String(record.status)
        : record.quality != null ? String(record.quality)
        : record.qc != null ? String(record.qc)
        : null;
      points.push({
        observed_at: observedAt.toISOString(),
        value,
        status,
      });
    }
  }
  if (!points.length) {
    logEmptySeries(seriesId, rows[0], "no parsed datapoints");
  }
  return points;
}

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    const nested = record.value ?? record.result ?? record.v;
    if (nested === null || nested === undefined) {
      return null;
    }
    return toNumber(nested);
  }
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return null;
  }
  return num;
}

function parseTimestamp(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    const nested = record.timestamp ?? record.time ?? record.dateTime ?? record.datetime;
    return parseTimestamp(nested);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const timestamp = value < 1e12 ? value * 1000 : value;
    const observedAt = new Date(timestamp);
    return Number.isNaN(observedAt.getTime()) ? null : observedAt;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      const timestamp = numeric < 1e12 ? numeric * 1000 : numeric;
      const observedAt = new Date(timestamp);
      return Number.isNaN(observedAt.getTime()) ? null : observedAt;
    }
    const observedAt = new Date(trimmed);
    return Number.isNaN(observedAt.getTime()) ? null : observedAt;
  }
  return null;
}

function logEmptySeries(seriesId: number | undefined, sample: unknown, reason: string): void {
  if (emptySeriesLogs >= 3) {
    return;
  }
  emptySeriesLogs += 1;
  const log = reason === "no rows" ? console.info : console.warn;
  log("No datapoints parsed", {
    series_id: seriesId ?? null,
    reason,
    sample,
  });
}

async function upsertLastValue(
  seriesId: number,
  _data: Record<string, unknown>,
  points: Array<{ observed_at: string; value: number | null }>,
  errorLogger: { logError: (entry: ErrorLogEntry) => Promise<void> },
  connectorId: string | null,
  connectorCode: string | null,
): Promise<void> {
  // Integrity rule: `timeseries.last_value_at` / `last_value` must only ever
  // be advanced from an actual observation row that this run inserted (or at
  // minimum, parsed from the SOS time-series response). Earlier versions
  // fell back to the SOS API metadata fields `data.lastValueTimestamp` /
  // `data.lastValue` when no points were returned in the requested window —
  // those fields reflect what the SOS source *thinks* its latest value is,
  // not what's actually in our `observations` table. That mismatch poisoned
  // the dashboard (showed PM2.5 as fresh when no observation existed) and
  // the Cloud Run station scheduler (selector and checkpoint logic both read
  // `last_value_at`). We now write only when a real `lastPoint` exists.
  const lastPoint = points.length ? points[points.length - 1] : null;
  if (!lastPoint || !lastPoint.observed_at) {
    return;
  }

  const { error } = lastPoint.value !== null && lastPoint.value !== undefined
    ? await postgrestRequest(
      "POST",
      "rpc/uk_aq_rpc_timeseries_last_values_compact_update_v1",
      {},
      {
        timeseries_ids: [seriesId],
        last_values: [lastPoint.value],
        last_value_ats: [lastPoint.observed_at],
      },
      undefined,
      "uk_aq_public",
    )
    : await postgrestRequest(
      "PATCH",
      "timeseries",
      { id: `eq.${seriesId}` },
      { last_value_at: lastPoint.observed_at },
      "return=minimal",
    );
  if (error) {
    console.warn(`timeseries update failed for ${seriesId}: ${error.message}`);
    await errorLogger.logError({
      source: "edge",
      severity: "error",
      message: "Failed to update timeseries last_value fields.",
      context: {
        timeseries_id: seriesId,
        connector_id: connectorId,
        error: error.message,
      },
      connector_code: connectorCode ?? SOS_CONNECTOR_CODE,
      connector_id: connectorId ?? null,
      timeseries_id: seriesId,
    });
  }
}

async function upsertSosTimeseriesCheckpoints(
  series: Array<{ id: number }>,
  polledAt: string,
  errorLogger: { logError: (entry: ErrorLogEntry) => Promise<void> },
  connectorId: string | null,
  connectorCode: string | null,
): Promise<void> {
  if (!series.length) {
    return;
  }
  for (let idx = 0; idx < series.length; idx += 200) {
    const chunk = series.slice(idx, idx + 200);
    const rows = chunk.map((row) => ({
      timeseries_id: row.id,
      last_polled_at: polledAt,
      updated_at: polledAt,
    }));
    const { error } = await postgrestRequest(
      "POST",
      "sos_timeseries_checkpoints",
      { on_conflict: "timeseries_id" },
      rows,
      "resolution=merge-duplicates,return=minimal",
      UK_AQ_RAW_SCHEMA,
    );
    if (error) {
      console.warn("sos_timeseries_checkpoints upsert failed", error.message);
      await errorLogger.logError({
        source: "edge",
        severity: "error",
        message: "Failed to update sos_timeseries_checkpoints.",
        context: {
          connector_id: connectorId,
          error: error.message,
        },
        connector_code: connectorCode ?? SOS_CONNECTOR_CODE,
        connector_id: connectorId,
      });
    }
  }
}

async function runPool<T>(
  items: T[],
  limit: number,
  worker: (item: T) => Promise<void>,
  shouldStop?: () => boolean,
): Promise<boolean> {
  const executing = new Set<Promise<void>>();
  let stopped = false;
  for (const item of items) {
    if (shouldStop?.()) {
      stopped = true;
      break;
    }
    const task = worker(item).finally(() => executing.delete(task));
    executing.add(task);
    if (executing.size >= limit) {
      await Promise.race(executing);
    }
  }
  await Promise.all(executing);
  return stopped;
}
