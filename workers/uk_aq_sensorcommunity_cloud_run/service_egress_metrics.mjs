import { Buffer } from "node:buffer";

const SOURCE_TYPE = "supabase_postgrest";
const BYPASS_HEADER = "x-ukaq-egress-bypass";

function utcMinute(value) {
  const date = value instanceof Date ? new Date(value.getTime()) : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return new Date().toISOString().replace(/:\d{2}\.\d{3}Z$/, ":00.000Z");
  }
  date.setUTCSeconds(0, 0);
  return date.toISOString();
}

function httpStatusClass(status) {
  const numericStatus = Number(status);
  if (!Number.isInteger(numericStatus) || numericStatus < 100 || numericStatus > 599) {
    return null;
  }
  return `${Math.floor(numericStatus / 100)}xx`;
}

function metricKey(identity) {
  return JSON.stringify([
    identity.bucket_minute,
    identity.env_name,
    identity.project_ref,
    identity.service_name,
    identity.source_name,
    identity.route_name,
    identity.query_name,
    identity.window_label,
    identity.status,
  ]);
}

function responseRowCount(data) {
  return Array.isArray(data) ? data.length : 0;
}

function isLikelyJwt(value) {
  return typeof value === "string" &&
    value.startsWith("eyJ") &&
    value.split(".").length === 3;
}

function boundedWarning(logger, message, details = {}) {
  try {
    logger(JSON.stringify({
      ts: new Date().toISOString(),
      service_name: "ingest.sensorcommunity",
      message,
      ...details,
    }));
  } catch {
    // Monitoring diagnostics must never affect the business workload.
  }
}

export function deriveSupabaseProjectRef(url) {
  try {
    const hostname = new URL(url).hostname.toLowerCase();
    const suffix = ".supabase.co";
    if (!hostname.endsWith(suffix)) {
      return "";
    }
    return hostname.slice(0, -suffix.length).split(".").pop() || "";
  } catch {
    return "";
  }
}

export function createServiceEgressMetricsCollector({
  enabled,
  envName,
  serviceName,
  warningLogger = console.warn,
}) {
  const aggregates = new Map();

  function record({
    completedAt = new Date(),
    durationMs = 0,
    httpStatus,
    projectRef,
    queryName,
    responseData,
    responseOk,
    responseText = "",
    routeName,
    sourceName,
    windowLabel = "",
  }) {
    if (!enabled) {
      return;
    }

    try {
      const numericStatus = Number(httpStatus);
      const status = responseOk === true ||
          (responseOk === undefined && numericStatus >= 200 && numericStatus < 300)
        ? "ok"
        : "error";
      const identity = {
        bucket_minute: utcMinute(completedAt),
        env_name: String(envName || "unknown"),
        project_ref: String(projectRef || ""),
        service_name: String(serviceName),
        source_name: String(sourceName || ""),
        route_name: String(routeName),
        query_name: String(queryName || ""),
        window_label: String(windowLabel || ""),
        status,
      };
      const key = metricKey(identity);
      let aggregate = aggregates.get(key);
      if (!aggregate) {
        aggregate = {
          ...identity,
          source_type: SOURCE_TYPE,
          request_count: 0,
          response_rows: 0,
          response_bytes_est: 0,
          upstream_bytes_est: 0,
          duration_ms: 0,
          error_count: 0,
          httpStatuses: new Set(),
          httpStatusClasses: new Set(),
        };
        aggregates.set(key, aggregate);
      }

      aggregate.request_count += 1;
      aggregate.response_rows += responseRowCount(responseData);
      aggregate.response_bytes_est += Buffer.byteLength(String(responseText || ""), "utf8");
      aggregate.duration_ms += Math.max(0, Math.round(Number(durationMs) || 0));
      aggregate.error_count += status === "error" ? 1 : 0;
      if (Number.isInteger(numericStatus)) {
        aggregate.httpStatuses.add(numericStatus);
      }
      const statusClass = httpStatusClass(numericStatus);
      if (statusClass) {
        aggregate.httpStatusClasses.add(statusClass);
      }
    } catch (error) {
      boundedWarning(warningLogger, "service_egress_metrics_record_warning", {
        error: error instanceof Error ? error.message.slice(0, 300) : "unknown",
      });
    }
  }

  function rows() {
    return Array.from(aggregates.values()).map((aggregate) => {
      const notes = { measurement_method: "body_utf8" };
      if (aggregate.httpStatuses.size === 1) {
        notes.http_status = Array.from(aggregate.httpStatuses)[0];
      }
      if (aggregate.httpStatusClasses.size === 1) {
        notes.http_status_class = Array.from(aggregate.httpStatusClasses)[0];
      }
      return {
        bucket_minute: aggregate.bucket_minute,
        env_name: aggregate.env_name,
        project_ref: aggregate.project_ref,
        service_name: aggregate.service_name,
        source_type: aggregate.source_type,
        source_name: aggregate.source_name,
        route_name: aggregate.route_name,
        query_name: aggregate.query_name,
        window_label: aggregate.window_label,
        status: aggregate.status,
        request_count: aggregate.request_count,
        response_rows: aggregate.response_rows,
        response_bytes_est: aggregate.response_bytes_est,
        upstream_bytes_est: aggregate.upstream_bytes_est,
        duration_ms: aggregate.duration_ms,
        error_count: aggregate.error_count,
        notes,
      };
    });
  }

  async function flush({
    apiKey,
    rpcName,
    schema,
    supabaseUrl,
    timeoutMs = 10_000,
  }) {
    const pendingRows = rows();
    if (!enabled || pendingRows.length === 0) {
      return { attempted: false, persistedRows: 0 };
    }
    if (!apiKey || !rpcName || !schema || !supabaseUrl) {
      boundedWarning(warningLogger, "service_egress_metrics_flush_warning", {
        reason: "missing_metrics_configuration",
        aggregate_rows: pendingRows.length,
      });
      return { attempted: false, persistedRows: 0 };
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const baseUrl = String(supabaseUrl).replace(/\/$/, "");
      const url = `${baseUrl}/rest/v1/rpc/${encodeURIComponent(rpcName)}`;
      const headers = {
        apikey: apiKey,
        Accept: "application/json",
        "Accept-Profile": schema,
        "Content-Type": "application/json",
        "Content-Profile": schema,
        [BYPASS_HEADER]: "1",
      };
      if (isLikelyJwt(apiKey)) {
        headers.Authorization = `Bearer ${apiKey}`;
      }
      const response = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify({ p_rows: pendingRows }),
        signal: controller.signal,
      });
      await response.text();
      if (!response.ok) {
        boundedWarning(warningLogger, "service_egress_metrics_flush_warning", {
          reason: "metrics_rpc_failed",
          http_status: response.status,
          aggregate_rows: pendingRows.length,
        });
        return { attempted: true, persistedRows: 0 };
      }
      aggregates.clear();
      return { attempted: true, persistedRows: pendingRows.length };
    } catch (error) {
      boundedWarning(warningLogger, "service_egress_metrics_flush_warning", {
        reason: error?.name === "AbortError" ? "metrics_rpc_timeout" : "metrics_rpc_error",
        aggregate_rows: pendingRows.length,
      });
      return { attempted: true, persistedRows: 0 };
    } finally {
      clearTimeout(timer);
    }
  }

  return { flush, record, rows };
}
