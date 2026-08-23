const SOURCE_TYPE = "supabase_postgrest";
export const SERVICE_EGRESS_BYPASS_HEADER = "x-ukaq-egress-bypass";

type Aggregate = {
  bucket_minute: string;
  env_name: string;
  project_ref: string;
  service_name: string;
  source_type: string;
  source_name: string;
  route_name: string;
  query_name: string;
  window_label: string;
  status: "ok" | "error";
  request_count: number;
  response_rows: number;
  response_bytes_est: number;
  upstream_bytes_est: number;
  duration_ms: number;
  error_count: number;
  httpStatuses: Set<number>;
  httpStatusClasses: Set<string>;
  measurementMethods: Set<string>;
};

type RecordInput = {
  completedAt?: Date;
  durationMs?: number;
  httpStatus: number;
  method: string;
  queryName?: string;
  responseBytes: number;
  responseData?: unknown;
  routePath: string;
  sourceUrl: string;
  measurementMethod?: string;
};

function parseBoolean(raw: string | undefined, fallback = false): boolean {
  if (raw === undefined) return fallback;
  const normalized = raw.trim().toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "y", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "n", "off"].includes(normalized)) return false;
  return fallback;
}

function utcMinute(value: Date): string {
  const date = new Date(value.getTime());
  date.setUTCSeconds(0, 0);
  return date.toISOString();
}

function normalizedOrigin(raw: string): string {
  try {
    return new URL(raw).origin.toLowerCase();
  } catch {
    return "";
  }
}

function projectRef(raw: string): string {
  try {
    const hostname = new URL(raw).hostname.toLowerCase();
    const suffix = ".supabase.co";
    if (!hostname.endsWith(suffix)) return "";
    return hostname.slice(0, -suffix.length).split(".").pop() || "";
  } catch {
    return "";
  }
}

function sourceName(raw: string): string {
  const origin = normalizedOrigin(raw);
  const observsOrigin = normalizedOrigin(
    Deno.env.get("OBS_AQIDB_SUPABASE_URL") || "",
  );
  const ingestOrigin = normalizedOrigin(Deno.env.get("SUPABASE_URL") || "");
  if (origin && observsOrigin && origin === observsOrigin) return "obs_aqidb";
  if (origin && ingestOrigin && origin === ingestOrigin) return "ingestdb";
  return "supabase";
}

function normalizedRoute(routePath: string): string {
  const target = routePath.split("?", 1)[0].replace(/^\/+|\/+$/g, "");
  if (target.startsWith("rpc/")) return target;
  return `table/${target || "unknown"}`;
}

function defaultQueryName(method: string, routePath: string): string {
  const target = routePath.split("?", 1)[0].replace(/^\/+|\/+$/g, "");
  if (target.startsWith("rpc/")) {
    const rpc = target.slice(4);
    const names: Record<string, string> = {
      blondon_communities_select_station_refs: "lookup_station_refs",
      uk_aq_rpc_dispatch_claim: "dispatch_claim",
      uk_aq_rpc_phenomena_ids: "lookup_phenomena_ids",
      uk_aq_rpc_phenomena_upsert: "upsert_phenomena",
      uk_aq_rpc_observations_compact_upsert_v1: "compact_observation_upsert",
      uk_aq_rpc_observs_observations_compact_upsert_v1:
        "compact_observation_upsert",
      uk_aq_rpc_observs_outbox_enqueue: "enqueue_observs",
      uk_aq_rpc_timeseries_last_values_compact_update_v1:
        "update_timeseries_last_values",
    };
    return names[rpc] || "rpc_call";
  }
  const names: Record<string, string> = {
    "GET:connectors": "load_connector",
    "PATCH:connectors": "update_connector_run",
    "POST:uk_aq_ingest_runs": "insert_ingest_run",
    "POST:error_logs": "insert_error_log",
    "PATCH:error_logs": "update_error_log",
    "GET:stations": "load_stations",
    "POST:stations": "upsert_stations",
    "GET:station_metadata": "load_station_metadata",
    "POST:station_metadata": "upsert_station_metadata",
    "GET:timeseries": "lookup_timeseries_refs",
    "POST:timeseries": "upsert_timeseries",
    "GET:blondon_communities_station_checkpoints": "load_station_checkpoints",
    "POST:blondon_communities_station_checkpoints":
      "upsert_station_checkpoints",
  };
  return names[`${method.toUpperCase()}:${target}`] ||
    `${method.toLowerCase()}_table`;
}

function responseRows(data: unknown): number {
  return Array.isArray(data) ? data.length : 0;
}

function isLikelyJwt(value: string): boolean {
  return value.startsWith("eyJ") && value.split(".").length === 3;
}

class ServiceEgressMetricsCollector {
  readonly enabled: boolean;
  readonly envName: string;
  readonly serviceName: string;
  private aggregates = new Map<string, Aggregate>();

  constructor(serviceName: string) {
    this.serviceName = serviceName.trim();
    this.enabled = parseBoolean(
      Deno.env.get("UK_AQ_SERVICE_EGRESS_METRICS_ENABLED"),
      false,
    );
    this.envName = (Deno.env.get("UKAQ_ENV_NAME") || "TEST").trim();
  }

  private warn(message: string, details: Record<string, unknown> = {}): void {
    try {
      console.warn(JSON.stringify({
        ts: new Date().toISOString(),
        service_name: this.serviceName,
        message,
        ...details,
      }));
    } catch {
      // Monitoring diagnostics must never affect the business workload.
    }
  }

  record(input: RecordInput): void {
    if (!this.enabled) return;
    try {
      const numericStatus = Math.trunc(Number(input.httpStatus));
      const status = numericStatus >= 200 && numericStatus < 300
        ? "ok"
        : "error";
      const identity = {
        bucket_minute: utcMinute(input.completedAt || new Date()),
        env_name: this.envName,
        project_ref: projectRef(input.sourceUrl),
        service_name: this.serviceName,
        source_name: sourceName(input.sourceUrl),
        route_name: normalizedRoute(input.routePath),
        query_name: input.queryName ||
          defaultQueryName(input.method, input.routePath),
        window_label: "",
        status,
      } as const;
      const key = JSON.stringify(Object.values(identity));
      let aggregate = this.aggregates.get(key);
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
          httpStatuses: new Set<number>(),
          httpStatusClasses: new Set<string>(),
          measurementMethods: new Set<string>(),
        };
        this.aggregates.set(key, aggregate);
      }
      aggregate.request_count += 1;
      aggregate.response_rows += responseRows(input.responseData);
      aggregate.response_bytes_est += Math.max(
        0,
        Math.trunc(Number(input.responseBytes) || 0),
      );
      aggregate.duration_ms += Math.max(
        0,
        Math.trunc(Number(input.durationMs) || 0),
      );
      aggregate.error_count += status === "error" ? 1 : 0;
      if (numericStatus >= 100 && numericStatus <= 599) {
        aggregate.httpStatuses.add(numericStatus);
        aggregate.httpStatusClasses.add(`${Math.floor(numericStatus / 100)}xx`);
      }
      if (input.measurementMethod) {
        aggregate.measurementMethods.add(input.measurementMethod);
      }
    } catch (error) {
      this.warn("service_egress_metrics_record_warning", {
        error: error instanceof Error ? error.name : "unknown",
      });
    }
  }

  rows(): Array<Record<string, unknown>> {
    return Array.from(this.aggregates.values()).map((aggregate) => {
      const notes: Record<string, unknown> = {};
      if (aggregate.measurementMethods.size === 1) {
        notes.measurement_method = Array.from(aggregate.measurementMethods)[0];
      }
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

  async flush(): Promise<void> {
    const rows = this.rows();
    if (!this.enabled || rows.length === 0) return;
    const supabaseUrl = (
      Deno.env.get("UK_AQ_SERVICE_EGRESS_METRICS_SUPABASE_URL") ||
      Deno.env.get("OBS_AQIDB_SUPABASE_URL") || ""
    ).trim();
    const apiKey = (
      Deno.env.get("UK_AQ_SERVICE_EGRESS_METRICS_SB_SECRET_KEY") || ""
    ).trim();
    const schema = (
      Deno.env.get("UK_AQ_SERVICE_EGRESS_METRICS_SCHEMA") || "uk_aq_public"
    ).trim();
    const rpc = (
      Deno.env.get("UK_AQ_SERVICE_EGRESS_METRICS_RPC") ||
      "uk_aq_rpc_service_egress_metrics_batch_upsert"
    ).trim();
    if (!supabaseUrl || !apiKey || !schema || !rpc) {
      this.warn("service_egress_metrics_flush_warning", {
        reason: "missing_metrics_configuration",
        aggregate_rows: rows.length,
      });
      return;
    }
    const headers: Record<string, string> = {
      apikey: apiKey,
      Accept: "application/json",
      "Accept-Profile": schema,
      "Content-Type": "application/json",
      "Content-Profile": schema,
      [SERVICE_EGRESS_BYPASS_HEADER]: "1",
    };
    if (isLikelyJwt(apiKey)) headers.Authorization = `Bearer ${apiKey}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10_000);
    try {
      const response = await fetch(
        `${supabaseUrl.replace(/\/$/, "")}/rest/v1/rpc/${
          encodeURIComponent(rpc)
        }`,
        {
          method: "POST",
          headers,
          body: JSON.stringify({ p_rows: rows }),
          signal: controller.signal,
        },
      );
      await response.text();
      if (!response.ok) {
        this.warn("service_egress_metrics_flush_warning", {
          reason: "metrics_rpc_failed",
          http_status: response.status,
          aggregate_rows: rows.length,
        });
        return;
      }
      this.aggregates.clear();
    } catch (error) {
      this.warn("service_egress_metrics_flush_warning", {
        reason: error instanceof DOMException && error.name === "AbortError"
          ? "metrics_rpc_timeout"
          : "metrics_rpc_error",
        aggregate_rows: rows.length,
      });
    } finally {
      clearTimeout(timeout);
    }
  }
}

let collector: ServiceEgressMetricsCollector | null = null;

export function configureServiceEgressMetrics(
  serviceName: string,
): ServiceEgressMetricsCollector {
  if (!collector) collector = new ServiceEgressMetricsCollector(serviceName);
  return collector;
}

export function serviceEgressMetricsEnabled(): boolean {
  return collector?.enabled === true;
}

export function serviceEgressBypassHeaders(): Record<string, string> {
  return serviceEgressMetricsEnabled()
    ? { [SERVICE_EGRESS_BYPASS_HEADER]: "1" }
    : {};
}

export function recordServiceEgressPostgrestResponse(
  input: RecordInput,
): void {
  collector?.record(input);
}

export async function flushServiceEgressMetrics(): Promise<void> {
  try {
    await collector?.flush();
  } catch {
    // The collector is fail-open even if an unexpected implementation error occurs.
  }
}

export async function serviceEgressPostgrestFetch(
  input: Request | URL | string,
  init?: RequestInit,
): Promise<Response> {
  if (!serviceEgressMetricsEnabled()) return await fetch(input, init);

  const rawUrl = input instanceof Request ? input.url : String(input);
  let url: URL;
  try {
    url = new URL(rawUrl);
  } catch {
    return await fetch(input, init);
  }
  if (!url.pathname.startsWith("/rest/v1/")) {
    return await fetch(input, init);
  }

  const headers = new Headers(input instanceof Request ? input.headers : {});
  for (const [name, value] of new Headers(init?.headers).entries()) {
    headers.set(name, value);
  }
  headers.set(SERVICE_EGRESS_BYPASS_HEADER, "1");
  const method = (init?.method ||
    (input instanceof Request ? input.method : "GET")).toUpperCase();
  const startedAt = Date.now();
  const response = await fetch(input, { ...init, headers });
  const rawContentLength = response.headers.get("content-length");
  const contentLength = rawContentLength && rawContentLength.trim()
    ? Number(rawContentLength)
    : Number.NaN;
  const responseBytes = Number.isFinite(contentLength) && contentLength >= 0
    ? Math.trunc(contentLength)
    : 0;
  const routePath = url.pathname.replace(/^\/rest\/v1\/?/, "");
  collector?.record({
    durationMs: Date.now() - startedAt,
    httpStatus: response.status,
    method,
    responseBytes,
    routePath,
    sourceUrl: url.origin,
    measurementMethod: Number.isFinite(contentLength) && contentLength >= 0
      ? "content_length"
      : undefined,
  });
  return response;
}
