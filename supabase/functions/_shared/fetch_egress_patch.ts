import {
  EGRESS_BYPASS_HEADER,
  type MetricFields,
  recordEgressMetric,
} from "./egress_metrics.ts";

const PATCH_FLAG = "__uk_aq_postgrest_egress_patch__";
const ENABLED_ENV = "UK_AQ_POSTGREST_EGRESS_CAPTURE_ENABLED";
const SAMPLE_RATE_ENV = "UK_AQ_POSTGREST_EGRESS_CAPTURE_SAMPLE_RATE";
const URLS_ENV = "UK_AQ_POSTGREST_EGRESS_CAPTURE_URLS";
const DEFAULT_SAMPLE_RATE = 1;
const CALLER_HEADER = "x-ukaq-egress-caller";
const METRIC_RPC_PATHS = new Set([
  "/rest/v1/rpc/uk_aq_record_endpoint_metric",
  "/rest/v1/rpc/uk_aq_cleanup_endpoint_metrics",
]);

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ??
  Deno.env.get("SB_SUPABASE_URL") ??
  "";
const OBS_AQIDB_SUPABASE_URL = Deno.env.get("OBS_AQIDB_SUPABASE_URL") ?? "";

function parseBoolean(
  raw: string | undefined | null,
  fallback: boolean,
): boolean {
  if (!raw) {
    return fallback;
  }
  const normalized = raw.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function parseSampleRate(
  raw: string | undefined | null,
  fallback: number,
): number {
  const parsed = Number(raw ?? "");
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return clamp(parsed, 0, 1);
}

function parseUrl(input: Request | URL | string): URL | null {
  try {
    if (input instanceof URL) {
      return input;
    }
    if (typeof input === "string") {
      return new URL(input);
    }
    return new URL(input.url);
  } catch {
    return null;
  }
}

function normalizeMethod(
  input: Request | URL | string,
  init?: RequestInit,
): string {
  if (init?.method) {
    return String(init.method).toUpperCase();
  }
  if (input instanceof Request) {
    return input.method.toUpperCase();
  }
  return "GET";
}

function readHeader(
  input: Request | URL | string,
  init: RequestInit | undefined,
  name: string,
): string {
  const target = name.toLowerCase();
  const initHeaders = new Headers(init?.headers ?? undefined);
  const initValue = initHeaders.get(target);
  if (initValue) {
    return initValue;
  }
  if (input instanceof Request) {
    return input.headers.get(target) ?? "";
  }
  return "";
}

function parseOrigin(raw: string | undefined | null): string | null {
  if (!raw) {
    return null;
  }
  try {
    return new URL(raw).origin;
  } catch {
    return null;
  }
}

function parseConfiguredOrigins(raw: string | undefined | null): string[] {
  if (!raw) {
    return [];
  }
  return raw.split(/[\s,]+/)
    .map((value) => parseOrigin(value))
    .filter((value): value is string => Boolean(value));
}

function buildTrackedOrigins(): Set<string> {
  const origins = new Set<string>();
  for (const origin of [
    parseOrigin(SUPABASE_URL),
    parseOrigin(OBS_AQIDB_SUPABASE_URL),
  ]) {
    if (origin) {
      origins.add(origin);
    }
  }
  for (const origin of parseConfiguredOrigins(Deno.env.get(URLS_ENV))) {
    origins.add(origin);
  }
  return origins;
}

const TRACKED_ORIGINS = buildTrackedOrigins();

function originLabel(url: URL): string {
  const hostLabel = (url.hostname.split(".")[0] || "unknown").toLowerCase();
  const normalized = hostLabel.replace(/[^a-z0-9._-]/g, "_");
  return normalized.slice(0, 64) || "unknown";
}

function endpointForUrl(url: URL, caller: string | null): string | null {
  if (!TRACKED_ORIGINS.size) {
    return null;
  }
  if (!TRACKED_ORIGINS.has(url.origin)) {
    return null;
  }
  if (!url.pathname.startsWith("/rest/v1/")) {
    return null;
  }
  if (METRIC_RPC_PATHS.has(url.pathname)) {
    return null;
  }
  const trimmed = url.pathname.replace(/^\/rest\/v1\/?/, "");
  const base = `postgrest:${trimmed || "root"}`;
  const withOrigin = TRACKED_ORIGINS.size > 1
    ? `${base}|origin=${originLabel(url)}`
    : base;
  if (!caller) {
    return withOrigin;
  }
  return `${withOrigin}|caller=${caller}`;
}

async function responseBytes(response: Response): Promise<number | null> {
  const contentLength = response.headers.get("content-length");
  if (contentLength) {
    const parsed = Number(contentLength);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return Math.floor(parsed);
    }
  }
  if (!response.body) {
    return 0;
  }
  try {
    const bytes = await response.clone().arrayBuffer();
    return bytes.byteLength;
  } catch {
    return null;
  }
}

async function requestBodyBytes(
  input: Request | URL | string,
  init?: RequestInit,
): Promise<number | null> {
  const body = init?.body;
  if (typeof body === "string") return new TextEncoder().encode(body).byteLength;
  if (body instanceof URLSearchParams) {
    return new TextEncoder().encode(body.toString()).byteLength;
  }
  if (body instanceof Blob) return body.size;
  if (body instanceof ArrayBuffer) return body.byteLength;
  if (ArrayBuffer.isView(body)) return body.byteLength;
  if (body !== undefined && body !== null) return null;
  if (input instanceof Request && input.body) {
    try {
      return (await input.clone().arrayBuffer()).byteLength;
    } catch {
      return null;
    }
  }
  return 0;
}

function extractMeta(url: URL, method: string): MetricFields {
  const pathname = url.pathname.replace(/^\/rest\/v1\//, "");
  const select = url.searchParams.get("select");
  return {
    target: pathname || null,
    has_select: Boolean(select),
    query_params: Array.from(url.searchParams.keys()).slice(0, 12),
    method,
  };
}

function sanitizeCaller(value: string | null): string | null {
  if (!value) {
    return null;
  }
  const normalized = value.trim().toLowerCase().replace(/[^a-z0-9._-]/g, "_");
  if (!normalized) {
    return null;
  }
  return normalized.slice(0, 64);
}

function callerFromHeader(
  input: Request | URL | string,
  init?: RequestInit,
): string | null {
  return sanitizeCaller(readHeader(input, init, CALLER_HEADER));
}

function callerFromStack(): string | null {
  const stack = new Error().stack ?? "";
  if (!stack) {
    return null;
  }
  const lines = stack.split("\n");
  for (const line of lines) {
    const match = line.match(/\/(functions|workers)\/([^\/]+)\//i);
    if (!match) {
      continue;
    }
    const caller = sanitizeCaller(match[2]);
    if (!caller || caller === "_shared") {
      continue;
    }
    return caller;
  }
  return null;
}

function shouldSkipBypassHeader(
  input: Request | URL | string,
  init?: RequestInit,
): boolean {
  return readHeader(input, init, EGRESS_BYPASS_HEADER) === "1";
}

function applyPatch(): void {
  const globalRef = globalThis as Record<string, unknown>;
  if (globalRef[PATCH_FLAG]) {
    return;
  }
  if (!parseBoolean(Deno.env.get(ENABLED_ENV), true)) {
    globalRef[PATCH_FLAG] = true;
    return;
  }
  const sampleRate = parseSampleRate(
    Deno.env.get(SAMPLE_RATE_ENV),
    DEFAULT_SAMPLE_RATE,
  );
  const nativeFetch = globalThis.fetch.bind(globalThis);
  globalThis.fetch = async (
    input: Request | URL | string,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = parseUrl(input);
    const caller = callerFromHeader(input, init) ?? callerFromStack();
    const endpoint = url ? endpointForUrl(url, caller) : null;
    const track = Boolean(endpoint) && !shouldSkipBypassHeader(input, init);
    const method = normalizeMethod(input, init);
    const startedAt = track ? Date.now() : 0;
    const sentBodyBytes = track ? await requestBodyBytes(input, init) : null;
    const response = await nativeFetch(input as RequestInfo | URL, init);
    if (!track || !endpoint || !url) {
      return response;
    }
    const durationMs = Date.now() - startedAt;
    const bytes = await responseBytes(response);
    try {
      await recordEgressMetric({
        endpoint,
        method,
        status: response.status,
        durationMs,
        responseBytes: bytes,
        fields: {
          ...extractMeta(url, method),
          caller,
          request_body_bytes: sentBodyBytes,
        },
        sampleRate,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.warn(JSON.stringify({
        metric: "uk_aq_postgrest_egress_capture_warning",
        message,
        endpoint,
        ts: new Date().toISOString(),
      }));
    }
    return response;
  };
  globalRef[PATCH_FLAG] = true;
}

applyPatch();
