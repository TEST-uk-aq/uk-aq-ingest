import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import "../_shared/fetch_egress_patch.ts";
import {
  CACHE_CONTROL_SUCCESS_SMAXAGE_300,
  cacheControlHeaders,
} from "../_shared/cache.ts";
import { parsePublicNetworkFilter } from "../_shared/public_network_filter.ts";
import { validateWorkerUpstreamAuth } from "../_shared/worker_auth.ts";

const DEFAULT_PAGE_SIZE = 1000;
const MAX_PAGE_SIZE = 1000;
const MAX_LIMIT = 20000;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ??
  Deno.env.get("SB_SUPABASE_URL") ??
  "";
const SB_SECRET_KEY = Deno.env.get("SB_SECRET_KEY") ?? "";
const SUPABASE_PRIVILEGED_KEY = SB_SECRET_KEY;
const UK_AQ_CORE_SCHEMA = Deno.env.get("UK_AQ_CORE_SCHEMA") ??
  "uk_aq_core";
const UK_AQ_PUBLIC_SCHEMA = Deno.env.get("UK_AQ_PUBLIC_SCHEMA") ??
  "uk_aq_public";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
};

const REST_BASE_URL = SUPABASE_URL
  ? `${SUPABASE_URL.replace(/\/$/, "")}/rest/v1`
  : "";

function postgrestHeaders(schema = UK_AQ_CORE_SCHEMA): Record<string, string> {
  const headers: Record<string, string> = {
    apikey: SUPABASE_PRIVILEGED_KEY,
    "Content-Type": "application/json",
    "x-ukaq-egress-caller": "uk_aq_stations",
  };
  if (schema && schema !== "public") {
    headers["Accept-Profile"] = schema;
    headers["Content-Profile"] = schema;
  }
  return headers;
}

async function postgrestRequest<T>(
  method: string,
  path: string,
  params?: Record<string, string>,
  schema?: string,
  body?: unknown,
): Promise<{ data: T | null; error: { message: string } | null }> {
  if (!REST_BASE_URL || !SUPABASE_PRIVILEGED_KEY) {
    return {
      data: null,
      error: { message: "Missing SUPABASE_URL or SB_SECRET_KEY." },
    };
  }
  const url = new URL(`${REST_BASE_URL}/${path}`);
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  const resp = await fetch(url.toString(), {
    method,
    headers: postgrestHeaders(schema),
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const contentType = resp.headers.get("content-type") ?? "";
  const payload = contentType.includes("application/json")
    ? await resp.json()
    : await resp.text();
  if (!resp.ok) {
    const message = payload?.message || payload?.error_description ||
      payload?.error || resp.statusText;
    return { data: null, error: { message: String(message) } };
  }
  return { data: payload as T, error: null };
}

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        ...CORS_HEADERS,
        "Access-Control-Max-Age": "86400",
        ...cacheControlHeaders(204, CACHE_CONTROL_SUCCESS_SMAXAGE_300),
      },
    });
  }
  if (req.method !== "GET") {
    return new Response("Method not allowed", {
      status: 405,
      headers: {
        ...CORS_HEADERS,
        ...cacheControlHeaders(405, CACHE_CONTROL_SUCCESS_SMAXAGE_300),
      },
    });
  }
  const auth = validateWorkerUpstreamAuth(req);
  if (!auth.ok) {
    return json({ error: auth.error }, auth.status);
  }
  if (!SUPABASE_URL || !SUPABASE_PRIVILEGED_KEY) {
    return json({ error: "Missing SUPABASE_URL or SB_SECRET_KEY." }, 500);
  }

  const url = new URL(req.url);
  const networkFilter = parsePublicNetworkFilter(url);
  if (!networkFilter.ok) {
    return json({ error: networkFilter.error }, 400);
  }
  const networkCode = networkFilter.networkCode;
  const region = normalizeText(url.searchParams.get("region"));
  const stationLike = normalizeText(url.searchParams.get("station_like"));
  const targetLimit = parseLimit(url.searchParams.get("limit"), MAX_LIMIT);
  const pageSize = parseLimit(
    url.searchParams.get("page_size"),
    MAX_PAGE_SIZE,
    DEFAULT_PAGE_SIZE,
  ) ?? DEFAULT_PAGE_SIZE;

  try {
    const rows = await fetchStations({
      networkCode,
      region,
      stationLike,
      targetLimit,
      pageSize,
    });
    return json({
      contract_version: 2,
      network_code: networkCode,
      count: rows.length,
      data: rows,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error("uk_aq_stations runtime failure", { message });
    return json({ error: "Internal server error." }, 500);
  }
});

type FetchOptions = {
  networkCode: string | null;
  region: string | null;
  stationLike: string | null;
  targetLimit: number | null;
  pageSize: number;
};

async function fetchStations({
  networkCode,
  region,
  stationLike,
  targetLimit,
  pageSize,
}: FetchOptions) {
  const requestedLimit = targetLimit ?? pageSize;
  const rows: Array<Record<string, unknown>> = [];
  let offsetRows = 0;

  while (rows.length < requestedLimit) {
    const remaining = requestedLimit - rows.length;
    const requestPageSize = Math.min(pageSize, remaining, MAX_PAGE_SIZE);
    const { data, error } = await postgrestRequest<
      Array<Record<string, unknown>>
    >(
      "POST",
      "rpc/uk_aq_stations_rpc",
      undefined,
      UK_AQ_PUBLIC_SCHEMA,
      {
        network_code: networkCode,
        region,
        station_like: stationLike,
        limit_rows: requestedLimit,
        page_size: requestPageSize,
        offset_rows: offsetRows,
      },
    );
    if (error) {
      throw new Error(error.message);
    }

    const page = data ?? [];
    rows.push(...page);
    if (page.length < requestPageSize) {
      break;
    }
    offsetRows += page.length;
  }

  return rows.slice(0, requestedLimit);
}

function normalizeText(value: string | null): string | null {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function parseLimit(
  value: string | null,
  max: number,
  fallback: number | null = null,
): number | null {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  const clamped = Math.min(max, Math.max(1, Math.floor(parsed)));
  return clamped;
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...CORS_HEADERS,
      ...cacheControlHeaders(status, CACHE_CONTROL_SUCCESS_SMAXAGE_300),
    },
  });
}
