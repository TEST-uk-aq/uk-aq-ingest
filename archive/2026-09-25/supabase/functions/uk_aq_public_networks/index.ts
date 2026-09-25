import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import "../_shared/fetch_egress_patch.ts";
import {
  CACHE_CONTROL_SUCCESS_SMAXAGE_300,
  cacheControlHeaders,
} from "../_shared/cache.ts";
import { createWeakEtag, ifNoneMatchMatches } from "../_shared/etag.ts";
import { logEndpointEgress } from "../_shared/egress_metrics.ts";
import { parsePublicNetworkFilter } from "../_shared/public_network_filter.ts";
import { validateWorkerUpstreamAuth } from "../_shared/worker_auth.ts";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ??
  Deno.env.get("SB_SUPABASE_URL") ??
  "";
const SB_SECRET_KEY = Deno.env.get("SB_SECRET_KEY") ?? "";
const UK_AQ_PUBLIC_SCHEMA = Deno.env.get("UK_AQ_PUBLIC_SCHEMA") ??
  "uk_aq_public";

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type, if-none-match",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Expose-Headers": "ETag",
};

const REST_BASE_URL = SUPABASE_URL
  ? `${SUPABASE_URL.replace(/\/$/, "")}/rest/v1`
  : "";

type PublicNetwork = {
  network_id: number;
  network_code: string;
  network_label: string;
  network_type: "official" | "community" | "aggregator";
  public_display_enabled: true;
};

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

  const startedAtMs = Date.now();
  const finish = (response: Response, fields: Record<string, unknown> = {}) =>
    logEndpointEgress(
      req,
      "uk_aq_public_networks",
      startedAtMs,
      response,
      fields,
    );
  const auth = validateWorkerUpstreamAuth(req);
  if (!auth.ok) {
    return await finish(json({ error: auth.error }, auth.status), {
      error_type: "upstream_auth",
      auth_status: auth.status,
    });
  }
  if (!REST_BASE_URL || !SB_SECRET_KEY) {
    return await finish(
      json({ error: "Missing SUPABASE_URL or SB_SECRET_KEY." }, 500),
      { error_type: "missing_env" },
    );
  }

  const url = new URL(req.url);
  const networkFilter = parsePublicNetworkFilter(url);
  if (!networkFilter.ok) {
    return await finish(json({ error: networkFilter.error }, 400), {
      error_type: "invalid_public_filter",
    });
  }
  if (networkFilter.networkCode) {
    return await finish(
      json({
        error: "The public network catalog does not accept network_code.",
      }, 400),
      { error_type: "unsupported_catalog_filter" },
    );
  }

  try {
    const networks = await loadPublicNetworks();
    const payload = {
      contract_version: 2,
      count: networks.length,
      data: networks,
    };
    const etag = await createWeakEtag({
      endpoint: "uk_aq_public_networks",
      version: 2,
      payload,
    });
    if (ifNoneMatchMatches(req.headers.get("if-none-match"), etag)) {
      return await finish(notModified(etag), {
        result: "not_modified",
        row_count: networks.length,
      });
    }
    return await finish(json(payload, 200, { ETag: etag }), {
      result: "ok",
      row_count: networks.length,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("uk_aq_public_networks runtime failure", { message });
    return await finish(json({ error: "Internal server error." }, 500), {
      error_type: "runtime",
    });
  }
});

async function loadPublicNetworks(): Promise<PublicNetwork[]> {
  const url = new URL(`${REST_BASE_URL}/networks`);
  url.searchParams.set(
    "select",
    "network_id,network_code,network_label,network_type,public_display_enabled",
  );
  url.searchParams.set("order", "default_priority.asc,network_label.asc");

  const response = await fetch(url, {
    headers: {
      apikey: SB_SECRET_KEY,
      "Accept-Profile": UK_AQ_PUBLIC_SCHEMA,
      "x-ukaq-egress-caller": "uk_aq_public_networks",
    },
  });
  const payload = await response.json();
  if (!response.ok) {
    const message = payload?.message || payload?.error || response.statusText;
    throw new Error(String(message));
  }
  return Array.isArray(payload) ? payload as PublicNetwork[] : [];
}

function json(
  payload: unknown,
  status = 200,
  extraHeaders: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...CORS_HEADERS,
      ...cacheControlHeaders(status, CACHE_CONTROL_SUCCESS_SMAXAGE_300),
      ...extraHeaders,
    },
  });
}

function notModified(etag: string): Response {
  return new Response(null, {
    status: 304,
    headers: {
      ...CORS_HEADERS,
      ...cacheControlHeaders(200, CACHE_CONTROL_SUCCESS_SMAXAGE_300),
      ETag: etag,
    },
  });
}
