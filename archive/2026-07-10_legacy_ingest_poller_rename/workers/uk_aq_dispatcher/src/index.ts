export interface Env {
  SUPABASE_URL: unknown;
  SB_PUBLISHABLE_DEFAULT_KEY: unknown;
  SB_UK_AQ_CRON_SECRET?: unknown;
}

function normalizeBaseUrl(value: string): string {
  return value.replace(/\/$/, "");
}

async function readSecret(value: unknown): Promise<string> {
  if (typeof value === "string") {
    return value;
  }
  if (value && typeof value === "object") {
    const record = value as { get?: () => Promise<string>; then?: (cb: (v: unknown) => void) => void };
    if (typeof record.get === "function") {
      const resolved = await record.get();
      return typeof resolved === "string" ? resolved : String(resolved ?? "");
    }
    if (typeof record.then === "function") {
      const resolved = await (value as Promise<unknown>);
      return typeof resolved === "string" ? resolved : String(resolved ?? "");
    }
  }
  return value ? String(value) : "";
}

async function invokeDispatch(
  env: Env,
  mode: "enqueue" | "run_queue" | "legacy",
  payload: Record<string, unknown> = {},
): Promise<{ ok: boolean; status: number; body: unknown }> {
  const supabaseUrl = await readSecret(env.SUPABASE_URL);
  const supabasePublishableKey = await readSecret(env.SB_PUBLISHABLE_DEFAULT_KEY);
  const cronSecret = await readSecret(env.SB_UK_AQ_CRON_SECRET ?? "");
  if (!supabaseUrl || !supabasePublishableKey) {
    console.error("Missing SUPABASE_URL or SB_PUBLISHABLE_DEFAULT_KEY.");
    return {
      ok: false,
      status: 500,
      body: "missing_supabase_secrets",
    };
  }
  const url = `${normalizeBaseUrl(supabaseUrl)}/functions/v1/uk_aq_dispatch_polls`;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    apikey: supabasePublishableKey,
  };
  if (cronSecret) {
    headers["X-Cron-Secret"] = cronSecret;
  }
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify({ source: "cloudflare", mode, ...payload }),
    });
    if (!resp.ok) {
      const body = await resp.text().catch(() => "");
      console.error("uk_aq_dispatch_polls failed", { mode, status: resp.status, body });
      return { ok: false, status: resp.status, body };
    }
    const contentType = (resp.headers.get("content-type") ?? "").toLowerCase();
    const body = contentType.includes("application/json")
      ? await resp.json().catch(() => null)
      : await resp.text().catch(() => "");
    console.log("uk_aq_dispatch_polls succeeded", { mode, body });
    return { ok: true, status: resp.status, body };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error ?? "unknown_error");
    console.error("uk_aq_dispatch_polls failed", { mode, status: 0, body: message });
    return { ok: false, status: 0, body: message };
  }
}

function parsePositiveInt(
  value: unknown,
  fallback: number,
): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 1) {
    return fallback;
  }
  return Math.max(1, Math.floor(numeric));
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function resolveRunQueueFanout(enqueueBody: unknown): number {
  const root = asRecord(enqueueBody);
  const settings = asRecord(root?.dispatcher_settings);
  const maxRuns = parsePositiveInt(settings?.max_runs_per_dispatch_call, 1);
  // Prevent accidental fanout explosions from config or malformed payloads.
  return Math.max(1, Math.min(maxRuns, 8));
}

function shouldFallbackToLegacy(result: { status: number; body: unknown }): boolean {
  const bodyText = typeof result.body === "string" ? result.body.toLowerCase() : "";

  // Infra/transient failures should not trigger extra legacy calls.
  if (result.status >= 500 || result.status === 0) {
    return false;
  }
  if (bodyText.includes("worker_limit") || bodyText.includes("bad gateway") || bodyText.includes("timeout")) {
    return false;
  }

  // Legacy fallback is only for queue-mode compatibility issues.
  return (
    result.status === 400 ||
    result.status === 404 ||
    bodyText.includes("dispatch_mode") ||
    bodyText.includes("run_queue") ||
    bodyText.includes("unsupported")
  );
}

export default {
  async scheduled(_event: unknown, env: Env, _ctx: unknown): Promise<void> {
    const enqueueResult = await invokeDispatch(env, "enqueue");
    if (!enqueueResult.ok) {
      if (shouldFallbackToLegacy(enqueueResult)) {
        await invokeDispatch(env, "legacy");
      } else {
        console.warn("skipping_legacy_fallback_after_enqueue_failure", {
          status: enqueueResult.status,
          body: enqueueResult.body,
        });
      }
      return;
    }
    const fanout = resolveRunQueueFanout(enqueueResult.body);

    const runQueueResults = await Promise.all(
      Array.from({ length: fanout }, (_, index) =>
        invokeDispatch(env, "run_queue", {
          run_queue_claim_limit: 1,
          fanout_index: index + 1,
          fanout_total: fanout,
        })
      ),
    );
    if (runQueueResults.every((result) => !result.ok)) {
      const firstFailure = runQueueResults[0] ?? { status: 0, body: "unknown" };
      if (shouldFallbackToLegacy(firstFailure)) {
        await invokeDispatch(env, "legacy");
      } else {
        console.warn("skipping_legacy_fallback_after_run_queue_failures", {
          fanout,
          failures: runQueueResults.map((result) => ({ status: result.status, body: result.body })),
        });
      }
    }
  },
};
