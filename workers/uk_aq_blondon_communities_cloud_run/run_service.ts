import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

const PORT = Number(Deno.env.get("PORT") || "8080");
const RUN_JOB_SCRIPT =
  "/app/workers/uk_aq_blondon_communities_cloud_run/run_job.ts";
const INGEST_SCRIPT_PATH = "/app/runtime/ingest_blondon_communities/index.ts";
const ALLOWED_TRIGGER_MODES = new Set(["safety", "task", "manual"]);
const CHILD_TIMEOUT_MS = 14 * 60 * 1000;
const CHILD_SHUTDOWN_GRACE_MS = 10 * 1000;
const CONNECTOR_CODE_ERROR =
  "Use connector_code=blondon_communities for Breathe London Communities. network_code/service_ref may remain breathelondon.";
const CONNECTOR_CODE = resolveCommunitiesConnectorCode(
  Deno.env.get("BLONDON_COMMUNITIES_CONNECTOR_CODE"),
);
const SUPABASE_URL = (Deno.env.get("SUPABASE_URL") || "").trim();
const SUPABASE_PRIVILEGED_KEY = (Deno.env.get("SB_SECRET_KEY") || "").trim();
const UK_AQ_CORE_SCHEMA = (Deno.env.get("UK_AQ_CORE_SCHEMA") || "uk_aq_core")
  .trim();
const UK_AQ_EDGE_UPSTREAM_SECRET = (Deno.env.get("UK_AQ_EDGE_UPSTREAM_SECRET") || "").trim();

let inFlight = false;

function hasValidRunAuth(req: Request): boolean {
  if (!UK_AQ_EDGE_UPSTREAM_SECRET) return false;
  const upstream = (req.headers.get("x-uk-aq-upstream-auth") || "").trim();
  const dispatch = (req.headers.get("x-uk-aq-dispatch-secret") || "").trim();
  return upstream === UK_AQ_EDGE_UPSTREAM_SECRET ||
    dispatch === UK_AQ_EDGE_UPSTREAM_SECRET;
}

type RunJobResult = {
  success: boolean;
  code: number;
  signal: string | null;
  timedOut: boolean;
  timeoutSeconds?: number;
};

type PostgrestResponse = {
  ok: boolean;
  status: number;
  text: string;
  data: unknown;
};

type ConnectorState = {
  id: unknown;
  connector_code: unknown;
  last_run_start: unknown;
  last_run_end: unknown;
  last_run_status: unknown;
};

function resolveCommunitiesConnectorCode(raw: unknown): string {
  const value = typeof raw === "string" ? raw.trim() : "";
  if (value && value !== "blondon_communities") {
    throw new Error(CONNECTOR_CODE_ERROR);
  }
  return "blondon_communities";
}

async function cleanupStaleIngestProcesses(stage: string): Promise<void> {
  // Timeouts can leave a grandchild ingest process alive in the container.
  // Best-effort cleanup avoids port 8000 conflicts on the next run.
  const command = new Deno.Command("sh", {
    args: [
      "-lc",
      `pkill -f '${INGEST_SCRIPT_PATH.replace(/'/g, "'\\''")}' >/dev/null 2>&1 || true`,
    ],
    stdout: "null",
    stderr: "null",
  });
  try {
    await command.output();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      JSON.stringify({
        ts: new Date().toISOString(),
        service: "uk_aq_blondon_communities_cloud_run",
        message: "stale_process_cleanup_failed",
        stage,
        error: message,
      }),
    );
  }
}

function resolveTriggerMode(req: Request, body: unknown): string {
  const url = new URL(req.url);
  const queryMode = url.searchParams.get("trigger_mode");
  if (queryMode && ALLOWED_TRIGGER_MODES.has(queryMode)) {
    return queryMode;
  }

  const headerMode = (
    req.headers.get("x-blondon-communities-trigger-mode") ||
    ""
  ).trim().toLowerCase();
  if (headerMode && ALLOWED_TRIGGER_MODES.has(headerMode)) {
    return headerMode;
  }

  const root = body && typeof body === "object" && !Array.isArray(body)
    ? body as Record<string, unknown>
    : null;
  const bodyMode = typeof root?.trigger_mode === "string"
    ? root.trigger_mode.trim().toLowerCase()
    : "";
  if (bodyMode && ALLOWED_TRIGGER_MODES.has(bodyMode)) {
    return bodyMode;
  }

  return "manual";
}

function toStringOrNull(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed || null;
}

function toIntegerOrNull(value: unknown): number | null {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return null;
  }
  return Math.trunc(num);
}

function toObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function postgrestHeaders(write = false): Record<string, string> {
  const headers: Record<string, string> = {
    apikey: SUPABASE_PRIVILEGED_KEY,
    Accept: "application/json",
    "Accept-Profile": UK_AQ_CORE_SCHEMA,
  };
  if (write) {
    headers["Content-Type"] = "application/json";
    headers["Content-Profile"] = UK_AQ_CORE_SCHEMA;
  }
  return headers;
}

async function postgrestRequest(
  method: string,
  path: string,
  options: {
    query?: Record<string, string>;
    body?: unknown;
    prefer?: string;
  } = {},
): Promise<PostgrestResponse> {
  const url = new URL(`${SUPABASE_URL.replace(/\/$/, "")}/rest/v1/${path}`);
  if (options.query) {
    for (const [key, value] of Object.entries(options.query)) {
      if (!value) {
        continue;
      }
      url.searchParams.set(key, value);
    }
  }
  const headers = postgrestHeaders(method !== "GET");
  if (options.prefer) {
    headers.Prefer = options.prefer;
  }
  const response = await fetch(url.toString(), {
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

async function recoverTimedOutConnectorState(
  timeoutSeconds: number,
): Promise<void> {
  if (!SUPABASE_URL || !SUPABASE_PRIVILEGED_KEY) {
    console.error(
      JSON.stringify({
        ts: new Date().toISOString(),
        service: "uk_aq_blondon_communities_cloud_run",
        message: "timeout_recovery_skipped",
        reason: "missing_supabase_config",
      }),
    );
    return;
  }

  const connectorResponse = await postgrestRequest("GET", "connectors", {
    query: {
      connector_code: `eq.${CONNECTOR_CODE}`,
      select: "id,connector_code,last_run_start,last_run_end,last_run_status",
      limit: "1",
    },
  });
  if (!connectorResponse.ok) {
    throw new Error(
      `timeout_recovery_load_failed (${connectorResponse.status}): ${connectorResponse.text}`,
    );
  }

  const rows = Array.isArray(connectorResponse.data) ? connectorResponse.data : [];
  const connector = toObject(rows[0]) as ConnectorState | null;
  const connectorId = toIntegerOrNull(connector?.id);
  const lastRunStart = toStringOrNull(connector?.last_run_start);
  const lastRunEnd = toStringOrNull(connector?.last_run_end);
  if (connectorId === null || !lastRunStart || lastRunEnd) {
    return;
  }

  const runEndedAtIso = new Date().toISOString();
  const revision = (Deno.env.get("K_REVISION") || "").trim() || "unknown";
  const runMessage =
    `cloud_run child_timeout after ${timeoutSeconds}s on revision ${revision}`;

  const connectorPatch = await postgrestRequest("PATCH", "connectors", {
    query: { id: `eq.${connectorId}` },
    body: {
      last_run_end: runEndedAtIso,
      last_run_status: "failed",
      last_run_message: runMessage,
    },
    prefer: "return=minimal",
  });
  if (!connectorPatch.ok) {
    throw new Error(
      `timeout_recovery_patch_failed (${connectorPatch.status}): ${connectorPatch.text}`,
    );
  }

  const runInsert = await postgrestRequest("POST", "uk_aq_ingest_runs", {
    body: {
      connector_id: connectorId,
      connector_code: CONNECTOR_CODE,
      run_started_at: lastRunStart,
      run_ended_at: runEndedAtIso,
      run_status: "failed",
      run_message: runMessage,
      response_status: 504,
      response_payload: {
        timed_out: true,
        timeout_seconds: timeoutSeconds,
        wrapper: "cloud_run_run_service",
      },
    },
    prefer: "return=minimal",
  });
  if (!runInsert.ok) {
    throw new Error(
      `timeout_recovery_insert_run_failed (${runInsert.status}): ${runInsert.text}`,
    );
  }
}

async function runJob(
  triggerMode: string,
  currentTaskName: string | null,
): Promise<RunJobResult> {
  const childEnv: Record<string, string> = {
    ...Deno.env.toObject(),
    BLONDON_COMMUNITIES_TRIGGER_MODE: triggerMode,
  };
  if (currentTaskName) {
    childEnv.BLONDON_COMMUNITIES_CURRENT_TASK_NAME = currentTaskName;
  }
  const child = new Deno.Command("deno", {
    args: [
      "run",
      "--allow-env",
      "--allow-net",
      "--allow-read",
      "--allow-write",
      "--allow-run",
      RUN_JOB_SCRIPT,
    ],
    env: childEnv,
    stdout: "inherit",
    stderr: "inherit",
  }).spawn();
  const statusPromise = child.status;
  statusPromise.catch(() => {
    // Avoid an unhandled rejection if the child exits after the timeout path returns.
  });
  let timeout: number | undefined;
  const timeoutPromise = new Promise<"timeout">((resolve) => {
    timeout = setTimeout(() => resolve("timeout"), CHILD_TIMEOUT_MS);
  });
  const result = await Promise.race([statusPromise, timeoutPromise]);
  if (timeout !== undefined) {
    clearTimeout(timeout);
  }
  if (result !== "timeout") {
    return {
      success: result.success,
      code: result.code,
      signal: result.signal,
      timedOut: false,
    };
  }

  console.error(
    JSON.stringify({
      ts: new Date().toISOString(),
      service: "uk_aq_blondon_communities_cloud_run",
      message: "child_timeout",
      timeout_seconds: Math.trunc(CHILD_TIMEOUT_MS / 1000),
      trigger_mode: triggerMode,
      current_task_name: currentTaskName,
    }),
  );
  try {
    child.kill("SIGTERM");
  } catch {
    // Child may already have exited between timeout and termination.
  }
  const terminated = await Promise.race([
    statusPromise.then((status) => ({ status })),
    new Promise<"grace_timeout">((resolve) =>
      setTimeout(() => resolve("grace_timeout"), CHILD_SHUTDOWN_GRACE_MS)
    ),
  ]);
  if (terminated === "grace_timeout") {
    try {
      child.kill("SIGKILL");
    } catch {
      // Ignore; statusPromise below will settle if the process is already gone.
    }
    return {
      success: false,
      code: -1,
      signal: "SIGKILL",
      timedOut: true,
      timeoutSeconds: Math.trunc(CHILD_TIMEOUT_MS / 1000),
    };
  }
  const status = terminated.status;
  return {
    success: false,
    code: status.code,
    signal: status.signal,
    timedOut: true,
    timeoutSeconds: Math.trunc(CHILD_TIMEOUT_MS / 1000),
  };
}

serve(async (req: Request) => {
  if (req.method === "GET") {
    return new Response(
      JSON.stringify({
        ok: true,
        service: "uk_aq_blondon_communities_cloud_run",
      }),
      {
        status: 200,
        headers: { "content-type": "application/json" },
      },
    );
  }
  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  if (!hasValidRunAuth(req)) {
    return new Response(JSON.stringify({ ok: false, error: "forbidden" }), {
      status: 403,
      headers: { "content-type": "application/json" },
    });
  }
  if (inFlight) {
    return new Response(
      JSON.stringify({
        ok: false,
        error: "run_in_flight",
      }),
      {
        status: 409,
        headers: { "content-type": "application/json" },
      },
    );
  }

  let body: unknown = null;
  try {
    body = await req.json();
  } catch {
    body = null;
  }
  const triggerMode = resolveTriggerMode(req, body);
  const currentTaskName =
    (req.headers.get("x-cloudtasks-taskname") || "").trim() || null;

  await cleanupStaleIngestProcesses("pre_run");

  inFlight = true;
  try {
    const result = await runJob(triggerMode, currentTaskName);
    if (result.timedOut) {
      await cleanupStaleIngestProcesses("post_timeout");
    }
    if (result.timedOut) {
      try {
        await recoverTimedOutConnectorState(result.timeoutSeconds ?? 0);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(
          JSON.stringify({
            ts: new Date().toISOString(),
            service: "uk_aq_blondon_communities_cloud_run",
            message: "timeout_recovery_failed",
            error: message,
          }),
        );
      }
    }
    return new Response(
      JSON.stringify({
        ok: result.success,
        trigger_mode: triggerMode,
        current_task_name: currentTaskName,
        code: result.code,
        signal: result.signal,
        timed_out: result.timedOut,
        timeout_seconds: result.timeoutSeconds ?? null,
      }),
      {
        status: result.timedOut ? 504 : result.success ? 200 : 500,
        headers: { "content-type": "application/json" },
      },
    );
  } finally {
    inFlight = false;
  }
}, { port: PORT });
