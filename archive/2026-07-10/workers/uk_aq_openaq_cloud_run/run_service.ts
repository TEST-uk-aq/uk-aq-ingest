import { serve } from "https://deno.land/std@0.224.0/http/server.ts";

const PORT = Number(Deno.env.get("PORT") || "8080");
const RUN_JOB_SCRIPT = "/app/workers/uk_aq_openaq_cloud_run/run_job.ts";
const ALLOWED_TRIGGER_MODES = new Set(["safety", "task", "manual"]);

let inFlight = false;

function resolveTriggerMode(req: Request, body: unknown): string {
  const url = new URL(req.url);
  const queryMode = url.searchParams.get("trigger_mode");
  if (queryMode && ALLOWED_TRIGGER_MODES.has(queryMode)) {
    return queryMode;
  }

  const headerMode = (req.headers.get("x-openaq-trigger-mode") || "").trim()
    .toLowerCase();
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

async function runJob(
  triggerMode: string,
  currentTaskName: string | null,
): Promise<Deno.CommandStatus> {
  const childEnv: Record<string, string> = {
    ...Deno.env.toObject(),
    OPENAQ_TRIGGER_MODE: triggerMode,
  };
  if (currentTaskName) {
    childEnv.OPENAQ_CURRENT_TASK_NAME = currentTaskName;
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
  return await child.status;
}

serve(async (req: Request) => {
  if (req.method === "GET") {
    return new Response(
      JSON.stringify({
        ok: true,
        service: "uk_aq_openaq_cloud_run",
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
  inFlight = true;
  try {
    const status = await runJob(triggerMode, currentTaskName);
    return new Response(
      JSON.stringify({
        ok: status.success,
        trigger_mode: triggerMode,
        current_task_name: currentTaskName,
        code: status.code,
      }),
      {
        status: status.success ? 200 : 500,
        headers: { "content-type": "application/json" },
      },
    );
  } finally {
    inFlight = false;
  }
}, { port: PORT });
