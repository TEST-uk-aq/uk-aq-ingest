import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import {
  decideSosCloudRunServiceResult,
  isSosCloudRunChildResult,
  type SosCloudRunResultReadState,
  type SosCloudRunChildResult,
} from "./result_contract.ts";

const PORT = Number(Deno.env.get("PORT") || "8080");
const RUN_JOB_SCRIPT = "/app/workers/uk_aq_sos_cloud_run/run_job.ts";
const ALLOWED_TRIGGER_MODES = new Set(["safety", "task", "manual"]);
const UK_AQ_EDGE_UPSTREAM_SECRET = (Deno.env.get("UK_AQ_EDGE_UPSTREAM_SECRET") || "").trim();

let inFlight = false;

function hasValidRunAuth(req: Request): boolean {
  if (!UK_AQ_EDGE_UPSTREAM_SECRET) return false;
  const upstream = (req.headers.get("x-uk-aq-upstream-auth") || "").trim();
  const dispatch = (req.headers.get("x-uk-aq-dispatch-secret") || "").trim();
  return upstream === UK_AQ_EDGE_UPSTREAM_SECRET ||
    dispatch === UK_AQ_EDGE_UPSTREAM_SECRET;
}

function resolveTriggerMode(req: Request, body: unknown): string {
  const url = new URL(req.url);
  const queryMode = url.searchParams.get("trigger_mode");
  if (queryMode && ALLOWED_TRIGGER_MODES.has(queryMode)) {
    return queryMode;
  }

  const headerMode = (req.headers.get("x-sos-trigger-mode") || "").trim()
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
  resultPath: string,
): Promise<Deno.CommandStatus> {
  const childEnv: Record<string, string> = {
    ...Deno.env.toObject(),
    SOS_TRIGGER_MODE: triggerMode,
    SOS_RUN_RESULT_PATH: resultPath,
  };
  if (currentTaskName) {
    childEnv.SOS_CURRENT_TASK_NAME = currentTaskName;
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

async function readChildResult(
  path: string,
): Promise<{ result: SosCloudRunChildResult | null; state: SosCloudRunResultReadState }> {
  try {
    const content = await Deno.readTextFile(path);
    if (!content.trim()) {
      return { result: null, state: "missing" };
    }
    const parsed = JSON.parse(content);
    return isSosCloudRunChildResult(parsed)
      ? { result: parsed, state: "valid" }
      : { result: null, state: "invalid" };
  } catch {
    return { result: null, state: "invalid" };
  }
}

serve(async (req: Request) => {
  if (req.method === "GET") {
    return new Response(
      JSON.stringify({
        ok: true,
        service: "uk_aq_sos_cloud_run",
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

  inFlight = true;
  let resultPath: string | null = null;
  try {
    resultPath = await Deno.makeTempFile({ prefix: "uk-aq-sos-result-" });
    const status = await runJob(triggerMode, currentTaskName, resultPath);
    const childRead = status.success
      ? await readChildResult(resultPath)
      : { result: null, state: "missing" as const };
    const serviceResult = decideSosCloudRunServiceResult(
      status.success,
      status.code,
      childRead.result,
      childRead.state,
    );
    return new Response(
      JSON.stringify({
        ...serviceResult.payload,
        trigger_mode: triggerMode,
        current_task_name: currentTaskName,
        code: status.code,
      }),
      {
        status: serviceResult.httpStatus,
        headers: { "content-type": "application/json" },
      },
    );
  } finally {
    if (resultPath) {
      await Deno.remove(resultPath).catch(() => undefined);
    }
    inFlight = false;
  }
}, { port: PORT });
