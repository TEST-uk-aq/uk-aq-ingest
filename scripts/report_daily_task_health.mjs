import { pathToFileURL } from "node:url";

const RPC_SCHEMA = "uk_aq_public";
const SUPABASE_RETRY_MAX_ATTEMPTS = 3;
const SUPABASE_RETRY_DELAYS_MS = [250, 500];
const SUPABASE_TRANSIENT_STATUS_CODES = new Set([502, 503, 504]);
const RETRY_SAFE_HEALTH_RPCS = new Set([
  "uk_aq_rpc_daily_task_finished",
  "uk_aq_rpc_daily_task_failed",
  "uk_aq_rpc_recompute_daily_task_status",
]);

function parseBoolean(raw, fallback = false) {
  if (raw === undefined || raw === null || raw === "") {
    return fallback;
  }
  const value = String(raw).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(value)) {
    return true;
  }
  if (["0", "false", "no", "n", "off"].includes(value)) {
    return false;
  }
  return fallback;
}

function currentUtcDate() {
  return new Date().toISOString().slice(0, 10);
}

function requiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function optionalEnv(name) {
  return String(process.env[name] || "").trim();
}

function buildLogUrl() {
  const repository = optionalEnv("GITHUB_REPOSITORY");
  const runId = optionalEnv("GITHUB_RUN_ID");
  const serverUrl = optionalEnv("GITHUB_SERVER_URL") || "https://github.com";
  if (!repository || !runId) {
    return null;
  }
  return `${serverUrl}/${repository}/actions/runs/${runId}`;
}

async function writeGithubOutputs(values) {
  const outputFile = optionalEnv("GITHUB_OUTPUT");
  if (!outputFile) {
    return;
  }
  const lines = [];
  for (const [key, value] of Object.entries(values)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    lines.push(`${key}=${value}`);
  }
  if (lines.length === 0) {
    return;
  }

  const fs = await import("node:fs/promises");
  await fs.appendFile(outputFile, `${lines.join("\n")}\n`, { encoding: "utf-8" });
}

export function isTransientSupabaseError(error) {
  const status = Number(error?.status ?? error?.statusCode ?? error?.response?.status);
  if (SUPABASE_TRANSIENT_STATUS_CODES.has(status)) {
    return true;
  }
  const causeCode = String(error?.cause?.code || "").toUpperCase();
  if ([
    "ECONNRESET",
    "ECONNREFUSED",
    "ENOTFOUND",
    "EPIPE",
    "ETIMEDOUT",
    "UND_ERR_CONNECT_TIMEOUT",
    "UND_ERR_HEADERS_TIMEOUT",
    "UND_ERR_BODY_TIMEOUT",
    "UND_ERR_SOCKET",
  ].includes(causeCode)) {
    return true;
  }
  const name = String(error?.name || "").toLowerCase();
  const message = String(error?.message || "").toLowerCase();
  return name === "aborterror" || message.includes("timed out");
}

export function isRetrySafeHealthRpc(rpcName) {
  return RETRY_SAFE_HEALTH_RPCS.has(rpcName);
}

function defaultSleep(delayMs) {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

export async function retrySupabaseOperation(operationName, operation, {
  sleep = defaultSleep,
  logger = console,
} = {}) {
  for (let attempt = 1; attempt <= SUPABASE_RETRY_MAX_ATTEMPTS; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      if (!isTransientSupabaseError(error) || attempt === SUPABASE_RETRY_MAX_ATTEMPTS) {
        throw error;
      }
      const delayMs = SUPABASE_RETRY_DELAYS_MS[attempt - 1];
      const status = Number(error?.status ?? error?.statusCode ?? error?.response?.status);
      const reason = SUPABASE_TRANSIENT_STATUS_CODES.has(status)
        ? `HTTP ${status}`
        : error?.cause?.code || error?.name || "connection failure";
      logger.warn(
        `Retrying Supabase ${operationName} after transient ${reason} `
        + `(attempt ${attempt + 1}/${SUPABASE_RETRY_MAX_ATTEMPTS}; waiting ${delayMs}ms).`,
      );
      await sleep(delayMs);
    }
  }
  throw new Error("Unreachable Supabase retry state.");
}

export async function postRpc(
  { supabaseUrl, serviceRoleKey, rpcName, body },
  { fetchImpl = fetch, sleep, logger } = {},
) {
  const request = async () => {
    const response = await fetchImpl(`${supabaseUrl}/rest/v1/rpc/${rpcName}`, {
      method: "POST",
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
        "Content-Type": "application/json",
        "Accept-Profile": RPC_SCHEMA,
        "Content-Profile": RPC_SCHEMA,
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const error = new Error(`RPC ${rpcName} failed (${response.status}).`);
      error.status = response.status;
      throw error;
    }

    const text = await response.text();
    return text.trim() ? JSON.parse(text) : null;
  };
  if (!isRetrySafeHealthRpc(rpcName)) {
    return request();
  }
  return retrySupabaseOperation(`RPC ${rpcName}`, request, { sleep, logger });
}

function mapJobStatus(jobStatus) {
  return String(jobStatus || "").trim().toLowerCase() === "success"
    ? "Finished"
    : "Failed";
}

function mapReportStage(rawStage) {
  const value = String(rawStage || "final").trim().toLowerCase();
  if (value === "started" || value === "final") {
    return value;
  }
  throw new Error(`Invalid DAILY_TASK_HEALTH_REPORT_STAGE: ${rawStage}`);
}

function buildSummary(jobStatus) {
  return {
    github_repository: optionalEnv("GITHUB_REPOSITORY") || null,
    github_workflow: optionalEnv("GITHUB_WORKFLOW") || null,
    github_run_id: optionalEnv("GITHUB_RUN_ID") || null,
    github_run_number: optionalEnv("GITHUB_RUN_NUMBER") || null,
    github_run_attempt: optionalEnv("GITHUB_RUN_ATTEMPT") || null,
    github_sha: optionalEnv("GITHUB_SHA") || null,
    github_ref_name: optionalEnv("GITHUB_REF_NAME") || null,
    github_event_name: optionalEnv("GITHUB_EVENT_NAME") || null,
    github_actor: optionalEnv("GITHUB_ACTOR") || null,
    job_status: jobStatus || null,
    trigger: "github_actions",
  };
}

function stripUndefined(input) {
  Object.keys(input).forEach((key) => {
    if (input[key] === undefined) {
      delete input[key];
    }
  });
  return input;
}

async function main() {
  const disabled = parseBoolean(process.env.DAILY_TASK_HEALTH_DISABLED, false);
  const strict = parseBoolean(process.env.DAILY_TASK_HEALTH_STRICT, false);
  if (disabled) {
    console.log("Daily task health reporting disabled by DAILY_TASK_HEALTH_DISABLED=true.");
    return;
  }

  try {
    const stage = mapReportStage(process.env.DAILY_TASK_HEALTH_REPORT_STAGE);
    const supabaseUrl = requiredEnv("SUPABASE_URL").replace(/\/+$/, "");
    const serviceRoleKey = requiredEnv("SUPABASE_SERVICE_ROLE_KEY");
    const taskKey = requiredEnv("DAILY_TASK_KEY");
    const scheduledForDate = optionalEnv("DAILY_TASK_SCHEDULED_FOR_DATE") || currentUtcDate();
    const now = new Date().toISOString();
    const logUrl = buildLogUrl();
    const sourceRepo = optionalEnv("GITHUB_REPOSITORY") || null;
    const sourceWorker = optionalEnv("GITHUB_WORKFLOW") || null;
    const platformRunId = optionalEnv("GITHUB_RUN_ID") || null;

    if (stage === "started") {
      const startedPayload = stripUndefined({
        task_key: taskKey,
        scheduled_for_date: scheduledForDate,
        started_at: now,
        summary: buildSummary("started"),
        source_repo: sourceRepo,
        source_worker: sourceWorker,
        platform_run_id: platformRunId,
        log_url: logUrl,
      });

      const runId = await postRpc({
        supabaseUrl,
        serviceRoleKey,
        rpcName: "uk_aq_rpc_daily_task_started",
        body: { p: startedPayload },
      });

      const healthRunId = typeof runId === "string" ? runId : "";
      await writeGithubOutputs({ health_run_id: healthRunId });

      console.log(
        `Reported daily task health STARTED: task_key=${taskKey}, date=${scheduledForDate}, run_id=${healthRunId || '<none>'}`,
      );
      return;
    }

    const jobStatus = requiredEnv("JOB_STATUS");
    const status = mapJobStatus(jobStatus);
    const healthRunId = optionalEnv("DAILY_TASK_HEALTH_RUN_ID");

    if (healthRunId) {
      const payload = stripUndefined({
        summary: buildSummary(jobStatus),
        finished_at: status === "Finished" ? now : undefined,
        failed_at: status === "Failed" ? now : undefined,
        error_message: status === "Failed"
          ? `GitHub Actions job ended with status: ${jobStatus}`
          : undefined,
        error: status === "Failed"
          ? {
            job_status: jobStatus,
            github_run_id: optionalEnv("GITHUB_RUN_ID") || null,
            github_run_number: optionalEnv("GITHUB_RUN_NUMBER") || null,
            log_url: logUrl,
          }
          : undefined,
        source_repo: sourceRepo,
        source_worker: sourceWorker,
        platform_run_id: platformRunId,
        log_url: logUrl,
      });

      await postRpc({
        supabaseUrl,
        serviceRoleKey,
        rpcName: status === "Finished"
          ? "uk_aq_rpc_daily_task_finished"
          : "uk_aq_rpc_daily_task_failed",
        body: {
          p_run_id: healthRunId,
          p: payload,
        },
      });

      console.log(
        `Reported daily task health via run_id: task_key=${taskKey}, status=${status}, date=${scheduledForDate}, run_id=${healthRunId}`,
      );
    } else {
      const reportPayload = stripUndefined({
        task_key: taskKey,
        status,
        scheduled_for_date: scheduledForDate,
        started_at: optionalEnv("DAILY_TASK_STARTED_AT") || undefined,
        finished_at: status === "Finished" ? now : undefined,
        failed_at: status === "Failed" ? now : undefined,
        summary: buildSummary(jobStatus),
        error_message: status === "Failed"
          ? `GitHub Actions job ended with status: ${jobStatus}`
          : undefined,
        error: status === "Failed"
          ? {
            job_status: jobStatus,
            github_run_id: optionalEnv("GITHUB_RUN_ID") || null,
            github_run_number: optionalEnv("GITHUB_RUN_NUMBER") || null,
            log_url: logUrl,
          }
          : undefined,
        source_repo: sourceRepo,
        source_worker: sourceWorker,
        platform_run_id: platformRunId,
        log_url: logUrl,
      });

      await postRpc({
        supabaseUrl,
        serviceRoleKey,
        rpcName: "uk_aq_rpc_daily_task_report_final",
        body: { p: reportPayload },
      });

      console.log(
        `Reported daily task health FINAL (fallback): task_key=${taskKey}, status=${status}, date=${scheduledForDate}`,
      );
    }

    await postRpc({
      supabaseUrl,
      serviceRoleKey,
      rpcName: "uk_aq_rpc_recompute_daily_task_status",
      body: { p_date: scheduledForDate },
    });
    console.log(`Recomputed daily task status for ${scheduledForDate}.`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (strict) {
      throw error;
    }
    console.warn(`Daily task health reporting warning: ${message}`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await main();
}
