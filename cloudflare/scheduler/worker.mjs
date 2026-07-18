import { jsonResponse, logJson, nowIso, readSecret } from "./shared.mjs";

export const SCHEDULER_NAME = "uk-aq-cron-scheduler-ingest";
export const WORKER_NAME = "uk-aq-cron-scheduler-ingest";
export const D1_BINDING_NAME = "SCHEDULER_DB";
export const DEFAULT_LOOKBACK_MINUTES = 2;
export const MINUTE_MS = 60_000;
export const DISPATCH_LEAD_MINUTES = 0.5;
export const DISPATCH_LEAD_MS = DISPATCH_LEAD_MINUTES * MINUTE_MS;
export const RESPONSE_PREVIEW_LIMIT = 1_000;
export const GITHUB_USER_AGENT = "uk-aq-cloudflare-cron-scheduler";
export const TRIGGER_SOURCE_CLOUDFLARE_CRON = "cloudflare_cron";
export const TRIGGER_SOURCE_EXTERNAL_WATCHDOG = "external_watchdog";

const SCHEDULER_TRIGGER_HEADER = "x-uk-aq-scheduler-trigger";
const SCHEDULER_TRIGGER_SECRET_MAX_LENGTH = 512;
const ALLOWED_TRIGGER_SOURCES = new Set([
  TRIGGER_SOURCE_CLOUDFLARE_CRON,
  TRIGGER_SOURCE_EXTERNAL_WATCHDOG,
]);
export const BLONDON_NODES_JOB_KEY = "uk_aq_blondon_nodes";
export const BLONDON_NODES_RESPONSE_WAIT_MS = 90_000;
export const BLONDON_NODES_RECONCILE_AFTER_MS = [
  5 * 60_000,
  10 * 60_000,
  14 * 60_000,
];
const BLONDON_NODES_STATUS_MATCH_BEFORE_MS = 30_000;
const BLONDON_NODES_STATUS_MATCH_AFTER_MS = 4 * 60_000;
const BLONDON_NODES_STATUS_TIMEOUT_MS = 20_000;

const MONTH_NAME_TO_NUMBER = Object.freeze({
  JAN: 1,
  FEB: 2,
  MAR: 3,
  APR: 4,
  MAY: 5,
  JUN: 6,
  JUL: 7,
  AUG: 8,
  SEP: 9,
  OCT: 10,
  NOV: 11,
  DEC: 12,
});

const DOW_NAME_TO_NUMBER = Object.freeze({
  SUN: 0,
  MON: 1,
  TUE: 2,
  WED: 3,
  THU: 4,
  FRI: 5,
  SAT: 6,
});

function trimText(value) {
  return String(value ?? "").trim();
}

function boundedText(value, maxLength) {
  const text = trimText(value);
  return text && text.length <= maxLength ? text : "";
}

export function canonicalMinuteSlot(nowMs) {
  const timestamp = Number(nowMs);
  if (!Number.isFinite(timestamp)) {
    throw new Error("Invalid scheduler invocation time");
  }
  return nowIso(Math.floor(timestamp / MINUTE_MS) * MINUTE_MS);
}

function normalizeTriggerSource(value) {
  const source = boundedText(value, 64);
  if (!ALLOWED_TRIGGER_SOURCES.has(source)) {
    throw new Error("Invalid scheduler trigger source");
  }
  return source;
}

async function hasValidSchedulerTriggerSecret(request, env) {
  const provided = boundedText(
    request.headers.get(SCHEDULER_TRIGGER_HEADER),
    SCHEDULER_TRIGGER_SECRET_MAX_LENGTH,
  );
  const expected = boundedText(
    await readSecret(env?.UK_AQ_SCHEDULER_TRIGGER_SECRET),
    SCHEDULER_TRIGGER_SECRET_MAX_LENGTH,
  );
  return Boolean(provided && expected && provided === expected);
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function parseInteger(value, fieldName) {
  const parsed = Number(trimText(value));
  if (!Number.isInteger(parsed)) {
    throw new Error(`Invalid ${fieldName}: expected an integer`);
  }
  return parsed;
}

function parseBinaryFlag(value, fieldName) {
  const parsed = parseInteger(value, fieldName);
  if (parsed !== 0 && parsed !== 1) {
    throw new Error(`Invalid ${fieldName}: expected 0 or 1`);
  }
  return parsed;
}

function parseJsonValue(text, fieldName, { allowEmptyObject = false } = {}) {
  const raw = trimText(text);
  if (!raw) {
    return allowEmptyObject ? {} : null;
  }

  try {
    return JSON.parse(raw);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Invalid JSON in ${fieldName}: ${message}`);
  }
}

function parseJsonObject(text, fieldName) {
  const parsed = parseJsonValue(text, fieldName, { allowEmptyObject: true });
  if (!isPlainObject(parsed)) {
    throw new Error(`Invalid JSON in ${fieldName}: expected a JSON object`);
  }
  return parsed;
}

function parseJsonBody(text, fieldName) {
  const parsed = parseJsonValue(text, fieldName, { allowEmptyObject: true });
  return parsed === null ? {} : parsed;
}

function normalizeTimezone(value) {
  const normalized = trimText(value);
  if (!normalized) {
    return "UTC";
  }
  if (normalized === "UTC" || normalized === "Etc/UTC") {
    return normalized;
  }
  throw new Error(`Unsupported timezone ${JSON.stringify(normalized)}; expected UTC`);
}

function normalizeCronAtom(token, fieldName, min, max, names = {}) {
  const normalized = trimText(token).toUpperCase();
  if (!normalized) {
    throw new Error(`Invalid ${fieldName}: empty token`);
  }

  if (Object.prototype.hasOwnProperty.call(names, normalized)) {
    return names[normalized];
  }

  if (fieldName === "day-of-week" && normalized === "7") {
    return 0;
  }

  const parsed = Number(normalized);
  if (!Number.isInteger(parsed)) {
    throw new Error(`Invalid ${fieldName}: ${JSON.stringify(token)} is not a valid value`);
  }
  if (parsed < min || parsed > max) {
    throw new Error(`Invalid ${fieldName}: ${parsed} outside ${min}-${max}`);
  }
  return fieldName === "day-of-week" && parsed === 7 ? 0 : parsed;
}

function expandCronToken(token, fieldName, min, max, names = {}) {
  const parts = trimText(token).toUpperCase();
  if (!parts) {
    throw new Error(`Invalid ${fieldName}: empty cron token`);
  }

  let base = parts;
  let step = 1;
  if (parts.includes("/")) {
    const segments = parts.split("/");
    if (segments.length !== 2) {
      throw new Error(`Invalid ${fieldName}: ${JSON.stringify(token)}`);
    }
    [base] = segments;
    step = Number(trimText(segments[1]));
    if (!Number.isInteger(step) || step <= 0) {
      throw new Error(`Invalid ${fieldName}: step must be a positive integer`);
    }
  }

  let start;
  let end;
  if (base === "*") {
    start = min;
    end = max;
  } else if (base.includes("-")) {
    const range = base.split("-");
    if (range.length !== 2) {
      throw new Error(`Invalid ${fieldName}: ${JSON.stringify(token)}`);
    }
    start = normalizeCronAtom(range[0], fieldName, min, max, names);
    end = normalizeCronAtom(range[1], fieldName, min, max, names);
  } else {
    start = normalizeCronAtom(base, fieldName, min, max, names);
    end = start;
  }

  if (start > end) {
    throw new Error(`Invalid ${fieldName}: range start must be <= range end`);
  }

  const values = new Set();
  for (let value = start; value <= end; value += step) {
    values.add(fieldName === "day-of-week" && value === 7 ? 0 : value);
  }

  return values;
}

function parseCronField(field, fieldName, min, max, names = {}) {
  const normalized = trimText(field).toUpperCase();
  if (!normalized) {
    throw new Error(`Invalid cron expression: missing ${fieldName}`);
  }
  if (normalized === "*") {
    return { any: true, values: null };
  }

  const values = new Set();
  for (const token of normalized.split(",")) {
    const expanded = expandCronToken(token, fieldName, min, max, names);
    for (const value of expanded) {
      values.add(value);
    }
  }

  return { any: false, values };
}

export function parseCronExpression(expr) {
  const fields = trimText(expr).split(/\s+/);
  if (fields.length !== 5) {
    throw new Error(`Invalid cron expression ${JSON.stringify(expr)}: expected five fields`);
  }

  return {
    minute: parseCronField(fields[0], "minute", 0, 59),
    hour: parseCronField(fields[1], "hour", 0, 23),
    dayOfMonth: parseCronField(fields[2], "day-of-month", 1, 31),
    month: parseCronField(fields[3], "month", 1, 12, MONTH_NAME_TO_NUMBER),
    dayOfWeek: parseCronField(fields[4], "day-of-week", 0, 7, DOW_NAME_TO_NUMBER),
  };
}

function fieldMatches(field, value) {
  return field.any || field.values.has(value);
}

export function cronMatchesDate(parsedCron, date) {
  const minute = date.getUTCMinutes();
  const hour = date.getUTCHours();
  const dayOfMonth = date.getUTCDate();
  const month = date.getUTCMonth() + 1;
  const dayOfWeek = date.getUTCDay();

  const minuteMatches = fieldMatches(parsedCron.minute, minute);
  const hourMatches = fieldMatches(parsedCron.hour, hour);
  const monthMatches = fieldMatches(parsedCron.month, month);
  const domMatches = fieldMatches(parsedCron.dayOfMonth, dayOfMonth);
  const dowMatches = fieldMatches(parsedCron.dayOfWeek, dayOfWeek);

  const dayMatches = parsedCron.dayOfMonth.any || parsedCron.dayOfWeek.any
    ? domMatches && dowMatches
    : domMatches || dowMatches;

  return minuteMatches && hourMatches && monthMatches && dayMatches;
}

export function cronDueTimes(expr, startExclusiveMs, endInclusiveMs) {
  const parsedCron = parseCronExpression(expr);
  const start = Math.floor(Number(startExclusiveMs) / MINUTE_MS) * MINUTE_MS + MINUTE_MS;
  const end = Math.floor(Number(endInclusiveMs) / MINUTE_MS) * MINUTE_MS;

  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) {
    return [];
  }

  const dueTimes = [];
  for (let timestamp = start; timestamp <= end; timestamp += MINUTE_MS) {
    if (cronMatchesDate(parsedCron, new Date(timestamp))) {
      dueTimes.push(nowIso(timestamp));
    }
  }
  return dueTimes;
}

function truncatePreview(text, limit = RESPONSE_PREVIEW_LIMIT) {
  const raw = trimText(text);
  if (!raw) {
    return null;
  }
  if (raw.length <= limit) {
    return raw;
  }
  return `${raw.slice(0, Math.max(0, limit - 3))}...`;
}

function parseSchedulerJobRow(row) {
  const jobKey = trimText(row.job_key);
  if (!jobKey) {
    throw new Error("Invalid scheduler job: missing job_key");
  }

  const enabled = parseBinaryFlag(row.enabled, `scheduler_jobs.enabled for ${jobKey}`);
  const targetType = trimText(row.target_type);
  if (targetType !== "github_workflow" && targetType !== "cloud_run") {
    throw new Error(`Invalid scheduler job ${jobKey}: unsupported target_type ${JSON.stringify(targetType)}`);
  }

  const cronExpr = trimText(row.cron_expr);
  if (!cronExpr) {
    throw new Error(`Invalid scheduler job ${jobKey}: missing cron_expr`);
  }

  const timezone = normalizeTimezone(row.timezone);
  const dryRun = parseBinaryFlag(row.dry_run, `scheduler_jobs.dry_run for ${jobKey}`);

  const normalized = {
    job_key: jobKey,
    enabled,
    target_type: targetType,
    cron_expr: cronExpr,
    timezone,
    dry_run: dryRun,
    notes: trimText(row.notes) || null,
  };

  if (targetType === "github_workflow") {
    const githubRepo = trimText(row.github_repo);
    const githubWorkflowFile = trimText(row.github_workflow_file);
    const githubRef = trimText(row.github_ref) || "main";
    if (!githubRepo || !githubWorkflowFile || !githubRef) {
      throw new Error(`Invalid scheduler job ${jobKey}: github_workflow requires github_repo, github_workflow_file, and github_ref`);
    }

    normalized.github_repo = githubRepo;
    normalized.github_workflow_file = githubWorkflowFile;
    normalized.github_ref = githubRef;
    normalized.github_inputs = parseJsonObject(row.github_inputs_json, `scheduler_jobs.github_inputs_json for ${jobKey}`);
  } else {
    const cloudRunUrl = trimText(row.cloud_run_url);
    if (!cloudRunUrl) {
      throw new Error(`Invalid scheduler job ${jobKey}: cloud_run requires cloud_run_url`);
    }

    normalized.cloud_run_url = cloudRunUrl;
    normalized.cloud_run_method = trimText(row.cloud_run_method) || "POST";
    normalized.cloud_run_headers = parseJsonObject(row.cloud_run_headers_json, `scheduler_jobs.cloud_run_headers_json for ${jobKey}`);
    normalized.cloud_run_body = parseJsonBody(row.cloud_run_body_json, `scheduler_jobs.cloud_run_body_json for ${jobKey}`);
  }

  return normalized;
}

function requireD1Db(db) {
  if (!db || typeof db.prepare !== "function") {
    throw new Error(`Missing required Worker binding: ${D1_BINDING_NAME}`);
  }
  return db;
}

function bindStatement(db, sql, params = []) {
  const statement = db.prepare(sql);
  return params.length > 0 ? statement.bind(...params) : statement;
}

async function dbAll(db, sql, params = []) {
  const result = await bindStatement(db, sql, params).all();
  return Array.isArray(result?.results) ? result.results : [];
}

async function dbFirst(db, sql, params = []) {
  const result = await bindStatement(db, sql, params).first();
  return result ?? null;
}

async function dbRun(db, sql, params = []) {
  return bindStatement(db, sql, params).run();
}

export function createSchedulerStore(db) {
  const schedulerDb = requireD1Db(db);

  return {
    async getLatestFinishedRun(schedulerName, minuteSlot) {
      return dbFirst(
        schedulerDb,
        `
          select id, scheduler_name, started_at, evaluation_window_end
          from scheduler_runs
          where scheduler_name = ?
            and status = 'finished'
            and evaluation_window_end is not null
            and evaluation_window_end < ?
          order by evaluation_window_end desc, id desc
          limit 1
        `,
        [schedulerName, minuteSlot],
      );
    },

    async claimMinute(run) {
      const result = await dbRun(
        schedulerDb,
        `
          insert or ignore into scheduler_runs (
            scheduler_name,
            minute_slot,
            trigger_source,
            started_at,
            status,
            previous_run_started_at,
            evaluation_window_start,
            evaluation_window_end,
            jobs_checked,
            jobs_due,
            jobs_claimed,
            jobs_dispatched,
            jobs_failed,
            error_message
          ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          run.scheduler_name,
          run.minute_slot,
          run.trigger_source,
          run.started_at,
          run.status,
          run.previous_run_started_at,
          run.evaluation_window_start,
          run.evaluation_window_end,
          run.jobs_checked,
          run.jobs_due,
          run.jobs_claimed,
          run.jobs_dispatched,
          run.jobs_failed,
          run.error_message,
        ],
      );
      if (Number(result?.meta?.changes ?? 0) > 0) {
        return {
          claimed: true,
          scheduler_run_id: Number(result?.meta?.last_row_id ?? 0) || null,
          run_status: "started",
          trigger_source: run.trigger_source,
        };
      }
      const existing = await dbFirst(
        schedulerDb,
        `
          select id, status, trigger_source
          from scheduler_runs
          where scheduler_name = ? and minute_slot = ?
          limit 1
        `,
        [run.scheduler_name, run.minute_slot],
      );
      if (!existing) {
        throw new Error("Scheduler minute claim was not recorded");
      }
      return {
        claimed: false,
        scheduler_run_id: Number(existing.id) || null,
        run_status: boundedText(existing.status, 64) || "unknown",
        trigger_source: boundedText(existing.trigger_source, 64) || null,
      };
    },

    async listEnabledJobs() {
      return dbAll(
        schedulerDb,
        `
          select
            job_key,
            enabled,
            target_type,
            cron_expr,
            timezone,
            github_repo,
            github_workflow_file,
            github_ref,
            github_inputs_json,
            cloud_run_url,
            cloud_run_method,
            cloud_run_headers_json,
            cloud_run_body_json,
            dry_run,
            notes,
            created_at,
            updated_at
          from scheduler_jobs
          where enabled = 1
          order by job_key
        `,
      );
    },

    async claimDispatch(claim) {
      const result = await dbRun(
        schedulerDb,
        `
          insert or ignore into scheduler_dispatches (
            job_key,
            due_at,
            claimed_at,
            dispatched_at,
            target_type,
            dry_run,
            dispatch_status,
            reason,
            response_status,
            response_preview
          ) values (?, ?, ?, null, ?, ?, 'claimed', null, null, null)
        `,
        [
          claim.job_key,
          claim.due_at,
          claim.claimed_at,
          claim.target_type,
          claim.dry_run,
        ],
      );
      const changes = Number(result?.meta?.changes ?? 0);
      return changes > 0 ? Number(result?.meta?.last_row_id ?? 0) || null : null;
    },

    async updateDispatch(dispatchId, patch) {
      const patchColumns = [
        "dispatched_at",
        "dispatch_status",
        "reason",
        "response_status",
        "response_preview",
        "next_reconcile_at",
        "reconcile_stage",
        "ingest_status",
      ].filter((column) => Object.prototype.hasOwnProperty.call(patch, column));
      if (patchColumns.length === 0) {
        return;
      }
      await dbRun(
        schedulerDb,
        `
          update scheduler_dispatches
          set ${patchColumns.map((column) => `${column} = ?`).join(",\n              ")}
          where id = ?
        `,
        [...patchColumns.map((column) => patch[column]), dispatchId],
      );
    },

    async listDueBlondonNodesReconciliations(nowIsoText) {
      return dbAll(
        schedulerDb,
        `
          select
            d.id,
            d.due_at,
            d.claimed_at,
            d.next_reconcile_at,
            d.reconcile_stage,
            j.cloud_run_url
          from scheduler_dispatches d
          join scheduler_jobs j on j.job_key = d.job_key
          where d.job_key = ?
            and d.dispatch_status = 'waiting_response'
            and d.next_reconcile_at is not null
            and d.next_reconcile_at <= ?
          order by d.next_reconcile_at asc, d.id asc
        `,
        [BLONDON_NODES_JOB_KEY, nowIsoText],
      );
    },

    async finishBlondonNodesResponse(dispatchId, patch) {
      const result = await dbRun(
        schedulerDb,
        `
          update scheduler_dispatches
          set dispatched_at = ?,
              dispatch_status = ?,
              reason = ?,
              response_status = ?,
              response_preview = ?,
              next_reconcile_at = null,
              reconcile_stage = null
          where id = ?
            and dispatch_status in ('claimed', 'waiting_response')
        `,
        [
          patch.dispatched_at,
          patch.dispatch_status,
          patch.reason ?? null,
          patch.response_status ?? null,
          patch.response_preview ?? null,
          dispatchId,
        ],
      );
      return Number(result?.meta?.changes ?? 0) > 0;
    },

    async recordBlondonNodesProxyTimeout(dispatchId, patch) {
      await dbRun(
        schedulerDb,
        `
          update scheduler_dispatches
          set reason = ?,
              response_status = ?,
              response_preview = ?
          where id = ?
            and dispatch_status = 'waiting_response'
        `,
        [patch.reason, patch.response_status ?? null, patch.response_preview ?? null, dispatchId],
      );
    },

    async finishRun(runId, patch) {
      await dbRun(
        schedulerDb,
        `
          update scheduler_runs
          set status = ?,
              finished_at = ?,
              previous_run_started_at = ?,
              evaluation_window_start = ?,
              evaluation_window_end = ?,
              jobs_checked = ?,
              jobs_due = ?,
              jobs_claimed = ?,
              jobs_dispatched = ?,
              jobs_failed = ?,
              error_message = ?
          where id = ?
        `,
        [
          patch.status,
          patch.finished_at,
          patch.previous_run_started_at,
          patch.evaluation_window_start,
          patch.evaluation_window_end,
          patch.jobs_checked,
          patch.jobs_due,
          patch.jobs_claimed,
          patch.jobs_dispatched,
          patch.jobs_failed,
          patch.error_message ?? null,
          runId,
        ],
      );
    },
  };
}

async function dispatchGitHubWorkflow(job, env) {
  const token = trimText(await readSecret(env.UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT));
  if (!token) {
    throw new Error("Missing required Worker secret: UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT");
  }

  const repoParts = job.github_repo.split("/");

  if (repoParts.length !== 2 || !repoParts[0] || !repoParts[1]) {
    throw new Error(
      `Invalid github_repo ${JSON.stringify(job.github_repo)}; expected owner/repo`,
    );
  }

  const [owner, repo] = repoParts;

  const url =
    `https://api.github.com/repos/${encodeURIComponent(owner)}` +
    `/${encodeURIComponent(repo)}` +
    `/actions/workflows/${encodeURIComponent(job.github_workflow_file)}` +
    `/dispatches`;
	
  const response = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "Content-Type": "application/json",
      "User-Agent": GITHUB_USER_AGENT,
    },
    body: JSON.stringify({
      ref: job.github_ref,
      inputs: job.github_inputs,
    }),
  });

  if (response.status === 204) {
    return {
      ok: true,
      response_status: 204,
      response_preview: null,
    };
  }

  const responsePreview = truncatePreview(await response.text());
  return {
    ok: false,
    response_status: response.status,
    response_preview: responsePreview,
    reason: `GitHub dispatch failed with HTTP ${response.status}`,
  };
}

async function dispatchCloudRun(job, env) {
  const secret = trimText(await readSecret(env.UK_AQ_EDGE_UPSTREAM_SECRET));
  if (!secret) {
    throw new Error("Missing required Worker secret: UK_AQ_EDGE_UPSTREAM_SECRET");
  }

  const headers = new Headers();
  for (const [key, value] of Object.entries(job.cloud_run_headers)) {
    if (value !== undefined && value !== null) {
      headers.set(key, String(value));
    }
  }
  headers.set("x-uk-aq-dispatch-secret", secret);

  const method = trimText(job.cloud_run_method || "POST").toUpperCase();
  const fetchOptions = {
    method,
    headers,
  };

  if (method !== "GET" && method !== "HEAD") {
    if (!headers.has("content-type")) {
      headers.set("content-type", "application/json; charset=utf-8");
    }
    fetchOptions.body = JSON.stringify(job.cloud_run_body ?? {});
  }

  const response = await fetch(job.cloud_run_url, fetchOptions);
  const responseText = await response.text();
  const responsePreview = truncatePreview(responseText.split(secret).join("[REDACTED]"));
  return response.ok
    ? {
        ok: true,
        response_status: response.status,
        response_preview: responsePreview,
      }
    : {
        ok: false,
        response_status: response.status,
        response_preview: responsePreview,
        reason: `Cloud Run dispatch failed with HTTP ${response.status}`,
      };
}

async function dispatchTarget(job, env) {
  if (job.target_type === "github_workflow") {
    return dispatchGitHubWorkflow(job, env);
  }
  if (job.target_type === "cloud_run") {
    return dispatchCloudRun(job, env);
  }
  throw new Error(`Unsupported target_type ${JSON.stringify(job.target_type)}`);
}

function formatLogJobPayload(job, dueAt) {
  return {
    scheduler_name: SCHEDULER_NAME,
    job_key: job.job_key,
    target_type: job.target_type,
    due_at: dueAt,
    dry_run: job.dry_run === 1,
  };
}

export function dueSlotsForJob(job, windowStartMs, windowEndMs) {
  return cronDueTimes(
    job.cron_expr,
    windowStartMs + DISPATCH_LEAD_MS,
    windowEndMs + DISPATCH_LEAD_MS,
  );
}

export function latestDueSlotForJob(job, windowStartMs, windowEndMs) {
  const dueSlots = dueSlotsForJob(job, windowStartMs, windowEndMs);
  return dueSlots.length > 0 ? dueSlots.at(-1) : null;
}

function delay(delayMs) {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

function isProxyTimeoutResult(result) {
  return Number(result?.response_status) >= 520 && Number(result?.response_status) <= 524;
}

function isProxyTimeoutError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return /\b(?:520|522|523|524)\b|timeout|timed out/i.test(message);
}

function isRecognizedSkippedResult(result) {
  const text = `${result?.reason ?? ""} ${result?.response_preview ?? ""}`.toLowerCase();
  return ["run_in_flight", "not_due", "claim_not_acquired"].some((reason) => text.includes(reason));
}

function addMillisecondsToIso(isoTimestamp, milliseconds) {
  const timestampMs = Date.parse(isoTimestamp);
  if (!Number.isFinite(timestampMs)) {
    throw new Error(`Invalid scheduler dispatch timestamp: ${isoTimestamp}`);
  }
  return nowIso(timestampMs + milliseconds);
}

async function dispatchTargetSafely(job, env) {
  try {
    return { result: await dispatchTarget(job, env) };
  } catch (error) {
    return {
      error,
      result: {
        ok: false,
        response_status: null,
        response_preview: null,
        reason: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

async function persistLiveDispatchOutcome(store, job, dispatchId, dueAt, context, outcome, { nodesOnly = false } = {}) {
  const { result, error: dispatchError } = outcome;
  const skipped = !result.ok && isRecognizedSkippedResult(result);
  const dispatchStatus = result.ok ? "dispatched" : skipped ? "skipped" : "failed";
  const patch = {
    dispatched_at: nowIso(Date.now()),
    dispatch_status: dispatchStatus,
    reason: result.ok ? null : result.reason,
    response_status: result.response_status ?? null,
    response_preview: result.response_preview ?? null,
  };
  if (nodesOnly) {
    await store.finishBlondonNodesResponse(dispatchId, patch);
  } else {
    await store.updateDispatch(dispatchId, patch);
  }

  logJson(WORKER_NAME, dispatchError ? "scheduler_dispatch_failed" : "scheduler_dispatch_result", {
    scheduler_name: SCHEDULER_NAME,
    scheduler_run_id: context.scheduler_run_id ?? null,
    job_key: job.job_key,
    target_type: job.target_type,
    due_at: dueAt,
    dry_run: false,
    dispatch_status: dispatchStatus,
    reason: result.reason ?? null,
    response_status: result.response_status ?? null,
  });
  return { jobs_dispatched: result.ok ? 1 : 0, jobs_failed: result.ok || skipped ? 0 : 1 };
}

async function dispatchBlondonNodes(store, dispatch, env, context) {
  const { dispatch_id: dispatchId, due_at: dueAt, job, claimed_at: claimedAt } = dispatch;
  const requestOutcome = dispatchTargetSafely(job, env);
  logJson(WORKER_NAME, "scheduler_dispatch_attempt", formatLogJobPayload(job, dueAt));
  const firstOutcome = await Promise.race([
    requestOutcome.then((outcome) => ({ type: "response", outcome })),
    delay(BLONDON_NODES_RESPONSE_WAIT_MS).then(() => ({ type: "deadline" })),
  ]);

  if (firstOutcome.type === "response") {
    return persistLiveDispatchOutcome(store, job, dispatchId, dueAt, context, firstOutcome.outcome, { nodesOnly: true });
  }

  await store.updateDispatch(dispatchId, {
    dispatch_status: "waiting_response",
    next_reconcile_at: addMillisecondsToIso(claimedAt, BLONDON_NODES_RECONCILE_AFTER_MS[0]),
    reconcile_stage: 1,
  });
  logJson(WORKER_NAME, "scheduler_dispatch_waiting_response", {
    scheduler_name: SCHEDULER_NAME,
    scheduler_run_id: context.scheduler_run_id ?? null,
    dispatch_id: dispatchId,
    job_key: job.job_key,
    due_at: dueAt,
  });

  const eventualOutcome = await requestOutcome;
  if (eventualOutcome.result.ok
    || (!isProxyTimeoutResult(eventualOutcome.result) && !isProxyTimeoutError(eventualOutcome.error))) {
    return persistLiveDispatchOutcome(store, job, dispatchId, dueAt, context, eventualOutcome, { nodesOnly: true });
  }

  await store.recordBlondonNodesProxyTimeout(dispatchId, {
    reason: eventualOutcome.result.reason ?? "proxy_timeout",
    response_status: eventualOutcome.result.response_status ?? null,
    response_preview: eventualOutcome.result.response_preview ?? null,
  });
  return { jobs_dispatched: 0, jobs_failed: 0 };
}

async function dispatchClaimedLiveJob(store, dispatch, env, context) {
  if (dispatch.job.job_key === BLONDON_NODES_JOB_KEY) {
    return dispatchBlondonNodes(store, dispatch, env, context);
  }
  logJson(WORKER_NAME, "scheduler_dispatch_attempt", formatLogJobPayload(dispatch.job, dispatch.due_at));
  return persistLiveDispatchOutcome(
    store,
    dispatch.job,
    dispatch.dispatch_id,
    dispatch.due_at,
    context,
    await dispatchTargetSafely(dispatch.job, env),
  );
}

function mapBlondonNodesStatus(status, message) {
  const normalizedStatus = trimText(status).toLowerCase();
  const normalizedMessage = trimText(message).toLowerCase();
  if (normalizedStatus === "partial" || /budget[-_ ]?limit/.test(normalizedMessage)) return "partial";
  if (normalizedStatus === "success" || normalizedStatus === "succeeded") return "succeeded";
  if (normalizedStatus === "skipped") return "skipped";
  if (normalizedStatus === "failed" || normalizedStatus === "error") return "failed";
  return "unconfirmed";
}

function blondonNodesStatusUrl(cloudRunUrl) {
  const url = new URL(cloudRunUrl);
  url.pathname = "/status";
  url.search = "";
  url.hash = "";
  return url.toString();
}

async function loadBlondonNodesStatus(cloudRunUrl, env) {
  const secret = trimText(await readSecret(env.UK_AQ_EDGE_UPSTREAM_SECRET));
  if (!secret) throw new Error("Missing required Worker secret: UK_AQ_EDGE_UPSTREAM_SECRET");
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), BLONDON_NODES_STATUS_TIMEOUT_MS);
  let response;
  try {
    response = await fetch(blondonNodesStatusUrl(cloudRunUrl), {
      headers: { "x-uk-aq-dispatch-secret": secret },
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
  if (!response.ok) throw new Error(`Breathe London Nodes status returned HTTP ${response.status}`);
  const status = await response.json();
  if (!isPlainObject(status)) throw new Error("Breathe London Nodes status response must be an object");
  return status;
}

function statusMatchesBlondonNodesDispatch(status, claimedAt) {
  if (trimText(status.connector_code) !== "blondon_nodes") return false;
  const startedAtMs = Date.parse(trimText(status.last_run_start));
  const claimedAtMs = Date.parse(claimedAt);
  return Number.isFinite(startedAtMs)
    && Number.isFinite(claimedAtMs)
    && startedAtMs >= claimedAtMs - BLONDON_NODES_STATUS_MATCH_BEFORE_MS
    && startedAtMs <= claimedAtMs + BLONDON_NODES_STATUS_MATCH_AFTER_MS;
}

async function reconcileBlondonNodesDispatches(store, env, nowMs, context) {
  if (typeof store.listDueBlondonNodesReconciliations !== "function") {
    return;
  }
  const dueDispatches = await store.listDueBlondonNodesReconciliations(nowIso(nowMs));
  for (const dispatch of dueDispatches) {
    const stage = Number(dispatch.reconcile_stage || 1);
    const finalStage = stage >= BLONDON_NODES_RECONCILE_AFTER_MS.length;
    let status;
    let reason = "status_unavailable";
    try {
      status = await loadBlondonNodesStatus(dispatch.cloud_run_url, env);
      reason = statusMatchesBlondonNodesDispatch(status, dispatch.claimed_at)
        ? "run_not_finished"
        : "status_does_not_match_dispatch";
    } catch (error) {
      reason = error instanceof Error ? error.message : String(error);
    }

    if (status && statusMatchesBlondonNodesDispatch(status, dispatch.claimed_at) && status.last_run_end) {
      const ingestStatus = mapBlondonNodesStatus(status.last_run_status, status.last_run_message);
      await store.updateDispatch(dispatch.id, {
        dispatch_status: "dispatched",
        next_reconcile_at: null,
        reconcile_stage: null,
        ingest_status: ingestStatus,
        reason: status.last_run_message ?? null,
      });
      continue;
    }
    if (finalStage) {
      await store.updateDispatch(dispatch.id, {
        dispatch_status: "dispatched",
        next_reconcile_at: null,
        reconcile_stage: null,
        ingest_status: "unconfirmed",
        reason,
      });
      continue;
    }
    await store.updateDispatch(dispatch.id, {
      next_reconcile_at: addMillisecondsToIso(
        dispatch.claimed_at,
        BLONDON_NODES_RECONCILE_AFTER_MS[stage],
      ),
      reconcile_stage: stage + 1,
      reason,
    });
  }
}

export async function dispatchDueJobsForWindow(store, jobs, env, windowStartMs, windowEndMs, context = {}) {
  const summary = {
    jobs_checked: 0,
    jobs_due: 0,
    jobs_claimed: 0,
    jobs_dispatched: 0,
    jobs_failed: 0,
  };
  const claimedLiveDispatches = [];

  for (const rawJob of jobs) {
    summary.jobs_checked += 1;

    let job;
    try {
      job = parseSchedulerJobRow(rawJob);
    } catch (error) {
      summary.jobs_failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      logJson(WORKER_NAME, "scheduler_job_invalid", {
        scheduler_name: SCHEDULER_NAME,
        scheduler_run_id: context.scheduler_run_id ?? null,
        job_key: trimText(rawJob?.job_key) || null,
        target_type: trimText(rawJob?.target_type) || null,
        reason: message,
      });
      continue;
    }

    let allDueTimes;
    try {
      allDueTimes = dueSlotsForJob(job, windowStartMs, windowEndMs);
    } catch (error) {
      summary.jobs_failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      logJson(WORKER_NAME, "scheduler_job_invalid", {
        scheduler_name: SCHEDULER_NAME,
        scheduler_run_id: context.scheduler_run_id ?? null,
        job_key: job.job_key,
        target_type: job.target_type,
        reason: message,
      });
      continue;
    }

    const latestDueAt = allDueTimes.at(-1) ?? null;
    const dueTimes = latestDueAt ? [latestDueAt] : [];
    summary.jobs_due += dueTimes.length;

    if (allDueTimes.length > 1) {
      logJson(WORKER_NAME, "scheduler_due_slots_coalesced", {
        scheduler_name: SCHEDULER_NAME,
        scheduler_run_id: context.scheduler_run_id ?? null,
        job_key: job.job_key,
        coalesced_due_slot_count: allDueTimes.length,
        due_at: latestDueAt,
        earliest_due_at: allDueTimes[0],
        selected_latest_due_at: latestDueAt,
        older_slots_coalesced: allDueTimes.length - 1,
      });
    }

    for (const dueAt of dueTimes) {
      const claimedAt = nowIso(Date.now());
      const dispatchId = await store.claimDispatch({
        job_key: job.job_key,
        due_at: dueAt,
        claimed_at: claimedAt,
        target_type: job.target_type,
        dry_run: job.dry_run,
      });

      if (!dispatchId) {
        logJson(WORKER_NAME, "scheduler_dispatch_duplicate", {
          scheduler_name: SCHEDULER_NAME,
          scheduler_run_id: context.scheduler_run_id ?? null,
          job_key: job.job_key,
          target_type: job.target_type,
          due_at: dueAt,
          dry_run: job.dry_run === 1,
          dispatch_status: "skipped",
          reason: "duplicate_claim",
        });
        continue;
      }

      summary.jobs_claimed += 1;

      if (job.dry_run === 1) {
        await store.updateDispatch(dispatchId, {
          dispatched_at: nowIso(Date.now()),
          dispatch_status: "dry_run",
          reason: "dry_run_enabled",
          response_status: null,
          response_preview: null,
        });
        logJson(WORKER_NAME, "scheduler_dispatch_dry_run", {
          scheduler_name: SCHEDULER_NAME,
          scheduler_run_id: context.scheduler_run_id ?? null,
          job_key: job.job_key,
          target_type: job.target_type,
          due_at: dueAt,
          dry_run: true,
          dispatch_status: "dry_run",
          reason: "dry_run_enabled",
        });
        continue;
      }

      claimedLiveDispatches.push({
        dispatch_id: dispatchId,
        due_at: dueAt,
        job,
        claimed_at: claimedAt,
      });
    }
  }

  const dispatchResults = await Promise.allSettled(
    claimedLiveDispatches.map((dispatch) => dispatchClaimedLiveJob(store, dispatch, env, context)),
  );

  for (let index = 0; index < dispatchResults.length; index += 1) {
    const dispatchResult = dispatchResults[index];
    if (dispatchResult.status === "fulfilled") {
      summary.jobs_dispatched += dispatchResult.value.jobs_dispatched;
      summary.jobs_failed += dispatchResult.value.jobs_failed;
      continue;
    }

    summary.jobs_failed += 1;
    const dispatch = claimedLiveDispatches[index];
    const message = dispatchResult.reason instanceof Error
      ? dispatchResult.reason.message
      : String(dispatchResult.reason);
    logJson(WORKER_NAME, "scheduler_dispatch_failed", {
      scheduler_name: SCHEDULER_NAME,
      scheduler_run_id: context.scheduler_run_id ?? null,
      job_key: dispatch.job.job_key,
      target_type: dispatch.job.target_type,
      due_at: dispatch.due_at,
      dry_run: false,
      dispatch_status: "failed",
      reason: message,
    });
  }

  return summary;
}

export async function runScheduler(
  store,
  env = {},
  nowMs = Date.now(),
  schedulerName = SCHEDULER_NAME,
  triggerSource = TRIGGER_SOURCE_CLOUDFLARE_CRON,
) {
  const startedAtMs = Number(nowMs);
  if (!Number.isFinite(startedAtMs)) {
    throw new Error("Invalid scheduler invocation time");
  }

  const startedAt = nowIso(startedAtMs);
  const minuteSlot = canonicalMinuteSlot(startedAtMs);
  const minuteSlotMs = Date.parse(minuteSlot);
  const normalizedTriggerSource = normalizeTriggerSource(triggerSource);
  const previousRun = await store.getLatestFinishedRun(schedulerName, minuteSlot);
  const previousRunStartedAt = previousRun?.started_at ? trimText(previousRun.started_at) : null;
  const previousEvaluationWindowEnd = previousRun?.evaluation_window_end
    ? trimText(previousRun.evaluation_window_end)
    : null;
  const previousEvaluationWindowEndMs = previousEvaluationWindowEnd
    ? Date.parse(previousEvaluationWindowEnd)
    : null;
  if (previousEvaluationWindowEnd && !Number.isFinite(previousEvaluationWindowEndMs)) {
    throw new Error(`Invalid previous scheduler evaluation window: ${previousEvaluationWindowEnd}`);
  }
  const evaluationWindowStartMs = Number.isFinite(previousEvaluationWindowEndMs)
    ? previousEvaluationWindowEndMs
    : minuteSlotMs - DEFAULT_LOOKBACK_MINUTES * MINUTE_MS;
  const evaluationWindowStart = nowIso(evaluationWindowStartMs);

  const minuteClaim = await store.claimMinute({
    scheduler_name: schedulerName,
    minute_slot: minuteSlot,
    trigger_source: normalizedTriggerSource,
    started_at: startedAt,
    status: "started",
    previous_run_started_at: previousRunStartedAt,
    evaluation_window_start: evaluationWindowStart,
    evaluation_window_end: minuteSlot,
    jobs_checked: 0,
    jobs_due: 0,
    jobs_claimed: 0,
    jobs_dispatched: 0,
    jobs_failed: 0,
    error_message: null,
  });
  if (!minuteClaim.claimed) {
    logJson(WORKER_NAME, "scheduler_run_already_claimed", {
      scheduler_name: schedulerName,
      scheduler_run_id: minuteClaim.scheduler_run_id,
      minute_slot: minuteSlot,
      trigger_source: normalizedTriggerSource,
      claimed_trigger_source: minuteClaim.trigger_source,
      run_status: minuteClaim.run_status,
    });
    return {
      scheduler_run_id: minuteClaim.scheduler_run_id,
      scheduler_name: schedulerName,
      minute_slot: minuteSlot,
      trigger_source: normalizedTriggerSource,
      claimed_trigger_source: minuteClaim.trigger_source,
      run_status: minuteClaim.run_status,
      status: "already_claimed",
    };
  }

  const runId = minuteClaim.scheduler_run_id;
  if (runId === null || runId === undefined) {
    throw new Error("Failed to claim scheduler minute");
  }

  logJson(WORKER_NAME, "scheduler_run_started", {
    scheduler_name: schedulerName,
    scheduler_run_id: runId,
    minute_slot: minuteSlot,
    trigger_source: normalizedTriggerSource,
    previous_run_started_at: previousRunStartedAt,
    evaluation_window_start: evaluationWindowStart,
    evaluation_window_end: minuteSlot,
  });

  try {
    await reconcileBlondonNodesDispatches(store, env, startedAtMs, { scheduler_run_id: runId });
    const jobs = await store.listEnabledJobs();
    const summary = await dispatchDueJobsForWindow(
      store,
      jobs,
      env,
      evaluationWindowStartMs,
      minuteSlotMs,
      { scheduler_run_id: runId },
    );

    await store.finishRun(runId, {
      status: "finished",
      finished_at: nowIso(Date.now()),
      previous_run_started_at: previousRunStartedAt,
      evaluation_window_start: evaluationWindowStart,
      evaluation_window_end: minuteSlot,
      jobs_checked: summary.jobs_checked,
      jobs_due: summary.jobs_due,
      jobs_claimed: summary.jobs_claimed,
      jobs_dispatched: summary.jobs_dispatched,
      jobs_failed: summary.jobs_failed,
      error_message: null,
    });

    logJson(WORKER_NAME, "scheduler_run_finished", {
      scheduler_name: schedulerName,
      scheduler_run_id: runId,
      status: "finished",
      minute_slot: minuteSlot,
      trigger_source: normalizedTriggerSource,
      previous_run_started_at: previousRunStartedAt,
      evaluation_window_start: evaluationWindowStart,
      evaluation_window_end: minuteSlot,
      jobs_checked: summary.jobs_checked,
      jobs_due: summary.jobs_due,
      jobs_claimed: summary.jobs_claimed,
      jobs_dispatched: summary.jobs_dispatched,
      jobs_failed: summary.jobs_failed,
    });

    return {
      scheduler_run_id: runId,
      scheduler_name: schedulerName,
      started_at: startedAt,
      minute_slot: minuteSlot,
      trigger_source: normalizedTriggerSource,
      previous_run_started_at: previousRunStartedAt,
      evaluation_window_start: evaluationWindowStart,
      evaluation_window_end: minuteSlot,
      ...summary,
      run_status: "finished",
      status: "triggered",
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    try {
      await store.finishRun(runId, {
        status: "failed",
        finished_at: nowIso(Date.now()),
        previous_run_started_at: previousRunStartedAt,
        evaluation_window_start: evaluationWindowStart,
        evaluation_window_end: minuteSlot,
        jobs_checked: 0,
        jobs_due: 0,
        jobs_claimed: 0,
        jobs_dispatched: 0,
        jobs_failed: 0,
        error_message: truncatePreview(message, RESPONSE_PREVIEW_LIMIT),
      });
    } catch {
      // Best effort: the run row may already be unavailable if the store failed.
    }

    logJson(WORKER_NAME, "scheduler_run_failed", {
      scheduler_name: schedulerName,
      scheduler_run_id: runId,
      status: "failed",
      minute_slot: minuteSlot,
      trigger_source: normalizedTriggerSource,
      previous_run_started_at: previousRunStartedAt,
      evaluation_window_start: evaluationWindowStart,
      evaluation_window_end: minuteSlot,
      reason: message,
    });

    throw error;
  }
}

function getSchedulerStoreFromEnv(env) {
  return createSchedulerStore(env?.[D1_BINDING_NAME]);
}

export default {
  async scheduled(controller, env, ctx) {
    const scheduledTime = Number(controller?.scheduledTime ?? Date.now());
    ctx.waitUntil(
      runScheduler(
        getSchedulerStoreFromEnv(env),
        env,
        scheduledTime,
        SCHEDULER_NAME,
        TRIGGER_SOURCE_CLOUDFLARE_CRON,
      ),
    );
  },

  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/run-if-due") {
      if (request.method !== "POST") {
        return new Response("Method not allowed", { status: 405 });
      }
      if (!await hasValidSchedulerTriggerSecret(request, env)) {
        return jsonResponse({ ok: false, error: "unauthorized" }, 401);
      }
      const invokedAtMs = Date.now();
      try {
        const result = await runScheduler(
          getSchedulerStoreFromEnv(env),
          env,
          invokedAtMs,
          SCHEDULER_NAME,
          TRIGGER_SOURCE_EXTERNAL_WATCHDOG,
        );
        return jsonResponse({ ok: true, ...result });
      } catch {
        return jsonResponse({
          ok: false,
          status: "failed",
          scheduler_name: SCHEDULER_NAME,
          trigger_source: TRIGGER_SOURCE_EXTERNAL_WATCHDOG,
          minute_slot: canonicalMinuteSlot(invokedAtMs),
        }, 500);
      }
    }
    if (url.pathname !== "/" && url.pathname !== "/health") {
      return new Response("Not found", { status: 404 });
    }

    return jsonResponse({
      ok: true,
      worker: WORKER_NAME,
      scheduler_name: SCHEDULER_NAME,
      binding: D1_BINDING_NAME,
    });
  },
};
