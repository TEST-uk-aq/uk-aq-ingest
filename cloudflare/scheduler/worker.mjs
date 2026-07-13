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
    async getPreviousRun(schedulerName, startedAtIso) {
      return dbFirst(
        schedulerDb,
        `
          select id, scheduler_name, started_at
          from scheduler_runs
          where scheduler_name = ? and started_at < ?
          order by started_at desc
          limit 1
        `,
        [schedulerName, startedAtIso],
      );
    },

    async insertRun(run) {
      const result = await dbRun(
        schedulerDb,
        `
          insert into scheduler_runs (
            scheduler_name,
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
          ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          run.scheduler_name,
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
      return Number(result?.meta?.last_row_id ?? 0) || null;
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
      await dbRun(
        schedulerDb,
        `
          update scheduler_dispatches
          set dispatched_at = ?,
              dispatch_status = ?,
              reason = ?,
              response_status = ?,
              response_preview = ?
          where id = ?
        `,
        [
          patch.dispatched_at ?? null,
          patch.dispatch_status,
          patch.reason ?? null,
          patch.response_status ?? null,
          patch.response_preview ?? null,
          dispatchId,
        ],
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

async function dispatchClaimedLiveJob(store, job, dispatchId, dueAt, env, context) {
  logJson(WORKER_NAME, "scheduler_dispatch_attempt", formatLogJobPayload(job, dueAt));

  let result;
  let dispatchError = null;
  try {
    result = await dispatchTarget(job, env);
  } catch (error) {
    dispatchError = error;
    const message = error instanceof Error ? error.message : String(error);
    result = {
      ok: false,
      response_status: null,
      response_preview: null,
      reason: message,
    };
  }

  const dispatchStatus = result.ok ? "dispatched" : "failed";
  await store.updateDispatch(dispatchId, {
    dispatched_at: nowIso(Date.now()),
    dispatch_status: dispatchStatus,
    reason: result.ok ? null : result.reason,
    response_status: result.response_status ?? null,
    response_preview: result.response_preview ?? null,
  });

  if (dispatchError) {
    logJson(WORKER_NAME, "scheduler_dispatch_failed", {
      scheduler_name: SCHEDULER_NAME,
      scheduler_run_id: context.scheduler_run_id ?? null,
      job_key: job.job_key,
      target_type: job.target_type,
      due_at: dueAt,
      dry_run: false,
      dispatch_status: "failed",
      reason: result.reason,
    });
  } else {
    logJson(WORKER_NAME, "scheduler_dispatch_result", {
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
  }

  return {
    jobs_dispatched: result.ok ? 1 : 0,
    jobs_failed: result.ok ? 0 : 1,
  };
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

    let dueTimes;
    try {
      dueTimes = dueSlotsForJob(job, windowStartMs, windowEndMs);
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

    summary.jobs_due += dueTimes.length;

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
      });
    }
  }

  const dispatchResults = await Promise.allSettled(
    claimedLiveDispatches.map((dispatch) => dispatchClaimedLiveJob(
      store,
      dispatch.job,
      dispatch.dispatch_id,
      dispatch.due_at,
      env,
      context,
    )),
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

export async function runScheduler(store, env = {}, nowMs = Date.now(), schedulerName = SCHEDULER_NAME) {
  const startedAtMs = Number(nowMs);
  if (!Number.isFinite(startedAtMs)) {
    throw new Error("Invalid scheduler invocation time");
  }

  const startedAt = nowIso(startedAtMs);
  const previousRun = await store.getPreviousRun(schedulerName, startedAt);
  const previousRunStartedAt = previousRun?.started_at ? trimText(previousRun.started_at) : null;
  const previousRunStartedAtMs = previousRunStartedAt ? Date.parse(previousRunStartedAt) : null;
  if (previousRunStartedAt && !Number.isFinite(previousRunStartedAtMs)) {
    throw new Error(`Invalid previous scheduler run timestamp: ${previousRunStartedAt}`);
  }
  const evaluationWindowStartMs = Number.isFinite(previousRunStartedAtMs)
    ? previousRunStartedAtMs
    : startedAtMs - DEFAULT_LOOKBACK_MINUTES * MINUTE_MS;
  const evaluationWindowStart = nowIso(evaluationWindowStartMs);

  const runId = await store.insertRun({
    scheduler_name: schedulerName,
    started_at: startedAt,
    status: "started",
    previous_run_started_at: previousRunStartedAt,
    evaluation_window_start: evaluationWindowStart,
    evaluation_window_end: startedAt,
    jobs_checked: 0,
    jobs_due: 0,
    jobs_claimed: 0,
    jobs_dispatched: 0,
    jobs_failed: 0,
    error_message: null,
  });
  if (runId === null || runId === undefined) {
    throw new Error("Failed to insert scheduler run row");
  }

  logJson(WORKER_NAME, "scheduler_run_started", {
    scheduler_name: schedulerName,
    scheduler_run_id: runId,
    previous_run_started_at: previousRunStartedAt,
    evaluation_window_start: evaluationWindowStart,
    evaluation_window_end: startedAt,
  });

  try {
    const jobs = await store.listEnabledJobs();
    const summary = await dispatchDueJobsForWindow(
      store,
      jobs,
      env,
      evaluationWindowStartMs,
      startedAtMs,
      { scheduler_run_id: runId },
    );

    await store.finishRun(runId, {
      status: "finished",
      finished_at: nowIso(Date.now()),
      previous_run_started_at: previousRunStartedAt,
      evaluation_window_start: evaluationWindowStart,
      evaluation_window_end: startedAt,
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
      previous_run_started_at: previousRunStartedAt,
      evaluation_window_start: evaluationWindowStart,
      evaluation_window_end: startedAt,
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
      previous_run_started_at: previousRunStartedAt,
      evaluation_window_start: evaluationWindowStart,
      evaluation_window_end: startedAt,
      ...summary,
      status: "finished",
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    try {
      await store.finishRun(runId, {
        status: "failed",
        finished_at: nowIso(Date.now()),
        previous_run_started_at: previousRunStartedAt,
        evaluation_window_start: evaluationWindowStart,
        evaluation_window_end: startedAt,
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
      previous_run_started_at: previousRunStartedAt,
      evaluation_window_start: evaluationWindowStart,
      evaluation_window_end: startedAt,
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
    ctx.waitUntil(runScheduler(getSchedulerStoreFromEnv(env), env, scheduledTime));
  },

  async fetch(request) {
    const url = new URL(request.url);
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
