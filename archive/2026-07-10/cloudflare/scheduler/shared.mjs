const MS_PER_SECOND = 1000;
const MS_PER_MINUTE = 60 * MS_PER_SECOND;

function isRecord(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function normalizeBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

export async function readSecret(value) {
  if (typeof value === "string") {
    return value;
  }
  if (value && typeof value === "object") {
    const record = value;
    if (typeof record.get === "function") {
      const resolved = await record.get();
      return typeof resolved === "string" ? resolved : String(resolved ?? "");
    }
    if (typeof record.then === "function") {
      const resolved = await value;
      return typeof resolved === "string" ? resolved : String(resolved ?? "");
    }
  }
  return value ? String(value) : "";
}

export function nowIso(nowMs = Date.now()) {
  return new Date(nowMs).toISOString();
}

export function currentUtcDate(nowMs = Date.now()) {
  return new Date(nowMs).toISOString().slice(0, 10);
}

export function shiftUtcDate(isoDate, dayDelta) {
  const date = new Date(`${isoDate}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + dayDelta);
  return date.toISOString().slice(0, 10);
}

export function combineUtcDateAndTime(isoDate, hhmm) {
  const match = String(hhmm || "").trim().match(/^(\d{2}):(\d{2})$/);
  if (!match) {
    return null;
  }
  const [, hourText, minuteText] = match;
  const hour = Number(hourText);
  const minute = Number(minuteText);
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) {
    return null;
  }
  const date = new Date(`${isoDate}T00:00:00.000Z`);
  date.setUTCHours(hour, minute, 0, 0);
  return date.getTime();
}

export function parseTimestamp(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  if (value instanceof Date) {
    const time = value.getTime();
    return Number.isNaN(time) ? null : time;
  }
  const text = String(value).trim();
  if (!text) {
    return null;
  }
  const parsed = Date.parse(text);
  return Number.isNaN(parsed) ? null : parsed;
}

export function minutesBetween(earlierMs, laterMs) {
  if (earlierMs === null || earlierMs === undefined) {
    return null;
  }
  return Math.max(0, Math.floor((laterMs - earlierMs) / MS_PER_MINUTE));
}

export function logJson(workerName, event, payload = {}) {
  console.log(JSON.stringify({
    worker: workerName,
    event,
    timestamp: nowIso(),
    ...payload,
  }));
}

export function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

async function readResponseText(response, limit = 1200) {
  const text = await response.text();
  return text.length <= limit ? text : `${text.slice(0, Math.max(0, limit - 3))}...`;
}

export async function fetchPostgrestRows({
  baseUrl,
  schema,
  table,
  secretKey,
  select,
  filters = {},
  order,
  limit = 10,
}) {
  const normalizedBaseUrl = normalizeBaseUrl(baseUrl);
  if (!normalizedBaseUrl) {
    throw new Error(`Missing PostgREST base URL for ${table}`);
  }
  if (!String(secretKey || "").trim()) {
    throw new Error(`Missing PostgREST secret key for ${table}`);
  }

  const url = new URL(`${normalizedBaseUrl}/rest/v1/${table}`);
  url.searchParams.set("select", select);
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, value);
    }
  }
  if (order) {
    url.searchParams.set("order", order);
  }
  if (limit !== undefined && limit !== null) {
    url.searchParams.set("limit", String(limit));
  }

  const response = await fetch(url, {
    method: "GET",
    headers: {
      apikey: secretKey,
      Authorization: `Bearer ${secretKey}`,
      "Accept-Profile": schema,
    },
  });

  if (!response.ok) {
    const body = await readResponseText(response);
    throw new Error(`PostgREST ${table} query failed (${response.status}): ${body}`);
  }

  const text = await response.text();
  if (!text.trim()) {
    return [];
  }
  const parsed = JSON.parse(text);
  if (!Array.isArray(parsed)) {
    throw new Error(`PostgREST ${table} query did not return an array`);
  }
  return parsed.filter(isRecord);
}

function latestRunState(rows, startedAtFieldNames, successStatusMatch, inFlightPredicate) {
  let latestStartedAt = null;
  let latestSuccessAt = null;
  let latestInFlightAt = null;
  let latestRow = null;

  for (const row of rows) {
    if (!latestRow) {
      latestRow = row;
    }

    const startedAt = startedAtFieldNames
      .map((field) => parseTimestamp(row[field]))
      .find((value) => value !== null);
    if (startedAt !== undefined && startedAt !== null) {
      latestStartedAt = latestStartedAt === null ? startedAt : Math.max(latestStartedAt, startedAt);
    }

    if (successStatusMatch(row)) {
      const successAt = parseTimestamp(row.run_ended_at || row.finished_at || row.finished_or_failed_at || row.updated_at || row.created_at);
      if (successAt !== null) {
        latestSuccessAt = latestSuccessAt === null ? successAt : Math.max(latestSuccessAt, successAt);
      }
    }

    if (inFlightPredicate(row)) {
      const inFlightAt = startedAt;
      if (inFlightAt !== null) {
        latestInFlightAt = latestInFlightAt === null ? inFlightAt : Math.max(latestInFlightAt, inFlightAt);
      }
    }
  }

  return {
    latestRow,
    latestStartedAt,
    latestSuccessAt,
    latestInFlightAt,
  };
}

export function evaluateIngestJob(job, rows, nowMs = Date.now()) {
  const enabled = job.enabled !== false;
  const state = latestRunState(
    rows,
    ["run_started_at", "created_at"],
    (row) => String(row.run_status || "").trim().toLowerCase().includes("succeed"),
    (row) => !String(row.run_ended_at || "").trim() && !String(row.run_status || "").trim().toLowerCase().includes("failed"),
  );

  const lastStartedAt = state.latestStartedAt;
  const lastSuccessAt = state.latestSuccessAt;
  const latestActivityAt = Math.max(lastStartedAt || 0, lastSuccessAt || 0);
  const currentInFlight = Boolean(
    state.latestRow
    && !String(state.latestRow.run_ended_at || "").trim()
    && !String(state.latestRow.run_status || "").trim().toLowerCase().includes("failed")
    && (state.latestInFlightAt !== null),
  );
  const currentInFlightAgeMinutes = currentInFlight && state.latestInFlightAt !== null
    ? minutesBetween(state.latestInFlightAt, nowMs)
    : null;

  if (!enabled) {
    return {
      due: false,
      reason: "disabled",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt,
      lastSuccessAt,
      latestActivityAt: latestActivityAt || null,
      wouldTrigger: false,
    };
  }

  if (currentInFlight && currentInFlightAgeMinutes !== null && currentInFlightAgeMinutes < job.stale_after_minutes) {
    return {
      due: false,
      reason: "run_in_progress",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt,
      lastSuccessAt,
      latestActivityAt: latestActivityAt || null,
      wouldTrigger: false,
    };
  }

  if (lastSuccessAt !== null && minutesBetween(lastSuccessAt, nowMs) < job.interval_minutes) {
    return {
      due: false,
      reason: "recent_success",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt,
      lastSuccessAt,
      latestActivityAt: latestActivityAt || null,
      wouldTrigger: false,
    };
  }

  if (lastStartedAt !== null && minutesBetween(lastStartedAt, nowMs) < job.min_gap_minutes) {
    return {
      due: false,
      reason: "recent_start",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt,
      lastSuccessAt,
      latestActivityAt: latestActivityAt || null,
      wouldTrigger: false,
    };
  }

  return {
    due: true,
    reason: "due",
    state,
    currentInFlight,
    currentInFlightAgeMinutes,
    lastStartedAt,
    lastSuccessAt,
    latestActivityAt: latestActivityAt || null,
    wouldTrigger: true,
  };
}

export function evaluateDailyTaskJob(job, rows, nowMs = Date.now()) {
  const enabled = job.enabled !== false;
  const state = latestRunState(
    rows,
    ["started_at", "scheduled_at_utc", "scheduled_or_started_at"],
    (row) => {
      const status = String(row.raw_status || row.effective_status || "").trim().toLowerCase();
      return status === "finished" || status === "succeeded" || String(row.finished_at || "").trim() !== "";
    },
    (row) => {
      const status = String(row.raw_status || row.effective_status || "").trim().toLowerCase();
      return status === "started" || (String(row.started_at || "").trim() !== "" && String(row.finished_at || "").trim() === "" && String(row.failed_at || "").trim() === "");
    },
  );

  const todayIso = currentUtcDate(nowMs);
  const scheduledAtMs = combineUtcDateAndTime(todayIso, job.scheduled_time_utc);
  if (scheduledAtMs === null) {
    return {
      due: false,
      reason: "invalid_schedule",
      state,
      currentInFlight: false,
      currentInFlightAgeMinutes: null,
      lastStartedAt: state.latestStartedAt,
      lastSuccessAt: state.latestSuccessAt,
      latestActivityAt: Math.max(state.latestStartedAt || 0, state.latestSuccessAt || 0) || null,
      wouldTrigger: false,
    };
  }

  const dueAtMs = scheduledAtMs + (job.due_after_minutes * MS_PER_MINUTE);
  const latestActivityAt = Math.max(state.latestStartedAt || 0, state.latestSuccessAt || 0);
  const currentInFlight = Boolean(
    state.latestRow
    && String(state.latestRow.started_at || "").trim()
    && String(state.latestRow.finished_at || "").trim() === ""
    && String(state.latestRow.failed_at || "").trim() === "",
  );
  const currentInFlightAgeMinutes = currentInFlight && state.latestStartedAt !== null
    ? minutesBetween(state.latestStartedAt, nowMs)
    : null;

  if (!enabled) {
    return {
      due: false,
      reason: "disabled",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt: state.latestStartedAt,
      lastSuccessAt: state.latestSuccessAt,
      latestActivityAt: latestActivityAt || null,
      scheduledAtMs,
      dueAtMs,
      wouldTrigger: false,
    };
  }

  if (nowMs < dueAtMs) {
    return {
      due: false,
      reason: "not_due_yet",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt: state.latestStartedAt,
      lastSuccessAt: state.latestSuccessAt,
      latestActivityAt: latestActivityAt || null,
      scheduledAtMs,
      dueAtMs,
      wouldTrigger: false,
    };
  }

  if (currentInFlight && currentInFlightAgeMinutes !== null && currentInFlightAgeMinutes < job.stale_after_minutes) {
    return {
      due: false,
      reason: "run_in_progress",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt: state.latestStartedAt,
      lastSuccessAt: state.latestSuccessAt,
      latestActivityAt: latestActivityAt || null,
      scheduledAtMs,
      dueAtMs,
      wouldTrigger: false,
    };
  }

  if (state.latestSuccessAt !== null && state.latestSuccessAt >= scheduledAtMs) {
    return {
      due: false,
      reason: "recent_success",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt: state.latestStartedAt,
      lastSuccessAt: state.latestSuccessAt,
      latestActivityAt: latestActivityAt || null,
      scheduledAtMs,
      dueAtMs,
      wouldTrigger: false,
    };
  }

  if (state.latestStartedAt !== null && minutesBetween(state.latestStartedAt, nowMs) < job.min_gap_minutes) {
    return {
      due: false,
      reason: "recent_start",
      state,
      currentInFlight,
      currentInFlightAgeMinutes,
      lastStartedAt: state.latestStartedAt,
      lastSuccessAt: state.latestSuccessAt,
      latestActivityAt: latestActivityAt || null,
      scheduledAtMs,
      dueAtMs,
      wouldTrigger: false,
    };
  }

  return {
    due: true,
    reason: "due",
    state,
    currentInFlight,
    currentInFlightAgeMinutes,
    lastStartedAt: state.latestStartedAt,
    lastSuccessAt: state.latestSuccessAt,
    latestActivityAt: latestActivityAt || null,
    scheduledAtMs,
    dueAtMs,
    wouldTrigger: true,
  };
}

export function summarizeDecision(job, decision, rows, nowMs = Date.now()) {
  return {
    job_key: job.job_key,
    label: job.label,
    target_label: job.target_label,
    state_source: job.state_source,
    enabled: job.enabled !== false,
    dry_run: true,
    cron: job.cron,
    due: decision.due,
    reason: decision.reason,
    would_trigger: decision.wouldTrigger,
    rows_examined: rows.length,
    last_started_at: decision.lastStartedAt !== null ? nowIso(decision.lastStartedAt) : null,
    last_success_at: decision.lastSuccessAt !== null ? nowIso(decision.lastSuccessAt) : null,
    latest_activity_at: decision.latestActivityAt !== null ? nowIso(decision.latestActivityAt) : null,
    in_flight: decision.currentInFlight,
    in_flight_age_minutes: decision.currentInFlightAgeMinutes,
    now_utc: nowIso(nowMs),
    ...(decision.scheduledAtMs ? { scheduled_at_utc: nowIso(decision.scheduledAtMs) } : {}),
    ...(decision.dueAtMs ? { due_at_utc: nowIso(decision.dueAtMs) } : {}),
  };
}
