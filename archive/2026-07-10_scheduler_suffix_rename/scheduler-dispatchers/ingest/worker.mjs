import {
  evaluateIngestJob,
  fetchPostgrestRows,
  jsonResponse,
  logJson,
  normalizeBaseUrl,
  nowIso,
  readSecret,
  summarizeDecision,
} from "../shared.mjs";

const WORKER_NAME = "uk-aq-scheduler-ingest";
const INGEST_SCHEMA = "uk_aq_core";
const INGEST_TABLE = "uk_aq_ingest_runs";
const INGEST_SELECT = "connector_code,run_started_at,run_ended_at,run_status,run_message,created_at,response_status";
const DEFAULT_LIMIT = 10;

export const INGEST_JOBS = [
  {
    job_key: "uk_aq_blondon_communities",
    label: "Breathe London Communities ingest",
    target_label: "uk-aq-blondon-communities-ingest",
    state_source: "ingest_runs",
    connector_code: "blondon_communities",
    cron: "*/15 * * * *",
    interval_minutes: 15,
    min_gap_minutes: 10,
    stale_after_minutes: 45,
    enabled: true,
  },
  {
    job_key: "uk_aq_blondon_nodes",
    label: "Breathe London Nodes ingest",
    target_label: "uk-aq-blondon-nodes-ingest",
    state_source: "ingest_runs",
    connector_code: "blondon_nodes",
    cron: "*/15 * * * *",
    interval_minutes: 15,
    min_gap_minutes: 10,
    stale_after_minutes: 45,
    enabled: true,
  },
  {
    job_key: "uk_aq_scomm",
    label: "Sensor.Community ingest",
    target_label: "uk-aq-scomm-ingest",
    state_source: "ingest_runs",
    connector_code: "sensorcommunity",
    cron: "*/15 * * * *",
    interval_minutes: 15,
    min_gap_minutes: 10,
    stale_after_minutes: 45,
    enabled: true,
  },
  {
    job_key: "uk_aq_sos",
    label: "UK-AIR SOS ingest",
    target_label: "uk-aq-sos-ingest",
    state_source: "ingest_runs",
    connector_code: "sos",
    cron: "*/15 * * * *",
    interval_minutes: 15,
    min_gap_minutes: 10,
    stale_after_minutes: 45,
    enabled: true,
  },
  {
    job_key: "uk_aq_openaq_safety",
    label: "OpenAQ safety trigger",
    target_label: "uk-aq-openaq-ingest",
    state_source: "ingest_runs",
    connector_code: "openaq",
    cron: "*/15 * * * *",
    interval_minutes: 30,
    min_gap_minutes: 15,
    stale_after_minutes: 90,
    enabled: true,
    safety_only: true,
  },
];

async function loadRowsForJob(job, env) {
  const supabaseUrl = normalizeBaseUrl(await readSecret(env.SUPABASE_URL));
  const secretKey = await readSecret(env.SB_SECRET_KEY);
  return fetchPostgrestRows({
    baseUrl: supabaseUrl,
    schema: INGEST_SCHEMA,
    table: INGEST_TABLE,
    secretKey,
    select: INGEST_SELECT,
    filters: {
      connector_code: `eq.${job.connector_code}`,
    },
    order: "run_started_at.desc.nullslast,created_at.desc",
    limit: DEFAULT_LIMIT,
  });
}

async function evaluateJob(job, env, nowMs) {
  try {
    const rows = await loadRowsForJob(job, env);
    const decision = evaluateIngestJob(job, rows, nowMs);
    const summary = summarizeDecision(job, decision, rows, nowMs);
    logJson(WORKER_NAME, "dry_run_decision", summary);
    return { ok: true, ...summary };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const failure = {
      job_key: job.job_key,
      label: job.label,
      target_label: job.target_label,
      state_source: job.state_source,
      enabled: job.enabled !== false,
      dry_run: true,
      cron: job.cron,
      due: false,
      reason: "state_load_failed",
      would_trigger: false,
      error: message,
      now_utc: nowIso(nowMs),
    };
    logJson(WORKER_NAME, "dry_run_error", failure);
    return { ok: false, ...failure };
  }
}

async function runDispatcher(env, nowMs = Date.now()) {
  const results = [];
  for (const job of INGEST_JOBS) {
    results.push(await evaluateJob(job, env, nowMs));
  }

  logJson(WORKER_NAME, "dry_run_summary", {
    job_count: results.length,
    due_count: results.filter((result) => result.due).length,
    would_trigger_count: results.filter((result) => result.would_trigger).length,
    failed_count: results.filter((result) => !result.ok).length,
    skipped_count: results.filter((result) => !result.due && result.ok).length,
    dry_run: true,
  });

  return results;
}

export default {
  async scheduled(_controller, env, ctx) {
    ctx.waitUntil(runDispatcher(env));
  },

  async fetch() {
    return jsonResponse({
      ok: true,
      worker: WORKER_NAME,
      dry_run: true,
      scheduled_cron: "*/15 * * * *",
      jobs: INGEST_JOBS.map((job) => ({
        job_key: job.job_key,
        target_label: job.target_label,
        cron: job.cron,
        enabled: job.enabled !== false,
      })),
    }, 200);
  },
};

export { runDispatcher };
