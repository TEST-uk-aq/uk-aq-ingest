import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  cronDueTimes,
  dispatchDueJobsForWindow,
  dueSlotsForJob,
  runScheduler,
} from "../cloudflare/scheduler/worker.mjs";

const wranglerText = readFileSync("cloudflare/scheduler/wrangler.toml", "utf8");
const workerText = readFileSync("cloudflare/scheduler/worker.mjs", "utf8");
const migrationText = readFileSync("cloudflare/scheduler/migrations/0001_scheduler_schema.sql", "utf8");

function parseWranglerCronList() {
  const match = wranglerText.match(/crons\s*=\s*\[([\s\S]*?)\]/);
  assert.ok(match, "wrangler.toml must contain [triggers].crons");
  return match[1]
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const cronMatch = line.match(/^"([^"]+)"/);
      assert.ok(cronMatch, `invalid cron line: ${line}`);
      return cronMatch[1];
    });
}

function createMemorySchedulerStore({ jobs = [], previousRuns = [] } = {}) {
  const state = {
    jobs: jobs.map((job, index) => ({ id: index + 1, ...job })),
    previousRuns: previousRuns.map((run, index) => ({ id: index + 1, ...run })),
    runs: [],
    dispatches: [],
  };
  let nextRunId = state.previousRuns.length + 1;
  let nextDispatchId = 1;

  return {
    state,

    async getPreviousRun(schedulerName, startedAtIso) {
      const matches = state.previousRuns
        .concat(state.runs)
        .filter((run) => run.scheduler_name === schedulerName && run.started_at < startedAtIso)
        .sort((left, right) => left.started_at.localeCompare(right.started_at));
      return matches.at(-1) ?? null;
    },

    async insertRun(run) {
      const row = { id: nextRunId++, ...run };
      state.runs.push(row);
      return row.id;
    },

    async listEnabledJobs() {
      return state.jobs.filter((job) => Number(job.enabled) === 1).map((job) => ({ ...job }));
    },

    async claimDispatch(claim) {
      const duplicate = state.dispatches.find(
        (dispatch) => dispatch.job_key === claim.job_key && dispatch.due_at === claim.due_at,
      );
      if (duplicate) {
        return null;
      }

      const row = {
        id: nextDispatchId++,
        ...claim,
        dispatch_status: "claimed",
        dispatched_at: null,
        reason: null,
        response_status: null,
        response_preview: null,
      };
      state.dispatches.push(row);
      return row.id;
    },

    async updateDispatch(dispatchId, patch) {
      const row = state.dispatches.find((dispatch) => dispatch.id === dispatchId);
      assert.ok(row, `missing dispatch row ${dispatchId}`);
      Object.assign(row, patch);
    },

    async finishRun(runId, patch) {
      const row = state.runs.find((run) => run.id === runId);
      assert.ok(row, `missing run row ${runId}`);
      Object.assign(row, patch);
    },
  };
}

function getJob(template = {}) {
  return {
    job_key: "uk_aq_r2_core_snapshot",
    enabled: 1,
    target_type: "github_workflow",
    cron_expr: "15 4 * * *",
    timezone: "UTC",
    github_repo: "TEST-uk-aq/uk-aq-ops",
    github_workflow_file: "uk_aq_r2_core_snapshot.yml",
    github_ref: "main",
    github_inputs_json: "{}",
    cloud_run_url: null,
    cloud_run_method: "POST",
    cloud_run_headers_json: null,
    cloud_run_body_json: null,
    dry_run: 1,
    notes: "test",
    ...template,
  };
}

function captureLogs() {
  const original = console.log;
  const lines = [];
  console.log = (...args) => {
    lines.push(args.map((value) => String(value)).join(" "));
  };
  return {
    lines,
    restore() {
      console.log = original;
    },
  };
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function waitFor(condition, message) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (condition()) {
      return;
    }
    await Promise.resolve();
  }
  assert.fail(message);
}

function getCloudRunJob(template = {}) {
  return getJob({
    job_key: "uk_aq_cloud_run_test",
    target_type: "cloud_run",
    github_repo: null,
    github_workflow_file: null,
    github_ref: null,
    github_inputs_json: null,
    cloud_run_url: "https://example.invalid/run",
    cloud_run_method: "POST",
    cloud_run_headers_json: "{}",
    cloud_run_body_json: "{}",
    dry_run: 0,
    ...template,
  });
}

test("wrangler config uses exactly one minute cron and the ingest D1 binding", () => {
  const crons = parseWranglerCronList();
  assert.deepEqual(crons, ["* * * * *"]);
  assert.match(wranglerText, /name\s*=\s*"uk-aq-cron-scheduler-ingest"/);
  assert.match(wranglerText, /binding\s*=\s*"SCHEDULER_DB"/);
  assert.match(wranglerText, /database_name\s*=\s*"uk_aq_cron_scheduler_ingest_db"/);
});

test("worker loads jobs from D1 instead of a hard-coded production job array", () => {
  assert.match(workerText, /createSchedulerStore/);
  assert.match(workerText, /listEnabledJobs/);
  assert.doesNotMatch(workerText, /const\s+JOBS\s*=/);
  assert.doesNotMatch(workerText, /job_key:\s*"uk_aq_/);
  assert.doesNotMatch(
    workerText,
    /SUPABASE_URL|SB_SECRET_KEY|INGEST_JOBS|interval_minutes|min_gap_minutes|stale_after_minutes|evaluateIngestJob|fetchPostgrestRows|uk_aq_ingest_runs|PostgREST/i,
  );
  assert.match(workerText, /env\.UK_AQ_EDGE_UPSTREAM_SECRET/);
  assert.match(workerText, /headers\.set\("x-uk-aq-dispatch-secret", secret\)/);
  const retiredSecret = "UK_AQ_CLOUD_RUN_" + "DISPATCH_SECRET";
  assert.equal(workerText.includes(retiredSecret), false);
});

test("job due slots dispatch one minute before the configured cron", () => {
  assert.deepEqual(
    dueSlotsForJob(
      getJob({ cron_expr: "*/15 * * * *" }),
      Date.parse("2026-07-10T04:13:30Z"),
      Date.parse("2026-07-10T04:14:30Z"),
    ),
    ["2026-07-10T04:15:00.000Z"],
  );
  assert.deepEqual(
    dueSlotsForJob(
      getJob({ cron_expr: "*/30 * * * *" }),
      Date.parse("2026-07-10T04:28:30Z"),
      Date.parse("2026-07-10T04:29:30Z"),
    ),
    ["2026-07-10T04:30:00.000Z"],
  );
});

test("cron parser handles the supported due windows", () => {
  assert.deepEqual(
    cronDueTimes(
      "15 4 * * *",
      Date.parse("2026-07-10T04:14:30Z"),
      Date.parse("2026-07-10T04:15:30Z"),
    ),
    ["2026-07-10T04:15:00.000Z"],
  );

  assert.deepEqual(
    cronDueTimes(
      "15 4 * * *",
      Date.parse("2026-07-10T04:15:00Z"),
      Date.parse("2026-07-10T04:16:00Z"),
    ),
    [],
  );

  assert.deepEqual(
    cronDueTimes(
      "0 22 * * SUN",
      Date.parse("2026-07-11T21:59:30Z"),
      Date.parse("2026-07-12T22:00:30Z"),
    ),
    ["2026-07-12T22:00:00.000Z"],
  );

  assert.deepEqual(
    cronDueTimes(
      "15 4 * * *",
      Date.parse("2026-07-10T04:14:00Z"),
      Date.parse("2026-07-10T04:16:00Z"),
    ),
    ["2026-07-10T04:15:00.000Z"],
  );
});

test("startup lookback uses two minutes when there is no previous scheduler run", async () => {
  const store = createMemorySchedulerStore({
    jobs: [getJob()],
  });
  const logs = captureLogs();

  try {
    const result = await runScheduler(
      store,
      { UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "pat" },
      Date.parse("2026-07-10T04:15:30Z"),
    );

    assert.equal(result.previous_run_started_at, null);
    assert.equal(result.jobs_due, 1);
    assert.equal(store.state.dispatches.length, 1);
  } finally {
    logs.restore();
  }
});

test("first claim succeeds and an identical due slot is not dispatched twice", async () => {
  const store = createMemorySchedulerStore({
    jobs: [getJob({ dry_run: 0 })],
  });
  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(null, { status: 204 });
  };

  try {
    const windowStart = Date.parse("2026-07-10T04:13:30Z");
    const windowEnd = Date.parse("2026-07-10T04:14:30Z");

    const first = await dispatchDueJobsForWindow(
      store,
      await store.listEnabledJobs(),
      { UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "pat" },
      windowStart,
      windowEnd,
      { scheduler_run_id: 1 },
    );
    const second = await dispatchDueJobsForWindow(
      store,
      await store.listEnabledJobs(),
      { UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "pat" },
      windowStart,
      windowEnd,
      { scheduler_run_id: 2 },
    );

    assert.equal(first.jobs_claimed, 1);
    assert.equal(second.jobs_claimed, 0);
    assert.equal(fetchCalls, 1);
    assert.equal(store.state.dispatches.length, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("dry-run jobs do not make network requests", async () => {
  const store = createMemorySchedulerStore({
    jobs: [getJob({ dry_run: 1 })],
  });
  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(null, { status: 204 });
  };

  try {
    await runScheduler(store, { UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "pat" }, Date.parse("2026-07-10T04:15:30Z"));
    assert.equal(fetchCalls, 0);
    assert.equal(store.state.dispatches[0].dispatch_status, "dry_run");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("claimed Cloud Run jobs start concurrently and each dispatch row is updated once", async () => {
  const store = createMemorySchedulerStore({
    jobs: [
      getCloudRunJob({ job_key: "uk_aq_slow" }),
      getCloudRunJob({ job_key: "uk_aq_fast" }),
    ],
  });
  const responses = {
    "https://example.invalid/slow": createDeferred(),
    "https://example.invalid/fast": createDeferred(),
  };
  const fetchUrls = [];
  const updateDispatchCalls = [];
  const originalFetch = globalThis.fetch;
  const originalUpdateDispatch = store.updateDispatch.bind(store);
  store.updateDispatch = async (dispatchId, patch) => {
    updateDispatchCalls.push({ dispatchId, patch });
    await originalUpdateDispatch(dispatchId, patch);
  };
  globalThis.fetch = async (url) => {
    assert.equal(store.state.dispatches.length, 2, "all due jobs must be claimed before dispatch starts");
    const urlText = String(url);
    fetchUrls.push(urlText);
    return responses[urlText].promise;
  };

  try {
    const dispatchPromise = dispatchDueJobsForWindow(
      store,
      [
        getCloudRunJob({ job_key: "uk_aq_slow", cloud_run_url: "https://example.invalid/slow" }),
        getCloudRunJob({ job_key: "uk_aq_fast", cloud_run_url: "https://example.invalid/fast" }),
      ],
      { UK_AQ_EDGE_UPSTREAM_SECRET: "cloud-secret" },
      Date.parse("2026-07-10T04:13:30Z"),
      Date.parse("2026-07-10T04:14:30Z"),
      { scheduler_run_id: 1 },
    );

    await waitFor(
      () => fetchUrls.length === 2,
      "both Cloud Run dispatches should start before either response settles",
    );
    assert.deepEqual(fetchUrls, [
      "https://example.invalid/slow",
      "https://example.invalid/fast",
    ]);

    responses["https://example.invalid/fast"].resolve(new Response("fast", { status: 200 }));
    responses["https://example.invalid/slow"].resolve(new Response("slow", { status: 200 }));

    const summary = await dispatchPromise;
    assert.deepEqual(summary, {
      jobs_checked: 2,
      jobs_due: 2,
      jobs_claimed: 2,
      jobs_dispatched: 2,
      jobs_failed: 0,
    });
    assert.equal(updateDispatchCalls.length, 2);
    assert.deepEqual(
      updateDispatchCalls.map((call) => call.dispatchId).sort((left, right) => left - right),
      [1, 2],
    );
    assert.deepEqual(
      store.state.dispatches.map((dispatch) => dispatch.dispatch_status).sort(),
      ["dispatched", "dispatched"],
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("concurrent Cloud Run and GitHub dispatches retain independent outcomes and logs", async () => {
  const store = createMemorySchedulerStore({
    jobs: [
      getCloudRunJob({
        job_key: "uk_aq_cloud_run_failure",
        cloud_run_url: "https://example.invalid/failure",
      }),
      getJob({
        job_key: "uk_aq_github_success",
        dry_run: 0,
      }),
    ],
  });
  const logs = captureLogs();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    if (String(url) === "https://example.invalid/failure") {
      return new Response("service failure", { status: 503 });
    }
    return new Response(null, { status: 204 });
  };

  try {
    const summary = await dispatchDueJobsForWindow(
      store,
      await store.listEnabledJobs(),
      {
        UK_AQ_EDGE_UPSTREAM_SECRET: "cloud-secret",
        UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "github-pat",
      },
      Date.parse("2026-07-10T04:13:30Z"),
      Date.parse("2026-07-10T04:14:30Z"),
      { scheduler_run_id: 9 },
    );

    assert.deepEqual(summary, {
      jobs_checked: 2,
      jobs_due: 2,
      jobs_claimed: 2,
      jobs_dispatched: 1,
      jobs_failed: 1,
    });
    const dispatchesByJob = Object.fromEntries(
      store.state.dispatches.map((dispatch) => [dispatch.job_key, dispatch]),
    );
    assert.deepEqual(dispatchesByJob.uk_aq_cloud_run_failure, {
      id: 1,
      job_key: "uk_aq_cloud_run_failure",
      due_at: "2026-07-10T04:15:00.000Z",
      claimed_at: dispatchesByJob.uk_aq_cloud_run_failure.claimed_at,
      target_type: "cloud_run",
      dry_run: 0,
      dispatch_status: "failed",
      dispatched_at: dispatchesByJob.uk_aq_cloud_run_failure.dispatched_at,
      reason: "Cloud Run dispatch failed with HTTP 503",
      response_status: 503,
      response_preview: "service failure",
    });
    assert.equal(dispatchesByJob.uk_aq_github_success.dispatch_status, "dispatched");
    assert.equal(dispatchesByJob.uk_aq_github_success.response_status, 204);

    const events = logs.lines.map((line) => JSON.parse(line));
    const attempts = events.filter((event) => event.event === "scheduler_dispatch_attempt");
    const results = events.filter((event) => event.event === "scheduler_dispatch_result");
    assert.equal(attempts.length, 2);
    assert.equal(results.length, 2);
    assert.deepEqual(
      results.map((event) => ({
        job_key: event.job_key,
        target_type: event.target_type,
        due_at: event.due_at,
        dispatch_status: event.dispatch_status,
        response_status: event.response_status,
      })).sort((left, right) => left.job_key.localeCompare(right.job_key)),
      [
        {
          job_key: "uk_aq_cloud_run_failure",
          target_type: "cloud_run",
          due_at: "2026-07-10T04:15:00.000Z",
          dispatch_status: "failed",
          response_status: 503,
        },
        {
          job_key: "uk_aq_github_success",
          target_type: "github_workflow",
          due_at: "2026-07-10T04:15:00.000Z",
          dispatch_status: "dispatched",
          response_status: 204,
        },
      ],
    );
  } finally {
    logs.restore();
    globalThis.fetch = originalFetch;
  }
});

test("GitHub dispatch records success and sends workflow inputs", async () => {
  const store = createMemorySchedulerStore({
    jobs: [
      getJob({
        dry_run: 0,
        github_inputs_json: '{"force_prune_recheck":"true"}',
        cron_expr: "15 4 * * *",
      }),
    ],
  });
  const calls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    calls.push({ url: String(url), init });
    return new Response(null, { status: 204 });
  };

  try {
    const result = await runScheduler(store, { UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "pat" }, Date.parse("2026-07-10T04:15:30Z"));
    assert.equal(result.jobs_dispatched, 1);
    assert.equal(calls.length, 1);

	assert.equal(
	  calls[0].url,
	  "https://api.github.com/repos/TEST-uk-aq/uk-aq-ops/actions/workflows/uk_aq_r2_core_snapshot.yml/dispatches",
	);

    assert.deepEqual(JSON.parse(calls[0].init.body), {
      ref: "main",
      inputs: { force_prune_recheck: "true" },
    });
    assert.equal(store.state.dispatches[0].dispatch_status, "dispatched");
    assert.equal(store.state.dispatches[0].response_status, 204);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("GitHub dispatch records failure on non-204 responses", async () => {
  const store = createMemorySchedulerStore({
    jobs: [getJob({ dry_run: 0 })],
  });
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("nope", { status: 500 });

  try {
    const result = await runScheduler(store, { UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "pat" }, Date.parse("2026-07-10T04:15:30Z"));
    assert.equal(result.jobs_failed, 1);
    assert.equal(store.state.dispatches[0].dispatch_status, "failed");
    assert.equal(store.state.dispatches[0].response_status, 500);
    assert.match(store.state.dispatches[0].response_preview, /nope/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("malformed GitHub inputs fail safely without dispatching the target", async () => {
  const store = createMemorySchedulerStore({
    jobs: [
      getJob({
        dry_run: 0,
        github_inputs_json: "{oops",
      }),
    ],
  });
  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(null, { status: 204 });
  };

  try {
    const result = await runScheduler(store, { UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "pat" }, Date.parse("2026-07-10T04:15:30Z"));
    assert.equal(result.jobs_failed, 1);
    assert.equal(fetchCalls, 0);
    assert.equal(store.state.dispatches.length, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("GitHub PAT and Cloud Run secrets do not appear in logs", async () => {
  const store = createMemorySchedulerStore({
    jobs: [getJob({ dry_run: 0, cron_expr: "15 4 * * *" })],
  });
  const logs = captureLogs();
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(null, { status: 204 });

  try {
    await runScheduler(
      store,
      {
        UK_AQ_GITHUB_WORKFLOW_DISPATCH_PAT: "super-secret-pat",
        UK_AQ_EDGE_UPSTREAM_SECRET: "super-secret-cloud-run",
      },
      Date.parse("2026-07-10T04:15:30Z"),
    );

    assert.equal(
      logs.lines.some((line) => line.includes("super-secret-pat") || line.includes("super-secret-cloud-run")),
      false,
    );
  } finally {
    logs.restore();
    globalThis.fetch = originalFetch;
  }
});

test("Cloud Run dispatch adds the shared dispatch secret and records success", async () => {
  const store = createMemorySchedulerStore({
    jobs: [
      getJob({
        job_key: "uk_aq_db_size_logger",
        target_type: "cloud_run",
        cron_expr: "15 4 * * *",
        github_repo: null,
        github_workflow_file: null,
        github_ref: null,
        github_inputs_json: null,
        cloud_run_url: "https://example.invalid/run",
        cloud_run_method: "POST",
        cloud_run_headers_json: '{"x-custom-header":"value"}',
        cloud_run_body_json: '{"trigger_mode":"scheduler"}',
        dry_run: 0,
      }),
    ],
  });
  const calls = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    calls.push({
      url: String(url),
      method: init.method,
      headers: Object.fromEntries(new Headers(init.headers).entries()),
      body: init.body,
    });
    return new Response("ok", { status: 200 });
  };

  try {
    const result = await runScheduler(
      store,
      { UK_AQ_EDGE_UPSTREAM_SECRET: "cloud-secret" },
      Date.parse("2026-07-10T04:15:30Z"),
    );

    assert.equal(result.jobs_dispatched, 1);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].method, "POST");
    assert.equal(calls[0].headers["x-uk-aq-dispatch-secret"], "cloud-secret");
    assert.equal(calls[0].headers["x-custom-header"], "value");
    assert.match(calls[0].body, /trigger_mode/);
    assert.equal(store.state.dispatches[0].dispatch_status, "dispatched");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Cloud Run dispatch records failure for non-2xx responses", async () => {
  const store = createMemorySchedulerStore({
    jobs: [
      getJob({
        job_key: "uk_aq_db_size_logger",
        target_type: "cloud_run",
        cron_expr: "15 4 * * *",
        github_repo: null,
        github_workflow_file: null,
        github_ref: null,
        github_inputs_json: null,
        cloud_run_url: "https://example.invalid/run",
        cloud_run_method: "POST",
        cloud_run_headers_json: "{}",
        cloud_run_body_json: "{}",
        dry_run: 0,
      }),
    ],
  });
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("nope", { status: 500 });

  try {
    const result = await runScheduler(
      store,
      { UK_AQ_EDGE_UPSTREAM_SECRET: "cloud-secret" },
      Date.parse("2026-07-10T04:15:30Z"),
    );

    assert.equal(result.jobs_failed, 1);
    assert.equal(store.state.dispatches[0].dispatch_status, "failed");
    assert.equal(store.state.dispatches[0].response_status, 500);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Cloud Run response previews are truncated and redact the shared secret", async () => {
  const secret = "do-not-store-this-secret";
  const store = createMemorySchedulerStore({
    jobs: [
      getJob({
        job_key: "uk_aq_scomm",
        target_type: "cloud_run",
        cron_expr: "15 4 * * *",
        github_repo: null,
        github_workflow_file: null,
        github_ref: null,
        github_inputs_json: null,
        cloud_run_url: "https://example.invalid",
        cloud_run_body_json: '{"trigger_mode":"safety"}',
        dry_run: 0,
      }),
    ],
  });
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response(`${secret}:${"x".repeat(1_500)}`, { status: 500 });

  try {
    await runScheduler(
      store,
      { UK_AQ_EDGE_UPSTREAM_SECRET: secret },
      Date.parse("2026-07-10T04:15:30Z"),
    );
    const preview = store.state.dispatches[0].response_preview;
    assert.equal(preview.includes(secret), false);
    assert.match(preview, /^\[REDACTED\]:/);
    assert.equal(preview.length, 1_000);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("Cloud Run dispatch fails closed when the Worker secret is missing", async () => {
  const store = createMemorySchedulerStore({
    jobs: [
      getJob({
        job_key: "uk_aq_sos",
        target_type: "cloud_run",
        cron_expr: "15 4 * * *",
        github_repo: null,
        github_workflow_file: null,
        github_ref: null,
        github_inputs_json: null,
        cloud_run_url: "https://example.invalid",
        cloud_run_body_json: '{"trigger_mode":"safety"}',
        dry_run: 0,
      }),
    ],
  });
  let fetchCalls = 0;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(null, { status: 204 });
  };

  try {
    const result = await runScheduler(store, {}, Date.parse("2026-07-10T04:15:30Z"));
    assert.equal(result.jobs_failed, 1);
    assert.equal(fetchCalls, 0);
    assert.equal(store.state.dispatches[0].dispatch_status, "failed");
    assert.match(store.state.dispatches[0].reason, /UK_AQ_EDGE_UPSTREAM_SECRET/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("migration defines the three scheduler tables and duplicate-prevention index", () => {
  assert.match(migrationText, /create table if not exists scheduler_jobs/);
  assert.match(migrationText, /create table if not exists scheduler_dispatches/);
  assert.match(migrationText, /create table if not exists scheduler_runs/);
  assert.match(migrationText, /unique \(job_key, due_at\)/);
  assert.match(migrationText, /create index if not exists scheduler_dispatches_job_time_idx/);
  assert.match(migrationText, /create index if not exists scheduler_runs_name_started_idx/);
});
