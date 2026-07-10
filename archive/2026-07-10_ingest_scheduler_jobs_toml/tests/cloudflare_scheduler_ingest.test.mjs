import assert from "node:assert/strict";
import test from "node:test";

import { evaluateIngestJob } from "../cloudflare/scheduler/shared.mjs";
import { INGEST_JOBS } from "../cloudflare/scheduler/ingest/worker.mjs";

test("ingest job is due when last success is stale and no run is in progress", () => {
  const job = INGEST_JOBS.find((item) => item.job_key === "uk_aq_sos");
  assert.ok(job);
  const now = Date.parse("2026-07-10T12:15:00Z");
  const rows = [
    {
      connector_code: "sos",
      run_started_at: "2026-07-10T11:40:00Z",
      run_ended_at: "2026-07-10T11:41:00Z",
      run_status: "succeeded",
      created_at: "2026-07-10T11:41:00Z",
    },
  ];

  const decision = evaluateIngestJob(job, rows, now);
  assert.equal(decision.due, true);
  assert.equal(decision.reason, "due");
  assert.equal(decision.wouldTrigger, true);
});

test("ingest job skips recent run and in-flight run", () => {
  const job = INGEST_JOBS.find((item) => item.job_key === "uk_aq_blondon_nodes");
  assert.ok(job);
  const now = Date.parse("2026-07-10T12:15:00Z");
  const rows = [
    {
      connector_code: "blondon_nodes",
      run_started_at: "2026-07-10T12:10:00Z",
      run_ended_at: null,
      run_status: "running",
      created_at: "2026-07-10T12:10:00Z",
    },
  ];

  const decision = evaluateIngestJob(job, rows, now);
  assert.equal(decision.due, false);
  assert.equal(decision.reason, "run_in_progress");
  assert.equal(decision.wouldTrigger, false);
});

test("planned phase-2 jobs exclude deferred ingest targets", () => {
  assert.equal(INGEST_JOBS.some((job) => job.job_key === "uk_aq_db_size_logger"), false);
  assert.equal(INGEST_JOBS.some((job) => job.job_key === "uk_aq_timeseries_aqi_hourly"), false);
});
