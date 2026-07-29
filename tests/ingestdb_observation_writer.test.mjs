import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  classifyIngestDbObservationWriteFailure,
  INGESTDB_OBSERVATION_WRITE_DEFAULTS,
  parseIngestDbObservationWriteConfig,
  writeIngestDbObservations,
} from "../supabase/functions/_shared/ingestdb_observation_writer.mjs";

const silentLogger = { warn() {}, error() {} };
const rows = (count) => Array.from({ length: count }, (_, index) => ({
  connector_id: 1,
  timeseries_id: index + 1,
  observed_at: "2026-07-29T00:00:00.000Z",
  value: index,
  status: null,
}));

function statementTimeout() {
  return Object.assign(
    new Error("canceling statement due to statement timeout"),
    { code: "57014", httpStatus: 500 },
  );
}

test("normal write commits one logical chunk on its first request", async () => {
  let calls = 0;
  const stats = await writeIngestDbObservations({
    rows: rows(3),
    chunkSize: 3,
    connectorCode: "test",
    logger: silentLogger,
    writeChunk: async () => calls += 1,
  });
  assert.equal(calls, 1);
  assert.deepEqual(stats, {
    input_rows: 3,
    normal_chunk_size: 3,
    committed_rows: 3,
    write_requests: 1,
    retry_attempts: 0,
    retried_chunks: 0,
    split_operations: 0,
    smallest_attempted_chunk: 3,
    unresolved_rows: 0,
    terminal_failure_classification: null,
    terminal_reason: null,
    stopped_for_runtime_budget: false,
  });
});

test("statement timeout retries with increasing bounded backoff and positive jitter", async () => {
  let calls = 0;
  const delays = [];
  const stats = await writeIngestDbObservations({
    rows: rows(2),
    connectorCode: "test",
    logger: silentLogger,
    config: { attempts: 3, retryBaseMs: 100, retryMaxMs: 1_000 },
    random: () => 0,
    sleep: async (delay) => delays.push(delay),
    writeChunk: async () => {
      calls += 1;
      if (calls < 3) throw statementTimeout();
    },
  });
  assert.deepEqual(delays, [101, 201]);
  assert.ok(delays.every((delay) => delay > 0 && delay <= 1_000));
  assert.equal(stats.committed_rows, 2);
  assert.equal(stats.write_requests, 3);
  assert.equal(stats.retry_attempts, 2);
  assert.equal(stats.retried_chunks, 1);
});

test("jitter is bounded at both ends", async () => {
  const observed = [];
  for (const randomValue of [0, 0.999999]) {
    let calls = 0;
    await writeIngestDbObservations({
      rows: rows(1),
      connectorCode: "test",
      logger: silentLogger,
      config: { attempts: 2, retryBaseMs: 100, retryMaxMs: 250 },
      random: () => randomValue,
      sleep: async (delay) => observed.push(delay),
      writeChunk: async () => {
        calls += 1;
        if (calls === 1) throw statementTimeout();
      },
    });
  }
  assert.deepEqual(observed, [101, 200]);
});

test("non-retryable and clearly permanent HTTP 500 failures fail immediately", async () => {
  for (const error of [
    Object.assign(new Error("invalid payload"), { httpStatus: 400 }),
    Object.assign(new Error("column bad_name does not exist"), {
      code: "42703",
      httpStatus: 500,
    }),
    Object.assign(new Error("canceling statement due to user request"), {
      code: "57014",
      httpStatus: 500,
    }),
  ]) {
    let calls = 0;
    await assert.rejects(
      writeIngestDbObservations({
        rows: rows(2),
        connectorCode: "test",
        logger: silentLogger,
        sleep: async () => assert.fail("must not sleep"),
        writeChunk: async () => {
          calls += 1;
          throw error;
        },
      }),
      (thrown) => thrown.terminalReason === "non_retryable_error",
    );
    assert.equal(calls, 1);
  }
});

test("persistent statement timeout splits both children and preserves order", async () => {
  const attempted = [];
  const input = rows(8);
  const stats = await writeIngestDbObservations({
    rows: input,
    connectorCode: "test",
    logger: silentLogger,
    config: { attempts: 1, splitMinRows: 2, splitMaxDepth: 3 },
    writeChunk: async (chunk) => {
      attempted.push(chunk.map((row) => row.timeseries_id));
      if (chunk.length === 8) throw statementTimeout();
    },
  });
  assert.deepEqual(attempted, [
    [1, 2, 3, 4, 5, 6, 7, 8],
    [1, 2, 3, 4],
    [5, 6, 7, 8],
  ]);
  assert.equal(stats.committed_rows, 8);
  assert.equal(stats.split_operations, 1);
  assert.equal(stats.smallest_attempted_chunk, 4);
});

test("a successful child is retained while its sibling splits further", async () => {
  const attempted = [];
  const stats = await writeIngestDbObservations({
    rows: rows(8),
    connectorCode: "test",
    logger: silentLogger,
    config: { attempts: 1, splitMinRows: 2, splitMaxDepth: 3 },
    writeChunk: async (chunk) => {
      const ids = chunk.map((row) => row.timeseries_id);
      attempted.push(ids);
      if (chunk.length === 8 || ids[0] === 5 && chunk.length === 4) {
        throw statementTimeout();
      }
    },
  });
  assert.deepEqual(attempted, [
    [1, 2, 3, 4, 5, 6, 7, 8],
    [1, 2, 3, 4],
    [5, 6, 7, 8],
    [5, 6],
    [7, 8],
  ]);
  assert.equal(attempted.filter((ids) => ids.join(",") === "1,2,3,4").length, 1);
  assert.equal(stats.committed_rows, 8);
  assert.equal(stats.split_operations, 2);
});

test("successful child is not repeated when its sibling reaches a terminal failure", async () => {
  const attempted = [];
  await assert.rejects(
    writeIngestDbObservations({
      rows: rows(8),
      connectorCode: "test",
      logger: silentLogger,
      config: { attempts: 1, splitMinRows: 2, splitMaxDepth: 1 },
      writeChunk: async (chunk) => {
        const ids = chunk.map((row) => row.timeseries_id);
        attempted.push(ids);
        if (chunk.length === 8 || ids[0] === 5) throw statementTimeout();
      },
    }),
    (error) => {
      assert.equal(error.stats.committed_rows, 4);
      assert.equal(error.stats.unresolved_rows, 4);
      assert.equal(error.stats.split_operations, 1);
      return true;
    },
  );
  assert.deepEqual(attempted, [
    [1, 2, 3, 4, 5, 6, 7, 8],
    [1, 2, 3, 4],
    [5, 6, 7, 8],
  ]);
});

test("minimum chunk size and maximum split depth are terminal and never create empty children", async () => {
  const attemptedSizes = [];
  await assert.rejects(
    writeIngestDbObservations({
      rows: rows(5),
      connectorCode: "test",
      logger: silentLogger,
      config: { attempts: 1, splitMinRows: 3, splitMaxDepth: 5 },
      writeChunk: async (chunk) => {
        attemptedSizes.push(chunk.length);
        throw statementTimeout();
      },
    }),
    (error) => error.terminalReason === "minimum_chunk_failed",
  );
  await assert.rejects(
    writeIngestDbObservations({
      rows: rows(8),
      connectorCode: "test",
      logger: silentLogger,
      config: { attempts: 1, splitMinRows: 1, splitMaxDepth: 1 },
      writeChunk: async (chunk) => {
        attemptedSizes.push(chunk.length);
        throw statementTimeout();
      },
    }),
    (error) => error.stats.split_operations === 1,
  );
  assert.ok(attemptedSizes.every((size) => size > 0));
});

test("ambiguous retry is idempotent on the canonical observation key", async () => {
  const stored = new Map();
  let calls = 0;
  const input = rows(3);
  const stats = await writeIngestDbObservations({
    rows: input,
    connectorCode: "test",
    logger: silentLogger,
    sleep: async () => {},
    writeChunk: async (chunk) => {
      calls += 1;
      for (const row of chunk) {
        const key = `${row.connector_id}|${row.timeseries_id}|${row.observed_at}`;
        stored.set(key, row);
      }
      if (calls === 1) throw statementTimeout();
    },
  });
  assert.equal(stored.size, 3);
  assert.equal(stats.committed_rows, 3);
  assert.equal(stats.write_requests, 2);
});

test("runtime budget stop is distinct from retry exhaustion", async () => {
  let budgetChecks = 0;
  let calls = 0;
  await assert.rejects(
    writeIngestDbObservations({
      rows: rows(2),
      connectorCode: "test",
      logger: silentLogger,
      config: {
        attempts: 3,
        retryBaseMs: 10,
        retryMaxMs: 100,
        minimumAttemptRuntimeMs: 100,
        shutdownBufferMs: 100,
      },
      runtimeBudget: {
        shouldStop: () => false,
        remainingRuntimeMs: () => ++budgetChecks === 1 ? 1_000 : 100,
      },
      sleep: async () => assert.fail("insufficient budget must prevent delay"),
      writeChunk: async () => {
        calls += 1;
        throw statementTimeout();
      },
    }),
    (error) => {
      assert.equal(error.terminalReason, "runtime_budget");
      assert.equal(error.classification, "runtime_budget");
      assert.equal(error.stats.stopped_for_runtime_budget, true);
      return true;
    },
  );
  assert.equal(calls, 1);
});

test("failure classifier covers required transient classes", () => {
  const cases = [
    [statementTimeout(), "statement_timeout"],
    [Object.assign(new Error("deadlock detected"), { code: "40P01" }), "deadlock"],
    [Object.assign(new Error("could not serialize access"), { code: "40001" }), "serialization_failure"],
    [Object.assign(new Error("connection reset"), { code: "08006" }), "connection_failure"],
    [Object.assign(new Error("too many requests"), { httpStatus: 429 }), "rate_limited"],
    [Object.assign(new Error("temporary upstream failure"), { httpStatus: 503 }), "temporary_service_failure"],
  ];
  for (const [error, expected] of cases) {
    const result = classifyIngestDbObservationWriteFailure(error);
    assert.equal(result.classification, expected);
    assert.equal(result.retryable, true);
  }
});

test("configuration parsing rejects invalid and out-of-bounds values", () => {
  assert.deepEqual(
    parseIngestDbObservationWriteConfig({
      attempts: 0,
      retryBaseMs: "nope",
      retryMaxMs: 31_000,
      splitMinRows: -1,
      splitMaxDepth: 11,
      minimumAttemptRuntimeMs: 0,
      shutdownBufferMs: -1,
    }),
    INGESTDB_OBSERVATION_WRITE_DEFAULTS,
  );
  const parsed = parseIngestDbObservationWriteConfig({
    attempts: 5,
    retryBaseMs: 100,
    retryMaxMs: 1_000,
    splitMinRows: 1,
    splitMaxDepth: 0,
    minimumAttemptRuntimeMs: 50,
    shutdownBufferMs: 0,
  });
  assert.equal(parsed.attempts, 5);
  assert.equal(parsed.splitMaxDepth, 0);
  assert.equal(
    parseIngestDbObservationWriteConfig({ minimumAttemptRuntimeMs: 60_000 })
      .minimumAttemptRuntimeMs,
    60_000,
  );
});

test("the shared helper is directly importable by the Node runtime", async () => {
  const helper = await import(
    "../supabase/functions/_shared/ingestdb_observation_writer.mjs"
  );
  assert.equal(typeof helper.writeIngestDbObservations, "function");
});

test("changed callers preserve canonical upsert, committed accounting, and checkpoint safety", async () => {
  const callerPaths = [
    "../workers/uk_aq_sensorcommunity_cloud_run/index.mjs",
    "../supabase/functions/ingest_sensorcommunity/index.ts",
    "../supabase/functions/ingest_breathelondon/index.ts",
    "../supabase/functions/ingest_openaq/index.ts",
    "../supabase/functions/ingest_erg_laqn/index.ts",
    "../supabase/functions/ingest_sos/index.ts",
  ];
  const sources = await Promise.all(callerPaths.map((path) =>
    readFile(new URL(path, import.meta.url), "utf8")
  ));
  for (const source of sources) {
    assert.match(source, /writeIngestDbObservations/);
    assert.match(
      source,
      /connector_id,timeseries_id,observed_at|uk_aq_rpc_observations_upsert/,
    );
  }
  for (const index of [1, 2, 3, 4, 5]) {
    assert.match(sources[index], /minimumAttemptRuntimeMs: DEFAULT(?:_POSTGREST)?_TIMEOUT_MS/);
    assert.match(sources[index], /cross_database_transaction: false/);
  }
  assert.match(sources[0], /Promise\.allSettled/);
  assert.match(sources[0], /minimumAttemptRuntimeMs: HTTP_TIMEOUT_MS/);
  assert.match(sources[0], /observations_upserted: ingestDbWriteStats\.committed_rows/);
  assert.match(sources[1], /Promise\.allSettled/);
  assert.match(sources[1], /obsaqidb_write/);
  assert.ok(
    sources[0].indexOf("upsertTimeseries(timeseriesMetadataPayload)") <
      sources[0].indexOf("upsertObservations(observationRows"),
  );
  assert.ok(
    sources[0].indexOf("upsertTimeseries(timeseriesPayload)",
      sources[0].indexOf("upsertObservations(observationRows")) >
      sources[0].indexOf("upsertObservations(observationRows"),
  );
  assert.ok(
    sources[1].indexOf("upsertTimeseries(timeseriesMetadataPayload)") <
      sources[1].indexOf("upsertObservations(observationRows"),
  );
  assert.ok(
    sources[1].indexOf("upsertTimeseries(timeseriesPayload)",
      sources[1].indexOf("upsertObservations(observationRows")) >
      sources[1].indexOf("upsertObservations(observationRows"),
  );
  assert.match(sources[5], /successfulCheckpointCandidates/);
  assert.match(sources[5], /if \(!ingestDbObservationWriteFailed\)/);
  for (const index of [0, 1, 2, 3, 5]) {
    assert.match(sources[index], /dedupe|deduped/);
  }
});
