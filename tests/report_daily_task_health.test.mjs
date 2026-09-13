import assert from "node:assert/strict";
import test from "node:test";

import {
  isTransientSupabaseError,
  isRetrySafeHealthRpc,
  postRpc,
  retrySupabaseOperation,
} from "../scripts/report_daily_task_health.mjs";

function response(status, body) {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => body,
  };
}

test("health reporting retries safe idempotent RPCs after transient PostgREST HTTP 504", async () => {
  let calls = 0;
  const delays = [];
  const result = await postRpc({
    supabaseUrl: "https://example.test",
    serviceRoleKey: "test-key",
    rpcName: "uk_aq_rpc_daily_task_finished",
    body: { p: {} },
  }, {
    fetchImpl: async () => {
      calls += 1;
      return calls === 1 ? response(504, "gateway timeout") : response(200, '"run-1"');
    },
    sleep: async (delay) => delays.push(delay),
    logger: { warn() {} },
  });

  assert.equal(result, "run-1");
  assert.equal(calls, 2);
  assert.deepEqual(delays, [250]);
});

test("health reporting does not retry non-idempotent RPCs after transient failure", async () => {
  let calls = 0;
  const delays = [];
  await assert.rejects(postRpc({
    supabaseUrl: "https://example.test",
    serviceRoleKey: "test-key",
    rpcName: "uk_aq_rpc_daily_task_started",
    body: { p: {} },
  }, {
    fetchImpl: async () => {
      calls += 1;
      return response(504, "gateway timeout");
    },
    sleep: async (delay) => delays.push(delay),
    logger: { warn() {} },
  }));

  assert.equal(calls, 1);
  assert.deepEqual(delays, []);
  assert.equal(isRetrySafeHealthRpc("uk_aq_rpc_daily_task_started"), false);
  assert.equal(isRetrySafeHealthRpc("uk_aq_rpc_daily_task_report_final"), false);
  assert.equal(isRetrySafeHealthRpc("uk_aq_rpc_daily_task_finished"), true);
  assert.equal(isRetrySafeHealthRpc("uk_aq_rpc_daily_task_failed"), true);
  assert.equal(isRetrySafeHealthRpc("uk_aq_rpc_recompute_daily_task_status"), true);
});

test("health reporting does not retry permanent HTTP errors", async () => {
  let calls = 0;
  await assert.rejects(
    retrySupabaseOperation("RPC test", async () => {
      calls += 1;
      const error = new Error("unauthorised");
      error.status = 401;
      throw error;
    }, { sleep: async () => assert.fail("permanent failures must not sleep") }),
  );
  assert.equal(calls, 1);
  assert.equal(isTransientSupabaseError({ status: 401 }), false);
  assert.equal(isTransientSupabaseError({ status: 503 }), true);
});
