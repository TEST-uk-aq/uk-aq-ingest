import {
  isTransientJwtIssuedAtFuture,
  type PostgrestResponse,
  requestWithTransientJwtFutureRetry,
} from "../workers/uk_aq_sos_cloud_run/postgrest_auth_retry.ts";

function response(
  status: number,
  data: unknown,
): PostgrestResponse {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: JSON.stringify(data),
    data,
  };
}

function assertEquals(actual: unknown, expected: unknown): void {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}.`,
    );
  }
}

Deno.test("SOS PostgREST retry classifier requires the exact JWT-future evidence", () => {
  const jwtFuture = response(401, {
    code: "PGRST303",
    message: "JWT issued at future",
  });
  if (!isTransientJwtIssuedAtFuture(jwtFuture)) {
    throw new Error(
      "Canonical transient JWT-future response was not recognised.",
    );
  }

  for (
    const unrelated of [
      response(403, jwtFuture.data),
      response(401, { code: "PGRST301", message: "JWT expired" }),
      response(401, { code: "PGRST303", message: "Invalid JWT" }),
      response(401, { message: "JWT issued at future" }),
      response(401, "JWT issued at future"),
    ]
  ) {
    if (isTransientJwtIssuedAtFuture(unrelated)) {
      throw new Error(
        `Unrelated response was classified as retryable: ${unrelated.text}`,
      );
    }
  }
});

Deno.test("SOS PostgREST JWT-future retry uses at most three attempts with 1s/2s backoff", async () => {
  const responses = [
    response(401, { code: "PGRST303", message: "JWT issued at future" }),
    response(401, { code: "PGRST303", message: "JWT issued at future" }),
    response(200, [{ id: 1 }]),
  ];
  const delays: number[] = [];
  const retryNumbers: number[] = [];
  let calls = 0;

  const finalResponse = await requestWithTransientJwtFutureRetry(
    async () => responses[calls++],
    {
      operation: "load_connector",
      target: "GET connectors",
      logRetry: (details) => retryNumbers.push(details.retry_number),
      sleep: (delayMs) => {
        delays.push(delayMs);
        return Promise.resolve();
      },
    },
  );

  assertEquals(calls, 3);
  assertEquals(delays, [1000, 2000]);
  assertEquals(retryNumbers, [1, 2]);
  assertEquals(finalResponse.status, 200);
});

Deno.test("SOS PostgREST retry stops immediately when a later 401 is unrelated", async () => {
  const responses = [
    response(401, { code: "PGRST303", message: "JWT issued at future" }),
    response(401, { code: "PGRST301", message: "JWT expired" }),
  ];
  const delays: number[] = [];
  let calls = 0;

  const finalResponse = await requestWithTransientJwtFutureRetry(
    async () => responses[calls++],
    {
      operation: "load_connector",
      target: "GET connectors",
      logRetry: () => {},
      sleep: (delayMs) => {
        delays.push(delayMs);
        return Promise.resolve();
      },
    },
  );

  assertEquals(calls, 2);
  assertEquals(delays, [1000]);
  assertEquals(finalResponse.status, 401);
  assertEquals(finalResponse.data, {
    code: "PGRST301",
    message: "JWT expired",
  });
});

Deno.test("SOS PostgREST retry returns the third persistent JWT-future response", async () => {
  const jwtFuture = response(401, {
    code: "PGRST303",
    message: "JWT issued at future",
  });
  const delays: number[] = [];
  let calls = 0;

  const finalResponse = await requestWithTransientJwtFutureRetry(
    () => {
      calls += 1;
      return Promise.resolve(jwtFuture);
    },
    {
      operation: "load_connector",
      target: "GET connectors",
      logRetry: () => {},
      sleep: (delayMs) => {
        delays.push(delayMs);
        return Promise.resolve();
      },
    },
  );

  assertEquals(calls, 3);
  assertEquals(delays, [1000, 2000]);
  assertEquals(finalResponse, jwtFuture);
});
