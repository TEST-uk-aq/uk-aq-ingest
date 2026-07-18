import {
  addRuntimeDeadlineFailure,
  connectorHttpStatusForProbe,
  isRuntimeDeadlineFailure,
  SosFetchFailure,
} from "../supabase/functions/ingest_sos/failure.ts";

Deno.test("SOS probe maps timeout failures without an upstream response to 503", () => {
  for (const kind of ["request_timeout", "runtime_deadline"] as const) {
    const failure = new SosFetchFailure({ kind, message: kind });
    if (connectorHttpStatusForProbe(failure) !== 503) {
      throw new Error(`${kind} did not map to HTTP 503`);
    }
  }
});

Deno.test("SOS probe preserves actual upstream HTTP statuses and unknown failures stay 500", () => {
  const upstreamFailure = new SosFetchFailure({
    kind: "http",
    message: "HTTP 502 Bad Gateway",
    upstreamStatus: 502,
    retryable: true,
  });
  if (connectorHttpStatusForProbe(upstreamFailure) !== 502) {
    throw new Error("HTTP 502 was not preserved");
  }
  const unknownFailure = new SosFetchFailure({
    kind: "unknown",
    message: "local error",
  });
  if (connectorHttpStatusForProbe(unknownFailure) !== 500) {
    throw new Error("Unknown failure did not map to HTTP 500");
  }
});

Deno.test("SOS runtime-deadline failures are retained as one bounded run summary", () => {
  const summary = { count: 0, timeseriesSample: [] as number[] };
  for (const id of [101, 102, 103, 104]) {
    addRuntimeDeadlineFailure(summary, id, 2);
  }
  if (
    summary.count !== 4 ||
    JSON.stringify(summary.timeseriesSample) !== "[101,102]"
  ) {
    throw new Error(`Unexpected summary: ${JSON.stringify(summary)}`);
  }

  const requestTimeout = new SosFetchFailure({
    kind: "request_timeout",
    message: "request timeout",
    retryable: true,
  });
  const httpFailure = new SosFetchFailure({
    kind: "http",
    message: "HTTP 502 Bad Gateway",
    upstreamStatus: 502,
  });
  if (
    isRuntimeDeadlineFailure(requestTimeout) ||
    isRuntimeDeadlineFailure(httpFailure)
  ) {
    throw new Error(
      "Non-deadline timeseries failures would be incorrectly consolidated",
    );
  }
});
