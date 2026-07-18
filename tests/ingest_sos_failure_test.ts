import {
  addRuntimeDeadlineFailure,
  connectorHttpStatusForProbe,
  isIndividuallyReportedTimeseriesFailure,
  isRuntimeDeadlineFailure,
  runtimeBudgetStopObserved,
  SosFetchFailure,
} from "../supabase/functions/ingest_sos/failure.ts";
import {
  buildSosCloudRunChildResult,
  buildSosCloudRunSkippedResult,
  decideSosCloudRunServiceResult,
  isCompletedSosChildResponse,
  isRecognizedSosDependencyFailure,
  isSosCloudRunChildResult,
} from "../workers/uk_aq_sos_cloud_run/result_contract.ts";

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
  if (
    !isIndividuallyReportedTimeseriesFailure(requestTimeout) ||
    !isIndividuallyReportedTimeseriesFailure(httpFailure) ||
    isIndividuallyReportedTimeseriesFailure(
      new SosFetchFailure({
        kind: "runtime_deadline",
        message: "runtime deadline",
      }),
    )
  ) {
    throw new Error("Individual timeseries error classification was incorrect");
  }

  if (!runtimeBudgetStopObserved(true, 0)) {
    throw new Error("Pool stop without an in-flight abort was not recognised");
  }
  if (runtimeBudgetStopObserved(false, 0)) {
    throw new Error(
      "Completed work was incorrectly treated as a runtime-budget stop",
    );
  }
});

Deno.test("SOS Cloud Run preserves structured dependency failures without wrapper failure routing", () => {
  const response = {
    ok: false,
    status: 503,
    body: {
      status: "upstream_unavailable",
      upstream_status: null,
      upstream_failure_kind: "request_timeout",
      connector_http_status: 503,
      series_polled: 0,
      observations_upserted: 0,
    },
  };
  if (
    !isRecognizedSosDependencyFailure(response) ||
    !isCompletedSosChildResponse(response)
  ) {
    throw new Error(
      "Recognised dependency failure entered the wrapper-failure route",
    );
  }
  const result = buildSosCloudRunChildResult(
    response,
    "failed",
    "UK-AIR SOS upstream unavailable: request_timeout (connector HTTP 503).",
  );
  if (
    !result || result.httpStatus !== 503 ||
    result.payload.upstream_status !== null ||
    result.payload.upstream_failure_kind !== "request_timeout" ||
    result.payload.connector_http_status !== 503
  ) {
    throw new Error(
      `Structured dependency result was not retained: ${
        JSON.stringify(result)
      }`,
    );
  }

  const upstreamHttpFailure = {
    ok: false,
    status: 502,
    body: {
      status: "upstream_unavailable",
      upstream_status: 502,
      upstream_failure_kind: "http",
      connector_http_status: 502,
    },
  };
  const upstreamHttpResult = buildSosCloudRunChildResult(
    upstreamHttpFailure,
    "failed",
    "UK-AIR SOS upstream unavailable: HTTP 502.",
  );
  if (
    !isRecognizedSosDependencyFailure(upstreamHttpFailure) ||
    !upstreamHttpResult || upstreamHttpResult.httpStatus !== 502
  ) {
    throw new Error(
      "Actual upstream HTTP status was not preserved through Cloud Run",
    );
  }
});

Deno.test("SOS Cloud Run keeps local failures generic and preserves partial runtime-budget results", () => {
  const localFailure = {
    ok: false,
    status: 500,
    body: {
      status: "upstream_unavailable",
      upstream_status: null,
      upstream_failure_kind: "unknown",
      connector_http_status: 500,
    },
  };
  if (
    isRecognizedSosDependencyFailure(localFailure) ||
    isCompletedSosChildResponse(localFailure)
  ) {
    throw new Error(
      "Unknown local failure was incorrectly treated as dependency availability",
    );
  }

  const partial = {
    ok: true,
    status: 207,
    body: {
      status: "ok",
      partial: true,
      stopped_reason: "runtime_budget_exceeded",
      runtime_deadline_failure_count: 0,
      runtime_deadline_timeseries_sample: [],
      individual_error_count: 2,
      series_polled: 4,
      observations_upserted: 7,
    },
  };
  if (!isCompletedSosChildResponse(partial)) {
    throw new Error(
      "Partial runtime-budget response entered the wrapper-failure route",
    );
  }
  const result = buildSosCloudRunChildResult(
    partial,
    "partial",
    "runtime_budget_exceeded",
  );
  if (!result || result.httpStatus !== 207 || result.payload.partial !== true) {
    throw new Error(
      `Partial result was not retained: ${JSON.stringify(result)}`,
    );
  }
});

Deno.test("SOS Cloud Run writes and accepts compact results for every skipped outcome", () => {
  for (
    const reason of [
      "poll_disabled",
      "claim_not_acquired",
      "no_station_refs",
      "no_timeseries_ids",
    ]
  ) {
    const result = buildSosCloudRunSkippedResult(reason, 42);
    if (
      !isSosCloudRunChildResult(result) || result.httpStatus !== 200 ||
      result.payload.ok !== true || result.payload.status !== "skipped" ||
      result.payload.reason !== reason || result.payload.connector_id !== 42
    ) {
      throw new Error(`Skipped result was invalid: ${JSON.stringify(result)}`);
    }
  }
});

Deno.test("SOS Cloud Run service fails closed when a successful child has no valid result", () => {
  const valid = buildSosCloudRunSkippedResult("poll_disabled");
  const validDecision = decideSosCloudRunServiceResult(
    true,
    0,
    valid,
    "valid",
  );
  if (
    validDecision.httpStatus !== 200 ||
    validDecision.payload.status !== "skipped"
  ) {
    throw new Error(
      `Valid child result was not returned: ${JSON.stringify(validDecision)}`,
    );
  }

  const missingDecision = decideSosCloudRunServiceResult(
    true,
    0,
    null,
    "missing",
  );
  const invalidDecision = decideSosCloudRunServiceResult(
    true,
    0,
    null,
    "invalid",
  );
  const failedDecision = decideSosCloudRunServiceResult(
    false,
    1,
    null,
    "missing",
  );
  if (
    missingDecision.httpStatus !== 500 ||
    missingDecision.payload.error !== "missing_child_result" ||
    invalidDecision.httpStatus !== 500 ||
    invalidDecision.payload.error !== "invalid_child_result" ||
    failedDecision.httpStatus !== 500 || failedDecision.payload.ok !== false
  ) {
    throw new Error("Cloud Run service did not fail closed");
  }

  if (
    isSosCloudRunChildResult({ httpStatus: 200 }) ||
    isSosCloudRunChildResult({ httpStatus: 200, payload: {} }) ||
    isSosCloudRunChildResult({
      httpStatus: 200,
      payload: {
        ok: true,
        status: "skipped",
        run_status: "skipped",
      },
    })
  ) {
    throw new Error("Malformed child result was incorrectly accepted");
  }
});
