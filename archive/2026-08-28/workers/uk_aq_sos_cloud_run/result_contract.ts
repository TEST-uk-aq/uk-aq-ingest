type IngestResponseLike = {
  ok: boolean;
  status: number;
  body: unknown;
};

export type SosCloudRunChildResult = {
  httpStatus: number;
  payload: Record<string, unknown>;
};

export type SosCloudRunResultReadState = "missing" | "invalid" | "valid";

export type SosCloudRunServiceResult = {
  httpStatus: number;
  payload: Record<string, unknown>;
};

const RESPONSE_KEYS = [
  "status",
  "partial",
  "stopped_reason",
  "upstream_status",
  "upstream_failure_kind",
  "connector_http_status",
  "runtime_deadline_failure_count",
  "runtime_deadline_timeseries_sample",
  "individual_error_count",
  "series_polled",
  "observations_upserted",
  "connector_id",
] as const;

const CHILD_PAYLOAD_KEYS = new Set([
  "ok",
  "run_status",
  "run_message",
  "reason",
  ...RESPONSE_KEYS,
]);

function asObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function asInteger(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isInteger(parsed) ? parsed : null;
}

function asBoundedString(value: unknown, maxLength = 500): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  return trimmed.length <= maxLength
    ? trimmed
    : `${trimmed.slice(0, maxLength - 3)}...`;
}

function isBoundedString(value: unknown, maxLength = 500): value is string {
  return typeof value === "string" && value.trim() === value &&
    value.length > 0 && value.length <= maxLength;
}

function isHttpStatus(value: unknown): value is number {
  const status = asInteger(value);
  return status !== null && status >= 100 && status <= 599;
}

function isNonNegativeInteger(value: unknown): value is number {
  const integer = asInteger(value);
  return integer !== null && integer >= 0;
}

function isOptionalHttpStatus(value: unknown): boolean {
  return value === null || isHttpStatus(value);
}

function isValidChildPayload(payload: Record<string, unknown>): boolean {
  if (Object.keys(payload).some((key) => !CHILD_PAYLOAD_KEYS.has(key))) {
    return false;
  }
  if (
    typeof payload.ok !== "boolean" ||
    !isBoundedString(payload.status, 64) ||
    !isBoundedString(payload.run_status, 64) ||
    !isBoundedString(payload.run_message)
  ) {
    return false;
  }
  if (payload.reason !== undefined && !isBoundedString(payload.reason, 128)) {
    return false;
  }
  if (payload.partial !== undefined && typeof payload.partial !== "boolean") {
    return false;
  }
  if (
    payload.stopped_reason !== undefined &&
    payload.stopped_reason !== null &&
    !isBoundedString(payload.stopped_reason, 128)
  ) {
    return false;
  }
  if (
    payload.upstream_status !== undefined &&
    !isOptionalHttpStatus(payload.upstream_status)
  ) {
    return false;
  }
  if (
    payload.upstream_failure_kind !== undefined &&
    !isBoundedString(payload.upstream_failure_kind, 64)
  ) {
    return false;
  }
  if (
    payload.connector_http_status !== undefined &&
    !isHttpStatus(payload.connector_http_status)
  ) {
    return false;
  }
  for (
    const key of [
      "runtime_deadline_failure_count",
      "individual_error_count",
      "series_polled",
      "observations_upserted",
    ]
  ) {
    if (payload[key] !== undefined && !isNonNegativeInteger(payload[key])) {
      return false;
    }
  }
  if (
    payload.runtime_deadline_timeseries_sample !== undefined &&
    (!Array.isArray(payload.runtime_deadline_timeseries_sample) ||
      payload.runtime_deadline_timeseries_sample.length > 10 ||
      payload.runtime_deadline_timeseries_sample.some((value) =>
        !isNonNegativeInteger(value)
      ))
  ) {
    return false;
  }
  return payload.connector_id === undefined ||
    isNonNegativeInteger(payload.connector_id) ||
    isBoundedString(payload.connector_id, 128);
}

function compactPayload(
  payload: Record<string, unknown>,
  responseOk: boolean,
  runStatus: string,
  runMessage: string,
): Record<string, unknown> {
  const compact: Record<string, unknown> = {
    ok: responseOk,
    run_status: runStatus,
    run_message: asBoundedString(runMessage),
  };
  for (const key of RESPONSE_KEYS) {
    if (Object.prototype.hasOwnProperty.call(payload, key)) {
      compact[key] = payload[key];
    }
  }
  const sample = compact.runtime_deadline_timeseries_sample;
  if (Array.isArray(sample)) {
    compact.runtime_deadline_timeseries_sample = sample
      .map(asInteger)
      .filter((value): value is number => value !== null)
      .slice(0, 10);
  }
  return compact;
}

export function isRecognizedSosDependencyFailure(
  response: IngestResponseLike,
): boolean {
  const payload = asObject(response.body);
  if (!payload || payload.status !== "upstream_unavailable") {
    return false;
  }
  const connectorStatus = asInteger(payload.connector_http_status);
  const failureKind = asBoundedString(payload.upstream_failure_kind, 64);
  if (connectorStatus === null || connectorStatus !== response.status) {
    return false;
  }
  if (failureKind === "http") {
    return asInteger(payload.upstream_status) === response.status;
  }
  if (
    (failureKind === "request_timeout" ||
      failureKind === "runtime_deadline") &&
    payload.upstream_status === null && response.status === 503
  ) {
    return true;
  }
  return failureKind === "network" &&
    payload.upstream_status === null && response.status === 500;
}

export function isCompletedSosChildResponse(
  response: IngestResponseLike,
): boolean {
  return response.ok || isRecognizedSosDependencyFailure(response);
}

export function describeSosDependencyFailure(
  response: IngestResponseLike,
): string {
  const payload = asObject(response.body);
  const failureKind = asBoundedString(payload?.upstream_failure_kind, 64) ??
    "unknown";
  const upstreamStatus = asInteger(payload?.upstream_status);
  if (upstreamStatus !== null) {
    return `UK-AIR SOS upstream unavailable: HTTP ${upstreamStatus}.`;
  }
  return `UK-AIR SOS upstream unavailable: ${failureKind} (connector HTTP ${response.status}).`;
}

export function buildSosCloudRunChildResult(
  response: IngestResponseLike,
  runStatus: string,
  runMessage: string,
): SosCloudRunChildResult | null {
  const payload = asObject(response.body);
  if (
    !payload || !Number.isInteger(response.status) || response.status < 100 ||
    response.status > 599
  ) {
    return null;
  }
  const result: SosCloudRunChildResult = {
    httpStatus: response.status,
    payload: compactPayload(payload, response.ok, runStatus, runMessage),
  };
  return isSosCloudRunChildResult(result) ? result : null;
}

export function buildSosCloudRunSkippedResult(
  reason: string,
  connectorId: number | null = null,
): SosCloudRunChildResult {
  const safeReason = asBoundedString(reason, 128) ?? "skipped";
  const payload: Record<string, unknown> = {
    ok: true,
    status: "skipped",
    run_status: "skipped",
    run_message: safeReason,
    reason: safeReason,
  };
  if (connectorId !== null && isNonNegativeInteger(connectorId)) {
    payload.connector_id = connectorId;
  }
  return { httpStatus: 200, payload };
}

export function isSosCloudRunChildResult(
  value: unknown,
): value is SosCloudRunChildResult {
  const result = asObject(value);
  const payload = asObject(result?.payload);
  return isHttpStatus(result?.httpStatus) && payload !== null &&
    isValidChildPayload(payload);
}

export function decideSosCloudRunServiceResult(
  childSucceeded: boolean,
  childCode: number | null,
  childResult: SosCloudRunChildResult | null,
  resultReadState: SosCloudRunResultReadState,
): SosCloudRunServiceResult {
  if (!childSucceeded) {
    return {
      httpStatus: 500,
      payload: { ok: false, code: childCode },
    };
  }
  if (childResult) {
    return {
      httpStatus: childResult.httpStatus,
      payload: childResult.payload,
    };
  }
  return {
    httpStatus: 500,
    payload: {
      ok: false,
      error: resultReadState === "invalid"
        ? "invalid_child_result"
        : "missing_child_result",
      code: childCode,
    },
  };
}
