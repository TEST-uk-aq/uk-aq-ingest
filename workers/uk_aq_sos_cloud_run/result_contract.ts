type IngestResponseLike = {
  ok: boolean;
  status: number;
  body: unknown;
};

export type SosCloudRunChildResult = {
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
  return (failureKind === "request_timeout" ||
    failureKind === "runtime_deadline") &&
    payload.upstream_status === null && response.status === 503;
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
  return {
    httpStatus: response.status,
    payload: compactPayload(payload, response.ok, runStatus, runMessage),
  };
}

export function isSosCloudRunChildResult(
  value: unknown,
): value is SosCloudRunChildResult {
  const result = asObject(value);
  const httpStatus = asInteger(result?.httpStatus);
  return httpStatus !== null && httpStatus >= 100 && httpStatus <= 599 &&
    asObject(result?.payload) !== null;
}
