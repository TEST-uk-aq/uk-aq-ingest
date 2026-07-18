export type SosFetchFailureKind =
  | "http"
  | "request_timeout"
  | "runtime_deadline"
  | "network"
  | "unknown";

export type SosFetchFailureInit = {
  kind: SosFetchFailureKind;
  message: string;
  upstreamStatus?: number | null;
  retryable?: boolean;
};

export class SosFetchFailure extends Error {
  readonly kind: SosFetchFailureKind;
  readonly upstreamStatus: number | null;
  readonly retryable: boolean;

  constructor(init: SosFetchFailureInit) {
    super(boundMessage(init.message));
    this.name = "SosFetchFailure";
    this.kind = init.kind;
    this.upstreamStatus = init.upstreamStatus ?? null;
    this.retryable = init.retryable ?? false;
  }
}

export type RuntimeDeadlineFailureSummary = {
  count: number;
  timeseriesSample: number[];
};

export function boundMessage(value: unknown, maxLength = 500): string {
  const message = value instanceof Error ? value.message : String(value);
  const trimmed = message.trim() || "Unknown SOS fetch failure.";
  return trimmed.length <= maxLength
    ? trimmed
    : `${trimmed.slice(0, maxLength - 3)}...`;
}

export function asSosFetchFailure(error: unknown): SosFetchFailure {
  if (error instanceof SosFetchFailure) {
    return error;
  }
  return new SosFetchFailure({
    kind: "unknown",
    message: boundMessage(error),
  });
}

export function connectorHttpStatusForProbe(failure: SosFetchFailure): number {
  if (failure.upstreamStatus !== null) {
    return failure.upstreamStatus;
  }
  if (
    failure.kind === "request_timeout" || failure.kind === "runtime_deadline"
  ) {
    return 503;
  }
  return 500;
}

export function isRuntimeDeadlineFailure(failure: SosFetchFailure): boolean {
  return failure.kind === "runtime_deadline";
}

export function addRuntimeDeadlineFailure(
  summary: RuntimeDeadlineFailureSummary,
  timeseriesId: number,
  sampleLimit = 10,
): void {
  summary.count += 1;
  if (summary.timeseriesSample.length < sampleLimit) {
    summary.timeseriesSample.push(timeseriesId);
  }
}
