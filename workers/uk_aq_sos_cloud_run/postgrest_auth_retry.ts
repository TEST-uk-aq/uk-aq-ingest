export type PostgrestResponse = {
  ok: boolean;
  status: number;
  text: string;
  data: unknown;
};

type RetryLogDetails = {
  operation: string;
  target: string;
  retry_number: number;
  next_attempt: number;
  max_attempts: number;
  delay_ms: number;
  response_status: number;
  postgrest_code: "PGRST303";
};

type TransientJwtFutureRetryOptions = {
  operation: string;
  target: string;
  logRetry: (details: RetryLogDetails) => void;
  sleep?: (delayMs: number) => Promise<void>;
};

export const TRANSIENT_JWT_FUTURE_RETRY_DELAYS_MS = [1000, 2000] as const;

function toObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

export function isTransientJwtIssuedAtFuture(
  response: PostgrestResponse,
): boolean {
  if (response.status !== 401) {
    return false;
  }
  const payload = toObject(response.data);
  const code = typeof payload?.code === "string" ? payload.code.trim() : "";
  const message = typeof payload?.message === "string"
    ? payload.message.trim().toLowerCase()
    : "";
  return code === "PGRST303" && message === "jwt issued at future";
}

export async function requestWithTransientJwtFutureRetry<
  T extends PostgrestResponse,
>(
  request: () => Promise<T>,
  options: TransientJwtFutureRetryOptions,
): Promise<T> {
  const sleep = options.sleep ??
    ((delayMs: number) =>
      new Promise((resolve) => setTimeout(resolve, delayMs)));
  const maxAttempts = TRANSIENT_JWT_FUTURE_RETRY_DELAYS_MS.length + 1;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const response = await request();
    const delayMs = TRANSIENT_JWT_FUTURE_RETRY_DELAYS_MS[attempt - 1];
    if (!isTransientJwtIssuedAtFuture(response) || delayMs === undefined) {
      return response;
    }

    options.logRetry({
      operation: options.operation,
      target: options.target,
      retry_number: attempt,
      next_attempt: attempt + 1,
      max_attempts: maxAttempts,
      delay_ms: delayMs,
      response_status: response.status,
      postgrest_code: "PGRST303",
    });
    await sleep(delayMs);
  }

  throw new Error("Transient JWT-future retry loop exhausted unexpectedly.");
}
