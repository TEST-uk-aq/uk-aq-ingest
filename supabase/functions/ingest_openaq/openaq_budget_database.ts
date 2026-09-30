import postgres from "npm:postgres@3.4.7";

export type OpenAQBudgetDatabaseArgs = {
  pBudgetKey: string;
  pTokens: number;
  pMinuteLimit: number;
  pHourLimit: number;
  pCaller: string;
};

export type OpenAQBudgetDatabaseRow = {
  granted: boolean;
  reason: string;
  budget_key: string;
  caller: string;
  requested_tokens: number;
  minute_bucket: string;
  minute_limit: number;
  minute_used_before: number;
  minute_used_after: number;
  minute_remaining: number;
  minute_reset_at: string;
  hour_window_start: string;
  hour_limit: number;
  hour_used_before: number;
  hour_used_after: number;
  hour_remaining: number;
  hour_reset_at: string;
  retry_after_seconds: number;
};

const DATABASE_URL_ENV = "UK_AQ_GCP_CLOUD_RUN_DATABASE_URL";
const QUERY_TIMEOUT_MS = 30_000;

let budgetDatabaseClient: ReturnType<typeof postgres> | null = null;
let budgetDatabaseClientFailed = false;
let budgetDatabaseClosePromise: Promise<void> | null = null;
let budgetReservationQueue: Promise<void> = Promise.resolve();

function getBudgetDatabaseClient(): ReturnType<typeof postgres> {
  if (budgetDatabaseClientFailed) {
    throw new Error("OpenAQ shared budget database transport is unavailable");
  }
  if (budgetDatabaseClient) {
    return budgetDatabaseClient;
  }

  const databaseUrl = (Deno.env.get(DATABASE_URL_ENV) ?? "").trim();
  if (!databaseUrl) {
    throw new Error(
      `Missing required environment variable: ${DATABASE_URL_ENV}`,
    );
  }

  budgetDatabaseClient = postgres(databaseUrl, {
    max: 1,
    prepare: false,
    ssl: "require",
    connect_timeout: 10,
    idle_timeout: 5,
    max_lifetime: 300,
    fetch_types: false,
    onnotice: () => undefined,
    connection: {
      application_name: "uk_aq_openaq_budget",
    },
  });
  return budgetDatabaseClient;
}

async function closeFailedBudgetDatabaseClient(): Promise<void> {
  budgetDatabaseClientFailed = true;
  if (budgetDatabaseClosePromise) {
    await budgetDatabaseClosePromise;
    return;
  }

  const client = budgetDatabaseClient;
  budgetDatabaseClient = null;
  if (!client) {
    return;
  }

  budgetDatabaseClosePromise = client.end({ timeout: 1 }).catch(() => {
    // The reservation result is already uncertain. Do not retry or expose
    // connection details while forcing this child-process client closed.
  });
  await budgetDatabaseClosePromise;
}

function timestampString(value: unknown): string | null {
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return value.toISOString();
  }
  if (typeof value === "string" && value.trim()) {
    return value;
  }
  return null;
}

function finiteInteger(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed)) {
    return null;
  }
  return parsed;
}

function normalizeBudgetRow(
  raw: Record<string, unknown>,
): OpenAQBudgetDatabaseRow | null {
  const requestedTokens = finiteInteger(raw.requested_tokens);
  const minuteLimit = finiteInteger(raw.minute_limit);
  const minuteUsedBefore = finiteInteger(raw.minute_used_before);
  const minuteUsedAfter = finiteInteger(raw.minute_used_after);
  const minuteRemaining = finiteInteger(raw.minute_remaining);
  const hourLimit = finiteInteger(raw.hour_limit);
  const hourUsedBefore = finiteInteger(raw.hour_used_before);
  const hourUsedAfter = finiteInteger(raw.hour_used_after);
  const hourRemaining = finiteInteger(raw.hour_remaining);
  const retryAfterSeconds = finiteInteger(raw.retry_after_seconds);
  const minuteBucket = timestampString(raw.minute_bucket);
  const minuteResetAt = timestampString(raw.minute_reset_at);
  const hourWindowStart = timestampString(raw.hour_window_start);
  const hourResetAt = timestampString(raw.hour_reset_at);

  if (
    typeof raw.granted !== "boolean" ||
    typeof raw.reason !== "string" ||
    typeof raw.budget_key !== "string" ||
    typeof raw.caller !== "string" ||
    requestedTokens === null ||
    minuteLimit === null ||
    minuteUsedBefore === null ||
    minuteUsedAfter === null ||
    minuteRemaining === null ||
    hourLimit === null ||
    hourUsedBefore === null ||
    hourUsedAfter === null ||
    hourRemaining === null ||
    retryAfterSeconds === null ||
    minuteBucket === null ||
    minuteResetAt === null ||
    hourWindowStart === null ||
    hourResetAt === null
  ) {
    return null;
  }

  return {
    granted: raw.granted,
    reason: raw.reason,
    budget_key: raw.budget_key,
    caller: raw.caller,
    requested_tokens: requestedTokens,
    minute_bucket: minuteBucket,
    minute_limit: minuteLimit,
    minute_used_before: minuteUsedBefore,
    minute_used_after: minuteUsedAfter,
    minute_remaining: minuteRemaining,
    minute_reset_at: minuteResetAt,
    hour_window_start: hourWindowStart,
    hour_limit: hourLimit,
    hour_used_before: hourUsedBefore,
    hour_used_after: hourUsedAfter,
    hour_remaining: hourRemaining,
    hour_reset_at: hourResetAt,
    retry_after_seconds: retryAfterSeconds,
  };
}

function boundedDatabaseError(error: unknown): Error {
  const rawCode = error && typeof error === "object" && "code" in error
    ? String((error as { code?: unknown }).code ?? "")
    : "";
  const code = /^[A-Z0-9_]{2,32}$/.test(rawCode) ? rawCode : null;
  return new Error(
    code
      ? `OpenAQ shared budget database reservation failed (${code})`
      : "OpenAQ shared budget database reservation failed",
  );
}

export async function reserveOpenaqBudgetViaDatabase(
  args: OpenAQBudgetDatabaseArgs,
): Promise<OpenAQBudgetDatabaseRow[]> {
  const previousReservation = budgetReservationQueue;
  let releaseReservation: () => void = () => undefined;
  budgetReservationQueue = new Promise<void>((resolve) => {
    releaseReservation = resolve;
  });
  await previousReservation;

  let timeoutId: number | undefined;

  try {
    const sql = getBudgetDatabaseClient();
    const query = sql<Record<string, unknown>[]>`
      select *
      from uk_aq_public.uk_aq_rpc_openaq_token_budget_reserve(
        ${args.pBudgetKey}::text,
        ${args.pTokens}::integer,
        ${args.pMinuteLimit}::integer,
        ${args.pHourLimit}::integer,
        ${args.pCaller}::text
      )
    `;
    const timeout = new Promise<never>((_resolve, reject) => {
      timeoutId = setTimeout(
        () =>
          reject(new Error("OpenAQ shared budget database query timed out")),
        QUERY_TIMEOUT_MS,
      );
    });
    const rows = await Promise.race([query, timeout]);

    if (rows.length !== 1) {
      throw new Error("OpenAQ shared budget database returned no usable row");
    }
    const row = normalizeBudgetRow(rows[0]);
    if (!row) {
      throw new Error("OpenAQ shared budget database returned no usable row");
    }
    return [row];
  } catch (error) {
    await closeFailedBudgetDatabaseClient();
    throw boundedDatabaseError(error);
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    releaseReservation();
  }
}
