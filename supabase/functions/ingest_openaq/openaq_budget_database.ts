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
  // Null when no current-minute row existed before the first reservation.
  minute_used_before: number | null;
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

type DatabaseClient = ReturnType<typeof postgres>;

let databaseClient: DatabaseClient | null = null;
let budgetReservationFailed = false;
let databaseOperationQueue: Promise<void> = Promise.resolve();

function getDatabaseClient(): DatabaseClient {
  if (databaseClient) {
    return databaseClient;
  }

  const databaseUrl = (Deno.env.get(DATABASE_URL_ENV) ?? "").trim();
  if (!databaseUrl) {
    throw new Error(
      `Missing required environment variable: ${DATABASE_URL_ENV}`,
    );
  }

  databaseClient = postgres(databaseUrl, {
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
  return databaseClient;
}

async function closeFailedDatabaseClient(): Promise<void> {
  const client = databaseClient;
  databaseClient = null;
  if (client) {
    await client.end({ timeout: 1 }).catch(() => {
      // Keep connection details out of the operation error.
    });
  }
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
  if (value === null || value === undefined || value === "") {
    return null;
  }
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

function boundedDatabaseError(message: string, error: unknown): Error {
  const rawCode = error && typeof error === "object" && "code" in error
    ? String((error as { code?: unknown }).code ?? "")
    : "";
  const code = /^[A-Z0-9_]{2,32}$/.test(rawCode) ? rawCode : null;
  return new Error(code ? `${message} (${code})` : message);
}

function runDatabaseOperation<T>(
  kind: "budget reservation" | "timeseries station read",
  operation: (sql: DatabaseClient) => Promise<T>,
): Promise<T> {
  const result = databaseOperationQueue.then(async () => {
    const isReservation = kind === "budget reservation";
    const failureMessage = isReservation
      ? "OpenAQ shared budget database reservation failed"
      : "OpenAQ timeseries station database read failed";
    // An uncertain reservation stays latched for this child process even if a
    // later read reconnects to finish processing observations already fetched.
    if (isReservation && budgetReservationFailed) {
      throw new Error(failureMessage);
    }

    let timeoutId: number | undefined;
    try {
      const sql = getDatabaseClient();
      const timeout = new Promise<never>((_resolve, reject) => {
        timeoutId = setTimeout(
          () => reject(new Error(`${failureMessage}: query timed out`)),
          QUERY_TIMEOUT_MS,
        );
      });
      return await Promise.race([operation(sql), timeout]);
    } catch (error) {
      if (isReservation) {
        budgetReservationFailed = true;
      }
      await closeFailedDatabaseClient();
      throw boundedDatabaseError(failureMessage, error);
    } finally {
      if (timeoutId !== undefined) {
        clearTimeout(timeoutId);
      }
    }
  });
  databaseOperationQueue = result.then(() => undefined, () => undefined);
  return result;
}

export function reserveOpenaqBudgetViaDatabase(
  args: OpenAQBudgetDatabaseArgs,
): Promise<OpenAQBudgetDatabaseRow[]> {
  return runDatabaseOperation("budget reservation", async (sql) => {
    const rows = await sql<Record<string, unknown>[]>`
      select *
      from uk_aq_public.uk_aq_rpc_openaq_token_budget_reserve(
        ${args.pBudgetKey}::text,
        ${args.pTokens}::integer,
        ${args.pMinuteLimit}::integer,
        ${args.pHourLimit}::integer,
        ${args.pCaller}::text
      )
    `;
    if (rows.length !== 1) {
      throw new Error("OpenAQ shared budget database returned no usable row");
    }
    const row = normalizeBudgetRow(rows[0]);
    if (!row) {
      throw new Error("OpenAQ shared budget database returned no usable row");
    }
    return [row];
  });
}

export function readTimeseriesStationIdsViaDatabase(
  timeseriesIds: number[],
): Promise<Array<{ id: number; station_id: number | null }>> {
  return runDatabaseOperation("timeseries station read", async (sql) => {
    // Bind the integer array as text since type discovery is disabled.
    const ids = `{${timeseriesIds.join(",")}}`;
    const rows = await sql<Array<{ id: number; station_id: string | null }>>`
      select id, station_id
      from uk_aq_public.uk_aq_rpc_timeseries_station_ids_read_v1(
        ${ids}::integer[]
      )
    `;
    // postgres.js returns bigint as text. Preserve the PostgREST numbers and nulls.
    return rows.map(({ id, station_id }) => ({
      id,
      station_id: station_id === null ? null : Number(station_id),
    }));
  });
}
