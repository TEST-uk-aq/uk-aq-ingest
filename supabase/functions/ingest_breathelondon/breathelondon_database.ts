import postgres from "npm:postgres@3.4.7";
import { performanceNow, recordDatabaseOperation } from "./performance_diagnostics.ts";

export type CommunitiesCompactObservationRpcArgsV1 = {
  timeseries_ids: number[];
  observed_ats: string[];
  values: Array<number | null>;
  statuses?: Array<string | null>;
};

export type CommunitiesLastValuesRpcArgsV1 = {
  timeseries_ids: number[];
  last_values: number[];
  last_value_ats: string[];
};

const DATABASE_URL_ENV = "UK_AQ_GCP_CLOUD_RUN_DATABASE_URL";
const QUERY_TIMEOUT_MS = 30_000;
const STATEMENT_TIMEOUT_MS = 29_000;

let communitiesDatabaseClient: ReturnType<typeof postgres> | null = null;
let communitiesDatabaseClosePromise: Promise<void> | null = null;
let communitiesDatabaseOperationQueue: Promise<void> = Promise.resolve();

function requiredDatabaseUrl(): string {
  const databaseUrl = (Deno.env.get(DATABASE_URL_ENV) ?? "").trim();
  if (!databaseUrl) {
    throw new Error(
      `Missing required environment variable: ${DATABASE_URL_ENV}`,
    );
  }

  let parsed: URL;
  try {
    parsed = new URL(databaseUrl);
  } catch {
    throw new Error(`${DATABASE_URL_ENV} is not a valid PostgreSQL URL.`);
  }
  if (
    !["postgres:", "postgresql:"].includes(parsed.protocol) ||
    parsed.port !== "6543"
  ) {
    throw new Error(
      `${DATABASE_URL_ENV} must use the Shared Pooler transaction port 6543.`,
    );
  }
  return databaseUrl;
}

function getCommunitiesDatabaseClient(): ReturnType<typeof postgres> {
  if (communitiesDatabaseClient) {
    return communitiesDatabaseClient;
  }

  communitiesDatabaseClient = postgres(requiredDatabaseUrl(), {
    max: 1,
    prepare: false,
    ssl: "require",
    connect_timeout: 10,
    idle_timeout: 5,
    max_lifetime: 300,
    fetch_types: false,
    onnotice: () => undefined,
    connection: {
      application_name: "uk_aq_blondon_communities_ingest",
    },
  });
  return communitiesDatabaseClient;
}

async function closeCommunitiesDatabaseClient(): Promise<void> {
  if (communitiesDatabaseClosePromise) {
    await communitiesDatabaseClosePromise;
    return;
  }

  const client = communitiesDatabaseClient;
  communitiesDatabaseClient = null;
  if (!client) {
    return;
  }

  communitiesDatabaseClosePromise = client.end({ timeout: 1 }).catch(() => {
    // The statement result may be uncertain. Discard this client without
    // exposing connection details; a later same-transport retry may reconnect.
  }).finally(() => {
    communitiesDatabaseClosePromise = null;
  });
  await communitiesDatabaseClosePromise;
}

function requiredInteger(value: unknown): number {
  if (
    (typeof value !== "number" && typeof value !== "string") ||
    (typeof value === "string" && !value.trim())
  ) {
    throw new Error(
      "Communities database observation upsert returned no usable row count.",
    );
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) {
    throw new Error(
      "Communities database observation upsert returned no usable row count.",
    );
  }
  return parsed;
}

function postgresArrayLiteral(
  values: ReadonlyArray<string | number | null>,
): string {
  return `{${
    values.map((value) => {
      if (
        value === null ||
        (typeof value === "number" && !Number.isFinite(value))
      ) {
        return "NULL";
      }
      const escaped = String(value)
        .replaceAll("\\", "\\\\")
        .replaceAll('"', '\\"');
      return `"${escaped}"`;
    }).join(",")
  }}`;
}

export async function upsertCommunitiesObservationsViaDatabase(
  args: CommunitiesCompactObservationRpcArgsV1,
): Promise<number> {
  const diagnosticStarted = performanceNow();
  let queueMs = 0;
  let failed = false;
  const previousOperation = communitiesDatabaseOperationQueue;
  let releaseOperation: () => void = () => undefined;
  communitiesDatabaseOperationQueue = new Promise<void>((resolve) => {
    releaseOperation = resolve;
  });
  await previousOperation;
  queueMs = performanceNow() - diagnosticStarted;

  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    const sql = getCommunitiesDatabaseClient();
    const query = sql.begin(async (transaction) => {
      await transaction.unsafe(
        `set local statement_timeout = '${STATEMENT_TIMEOUT_MS}ms'`,
      );
      const statuses = args.statuses === undefined
        ? null
        : postgresArrayLiteral(args.statuses);
      return await transaction<Array<{ observations_upserted: number }>>`
        select observations_upserted
        from uk_aq_public.uk_aq_rpc_observations_compact_upsert_v1(
          ${postgresArrayLiteral(args.timeseries_ids)}::integer[],
          ${postgresArrayLiteral(args.observed_ats)}::timestamptz[],
          ${postgresArrayLiteral(args.values)}::double precision[],
          ${statuses}::text[]
        )
      `;
    });
    const timeout = new Promise<never>((_resolve, reject) => {
      timeoutId = setTimeout(
        () => reject(new Error("Communities database request timed out.")),
        QUERY_TIMEOUT_MS,
      );
    });
    const rows = await Promise.race([query, timeout]);
    if (rows.length !== 1) {
      throw new Error(
        "Communities database observation upsert returned no usable row count.",
      );
    }
    return requiredInteger(rows[0].observations_upserted);
  } catch (error) {
    failed = true;
    await closeCommunitiesDatabaseClient();
    // Preserve postgres.js SQLSTATE/code fields for the existing observation
    // writer classifier. Cross-transport fallback is deliberately absent.
    throw error;
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    releaseOperation();
    recordDatabaseOperation("observations", diagnosticStarted, queueMs, failed);
  }
}

export async function updateCommunitiesLastValuesViaDatabase(
  args: CommunitiesLastValuesRpcArgsV1,
): Promise<number> {
  const diagnosticStarted = performanceNow();
  let queueMs = 0;
  let failed = false;
  const previousOperation = communitiesDatabaseOperationQueue;
  let releaseOperation: () => void = () => undefined;
  communitiesDatabaseOperationQueue = new Promise<void>((resolve) => {
    releaseOperation = resolve;
  });
  await previousOperation;
  queueMs = performanceNow() - diagnosticStarted;

  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    const sql = getCommunitiesDatabaseClient();
    const query = sql.begin(async (transaction) => {
      await transaction.unsafe(
        `set local statement_timeout = '${STATEMENT_TIMEOUT_MS}ms'`,
      );
      return await transaction<Array<{ timeseries_updated: number }>>`
        select timeseries_updated
        from uk_aq_public.uk_aq_rpc_timeseries_last_values_compact_update_v1(
          ${postgresArrayLiteral(args.timeseries_ids)}::integer[],
          ${postgresArrayLiteral(args.last_values)}::double precision[],
          ${postgresArrayLiteral(args.last_value_ats)}::timestamptz[]
        )
      `;
    });
    const timeout = new Promise<never>((_resolve, reject) => {
      timeoutId = setTimeout(
        () => reject(new Error(
          "Communities latest-value database request timed out; commit outcome is uncertain.",
        )),
        QUERY_TIMEOUT_MS,
      );
    });
    const rows = await Promise.race([query, timeout]);
    const rawCount: unknown = rows[0]?.timeseries_updated;
    const count = Number(rawCount);
    if (
      rows.length !== 1 || rawCount === null || rawCount === undefined ||
      (typeof rawCount === "string" && !rawCount.trim()) ||
      !Number.isInteger(count) || count < 0
    ) {
      throw new Error(
        "Communities latest-value database write returned an unexpected row count.",
      );
    }
    return count;
  } catch (error) {
    failed = true;
    await closeCommunitiesDatabaseClient();
    // Preserve postgres.js SQLSTATE/code; an uncertain commit is never retried
    // through PostgREST.
    throw error;
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    releaseOperation();
    recordDatabaseOperation("latest_values", diagnosticStarted, queueMs, failed);
  }
}

type CheckpointOperation = "station" | "timeseries";

async function upsertCommunitiesCheckpointViaDatabase(
  operation: CheckpointOperation,
  payload: Record<string, unknown> | Record<string, unknown>[],
  expectedCount: number,
): Promise<number> {
  const diagnosticStarted = performanceNow();
  let queueMs = 0;
  let failed = false;
  // Share the observation client's queue, but commit each checkpoint invocation
  // in its own transaction. A later failed write cannot undo an earlier one.
  const previousOperation = communitiesDatabaseOperationQueue;
  let releaseOperation: () => void = () => undefined;
  communitiesDatabaseOperationQueue = new Promise<void>((resolve) => {
    releaseOperation = resolve;
  });
  await previousOperation;
  queueMs = performanceNow() - diagnosticStarted;

  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    const sql = getCommunitiesDatabaseClient();
    const query = sql.begin(async (transaction) => {
      await transaction.unsafe(
        `set local statement_timeout = '${STATEMENT_TIMEOUT_MS}ms'`,
      );
      const jsonPayload = payload as Parameters<typeof transaction.json>[0];
      if (operation === "station") {
        return await transaction<Array<{ checkpoint_rows_upserted: number }>>`
          select uk_aq_public.uk_aq_rpc_blondon_communities_station_checkpoint_upsert_v1(
            ${transaction.json(jsonPayload)}::jsonb
          ) as checkpoint_rows_upserted
        `;
      }
      return await transaction<Array<{ checkpoint_rows_upserted: number }>>`
        select uk_aq_public.uk_aq_rpc_blondon_communities_timeseries_checkpoint_upsert_v1(
          ${transaction.json(jsonPayload)}::jsonb
        ) as checkpoint_rows_upserted
      `;
    });
    const timeout = new Promise<never>((_resolve, reject) => {
      timeoutId = setTimeout(
        () =>
          reject(
            new Error(
              `Communities ${operation} checkpoint database request timed out; commit outcome is uncertain.`,
            ),
          ),
        QUERY_TIMEOUT_MS,
      );
    });
    const rows = await Promise.race([query, timeout]);
    const count = Number(rows[0]?.checkpoint_rows_upserted);
    if (
      rows.length !== 1 || !Number.isInteger(count) || count !== expectedCount
    ) {
      throw new Error(
        `Communities ${operation} checkpoint database write returned an unexpected row count.`,
      );
    }
    return count;
  } catch (error) {
    failed = true;
    await closeCommunitiesDatabaseClient();
    // Retain the original postgres.js SQLSTATE/code and never retry through
    // PostgREST, including when the commit outcome is uncertain.
    throw error;
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    releaseOperation();
    recordDatabaseOperation(operation === "station" ? "station_checkpoints" : "species_checkpoints", diagnosticStarted, queueMs, failed);
  }
}

export async function upsertCommunitiesStationCheckpointsViaDatabase(
  rows: Record<string, unknown>[],
): Promise<number> {
  return await upsertCommunitiesCheckpointViaDatabase(
    "station",
    rows,
    rows.length,
  );
}

export async function upsertCommunitiesSpeciesCheckpointViaDatabase(
  row: Record<string, unknown>,
): Promise<void> {
  await upsertCommunitiesCheckpointViaDatabase("timeseries", row, 1);
}
