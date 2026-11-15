import postgres from "npm:postgres@3.4.7";

export type CommunitiesCompactObservationRpcArgsV1 = {
  timeseries_ids: number[];
  observed_ats: string[];
  values: Array<number | null>;
  statuses?: Array<string | null>;
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
  const previousOperation = communitiesDatabaseOperationQueue;
  let releaseOperation: () => void = () => undefined;
  communitiesDatabaseOperationQueue = new Promise<void>((resolve) => {
    releaseOperation = resolve;
  });
  await previousOperation;

  let timeoutId: number | undefined;
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
    await closeCommunitiesDatabaseClient();
    // Preserve postgres.js SQLSTATE/code fields for the existing observation
    // writer classifier. Cross-transport fallback is deliberately absent.
    throw error;
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    releaseOperation();
  }
}
