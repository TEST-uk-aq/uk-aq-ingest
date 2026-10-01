import postgres from "npm:postgres@3.4.7";

export type SosCompactObservationRpcArgsV2 = {
  timeseries_ids: number[];
  observed_ats: string[];
  values: Array<number | null>;
  acquisition_method: string;
  statuses?: Array<string | null>;
};

export type SosCompactLatestValueRpcArgs = {
  timeseries_ids: number[];
  last_values: number[];
  last_value_ats: string[];
};

export type SosObservationUpsertRow = {
  observations_upserted: number;
};

export type SosLatestValueUpdateRow = {
  timeseries_updated: number;
};

const DATABASE_URL_ENV = "UK_AQ_GCP_CLOUD_RUN_DATABASE_URL";
const QUERY_TIMEOUT_MS = 30_000;

let sosDatabaseClient: ReturnType<typeof postgres> | null = null;
let sosDatabaseClosePromise: Promise<void> | null = null;
let sosDatabaseOperationQueue: Promise<void> = Promise.resolve();

function getSosDatabaseClient(): ReturnType<typeof postgres> {
  if (sosDatabaseClient) {
    return sosDatabaseClient;
  }

  const databaseUrl = (Deno.env.get(DATABASE_URL_ENV) ?? "").trim();
  if (!databaseUrl) {
    throw new Error(
      `Missing required environment variable: ${DATABASE_URL_ENV}`,
    );
  }

  sosDatabaseClient = postgres(databaseUrl, {
    max: 1,
    prepare: false,
    ssl: "require",
    connect_timeout: 10,
    idle_timeout: 5,
    max_lifetime: 300,
    fetch_types: false,
    onnotice: () => undefined,
    connection: {
      application_name: "uk_aq_sos_ingest",
    },
  });
  return sosDatabaseClient;
}

async function closeSosDatabaseClient(): Promise<void> {
  if (sosDatabaseClosePromise) {
    await sosDatabaseClosePromise;
    return;
  }

  const client = sosDatabaseClient;
  sosDatabaseClient = null;
  if (!client) {
    return;
  }

  sosDatabaseClosePromise = client.end({ timeout: 1 }).catch(() => {
    // The statement result may be uncertain. Do not expose connection details
    // while discarding this client; a later same-transport call may reconnect.
  }).finally(() => {
    sosDatabaseClosePromise = null;
  });
  await sosDatabaseClosePromise;
}

async function runSosDatabaseOperation<T>(
  operation: (sql: ReturnType<typeof postgres>) => Promise<T>,
): Promise<T> {
  const previousOperation = sosDatabaseOperationQueue;
  let releaseOperation: () => void = () => undefined;
  sosDatabaseOperationQueue = new Promise<void>((resolve) => {
    releaseOperation = resolve;
  });
  await previousOperation;

  let timeoutId: number | undefined;
  try {
    const query = operation(getSosDatabaseClient());
    const timeout = new Promise<never>((_resolve, reject) => {
      timeoutId = setTimeout(
        () => reject(new Error("SOS database request timed out.")),
        QUERY_TIMEOUT_MS,
      );
    });
    return await Promise.race([query, timeout]);
  } catch (error) {
    await closeSosDatabaseClient();
    // Preserve postgres.js SQLSTATE/code data for the existing observation
    // writer classifier. Cross-transport fallback is deliberately absent.
    throw error;
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
    releaseOperation();
  }
}

function requiredInteger(value: unknown, operation: string): number {
  if (
    (typeof value !== "number" && typeof value !== "string") ||
    (typeof value === "string" && !value.trim())
  ) {
    throw new Error(`SOS database ${operation} returned no usable row count.`);
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed)) {
    throw new Error(`SOS database ${operation} returned no usable row count.`);
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

export async function recordSosStationAttemptsViaDatabase(
  stationIds: number[],
  attemptedAt: string,
): Promise<number> {
  return await runSosDatabaseOperation(async (sql) => {
    const rows = await sql<Array<{ affected_rows: number }>>`
      select uk_aq_public.uk_aq_rpc_sos_record_station_attempts_v1(
        ${postgresArrayLiteral(stationIds)}::bigint[],
        ${attemptedAt}::timestamptz
      ) as affected_rows
    `;
    if (rows.length !== 1) {
      throw new Error(
        "SOS database station-attempt write returned no usable row count.",
      );
    }
    return requiredInteger(rows[0].affected_rows, "station-attempt write");
  });
}

export async function upsertSosObservationsViaDatabase(
  args: SosCompactObservationRpcArgsV2,
): Promise<SosObservationUpsertRow[]> {
  return await runSosDatabaseOperation(async (sql) => {
    const statuses = args.statuses === undefined
      ? null
      : postgresArrayLiteral(args.statuses);
    const rows = await sql<Array<{ observations_upserted: number }>>`
      select observations_upserted
      from uk_aq_public.uk_aq_rpc_observations_compact_upsert_v2(
        ${postgresArrayLiteral(args.timeseries_ids)}::integer[],
        ${postgresArrayLiteral(args.observed_ats)}::timestamptz[],
        ${postgresArrayLiteral(args.values)}::double precision[],
        ${args.acquisition_method}::text,
        ${statuses}::text[]
      )
    `;
    return rows.map((row) => ({
      observations_upserted: requiredInteger(
        row.observations_upserted,
        "observation upsert",
      ),
    }));
  });
}

export async function updateSosLatestValuesViaDatabase(
  args: SosCompactLatestValueRpcArgs,
): Promise<SosLatestValueUpdateRow[]> {
  return await runSosDatabaseOperation(async (sql) => {
    const rows = await sql<Array<{ timeseries_updated: number }>>`
      select timeseries_updated
      from uk_aq_public.uk_aq_rpc_timeseries_last_values_compact_update_v1(
        ${postgresArrayLiteral(args.timeseries_ids)}::integer[],
        ${postgresArrayLiteral(args.last_values)}::double precision[],
        ${postgresArrayLiteral(args.last_value_ats)}::timestamptz[]
      )
    `;
    return rows.map((row) => ({
      timeseries_updated: requiredInteger(
        row.timeseries_updated,
        "latest-value update",
      ),
    }));
  });
}
