import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.8";

type RpcError = { message: string };

type RpcResult<T> = {
  data: T | null;
  error: RpcError | null;
};

export type MainRpcCaller = <T>(
  fn: string,
  args?: Record<string, unknown>,
) => Promise<RpcResult<T>>;

export type ObservsObservationRow = {
  connector_id: number;
  timeseries_id: number;
  observed_at: string;
  value: number | null;
  value_float8_hex?: string | null;
  status: string | null;
};

type ObservsOutboxClaimRow = {
  id: string;
  payload: unknown;
  attempts: number;
};

type ObservsOutboxResolveRow = {
  rows_resolved: number;
};

type ObservsOutboxEnqueueRow = {
  rows_enqueued: number;
};

type ObservsReceiptUpsertRow = {
  rows_upserted: number;
};

type ObservsUpsertRow = {
  observations_upserted: number;
};

export type ObservsSyncReceiptRow = {
  connector_id: number;
  timeseries_id: number;
  observed_day: string;
};

export type ObservsOutboxFlushStats = {
  claimed: number;
  delivered: number;
  failed: number;
  receipts_upserted: number;
  rows_resolved: number;
};

export type ObservsWriteStats = {
  written: number;
  receipts_upserted: number;
  enqueued: number;
};

export type ObservsOutboxFlushOptions = {
  claim_batch_limit?: number;
};

type ObservsWriteMode = "direct" | "outbox_only" | "pubsub_only";

const OBS_AQIDB_SUPABASE_URL = (
  Deno.env.get("OBS_AQIDB_SUPABASE_URL") ?? ""
).trim();

const OBS_AQIDB_SECRET_KEY = (
  Deno.env.get("OBS_AQIDB_SECRET_KEY") ?? ""
).trim();

const OBS_AQIDB_RPC_SCHEMA = normalizeObservsRpcSchema(
  (Deno.env.get("OBS_AQIDB_RPC_SCHEMA") ?? "uk_aq_public").trim(),
);

function normalizeObservsRpcSchema(raw: string): string {
  const normalized = raw.trim().toLowerCase();
  // Observs RPC functions live in uk_aq_public.
  if (
    !normalized || normalized === "uk_aq_observs" || normalized === "public"
  ) {
    return "uk_aq_public";
  }
  return raw.trim();
}

const OBSERVS_UPSERT_RPC = (Deno.env.get("OBSERVS_UPSERT_RPC") ||
  "uk_aq_rpc_observs_observations_compact_upsert_v1")
  .trim();

const OBSERVS_OUTBOX_FLUSH_LIMIT = parsePositiveInt(
  Deno.env.get("OBSERVS_OUTBOX_FLUSH_LIMIT"),
  40,
);

const OBSERVS_UPSERT_CHUNK_SIZE = parsePositiveInt(
  Deno.env.get("OBSERVS_UPSERT_CHUNK_SIZE"),
  5000,
);

const OBSERVS_UPSERT_RPC_RETRIES = parsePositiveInt(
  Deno.env.get("OBSERVS_UPSERT_RPC_RETRIES"),
  3,
);

const OBSERVS_UPSERT_RETRY_BASE_MS = parsePositiveInt(
  Deno.env.get("OBSERVS_UPSERT_RETRY_BASE_MS"),
  1000,
);

const OBSERVS_UPSERT_TIMEOUT_SPLIT_MIN_ROWS = parsePositiveInt(
  Deno.env.get("OBSERVS_UPSERT_TIMEOUT_SPLIT_MIN_ROWS"),
  32,
);

const OBSERVS_UPSERT_TIMEOUT_SPLIT_MAX_DEPTH = parsePositiveInt(
  Deno.env.get("OBSERVS_UPSERT_TIMEOUT_SPLIT_MAX_DEPTH"),
  4,
);

const OBSERVS_WRITE_MODE = normalizeObservsWriteMode(
  Deno.env.get("OBSERVS_WRITE_MODE"),
);

const GCP_PROJECT_ID = (
  Deno.env.get("GCP_PROJECT_ID") ??
    Deno.env.get("GOOGLE_CLOUD_PROJECT") ??
    ""
).trim();

const GCP_OBSERVS_PUBSUB_TOPIC = (
  Deno.env.get("GCP_OBSERVS_PUBSUB_TOPIC") ??
    ""
).trim();

const OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE = parsePositiveInt(
  Deno.env.get("OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE"),
  500,
);

let observsClientCache: ReturnType<typeof createClient> | null = null;
let observsFetchOverride: typeof fetch | null = null;

export function configureObservsPostgrestFetch(
  fetchOverride: typeof fetch | null,
): void {
  observsFetchOverride = fetchOverride;
  observsClientCache = null;
}

function parsePositiveInt(raw: string | undefined, fallback: number): number {
  const value = Number(raw ?? "");
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.max(1, Math.trunc(value));
}

function normalizeObservsWriteMode(raw: string | undefined): ObservsWriteMode {
  const value = (raw ?? "").trim().toLowerCase();
  if (value === "direct") {
    return "direct";
  }
  if (value === "pubsub_only") {
    return "pubsub_only";
  }
  return "outbox_only";
}

function chunkRows<T>(rows: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let idx = 0; idx < rows.length; idx += chunkSize) {
    chunks.push(rows.slice(idx, idx + chunkSize));
  }
  return chunks;
}

function asFiniteNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function float64ToHex(value: number): string {
  const buffer = new ArrayBuffer(8);
  const view = new DataView(buffer);
  view.setFloat64(0, value, false);
  const bytes = new Uint8Array(buffer);
  return Array.from(bytes).map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function float64FromHex(value: unknown): number | null {
  if (typeof value !== "string") {
    return null;
  }
  const hex = value.trim().toLowerCase();
  if (!/^[0-9a-f]{16}$/.test(hex)) {
    return null;
  }
  const bytes = new Uint8Array(8);
  for (let i = 0; i < 8; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  const parsed = new DataView(bytes.buffer).getFloat64(0, false);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeObservsValue(
  value: unknown,
  valueFloat8Hex: unknown,
): { value: number | null; valueFloat8Hex: string | null } {
  const fromHex = float64FromHex(valueFloat8Hex);
  if (fromHex !== null) {
    return {
      value: fromHex,
      valueFloat8Hex: float64ToHex(fromHex),
    };
  }
  const numeric = asFiniteNumber(value);
  if (numeric === null) {
    return {
      value: null,
      valueFloat8Hex: null,
    };
  }
  return {
    value: numeric,
    valueFloat8Hex: float64ToHex(numeric),
  };
}

function asPositiveInt(value: unknown): number | null {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  const intValue = Math.trunc(parsed);
  return intValue > 0 ? intValue : null;
}

function toObservedAt(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    const parsed = new Date(trimmed);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString();
    }
  }
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

function normalizeStatus(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  const status = String(value).trim();
  return status ? status : null;
}

export function prepareObservsRows(
  observsRows: ObservsObservationRow[],
): ObservsObservationRow[] {
  if (!observsRows.length) {
    return [];
  }
  const dedup = new Map<string, ObservsObservationRow>();
  for (const row of observsRows) {
    const connectorId = asPositiveInt(row.connector_id);
    const timeseriesId = asPositiveInt(row.timeseries_id);
    const observedAt = toObservedAt(row.observed_at);
    if (connectorId === null || timeseriesId === null || !observedAt) {
      continue;
    }
    const key = `${connectorId}:${timeseriesId}:${observedAt}`;
    const normalizedValue = normalizeObservsValue(
      row.value,
      row.value_float8_hex,
    );
    dedup.set(key, {
      connector_id: connectorId,
      timeseries_id: timeseriesId,
      observed_at: observedAt,
      value: normalizedValue.value,
      value_float8_hex: normalizedValue.valueFloat8Hex,
      status: normalizeStatus(row.status),
    });
  }
  return Array.from(dedup.values());
}

function toObservedDay(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed.length >= 10) {
      return trimmed.slice(0, 10);
    }
  }
  const parsed = new Date(String(value));
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString().slice(0, 10);
}

function shortError(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.length > 400 ? `${message.slice(0, 397)}...` : message;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isObservsStatementTimeoutError(message: string): boolean {
  return /statement timeout|canceling statement due to statement timeout/i.test(
    message,
  );
}

function isRetryableObservsUpsertError(message: string): boolean {
  const normalized = message.toLowerCase();
  return (
    isObservsStatementTimeoutError(normalized) ||
    normalized.includes("deadlock detected") ||
    normalized.includes("could not serialize access due to") ||
    normalized.includes("connection terminated") ||
    normalized.includes("connection reset") ||
    normalized.includes("http 429") ||
    normalized.includes("http 500") ||
    normalized.includes("http 502") ||
    normalized.includes("http 503") ||
    normalized.includes("http 504")
  );
}

function pubsubTopicPath(): string {
  if (!GCP_OBSERVS_PUBSUB_TOPIC) {
    return "";
  }
  if (GCP_OBSERVS_PUBSUB_TOPIC.startsWith("projects/")) {
    return GCP_OBSERVS_PUBSUB_TOPIC;
  }
  if (!GCP_PROJECT_ID) {
    return "";
  }
  return `projects/${GCP_PROJECT_ID}/topics/${GCP_OBSERVS_PUBSUB_TOPIC}`;
}

function observsPubsubConfigured(): boolean {
  return Boolean(pubsubTopicPath());
}

async function fetchGoogleAccessToken(): Promise<string> {
  const response = await fetch(
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
    {
      headers: { "Metadata-Flavor": "Google" },
    },
  );
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(
      `Metadata token request failed (${response.status}): ${text}`,
    );
  }
  const payload = await response.json().catch(() => null);
  const token = typeof payload?.access_token === "string"
    ? payload.access_token.trim()
    : "";
  if (!token) {
    throw new Error("Metadata token response missing access_token");
  }
  return token;
}

type PubsubPublishResponse = {
  messageIds?: unknown;
};

async function publishObservsRowsToPubsub(
  preparedRows: ObservsObservationRow[],
): Promise<number> {
  if (!preparedRows.length) {
    return 0;
  }
  const topicPath = pubsubTopicPath();
  if (!topicPath) {
    throw new Error(
      "Observs Pub/Sub is not configured (missing GCP_OBSERVS_PUBSUB_TOPIC or GCP_PROJECT_ID).",
    );
  }

  const token = await fetchGoogleAccessToken();
  let published = 0;

  for (
    const chunk of chunkRows(preparedRows, OBSERVS_PUBSUB_PUBLISH_BATCH_SIZE)
  ) {
    const messages = chunk.map((row) => ({
      data: btoa(JSON.stringify(row)),
      attributes: {
        connector_id: String(row.connector_id),
        timeseries_id: String(row.timeseries_id),
        observed_at: row.observed_at,
      },
    }));

    const response = await fetch(
      `https://pubsub.googleapis.com/v1/${topicPath}:publish`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ messages }),
      },
    );

    const payload = await response.json().catch(() => null) as
      | PubsubPublishResponse
      | null;
    if (!response.ok) {
      const message = typeof payload === "object" && payload !== null
        ? JSON.stringify(payload)
        : `HTTP ${response.status}`;
      throw new Error(`Observs Pub/Sub publish failed: ${message}`);
    }
    const messageIds = payload?.messageIds;
    published += Array.isArray(messageIds) ? messageIds.length : chunk.length;
  }

  return published;
}

function countRowsFromPayload<T extends string>(
  payload: Array<Record<T, number>> | null,
  field: T,
  fallback: number,
): number {
  const value = Number(payload?.[0]?.[field] ?? fallback);
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return value;
}

function observsConfigured(): boolean {
  return Boolean(OBS_AQIDB_SUPABASE_URL && OBS_AQIDB_SECRET_KEY);
}

export function createSupabaseObservsClient(): ReturnType<typeof createClient> {
  if (!observsConfigured()) {
    throw new Error(
      "Observs client is not configured (missing OBS_AQIDB_SUPABASE_URL or OBS_AQIDB_SECRET_KEY)",
    );
  }
  if (!observsClientCache) {
    const schema = OBS_AQIDB_RPC_SCHEMA || "uk_aq_public";
    observsClientCache = createClient(
      OBS_AQIDB_SUPABASE_URL,
      OBS_AQIDB_SECRET_KEY,
      ({
        auth: { persistSession: false, autoRefreshToken: false },
        db: { schema: schema as never },
        global: {
          ...(observsFetchOverride ? { fetch: observsFetchOverride } : {}),
          headers: {
            "X-Client-Info": "uk-aq-ingest-observs-dualwrite",
          },
        },
      }) as never,
    );
  }
  return observsClientCache;
}

async function upsertObservsChunk(
  observs: ReturnType<typeof createClient>,
  chunk: ObservsObservationRow[],
): Promise<number> {
  const { data, error } = await observs.rpc(OBSERVS_UPSERT_RPC, {
    timeseries_ids: chunk.map((row) => row.timeseries_id),
    observed_ats: chunk.map((row) => row.observed_at),
    values: chunk.map((row) => row.value),
  });
  if (error) {
    throw new Error(error.message || "unknown_observs_upsert_error");
  }
  return countRowsFromPayload(
    (Array.isArray(data) ? data : null) as
      | Array<Record<"observations_upserted", number>>
      | null,
    "observations_upserted",
    chunk.length,
  );
}

async function upsertObservsChunkWithFallback(
  observs: ReturnType<typeof createClient>,
  chunk: ObservsObservationRow[],
  splitDepth = 0,
): Promise<number> {
  let lastMessage = "unknown_observs_upsert_error";

  for (let attempt = 1; attempt <= OBSERVS_UPSERT_RPC_RETRIES; attempt += 1) {
    try {
      return await upsertObservsChunk(observs, chunk);
    } catch (error) {
      lastMessage = shortError(error);
      if (
        attempt < OBSERVS_UPSERT_RPC_RETRIES &&
        isRetryableObservsUpsertError(lastMessage)
      ) {
        await sleep(Math.min(5000, OBSERVS_UPSERT_RETRY_BASE_MS * attempt));
        continue;
      }
      break;
    }
  }

  if (
    isObservsStatementTimeoutError(lastMessage) &&
    splitDepth < OBSERVS_UPSERT_TIMEOUT_SPLIT_MAX_DEPTH &&
    chunk.length >= OBSERVS_UPSERT_TIMEOUT_SPLIT_MIN_ROWS * 2
  ) {
    const midpoint = Math.floor(chunk.length / 2);
    const left = chunk.slice(0, midpoint);
    const right = chunk.slice(midpoint);
    if (!left.length || !right.length) {
      throw new Error(`Observs upsert failed: ${lastMessage}`);
    }
    const leftWritten = await upsertObservsChunkWithFallback(
      observs,
      left,
      splitDepth + 1,
    );
    const rightWritten = await upsertObservsChunkWithFallback(
      observs,
      right,
      splitDepth + 1,
    );
    return leftWritten + rightWritten;
  }

  throw new Error(`Observs upsert failed: ${lastMessage}`);
}

export async function observsUpsertObservations(
  observsRows: ObservsObservationRow[],
): Promise<number> {
  const preparedRows = prepareObservsRows(observsRows);
  if (!preparedRows.length) {
    return 0;
  }

  const observs = createSupabaseObservsClient();
  let written = 0;

  for (const chunk of chunkRows(preparedRows, OBSERVS_UPSERT_CHUNK_SIZE)) {
    written += await upsertObservsChunkWithFallback(observs, chunk);
  }

  return written;
}

export function buildObservsSyncReceipts(
  rows: Array<
    Pick<
      ObservsObservationRow,
      "connector_id" | "timeseries_id" | "observed_at"
    >
  >,
): ObservsSyncReceiptRow[] {
  const dedup = new Map<string, ObservsSyncReceiptRow>();
  for (const row of rows) {
    const connectorId = asFiniteNumber(row.connector_id);
    const timeseriesId = asFiniteNumber(row.timeseries_id);
    const observedDay = toObservedDay(row.observed_at);
    if (connectorId === null || timeseriesId === null || !observedDay) {
      continue;
    }
    const key = `${connectorId}:${timeseriesId}:${observedDay}`;
    dedup.set(key, {
      connector_id: connectorId,
      timeseries_id: timeseriesId,
      observed_day: observedDay,
    });
  }
  return Array.from(dedup.values());
}

export async function upsertObservsSyncReceipts(
  mainRpc: MainRpcCaller,
  rows: ObservsSyncReceiptRow[],
): Promise<number> {
  if (!rows.length) {
    return 0;
  }
  const { data, error } = await mainRpc<ObservsReceiptUpsertRow[]>(
    "uk_aq_rpc_observs_sync_receipt_daily_upsert",
    { rows },
  );
  if (error) {
    throw new Error(`Observs receipt upsert failed: ${error.message}`);
  }
  return countRowsFromPayload(
    data as Array<Record<"rows_upserted", number>> | null,
    "rows_upserted",
    rows.length,
  );
}

export async function enqueueObservsOutbox(
  mainRpc: MainRpcCaller,
  observsRows: ObservsObservationRow[],
): Promise<number> {
  const preparedRows = prepareObservsRows(observsRows);
  if (!preparedRows.length) {
    return 0;
  }
  const { data, error } = await mainRpc<ObservsOutboxEnqueueRow[]>(
    "uk_aq_rpc_observs_outbox_enqueue",
    {
      entries: [{ payload: preparedRows }],
    },
  );
  if (error) {
    throw new Error(`Observs outbox enqueue failed: ${error.message}`);
  }
  return countRowsFromPayload(
    data as Array<Record<"rows_enqueued", number>> | null,
    "rows_enqueued",
    1,
  );
}

export async function writeObservsWithOutbox(
  mainRpc: MainRpcCaller,
  observsRows: ObservsObservationRow[],
  onWarning?: (message: string) => void,
): Promise<ObservsWriteStats> {
  const preparedRows = prepareObservsRows(observsRows);
  if (!preparedRows.length) {
    return { written: 0, receipts_upserted: 0, enqueued: 0 };
  }

  if (OBSERVS_WRITE_MODE === "outbox_only") {
    const enqueued = await enqueueObservsOutbox(mainRpc, preparedRows);
    return {
      written: 0,
      receipts_upserted: 0,
      enqueued,
    };
  }

  if (OBSERVS_WRITE_MODE === "pubsub_only") {
    if (!observsPubsubConfigured()) {
      throw new Error(
        "OBSERVS_WRITE_MODE=pubsub_only but Pub/Sub is not configured.",
      );
    }
    const enqueued = await publishObservsRowsToPubsub(preparedRows);
    return {
      written: 0,
      receipts_upserted: 0,
      enqueued,
    };
  }

  if (!observsConfigured()) {
    onWarning?.("Observs DB is not configured; skipping observs write.");
    return { written: 0, receipts_upserted: 0, enqueued: 0 };
  }

  try {
    const written = await observsUpsertObservations(preparedRows);
    const receipts = buildObservsSyncReceipts(preparedRows);
    const receiptsUpserted = await upsertObservsSyncReceipts(mainRpc, receipts);
    return {
      written,
      receipts_upserted: receiptsUpserted,
      enqueued: 0,
    };
  } catch (error) {
    const enqueued = await enqueueObservsOutbox(mainRpc, preparedRows);
    onWarning?.(`Observs write failed, queued to outbox: ${shortError(error)}`);
    return {
      written: 0,
      receipts_upserted: 0,
      enqueued,
    };
  }
}

export async function flushObservsOutbox(
  mainRpc: MainRpcCaller,
  onWarning?: (message: string) => void,
  options: ObservsOutboxFlushOptions = {},
): Promise<ObservsOutboxFlushStats> {
  const stats: ObservsOutboxFlushStats = {
    claimed: 0,
    delivered: 0,
    failed: 0,
    receipts_upserted: 0,
    rows_resolved: 0,
  };

  if (!observsConfigured()) {
    return stats;
  }

  const claimResult = await mainRpc<ObservsOutboxClaimRow[]>(
    "uk_aq_rpc_observs_outbox_claim",
    {
      batch_limit: (() => {
        const candidate = Number(options.claim_batch_limit);
        if (Number.isFinite(candidate) && candidate > 0) {
          return Math.max(1, Math.trunc(candidate));
        }
        return OBSERVS_OUTBOX_FLUSH_LIMIT;
      })(),
    },
  );

  if (claimResult.error) {
    onWarning?.(`Observs outbox claim failed: ${claimResult.error.message}`);
    return stats;
  }

  const claimedRows = claimResult.data ?? [];
  stats.claimed = claimedRows.length;
  if (!claimedRows.length) {
    return stats;
  }

  const resolutions: Array<{
    id: string;
    ok: boolean;
    error?: string;
  }> = [];
  const deliveryRows: ObservsObservationRow[] = [];
  const deliveryIds: string[] = [];

  for (const row of claimedRows) {
    const payloadRows = Array.isArray(row.payload)
      ? row.payload as ObservsObservationRow[]
      : [];
    const preparedRows = prepareObservsRows(payloadRows);
    if (!preparedRows.length) {
      resolutions.push({ id: row.id, ok: true });
      continue;
    }
    deliveryRows.push(...preparedRows);
    deliveryIds.push(row.id);
  }

  if (deliveryIds.length > 0) {
    const mergedRows = prepareObservsRows(deliveryRows);
    try {
      const delivered = await observsUpsertObservations(mergedRows);
      stats.delivered += delivered;
      const receipts = buildObservsSyncReceipts(mergedRows);
      stats.receipts_upserted += await upsertObservsSyncReceipts(
        mainRpc,
        receipts,
      );
      for (const id of deliveryIds) {
        resolutions.push({ id, ok: true });
      }
    } catch (error) {
      const message = shortError(error);
      stats.failed += deliveryIds.length;
      for (const id of deliveryIds) {
        resolutions.push({
          id,
          ok: false,
          error: message,
        });
      }
      onWarning?.(
        `Observs outbox batch delivery failed for ${deliveryIds.length} entries: ${message}`,
      );
    }
  }

  const resolveResult = await mainRpc<ObservsOutboxResolveRow[]>(
    "uk_aq_rpc_observs_outbox_resolve",
    { resolutions },
  );

  if (resolveResult.error) {
    onWarning?.(
      `Observs outbox resolve failed: ${resolveResult.error.message}`,
    );
    return stats;
  }

  stats.rows_resolved = countRowsFromPayload(
    resolveResult.data as Array<Record<"rows_resolved", number>> | null,
    "rows_resolved",
    resolutions.length,
  );

  return stats;
}
