const DEFAULT_CONFIG = Object.freeze({
  attempts: 3,
  retryBaseMs: 500,
  retryMaxMs: 5_000,
  splitMinRows: 25,
  splitMaxDepth: 5,
  minimumAttemptRuntimeMs: 1_000,
  shutdownBufferMs: 1_000,
});

const CONFIG_BOUNDS = Object.freeze({
  attempts: [1, 5],
  retryBaseMs: [1, 10_000],
  retryMaxMs: [2, 30_000],
  splitMinRows: [1, 10_000],
  splitMaxDepth: [0, 10],
  minimumAttemptRuntimeMs: [1, 120_000],
  shutdownBufferMs: [0, 30_000],
});

const RETRYABLE_HTTP_STATUSES = new Set([429, 500, 502, 503, 504]);
const TRANSIENT_HTTP_STATUSES = new Set([500, 502, 503, 504]);
const MAX_DIAGNOSTIC_LENGTH = 500;

export class IngestDbObservationWriteError extends Error {
  constructor(message, options = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "IngestDbObservationWriteError";
    this.classification = options.classification ?? "non_retryable";
    this.terminalReason = options.terminalReason ?? "non_retryable_error";
    this.errorCode = options.errorCode ?? null;
    this.httpStatus = options.httpStatus ?? null;
    this.stats = options.stats ?? null;
  }
}

function boundedInteger(value, fallback, minimum, maximum) {
  const parsed = typeof value === "number" ? value : Number(String(value ?? "").trim());
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    return fallback;
  }
  return parsed;
}

export function parseIngestDbObservationWriteConfig(input = {}) {
  const source = input && typeof input === "object" ? input : {};
  const parsed = {};
  for (const key of Object.keys(DEFAULT_CONFIG)) {
    const [minimum, maximum] = CONFIG_BOUNDS[key];
    parsed[key] = boundedInteger(
      source[key],
      DEFAULT_CONFIG[key],
      minimum,
      maximum,
    );
  }
  if (parsed.retryMaxMs <= parsed.retryBaseMs) {
    parsed.retryBaseMs = DEFAULT_CONFIG.retryBaseMs;
    parsed.retryMaxMs = DEFAULT_CONFIG.retryMaxMs;
  }
  return Object.freeze(parsed);
}

function asObject(value) {
  return value && typeof value === "object" ? value : null;
}

function boundedText(value) {
  if (value === null || value === undefined) {
    return "";
  }
  let text;
  try {
    text = typeof value === "string" ? value : JSON.stringify(value);
  } catch {
    text = String(value);
  }
  return String(text ?? "").slice(0, MAX_DIAGNOSTIC_LENGTH);
}

function maybeJson(value) {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
    return null;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

function collectFailureFields(error) {
  const queue = [error];
  const seen = new Set();
  const messages = [];
  let code = null;
  let httpStatus = null;

  while (queue.length && seen.size < 12) {
    const current = queue.shift();
    if (current === null || current === undefined || seen.has(current)) {
      continue;
    }
    if (typeof current === "string") {
      const text = boundedText(current);
      if (text) messages.push(text);
      const parsed = maybeJson(current);
      if (parsed) queue.push(parsed);
      continue;
    }
    if (typeof current !== "object") {
      messages.push(boundedText(current));
      continue;
    }
    seen.add(current);
    const object = asObject(current);
    if (!object) continue;

    for (const key of ["message", "details", "hint", "statusText", "text"]) {
      const text = boundedText(object[key]);
      if (text) {
        messages.push(text);
        const parsed = maybeJson(text);
        if (parsed) queue.push(parsed);
      }
    }
    if (code === null) {
      const candidate = object.code ?? object.sqlstate ?? object.error_code;
      if (candidate !== null && candidate !== undefined) {
        code = String(candidate).trim().toUpperCase() || null;
      }
    }
    if (httpStatus === null) {
      const candidate = Number(
        object.httpStatus ?? object.http_status ?? object.status ?? object.statusCode,
      );
      if (Number.isInteger(candidate) && candidate >= 100 && candidate <= 599) {
        httpStatus = candidate;
      }
    }
    for (const key of ["cause", "error", "response", "data", "body", "payload"]) {
      if (object[key] !== undefined) queue.push(object[key]);
    }
  }

  return {
    code,
    httpStatus,
    message: messages.join(" | ").slice(0, MAX_DIAGNOSTIC_LENGTH),
  };
}

function clearlyPermanent(code, message) {
  if (code) {
    if (/^(22|23|28|42)/.test(code) || /^PGRST(1|2|3)/.test(code)) {
      return true;
    }
  }
  return /(?:authentication|authorization|permission denied|invalid (?:input|payload|timestamp|connector|timeseries)|malformed|unknown column|column .+ does not exist|relation .+ does not exist|schema cache|not-null|foreign key|unique constraint|violates .+ constraint)/i
    .test(message);
}

export function classifyIngestDbObservationWriteFailure(error) {
  const fields = collectFailureFields(error);
  const message = fields.message;
  const code = fields.code;
  const httpStatus = fields.httpStatus;

  if (
    code === "57014" &&
    /statement timeout|canceling statement due to statement timeout/i.test(message)
  ) {
    return { classification: "statement_timeout", retryable: true, ...fields };
  }
  if (code === "57014") {
    return { classification: "non_retryable", retryable: false, ...fields };
  }
  if (code === "40P01" || /deadlock detected/i.test(message)) {
    return { classification: "deadlock", retryable: true, ...fields };
  }
  if (code === "40001" || /serialization failure|could not serialize access/i.test(message)) {
    return { classification: "serialization_failure", retryable: true, ...fields };
  }
  if (
    (code && (/^08/.test(code) || ["57P01", "57P02", "57P03"].includes(code))) ||
    /connection (?:terminated|reset|closed|refused)|econnreset|socket hang up|temporary network|network error|network request failed|fetch failed|error sending request|postgrest request timed out|request timed out|operation was aborted|aborterror/i
      .test(message)
  ) {
    return { classification: "connection_failure", retryable: true, ...fields };
  }
  if (httpStatus === 429) {
    return { classification: "rate_limited", retryable: true, ...fields };
  }
  if (clearlyPermanent(code, message)) {
    return { classification: "non_retryable", retryable: false, ...fields };
  }
  if (httpStatus !== null && TRANSIENT_HTTP_STATUSES.has(httpStatus)) {
    return {
      classification: "temporary_service_failure",
      retryable: true,
      ...fields,
    };
  }
  return { classification: "non_retryable", retryable: false, ...fields };
}

function createStats(inputRows, normalChunkSize = 0) {
  return {
    input_rows: inputRows,
    normal_chunk_size: normalChunkSize,
    committed_rows: 0,
    write_requests: 0,
    retry_attempts: 0,
    retried_chunks: 0,
    split_operations: 0,
    smallest_attempted_chunk: inputRows ? Number.POSITIVE_INFINITY : 0,
    unresolved_rows: inputRows,
    terminal_failure_classification: null,
    terminal_reason: null,
    stopped_for_runtime_budget: false,
  };
}

function snapshotStats(stats) {
  return {
    ...stats,
    smallest_attempted_chunk: Number.isFinite(stats.smallest_attempted_chunk)
      ? stats.smallest_attempted_chunk
      : 0,
  };
}

export function mergeIngestDbObservationWriteStats(target, addition) {
  target.input_rows += addition.input_rows;
  target.normal_chunk_size = Math.max(
    Number(target.normal_chunk_size) || 0,
    Number(addition.normal_chunk_size) || 0,
  );
  target.committed_rows += addition.committed_rows;
  target.write_requests += addition.write_requests;
  target.retry_attempts += addition.retry_attempts;
  target.retried_chunks += addition.retried_chunks;
  target.split_operations += addition.split_operations;
  const sizes = [target.smallest_attempted_chunk, addition.smallest_attempted_chunk]
    .filter((value) => Number.isFinite(value) && value > 0);
  target.smallest_attempted_chunk = sizes.length ? Math.min(...sizes) : 0;
  target.unresolved_rows += addition.unresolved_rows;
  if (addition.terminal_failure_classification) {
    target.terminal_failure_classification = addition.terminal_failure_classification;
  }
  if (addition.terminal_reason) target.terminal_reason = addition.terminal_reason;
  target.stopped_for_runtime_budget ||= addition.stopped_for_runtime_budget;
  return target;
}

export function createEmptyIngestDbObservationWriteStats() {
  return createStats(0);
}

export function isIngestDbObservationWriteError(error) {
  return error instanceof IngestDbObservationWriteError ||
    asObject(error)?.name === "IngestDbObservationWriteError";
}

function retryDelayMs(retryNumber, config, random) {
  const exponential = Math.min(
    config.retryMaxMs - config.retryBaseMs,
    config.retryBaseMs * (2 ** Math.max(0, retryNumber - 1)),
  );
  const jitterCeiling = Math.max(
    1,
    Math.min(config.retryBaseMs, config.retryMaxMs - exponential),
  );
  const randomValue = Math.min(0.999999999, Math.max(0, Number(random()) || 0));
  return Math.min(
    config.retryMaxMs,
    exponential + 1 + Math.floor(randomValue * jitterCeiling),
  );
}

function emit(logger, level, event, context) {
  const method = logger?.[level];
  if (typeof method === "function") {
    method.call(logger, event, context);
  }
}

function remainingRuntimeMs(runtimeBudget) {
  if (typeof runtimeBudget?.remainingRuntimeMs === "function") {
    const remaining = Number(runtimeBudget.remainingRuntimeMs());
    return Number.isFinite(remaining) ? Math.max(0, remaining) : null;
  }
  if (Number.isFinite(runtimeBudget?.deadlineMs)) {
    return Math.max(0, Number(runtimeBudget.deadlineMs) - Date.now());
  }
  return null;
}

function hasRuntimeFor(runtimeBudget, requiredMs) {
  if (typeof runtimeBudget?.shouldStop === "function" && runtimeBudget.shouldStop()) {
    return false;
  }
  const remaining = remainingRuntimeMs(runtimeBudget);
  return remaining === null || remaining >= requiredMs;
}

function terminalError({
  cause,
  classification,
  terminalReason,
  stats,
  connectorCode,
  originalChunkRows,
  finalChunkRows,
  attempts,
  splitDepth,
}) {
  stats.unresolved_rows = stats.input_rows - stats.committed_rows;
  stats.terminal_failure_classification = classification.classification;
  stats.terminal_reason = terminalReason;
  stats.stopped_for_runtime_budget = terminalReason === "runtime_budget";
  const diagnostic = {
    connector_code: connectorCode,
    original_chunk_rows: originalChunkRows,
    final_chunk_rows: finalChunkRows,
    attempts,
    split_depth: splitDepth,
    unresolved_rows: stats.unresolved_rows,
    failure_classification: classification.classification,
    terminal_reason: terminalReason,
    error_code: classification.code,
    http_status: classification.httpStatus,
  };
  const error = new IngestDbObservationWriteError(
    `IngestDB observation write failed (${terminalReason}; ${classification.classification}; unresolved_rows=${stats.unresolved_rows}).`,
    {
      cause,
      classification: classification.classification,
      terminalReason,
      errorCode: classification.code,
      httpStatus: classification.httpStatus,
      stats: snapshotStats(stats),
    },
  );
  return { error, diagnostic };
}

export async function writeIngestDbObservations(options) {
  const rows = Array.isArray(options?.rows) ? options.rows : [];
  const writeChunk = options?.writeChunk;
  if (typeof writeChunk !== "function") {
    throw new TypeError("writeChunk must be a function.");
  }
  const config = parseIngestDbObservationWriteConfig(options.config);
  const chunkSize = boundedInteger(
    options.chunkSize,
    Math.max(1, rows.length || 1),
    1,
    100_000,
  );
  const connectorCode = boundedText(options.connectorCode || "unknown").slice(0, 100);
  const logger = options.logger ?? console;
  const sleep = typeof options.sleep === "function"
    ? options.sleep
    : (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const random = typeof options.random === "function" ? options.random : Math.random;
  const runtimeBudget = options.runtimeBudget ?? null;
  const stats = createStats(rows.length, rows.length ? chunkSize : 0);

  const failForBudget = (chunk, splitDepth, originalChunkRows, attempts) => {
    const classification = {
      classification: "runtime_budget",
      retryable: false,
      code: null,
      httpStatus: null,
    };
    return terminalError({
      cause: null,
      classification,
      terminalReason: "runtime_budget",
      stats,
      connectorCode,
      originalChunkRows,
      finalChunkRows: chunk.length,
      attempts,
      splitDepth,
    });
  };

  const processChunk = async (chunk, splitDepth, originalChunkRows) => {
    let lastFailure = null;
    let lastClassification = null;
    let retriedThisChunk = false;

    for (let attempt = 1; attempt <= config.attempts; attempt += 1) {
      if (attempt > 1) {
        const delayMs = retryDelayMs(attempt - 1, config, random);
        const requiredMs = delayMs + config.minimumAttemptRuntimeMs +
          config.shutdownBufferMs;
        if (!hasRuntimeFor(runtimeBudget, requiredMs)) {
          const terminal = failForBudget(chunk, splitDepth, originalChunkRows, attempt - 1);
          emit(logger, "error", "ingestdb_observation_upsert_terminal", terminal.diagnostic);
          throw terminal.error;
        }
        emit(logger, "warn", "ingestdb_observation_upsert_retry", {
          connector_code: connectorCode,
          chunk_rows: chunk.length,
          attempt,
          maximum_attempts: config.attempts,
          failure_classification: lastClassification.classification,
          delay_ms: delayMs,
        });
        await sleep(delayMs);
        if (
          !hasRuntimeFor(
            runtimeBudget,
            config.minimumAttemptRuntimeMs + config.shutdownBufferMs,
          )
        ) {
          const terminal = failForBudget(chunk, splitDepth, originalChunkRows, attempt - 1);
          emit(logger, "error", "ingestdb_observation_upsert_terminal", terminal.diagnostic);
          throw terminal.error;
        }
        stats.retry_attempts += 1;
        if (!retriedThisChunk) {
          retriedThisChunk = true;
          stats.retried_chunks += 1;
        }
      } else if (
        !hasRuntimeFor(
          runtimeBudget,
          config.minimumAttemptRuntimeMs + config.shutdownBufferMs,
        )
      ) {
        const terminal = failForBudget(chunk, splitDepth, originalChunkRows, 0);
        emit(logger, "error", "ingestdb_observation_upsert_terminal", terminal.diagnostic);
        throw terminal.error;
      }

      stats.write_requests += 1;
      stats.smallest_attempted_chunk = Math.min(
        stats.smallest_attempted_chunk,
        chunk.length,
      );
      try {
        await writeChunk(chunk);
        stats.committed_rows += chunk.length;
        stats.unresolved_rows = stats.input_rows - stats.committed_rows;
        return;
      } catch (error) {
        lastFailure = error;
        lastClassification = classifyIngestDbObservationWriteFailure(error);
        if (!lastClassification.retryable) {
          const terminal = terminalError({
            cause: error,
            classification: lastClassification,
            terminalReason: "non_retryable_error",
            stats,
            connectorCode,
            originalChunkRows,
            finalChunkRows: chunk.length,
            attempts: attempt,
            splitDepth,
          });
          emit(logger, "error", "ingestdb_observation_upsert_terminal", terminal.diagnostic);
          throw terminal.error;
        }
      }
    }

    const canSplit = lastClassification?.classification === "statement_timeout" &&
      splitDepth < config.splitMaxDepth &&
      Math.floor(chunk.length / 2) >= config.splitMinRows;
    if (canSplit) {
      if (
        !hasRuntimeFor(
          runtimeBudget,
          config.minimumAttemptRuntimeMs + config.shutdownBufferMs,
        )
      ) {
        const terminal = failForBudget(
          chunk,
          splitDepth,
          originalChunkRows,
          config.attempts,
        );
        emit(logger, "error", "ingestdb_observation_upsert_terminal", terminal.diagnostic);
        throw terminal.error;
      }
      const midpoint = Math.floor(chunk.length / 2);
      const left = chunk.slice(0, midpoint);
      const right = chunk.slice(midpoint);
      if (!left.length || !right.length) {
        throw new Error("IngestDB observation split produced an empty child.");
      }
      stats.split_operations += 1;
      emit(logger, "warn", "ingestdb_observation_upsert_split", {
        connector_code: connectorCode,
        parent_chunk_rows: chunk.length,
        left_chunk_rows: left.length,
        right_chunk_rows: right.length,
        split_depth: splitDepth + 1,
        failure_classification: lastClassification.classification,
      });
      await processChunk(left, splitDepth + 1, originalChunkRows);
      await processChunk(right, splitDepth + 1, originalChunkRows);
      return;
    }

    const terminalReason = lastClassification?.classification === "statement_timeout" &&
        (chunk.length < config.splitMinRows * 2 || config.splitMaxDepth === 0)
      ? "minimum_chunk_failed"
      : "retry_exhausted";
    const terminal = terminalError({
      cause: lastFailure,
      classification: lastClassification,
      terminalReason,
      stats,
      connectorCode,
      originalChunkRows,
      finalChunkRows: chunk.length,
      attempts: config.attempts,
      splitDepth,
    });
    emit(logger, "error", "ingestdb_observation_upsert_terminal", terminal.diagnostic);
    throw terminal.error;
  };

  for (let offset = 0; offset < rows.length; offset += chunkSize) {
    const chunk = rows.slice(offset, offset + chunkSize);
    await processChunk(chunk, 0, chunk.length);
  }
  return snapshotStats(stats);
}

export const INGESTDB_OBSERVATION_WRITE_DEFAULTS = DEFAULT_CONFIG;
export const INGESTDB_OBSERVATION_RETRYABLE_HTTP_STATUSES = RETRYABLE_HTTP_STATUSES;
