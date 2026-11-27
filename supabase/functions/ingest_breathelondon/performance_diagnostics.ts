import { AsyncLocalStorage } from "node:async_hooks";

// performance_diagnostics_v1: one request-scoped, fixed-shape response object.
// Durations use performance.now() and are rounded to integer ms at response time.
// Each Operation counts invocations/HTTP attempts, elapsed time through response
// parsing, longest invocation, and rejected/failed invocations. Database total_ms
// covers queue wait plus execution, including connection, transaction and error
// cleanup. source_api.total_ms sums HTTP attempts; total_with_backoff_ms sums
// complete fetchJson cycles including retry sleeps. PostgREST and forwarding
// durations may overlap when forwarding uses an IngestDB RPC. No identifiers,
// request details, URLs, payloads or per-station records are retained.
export type Operation = {
  calls: number;
  total_ms: number;
  max_ms: number;
  failed: number;
};
export type DatabaseOperation = Operation & { queue_ms: number; execution_ms: number };

function operation(): Operation {
  return { calls: 0, total_ms: 0, max_ms: 0, failed: 0 };
}
function databaseOperation(): DatabaseOperation {
  return { ...operation(), queue_ms: 0, execution_ms: 0 };
}

export type PerformanceDiagnostics = {
  elapsed_ms: number;
  stations_selected: number;
  stations_processed: number;
  timeseries_processed: number;
  observations_upserted: number;
  throttle_sleep_calls: number;
  throttle_sleep_ms: number;
  runtime_budget_reached: boolean;
  runtime_budget_phase: string | null;
  database: {
    timeseries_references: DatabaseOperation;
    observations: DatabaseOperation;
    species_checkpoints: DatabaseOperation;
    station_checkpoints: DatabaseOperation;
    latest_values: DatabaseOperation;
  };
  source_api: Operation & { retries: number; backoff_ms: number; total_with_backoff_ms: number };
  postgrest_reads: Operation;
  postgrest_writes: Operation;
  obsaqidb_forwarding: Operation;
};

type State = { started: number; phase: string; data: PerformanceDiagnostics };
const context = new AsyncLocalStorage<State>();

export function withPerformanceDiagnostics<T>(run: () => Promise<T>): Promise<T> {
  const data: PerformanceDiagnostics = {
    elapsed_ms: 0,
    stations_selected: 0,
    stations_processed: 0,
    timeseries_processed: 0,
    observations_upserted: 0,
    throttle_sleep_calls: 0,
    throttle_sleep_ms: 0,
    runtime_budget_reached: false,
    runtime_budget_phase: null,
    database: {
      timeseries_references: databaseOperation(),
      observations: databaseOperation(),
      species_checkpoints: databaseOperation(),
      station_checkpoints: databaseOperation(),
      latest_values: databaseOperation(),
    },
    source_api: { ...operation(), retries: 0, backoff_ms: 0, total_with_backoff_ms: 0 },
    postgrest_reads: operation(),
    postgrest_writes: operation(),
    obsaqidb_forwarding: operation(),
  };
  return context.run({ started: performance.now(), phase: "initialization", data }, run);
}

export function performancePhase(phase: string): void {
  try { const state = context.getStore(); if (state) state.phase = phase; } catch { /* diagnostics are best effort */ }
}

export function performanceBudgetReached(): void {
  try {
    const state = context.getStore();
    if (state && !state.data.runtime_budget_reached) {
      state.data.runtime_budget_reached = true;
      state.data.runtime_budget_phase = state.phase;
    }
  } catch { /* diagnostics are best effort */ }
}

export function performanceNow(): number { return performance.now(); }

export function recordThrottleSleep(started: number): void {
  try {
    const target = context.getStore()?.data;
    const elapsed = performance.now() - started;
    if (target && Number.isFinite(elapsed)) {
      target.throttle_sleep_calls += 1;
      target.throttle_sleep_ms += Math.max(0, elapsed);
    }
  } catch { /* diagnostics are best effort */ }
}

function add(target: Operation, elapsed: number, failed: boolean): void {
  if (!Number.isFinite(elapsed)) return;
  const ms = Math.max(0, elapsed);
  target.calls += 1;
  target.total_ms += ms;
  target.max_ms = Math.max(target.max_ms, ms);
  if (failed) target.failed += 1;
}

export function recordDatabaseOperation(
  category: keyof PerformanceDiagnostics["database"], started: number,
  queueMs: number, failed: boolean,
): void {
  try {
    const target = context.getStore()?.data.database[category];
    if (!target) return;
    const elapsed = performance.now() - started;
    add(target, elapsed, failed);
    target.queue_ms += queueMs;
    target.execution_ms += Math.max(0, elapsed - queueMs);
  } catch { /* diagnostics are best effort */ }
}

export function recordOperation(
  category: "postgrest_reads" | "postgrest_writes" | "obsaqidb_forwarding" | "source_api",
  started: number, failed: boolean,
): void {
  try {
    const target = context.getStore()?.data[category];
    if (target) add(target, performance.now() - started, failed);
  } catch { /* diagnostics are best effort */ }
}

export function recordSourceRetry(backoffMs: number): void {
  try {
    const target = context.getStore()?.data.source_api;
    if (target) { target.retries += 1; target.backoff_ms += Math.max(0, backoffMs); }
  } catch { /* diagnostics are best effort */ }
}

export function recordSourceCycle(started: number): void {
  try {
    const target = context.getStore()?.data.source_api;
    if (target) target.total_with_backoff_ms += Math.max(0, performance.now() - started);
  } catch { /* diagnostics are best effort */ }
}

export function performanceSnapshot(counts: {
  stations_selected: number; stations_processed: number; timeseries_processed: number;
  observations_upserted: number;
}): PerformanceDiagnostics | null {
  try {
    const state = context.getStore();
    if (!state) return null;
    const data = state.data;
    Object.assign(data, counts);
    data.elapsed_ms = performance.now() - state.started;
    // Integer milliseconds bound representation size without losing useful scale.
    const round = (op: Operation) => {
      op.total_ms = Math.round(op.total_ms);
      op.max_ms = Math.round(op.max_ms);
    };
    for (const op of Object.values(data.database)) {
      round(op);
      op.queue_ms = Math.round(op.queue_ms);
      op.execution_ms = Math.round(op.execution_ms);
    }
    for (const op of [data.source_api, data.postgrest_reads, data.postgrest_writes, data.obsaqidb_forwarding]) round(op);
    data.source_api.backoff_ms = Math.round(data.source_api.backoff_ms);
    data.source_api.total_with_backoff_ms = Math.round(data.source_api.total_with_backoff_ms);
    data.throttle_sleep_ms = Math.round(data.throttle_sleep_ms);
    data.elapsed_ms = Math.round(data.elapsed_ms);
    return data;
  } catch { return null; }
}
