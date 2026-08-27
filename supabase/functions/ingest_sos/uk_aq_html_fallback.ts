import {
  asSosFetchFailure,
  boundMessage,
  type SosFetchFailure,
  SosFetchFailure as SosFetchFailureError,
} from "./failure.ts";
import {
  evaluateUkAirHtmlDestinationUnit,
  UK_AIR_HTML_BRIDGE_POLLUTANT_CODES,
} from "./uk_aq_html_parser.ts";

const DATA_PLOT_URL = "https://uk-air.defra.gov.uk/data-plot";
const DEFAULT_TIMEOUT_MS = 30_000;
const MIN_FETCH_TIMEOUT_MS = 4_000;
const FETCH_RETRY_ATTEMPTS = 3;
const FETCH_RETRY_BACKOFF_BASE_MS = 1_000;
const FETCH_RETRY_BACKOFF_MAX_MS = 30_000;
const RETRYABLE_FETCH_STATUSES = new Set([429, 500, 502, 503, 504]);

export type UkAirHtmlSelectedTimeseries = {
  id: number;
  last_value_at: string | null;
  uom: string | null;
};

export type UkAirHtmlBridgeRow = {
  site_ref?: unknown;
  uk_air_ref?: unknown;
  pollutant_code?: unknown;
  station_id?: unknown;
  station_ref?: unknown;
  timeseries_id?: unknown;
  timeseries_ref?: unknown;
  valid_from_day_utc?: unknown;
  valid_to_day_utc?: unknown;
};

export type UkAirHtmlMappedWork = {
  siteRef: string;
  pollutantCode: string;
  timeseries: UkAirHtmlSelectedTimeseries;
};

export type UkAirHtmlMappingResolution = {
  mappedWork: UkAirHtmlMappedWork[];
  unmappedTimeseriesIds: number[];
  ambiguousTimeseriesIds: number[];
  coWriteDisabledTimeseriesIds: number[];
  unsafeUnitTimeseriesIds: number[];
};

export function isUkAirHtmlFallbackProbeFailure(
  failure: SosFetchFailure,
): boolean {
  if (failure.kind === "http") {
    return failure.upstreamStatus !== null &&
      failure.upstreamStatus >= 500 && failure.upstreamStatus <= 599;
  }
  return failure.kind === "request_timeout" ||
    failure.kind === "runtime_deadline" || failure.kind === "network";
}

export function resolveUkAirHtmlMappings(
  selectedTimeseries: UkAirHtmlSelectedTimeseries[],
  bridgeRows: UkAirHtmlBridgeRow[],
): UkAirHtmlMappingResolution {
  const selectedById = new Map(
    selectedTimeseries.map((row) => [row.id, row] as const),
  );
  const candidatesByTimeseries = new Map<
    number,
    Map<string, { siteRef: string; pollutantCode: string }>
  >();
  const invalidMappingTimeseriesIds = new Set<number>();

  for (const row of bridgeRows) {
    const timeseriesId = positiveInteger(row.timeseries_id);
    if (timeseriesId === null || !selectedById.has(timeseriesId)) continue;
    const siteRef = asString(row.site_ref);
    const pollutantCode = asString(row.pollutant_code)?.toLowerCase();
    if (
      !siteRef || !pollutantCode ||
      !UK_AIR_HTML_BRIDGE_POLLUTANT_CODES.includes(
        pollutantCode as typeof UK_AIR_HTML_BRIDGE_POLLUTANT_CODES[number],
      )
    ) {
      invalidMappingTimeseriesIds.add(timeseriesId);
      continue;
    }
    const signature = JSON.stringify([
      siteRef,
      asString(row.uk_air_ref) ?? null,
      pollutantCode,
      row.station_id == null ? null : String(row.station_id),
      asString(row.station_ref) ?? null,
      timeseriesId,
      asString(row.timeseries_ref) ?? null,
      row.valid_from_day_utc == null ? null : String(row.valid_from_day_utc),
      row.valid_to_day_utc == null ? null : String(row.valid_to_day_utc),
    ]);
    const candidates = candidatesByTimeseries.get(timeseriesId) ?? new Map();
    candidates.set(signature, { siteRef, pollutantCode });
    candidatesByTimeseries.set(timeseriesId, candidates);
  }

  const ambiguousTimeseriesIds = new Set(invalidMappingTimeseriesIds);
  const preliminarilyMapped: UkAirHtmlMappedWork[] = [];
  for (const timeseries of selectedTimeseries) {
    const candidates = candidatesByTimeseries.get(timeseries.id);
    if (!candidates || candidates.size === 0) continue;
    if (candidates.size !== 1 || ambiguousTimeseriesIds.has(timeseries.id)) {
      ambiguousTimeseriesIds.add(timeseries.id);
      continue;
    }
    const mapping = candidates.values().next().value as
      | { siteRef: string; pollutantCode: string }
      | undefined;
    if (!mapping) continue;
    preliminarilyMapped.push({ ...mapping, timeseries });
  }

  const idsBySitePollutant = new Map<string, number[]>();
  for (const work of preliminarilyMapped) {
    const key = `${work.siteRef}\u0000${work.pollutantCode}`;
    const ids = idsBySitePollutant.get(key) ?? [];
    ids.push(work.timeseries.id);
    idsBySitePollutant.set(key, ids);
  }
  for (const ids of idsBySitePollutant.values()) {
    if (ids.length > 1) {
      ids.forEach((id) => ambiguousTimeseriesIds.add(id));
    }
  }

  const coWriteDisabledTimeseriesIds: number[] = [];
  const unsafeUnitTimeseriesIds: number[] = [];
  const mappedWork: UkAirHtmlMappedWork[] = [];
  for (const work of preliminarilyMapped) {
    if (ambiguousTimeseriesIds.has(work.timeseries.id)) continue;
    const unit = evaluateUkAirHtmlDestinationUnit(
      work.pollutantCode,
      work.timeseries.uom,
    );
    if (!unit.safe) {
      if (unit.reason === "co_write_disabled") {
        coWriteDisabledTimeseriesIds.push(work.timeseries.id);
      } else {
        unsafeUnitTimeseriesIds.push(work.timeseries.id);
      }
      continue;
    }
    mappedWork.push(work);
  }

  const mappedOrRejectedIds = new Set([
    ...mappedWork.map((work) => work.timeseries.id),
    ...ambiguousTimeseriesIds,
    ...coWriteDisabledTimeseriesIds,
    ...unsafeUnitTimeseriesIds,
  ]);
  return {
    mappedWork,
    unmappedTimeseriesIds: selectedTimeseries
      .map((row) => row.id)
      .filter((id) => !mappedOrRejectedIds.has(id)),
    ambiguousTimeseriesIds: Array.from(ambiguousTimeseriesIds).sort((a, b) =>
      a - b
    ),
    coWriteDisabledTimeseriesIds: coWriteDisabledTimeseriesIds.sort((a, b) =>
      a - b
    ),
    unsafeUnitTimeseriesIds: unsafeUnitTimeseriesIds.sort((a, b) => a - b),
  };
}

export async function fetchUkAirHtmlPage(
  siteRef: string,
  runtimeDeadline: number,
): Promise<{ html: string; status: number; durationMs: number }> {
  const url = new URL(DATA_PLOT_URL);
  url.searchParams.set("site_id", siteRef);
  url.searchParams.set("days", "0");
  const startedAt = Date.now();

  for (let attempt = 1; attempt <= FETCH_RETRY_ATTEMPTS; attempt += 1) {
    const remainingBudgetMs = runtimeDeadline - Date.now();
    if (remainingBudgetMs <= MIN_FETCH_TIMEOUT_MS) {
      throw new SosFetchFailureError({
        kind: "runtime_deadline",
        message: "Runtime budget exhausted before UK-AIR HTML fetch completed.",
      });
    }
    const timeoutMs = Math.max(
      MIN_FETCH_TIMEOUT_MS,
      Math.min(DEFAULT_TIMEOUT_MS, remainingBudgetMs - 250),
    );
    const timeoutKind = timeoutMs < DEFAULT_TIMEOUT_MS
      ? "runtime_deadline"
      : "request_timeout";
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      let response: Response;
      try {
        response = await fetch(url.toString(), {
          headers: { Accept: "text/html" },
          signal: controller.signal,
        });
      } catch (error) {
        if (controller.signal.aborted) {
          throw new SosFetchFailureError({
            kind: timeoutKind,
            message: timeoutKind === "runtime_deadline"
              ? "Runtime budget exhausted during UK-AIR HTML fetch."
              : "UK-AIR HTML request timed out.",
            retryable: timeoutKind === "request_timeout",
          });
        }
        throw new SosFetchFailureError({
          kind: "network",
          message: boundMessage(error),
          retryable: true,
        });
      }
      const html = await response.text();
      if (!response.ok) {
        throw new SosFetchFailureError({
          kind: "http",
          message: `HTTP ${response.status} ${response.statusText}`,
          upstreamStatus: response.status,
          retryable: RETRYABLE_FETCH_STATUSES.has(response.status),
        });
      }
      return {
        html,
        status: response.status,
        durationMs: Math.max(0, Date.now() - startedAt),
      };
    } catch (error) {
      const failure = asSosFetchFailure(error);
      if (attempt >= FETCH_RETRY_ATTEMPTS || !failure.retryable) {
        throw failure;
      }
      const retryDelayMs = Math.min(
        FETCH_RETRY_BACKOFF_MAX_MS,
        FETCH_RETRY_BACKOFF_BASE_MS * (2 ** (attempt - 1)),
      );
      if (runtimeDeadline - Date.now() <= retryDelayMs + MIN_FETCH_TIMEOUT_MS) {
        throw failure;
      }
      await sleep(retryDelayMs);
    } finally {
      clearTimeout(timeout);
    }
  }
  throw new SosFetchFailureError({
    kind: "unknown",
    message: "UK-AIR HTML fetch exhausted retries.",
  });
}

function positiveInteger(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
