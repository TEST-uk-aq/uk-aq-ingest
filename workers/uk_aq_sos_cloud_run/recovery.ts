import { parseDatapoints } from "../../supabase/functions/ingest_sos/sos_datapoints.ts";
import {
  fetchUkAirHtmlPage,
  resolveUkAirHtmlMappings,
  type UkAirHtmlBridgeRow,
} from "../../supabase/functions/ingest_sos/uk_aq_html_fallback.ts";
import {
  parseUkAirHtmlChart,
  UK_AIR_HTML_BRIDGE_POLLUTANT_CODES,
} from "../../supabase/functions/ingest_sos/uk_aq_html_parser.ts";
import { boundMessage } from "../../supabase/functions/ingest_sos/failure.ts";

const HOUR = 3_600_000;
// This is persisted in the existing run record, never in successful-poll state.
export type SosRecoveryState = {
  version: 1;
  cursor: number;
  pending: Record<string, { until: number; count: number }>;
  sites: Record<string, { after: number; attempts: number }>;
};
export type SosRecoveryConfig = {
  enabled: boolean;
  windowHours: number;
  scanLimit: number;
  siteLimit: number;
  retryMinutes: number;
  budgetSeconds: number;
  retentionDays: number;
};
export function readSosRecoveryConfig(): SosRecoveryConfig {
  const flag = Deno.env.get("SOS_RECOVERY_ENABLED") ?? "true";
  if (flag !== "true" && flag !== "false") {
    throw new Error("SOS_RECOVERY_ENABLED must be true or false.");
  }
  const integer = (key: string, fallback: number, min: number, max: number) => {
    const value = Number(Deno.env.get(key) ?? fallback);
    if (!Number.isInteger(value) || value < min || value > max) {
      throw new Error(`${key} must be an integer in ${min}..${max}.`);
    }
    return value;
  };
  return {
    enabled: flag === "true",
    windowHours: integer("SOS_RECOVERY_WINDOW_HOURS", 72, 24, 72),
    scanLimit: integer("SOS_RECOVERY_SCAN_LIMIT", 48, 1, 96),
    siteLimit: integer("SOS_RECOVERY_SITE_LIMIT", 4, 1, 8),
    retryMinutes: integer("SOS_RECOVERY_RETRY_MINUTES", 180, 60, 720),
    budgetSeconds: integer("SOS_RECOVERY_MAX_RUNTIME_SECONDS", 60, 45, 120),
    retentionDays: flag === "true"
      ? integer("INGESTDB_RETENTION_DAYS", 0, 1, 30)
      : 1,
  };
}

type Query = Record<string, string>;
type RecoveryTimeseries = {
  id: number;
  station_id: number;
  uom: string | null;
  last_value_at: string | null;
  first_value_at: string | null;
  timeseries_ref: string | null;
};
export type RecoveryPoint = {
  observed_at: string;
  value: number;
  status: null;
  acquisition_method: "sos" | "ukair_html";
};
export type SosRecoveryDependencies = {
  read: (table: string, query: Query) => Promise<Record<string, unknown>[]>;
  mappings: (fromDay: string, toDay: string) => Promise<UkAirHtmlBridgeRow[]>;
  // The existing compact writer and Observs/outbox path, after a database recheck.
  write: (
    rows: Array<RecoveryPoint & { timeseries_id: number }>,
  ) => Promise<number>;
};

export async function recoverSosRecentGaps(
  connectorId: number,
  config: SosRecoveryConfig,
  deadline: number,
  deps: SosRecoveryDependencies,
  primaryAvailable: boolean,
  sosBaseUrl: string,
): Promise<Record<string, unknown>> {
  const now = Date.now();
  const requestedFrom = Math.ceil((now - config.windowHours * HOUR) / HOUR) *
    HOUR;
  // Stay one complete UTC day inside the configured retention boundary.
  const retentionFloor = Date.parse(new Date(now).toISOString().slice(0, 10)) -
    (config.retentionDays - 1) * 24 * HOUR;
  const from = Math.max(requestedFrom, retentionFloor);
  // Do not diagnose the two newest publishing hours as data loss.
  const to = Math.floor(now / HOUR) * HOUR - 2 * HOUR;
  const fromDay = new Date(from).toISOString().slice(0, 10);
  const toDay = new Date(now).toISOString().slice(0, 10);
  const summary: Record<string, unknown> = {
    recovery_status: config.enabled ? "pending" : "disabled",
    recovery_series_checked: 0,
    recovery_missing: 0,
    recovery_recovered: 0,
    recovery_unresolved: 0,
    recovery_unresolved_known: 0,
    recovery_source_unavailable: 0,
    recovery_mapping_unavailable: 0,
    recovery_prior_day_points: 0,
    recovery_acquisition_method: primaryAvailable ? "sos" : "ukair_html",
    recovery_sos_requests: 0,
    recovery_html_requests: 0,
    recovery_cadence_unconfirmed: 0,
    recovery_retention_excluded_hours: Math.max(
      0,
      (from - requestedFrom) / HOUR,
    ),
    recovery_requests: 0,
    recovery_retries: 0,
    recovery_retry_deferred: 0,
    recovery_source_failures: 0,
    recovery_scope: "bounded_scan",
  };
  if (!config.enabled) return summary;
  if (from > to) return { ...summary, recovery_status: "retention_excluded" };
  // Leave time for an ordinary 30-second database write plus bookkeeping.
  const canStart = () => deadline - Date.now() > 35_000;
  if (!canStart()) {
    return { ...summary, recovery_status: "runtime_budget_exceeded" };
  }
  let state: SosRecoveryState | undefined;
  let recovered = 0;
  let missing = 0;
  let requests = 0;
  let stopped = false;
  try {
    // Select only the last run which actually carried recovery state. A skipped
    // trigger or failed primary probe must not reset retry pacing or fairness.
    const runs = await deps.read("uk_aq_ingest_runs", {
      connector_id: `eq.${connectorId}`,
      "response_payload->sos_recovery_state": "not.is.null",
      select: "recovery_state:response_payload->sos_recovery_state",
      order: "run_ended_at.desc",
      limit: "1",
    });
    state = decodeState(runs[0]?.recovery_state, now);
    let rows = await deps.read("timeseries", {
      connector_id: `eq.${connectorId}`,
      ended_at: "is.null",
      id: `gt.${state.cursor}`,
      select: "id,station_id,uom,last_value_at,first_value_at,timeseries_ref",
      order: "id.asc",
      limit: String(config.scanLimit),
    });
    if (!rows.length && state.cursor !== 0) {
      state.cursor = 0;
      rows = await deps.read("timeseries", {
        connector_id: `eq.${connectorId}`,
        ended_at: "is.null",
        select: "id,station_id,uom,last_value_at,first_value_at,timeseries_ref",
        order: "id.asc",
        limit: String(config.scanLimit),
      });
    }
    const series = rows.map((row) => {
      if (
        !Number.isSafeInteger(row.id) || Number(row.id) <= 0 ||
        !Number.isSafeInteger(row.station_id) || Number(row.station_id) <= 0
      ) {
        throw new Error("Invalid SOS recovery timeseries identity.");
      }
      return row as unknown as RecoveryTimeseries;
    });
    if (!series.length) {
      return {
        ...summary,
        recovery_status: Object.keys(state.pending).length
          ? "unresolved"
          : "batch_complete",
        recovery_unresolved_known: Object.values(state.pending).reduce(
          (n, item) => n + item.count,
          0,
        ),
        sos_recovery_state: state,
      };
    }
    // Bound both input IDs and returned rows. Never infer continuity from latest.
    const ids = series.map((row) => row.id);
    const present = await loadPresent(deps, connectorId, ids, from, to);
    const bridge = await deps.mappings(fromDay, toDay);
    const stationByTimeseries = new Map(
      series.map((row) => [row.id, row.station_id]),
    );
    if (
      bridge.some((row) =>
        stationByTimeseries.has(Number(row.timeseries_id)) &&
        Number(row.station_id) !==
          stationByTimeseries.get(Number(row.timeseries_id))
      )
    ) {
      throw new Error("SOS recovery bridge station identity mismatch.");
    }
    // Resolve independently for EACH observation day, including validity and
    // ambiguity. A current bridge is not authority for a previous UTC day.
    const mappingByDay = new Map<
      string,
      ReturnType<typeof resolveUkAirHtmlMappings>
    >();
    for (let dayMs = Date.parse(fromDay); dayMs <= to; dayMs += 24 * HOUR) {
      const day = new Date(dayMs).toISOString().slice(0, 10);
      mappingByDay.set(
        day,
        resolveUkAirHtmlMappings(
          series,
          bridge.filter((row) =>
            typeof row.valid_from_day_utc === "string" &&
            row.valid_from_day_utc <= day &&
            (row.valid_to_day_utc === null ||
              (typeof row.valid_to_day_utc === "string" &&
                row.valid_to_day_utc >= day))
          ),
        ),
      );
    }
    const pages = new Map<
      string,
      ReturnType<typeof parseUkAirHtmlChart> | null
    >();
    const currentRunSites = new Set<string>();
    const pending: Array<RecoveryPoint & { timeseries_id: number }> = [];
    for (const row of series) {
      if (!canStart()) {
        stopped = true;
        break;
      }
      const first = row.first_value_at === null
        ? Number.NaN
        : Date.parse(row.first_value_at);
      // Require lifecycle and demonstrated hourly cadence; a catalogue row
      // alone does not prove that every absent hourly key is an expected sample.
      const hourly = [...present].filter((key) => key.startsWith(`${row.id}:`))
        .map((key) => Number(key.split(":")[1])).sort((a, b) => a - b);
      if (
        !Number.isFinite(first) ||
        !hourly.some((at, i) => i > 0 && at - hourly[i - 1] === HOUR)
      ) {
        summary.recovery_cadence_unconfirmed =
          Number(summary.recovery_cadence_unconfirmed) + 1;
        state.cursor = row.id;
        continue;
      }
      const gaps = new Map<string, number[]>();
      for (
        let at = Math.max(from, Math.ceil(first / HOUR) * HOUR);
        at <= to;
        at += HOUR
      ) {
        if (present.has(`${row.id}:${at}`)) continue;
        const day = new Date(at).toISOString().slice(0, 10);
        const times = gaps.get(day) ?? [];
        times.push(at);
        gaps.set(day, times);
      }
      // Advance only after handling this row. Request-cap stops leave the next
      // row as next run's first candidate rather than starving it on each sweep.
      const gapTimes = [...gaps.values()].flat();
      if (gapTimes.length) {
        state.pending[String(row.id)] = {
          until: Math.max(...gapTimes) + config.windowHours * HOUR,
          count: gapTimes.length,
        };
      } else {
        delete state.pending[String(row.id)];
      }
      let deferredByLimit = false;
      const points: RecoveryPoint[] = [];
      let rowMissing = 0;
      for (const [day, timestamps] of gaps) {
        rowMissing += timestamps.length;
        const work = mappingByDay.get(day)?.mappedWork.find((item) =>
          item.timeseries.id === row.id
        );
        if (!work) {
          summary.recovery_mapping_unavailable =
            Number(summary.recovery_mapping_unavailable) + timestamps.length;
          continue;
        }
        // The current authorised HTML route cannot request earlier UTC days.
        if (!primaryAvailable && day !== toDay) continue;
        const site = primaryAvailable ? `sos_${row.id}` : work.siteRef;
        if (!pages.has(site)) {
          const prior = state.sites[site];
          if (prior && prior.after > now && !currentRunSites.has(site)) {
            summary.recovery_retry_deferred =
              Number(summary.recovery_retry_deferred) + 1;
            continue;
          }
          if (requests >= config.siteLimit || !canStart()) {
            deferredByLimit = true;
            break;
          }
          // Record the cooldown before the single request, including failed
          // requests and empty sources. Persisted in this run's normal record.
          state.sites[site] = {
            after: now + config.retryMinutes * 60_000,
            attempts: Math.min(1000, (prior?.attempts ?? 0) + 1),
          };
          currentRunSites.add(site);
          requests += 1;
          if (prior) {
            summary.recovery_retries = Number(summary.recovery_retries) + 1;
          }
          try {
            let parsed: ReturnType<typeof parseUkAirHtmlChart>;
            if (primaryAvailable) {
              if (!row.timeseries_ref || !sosBaseUrl) {
                throw new Error(
                  "Missing native SOS recovery source configuration.",
                );
              }
              summary.recovery_sos_requests =
                Number(summary.recovery_sos_requests) + 1;
              const native = await fetchNativeRecovery(
                row.timeseries_ref,
                sosBaseUrl,
                Math.min(...gapTimes),
                Math.max(...gapTimes) + HOUR,
                deadline - 30_000,
              );
              parsed = {
                chartInvocationCount: 0,
                chartCandidateCount: 0,
                ambiguousPollutantCodes: [],
                unknownSeriesNames: [],
                rejectedSeriesCount: 0,
                series: [{
                  htmlSeriesName: "native_sos",
                  pollutantCode: work.pollutantCode,
                  writeEnabled: true,
                  points: native.filter((point) => point.value !== null).map((
                    point,
                  ) => ({
                    observed_at: point.observed_at,
                    value: point.value!,
                  })),
                  unavailableTimestamps: native.filter((point) =>
                    point.value === null
                  ).map((point) => point.observed_at),
                  totalPointCount: native.length,
                  nullPointCount: native.filter((point) =>
                    point.value === null
                  ).length,
                  rejectedPointCount: 0,
                  futurePointCount: 0,
                  outOfDayPointCount: 0,
                  newestEligibleTimestamp: null,
                }],
              };
            } else {
              summary.recovery_html_requests =
                Number(summary.recovery_html_requests) + 1;
              const page = await fetchUkAirHtmlPage(
                site,
                deadline - 30_000,
                undefined,
                1,
              );
              parsed = parseUkAirHtmlChart(page.html, toDay, now);
            }
            pages.set(site, parsed);
            const timestamps = parsed.series.flatMap((series) => [
              ...series.points.map((point) => point.observed_at),
              ...series.unavailableTimestamps,
            ]).sort();
            if (timestamps.length) {
              const first = timestamps[0];
              const last = timestamps[timestamps.length - 1];
              if (
                !summary.recovery_source_first_at ||
                first < String(summary.recovery_source_first_at)
              ) summary.recovery_source_first_at = first;
              if (
                !summary.recovery_source_last_at ||
                last > String(summary.recovery_source_last_at)
              ) summary.recovery_source_last_at = last;
              summary.recovery_prior_day_points =
                Number(summary.recovery_prior_day_points) +
                timestamps.filter((at) => at.slice(0, 10) < toDay).length;
            }
          } catch (error) {
            pages.set(site, null);
            summary.recovery_source_failures =
              Number(summary.recovery_source_failures) + 1;
            summary.recovery_error = boundMessage(error);
          }
        }
        const parsed = pages.get(site);
        if (
          !parsed || parsed.ambiguousPollutantCodes.includes(work.pollutantCode)
        ) continue;
        const source = parsed.series.find((item) =>
          item.pollutantCode === work.pollutantCode
        );
        if (!source?.writeEnabled) continue;
        const wanted = new Set(timestamps);
        for (const at of source.unavailableTimestamps) {
          if (wanted.has(Date.parse(at))) {
            summary.recovery_source_unavailable =
              Number(summary.recovery_source_unavailable) + 1;
          }
        }
        for (const point of source.points) {
          if (wanted.has(Date.parse(point.observed_at))) {
            points.push({
              ...point,
              status: null,
              acquisition_method: primaryAvailable ? "sos" : "ukair_html",
            });
          }
        }
      }
      missing += rowMissing;
      for (const point of points) {
        pending.push({ ...point, timeseries_id: row.id });
      }
      if (deferredByLimit) {
        stopped = true;
        break;
      }
      state.cursor = row.id;
      summary.recovery_series_checked =
        Number(summary.recovery_series_checked) + 1;
    }
    if (pending.length) {
      const unique = new Map(
        pending.map((
          point,
        ) => [`${point.timeseries_id}:${point.observed_at}`, point]),
      );
      const candidates = [...unique.values()].filter((point) =>
        Date.parse(point.observed_at) >= Date.now() - config.windowHours * HOUR
      );
      const valuesByKey = new Map<string, number>();
      for (const point of pending) {
        const key = `${point.timeseries_id}:${point.observed_at}`;
        if (valuesByKey.has(key) && valuesByKey.get(key) !== point.value) {
          throw new Error("Conflicting source values for a SOS recovery key.");
        }
        valuesByKey.set(key, point.value);
      }
      // The canonical recovery RPC atomically protects concurrent valid writes.
      if (candidates.length) await deps.write(candidates);
      const after = candidates.length
        ? await loadPresent(
          deps,
          connectorId,
          [...new Set(candidates.map((point) => point.timeseries_id))],
          from,
          to,
        )
        : new Set<string>();
      for (const point of candidates) {
        if (
          !after.has(`${point.timeseries_id}:${Date.parse(point.observed_at)}`)
        ) continue;
        recovered += 1;
        const remaining = state.pending[String(point.timeseries_id)];
        if (remaining && --remaining.count <= 0) {
          delete state.pending[String(point.timeseries_id)];
        }
      }
    }
    summary.recovery_status = stopped
      ? (canStart() ? "request_limit" : "runtime_budget_exceeded")
      : Object.keys(state.pending).length > 0
      ? "unresolved"
      : Number(summary.recovery_cadence_unconfirmed) > 0
      ? "cadence_unconfirmed"
      : "batch_complete";
  } catch (error) {
    summary.recovery_status = "failed";
    summary.recovery_error = boundMessage(error);
  }
  summary.recovery_missing = missing;
  summary.recovery_recovered = recovered;
  summary.recovery_unresolved = Math.max(0, missing - recovered);
  summary.recovery_requests = requests;
  if (state) {
    summary.sos_recovery_state = state;
    summary.recovery_unresolved_known = Object.values(state.pending).reduce(
      (total, entry) => total + entry.count,
      0,
    );
  }
  return summary;
}

async function loadPresent(
  deps: SosRecoveryDependencies,
  connectorId: number,
  ids: number[],
  from: number,
  to: number,
): Promise<Set<string>> {
  const present = new Set<string>();
  const maxRows = ids.length * (Math.ceil((to - from) / HOUR) + 1);
  // Exact hourly keys can have at most maxRows; a non-hourly legacy source must
  // fail closed on this bound rather than silently use a truncated response.
  for (let offset = 0; offset <= maxRows; offset += 1000) {
    const rows = await deps.read("observations", {
      connector_id: `eq.${connectorId}`,
      timeseries_id: `in.(${ids.join(",")})`,
      and: `(observed_at.gte.${new Date(from).toISOString()},observed_at.lte.${
        new Date(to).toISOString()
      })`,
      value: "not.is.null",
      select: "timeseries_id,observed_at",
      order: "timeseries_id.asc,observed_at.asc",
      limit: "1000",
      offset: String(offset),
    });
    if (offset + rows.length > maxRows) {
      throw new Error("Recovery observation scan exceeded hourly bound.");
    }
    for (const row of rows) {
      const at = Date.parse(String(row.observed_at));
      if (
        !ids.includes(Number(row.timeseries_id)) || !Number.isFinite(at) ||
        at % HOUR !== 0
      ) {
        throw new Error("Invalid/non-hourly observation in SOS recovery scan.");
      }
      present.add(`${row.timeseries_id}:${at}`);
    }
    if (rows.length < 1000) return present;
  }
  throw new Error("Recovery observation scan exceeded page bound.");
}

function decodeState(raw: unknown, now: number): SosRecoveryState {
  if (raw === undefined || raw === null) {
    return { version: 1, cursor: 0, pending: {}, sites: {} };
  }
  const state = raw as SosRecoveryState;
  if (
    state.version !== 1 || !Number.isSafeInteger(state.cursor) ||
    state.cursor < 0 ||
    !state.sites || typeof state.sites !== "object" ||
    Array.isArray(state.sites) ||
    Object.keys(state.sites).length > 1024
  ) {
    throw new Error(
      "Invalid persisted SOS recovery state; refusing to reset retry pacing.",
    );
  }
  if (
    !state.pending || typeof state.pending !== "object" ||
    Array.isArray(state.pending) ||
    Object.keys(state.pending).length > 4096
  ) throw new Error("Invalid persisted recovery pending scope.");
  const pending: SosRecoveryState["pending"] = {};
  for (const [id, entry] of Object.entries(state.pending)) {
    if (
      !/^\d+$/.test(id) || !Number.isFinite(entry.until) ||
      entry.until > now + 72 * HOUR ||
      !Number.isInteger(entry.count) || entry.count < 1 || entry.count > 73
    ) {
      throw new Error("Invalid persisted recovery pending gap summary.");
    }
    if (entry.until > now) pending[id] = entry;
  }
  const sites: SosRecoveryState["sites"] = {};
  for (const [site, entry] of Object.entries(state.sites)) {
    if (
      !/^[A-Za-z0-9_-]{1,32}$/.test(site) || !Number.isFinite(entry.after) ||
      !Number.isInteger(entry.attempts) || entry.attempts < 1 ||
      entry.attempts > 1000 ||
      entry.after > now + 12 * HOUR
    ) {
      throw new Error("Invalid persisted SOS recovery site pacing.");
    }
    if (entry.after > now - 72 * HOUR) sites[site] = entry;
  }
  return { version: 1, cursor: state.cursor, pending, sites };
}

async function fetchNativeRecovery(
  ref: string,
  baseUrl: string,
  from: number,
  to: number,
  deadline: number,
) {
  const remaining = deadline - Date.now();
  if (remaining <= 1000) {
    throw new Error("Native SOS recovery runtime budget exhausted.");
  }
  const url = new URL(
    `${baseUrl.replace(/\/$/, "")}/timeseries/${
      encodeURIComponent(ref)
    }/getData`,
  );
  url.searchParams.set(
    "timespan",
    `${new Date(from).toISOString()}/${new Date(to).toISOString()}`,
  );
  url.searchParams.set("format", "tvp");
  const response = await fetch(url, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(Math.min(15_000, remaining)),
  });
  if (!response.ok) {
    try {
      await response.body?.cancel();
    } catch { /* preserve HTTP failure */ }
    throw new Error(`Native SOS recovery HTTP ${response.status}.`);
  }
  const payload = await response.json();
  return parseDatapoints(payload?.values).filter((point) => {
    const at = Date.parse(point.observed_at);
    return Number.isFinite(at) && at % HOUR === 0 && at >= from && at <= to;
  });
}
