export const UK_AIR_HTML_BRIDGE_POLLUTANT_CODES = [
  "o3",
  "no",
  "no2",
  "nox_as_no2",
  "so2",
  "co",
  "pm10",
  "pm25",
] as const;

const HTML_SERIES_TO_POLLUTANT = new Map<string, string>([
  ["O3", "o3"],
  ["NO", "no"],
  ["NO2", "no2"],
  ["NOXasNO2", "nox_as_no2"],
  ["SO2", "so2"],
  ["CO", "co"],
  ["PM10", "pm10"],
  ["PM2.5", "pm25"],
]);

const WRITE_ENABLED_POLLUTANTS = new Set([
  "o3",
  "no",
  "no2",
  "nox_as_no2",
  "so2",
  "pm10",
  "pm25",
]);

const SAFE_MICROGRAM_UNITS = new Set([
  "ug/m3",
  "ug.m-3",
  "ugm-3",
]);

export type UkAirHtmlPoint = {
  observed_at: string;
  value: number;
};

export type UkAirHtmlSeries = {
  htmlSeriesName: string;
  pollutantCode: string;
  writeEnabled: boolean;
  points: UkAirHtmlPoint[];
  totalPointCount: number;
  nullPointCount: number;
  rejectedPointCount: number;
  futurePointCount: number;
  outOfDayPointCount: number;
  newestEligibleTimestamp: string | null;
};

export type UkAirHtmlParseResult = {
  chartInvocationCount: number;
  chartCandidateCount: number;
  series: UkAirHtmlSeries[];
  ambiguousPollutantCodes: string[];
  unknownSeriesNames: string[];
  rejectedSeriesCount: number;
};

export class UkAirHtmlParseError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = "UkAirHtmlParseError";
    this.code = code;
  }
}

export function evaluateUkAirHtmlDestinationUnit(
  pollutantCode: string,
  destinationUnit: string | null,
): { safe: boolean; reason: string; normalizedUnit: string | null } {
  const normalizedUnit = normalizeUnit(destinationUnit);
  if (pollutantCode === "co") {
    return {
      safe: false,
      reason: "co_write_disabled",
      normalizedUnit,
    };
  }
  if (!WRITE_ENABLED_POLLUTANTS.has(pollutantCode)) {
    return {
      safe: false,
      reason: "unsupported_pollutant",
      normalizedUnit,
    };
  }
  if (!normalizedUnit || !SAFE_MICROGRAM_UNITS.has(normalizedUnit)) {
    return {
      safe: false,
      reason: "unsupported_destination_unit",
      normalizedUnit,
    };
  }
  return { safe: true, reason: "safe", normalizedUnit };
}

export function parseUkAirHtmlChart(
  html: string,
  currentUtcDay: string,
  nowMs: number,
): UkAirHtmlParseResult {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(currentUtcDay)) {
    throw new UkAirHtmlParseError(
      "invalid_current_utc_day",
      "Current UTC day must use YYYY-MM-DD.",
    );
  }
  if (!Number.isFinite(nowMs)) {
    throw new UkAirHtmlParseError(
      "invalid_current_time",
      "Current time must be finite epoch milliseconds.",
    );
  }

  const configs = collectHighchartsConfigs(html);
  if (configs.invocationCount === 0) {
    throw new UkAirHtmlParseError(
      "chart_not_found",
      "No Highcharts.chart invocation was found.",
    );
  }
  if (configs.candidates.length === 0) {
    throw new UkAirHtmlParseError(
      "chart_config_invalid",
      "No safely decoded Highcharts series configuration was found.",
    );
  }

  const recognizedCandidates = configs.candidates.filter((candidate) =>
    candidate.series.some((entry) => {
      const record = asRecord(entry);
      const name = typeof record?.name === "string" ? record.name.trim() : "";
      return HTML_SERIES_TO_POLLUTANT.has(name);
    })
  );
  const relevantCandidates = recognizedCandidates.length
    ? recognizedCandidates
    : configs.candidates;
  if (relevantCandidates.length !== 1) {
    throw new UkAirHtmlParseError(
      "chart_config_ambiguous",
      "Multiple Highcharts series configurations matched the data chart.",
    );
  }

  const parsedSeries: UkAirHtmlSeries[] = [];
  const unknownSeriesNames: string[] = [];
  let rejectedSeriesCount = 0;
  for (const entry of relevantCandidates[0].series) {
    const record = asRecord(entry);
    const htmlSeriesName = typeof record?.name === "string"
      ? record.name.trim()
      : "";
    if (!record || !htmlSeriesName) {
      rejectedSeriesCount += 1;
      continue;
    }
    const pollutantCode = HTML_SERIES_TO_POLLUTANT.get(htmlSeriesName);
    if (!pollutantCode) {
      unknownSeriesNames.push(htmlSeriesName.slice(0, 80));
      continue;
    }
    parsedSeries.push(parseSeries(
      htmlSeriesName,
      pollutantCode,
      record.data,
      currentUtcDay,
      nowMs,
    ));
  }

  const seriesCounts = new Map<string, number>();
  for (const series of parsedSeries) {
    seriesCounts.set(
      series.pollutantCode,
      (seriesCounts.get(series.pollutantCode) ?? 0) + 1,
    );
  }
  const ambiguousPollutantCodes = Array.from(seriesCounts.entries())
    .filter(([, count]) => count > 1)
    .map(([pollutantCode]) => pollutantCode)
    .sort();

  return {
    chartInvocationCount: configs.invocationCount,
    chartCandidateCount: configs.candidates.length,
    series: parsedSeries,
    ambiguousPollutantCodes,
    unknownSeriesNames,
    rejectedSeriesCount,
  };
}

function normalizeUnit(value: string | null): string | null {
  if (!value) return null;
  const normalized = value.trim().toLowerCase()
    .replace(/[µμ]/g, "u")
    .replace(/³/g, "3")
    .replace(/[·⋅]/g, ".")
    .replace(/\s+/g, "");
  return normalized || null;
}

function parseSeries(
  htmlSeriesName: string,
  pollutantCode: string,
  rawData: unknown,
  currentUtcDay: string,
  nowMs: number,
): UkAirHtmlSeries {
  const data = Array.isArray(rawData) ? rawData : [];
  const points: UkAirHtmlPoint[] = [];
  let nullPointCount = 0;
  let rejectedPointCount = Array.isArray(rawData) ? 0 : 1;
  let futurePointCount = 0;
  let outOfDayPointCount = 0;

  for (const point of data) {
    if (!Array.isArray(point) || point.length < 2) {
      rejectedPointCount += 1;
      continue;
    }
    const timestampMs = point[0];
    if (
      typeof timestampMs !== "number" || !Number.isFinite(timestampMs) ||
      !Number.isInteger(timestampMs)
    ) {
      rejectedPointCount += 1;
      continue;
    }
    const observedAt = new Date(timestampMs);
    if (Number.isNaN(observedAt.getTime())) {
      rejectedPointCount += 1;
      continue;
    }
    let observedAtIso: string;
    try {
      observedAtIso = observedAt.toISOString();
    } catch {
      rejectedPointCount += 1;
      continue;
    }
    if (observedAtIso.slice(0, 10) !== currentUtcDay) {
      outOfDayPointCount += 1;
      continue;
    }
    if (timestampMs > nowMs) {
      futurePointCount += 1;
      continue;
    }
    if (point[1] === null) {
      nullPointCount += 1;
      continue;
    }
    if (typeof point[1] !== "number" || !Number.isFinite(point[1])) {
      rejectedPointCount += 1;
      continue;
    }
    points.push({ observed_at: observedAtIso, value: point[1] });
  }

  points.sort((left, right) =>
    Date.parse(left.observed_at) - Date.parse(right.observed_at)
  );
  return {
    htmlSeriesName,
    pollutantCode,
    writeEnabled: WRITE_ENABLED_POLLUTANTS.has(pollutantCode),
    points,
    totalPointCount: data.length,
    nullPointCount,
    rejectedPointCount,
    futurePointCount,
    outOfDayPointCount,
    newestEligibleTimestamp: points.length
      ? points[points.length - 1].observed_at
      : null,
  };
}

function collectHighchartsConfigs(html: string): {
  invocationCount: number;
  candidates: Array<{ series: unknown[] }>;
} {
  const marker = "Highcharts.chart";
  const candidates: Array<{ series: unknown[] }> = [];
  let invocationCount = 0;
  let cursor = 0;
  while (cursor < html.length) {
    const markerIndex = html.indexOf(marker, cursor);
    if (markerIndex < 0) break;
    cursor = markerIndex + marker.length;
    let openParen = cursor;
    while (openParen < html.length && /\s/.test(html[openParen])) {
      openParen += 1;
    }
    if (html[openParen] !== "(") {
      continue;
    }
    invocationCount += 1;
    const objectStarts = findTopLevelObjectArguments(html, openParen);
    for (const start of objectStarts) {
      const end = findJsonObjectEnd(html, start);
      if (end === null) continue;
      try {
        const parsed = JSON.parse(html.slice(start, end + 1));
        const record = asRecord(parsed);
        if (record && Array.isArray(record.series)) {
          candidates.push({ series: record.series });
        }
      } catch {
        // The contract requires JSON-compatible configuration. Invalid JSON
        // is deliberately ignored here and fails closed if no candidate remains.
      }
    }
  }
  return { invocationCount, candidates };
}

function findTopLevelObjectArguments(
  input: string,
  openParen: number,
): number[] {
  const starts: number[] = [];
  let parenDepth = 1;
  for (let index = openParen + 1; index < input.length; index += 1) {
    const char = input[index];
    if (char === '"' || char === "'" || char === "`") {
      const stringEnd = findStringEnd(input, index, char);
      if (stringEnd === null) return starts;
      index = stringEnd;
      continue;
    }
    if (char === "(") {
      parenDepth += 1;
      continue;
    }
    if (char === ")") {
      parenDepth -= 1;
      if (parenDepth === 0) return starts;
      continue;
    }
    if (char === "{" && parenDepth === 1) {
      starts.push(index);
      const objectEnd = findJsonObjectEnd(input, index);
      if (objectEnd === null) return starts;
      index = objectEnd;
    }
  }
  return starts;
}

function findJsonObjectEnd(input: string, start: number): number | null {
  if (input[start] !== "{") return null;
  const stack: string[] = ["{"];
  for (let index = start + 1; index < input.length; index += 1) {
    const char = input[index];
    if (char === '"' || char === "'" || char === "`") {
      const stringEnd = findStringEnd(input, index, char);
      if (stringEnd === null) return null;
      index = stringEnd;
      continue;
    }
    if (char === "{" || char === "[") {
      stack.push(char);
      continue;
    }
    if (char !== "}" && char !== "]") continue;
    const expected = char === "}" ? "{" : "[";
    if (stack.pop() !== expected) return null;
    if (stack.length === 0) return index;
  }
  return null;
}

function findStringEnd(
  input: string,
  start: number,
  quote: string,
): number | null {
  let escaped = false;
  for (let index = start + 1; index < input.length; index += 1) {
    const char = input[index];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (char === "\\") {
      escaped = true;
      continue;
    }
    if (char === quote) return index;
  }
  return null;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}
