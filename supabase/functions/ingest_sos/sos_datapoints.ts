let emptySeriesLogs = 0;

export function parseDatapoints(
  values: unknown,
  seriesId?: number,
): Array<{ observed_at: string; value: number | null; status: string | null }> {
  let rows = values;
  if (!Array.isArray(rows) && rows && typeof rows === "object") {
    const nested = (rows as Record<string, unknown>).values ??
      (rows as Record<string, unknown>).data;
    if (Array.isArray(nested)) {
      rows = nested;
    }
  }
  if (!Array.isArray(rows)) {
    logEmptySeries(seriesId, rows, "values not array");
    return [];
  }
  if (rows.length === 0) {
    logEmptySeries(seriesId, { row_count: 0 }, "no rows");
    return [];
  }
  const points: Array<
    { observed_at: string; value: number | null; status: string | null }
  > = [];
  for (const row of rows) {
    if (Array.isArray(row)) {
      if (row.length < 2) {
        continue;
      }
      const observedAt = parseTimestamp(row[0]);
      if (!observedAt) {
        continue;
      }
      const value = toNumber(row[1]);
      const status = row.length > 2 && row[2] != null ? String(row[2]) : null;
      points.push({
        observed_at: observedAt.toISOString(),
        value,
        status,
      });
      continue;
    }
    if (row && typeof row === "object") {
      const record = row as Record<string, unknown>;
      const observedAt = parseTimestamp(
        record.timestamp ?? record.time ?? record.phenomenonTime ??
          record.dateTime ?? record.datetime,
      );
      if (!observedAt) {
        continue;
      }
      const value = toNumber(record.value ?? record.result ?? record.v);
      const status = record.status != null
        ? String(record.status)
        : record.quality != null
        ? String(record.quality)
        : record.qc != null
        ? String(record.qc)
        : null;
      points.push({
        observed_at: observedAt.toISOString(),
        value,
        status,
      });
    }
  }
  if (!points.length) {
    logEmptySeries(seriesId, rows[0], "no parsed datapoints");
  }
  return points;
}

function toNumber(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    const nested = record.value ?? record.result ?? record.v;
    if (nested === null || nested === undefined) {
      return null;
    }
    return toNumber(nested);
  }
  if (
    (typeof value !== "number" && typeof value !== "string") ||
    (typeof value === "string" && !value.trim())
  ) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return null;
  }
  return num;
}

function parseTimestamp(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "object" && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    const nested = record.timestamp ?? record.time ?? record.dateTime ??
      record.datetime;
    return parseTimestamp(nested);
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    const timestamp = value < 1e12 ? value * 1000 : value;
    const observedAt = new Date(timestamp);
    return Number.isNaN(observedAt.getTime()) ? null : observedAt;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) {
      return null;
    }
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      const timestamp = numeric < 1e12 ? numeric * 1000 : numeric;
      const observedAt = new Date(timestamp);
      return Number.isNaN(observedAt.getTime()) ? null : observedAt;
    }
    const observedAt = new Date(trimmed);
    return Number.isNaN(observedAt.getTime()) ? null : observedAt;
  }
  return null;
}

function logEmptySeries(
  seriesId: number | undefined,
  sample: unknown,
  reason: string,
): void {
  if (emptySeriesLogs >= 3) {
    return;
  }
  emptySeriesLogs += 1;
  const log = reason === "no rows" ? console.info : console.warn;
  log("No datapoints parsed", {
    series_id: seriesId ?? null,
    reason,
  });
}
