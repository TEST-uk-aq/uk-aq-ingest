export type SosSelectedStationRow = {
  id: number;
  station_ref: string;
};

export type SosSelectedTimeseriesDispatchRow = {
  id: number;
  station_id: number;
};

export type SosSelectedTimeseriesMetadata = {
  id: number;
  timeseries_ref: string | null;
  service_ref: string | null;
  phenomenon_id: string | null;
  last_value_at: string | null;
  uom: string | null;
};

export type SosHtmlBridgeMetadata = {
  site_ref: string;
  uk_air_ref: string | null;
  pollutant_code: string;
  station_id: number;
  station_ref: string | null;
  timeseries_id: number;
  timeseries_ref: string | null;
  valid_from_day_utc: string;
  valid_to_day_utc: string | null;
};

export type SosSelectedWorkPlan = {
  stationRows: SosSelectedStationRow[];
  timeseriesRows: SosSelectedTimeseriesDispatchRow[];
  selectedTimeseries: SosSelectedTimeseriesMetadata[];
  bridgeRows: SosHtmlBridgeMetadata[];
};

export type SosCompactChildPayload = {
  selectedTimeseries: SosSelectedTimeseriesMetadata[];
  bridgeRows: SosHtmlBridgeMetadata[];
  bridgeDayUtc: string;
};

const RPC_BRIDGE_FIELDS = [
  "bridge_site_ref",
  "bridge_uk_air_ref",
  "bridge_pollutant_code",
  "bridge_station_id",
  "bridge_station_ref",
  "bridge_timeseries_id",
  "bridge_timeseries_ref",
  "bridge_valid_from_day_utc",
  "bridge_valid_to_day_utc",
] as const;

export function normalizeSosSelectedWorkRpcResponse(
  payload: unknown,
): SosSelectedWorkPlan {
  if (!Array.isArray(payload)) {
    throw new Error("SOS selected-work RPC returned a non-array response.");
  }

  const stationById = new Map<number, SosSelectedStationRow>();
  const dispatchById = new Map<number, SosSelectedTimeseriesDispatchRow>();
  const metadataById = new Map<number, SosSelectedTimeseriesMetadata>();
  const bridgeRows: SosHtmlBridgeMetadata[] = [];

  for (const value of payload) {
    const row = record(value, "SOS selected-work RPC row");
    const stationId = positiveInteger(row.station_id, "station_id");
    const stationRef = nonEmptyString(row.station_ref, "station_ref");
    retainConsistent(
      stationById,
      stationId,
      { id: stationId, station_ref: stationRef },
      "station",
    );

    if (row.timeseries_id === null) {
      requireNullFields(row, [
        "timeseries_ref",
        "service_ref",
        "phenomenon_id",
        "last_value_at",
        "uom",
        ...RPC_BRIDGE_FIELDS,
      ], "station-only selected-work row");
      continue;
    }

    const timeseriesId = positiveInteger(row.timeseries_id, "timeseries_id");
    const metadata: SosSelectedTimeseriesMetadata = {
      id: timeseriesId,
      timeseries_ref: nullableString(row.timeseries_ref, "timeseries_ref"),
      service_ref: nullableString(row.service_ref, "service_ref"),
      phenomenon_id: nullablePositiveIntegerText(
        row.phenomenon_id,
        "phenomenon_id",
      ),
      last_value_at: nullableString(row.last_value_at, "last_value_at"),
      uom: nullableString(row.uom, "uom"),
    };
    retainConsistent(
      dispatchById,
      timeseriesId,
      { id: timeseriesId, station_id: stationId },
      "timeseries station",
    );
    retainConsistent(
      metadataById,
      timeseriesId,
      metadata,
      "selected timeseries",
    );

    const bridgeIsAbsent = RPC_BRIDGE_FIELDS.every((field) =>
      row[field] === null
    );
    if (bridgeIsAbsent) {
      continue;
    }

    const bridgeTimeseriesId = positiveInteger(
      row.bridge_timeseries_id,
      "bridge_timeseries_id",
    );
    if (bridgeTimeseriesId !== timeseriesId) {
      throw new Error(
        `SOS selected-work RPC bridge row ${bridgeTimeseriesId} does not match selected timeseries ${timeseriesId}.`,
      );
    }
    bridgeRows.push({
      site_ref: nonEmptyString(row.bridge_site_ref, "bridge_site_ref"),
      uk_air_ref: nullableString(row.bridge_uk_air_ref, "bridge_uk_air_ref"),
      pollutant_code: nonEmptyString(
        row.bridge_pollutant_code,
        "bridge_pollutant_code",
      ).toLowerCase(),
      station_id: positiveInteger(
        row.bridge_station_id,
        "bridge_station_id",
      ),
      station_ref: nullableString(
        row.bridge_station_ref,
        "bridge_station_ref",
      ),
      timeseries_id: bridgeTimeseriesId,
      timeseries_ref: nullableString(
        row.bridge_timeseries_ref,
        "bridge_timeseries_ref",
      ),
      valid_from_day_utc: dateString(
        row.bridge_valid_from_day_utc,
        "bridge_valid_from_day_utc",
      ),
      valid_to_day_utc: nullableDateString(
        row.bridge_valid_to_day_utc,
        "bridge_valid_to_day_utc",
      ),
    });
  }

  return {
    stationRows: Array.from(stationById.values()),
    timeseriesRows: Array.from(dispatchById.values()),
    selectedTimeseries: Array.from(metadataById.values()),
    bridgeRows,
  };
}

export function readSosCompactChildPayload(
  payload: Record<string, unknown>,
  requestedTimeseriesIds: number[] | undefined,
): SosCompactChildPayload | null {
  const hasSelectedTimeseries = hasOwn(payload, "selected_timeseries");
  const hasBridgeRows = hasOwn(payload, "uk_air_html_bridge_rows");
  const hasBridgeDay = hasOwn(payload, "uk_air_html_bridge_day_utc");
  if (!hasSelectedTimeseries && !hasBridgeRows && !hasBridgeDay) {
    return null;
  }
  if (!hasSelectedTimeseries || !hasBridgeRows || !hasBridgeDay) {
    throw new Error("Incomplete SOS compact selected-work payload.");
  }
  if (!Array.isArray(payload.selected_timeseries)) {
    throw new Error("selected_timeseries must be an array.");
  }
  if (!Array.isArray(payload.uk_air_html_bridge_rows)) {
    throw new Error("uk_air_html_bridge_rows must be an array.");
  }
  if (!requestedTimeseriesIds) {
    throw new Error(
      "SOS compact selected work requires an explicit timeseries_ids set.",
    );
  }

  const bridgeDayUtc = dateString(
    payload.uk_air_html_bridge_day_utc,
    "uk_air_html_bridge_day_utc",
  );
  const metadataById = new Map<number, SosSelectedTimeseriesMetadata>();
  for (const value of payload.selected_timeseries) {
    const row = record(value, "selected_timeseries row");
    const timeseriesId = positiveInteger(row.id, "selected_timeseries.id");
    retainConsistent(
      metadataById,
      timeseriesId,
      {
        id: timeseriesId,
        timeseries_ref: nullableString(
          row.timeseries_ref,
          "selected_timeseries.timeseries_ref",
        ),
        service_ref: nullableString(
          row.service_ref,
          "selected_timeseries.service_ref",
        ),
        phenomenon_id: nullablePositiveIntegerText(
          row.phenomenon_id,
          "selected_timeseries.phenomenon_id",
        ),
        last_value_at: nullableString(
          row.last_value_at,
          "selected_timeseries.last_value_at",
        ),
        uom: nullableString(row.uom, "selected_timeseries.uom"),
      },
      "selected timeseries",
    );
  }

  const requestedSet = new Set(requestedTimeseriesIds);
  if (
    requestedSet.size !== metadataById.size ||
    Array.from(requestedSet).some((id) => !metadataById.has(id))
  ) {
    throw new Error(
      "SOS compact selected_timeseries IDs do not match timeseries_ids.",
    );
  }

  const bridgeRows = payload.uk_air_html_bridge_rows.map((value) => {
    const row = record(value, "uk_air_html_bridge_rows row");
    const timeseriesId = positiveInteger(
      row.timeseries_id,
      "uk_air_html_bridge_rows.timeseries_id",
    );
    if (!requestedSet.has(timeseriesId)) {
      throw new Error(
        `SOS compact bridge row references unselected timeseries ${timeseriesId}.`,
      );
    }
    const validFromDayUtc = dateString(
      row.valid_from_day_utc,
      "uk_air_html_bridge_rows.valid_from_day_utc",
    );
    const validToDayUtc = nullableDateString(
      row.valid_to_day_utc,
      "uk_air_html_bridge_rows.valid_to_day_utc",
    );
    if (
      validFromDayUtc > bridgeDayUtc ||
      (validToDayUtc !== null && validToDayUtc < bridgeDayUtc)
    ) {
      throw new Error(
        `SOS compact bridge row is not valid on ${bridgeDayUtc}.`,
      );
    }
    return {
      site_ref: nonEmptyString(
        row.site_ref,
        "uk_air_html_bridge_rows.site_ref",
      ),
      uk_air_ref: nullableString(
        row.uk_air_ref,
        "uk_air_html_bridge_rows.uk_air_ref",
      ),
      pollutant_code: nonEmptyString(
        row.pollutant_code,
        "uk_air_html_bridge_rows.pollutant_code",
      ).toLowerCase(),
      station_id: positiveInteger(
        row.station_id,
        "uk_air_html_bridge_rows.station_id",
      ),
      station_ref: nullableString(
        row.station_ref,
        "uk_air_html_bridge_rows.station_ref",
      ),
      timeseries_id: timeseriesId,
      timeseries_ref: nullableString(
        row.timeseries_ref,
        "uk_air_html_bridge_rows.timeseries_ref",
      ),
      valid_from_day_utc: validFromDayUtc,
      valid_to_day_utc: validToDayUtc,
    } satisfies SosHtmlBridgeMetadata;
  });

  return {
    selectedTimeseries: Array.from(metadataById.values()),
    bridgeRows,
    bridgeDayUtc,
  };
}

function retainConsistent<T>(
  values: Map<number, T>,
  id: number,
  value: T,
  label: string,
): void {
  const existing = values.get(id);
  if (existing && JSON.stringify(existing) !== JSON.stringify(value)) {
    throw new Error(
      `SOS selected work returned conflicting ${label} metadata for ID ${id}.`,
    );
  }
  if (!existing) {
    values.set(id, value);
  }
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} must be an object.`);
  }
  return value as Record<string, unknown>;
}

function positiveInteger(value: unknown, field: string): number {
  const parsed = typeof value === "number" ? value : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${field} must be a positive integer.`);
  }
  return parsed;
}

function nonEmptyString(value: unknown, field: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${field} must be a non-empty string.`);
  }
  return value.trim();
}

function nullableString(value: unknown, field: string): string | null {
  if (value === null) return null;
  if (typeof value !== "string") {
    throw new Error(`${field} must be a string or null.`);
  }
  return value || null;
}

function nullablePositiveIntegerText(
  value: unknown,
  field: string,
): string | null {
  if (value === null) return null;
  return String(positiveInteger(value, field));
}

function dateString(value: unknown, field: string): string {
  const text = nonEmptyString(value, field);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    throw new Error(`${field} must be an ISO date.`);
  }
  const parsed = new Date(`${text}T00:00:00.000Z`);
  if (
    Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== text
  ) {
    throw new Error(`${field} must be a valid ISO date.`);
  }
  return text;
}

function nullableDateString(value: unknown, field: string): string | null {
  if (value === null) return null;
  return dateString(value, field);
}

function requireNullFields(
  row: Record<string, unknown>,
  fields: readonly string[],
  label: string,
): void {
  const nonNullField = fields.find((field) => row[field] !== null);
  if (nonNullField) {
    throw new Error(`${label} has unexpected ${nonNullField} metadata.`);
  }
}

function hasOwn(value: Record<string, unknown>, field: string): boolean {
  return Object.prototype.hasOwnProperty.call(value, field);
}
