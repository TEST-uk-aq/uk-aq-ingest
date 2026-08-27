export function recordSosObservationChanges(
  payload: unknown,
  timeseriesId: number,
  changedTimeseriesIds: Set<number>,
): number {
  if (!Array.isArray(payload) || payload.length !== 1) {
    throw new Error(
      "SOS observation upsert returned an invalid result row count.",
    );
  }

  const row = payload[0];
  if (!row || typeof row !== "object" || Array.isArray(row)) {
    throw new Error("SOS observation upsert returned an invalid result row.");
  }

  const rawChangedRows = (row as Record<string, unknown>)
    .observations_upserted;
  if (typeof rawChangedRows !== "number") {
    throw new Error(
      "SOS observation upsert returned an invalid observations_upserted count.",
    );
  }
  const changedRows = rawChangedRows;
  if (!Number.isSafeInteger(changedRows) || changedRows < 0) {
    throw new Error(
      "SOS observation upsert returned an invalid observations_upserted count.",
    );
  }

  if (changedRows > 0) {
    changedTimeseriesIds.add(timeseriesId);
  }
  return changedRows;
}
