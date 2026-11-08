/** Prune owns whole UTC days. Normal ingest starts at today's midnight minus N days. */
export function ingestRetentionDays(raw: string | undefined): number {
  const days = Number(raw);
  if (!raw || !/^[1-9][0-9]*$/.test(raw) || !Number.isSafeInteger(days) || days < 2) {
    throw new Error("INGESTDB_RETENTION_DAYS must be configured as an integer of at least 2");
  }
  return days;
}

export function ingestRetentionStart(now: Date, days: number): Date {
  return new Date(Date.UTC(
    now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - days,
  ));
}

export function eligibleIngestTimestamp(
  observedAt: unknown,
  now: Date,
  days: number,
): string | null {
  if (typeof observedAt !== "string" || !observedAt.trim()) return null;
  const observedMs = Date.parse(observedAt);
  if (!Number.isFinite(observedMs) || observedMs > now.getTime() ||
    observedMs < ingestRetentionStart(now, days).getTime()) return null;
  return new Date(observedMs).toISOString();
}
