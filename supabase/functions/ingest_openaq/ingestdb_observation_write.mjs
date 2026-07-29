/**
 * Run the OpenAQ IngestDB observation write while retaining aggregate
 * statistics from both successful writes and specialised terminal failures.
 */
export async function writeOpenAqIngestDbObservations({
  write,
  aggregateStats,
  isWriteError,
  mergeStats,
  onObservationsUpserted,
  onTerminalError,
}) {
  try {
    const writeStats = await write();
    mergeStats(aggregateStats, writeStats);
    onObservationsUpserted(aggregateStats.committed_rows);
  } catch (error) {
    if (isWriteError(error)) {
      if (error.stats) {
        mergeStats(aggregateStats, error.stats);
      }
      onObservationsUpserted(aggregateStats.committed_rows);
      onTerminalError?.(error);
    }
    throw error;
  }
}
