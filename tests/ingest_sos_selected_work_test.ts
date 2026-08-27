import {
  normalizeSosSelectedWorkRpcResponse,
  readSosCompactChildPayload,
} from "../supabase/functions/ingest_sos/selected_work.ts";

const DAY_UTC = "2026-08-27";

Deno.test("SOS selected-work transport deduplicates metadata and retains mapping candidates", () => {
  const first = selectedWorkRow({
    bridge_site_ref: "SITE1",
    bridge_uk_air_ref: "UKA001",
    bridge_pollutant_code: "no2",
    bridge_station_id: 10,
    bridge_station_ref: "station-10",
    bridge_timeseries_id: 101,
    bridge_timeseries_ref: "series-101",
    bridge_valid_from_day_utc: "2026-01-01",
  });
  const second = {
    ...first,
    bridge_site_ref: "SITE2",
    bridge_uk_air_ref: "UKA002",
  };
  const stationOnly = selectedWorkRow({
    station_id: 20,
    station_ref: "station-20",
    timeseries_id: null,
    timeseries_ref: null,
    service_ref: null,
    phenomenon_id: null,
    last_value_at: null,
    uom: null,
    bridge_site_ref: null,
    bridge_uk_air_ref: null,
    bridge_pollutant_code: null,
    bridge_station_id: null,
    bridge_station_ref: null,
    bridge_timeseries_id: null,
    bridge_timeseries_ref: null,
    bridge_valid_from_day_utc: null,
    bridge_valid_to_day_utc: null,
  });

  const plan = normalizeSosSelectedWorkRpcResponse([
    first,
    second,
    stationOnly,
  ]);
  assertEquals(plan.stationRows.length, 2);
  assertEquals(plan.timeseriesRows.length, 1);
  assertEquals(plan.selectedTimeseries.length, 1);
  assertEquals(plan.selectedTimeseries[0].phenomenon_id, "501");
  assertEquals(plan.bridgeRows.length, 2);

  const compact = readSosCompactChildPayload(
    {
      timeseries_ids: ["101"],
      selected_timeseries: plan.selectedTimeseries,
      uk_air_html_bridge_rows: plan.bridgeRows,
      uk_air_html_bridge_day_utc: DAY_UTC,
    },
    [101],
  );
  assertEquals(compact?.selectedTimeseries.length, 1);
  assertEquals(compact?.bridgeRows.length, 2);
});

Deno.test("SOS selected-work transport fails closed on conflicting timeseries metadata", () => {
  const row = selectedWorkRow();
  assertThrows(() =>
    normalizeSosSelectedWorkRpcResponse([
      row,
      { ...row, uom: "ppb" },
    ])
  );
});

Deno.test("SOS compact child payload rejects mismatched IDs and out-of-scope bridge rows", () => {
  const plan = normalizeSosSelectedWorkRpcResponse([selectedWorkRow()]);
  assertThrows(() =>
    readSosCompactChildPayload(
      {
        selected_timeseries: plan.selectedTimeseries,
        uk_air_html_bridge_rows: plan.bridgeRows,
        uk_air_html_bridge_day_utc: DAY_UTC,
      },
      [101, 102],
    )
  );
  assertThrows(() =>
    readSosCompactChildPayload(
      {
        selected_timeseries: plan.selectedTimeseries,
        uk_air_html_bridge_rows: [{
          ...plan.bridgeRows[0],
          timeseries_id: 999,
        }],
        uk_air_html_bridge_day_utc: DAY_UTC,
      },
      [101],
    )
  );
});

Deno.test("SOS compact child payload keeps the legacy path when compact fields are absent", () => {
  const compact = readSosCompactChildPayload(
    { timeseries_ids: ["101"] },
    [101],
  );
  assertEquals(compact, null);
});

function selectedWorkRow(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    station_id: 10,
    station_ref: "station-10",
    timeseries_id: 101,
    timeseries_ref: "series-101",
    service_ref: "service-1",
    phenomenon_id: 501,
    last_value_at: "2026-08-27T10:00:00Z",
    uom: "ug.m-3",
    bridge_site_ref: "SITE1",
    bridge_uk_air_ref: "UKA001",
    bridge_pollutant_code: "no2",
    bridge_station_id: 10,
    bridge_station_ref: "station-10",
    bridge_timeseries_id: 101,
    bridge_timeseries_ref: "series-101",
    bridge_valid_from_day_utc: "2026-01-01",
    bridge_valid_to_day_utc: null,
    ...overrides,
  };
}

function assertEquals(actual: unknown, expected: unknown): void {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(
      `Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}.`,
    );
  }
}

function assertThrows(callback: () => unknown): void {
  let threw = false;
  try {
    callback();
  } catch {
    threw = true;
  }
  if (!threw) {
    throw new Error("Expected callback to throw.");
  }
}
