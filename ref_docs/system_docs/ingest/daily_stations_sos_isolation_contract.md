# Daily Stations isolated SOS failure contract

## Authority

This contract owns the narrow exception that allows Daily Stations to continue useful independent work after failure of the UK-AIR SOS reference stages and, when the rest of the workflow remains safe and successful, finish with a structured warning rather than fail the whole Daily Stations run.

Read [`contract.md`](contract.md) for general Daily Stations ordering, reference consistency, mirroring and scheduler rules. Normal SOS observation ingestion remains under [`sos/README.md`](sos/README.md).

## Isolated stages

Daily Stations MAY isolate failure of only these two UK-AIR SOS reference stages:

- the initial UK-AIR SOS station/reference synchronisation;
- the later UK-AIR SOS timeseries-discovery stage.

This is a workflow-orchestration exception only.

The underlying SOS command MUST retain truthful failure semantics and MUST return non-zero on fatal failure. The workflow MUST retain the real failed step outcome and MUST NOT rewrite an SOS failure as successful reference refresh.

## Behaviour after an isolated SOS failure

When one or both isolated stages fail:

- independent Breathe London, OpenAQ, Sensor.Community, station-geography, station-name, export and other Daily Stations maintenance SHOULD continue where their own prerequisites remain satisfied;
- the later SOS discovery stage MAY still be attempted after an earlier SOS station-sync failure if UK-AIR availability may have recovered and the discovery command can run safely against the existing authoritative IngestDB reference state;
- existing SOS and OpenAQ polling pause/resume safety MUST preserve pre-run polling state and cleanup MUST remain reachable;
- after all non-isolated required IngestDB reference stages succeed, the IngestDB-to-ObsAQIDB core reference mirror MAY run against the authoritative IngestDB rows that actually exist;
- the mirror MUST NOT synthesise missing SOS reference rows, infer successful SOS refresh or conceal the failed SOS stage;
- any independent reference-integrity guard that detects an inconsistent or unsafe IngestDB state remains fatal and MUST NOT be bypassed because SOS was the originating connector.

If the isolated SOS failure is the only degraded part of the run, and all other required Daily Stations stages, cleanup and the final core reference mirror succeed, the overall Daily Stations workflow MUST finish successfully at the workflow/task level and final Daily Stations health MUST record `Finished`.

That `Finished` state MUST NOT imply that the SOS reference refresh succeeded. The run MUST carry the structured warning defined below.

If any non-isolated required stage fails, polling restoration fails where restoration is required, the final mirror fails, or an independent integrity/safety guard fails, the overall Daily Stations workflow MUST remain failed and final health MUST record `Failed`.

## Structured warning summary

A Daily Stations run that finishes under the isolated SOS exception MUST add a non-empty `warnings` array to the existing daily-task JSON `summary`.

Each warning object MUST be bounded machine-readable JSON and MUST contain:

- `code`: a stable warning identity;
- `reason`: a concise human-readable explanation of what was unavailable or failed.

For the SOS isolation path the warning code MUST be:

`uk_air_sos_reference_refresh_failed`

The warning SHOULD also contain:

- `connector_code: "sos"`;
- `stages`: the failed isolated stage names, using stable workflow-oriented names such as `sync_sos_stations` and `discover_sos_timeseries`;
- bounded upstream diagnostic context when cheaply available, such as an HTTP status or timeout class.

The summary MUST NOT contain credentials, full response bodies or unbounded logs.

Example shape:

```json
{
  "warnings": [
    {
      "code": "uk_air_sos_reference_refresh_failed",
      "reason": "UK-AIR SOS station/reference refresh was unavailable; existing SOS reference data was retained.",
      "connector_code": "sos",
      "stages": [
        "sync_sos_stations",
        "discover_sos_timeseries"
      ]
    }
  ]
}
```

The dashboard presentation of this warning is governed by [`../dashboards/daily_task_warning_status_contract.md`](../dashboards/daily_task_warning_status_contract.md).

## Narrow scope

This exception does **not** authorise blanket `continue-on-error` behaviour for:

- non-SOS connector reference stages;
- database failures;
- ObsAQIDB mirror failures;
- exports;
- integrity guards;
- unrelated Daily Stations work.

It does not weaken normal SOS polling/acquisition contracts and does not change source-observation behaviour.

## Mirror boundary

Running the core reference mirror under this exception means only that the currently authoritative, internally safe IngestDB reference state may still be mirrored.

It does not make the failed SOS stage successful. It means Daily Stations completed all other required work while retaining existing safe SOS reference state and recording the degraded SOS refresh explicitly.

Network/reference row ordering and source-of-truth rules remain owned by [`network_catalogue_mirror_contract.md`](network_catalogue_mirror_contract.md) and [`contract.md`](contract.md).

## Validation

No artificial pre-implementation SOS outage simulation is required.

Before deployment, establish only structural viability of the orchestration change, including that:

- the real failed SOS step outcome remains observable;
- cleanup/polling restoration remains reachable;
- the mirror can be gated on non-isolated prerequisites and independent integrity guards;
- a Finished daily-task run can include the structured `warnings` summary without changing the underlying SOS command exit semantics;
- non-isolated failures still drive the overall workflow/task to `Failed`.

Functional acceptance occurs through a real TEST Daily Stations run when an isolated SOS failure condition occurs. Confirm independent eligible work and the mirror continue, polling state is restored, the SOS failure remains visible, final health is `Finished`, and the JSON summary contains the warning reason and failed SOS stage information.
