# Breathe London Nodes Daily Stations reference-discovery contract

## Authority

This contract owns deterministic Breathe London Nodes station-reference/timeseries discovery performed by Daily Stations.

Read [`contract.md`](contract.md) for general Daily Stations ordering and scheduler rules. Normal Nodes observation capture/normalisation/transport remains under the existing narrower Nodes and compact-observation contracts.

## Stable identities

Connector identity:

```text
connector_code = blondon_nodes
service_ref = breathelondon
network_code = breathelondon
```

Station identity:

```text
connector_id + station_ref
```

where `station_ref` is the Breathe London Nodes `SiteCode`.

Timeseries identity:

```text
connector_id + timeseries_ref
```

For each active Nodes station the deterministic `timeseries_ref` set is:

```text
<station_ref>:PM25
<station_ref>:NO2
<station_ref>:PM25Index
<station_ref>:NO2Index
```

These identities MUST remain stable across repeated Daily Stations and quarter-hour observation runs.

## Daily Stations discovery stage

After Nodes station-list import, Daily Stations MUST run Nodes reference discovery that:

1. resolves connector `blondon_nodes` by `connector_code`;
2. selects active Nodes stations where `service_ref = 'breathelondon'` and `removed_at is null`;
3. upserts the four canonical Nodes phenomena through the current central phenomena RPC contract;
4. upserts four deterministic timeseries rows for every selected active station;
5. preserves existing timeseries IDs through unique identity `(connector_id, timeseries_ref)`;
6. reports active-station count, expected-timeseries count, pre-existing count, inserted/repaired count and final count;
7. fails when active Nodes stations exist but the final required deterministic timeseries set is incomplete.

For `N` active Nodes stations, the required active-station reference set contains exactly `4 × N` distinct `(connector_id, timeseries_ref)` identities.

Historical timeseries belonging to removed stations MAY remain in `uk_aq_core.timeseries` and MUST NOT be deleted by this discovery stage.

## Canonical phenomena mappings

| Source label | Source species | Mapping kind | Canonical observed property | AQI eligible |
|---|---|---|---|---|
| `breathelondon_nodes:pm2.5` | `PM25` | `raw_observed_property` | `pm25` | yes |
| `breathelondon_nodes:no2` | `NO2` | `raw_observed_property` | `no2` | yes |
| `breathelondon_nodes:pm2.5:daqi` | `PM25Index` | `derived_index` | `pm25index` | no |
| `breathelondon_nodes:no2:daqi` | `NO2Index` | `derived_index` | `no2index` | no |

Source-provided index observations are retained source observations. They MUST remain distinct from UK AQ calculated AQI/DAQI products.

Discovery MUST fail rather than silently create an unknown mapping or continue with a mapping warning.

## Discovery versus observation ingestion

Daily Stations owns creation/repair of the complete deterministic Nodes reference set.

The discovery stage MUST NOT:

- call Breathe London `/SensorData`;
- write observations;
- update station observation checkpoints;
- update timeseries value bounds.

Quarter-hour Nodes ingest owns due-station selection, `/SensorData` calls, observation writes, checkpoints and value-bound updates.

Quarter-hour ingest SHOULD retain an idempotent timeseries upsert as a self-repair guard, but MUST NOT be the sole normal creator of the deterministic Nodes reference set.

A quarter-hour run that processes observations successfully while authoritative IngestDB contains no Nodes timeseries is an invalid state and MUST be surfaced as a deployment/database-target mismatch rather than accepted as normal operation.

## Shared implementation boundary

The deterministic Nodes species definitions and timeseries-row construction MUST have one shared implementation used by:

- Daily Stations reference discovery;
- quarter-hour Nodes observation-ingest self-repair.

The Daily Stations workflow SHOULD remain an orchestrator of independently runnable connector stages. Connector-specific station import/reference discovery SHOULD live in connector modules or commands rather than large inline workflow scripts.

Modularisation MUST NOT change scheduling, polling, retry, checkpoint, observation-write or secondary-write behaviour unless separately authorised.

## Agreed future quarter-hour resilience extension

**Status: future implementation authority only; current quarter-hour behaviour is unchanged until deployed and accepted in TEST.**

For the upcoming matched Communities/Nodes resilience work, the shared [Breathe London reference self-repair and isolated-failure contract](blondon_reference_failure_isolation_contract.md) adds read-before-create repair of missing required identities in quarter-hour Nodes ingest. If one station remains unrepairable despite an available reference database, the future ingest skips that station, retains its previous checkpoint, continues other resolvable due stations and reports a truthful `partial` result. Shared database/mapping outages, all-selected-station reference failure and existing observation-writer integrity failures remain run-fatal. Daily Nodes discovery continues to fail on any incomplete active-station reference set and retains the normal final mirror gate.

The existing four Nodes species, stable ID rules and shared deterministic builder in this contract remain authoritative. This future extension MUST NOT be described as current deployed behaviour before TEST acceptance.

## ObsAQIDB reference mirror interaction

The final core reference mirror normally runs only after Nodes station and timeseries discovery succeeds.

The mirrored core set MUST include Nodes stations and timeseries with the same stable identities as authoritative IngestDB.

A schema-compatible mirror that copies zero Nodes timeseries while active Nodes stations require a non-zero deterministic set MUST be treated as incomplete.

Network-catalogue ordering/authority is owned separately by [`network_catalogue_mirror_contract.md`](network_catalogue_mirror_contract.md).

The isolated SOS exception MAY permit the final mirror despite an unrelated SOS failure, but does not weaken Nodes discovery success requirements.

## Structural viability before implementation

For a Nodes reference-discovery change, only targeted structural checks are required before implementation/deployment:

- confirm unique timeseries identity remains `(connector_id, timeseries_ref)`;
- confirm the central phenomena RPC exposes fields consumed by the shared Nodes mapping helper;
- confirm Daily Stations can invoke the connector-specific discovery stage before the ObsAQIDB mirror;
- confirm the mirror can carry required Nodes reference identities.

No broad speculative pre-implementation test suite is required.

## TEST operational validation

After deployment, validate through real TEST operation:

1. run Daily Stations;
2. confirm Nodes station import completes;
3. confirm active Nodes timeseries count is `4 × active Nodes stations`;
4. run a normal quarter-hour Nodes ingest and confirm it uses existing timeseries identities;
5. confirm the final ObsAQIDB mirror contains the same Nodes timeseries identities;
6. rerun Daily Stations and confirm identities remain stable with no duplicate timeseries.

## Non-goals

This contract does not authorise:

- changing the four Nodes source species;
- changing quarter-hour due scheduling;
- changing observation retention or R2 history;
- merging Nodes and Communities connector identities;
- deleting historical Nodes timeseries;
- changing public network naming independently of the network catalogue authority.
