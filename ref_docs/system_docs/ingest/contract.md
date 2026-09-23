# Ingest and Daily Stations contract

## Purpose

Daily Stations establishes and refreshes connector reference data required by normal observation ingestion and downstream reference synchronisation.

It is not only a station-name refresh. Where timeseries identities are deterministic from station identity and a fixed source-species contract, Daily Stations MUST establish those references before normal observation polling depends on them.

This contract is the cross-connector Daily Stations and scheduler/orchestration umbrella. Specialist reference behaviour is owned by:

- [`daily_stations_sos_isolation_contract.md`](daily_stations_sos_isolation_contract.md): narrow isolated SOS reference-stage failure handling;
- [`network_catalogue_mirror_contract.md`](network_catalogue_mirror_contract.md): authoritative IngestDB-to-ObsAQIDB network catalogue mirror;
- [`blondon_nodes_reference_discovery_contract.md`](blondon_nodes_reference_discovery_contract.md): deterministic Breathe London Nodes phenomena/timeseries discovery.

## Cross-repository authority

This contract remains authoritative even though:

- runtime implementation is primarily in `uk-aq-ingest`;
- canonical SQL is owned by `uk-aq-schema`;
- active prose is stored in `uk-aq-system-docs`.

Implementation/schema changes MUST preserve this contract unless the behaviour is intentionally re-contracted through the system-docs owner.

## Compact observation transport boundary

Compact observation transport, observation-write metadata ownership, Sensor.Community metadata change detection, OpenAQ normal station-fetch behaviour and compact ObsAQIDB delivery are governed by [`compact_observation_transport_and_metadata_ownership_contract.md`](compact_observation_transport_and_metadata_ownership_contract.md).

That narrower contract MAY change transport/metadata-write behaviour while preserving the Daily Stations identity, lifecycle, scheduler and reference-discovery rules here.

## Connector ingest scheduler ownership

For TEST Cloud Run connector-ingest services, recurring external dispatch is owned by the Cloudflare ingest scheduler and its D1 job configuration.

This applies to:

- Breathe London Nodes;
- Breathe London Communities;
- UK-AIR SOS;
- Sensor.Community;
- the OpenAQ external safety trigger.

Cloud Run deployment workflows MUST:

- deploy/configure the connector service;
- preserve authenticated application-level dispatch through the shared upstream secret;
- reconcile the deployed Cloud Run service URL into the corresponding Cloudflare scheduler D1 job.

Cloud Run deployment workflows MUST NOT create, update, resume or otherwise manage Google Cloud Scheduler jobs for connector ingestion.

The database value:

```text
scheduler_backend = 'google_cloud_run'
```

identifies the execution backend. It does not mean Google Cloud Scheduler owns dispatch.

OpenAQ retains worker-created one-off Cloud Tasks as its primary self-scheduling path. The Cloudflare OpenAQ safety job remains an external recovery trigger. Removing Google Cloud Scheduler MUST NOT remove/disable the OpenAQ Cloud Tasks queue, task creation, invoker identity or associated IAM permissions.

Scheduler-ownership changes MUST NOT alter connector poll intervals, due-state checks, dispatch claims, overlap protection, retries or connector-specific work selection unless separately authorised.

For OpenAQ due-state semantics also read [`openaq_self_scheduling_due_state_contract.md`](openaq_self_scheduling_due_state_contract.md).

## Daily Stations general contract

Daily Stations MUST:

- resolve connector identity by `connector_code`, not assumed numeric IDs;
- make connector-specific stages idempotent and safe to rerun;
- refresh station reference rows without deleting historical observations/timeseries;
- establish all deterministic reference rows required by a connector before downstream reference mirroring, except for the narrow SOS isolation path;
- fail when a required connector stage leaves internally inconsistent reference state;
- run IngestDB-to-ObsAQIDB core reference synchronisation only after required IngestDB reference stages succeed, except for the narrow SOS isolation path;
- mirror authoritative IngestDB `uk_aq_core.networks` before connector/station rows that reference network IDs;
- preserve existing SOS/OpenAQ polling pause/resume safety unless a separate contract intentionally changes it.

Daily Stations MUST NOT:

- fetch/write routine observation measurements merely to discover deterministic reference rows;
- reset observation checkpoints or normal polling schedule state;
- treat zero connector work as success when active source stations exist but required references are missing;
- delete historical timeseries because a station has been removed from the active source list;
- treat ObsAQIDB network seed values as independent runtime configuration.

## Connector-stage modularity

The workflow SHOULD remain an orchestrator of independently runnable connector/reference stages rather than embedding substantial connector-specific logic inline.

Connector-specific commands/modules MAY share deterministic reference-building helpers with normal observation ingestion where that is required for idempotent self-repair.

Such modularisation MUST NOT silently alter:

- connector scheduling;
- polling/due-state semantics;
- retry or overlap protection;
- observation checkpoints;
- observation writes or secondary writes;
- historical retention/R2 behaviour.

## Core reference mirror gate

The final ObsAQIDB core mirror runs only after required IngestDB reference work is in a safe authoritative state.

Detailed network row authority/order is defined by [`network_catalogue_mirror_contract.md`](network_catalogue_mirror_contract.md).

The narrow SOS exception MAY allow the mirror to proceed despite an SOS reference-stage failure when all non-isolated required stages succeeded and no independent integrity guard declares the source state unsafe. When that isolated SOS failure is the only degraded part of the run, the specialist contract MAY also allow final Daily Stations health to remain `Finished` while carrying an explicit structured warning in the run summary. That warning is not evidence that the SOS reference refresh succeeded. The exception is defined only by [`daily_stations_sos_isolation_contract.md`](daily_stations_sos_isolation_contract.md).

A successful source-side refresh is not sufficient when the required final ObsAQIDB mirror fails.

## Cross-database schema boundary

Daily Stations is a cross-database workflow. Any runtime change that depends on a changed mirror schema/RPC contract is incomplete until both source IngestDB and destination ObsAQIDB can represent the required canonical state.

Canonical schema/migrations belong to `uk-aq-schema`.

Before deploying a runtime that depends on a changed database surface, perform only the focused structural checks necessary to prove the required table/RPC shape and permissions exist in TEST. Functional acceptance then occurs through a real Daily Stations TEST run.

The network-specific current migration/RPC dependency is documented in [`network_catalogue_mirror_contract.md`](network_catalogue_mirror_contract.md).

## Structural viability before implementation

For a general Daily Stations/scheduler change, validate only the structure required by that task, for example:

- affected connector stage can be invoked in the required order;
- scheduler ownership/configuration remains unambiguous;
- mirror prerequisites and failure propagation remain representable;
- any required schema/RPC surface can represent the contracted reference state.

Use the relevant specialist contract for connector/network/SOS-specific targeted checks.

No broad speculative pre-implementation test suite is required.

## TEST operational validation

Functional validation occurs through real TEST operation after deployment.

At the general Daily Stations level confirm as relevant that:

- required connector reference stages run in contracted order;
- idempotent reruns retain stable identities rather than create duplicates;
- required final reference mirroring succeeds or the run fails truthfully;
- scheduler/polling state remains consistent with the unchanged ownership model;
- failures remain visible rather than being converted into false success.

Use the specialist contract for Breathe London Nodes, network mirror or isolated SOS acceptance evidence.

## Explicit non-goals

This umbrella does not authorise:

- changing stable canonical network identities merely for mirroring;
- changing Breathe London Nodes species/identity rules outside its specialist contract;
- changing quarter-hour connector due scheduling;
- changing observation retention or R2 history;
- merging connector identities;
- deleting historical timeseries;
- changing SOS, OpenAQ, Sensor.Community or Breathe London Communities lifecycle/source-observation behaviour except where an active narrower contract explicitly authorises it.
