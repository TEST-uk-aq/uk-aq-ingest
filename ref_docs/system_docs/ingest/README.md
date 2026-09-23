# Ingest and Daily Stations

## Purpose

This directory contains authoritative UK AQ contracts for connector reference discovery, Daily Stations, connector scheduling ownership, connector/network ingest enablement, observation transport and selected connector-specific ingest behaviour.

This README is an area router. Start with [`../SYSTEM_OVERVIEW.md`](../SYSTEM_OVERVIEW.md), then choose the smallest route below.

## Ownership boundary

- runtime implementation: primarily `TEST-uk-aq/uk-aq-ingest`;
- canonical database definitions/migrations: `TEST-uk-aq/uk-aq-schema`;
- active prose authority: `TEST-uk-aq/uk-aq-system-docs/system_docs/`.

Repository-local implementation docs and historical plans are contextual only and do not override active contracts here.

## Daily Stations and scheduler routes

### Cross-connector Daily Stations orchestration or scheduler ownership

Start with [`contract.md`](contract.md).

Use it for general Daily Stations ordering/idempotency/failure rules, connector Cloudflare scheduler ownership, cross-database mirror gating and cross-connector reference-discovery boundaries.

Add only the specialist contract whose component is changing.

### Isolated SOS Daily Stations failure handling

Read:

1. [`contract.md`](contract.md)
2. [`daily_stations_sos_isolation_contract.md`](daily_stations_sos_isolation_contract.md)

Use this only for the two isolated SOS reference stages, continuation of independent work, mirror eligibility after SOS failure, polling cleanup and the Finished-with-warning health semantics defined by the specialist contract.

Normal SOS observation ingest/fallback remains under [`sos/README.md`](sos/README.md).

### Network catalogue and core mirror

Read:

1. [`contract.md`](contract.md)
2. [`network_catalogue_mirror_contract.md`](network_catalogue_mirror_contract.md)

Use this for authoritative IngestDB `uk_aq_core.networks`, stable numeric network IDs, complete row mirroring, network-before-connector/station ordering and required ObsAQIDB schema/RPC surfaces.

### Breathe London Nodes deterministic reference discovery

Read:

1. [`contract.md`](contract.md)
2. [`blondon_nodes_reference_discovery_contract.md`](blondon_nodes_reference_discovery_contract.md)

Use this for deterministic Nodes phenomena/timeseries identity, the `4 × active stations` reference set, discovery versus quarter-hour ingest ownership and shared reference-building helpers.

For source observations add only the relevant Nodes raw/normalisation/transport contract below.

## Observation ingest routes

### Shared compact-ingest invariants

Read [`compact_observation_transport_and_metadata_ownership_contract.md`](compact_observation_transport_and_metadata_ownership_contract.md) when a task crosses both reference/metadata ownership and observation transport, or when changing a shared compact-ingest invariant.

For a bounded task, add only the relevant specialist contract below.

### Station/timeseries metadata ownership

Read:

1. [`compact_observation_transport_and_metadata_ownership_contract.md`](compact_observation_transport_and_metadata_ownership_contract.md)
2. [`station_metadata_ownership_contract.md`](station_metadata_ownership_contract.md)

Use this for:

- normal metadata authority by connector;
- Sensor.Community immediate discovery, presence and descriptive fingerprints;
- OpenAQ full-catalogue-fetch boundary;
- static timeseries metadata versus changing latest-value state;
- metadata-before-observation reference ordering.

Do not load compact RPC/Pub/Sub detail unless the wire/database transport is also changing.

### Compact observation database transport

Read:

1. [`compact_observation_transport_and_metadata_ownership_contract.md`](compact_observation_transport_and_metadata_ownership_contract.md)
2. [`compact_observation_transport_contract.md`](compact_observation_transport_contract.md)

Use this for:

- `timeseries_id` compact wire identity;
- IngestDB observation and latest-value compact RPCs;
- ObsAQIDB compact observation RPC;
- Pub/Sub writer/delivery boundary;
- retry, batching and request-body egress measurement;
- additive TEST rollout and transport rollback.

Add metadata ownership only when reference creation/lifecycle is also changing.

### Observation acquisition-method provenance

Read [`observation_acquisition_method_contract.md`](observation_acquisition_method_contract.md).

Add the compact transport umbrella/specialist only when the writer/transport boundary is involved. Current SOS acquisition methods include `sos` and `ukair_html`; acquisition method does not create a second logical observation identity.

For actual SOS HTML fallback also read [`sos/README.md`](sos/README.md).

### Breathe London Nodes source normalisation

Read [`blondon_nodes_source_observation_normalisation_contract.md`](blondon_nodes_source_observation_normalisation_contract.md).

Add only when relevant:

- [`blondon_nodes_raw_capture_contract.md`](blondon_nodes_raw_capture_contract.md) for source capture;
- [`compact_observation_transport_contract.md`](compact_observation_transport_contract.md) for post-normalisation observation writer/transport;
- [`station_metadata_ownership_contract.md`](station_metadata_ownership_contract.md) when observation ingest reference ownership is involved;
- [`blondon_nodes_reference_discovery_contract.md`](blondon_nodes_reference_discovery_contract.md) for deterministic Daily Stations reference identity.

### Breathe London Nodes raw source capture

Read [`blondon_nodes_raw_capture_contract.md`](blondon_nodes_raw_capture_contract.md).

Add source normalisation only if crossing from raw evidence into logical observation selection.

### OpenAQ self-scheduling

Read [`openaq_self_scheduling_due_state_contract.md`](openaq_self_scheduling_due_state_contract.md).

Add [`contract.md`](contract.md) only for cross-connector scheduler ownership/dispatch invariants. Add metadata or compact transport only when those boundaries actually change.

### Connector and network ingest enablement

Read [`network_ingest_enablement_contract.md`](network_ingest_enablement_contract.md).

Add `contract.md` only when the change also enters scheduler ownership or Daily Stations orchestration/reference mirroring.

`connectors.poll_enabled` and `networks.ingest_enabled` remain distinct concepts.

### Future high-frequency canonical observation product

Status: **future implementation authority; not current runtime behaviour**.

For auditing or designing sources that may emit observations more frequently than the canonical public/history product requires, read:

1. [`high_frequency_observation_canonicalisation_contract.md`](high_frequency_observation_canonicalisation_contract.md)
2. the connector-specific future contract if one exists.

For Sensor.Community add:

- [`sensorcommunity_canonical_observation_contract.md`](sensorcommunity_canonical_observation_contract.md)
- [`../integrity_factory/ifw_sensorcommunity_source_contract.md`](../integrity_factory/ifw_sensorcommunity_source_contract.md) when long-term raw source evidence or IFW reconstruction is involved.

These contracts intentionally do **not** change current compact-ingest semantics. TEST may continue the current raw Sensor.Community ingest while cadence and cleansing work is performed; the contracts do not authorise LIVE re-enable or public display.

The current audit/implementation sequencing is documented non-authoritatively under `plans/2026-09-01 High-frequency observation canonicalisation/`.

### UK-AIR SOS observation ingestion

Start with [`sos/README.md`](sos/README.md).

The SOS sub-area routes normal polling, current-day HTML fallback, parsing, acquisition provenance, interfaces, operations and validation separately.

## Ingest plus historical R2 repair

Start with the relevant ingest route above.

Add [`../r2_history/README.md`](../r2_history/README.md) only when the task changes historical source evidence/repair, Integrity, R2 publication, timeseries reconciliation or another historical-storage boundary.

A normal current-ingest task should not preload R2-history contracts merely because the same connector also supports historical repair.

## Contract catalogue

| Contract | Primary scope |
|---|---|
| [`contract.md`](contract.md) | Daily Stations cross-connector orchestration and connector scheduler ownership |
| [`daily_stations_sos_isolation_contract.md`](daily_stations_sos_isolation_contract.md) | Narrow isolated SOS reference-stage failure behaviour |
| [`network_catalogue_mirror_contract.md`](network_catalogue_mirror_contract.md) | IngestDB-to-ObsAQIDB network catalogue/core mirror authority |
| [`blondon_nodes_reference_discovery_contract.md`](blondon_nodes_reference_discovery_contract.md) | Deterministic Nodes Daily Stations phenomena/timeseries discovery |
| [`compact_observation_transport_and_metadata_ownership_contract.md`](compact_observation_transport_and_metadata_ownership_contract.md) | Shared compact-ingest invariants/boundary |
| [`station_metadata_ownership_contract.md`](station_metadata_ownership_contract.md) | Station/timeseries reference metadata ownership and Sensor.Community lifecycle split |
| [`compact_observation_transport_contract.md`](compact_observation_transport_contract.md) | Compact observation/latest-value RPC, Pub/Sub delivery and egress transport |
| [`observation_acquisition_method_contract.md`](observation_acquisition_method_contract.md) | Additive observation acquisition provenance |
| [`blondon_nodes_source_observation_normalisation_contract.md`](blondon_nodes_source_observation_normalisation_contract.md) | Nodes duplicate logical-timestamp normalisation |
| [`blondon_nodes_raw_capture_contract.md`](blondon_nodes_raw_capture_contract.md) | Nodes raw `/SensorData` evidence capture |
| [`openaq_self_scheduling_due_state_contract.md`](openaq_self_scheduling_due_state_contract.md) | OpenAQ checkpoint-derived next-due scheduling |
| [`network_ingest_enablement_contract.md`](network_ingest_enablement_contract.md) | Connector-wide/network-level ingest-enable semantics |
| [`high_frequency_observation_canonicalisation_contract.md`](high_frequency_observation_canonicalisation_contract.md) | Future explicitly designated high-frequency source to five-minute canonical product boundary |
| [`sensorcommunity_canonical_observation_contract.md`](sensorcommunity_canonical_observation_contract.md) | Future Sensor.Community raw/cleansing/five-minute canonical serving boundary |
| [`sos/`](sos/) | UK-AIR SOS observation polling and authorised HTML fallback |

## Implementation guidance

After selecting a route, inspect only the relevant implementation files. Typical locations are:

- `uk-aq-ingest/scripts/` for Daily Stations/connector scripts;
- `uk-aq-ingest/workers/` for connector Cloud Run services;
- `uk-aq-ingest/supabase/functions/` for ingest handlers/shared clients;
- `uk-aq-ingest/.github/workflows/` for connector deployment workflows;
- `uk-aq-schema/schemas/` and its active migration root for canonical database surfaces.

## Validation boundary

Before implementation, confirm only structural viability required by the selected contract. Do not create a broad speculative test programme.

Functional acceptance occurs through real TEST operation after deployment using the selected contract's acceptance evidence.

Codex and other coding agents are read-only consumers of `system_docs/`; behavioural documentation changes are handed back to ChatGPT in Chat mode.
