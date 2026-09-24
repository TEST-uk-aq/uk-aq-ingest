# Breathe London reference self-repair and isolated-failure contract

**Status: future implementation authority, not current deployed behaviour.** This contract is agreed for the upcoming TEST implementation for both Breathe London connectors. After real TEST acceptance, ChatGPT in Chat mode must update the documentation status to current before describing the behaviour as deployed.

## Authority and reading order

This contract owns the shared quarter-hour missing-timeseries repair and isolated-failure boundary for `blondon_nodes` and `blondon_communities`. Connector-specific deterministic identities, species, canonical mappings and Daily Stations reference sets remain owned by [`blondon_nodes_reference_discovery_contract.md`](blondon_nodes_reference_discovery_contract.md) and [`blondon_communities_reference_discovery_contract.md`](blondon_communities_reference_discovery_contract.md). Read [`contract.md`](contract.md) for Daily Stations ordering and the final ObsAQIDB mirror gate, and [`station_metadata_ownership_contract.md`](station_metadata_ownership_contract.md) for normal catalogue authority.

## Shared ownership and normal resolution

- Daily Stations MUST be the normal creator and metadata repairer of each connector's complete deterministic reference set. Each connector's existing station-list import MUST finish before its deterministic timeseries discovery. Reference discovery MUST finish before the final cross-database core mirror.
- Quarter-hour observation ingest MUST use persisted station identities and MUST NOT run an entire source station catalogue refresh solely because a timeseries is missing.
- For selected, due stations, ingest MUST fetch existing timeseries IDs first, build only missing deterministic rows with the connector's canonical phenomenon and observed-property mappings, idempotently create those missing rows, then re-read and verify their persisted IDs before dependent observation writes.
- The connector-specific deterministic builder and phenomena mapping logic MUST be shared between Daily Stations discovery and ingest-side self-repair within each connector. The two connectors MUST NOT be merged into a single connector or share source-species identities.
- Normal ingest MUST NOT resend complete static metadata for existing valid timeseries; normal value-bound updates remain on their existing compact path. Daily Stations MAY correct known mismatches in static reference metadata without changing existing IDs.
- Neither path may invent placeholder timeseries IDs, change an existing timeseries ID, overwrite latest values with null, reset checkpoints, delete historical references or publish an observation before its required identity is resolved.
- Use the existing authorised unique identity and canonical schema/RPC contract. Resolve numeric connector, phenomenon and observed-property IDs from the target environment; never hard-code TEST numeric IDs or assume that they match LIVE.

## Failure classification and partial continuation

An unresolved identity is not a reason to stop every other station. After a targeted, bounded self-repair attempt:

1. If one selected station still lacks any required timeseries identity, ingest MUST isolate that station for this run, record its missing refs and repair failure in structured diagnostics, and continue with the other resolvable due stations. It MUST NOT publish observations or value updates for the isolated station.
2. A station isolated because reference resolution failed MUST retain its previous successful observation and due checkpoints. If retry state is recorded, it MUST make the station eligible for prompt retry without fabricating a successful poll or advancing past data.
3. If at least one otherwise due station is processed normally, the overall run MUST report `partial` and expose counts and identifiers for isolated stations, missing identities, repair attempts and failures. An HTTP-success response or Cloud Run wrapper MUST NOT turn this into an unqualified `succeeded` run. Persist the actionable failure in the connector's established error/diagnostic path without dumping secrets or full upstream payloads.
4. Expected no-due-station or no-eligible-station runs MUST preserve each connector's existing honest `skipped` semantics. Do not disguise a failed reference resolution as no work.
5. Shared database unavailability, failed connector/phenomena resolution, inability to establish the canonical mappings, failed reference-ID reads, a deployment/database-target mismatch, or an outage affecting all selected due stations MUST fail the run. Such failures MUST NOT be labelled as isolated station errors or a successful partial run.
6. Observation-writer integrity errors and other existing fail-closed transport boundaries MUST retain their current severity. This contract only isolates individual station reference failures; it does not make unrelated failures fail-open.

A repair operation returning successfully is not itself proof of repair. Read back the required rows, verify station/connector/service/phenomenon/observed-property links and stable pre-existing IDs, and only then permit dependent observations. A database-level uniqueness race MUST be handled idempotently by re-reading the canonical identity rather than creating a duplicate.

## Daily Stations and mirror failure boundary

The quarter-hour partial-run allowance does not apply to Daily Stations. Each Daily Stations connector reference-discovery stage MUST verify its complete deterministic reference set, report its missing/mismatched/changed-ID counts and fail if incomplete. The final ObsAQIDB core reference mirror MUST NOT run against an incomplete Breathe London reference set. The separately contracted isolated SOS exception remains unchanged.

Observation ingest self-repair MUST NOT assume that newly created IngestDB references are already present in ObsAQIDB. Preserve the existing reference mirror and compact observation delivery safety contract. If safe downstream delivery of a newly created identity depends on mirroring, retain or schedule that prerequisite instead of publishing an unresolved cross-database identity.

## Scope protection

Do not change source polling cadence, station selection, source `enabled`/`SiteActive` interpretation, station retirement, observation timestamps/values, compact RPC payloads, Pub/Sub acknowledgement/retry, raw capture, checkpoint eligibility for successful stations, R2 history or calculated AQI products as part of this work. In particular, `Enabled=N` alone MUST NOT silently become a new Communities station exclusion rule.

## Structural viability

Before implementation, check only the relevant structural prerequisites: existing unique index and upsert target; canonical phenomenon and observed-property resolution; Daily Stations stage ordering; and existing `partial` status/error surfaces in each connector's worker and ingest handler. A targeted guard is justified if an existing writer could emit an observation for an unresolved identity. Do not build a speculative pre-implementation test suite.

## Real TEST acceptance after deployment

Use real TEST Daily Stations and normal quarter-hour runs to confirm both connector reference sets and stable IDs, immediate repair of a genuinely missing reference, continued processing of healthy due stations when one station is unrepairable, truthful `partial` and error diagnostics, unchanged failed station checkpoints, and the final ObsAQIDB mirror gate. Only promote to LIVE following review of TEST evidence and a separate authorised LIVE action.
