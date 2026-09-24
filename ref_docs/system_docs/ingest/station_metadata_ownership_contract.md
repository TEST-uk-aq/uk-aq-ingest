# Ingest station and timeseries metadata ownership contract

## Authority

This contract owns normal station/timeseries reference-metadata authority during observation ingestion, including the Sensor.Community discovery/presence exception and OpenAQ catalogue-fetch boundary.

Read [`compact_observation_transport_and_metadata_ownership_contract.md`](compact_observation_transport_and_metadata_ownership_contract.md) for shared compact-ingest objectives and protected behaviour. Compact observation wire/RPC semantics are owned by [`compact_observation_transport_contract.md`](compact_observation_transport_contract.md).

Daily Stations orchestration and deterministic Breathe London Nodes reference discovery remain under [`contract.md`](contract.md) and [`blondon_nodes_reference_discovery_contract.md`](blondon_nodes_reference_discovery_contract.md).

## Normal station metadata authority

| Connector | Normal station metadata authority | Observation-ingest responsibility |
|---|---|---|
| `sos` | Daily Stations | Consume existing station/timeseries identities; do not refresh station catalogue |
| `blondon_nodes` | Daily Stations | Consume existing station identities; retain only authorised narrow timeseries self-repair |
| `blondon_communities` | Daily Stations | Consume existing active station/timeseries identities; do not refresh station catalogue |
| `openaq` | Daily Stations | Consume existing station identities; normal scheduled ingest MUST NOT fetch full OpenAQ locations catalogue |
| `sensorcommunity` | Observation ingest | Discover new station/timeseries identities from country feed, update only changed descriptive metadata, preserve presence semantics, write observations |

A separately authorised repair/migration/diagnostic command MAY rebuild reference data. It MUST NOT silently become normal scheduled metadata authority.

## SOS and Breathe London ownership

Daily Stations remains authority for SOS station additions, metadata refresh and the existing SOS retirement rule.

Daily Stations remains authority for Breathe London Nodes station state and the complete deterministic Nodes timeseries set. Quarter-hour Nodes ingest MAY retain the authorised idempotent timeseries self-repair guard but MUST NOT resend full station metadata merely because observations are being written.

Daily Stations remains authority for Breathe London Communities station additions, metadata refresh and source `EndDate` retirement state. Normal Communities observation ingest MUST continue to skip station catalogue refresh.

Metadata/transport optimisation MUST NOT alter those connector lifecycle rules.

### Agreed future Breathe London reference resilience

**Status: future implementation authority; not the current deployed Communities runtime.** [`blondon_communities_reference_discovery_contract.md`](blondon_communities_reference_discovery_contract.md) adds deterministic Communities timeseries creation/repair to Daily Stations. [`blondon_reference_failure_isolation_contract.md`](blondon_reference_failure_isolation_contract.md) specifies the narrow missing-timeseries ingest-side repair and isolated station-failure behaviour for both Breathe London connectors. These future changes MUST NOT transfer full Breathe London station catalogue authority to quarter-hour ingest or change station retirement, presence or source filtering.

## OpenAQ station lifecycle

Daily Stations is normal authority for OpenAQ station additions and metadata refresh.

Normal scheduled OpenAQ observation ingest MUST keep full catalogue discovery disabled. The promoted TEST configuration was verified with:

```text
OPENAQ_INGEST_STATION_FETCH=false
```

The optional ingest-side station-fetch capability MAY remain only as an explicitly selected repair, migration or diagnostic path.

The current OpenAQ catalogue import upserts returned locations and clears `removed_at` for returned stations. It does not define retirement by catalogue absence.

This contract therefore MUST NOT introduce OpenAQ retirement-by-absence. A future retirement-by-absence rule requires an explicit station-lifecycle contract change.

## Sensor.Community lifecycle split

Sensor.Community is the deliberate exception to Daily Stations metadata ownership because its normal country feed contains both metadata and observations needed for immediate discovery.

Normal Sensor.Community ingest MUST continue to support a new station/timeseries encountered in the feed without waiting for a daily catalogue run.

It MUST keep these concerns distinct:

1. station identity and descriptive metadata;
2. station presence/liveness;
3. timeseries identity and descriptive metadata;
4. observation data;
5. changing timeseries latest-value state.

### Presence

Every station seen in a successful country feed MUST preserve current presence semantics:

- update `last_seen_at` to the applicable seen/observation time;
- clear `removed_at` to `NULL` so a previously removed station is restored when it reappears.

Presence MUST be updated independently of descriptive metadata change detection.

The compact presence operation SHOULD be keyed by internal `station_id`. TEST uses `uk_aq_core.stations.id bigint`, so a columnar presence RPC MAY use `bigint[]` station IDs with corresponding `timestamptz[]` seen times.

Presence handling MUST NOT overwrite descriptive fields with null/stale values.

### Descriptive metadata fingerprint

Unchanged Sensor.Community descriptive station metadata MUST NOT be resent in full on every poll.

Canonical descriptive fingerprint fields are:

- `label`;
- effective `station_name` only where connector overwrite policy allows the source to govern it;
- `station_type`;
- `station_exposure`;
- canonical longitude/latitude represented by persisted `geometry`.

The fingerprint MUST NOT include:

- `station_ref`;
- `connector_id` or `service_ref`;
- `last_seen_at`;
- `removed_at`;
- observation values/timestamps.

Where `overwrite_station_name=false`, source `station_name` MUST NOT cause repeated metadata changes and MUST be excluded from the source-controlled fingerprint.

Fingerprint canonicalisation MUST preserve coordinate precision and make harmless representation differences stable. A suitable v1 form is deterministic SHA-256 over canonical UTF-8 text fields plus IEEE-754 float64 coordinate bytes/hex. Another implementation MAY be used if deterministic across runtime/database comparison and coordinates are not rounded.

The receiver SHOULD expose a compact resolver returning only minimum comparison state, normally station identity plus current descriptive fingerprint.

Per station:

```text
unknown
    -> send required new-station metadata

known + fingerprint changed
    -> send required descriptive metadata update

known + fingerprint unchanged
    -> do not resend descriptive metadata

all seen stations
    -> touch presence independently
```

The fingerprint is an optimisation aid, not source authority. Source response and persisted station row remain authoritative.

A missing/invalid comparison fingerprint MUST fail safe by requiring descriptive refresh, not by skipping it.

Absence from one Sensor.Community country-feed response MUST NOT by itself retire a station.

## Timeseries reference ownership

Observation ingest MUST distinguish static timeseries reference metadata from changing latest-value state.

Static reference data includes as applicable:

- `timeseries_ref`;
- station link;
- connector/service identity;
- phenomenon link;
- source-defined unit/label.

Changing value state includes:

- `first_value_at` where existing connector behaviour requires ingest to establish it;
- `last_value_at`;
- `last_value`.

Existing `timeseries_ref -> timeseries_id` mappings SHOULD be resolved compactly. Existing valid timeseries MUST NOT have full static reference rows resent merely to update latest-value state.

Missing identities MUST retain connector-specific creation/self-repair behaviour already authorised. An observation MUST NOT be silently dropped because required reference data is absent.

## Metadata-before-observation ordering

Where a source feed introduces references and observations in one run, ordering is:

```text
resolve existing station/timeseries identities
    -> create/update only required reference metadata
    -> update station presence where applicable
    -> resolve newly created internal IDs
    -> compact observation write
    -> compact latest-value update
```

Sensor.Community MUST preserve immediate new-station/new-timeseries creation before dependent observations are written.

## Validation

Before deployment use only structural checks relevant to changed metadata ownership/fingerprint/reference handling.

Functional acceptance through real TEST operations must confirm as applicable:

- Sensor.Community new stations/timeseries are still created immediately;
- unchanged Sensor.Community descriptive metadata is not resent in full;
- `last_seen_at` and reappearance clearing `removed_at` remain correct;
- OpenAQ full catalogue fetching remains disabled during normal ingest;
- SOS/Breathe London observation ingest does not become normal catalogue authority;
- observations still wait for resolvable required references.

No speculative broad pre-implementation functional test suite is required.
