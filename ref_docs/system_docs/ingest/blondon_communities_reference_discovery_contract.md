# Breathe London Communities deterministic reference-discovery contract

**Status: future implementation authority, not current deployed behaviour.** Current Daily Stations imports Communities stations without a dedicated deterministic timeseries discovery stage; current quarter-hour Communities ingest fails the batch on missing identities. This contract defines the agreed replacement for TEST.

## Authority

This contract owns deterministic Communities phenomena/timeseries creation and repair by Daily Stations. The shared ingest-side reference repair and partial-failure behaviour for Communities and Nodes is defined in [`blondon_reference_failure_isolation_contract.md`](blondon_reference_failure_isolation_contract.md). Read [`contract.md`](contract.md) for Daily Stations sequencing and the ObsAQIDB mirror gate.

## Stable identities and canonical species

Connector and service identities MUST remain distinct from Breathe London Nodes:

```text
connector_code = blondon_communities
service_ref = breathelondon
network_code = breathelondon
station_ref = upstream Communities SiteCode
```

Communities has exactly two deterministic source timeseries per selected active station:

| Source species | Deterministic timeseries_ref | Phenomenon source_label | Canonical observed property | Domain |
|---|---|---|---|---|
| `IPM25` | `<station_ref>:IPM25` | `breathelondon:pm2.5` | `pm25` | `aq` |
| `INO2` | `<station_ref>:INO2` | `breathelondon:no2` | `no2` | `aq` |

Timeseries MUST retain the existing deployed `(connector_id, service_ref, timeseries_ref)` lookup/upsert identity, and the database's unique `(connector_id, timeseries_ref)` constraint MUST also remain satisfied. Repeated discovery, ingest repair and cross-database reference mirroring MUST preserve previously assigned numeric `timeseries.id` values. No Communities DAQI-index species are introduced by this contract.

## Daily Stations Communities stage

Immediately after the Communities station-list import, Daily Stations MUST invoke independently runnable Communities reference discovery that:

1. resolves the `blondon_communities` connector by code and selects current Communities stations with `service_ref='breathelondon'` and `removed_at IS NULL`;
2. uses the existing central phenomena RPC to establish and validate `IPM25` and `INO2` mapping, including non-null canonical `observed_property_id`; an unknown or warned mapping MUST fail discovery;
3. deterministically builds the two required rows for every selected station using shared Communities reference-building logic and source-defined labels/units;
4. compares the required rows with persisted references and upserts only missing or mismatched static metadata, preserving existing timeseries IDs and observation value bounds;
5. reads back the required set and verifies `2 × active-station count`, exact identity and canonical station/connector/service/phenomenon/observed-property links;
6. reports active stations, expected/pre-existing/created-or-repaired/final references, missing/mismatched identities and changed existing IDs; fails if its final required set is incomplete.

`removed_at IS NULL` is the existing station-active criterion for this stage. Do not introduce an additional filter on upstream `Enabled` or `SiteActive` without a separate explicit lifecycle change. Do not delete historical references for removed stations.

## Separation from observation ingestion

Daily Stations MUST NOT call Communities observation endpoints, write routine observations, change latest-value bounds or advance observation checkpoints as part of discovery. Normal quarter-hour ingest retains due-station selection and the existing observation/secondary-write paths, and applies the narrow missing-reference self-repair and per-station failure handling from the shared future [reference-isolation contract](blondon_reference_failure_isolation_contract.md).

The normal Communities Cloud Run path currently uses `skip_stations=true` and remains a consumer of authoritative Daily Stations station metadata, not a new station catalogue owner. The separately operated standalone Python ingest must use compatible canonical identity rules if it creates timeseries.

The final IngestDB-to-ObsAQIDB mirror MUST run only after this and all other required non-isolated reference stages succeed, preserving the independent SOS isolation exception.

## Structural viability and operational acceptance

Before implementation, establish that the existing unique constraints and central phenomena RPC support these deterministic rows, that the Daily Stations stage can run after Communities station import and before the final reference mirror, and that the deployed connector's reference lookup uses the same identities. No new tables or migrations are assumed without an identified structural need.

After deployment use a real TEST Daily Stations run to confirm `2 × active Communities stations`, correct canonical mappings, stable IDs on rerun and successful final reference mirroring. Confirm a subsequent real quarter-hour ingest reuses them. Validate isolated incomplete-station handling separately using the shared reference-isolation contract's real TEST acceptance rules.

## Non-goals

No change to source polling windows, frequency, `EndDate` retirement behaviour, public network identity, Nodes species, observation transport, Pub/Sub contracts, historical R2 or historical timeseries deletion is authorised.
