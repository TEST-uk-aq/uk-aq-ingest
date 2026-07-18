# UK-AIR SOS system area

This directory is the migration entry point for authoritative UK-AIR SOS ingest documentation in `uk-aq-ingest`.

The migration is intentionally gradual. Current SOS behaviour has one authoritative home for each subject, while older broad documents remain active only for the subjects listed below. Do not copy the same editable behavioural rule into more than one document.

## Repository ownership

`uk-aq-ingest` owns the authoritative documentation for:

- UK-AIR SOS discovery and recent observation ingest;
- the Supabase `ingest_sos` function;
- the SOS Cloud Run service and child process;
- SOS polling selection and checkpoints;
- ingest response, failure and partial-run semantics;
- SOS deployment workflows and runtime logging.

Cross-repository scheduling or operational indexes may link here, but must not maintain a second detailed version of these rules.

## Reading order

For current polling and Cloud Run behaviour, read:

1. [`contract.md`](contract.md)
2. [`interfaces.md`](interfaces.md)
3. [`operations.md`](operations.md)
4. [`validation.md`](validation.md)

Existing documents retain these non-overlapping responsibilities during migration:

- [`../sos.md`](../sos.md): source discovery, filters, station metadata, site register, archive mapping and network assignment.
- [`../sos_ingest_flow.md`](../sos_ingest_flow.md): high-level metadata and observation data flow.
- [`../uk_aq_edge_functions.md`](../uk_aq_edge_functions.md): repository-wide edge-function catalogue and a summary entry for `ingest_sos`.
- [`../uk_aq_github_actions.md`](../uk_aq_github_actions.md): repository-wide workflow catalogue.
- [`../uk_aq_sos_cloud_run.md`](../uk_aq_sos_cloud_run.md): compatibility page for existing links. It points to this area and is not a second Cloud Run contract.

## Implementation ownership

The polling contract is implemented by:

- `supabase/functions/ingest_sos/index.ts`
- `supabase/functions/ingest_sos/failure.ts`
- `workers/uk_aq_sos_cloud_run/run_job.ts`
- `workers/uk_aq_sos_cloud_run/run_service.ts`
- `workers/uk_aq_sos_cloud_run/result_contract.ts`
- `workers/uk_aq_sos_cloud_run/Dockerfile`
- `.github/workflows/uk_aq_sos_cloud_run_deploy.yml`

Relevant persistent state includes:

- `uk_aq_core.connectors`
- `uk_aq_core.timeseries`
- `uk_aq_core.observations`
- `uk_aq_core.uk_aq_ingest_runs`
- `uk_aq_raw.sos_timeseries_checkpoints`
- `uk_aq_raw.sos_station_checkpoints`
- `uk_aq_raw.error_logs`

## Migration status

The Cloud Run polling contract, interface, operations and validation rules are authoritative in this directory.

Discovery, site-register, archive-mapping and network-assignment material remains authoritative in the existing broad SOS documents until it is moved into this area. When moved, remove or reduce the old active wording so there is still only one editable source of truth.