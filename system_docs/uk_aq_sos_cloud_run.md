# UK-AIR SOS Cloud Run documentation moved

This page is retained for existing links.

The authoritative current Cloud Run documentation is now in the Ops system-documentation area:

1. [`system_docs/ingest/sos/README.md`](https://github.com/TEST-uk-aq/uk-aq-ops/blob/main/system_docs/ingest/sos/README.md): ownership, scope and reading order
2. [`system_docs/ingest/sos/contract.md`](https://github.com/TEST-uk-aq/uk-aq-ops/blob/main/system_docs/ingest/sos/contract.md): required polling and failure behaviour
3. [`system_docs/ingest/sos/interfaces.md`](https://github.com/TEST-uk-aq/uk-aq-ops/blob/main/system_docs/ingest/sos/interfaces.md): HTTP response and child-result contracts
4. [`system_docs/ingest/sos/operations.md`](https://github.com/TEST-uk-aq/uk-aq-ops/blob/main/system_docs/ingest/sos/operations.md): deployment, scheduling, logging and rollback
5. [`system_docs/ingest/sos/validation.md`](https://github.com/TEST-uk-aq/uk-aq-ops/blob/main/system_docs/ingest/sos/validation.md): focused checks and TEST operational validation

Do not maintain a second editable Cloud Run behavioural contract in this repository.

## Implementation ownership

- Connector: `sos`
- Worker: `workers/uk_aq_sos_cloud_run`
- Shared ingest handler: `supabase/functions/ingest_sos`
- Scheduler: configured external scheduler to Cloud Run service
- Default service name: `uk-aq-sos-ingest`
- Deployment workflow: `.github/workflows/uk_aq_sos_cloud_run_deploy.yml`
- Worker README: `workers/uk_aq_sos_cloud_run/README.md`

Broader SOS discovery, station, site-register and archive-mapping material remains in [`sos.md`](sos.md) and [`sos_ingest_flow.md`](sos_ingest_flow.md) until those non-overlapping subjects are separately migrated into the Ops ingest area.
