# UK AQ UK-AIR SOS Cloud Run

This page is retained for existing links.

The authoritative current Cloud Run documentation is now under [`system_docs/sos/`](sos/README.md):

1. [`sos/README.md`](sos/README.md): ownership, scope and reading order
2. [`sos/contract.md`](sos/contract.md): required polling and failure behaviour
3. [`sos/interfaces.md`](sos/interfaces.md): HTTP response and child-result contracts
4. [`sos/operations.md`](sos/operations.md): deployment, scheduling, logging and rollback
5. [`sos/validation.md`](sos/validation.md): focused checks and TEST operational validation

Do not maintain a second editable version of the Cloud Run behavioural contract in this file.

## Implementation ownership

- Connector: `sos`
- Worker: `workers/uk_aq_sos_cloud_run`
- Shared ingest handler: `supabase/functions/ingest_sos`
- Scheduler: Google Cloud Scheduler to Cloud Run Service
- Default service name: `uk-aq-sos-ingest`
- Deployment workflow: `.github/workflows/uk_aq_sos_cloud_run_deploy.yml`
- Worker README: `workers/uk_aq_sos_cloud_run/README.md`

The broader SOS discovery, station, site-register and archive-mapping documentation remains in [`sos.md`](sos.md) and [`sos_ingest_flow.md`](sos_ingest_flow.md) until those subjects are migrated into the area directory.