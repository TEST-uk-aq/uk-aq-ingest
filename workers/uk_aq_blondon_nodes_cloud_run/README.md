# uk_aq Breathe London Nodes Cloud Run service

Runs `scripts/blondon_nodes/blondon_nodes_ingest.py` through a Cloud Run
service/job split:

1. `run_service.py` validates the request and enforces the 840-second child timeout.
2. `run_job.py` checks due/in-flight state and claims `blondon_nodes` with
   `uk_aq_rpc_dispatch_claim`.
3. The Python ingest writes observations and emits `RUN_SUMMARY_JSON`.
4. The job updates connector run fields and inserts `uk_aq_ingest_runs`.

When `UK_AQ_SERVICE_EGRESS_METRICS_ENABLED=true`, the Nodes worker measures
PostgREST responses from clients created through `create_supabase_client()` and
flushes one best-effort aggregate batch per process. Main ingest traffic is
labelled `ingestdb`; direct Observs traffic is labelled `obs_aqidb`. The metrics
RPC uses a separate unmetered client and cannot fail the ingest run.

Required secret:
- `BLONDON_NODES_API_KEY` (no sensible default; add to `.env`/GitHub secrets/Secret Manager).

Defaults that do not require `.env` rows:
- `BLONDON_NODES_BASE_URL=https://breathe-london-7x54d7qf.ew.gateway.dev`
- `BLONDON_NODES_SERVICE_REF=breathelondon`
- `GCP_OBSERVS_PUBSUB_TOPIC=uk-aq-observs-observations`

Optional service-egress metrics configuration:

- `UK_AQ_SERVICE_EGRESS_METRICS_ENABLED` (default `false`)
- `UK_AQ_SERVICE_EGRESS_METRICS_SUPABASE_URL` (defaults to
  `OBS_AQIDB_SUPABASE_URL` in the TEST deployment workflow)
- `UK_AQ_SERVICE_EGRESS_METRICS_SCHEMA` (default `uk_aq_public`)
- `UK_AQ_SERVICE_EGRESS_METRICS_RPC` (default
  `uk_aq_rpc_service_egress_metrics_batch_upsert`)
- `UKAQ_ENV_NAME` (default `TEST` in this TEST repository)
- `UK_AQ_SERVICE_EGRESS_METRICS_SB_SECRET_KEY` (required when enabled; the
  workflow binds the existing OBS/AQI secret)

Observation delivery follows the shared Communities modes:

- `pubsub_only` publishes observation rows (including `RatificationStatus` as
  `status`) to `GCP_OBSERVS_PUBSUB_TOPIC`.
- `direct` calls `uk_aq_rpc_observs_observations_compact_upsert_v1` on Obs AQI DB.
- `outbox_only` enqueues rows through the ingest DB observs outbox.

`OBSERVS_WRITE_MODE` controls only this secondary Observs/obsAQIDB path.
Unless `--dry-run` is used, Nodes observations are always written first to
`uk_aq_core.observations`.

Latest-snapshot processing consumes the same observation messages through the
`uk-aq-latest-snapshot-sub` subscription. Nodes does not publish to or require
a separate latest-snapshot topic. A secondary delivery failure is reported in
the run summary but does not set station/species checkpoint errors or prevent
checkpoint advancement after a successful core observation write. A
secondary-only failure keeps `run_status=succeeded`, uses
`run_message=secondary_errors`, and still updates the connector's successful
polling timestamp.

Normal scheduled runs select due active stations from
`uk_aq_raw.blondon_nodes_station_checkpoints`. Scheduling is station-level
because `/SensorData` requests use `SiteCode` plus species; per-species
progress and errors are JSONB fields on the station checkpoint row.

For successfully written observations, the ingest updates
`timeseries.first_value_at`, `timeseries.last_value_at`, and `timeseries.last_value`
without regressing existing bounds.

The normal Cloudflare scheduler request body is `{}`. `trigger_mode=scheduled` is
equivalent to `{}`; `trigger_mode=manual` bypasses the poll-interval due check
but still requires an enabled Cloud Run connector and a successful dispatch
claim. There is no Nodes `safety` trigger mode.

The HTTP wrapper accepts only `start_time`, `end_time`, `site_code`, `species`,
`max_stations`, `max_api_calls`, `dry_run`, and the optional `trigger_mode`;
invalid values return HTTP 400 without starting the job.

The service allows unauthenticated transport access for Cloudflare, but POST
execution requires `x-uk-aq-dispatch-secret` or `x-uk-aq-upstream-auth` matching
`UK_AQ_EDGE_UPSTREAM_SECRET`. GET remains a health check. The shared secret is
required in addition to `BLONDON_NODES_API_KEY`.

Manual local dry run:

```bash
python3 scripts/blondon_nodes/blondon_nodes_ingest.py --dry-run --max-stations 1 --max-api-calls 4
```
