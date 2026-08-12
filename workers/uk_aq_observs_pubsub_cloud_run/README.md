# uk_aq observs Pub/Sub Cloud Run service

This Cloud Run service drains observs observation messages from Pub/Sub, merges all
connectors into mixed batches, deduplicates by
`(connector_id, timeseries_id, observed_at)`, and upserts to observs DB.

This supports the hourly mixed-row model so calls are chunked by total rows,
not by connector.

Scheduler triggers the service with an authenticated POST request.

## Required env vars / secrets

- `SUPABASE_URL`
- `SB_SECRET_KEY`
- `OBS_AQIDB_SUPABASE_URL`
- `OBS_AQIDB_SECRET_KEY`
- `GCP_PROJECT_ID` (or `GOOGLE_CLOUD_PROJECT`)
- `OBSERVS_PUBSUB_SUBSCRIPTION`

## Optional env vars

- `UK_AQ_PUBLIC_SCHEMA` (default `uk_aq_public`)
- `OBS_AQIDB_RPC_SCHEMA` (default `uk_aq_public`)
- `OBSERVS_UPSERT_RPC` (default `uk_aq_rpc_observs_observations_compact_upsert_v1`)
- `OBSERVS_UPSERT_CHUNK_SIZE` (default `5000`)
- `OBSERVS_UPSERT_RPC_RETRIES` (default `3`; retries per observs upsert RPC call for retryable failures)
- `OBSERVS_UPSERT_RETRY_BASE_MS` (default `1000`; base backoff between observs upsert retries)
- `OBSERVS_UPSERT_TIMEOUT_SPLIT_MIN_ROWS` (default `32`; minimum chunk size that can be split when statement timeouts occur)
- `OBSERVS_UPSERT_TIMEOUT_SPLIT_MAX_DEPTH` (default `4`; max recursive split depth for timeout fallback)
- `OBSERVS_PUBSUB_PULL_MAX_MESSAGES` (default `1000`)
- `OBSERVS_PUBSUB_WRITER_MAX_BATCHES` (default `24`)
- `OBSERVS_PUBSUB_WRITER_BUDGET_SECONDS` (default `1200`)
- `OBSERVS_PUBSUB_WRITER_SHUTDOWN_BUFFER_SECONDS` (default `20`)
- `OBSERVS_PUBSUB_WRITER_RPC_RETRIES` (default `3`)
- `OBSERVS_PUBSUB_WRITER_PUBSUB_RETRIES` (default `3`)
