FROM denoland/deno:2.1.4

WORKDIR /app

COPY workers/uk_aq_sos_cloud_run/run_job.ts /app/workers/uk_aq_sos_cloud_run/run_job.ts
COPY workers/uk_aq_sos_cloud_run/run_service.ts /app/workers/uk_aq_sos_cloud_run/run_service.ts
COPY workers/uk_aq_sos_cloud_run/result_contract.ts /app/workers/uk_aq_sos_cloud_run/result_contract.ts
COPY workers/shared/dropbox_error_log.ts /app/workers/shared/dropbox_error_log.ts

# Reuse existing SOS ingest implementation + shared helpers.
COPY supabase/functions/ingest_sos/index.ts /app/runtime/ingest_sos/index.ts
COPY supabase/functions/ingest_sos/failure.ts /app/runtime/ingest_sos/failure.ts
COPY supabase/functions/ingest_sos/run_metrics.ts /app/runtime/ingest_sos/run_metrics.ts
COPY supabase/functions/ingest_sos/selected_work.ts /app/runtime/ingest_sos/selected_work.ts
COPY supabase/functions/ingest_sos/uk_aq_html_fallback.ts /app/runtime/ingest_sos/uk_aq_html_fallback.ts
COPY supabase/functions/ingest_sos/uk_aq_html_parser.ts /app/runtime/ingest_sos/uk_aq_html_parser.ts
COPY supabase/functions/_shared/cache.ts /app/runtime/_shared/cache.ts
COPY supabase/functions/_shared/fetch_egress_patch.ts /app/runtime/_shared/fetch_egress_patch.ts
COPY supabase/functions/_shared/egress_metrics.ts /app/runtime/_shared/egress_metrics.ts
COPY supabase/functions/_shared/observs_client.ts /app/runtime/_shared/observs_client.ts
COPY supabase/functions/_shared/ingestdb_observation_writer.mjs /app/runtime/_shared/ingestdb_observation_writer.mjs
COPY supabase/functions/_shared/fetch_egress_patch.ts /app/supabase/functions/_shared/fetch_egress_patch.ts
COPY supabase/functions/_shared/egress_metrics.ts /app/supabase/functions/_shared/egress_metrics.ts
COPY supabase/functions/ingest_sos/selected_work.ts /app/supabase/functions/ingest_sos/selected_work.ts
COPY supabase/functions/ingest_sos/uk_aq_html_parser.ts /app/supabase/functions/ingest_sos/uk_aq_html_parser.ts

ENV PORT=8000
ENV SOS_LOCAL_PORT=8000

CMD ["deno", "run", "--allow-env", "--allow-net", "--allow-read", "--allow-write", "--allow-run", "/app/workers/uk_aq_sos_cloud_run/run_service.ts"]
