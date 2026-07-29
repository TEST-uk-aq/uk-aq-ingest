FROM denoland/deno:2.1.4

WORKDIR /app

# Cloud Run runner (service invokes run_job.ts per request)
COPY workers/uk_aq_openaq_cloud_run/run_job.ts /app/workers/uk_aq_openaq_cloud_run/run_job.ts
COPY workers/uk_aq_openaq_cloud_run/run_service.ts /app/workers/uk_aq_openaq_cloud_run/run_service.ts
COPY workers/shared/dropbox_error_log.ts /app/workers/shared/dropbox_error_log.ts

# Reuse existing OpenAQ ingest implementation + shared helpers.
COPY supabase/functions/ingest_openaq/index.ts /app/runtime/ingest_openaq/index.ts
COPY supabase/functions/_shared/cache.ts /app/runtime/_shared/cache.ts
COPY supabase/functions/_shared/fetch_egress_patch.ts /app/runtime/_shared/fetch_egress_patch.ts
COPY supabase/functions/_shared/egress_metrics.ts /app/runtime/_shared/egress_metrics.ts
COPY supabase/functions/_shared/observs_client.ts /app/runtime/_shared/observs_client.ts
COPY supabase/functions/_shared/fetch_egress_patch.ts /app/supabase/functions/_shared/fetch_egress_patch.ts
COPY supabase/functions/_shared/egress_metrics.ts /app/supabase/functions/_shared/egress_metrics.ts

ENV PORT=8000
ENV OPENAQ_LOCAL_PORT=8000

CMD ["deno", "run", "--allow-env", "--allow-net", "--allow-read", "--allow-write", "--allow-run", "/app/workers/uk_aq_openaq_cloud_run/run_service.ts"]
