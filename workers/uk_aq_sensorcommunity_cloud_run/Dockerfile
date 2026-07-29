FROM node:20-slim

WORKDIR /app
ENV NODE_ENV=production

COPY workers/uk_aq_sensorcommunity_cloud_run/index.mjs /app/index.mjs
COPY workers/uk_aq_sensorcommunity_cloud_run/run_service.mjs /app/run_service.mjs
COPY supabase/functions/_shared/ingestdb_observation_writer.mjs /supabase/functions/_shared/ingestdb_observation_writer.mjs

CMD ["node", "run_service.mjs"]
