FROM node:24.22.1-slim

# Fail closed if a rebuilt/mutable base image does not meet the security floor.
RUN node -e 'const [major, minor, patch] = process.versions.node.split(".").map(Number); if (major !== 24 || minor < 22 || (minor === 22 && patch < 1)) throw new Error("Node.js >=24.22.1 <25 is required"); console.log(JSON.stringify({node: process.versions.node, platform: process.platform, arch: process.arch}));'

WORKDIR /app
ENV NODE_ENV=production

COPY workers/uk_aq_sensorcommunity_cloud_run/index.mjs /app/index.mjs
COPY workers/uk_aq_sensorcommunity_cloud_run/run_service.mjs /app/run_service.mjs
COPY workers/uk_aq_sensorcommunity_cloud_run/service_egress_metrics.mjs /app/service_egress_metrics.mjs
COPY supabase/functions/_shared/ingestdb_observation_writer.mjs /supabase/functions/_shared/ingestdb_observation_writer.mjs

CMD ["node", "run_service.mjs"]
