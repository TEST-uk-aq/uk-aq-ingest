FROM node:20-slim

WORKDIR /app
ENV NODE_ENV=production

COPY index.mjs ./index.mjs
COPY run_service.mjs ./run_service.mjs

CMD ["node", "run_service.mjs"]
