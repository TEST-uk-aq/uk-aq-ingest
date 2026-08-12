#!/usr/bin/env bash
set -euo pipefail

job_key="${1:-}"
service_url="${2:-}"

if [ -z "${job_key}" ]; then
  echo "Usage: $0 <job-key> <https-service-url>" >&2
  exit 2
fi
if [[ ! "${service_url}" =~ ^https://[^/]+/?$ ]]; then
  echo "Invalid Cloud Run service URL: ${service_url:-<missing>}" >&2
  exit 2
fi
service_url="${service_url%/}"

: "${CLOUDFLARE_ACCOUNT_ID:?Missing CLOUDFLARE_ACCOUNT_ID}"
: "${CLOUDFLARE_API_TOKEN:?Missing CLOUDFLARE_API_TOKEN}"

database_name="uk_aq_cron_scheduler_ingest_db"
wrangler_config="cloudflare/scheduler/wrangler.toml"
jobs_file="cloudflare/scheduler/jobs.toml"
sync_script="cloudflare/scheduler/scripts/sync_jobs.py"

python3 "${sync_script}" \
  --jobs-file "${jobs_file}" \
  --sql-file /tmp/ingest_scheduler_jobs_sync.sql \
  --json-file /tmp/ingest_scheduler_jobs_expected.json

npx --yes wrangler@4.121.0 d1 execute "${database_name}" \
  --remote \
  --config "${wrangler_config}" \
  --file /tmp/ingest_scheduler_jobs_sync.sql

SCHEDULER_JOB_KEY="${job_key}" SERVICE_URL="${service_url}" python3 - <<'PY'
import os
from pathlib import Path


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


job_key = os.environ["SCHEDULER_JOB_KEY"]
service_url = os.environ["SERVICE_URL"]
Path("/tmp/ingest_scheduler_cloud_run_url.sql").write_text(
    "update scheduler_jobs\n"
    f"set cloud_run_url = {sql_literal(service_url)}, updated_at = current_timestamp\n"
    f"where job_key = {sql_literal(job_key)} and target_type = 'cloud_run';\n",
    encoding="utf-8",
)
PY

npx --yes wrangler@4.121.0 d1 execute "${database_name}" \
  --remote \
  --config "${wrangler_config}" \
  --file /tmp/ingest_scheduler_cloud_run_url.sql

npx --yes wrangler@4.121.0 d1 execute "${database_name}" \
  --remote \
  --config "${wrangler_config}" \
  --json \
  --command "select job_key, target_type, cloud_run_url from scheduler_jobs where job_key = '$(printf '%s' "${job_key}" | sed "s/'/''/g")'" \
  > /tmp/ingest_scheduler_cloud_run_job.json

SCHEDULER_JOB_KEY="${job_key}" SERVICE_URL="${service_url}" python3 - <<'PY'
import json
import os
from pathlib import Path

payload = json.loads(Path("/tmp/ingest_scheduler_cloud_run_job.json").read_text(encoding="utf-8"))
rows = payload[0]["results"]
expected = [{
    "job_key": os.environ["SCHEDULER_JOB_KEY"],
    "target_type": "cloud_run",
    "cloud_run_url": os.environ["SERVICE_URL"],
}]
if rows != expected:
    raise SystemExit(f"D1 Cloud Run URL reconciliation mismatch: expected={expected!r} actual={rows!r}")
PY

echo "Reconciled ${job_key} to ${service_url} in ${database_name}."
