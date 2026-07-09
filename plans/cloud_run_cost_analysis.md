# Cloud Run Cost Analysis (UK AQ)

Generated: 2026-02-16 (UTC)
Scope: `astute-lyceum-484111-k5`, Cloud Run Jobs/Services, Cloud Scheduler, Cloud Logging.
Primary region found: `europe-west2`.

## 1) Executive summary

Cloud Run spend is currently dominated by high-frequency Cloud Run **Jobs CPU time**. Using a CPU-seconds/day proxy (`executions_per_day * avg_duration_seconds * vCPU`), the main drivers are:

1. `uk-aq-openaq-ingest` (~38.4% of proxy CPU/day)
2. `uk-aq-breathelondon-ingest` (~30.4%)
3. `uk-aq-sos-ingest` (~14.3%)
4. `uk-aq-scomm-ingest` (~13.5%)

Key finding: a large share of runs are non-work runs (skip/not-due) for several jobs, and `uk-aq-openaq-ingest` runs far more often than its safety scheduler because it self-schedules via Cloud Tasks.

Label status for attribution:
- Deploy workflows set `job_name` labels at update time via `--update-labels`.
  - `.github/workflows/uk_aq_openaq_cloud_run_deploy.yml:474`
  - `.github/workflows/uk_aq_breathelondon_cloud_run_deploy.yml:322`
  - `.github/workflows/uk_aq_scomm_cloud_run_deploy.yml:320`
  - `.github/workflows/uk_aq_sos_cloud_run_deploy.yml:322`
  - `.github/workflows/uk_aq_observs_outbox_cloud_run_deploy.yml:288`
  - `.github/workflows/uk_aq_observs_pubsub_cloud_run_deploy.yml:333`
- Workflows also verify label presence post-deploy (`gcloud run jobs describe ... metadata.labels`).

## 2) Inventory table of jobs (config + schedule)

Cloud Run services used for ingest in this project/region: **none found** (`gcloud run services list` returned empty).

Repo scan (deploy/scheduler/labels) found Cloud Run job deployment in these workflows:
- `.github/workflows/uk_aq_openaq_cloud_run_deploy.yml:468` (`gcloud run jobs update`), `.github/workflows/uk_aq_openaq_cloud_run_deploy.yml:474` (`--update-labels`), `.github/workflows/uk_aq_openaq_cloud_run_deploy.yml:529` (scheduler update/create).
- `.github/workflows/uk_aq_breathelondon_cloud_run_deploy.yml:316` (`gcloud run jobs update`), `.github/workflows/uk_aq_breathelondon_cloud_run_deploy.yml:322` (`--update-labels`), `.github/workflows/uk_aq_breathelondon_cloud_run_deploy.yml:378` (scheduler update/create).
- `.github/workflows/uk_aq_scomm_cloud_run_deploy.yml:317` (`gcloud run jobs update`), `.github/workflows/uk_aq_scomm_cloud_run_deploy.yml:320` (`--update-labels`).
- `.github/workflows/uk_aq_sos_cloud_run_deploy.yml:316` (`gcloud run jobs update`), `.github/workflows/uk_aq_sos_cloud_run_deploy.yml:322` (`--update-labels`), `.github/workflows/uk_aq_sos_cloud_run_deploy.yml:377` (scheduler update/create).
- `.github/workflows/uk_aq_observs_outbox_cloud_run_deploy.yml:282` (`gcloud run jobs update`), `.github/workflows/uk_aq_observs_outbox_cloud_run_deploy.yml:288` (`--update-labels`), `.github/workflows/uk_aq_observs_outbox_cloud_run_deploy.yml:334` (scheduler update/create).
- `.github/workflows/uk_aq_observs_pubsub_cloud_run_deploy.yml:327` (`gcloud run jobs update`), `.github/workflows/uk_aq_observs_pubsub_cloud_run_deploy.yml:333` (`--update-labels`), `.github/workflows/uk_aq_observs_pubsub_cloud_run_deploy.yml:379` (scheduler update/create).

| Job | Region | Trigger(s) | CPU / Memory | Timeout | Retries | TaskCount / Parallelism | Service account | VPC connector | Labels |
|---|---|---|---|---:|---:|---:|---|---|---|
| `uk-aq-openaq-ingest` | `europe-west2` | Scheduler safety: `*/15 * * * *` (`uk-aq-openaq-safety-trigger`) + Cloud Tasks self-run chain | `1000m` / `512Mi` | 900s | 0 | 1 / 1 | `uk-aq-openaq-job@...` | none | `job_name=uk-aq-openaq-ingest` |
| `uk-aq-breathelondon-ingest` | `europe-west2` | Scheduler: `*/2 * * * *` | `1000m` / `512Mi` | 900s | 0 | 1 / 1 | `uk-aq-breathelondon-job@...` | none | `job_name=uk-aq-breathelondon-ingest` |
| `uk-aq-sos-ingest` | `europe-west2` | Scheduler: `*/2 * * * *` | `1000m` / `512Mi` | 900s | 0 | 1 / 1 | `uk-aq-sos-job@...` | none | `job_name=uk-aq-sos-ingest` |
| `uk-aq-scomm-ingest` | `europe-west2` | Scheduler: `*/2 * * * *` | `1000m` / `512Mi` | 600s | 0 | 1 / 1 | `uk-aq-scomm-job@...` | none | `job_name=uk-aq-scomm-ingest` |
| `uk-aq-observs-outbox-flush` | `europe-west2` | Scheduler: `*/10 * * * *` | `1000m` / `512Mi` | 600s | 0 | 1 / 1 | `uk-aq-observs-outbox-flusher@...` | none | `job_name=uk-aq-observs-outbox-flush` |
| `uk-aq-observs-pubsub-writer` | `europe-west2` | Scheduler: `0 * * * *` | `1000m` / `512Mi` | 1500s | 0 | 1 / 1 | `uk-aq-observs-pubsub-job@...` | none | `job_name=uk-aq-observs-pubsub-writer` |

Notes:
- All jobs are `gen2` execution environment.
- All jobs currently use the same resource size (`1 vCPU`, `512Mi`).

## 3) Proxy cost ranking (assumptions stated)

Window used for ranking: **2026-02-16 00:00 UTC to analysis time**.

Assumptions:
- vCPU allocated = 1.0 for all jobs (from live job configs).
- Proxy formula: `normalized_executions_per_day * average_success_duration_seconds * 1 vCPU`.
- This is a ranking proxy, not exact billing.

| Rank | Job | Executions (today) | p50 (s) | p95 (s) | Failure rate | Normalized exec/day | Proxy CPU-sec/day |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | `uk-aq-openaq-ingest` | 855 | 20.62 | 24.89 | 0.6% | 1261.3 | 25,256.9 |
| 2 | `uk-aq-breathelondon-ingest` | 488 | 11.00 | 78.90 | 1.0% | 719.9 | 19,981.4 |
| 3 | `uk-aq-sos-ingest` | 488 | 10.79 | 28.36 | 1.0% | 719.9 | 9,382.0 |
| 4 | `uk-aq-scomm-ingest` | 488 | 11.11 | 17.14 | 0.0% | 719.9 | 8,897.3 |
| 5 | `uk-aq-observs-outbox-flush` | 98 | 11.25 | 14.78 | 0.0% | 144.6 | 1,669.5 |
| 6 | `uk-aq-observs-pubsub-writer` | 17 | 20.44 | 23.13 | 0.0% | 25.1 | 509.5 |

## 4) Findings

### 4.1 Scheduling and no-op patterns

- `uk-aq-breathelondon-ingest`: many no-op checks before work.
  - Structured log summary (today): ~322 `skipped` (`reason=not_due`) vs ~161 dispatching/success runs.
- `uk-aq-scomm-ingest`: similar pattern.
  - ~325 `skip` (`reason=not_due`) vs ~163 success runs.
- `uk-aq-sos-ingest`: strongest no-op pattern.
  - ~399 `skipped` total, including ~322 `not_due` and ~77 `no_timeseries_ids`.
- `uk-aq-openaq-ingest`: high execution volume is **not** explained by safety scheduler alone.
  - Scheduler safety trigger is every 15 minutes, but observed run rate is ~1261/day normalized.
  - Worker self-schedules the next run via Cloud Tasks (`workers/uk_aq_openaq_cloud_run/run_job.ts:882`, `workers/uk_aq_openaq_cloud_run/run_job.ts:954`).

### 4.2 Failure and retry waste

- All jobs set `maxRetries=0` at Cloud Run Job level.
- Today error log counts are low but non-zero:
  - `uk-aq-breathelondon-ingest`: 5
  - `uk-aq-openaq-ingest`: 4
  - `uk-aq-sos-ingest`: 5
- Failures are mostly container exit failures at task level (from execution condition messages).

### 4.3 Repeated work / data-fetch behavior

- `uk-aq_sensorcommunity_cloud_run` fetches full country feed each due run (`index.mjs:1651`), then processes/upserts.
- `uk_aq_openaq_cloud_run` evaluates station checkpoints each run and can continue scheduling frequent follow-ups.
- `uk_aq_sos_cloud_run` can run frequently but skip due to `not_due` / `no_timeseries_ids` (`run_job.ts:1016`, `run_job.ts:1057`).
- `uk_aq_breathelondon_cloud_run` already avoids station refresh in ingest payload (`payload.skip_stations = true`, `run_job.ts:359`), which is good.

### 4.4 Likely CPU-bound vs I/O-bound

Based on code paths and runtime patterns, jobs are predominantly I/O-bound (external API + PostgREST/DB + Pub/Sub/Cloud Tasks calls), with occasional latency spikes (notably BreatheLondon p95). OpenAQ also includes substantial control-plane overhead from frequent self-triggering.

## 5) Options to reduce billed usage

### Option A: Align trigger cadence with actual due cadence (largest quick win)

What it is:
- Reduce scheduler frequency where most runs are `not_due`.
- Keep safety triggers, but avoid running containers when work is predictably not due.

Pros:
- Largest immediate CPU-second reduction with minimal code change.
- Reversible by restoring cron strings.
- Egress impact: reduced control-plane/API calls from fewer job starts.
- Database-size impact: neutral to slightly lower (fewer run-log rows, same observation payload when due).

Cons:
- If cadence is set too slow, latency to fresh data can increase.

Risks / effort:
- Low effort (workflow/scheduler cron change).
- Moderate operational risk if set below real upstream cadence.

### Option B: OpenAQ self-scheduling guardrails

What it is:
- Keep Cloud Tasks chaining, but increase minimum delay and/or backoff when repeated `no_station_refs`/`in_flight` occurs.
- Keep scheduler as safety net, not primary cadence driver.

Pros:
- Targets the highest CPU-cost job directly.
- Preserves responsiveness when data is available.
- Egress impact: lower Cloud Tasks + Run API traffic, fewer OpenAQ/API polls when no work.
- Database-size impact: lower run-log volume; observation volume unchanged.

Cons:
- Requires careful tuning to avoid missed bursts.

Risks / effort:
- Medium effort (runtime tuning + validation).
- Low-to-moderate risk if tuned gradually.

### Option C: Right-size resource limits by job class

What it is:
- Benchmark lower CPU/memory for low-compute jobs first (`history-outbox`, `history-pubsub`, possibly `sos/scomm`), keeping current settings for heavy jobs initially.

Pros:
- Direct per-execution cost reduction if lower limits are viable.
- Reversible via deploy config rollback.
- Egress impact: neutral.
- Database-size impact: neutral.

Cons:
- Wrong sizing can increase duration or failure rate.

Risks / effort:
- Medium effort (test + observe p95/failures).
- Moderate risk if changed broadly in one step.

### Option D: Reduce repeated work with cheap freshness checks

What it is:
- Add lightweight “unchanged” checks before expensive fetch/process paths.
- Examples: conditional requests (if upstream supports), checkpoint-hash checks, and longer backoff after repeated empty/no-timeseries outcomes.

Pros:
- Reduces compute without changing external behavior.
- Egress impact: lower upstream + DB round-trips for unchanged data.
- Database-size impact: fewer duplicate run artifacts; observation tables unchanged.

Cons:
- Adds logic complexity; requires careful idempotency handling.

Risks / effort:
- Medium effort.
- Low risk when introduced behind flags.

### Option E: Dispatcher pattern (architectural)

What it is:
- Replace multiple frequent per-network scheduler triggers with one dispatcher run that triggers only due jobs.

Pros:
- Fewer container cold starts and no-op runs across connectors.
- Egress impact: lower Run API invocations and startup overhead.
- Database-size impact: fewer run-log entries from no-op invocations.

Cons:
- More moving parts; larger operational change.

Risks / effort:
- Higher effort and integration risk.
- Best as later stage if simpler wins are insufficient.

## 6) Recommendation (staged, quick wins first)

### Stage 1: Attribution and baseline lock (now)

1. Keep `job_name` labels enforced on every deploy (already in place).
2. Enable/verify Billing export to BigQuery (if not already) and confirm labels in export tables.
3. Track these baseline metrics daily for each job:
   - executions/day
   - p50/p95 duration
   - failure rate
   - proxy CPU-sec/day

Validation after Stage 1:
- Data available by job label in BigQuery.
- Baseline dashboard/table saved for comparison.

### Stage 2: Quick usage cuts (lowest risk)

1. Reduce no-op-heavy scheduler frequencies (BreatheLondon, SensorCommunity, UK-AIR SOS) to match observed due cadence.
2. Tune OpenAQ self-scheduling to back off more aggressively on `no_station_refs` / repeated partial/empty runs.

Validation after Stage 2:
- >=20% drop in total executions/day for targeted jobs.
- No material increase in stale data indicators.

### Stage 3: Resource right-sizing

1. Trial lower CPU/memory on `history-outbox` and `history-pubsub` first.
2. Expand to other jobs only if p95 and failure rate remain stable.

Validation after Stage 3:
- Lower CPU-seconds/day for modified jobs with stable p95/failure.

### Stage 4: Optional deeper changes

- If spend remains high after Stages 2-3, implement dispatcher architecture or deeper profiling for OpenAQ path.

## 7) Future cost attribution by label

Current status:
- Deploy workflows set and verify `job_name` label.
- Live jobs in `europe-west2` show `metadata.labels.job_name` present.

Billing export status:
- Local check script exists: `scripts/gcp_billing_export_check.sh`.
- Current CLI run in this environment returned `FAIL` (dataset listing inaccessible in current context), so export status is not yet confirmed by CLI output here.

If export is not enabled:
- Console path: **Billing -> Billing export -> BigQuery export**.
- Enable at least Standard usage export; prefer detailed/resource export for richer label analysis.

Example query once export exists (resource export table preferred):

```sql
SELECT
  DATE(usage_start_time) AS usage_date,
  lbl.value AS job_name,
  SUM(cost) AS cost
FROM `BILLING_EXPORT_PROJECT.BILLING_EXPORT_DATASET.gcp_billing_export_resource_v1_*`,
UNNEST(labels) AS lbl
WHERE service.description = 'Cloud Run'
  AND lbl.key = 'job_name'
GROUP BY usage_date, job_name
ORDER BY usage_date DESC, cost DESC;
```

## 8) Appendix: Commands and log queries used

### Inventory + config

```bash
gcloud run jobs list --project "$GCP_PROJECT_ID" --region europe-west2 --format=json
gcloud run jobs describe <JOB> --project "$GCP_PROJECT_ID" --region europe-west2 --format=json
gcloud scheduler jobs list --project "$GCP_PROJECT_ID" --location europe-west2 --format=json
gcloud run services list --project "$GCP_PROJECT_ID" --region europe-west2 --format=json
```

### Execution frequency, duration, failure

```bash
gcloud run jobs executions list --project "$GCP_PROJECT_ID" --region europe-west2 --job <JOB> --limit 2000 --format=json
```

Fields used:
- `metadata.creationTimestamp`
- `status.startTime`
- `status.completionTime`
- `status.conditions[type=Completed]`

### Structured run/no-op patterns

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="<JOB>" AND timestamp>="2026-02-16T00:00:00Z" AND jsonPayload.message:*' \
  --project "$GCP_PROJECT_ID" --limit 5000 --format=json
```

### Error counts

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="<JOB>" AND timestamp>="2026-02-16T00:00:00Z" AND severity>=ERROR' \
  --project "$GCP_PROJECT_ID" --limit 2000 --format=json
```

### Billing export check

```bash
BILLING_EXPORT_PROJECT=<project> BILLING_EXPORT_DATASET=<dataset> ./scripts/gcp_billing_export_check.sh
```

## Missing for exact billing attribution (outside repo-only data)

- Exact billed CPU-seconds and cost by job label from Cloud Billing export tables (requires BigQuery export access/enablement).
- Per-container CPU utilization time series (if needed for tight right-sizing), which should be pulled from Cloud Monitoring after deciding target jobs.
