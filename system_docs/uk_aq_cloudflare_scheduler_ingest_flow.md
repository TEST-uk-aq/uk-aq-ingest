# UK AQ Cloudflare Scheduler Ingest Flow

1. Cloudflare invokes `uk-aq-cron-scheduler-ingest` every minute.
2. The Worker determines its D1 lookback window and shifts it forward by one
   minute to compensate for dispatch latency.
3. It reads enabled jobs from `scheduler_jobs`.
4. For every due slot it inserts a unique dispatch claim keyed by `job_key` and
   `due_at`.
5. Dry-run jobs record the decision and make no network request.
6. After all due slots have been claimed, real job targets start concurrently.
   Cloud Run jobs POST the D1-configured body to the deployment-managed URL with
   `x-uk-aq-dispatch-secret`; GitHub workflow jobs dispatch their configured
   workflow. One slow or failed target does not delay another target's initial
   dispatch.
7. The Cloud Run service validates `UK_AQ_EDGE_UPSTREAM_SECRET` before starting
   work and returns 403 for missing or invalid authentication.
8. The Worker records response status, a bounded response preview, and the final
   scheduler-run summary in D1.

Service deployment and schedule configuration are separate paths:

```text
jobs.toml change -> config-sync workflow -> D1 schedule/body/dry-run fields
Cloud Run deploy -> resolve status.url -> reconcile one D1 cloud_run_url field
```

Config sync deliberately preserves deployment-managed URLs. The service deploy
workflow verifies the exact D1 row after reconciliation, preventing one service
deployment from overwriting another service's target.
