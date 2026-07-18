# UK-AIR SOS validation

## Validation policy

Pre-deployment validation is limited to structural viability and the focused deterministic checks needed to protect the known failure-contract regression.

Functional validation happens through real operation on the TEST system after deployment.

Do not add a broad speculative test programme for this area.

## Current structural checks

The SOS failure-handling implementation was checked with:

```bash
deno check supabase/functions/ingest_sos/index.ts
deno check supabase/functions/uk_aq_dispatch_polls/index.ts
deno check workers/uk_aq_sos_cloud_run/result_contract.ts
deno check workers/uk_aq_sos_cloud_run/run_job.ts
deno check workers/uk_aq_sos_cloud_run/run_service.ts
deno test tests/ingest_sos_failure_test.ts
```

The focused tests cover:

- timeout and runtime-deadline probe mapping to HTTP 503 when no upstream response exists;
- preservation of actual upstream HTTP status;
- unknown local failure remaining HTTP 500;
- runtime-deadline aggregation and bounded timeseries samples;
- individual versus consolidated failure classification;
- recognised dependency failures retaining HTTP 502 or 503;
- partial runtime-budget results retaining HTTP 207;
- compact valid skip results;
- missing and invalid child results failing closed;
- rejection of incomplete or arbitrary child-result objects.

These checks establish code and contract structure. They do not prove the deployed TEST runtime or external UK-AIR behaviour.

## Required TEST operational validation

### 1. Healthy run

After deployment, run or observe one normal SOS Cloud Run invocation.

Confirm:

- the service returns HTTP 200 or a legitimate HTTP 207 partial response;
- the response contains the expected bounded fields;
- the connector run closes with the correct status and message;
- one `uk_aq_ingest_runs` row exists for a persisted attempt;
- observations and checkpoint updates are consistent with the selected work;
- no generic wrapper error is created.

### 2. Intentional skip

Observe a natural not-due or claim-not-acquired invocation, or inspect a run where no station or timeseries work was selected.

Confirm:

- HTTP 200;
- `status: skipped`;
- `run_status: skipped`;
- the correct bounded `reason`;
- connector ID where known;
- database persistence matches the skip type described in [`contract.md`](contract.md).

### 3. Genuine upstream failure

When UK-AIR naturally returns a probe failure, confirm:

- a real HTTP 502 remains 502 through Cloud Run; or
- a probe timeout/deadline with no upstream response returns HTTP 503;
- `upstream_status` is `null` only when no response was received;
- `upstream_failure_kind` and `connector_http_status` are retained;
- one authoritative edge-owned error record exists;
- there is no duplicate generic Cloud Run error row or wrapper Dropbox error JSON.

Do not manufacture an external outage solely to complete this validation. Record the check as pending until a representative natural failure occurs.

### 4. Runtime-budget stop

When a genuine runtime stop occurs, confirm:

- HTTP 207;
- `partial: true`;
- `stopped_reason: runtime_budget_exceeded`;
- exactly one consolidated runtime-budget error record;
- the consolidated record is present even when the in-flight deadline failure count is zero;
- the timeseries sample is bounded;
- non-deadline failures remain individually recorded;
- a fully completed run is not marked partial after the deadline merely passes.

## Fail-closed child-result validation

The missing and invalid child-result branches are protected by focused pure tests and should not be triggered deliberately in the deployed service unless diagnosing a suspected wrapper regression.

If observed naturally:

- exit code 0 plus empty result must return HTTP 500 with `missing_child_result`;
- exit code 0 plus malformed or invalid result must return HTTP 500 with `invalid_child_result`;
- either condition is a wrapper defect and should be investigated before treating the run as successful.

## Rollback validation

After rollback and redeployment:

- confirm the intended prior revision is active;
- run one normal TEST invocation;
- confirm the connector run closes;
- confirm no new result-contract error appears;
- retain incident evidence before deleting or superseding diagnostic notes.

## Current operational status

The code and focused local checks have been reviewed. Real TEST deployment and operational validation must be recorded separately. Do not describe the behaviour as operationally proven until the relevant deployed checks above have passed.