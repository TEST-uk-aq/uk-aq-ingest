# Breathe London Nodes staged response plan

## Objective

Stop the Cloudflare scheduler recording Breathe London Nodes as failed when the Cloud Run request exceeds Cloudflare's response wait, but the ingest later completes.

This applies only to `uk_aq_blondon_nodes`. Other scheduler jobs keep their current behaviour.

## Timing

- Keep the existing 30-second scheduler dispatch lead.
- Change the Breathe London Nodes child runtime default from `840` to `780` seconds.
- Wait up to 90 seconds for the Cloud Run HTTP response.
- If there is no response, mark the D1 dispatch `waiting_response`, not `failed`.
- Reconcile the final ingest result at 5, 10 and 14 minutes after `request_started_at`.
- At 14 minutes, clear the scheduler-side waiting state so the next 15-minute slot can proceed.
- Keep the Cloud Run service timeout at 900 seconds.

## Runtime changes

Change the default to `780` in:

- `workers/uk_aq_blondon_nodes_cloud_run/run_service.py`
- `.github/workflows/uk_aq_blondon_nodes_cloud_run_deploy.yml`

There is no repository variable overriding this value.

## D1 changes

Extend `scheduler_dispatches` with only the fields required for staged handling:

- `request_started_at`
- `response_received_at`
- `transport_status`
- `next_reconcile_at`
- `reconcile_stage`
- `ingest_run_id`
- `ingest_status`
- `confirmed_at`
- `reconcile_reason`

Add an index on `next_reconcile_at`.

Do not add a scheduler token to Supabase.

## Initial dispatch

For Breathe London Nodes:

1. Claim the due slot using the existing D1 duplicate protection.
2. Record `request_started_at`.
3. Start the Cloud Run request concurrently with other due jobs.
4. Wait for up to 90 seconds.
5. If HTTP 200 arrives, record the returned result normally.
6. If no response arrives, set:
   - `transport_status = 'waiting_response'`
   - `next_reconcile_at = request_started_at + 5 minutes`
   - `reconcile_stage = 1`
7. Do not increment the failed-job count for `waiting_response`.

Treat 90 seconds as a soft response deadline. Update D1 to waiting_response if the Cloud Run request is still pending, but do not cancel or alter the request. Allow the existing request to continue and process any later response without converting an HTTP 524 into a confirmed ingest failure.

## Reconciliation

At the start of each scheduler invocation, reconcile any Breathe London Nodes row whose `next_reconcile_at` is due before evaluating new cron slots.

Find the matching authoritative ingest run using:

```text
connector_code = 'blondon_nodes'
run_started_at >= request_started_at - 30 seconds
run_started_at < request_started_at + 2 minutes
```

Use a narrow authenticated RPC or endpoint that returns only:

- ingest-run ID
- connector code
- run start
- run end
- run status
- run message

### Match handling

- Exactly one terminal run:
  - store `ingest_run_id`
  - store the final `ingest_status`
  - set `confirmed_at`
  - clear `next_reconcile_at`

- No run at 5 minutes:
  - schedule the 10-minute check

- No run at 10 minutes:
  - schedule the 14-minute check

- No run at 14 minutes:
  - set `ingest_status = 'unconfirmed'`
  - clear `next_reconcile_at`
  - clear the scheduler-side waiting state
  - do not mark it failed

- More than one candidate:
  - do not guess
  - keep it unresolved until the next checkpoint
  - at 14 minutes mark it `unconfirmed`
  - record `multiple_candidate_ingest_runs`

A matched `ingest_run_id` must not be reused by another D1 dispatch.

## Next scheduled run

Keep the existing 30-second dispatch lead.

Reconciliation must run before new due-slot evaluation so the 14-minute checkpoint releases the previous scheduler-side hold before the next slot is claimed.

The Cloud Run `RUN_LOCK` and connector claim remain the final overlap protection.

Treat recognised `run_in_flight`, `not_due` and `claim_not_acquired` results as skipped, not failed.

## Configuration

Add staged settings for `uk_aq_blondon_nodes` in `cloudflare/scheduler/jobs.toml`:

```toml
response_wait_seconds = 90
reconcile_mode = "ingest_run_time_window"
reconcile_check_seconds = [300, 600, 840]
reconcile_connector_code = "blondon_nodes"
reconcile_window_before_seconds = 30
reconcile_window_after_seconds = 120
```

Update the scheduler config sync and validation code so these optional fields reach D1.

Jobs without these fields retain their current behaviour.

## Logging

Add concise structured events for:

- waiting for response
- reconciliation attempt
- reconciliation confirmed
- reconciliation pending
- reconciliation unconfirmed

Include the D1 dispatch ID, job key, due time, request start, checkpoint and matched ingest-run ID.

Do not log secrets.

## Dashboard

No dashboard changes are required.

The dashboard continues to use the authoritative Supabase connector and ingest-run state. D1 staged statuses are scheduler diagnostics only.

## Code validation

Before committing:

- run the existing scheduler code checks or tests that confirm the modified code loads and is syntactically valid
- run Python syntax/import validation for the changed Breathe London Nodes files
- validate the D1 migration and scheduler configuration files

Do not add a new pre-implementation test suite for this change.

## Live verification on the test system

After deployment, verify through normal real runs that:

- a run lasting over 90 seconds changes to `waiting_response`
- the ingest continues after the scheduler stops waiting
- the 5, 10 or 14-minute reconciliation records the authoritative result
- partial budget-limit completion is not shown as failed
- the next 15-minute run starts normally
- the dashboard continues to show its existing in-progress and final statuses

## Acceptance criteria

- Both runtime defaults are `780`.
- The 30-second dispatch lead is unchanged.
- No response after 90 seconds becomes `waiting_response`.
- Reconciliation occurs only at 5, 10 and 14 minutes.
- No Supabase scheduler token is added.
- D1 stores the matched ingest-run ID.
- No result at 14 minutes becomes `unconfirmed`, not failed.
- The next scheduled run is not blocked by the previous D1 waiting state.
- No dashboard changes are made.
