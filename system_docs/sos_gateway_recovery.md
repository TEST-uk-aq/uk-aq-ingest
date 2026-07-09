# UK-AIR SOS — Gateway Recovery Playbook

Operational playbook for recovering UK-AIR SOS ingest after an upstream gateway outage.

See also: [sos.md](sos.md) (network description), [sos_ingest_flow.md](sos_ingest_flow.md) (normal-operation flow), [`workers/uk_aq_sos_cloud_run/run_job.ts`](../workers/uk_aq_sos_cloud_run/run_job.ts) (Cloud Run dispatcher), [`scripts/sos/sos_ingest.py`](../scripts/sos/sos_ingest.py) (catalog + ingest worker).

## When to use this doc

- The upstream UK-AIR SOS gateway (`uk-air.defra.gov.uk/sos-ukair/...`) returned 5xx / timeouts for an extended period (typically hours)
- You paused polling (cron disabled, or Cloud Run job stopped) during the outage
- The gateway is now responding 200 and you want to resume safely

## What state the system is in after an outage

- `sos_station_checkpoints.last_polled_at` is stale or `NULL`
- `sos_station_checkpoints.next_due_at` may be far in the past or poisoned (rare — see below)
- `timeseries.last_value_at` is stale for everything not polled during the outage
- `timeseries.catalog_missing_runs` may be elevated if the catalog reconciler ran against a partial/empty gateway response
- Some `timeseries.ended_at` may be set if the reconciler hit its threshold during the outage
- Integrity job will surface gaps for outage days

## Recovery levers, in order of preference

1. **Wait** — if the outage was brief and polling kept running, normal cadence will catch up on its own
2. **Reset station checkpoints** — clears the cadence-shaping lag history and forces immediate eligibility
3. **Run catalog discovery once** — re-confirms which timeseries the gateway acknowledges; required if you're about to reactivate ended timeseries
4. **Selectively reactivate end-dated timeseries** — only those the catalog confirms exist
5. **Bulk end-date verified orphans** — clean up the 404-noise from any timeseries that the gateway no longer serves
6. **Backfill via integrity job** — for days the original polling missed entirely

Do **NOT** start by reactivating every `ended_at IS NOT NULL` row. That's the most common recovery mistake; see "Pitfalls" below.

## Standard recovery sequence

### 1. Confirm the gateway is back

```bash
curl -fsS "https://uk-air.defra.gov.uk/sos-ukair/api/v1/services" | head -c 200
```

Expect a valid 200 with a JSON body listing services. If still 5xx, stop — don't try to recover yet.

### 2. Reset station checkpoints (Cloud Run path)

This is the safe, idempotent reset for the Cloud Run station-level polling. Run inside a transaction:

```sql
begin;

-- Wipe every SOS station checkpoint so the dispatcher schedules them fresh:
-- next_due_at = now() (eligible immediately), clear last_polled_at, drop the
-- lag-sample history that's been poisoning the cadence, reseed last_observed_at
-- from the per-station max(last_value_at) we already have.
with sos as (
  select id from uk_aq_core.connectors where connector_code = 'sos'
),
station_truth as (
  select ts.station_id, max(ts.last_value_at) as max_last_value_at
  from uk_aq_core.timeseries ts
  join sos on sos.id = ts.connector_id
  where ts.station_id is not null
  group by ts.station_id
)
update uk_aq_raw.sos_station_checkpoints sc
set next_due_at        = now(),
    last_polled_at     = null,
    ingest_lag_samples = '{}'::int[],
    last_observed_at   = station_truth.max_last_value_at,
    updated_at         = now()
from station_truth
where sc.station_id = station_truth.station_id;

commit;
```

Verification:

```sql
select count(*) as due_now
from uk_aq_raw.sos_station_checkpoints
where next_due_at <= now();
```

### 3. Realign `timeseries.last_value_at` if drift suspected

Optional but recommended after a long outage — repairs cases where `last_value_at` was poisoned by API metadata (see [the SOS bug fix history](#known-bug-history-late-2026)):

```sql
with sos as (
  select id from uk_aq_core.connectors where connector_code = 'sos'
),
truth as (
  select o.timeseries_id, max(o.observed_at) as max_observed_at
  from uk_aq_core.observations o
  join uk_aq_core.timeseries ts on ts.id = o.timeseries_id
  join sos on sos.id = ts.connector_id
  group by o.timeseries_id
)
update uk_aq_core.timeseries ts
set last_value_at = truth.max_observed_at,
    updated_at    = now()
from truth
where ts.id = truth.timeseries_id
  and (ts.last_value_at is distinct from truth.max_observed_at);
```

### 4. Resume polling, then run catalog discovery once

Re-enable the SOS cron (currently `*/15`) or restart the Cloud Run job.

The catalog reconciler runs as part of full-catalog discovery in `sos_ingest.py` (see `reconcile_timeseries_catalog` around line 1227). It sets `last_catalog_seen_at` on every timeseries the gateway acknowledges and increments `catalog_missing_runs` for those it doesn't.

If you want to force one immediately (instead of waiting for the next scheduled discovery):

```bash
# From a host with the SOS ingest environment configured
python3 scripts/sos/sos_ingest.py --discover-catalog-only --connector sos
```

(Exact flag depends on the script revision — check `--help` if unsure.)

### 5. Verify recovery before cleanup

```sql
-- Stations being polled in the last hour
select count(*) as polled_recent
from uk_aq_core.sos_station_checkpoints
where last_polled_at > now() - interval '1 hour';

-- Fresh-data signal per pollutant (target ~3-hour freshness)
select
  ph.pollutant_key,
  count(*) filter (where ts.last_value_at > now() - interval '3 hours') as fresh,
  count(*) as total
from uk_aq_core.timeseries ts
join uk_aq_core.phenomena ph on ph.id = ts.phenomenon_id
join uk_aq_core.connectors c on c.id = ts.connector_id
where c.connector_code = 'sos'
  and ts.ended_at is null
group by ph.pollutant_key
order by fresh desc;
```

PM2.5/PM10/NO2/O3 should show non-zero `fresh` within ~30–60 min of restart. If they don't, see "Pitfalls — headline pollutants stay stale".

### 6. Clean up post-recovery orphans

After 1 or 2 catalog cycles, anything you reactivated that the gateway no longer serves will be auto-end-dated by the reconciler when `catalog_missing_runs >= UK_AIR_TIMESERIES_END_MISSING_RUNS` (currently `2`, defined at [`sos_ingest.py:87`](../scripts/sos/sos_ingest.py#L87)).

If you don't want to wait, identify and end-date orphans directly:

```sql
-- Find 404-storming orphans (verified-orphan profile)
select
  ts.id, ts.timeseries_ref,
  ts.last_value_at, ts.last_catalog_seen_at,
  ts.catalog_missing_runs,
  s.station_ref
from uk_aq_core.timeseries ts
join uk_aq_core.connectors c on c.id = ts.connector_id
left join uk_aq_core.stations s on s.id = ts.station_id
where c.connector_code = 'sos'
  and ts.ended_at is null
  and ts.last_value_at is null
  and ts.last_catalog_seen_at is null
order by ts.catalog_missing_runs desc;
```

If the list matches the 404 noise in `error_logs`, bulk-retire them:

```sql
-- Conservative bulk end-date: only timeseries that have never had data AND
-- the catalog has never confirmed since the last recovery reset.
update uk_aq_core.timeseries
set ended_at = now(), updated_at = now()
where connector_id = (select id from uk_aq_core.connectors where connector_code = 'sos')
  and last_value_at is null
  and last_catalog_seen_at is null
  and ended_at is null;
```

Safety net: if any of these *should* legitimately exist, the next catalog run will reactivate them via the "if seen → `ended_at = null`" branch in [`sos_ingest.py:1313-1320`](../scripts/sos/sos_ingest.py#L1313-L1320). No data loss risk.

### 7. Backfill missed days (only if observations actually exist upstream)

If the gateway has historical data for the outage window, run the integrity job to detect gaps and trigger source→R2 backfills. See [`/workspaces/uk-aq-ops/system_docs/uk-aq-r2-history-integrity.md`](../../uk-aq-ops/system_docs/uk-aq-r2-history-integrity.md). SOS adapter doesn't expose archive download today, so this typically applies to the OpenAQ-side data the integrity job covers.

## Pitfalls

### Don't reactivate every `ended_at` row at once

The biggest recovery mistake. A blanket `update timeseries set ended_at = null where connector_id = sos` reactivates legitimately-retired orphans alongside the timeseries you actually want back. They then get polled, return 404, and flood `error_logs`.

Instead, **only reactivate rows where the catalog has confirmed they exist** (`last_catalog_seen_at` is recent), or:

```sql
-- Surgical reactivation: only resurrect timeseries the catalog recently saw
update uk_aq_core.timeseries
set ended_at = null, catalog_missing_runs = 0, updated_at = now()
where connector_id = (select id from uk_aq_core.connectors where connector_code = 'sos')
  and ended_at is not null
  and last_catalog_seen_at > now() - interval '7 days';
```

### LIVE and TEST will not look identical post-recovery

LIVE and TEST receive the same upstream from UK-AIR SOS, but they often diverge after a gateway outage because the operator typically handles them differently (pausing one, leaving the other polling, etc.). The asymmetry isn't a bug — it's a clue that the recovery paths differed.

If only one env shows 404 noise, the *active* env almost certainly has timeseries the other doesn't (usually orphans the paused env was protected from). Run the orphan check on the noisy env, not the quiet one.

### Headline pollutants (PM2.5 / PM10 / NO2 / O3) stay stale

If station-level polling is happening but headline pollutants don't refresh, the most likely cause is the [`upsertLastValue` integrity bug](#known-bug-history-late-2026) — fixed in commit `<sha>` but worth checking the deployed code hasn't regressed. The symptom is `timeseries.last_value_at` updates from API metadata even when no real observation row was written.

Also possible: catalog reconciler end-dated those headline timeseries during the outage. Check:

```sql
select ph.pollutant_key, count(*) as ended
from uk_aq_core.timeseries ts
join uk_aq_core.phenomena ph on ph.id = ts.phenomenon_id
join uk_aq_core.connectors c on c.id = ts.connector_id
where c.connector_code = 'sos' and ts.ended_at is not null
group by ph.pollutant_key
order by ended desc;
```

If PM2.5/PM10/NO2 dominate, reactivate the recently-catalog-seen ones (per the surgical SQL above).

### Catalog reconciler can over-retire during a partial-response outage

The reconciler increments `catalog_missing_runs` for every timeseries the catalog response doesn't include. If the gateway returns a partial catalog during a degraded period, the reconciler will start ending things prematurely. Threshold is currently 2 missing runs.

Mitigations (not yet implemented — see Open improvements):
- Guard the reconciler with "don't end if catalog returned <80% of previous size"
- Raise the threshold from 2 to 5-10

### 404 cooldown is not implemented

There's no per-timeseries 404 cooldown in the dispatcher today. If your post-recovery orphan list is large, the only fix is bulk end-date. See [`/workspaces/uk-aq-ops/plans/sos-404-cooldown-and-retirement-plan.md`](../../uk-aq-ops/plans/sos-404-cooldown-and-retirement-plan.md) for the deferred design.

## Lifecycle quick reference

The reconciler runs as part of full catalog discovery and uses three fields on `uk_aq_core.timeseries`:

| Field | Meaning |
|---|---|
| `last_catalog_seen_at` | Most recent catalog run where the gateway returned this timeseries |
| `catalog_missing_runs` | Count of consecutive catalog runs where the gateway did NOT return it (reset to 0 when seen) |
| `ended_at` | Set to `now()` when `catalog_missing_runs >= UK_AIR_TIMESERIES_END_MISSING_RUNS` (currently 2). Cleared back to null when the catalog sees the timeseries again. |

The reconciler only runs for full catalog discovery (`station_refs is None` in `discover_timeseries`), not for scoped per-station polls.

## Known bug history (late 2026)

- **`upsertLastValue` integrity bug** ([fixed commit `ddedd35`+]): pre-fix code wrote `last_value_at` from API metadata even when no observation row was inserted, falsely advertising freshness. Post-fix, `last_value_at` is only written from real observation rows.
- **Recovery reactivation too broad** (this incident, May 2026): a `set ended_at = null` for every SOS row reactivated ~143 verified orphans, which then 404-stormed for ~24h until bulk-retired. Recovery SQL templates above are now scoped to avoid this.
- **Catalog reconciler too aggressive** (May 2026): `UK_AIR_TIMESERIES_END_MISSING_RUNS = 2` end-dated headline-pollutant timeseries during an extended gateway outage when the gateway returned partial catalog responses. Raising the threshold or adding a partial-catalog guard is a candidate fix.

## Open improvements

- Per-timeseries 404 cooldown (Phase 1 of the [SOS 404 plan](../../uk-aq-ops/plans/sos-404-cooldown-and-retirement-plan.md))
- Catalog reconciler sanity guard ("don't end-date if catalog shrank by more than X%")
- Recovery script that wraps the SQL fragments above behind a CLI with dry-run + confirmation prompts
