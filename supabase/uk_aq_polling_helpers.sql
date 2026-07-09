-- Helper RPCs for the dispatcher (no pg_cron schedules).
-- Functions live in uk_aq_core and read uk_aq_raw checkpoints.

create or replace function uk_aq_core.blondon_communities_select_station_refs(
  batch_limit integer default 10,
  stale_limit integer default 4
)
returns text[]
language plpgsql
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_connector_id integer;
  station_refs text[];
begin
  select id into v_connector_id
  from connectors
  where connector_code = 'blondon_communities'
  limit 1;

  if v_connector_id is null then
    return null;
  end if;

  with latest_obs as (
    select
      t.station_id,
      max(t.last_value_at) as last_observed_at
    from timeseries t
    where t.connector_id = v_connector_id
      and t.service_ref = 'breathelondon'
    group by t.station_id
  ),
  candidates as (
    select
      stn.id as station_id,
      stn.station_ref,
      bsc.next_due_at,
      bsc.last_polled_at,
      coalesce(bsc.last_observed_at, lo.last_observed_at) as last_observed_at,
      coalesce(bsc.next_due_at, now()) as due_at
    from stations stn
    left join blondon_communities_station_checkpoints bsc
      on bsc.station_id = stn.id
    left join latest_obs lo
      on lo.station_id = stn.id
    left join station_metadata sm
      on sm.station_id = stn.id
    where stn.connector_id = v_connector_id
      and stn.service_ref = 'breathelondon'
      and stn.station_ref is not null
      and stn.removed_at is null
      and (
        lower(coalesce(sm.attributes->>'enabled', '')) in ('y','yes','true','1')
        or lower(coalesce(sm.attributes->>'site_active', '')) in ('y','yes','true','1')
      )
  ),
  tiered as (
    select
      station_id,
      station_ref,
      due_at,
      last_polled_at
    from candidates
    where due_at <= now()
      and due_at >= now() - interval '3 hours'
      and (last_polled_at is null or last_polled_at <= now() - interval '5 minutes')
    union all
    select
      station_id,
      station_ref,
      due_at,
      last_polled_at
    from candidates
    where due_at < now() - interval '3 hours'
      and due_at >= now() - interval '24 hours'
      and (last_polled_at is null or last_polled_at <= now() - interval '1 hour')
  ),
  tiered_limited as (
    select *
    from tiered
    order by last_polled_at asc nulls first, due_at asc
    limit batch_limit
  ),
  stale as (
    select
      c.station_id,
      c.station_ref,
      c.last_observed_at
    from candidates c
    where c.due_at <= now()
      and (c.last_observed_at is null or c.last_observed_at <= now() - interval '24 hours')
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '12 hours')
      and not exists (
        select 1 from tiered_limited t where t.station_id = c.station_id
      )
    order by c.last_observed_at nulls first
    limit stale_limit
  ),
  combined as (
    select station_ref, 1 as group_order, due_at as sort_at
    from tiered_limited
    union all
    select station_ref, 2 as group_order, null as sort_at
    from stale
  )
  select array_agg(combined.station_ref order by group_order, sort_at nulls last)
  into station_refs
  from combined;

  return station_refs;
end;
$$;

create or replace function uk_aq_core.sos_select_station_refs(
  batch_limit integer default 100,
  stale_limit integer default 4
)
returns text[]
language plpgsql
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_connector_id integer;
  station_refs text[];
begin
  select id into v_connector_id
  from connectors
  where connector_code = 'sos'
  limit 1;

  if v_connector_id is null then
    return null;
  end if;

  with latest_obs as (
    select
      t.station_id,
      max(t.last_value_at) as last_observed_at
    from timeseries t
    where t.connector_id = v_connector_id
      and t.ended_at is null
    group by t.station_id
  ),
  station_with_timeseries as (
    select distinct t.station_id
    from timeseries t
    where t.connector_id = v_connector_id
      and t.ended_at is null
  ),
  candidates as (
    select
      stn.id as station_id,
      stn.station_ref,
      sc.next_due_at,
      sc.last_polled_at,
      nullif(
        greatest(
          coalesce(sc.last_observed_at, '-infinity'::timestamptz),
          coalesce(lo.last_observed_at, '-infinity'::timestamptz)
        ),
        '-infinity'::timestamptz
      ) as last_observed_at,
      coalesce(sc.next_due_at, now()) as due_at
    from stations stn
    join station_with_timeseries swt
      on swt.station_id = stn.id
    left join sos_station_checkpoints sc
      on sc.station_id = stn.id
    left join latest_obs lo
      on lo.station_id = stn.id
    where stn.connector_id = v_connector_id
      and stn.station_ref is not null
      and stn.removed_at is null
  ),
  tier1 as (
    select
      station_id,
      station_ref,
      due_at,
      last_polled_at
    from candidates
    where due_at <= now()
      and due_at >= now() - interval '6 hours'
      and (last_polled_at is null or last_polled_at <= now() - interval '5 minutes')
    order by last_polled_at asc nulls first, due_at asc
    limit batch_limit
  ),
  tier2 as (
    select
      c.station_id,
      c.station_ref,
      c.due_at,
      c.last_polled_at
    from candidates c
    where c.due_at < now() - interval '6 hours'
      and c.due_at >= now() - interval '24 hours'
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '1 hour')
      and not exists (
        select 1 from tier1 t where t.station_id = c.station_id
      )
    order by c.last_polled_at asc nulls first, c.due_at asc
    limit greatest(0, batch_limit - (select count(*) from tier1))
  ),
  tiered_limited as (
    select station_id, station_ref, due_at, last_polled_at
    from tier1
    union all
    select station_id, station_ref, due_at, last_polled_at
    from tier2
  ),
  stale as (
    select
      c.station_id,
      c.station_ref,
      c.last_observed_at
    from candidates c
    where (c.last_observed_at is null or c.last_observed_at <= now() - interval '24 hours')
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '12 hours')
      and not exists (
        select 1 from tiered_limited t where t.station_id = c.station_id
      )
    order by c.last_observed_at nulls first
    limit least(
      stale_limit,
      greatest(0, batch_limit - (select count(*) from tiered_limited))
    )
  ),
  combined as (
    select station_ref, 1 as group_order, due_at as sort_at
    from tiered_limited
    union all
    select station_ref, 2 as group_order, null as sort_at
    from stale
  )
  select array_agg(combined.station_ref order by group_order, sort_at nulls last)
  into station_refs
  from combined;

  return station_refs;
end;
$$;

-- Dispatch queue helpers (two-stage dispatcher).

create table if not exists uk_aq_raw.dispatch_connector_queue (
  id bigint generated by default as identity primary key,
  connector_code text not null unique,
  payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  next_attempt_at timestamptz not null default now(),
  attempts integer not null default 0,
  last_error text,
  lease_expires_at timestamptz
);

create index if not exists dispatch_connector_queue_next_attempt_idx
  on uk_aq_raw.dispatch_connector_queue (next_attempt_at, created_at);

create index if not exists dispatch_connector_queue_lease_idx
  on uk_aq_raw.dispatch_connector_queue (lease_expires_at);

create or replace function uk_aq_core.uk_aq_dispatch_queue_enqueue(
  p_entries jsonb
)
returns table(rows_enqueued int)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_count int := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_entries is null
    or jsonb_typeof(p_entries) <> 'array'
    or jsonb_array_length(p_entries) = 0
  then
    return query select 0;
    return;
  end if;

  insert into uk_aq_raw.dispatch_connector_queue (
    connector_code,
    payload,
    next_attempt_at,
    updated_at
  )
  select
    e.connector_code,
    coalesce(e.payload, '{}'::jsonb),
    coalesce(e.next_attempt_at, now()),
    now()
  from jsonb_to_recordset(p_entries) as e(
    connector_code text,
    payload jsonb,
    next_attempt_at timestamptz
  )
  where e.connector_code is not null
    and btrim(e.connector_code) <> ''
  on conflict (connector_code) do update set
    payload = case
      when excluded.payload = '{}'::jsonb then uk_aq_raw.dispatch_connector_queue.payload
      else excluded.payload
    end,
    next_attempt_at = least(
      uk_aq_raw.dispatch_connector_queue.next_attempt_at,
      coalesce(excluded.next_attempt_at, now())
    ),
    updated_at = now();

  get diagnostics v_count = row_count;
  return query select coalesce(v_count, 0);
end;
$$;

create or replace function uk_aq_core.uk_aq_dispatch_queue_claim(
  p_batch_limit integer default 1,
  p_lease_seconds integer default 900
)
returns table(
  id bigint,
  connector_code text,
  payload jsonb,
  attempts integer
)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_limit integer;
  v_lease_seconds integer;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  v_limit := greatest(1, coalesce(p_batch_limit, 1));
  v_lease_seconds := greatest(30, least(coalesce(p_lease_seconds, 900), 3600));

  return query
  with due as (
    select q.id, q.next_attempt_at, q.created_at
    from uk_aq_raw.dispatch_connector_queue q
    where q.next_attempt_at <= now()
      and (q.lease_expires_at is null or q.lease_expires_at <= now())
    order by q.next_attempt_at asc, q.created_at asc
    for update skip locked
    limit v_limit
  ),
  claimed as (
    update uk_aq_raw.dispatch_connector_queue q
    set
      lease_expires_at = now() + make_interval(secs => v_lease_seconds),
      updated_at = now()
    from due
    where q.id = due.id
    returning q.id, q.connector_code, q.payload, q.attempts
  )
  select c.id, c.connector_code, c.payload, c.attempts
  from claimed c
  join due d on d.id = c.id
  order by d.next_attempt_at asc, d.created_at asc;
end;
$$;

create or replace function uk_aq_core.uk_aq_dispatch_queue_resolve(
  p_resolutions jsonb
)
returns table(rows_resolved int)
language plpgsql
security definer
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_resolved int := 0;
begin
  if auth.role() <> 'service_role' then
    raise exception 'service_role required';
  end if;

  if p_resolutions is null
    or jsonb_typeof(p_resolutions) <> 'array'
    or jsonb_array_length(p_resolutions) = 0
  then
    return query select 0;
    return;
  end if;

  with incoming as (
    select
      (item->>'id')::bigint as id,
      coalesce((item->>'ok')::boolean, false) as ok,
      nullif(item->>'error', '') as error_message,
      case
        when item ? 'retry_in_seconds'
          and (item->>'retry_in_seconds') ~ '^[0-9]+$'
        then greatest(5, least((item->>'retry_in_seconds')::int, 3600))
        else null
      end as retry_in_seconds
    from jsonb_array_elements(p_resolutions) item
    where item ? 'id'
  ),
  deleted as (
    delete from uk_aq_raw.dispatch_connector_queue q
    using incoming i
    where i.ok = true
      and q.id = i.id
    returning q.id
  ),
  failed as (
    update uk_aq_raw.dispatch_connector_queue q
    set
      attempts = q.attempts + 1,
      last_error = coalesce(i.error_message, q.last_error, 'dispatch job failed'),
      next_attempt_at = now() + make_interval(
        secs => coalesce(
          i.retry_in_seconds,
          case q.attempts
            when 0 then 30
            when 1 then 120
            when 2 then 600
            else 1800
          end
        )
      ),
      lease_expires_at = null,
      updated_at = now()
    from incoming i
    where i.ok = false
      and q.id = i.id
    returning q.id
  )
  select
    (select count(*) from deleted) + (select count(*) from failed)
  into v_resolved;

  return query select coalesce(v_resolved, 0);
end;
$$;

create or replace function uk_aq_core.erg_laqn_select_station_refs(
  batch_limit integer default 10,
  active_only boolean default true
)
returns text[]
language plpgsql
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_connector_id integer;
  station_refs text[];
begin
  select id into v_connector_id
  from connectors
  where connector_code = 'erg_laqn'
  limit 1;

  if v_connector_id is null then
    return null;
  end if;

  with latest_obs as (
    select
      t.station_id,
      max(o.observed_at) as latest_observed_at
    from timeseries t
    join observations o on o.timeseries_id = t.id
    where t.connector_id = v_connector_id
      and t.service_ref = 'erg_laqn'
    group by t.station_id
  ),
  candidates as (
    select
      stn.station_ref,
      lo.latest_observed_at,
      esc.last_polled_at
    from stations stn
    left join latest_obs lo on lo.station_id = stn.id
    left join erg_laqn_station_checkpoints esc on esc.station_id = stn.id
    where stn.connector_id = v_connector_id
      and stn.service_ref = 'erg_laqn'
      and stn.station_ref is not null
      and (not active_only or stn.removed_at is null)
    order by esc.last_polled_at nulls first,
      stn.station_ref
    limit batch_limit
  )
  select array_agg(station_ref) into station_refs
  from candidates;

  return station_refs;
end;
$$;

create or replace function uk_aq_core.openaq_select_station_refs(
  batch_limit integer default 52,
  stale_limit integer default 4,
  tier1_retry_seconds integer default 300
)
returns text[]
language plpgsql
set search_path = uk_aq_core, uk_aq_raw, public, pg_catalog
as $$
declare
  v_connector_id integer;
  station_refs text[];
begin
  select id into v_connector_id
  from connectors
  where connector_code = 'openaq'
  limit 1;

  if v_connector_id is null then
    return null;
  end if;

  with latest_obs as (
    select
      t.station_id,
      max(t.last_value_at) as last_observed_at
    from timeseries t
    where t.connector_id = v_connector_id
      and t.service_ref = 'openaq'
    group by t.station_id
  ),
  candidates as (
    select
      stn.id as station_id,
      stn.station_ref,
      osc.next_due_at,
      osc.last_polled_at,
      nullif(
        greatest(
          coalesce(osc.last_observed_at, '-infinity'::timestamptz),
          coalesce(lo.last_observed_at, '-infinity'::timestamptz)
        ),
        '-infinity'::timestamptz
      ) as last_observed_at,
      coalesce(osc.next_due_at, now()) as due_at
    from stations stn
    left join openaq_station_checkpoints osc
      on osc.station_id = stn.id
    left join latest_obs lo
      on lo.station_id = stn.id
    where stn.connector_id = v_connector_id
      and stn.service_ref = 'openaq'
      and stn.station_ref is not null
      and stn.removed_at is null
  ),
  tier1 as (
    select
      station_id,
      station_ref,
      due_at,
      last_polled_at
    from candidates
    where due_at <= now()
      and due_at >= now() - interval '6 hours'
      and (
        last_polled_at is null or
        last_polled_at <= now() - make_interval(secs => greatest(0, tier1_retry_seconds))
      )
    order by last_polled_at asc nulls first, due_at asc
    limit batch_limit
  ),
  tier2 as (
    select
      c.station_id,
      c.station_ref,
      c.due_at,
      c.last_polled_at
    from candidates c
    where c.due_at < now() - interval '6 hours'
      and c.due_at >= now() - interval '24 hours'
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '1 hour')
      and not exists (
        select 1 from tier1 t where t.station_id = c.station_id
      )
    order by c.last_polled_at asc nulls first, c.due_at asc
    limit greatest(0, batch_limit - (select count(*) from tier1))
  ),
  tiered_limited as (
    select station_id, station_ref, due_at, last_polled_at
    from tier1
    union all
    select station_id, station_ref, due_at, last_polled_at
    from tier2
  ),
  stale as (
    select
      c.station_id,
      c.station_ref,
      c.last_observed_at
    from candidates c
    where c.due_at <= now() - interval '24 hours'
      and (c.last_polled_at is null or c.last_polled_at <= now() - interval '12 hours')
    order by c.due_at asc
    limit stale_limit
  ),
  combined as (
    select station_ref, 1 as group_order, due_at as sort_at
    from tier1
    union all
    select station_ref, 2 as group_order, due_at as sort_at
    from tier2
    union all
    select station_ref, 3 as group_order, null as sort_at
    from stale
  )
  select array_agg(combined.station_ref order by group_order, sort_at nulls last)
  into station_refs
  from combined;

  return station_refs;
end;
$$;
