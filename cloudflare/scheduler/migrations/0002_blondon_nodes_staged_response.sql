create table scheduler_dispatches_next (
  id integer primary key autoincrement,
  job_key text not null,
  due_at text not null,
  claimed_at text not null default current_timestamp,
  dispatched_at text,
  target_type text not null,
  dry_run integer not null default 0 check (dry_run in (0, 1)),
  dispatch_status text not null check (dispatch_status in (
    'claimed', 'dry_run', 'dispatched', 'failed', 'skipped', 'waiting_response'
  )),
  reason text,
  response_status integer,
  response_preview text,
  next_reconcile_at text,
  reconcile_stage integer,
  ingest_status text,
  unique (job_key, due_at),
  foreign key (job_key) references scheduler_jobs(job_key)
);

insert into scheduler_dispatches_next (
  id, job_key, due_at, claimed_at, dispatched_at, target_type, dry_run,
  dispatch_status, reason, response_status, response_preview
)
select
  id, job_key, due_at, claimed_at, dispatched_at, target_type, dry_run,
  dispatch_status, reason, response_status, response_preview
from scheduler_dispatches;

drop table scheduler_dispatches;
alter table scheduler_dispatches_next rename to scheduler_dispatches;

create index scheduler_dispatches_job_time_idx
on scheduler_dispatches(job_key, due_at desc);

create index scheduler_dispatches_next_reconcile_at_idx
on scheduler_dispatches(next_reconcile_at)
where next_reconcile_at is not null;
