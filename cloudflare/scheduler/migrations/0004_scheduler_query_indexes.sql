create index if not exists scheduler_runs_latest_finished_idx
on scheduler_runs (
  scheduler_name,
  evaluation_window_end desc,
  id desc
)
where status = 'finished'
  and evaluation_window_end is not null;

create index if not exists scheduler_dispatches_due_reconcile_idx
on scheduler_dispatches (
  job_key,
  next_reconcile_at asc,
  id asc
)
where dispatch_status = 'waiting_response'
  and next_reconcile_at is not null;