create table if not exists scheduler_jobs (
  job_key text primary key,

  enabled integer not null default 1
    check (enabled in (0, 1)),

  target_type text not null
    check (target_type in ('github_workflow', 'cloud_run')),

  cron_expr text not null,
  timezone text not null default 'UTC',

  github_repo text,
  github_workflow_file text,
  github_ref text not null default 'main',
  github_inputs_json text,

  cloud_run_url text,
  cloud_run_method text not null default 'POST',
  cloud_run_headers_json text,
  cloud_run_body_json text,

  dry_run integer not null default 1
    check (dry_run in (0, 1)),

  notes text,

  created_at text not null default current_timestamp,
  updated_at text not null default current_timestamp,

  check (timezone in ('UTC', 'Etc/UTC')),
  check (
    target_type != 'github_workflow'
    or (
      github_repo is not null
      and github_workflow_file is not null
      and github_ref is not null
    )
  ),
  check (
    target_type != 'cloud_run'
    or cloud_run_url is not null
  ),
  check (
    github_inputs_json is null or json_valid(github_inputs_json)
  ),
  check (
    cloud_run_headers_json is null or json_valid(cloud_run_headers_json)
  ),
  check (
    cloud_run_body_json is null or json_valid(cloud_run_body_json)
  )
);

create table if not exists scheduler_dispatches (
  id integer primary key autoincrement,

  job_key text not null,
  due_at text not null,

  claimed_at text not null default current_timestamp,
  dispatched_at text,

  target_type text not null,
  dry_run integer not null default 0
    check (dry_run in (0, 1)),

  dispatch_status text not null
    check (dispatch_status in (
      'claimed',
      'dry_run',
      'dispatched',
      'failed',
      'skipped'
    )),

  reason text,
  response_status integer,
  response_preview text,

  unique (job_key, due_at),

  foreign key (job_key)
    references scheduler_jobs(job_key)
);

create index if not exists scheduler_dispatches_job_time_idx
on scheduler_dispatches(job_key, due_at desc);

create table if not exists scheduler_runs (
  id integer primary key autoincrement,

  scheduler_name text not null,
  started_at text not null,
  finished_at text,

  status text not null
    check (status in ('started', 'finished', 'failed')),

  previous_run_started_at text,
  evaluation_window_start text,
  evaluation_window_end text,

  jobs_checked integer not null default 0,
  jobs_due integer not null default 0,
  jobs_claimed integer not null default 0,
  jobs_dispatched integer not null default 0,
  jobs_failed integer not null default 0,

  error_message text
);

create index if not exists scheduler_runs_name_started_idx
on scheduler_runs(scheduler_name, started_at desc);
