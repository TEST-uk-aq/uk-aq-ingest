alter table scheduler_runs add column minute_slot text;
alter table scheduler_runs add column trigger_source text;

create unique index if not exists scheduler_runs_name_minute_slot_idx
on scheduler_runs(scheduler_name, minute_slot);
