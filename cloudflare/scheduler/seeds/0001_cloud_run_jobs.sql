insert or ignore into scheduler_jobs (
  job_key,
  enabled,
  target_type,
  cron_expr,
  timezone,
  github_ref,
  cloud_run_url,
  cloud_run_method,
  cloud_run_body_json,
  dry_run,
  notes
) values
(
  'uk_aq_blondon_communities', 1, 'cloud_run', '*/15 * * * *', 'UTC', 'main',
  'https://deployment-pending.invalid/run', 'POST', '{"trigger_mode":"safety"}', 1,
  'Breathe London Communities safety ingest trigger'
),
(
  'uk_aq_blondon_nodes', 1, 'cloud_run', '*/15 * * * *', 'UTC', 'main',
  'https://deployment-pending.invalid/run', 'POST', '{}', 1,
  'Breathe London Nodes scheduled ingest trigger'
),
(
  'uk_aq_openaq_safety', 1, 'cloud_run', '*/30 * * * *', 'UTC', 'main',
  'https://deployment-pending.invalid/run', 'POST', '{"trigger_mode":"safety"}', 1,
  'OpenAQ safety trigger; service decides whether work is needed'
),
(
  'uk_aq_scomm', 1, 'cloud_run', '*/15 * * * *', 'UTC', 'main',
  'https://deployment-pending.invalid/run', 'POST', '{"trigger_mode":"safety"}', 1,
  'Sensor.Community safety ingest trigger'
),
(
  'uk_aq_sos', 1, 'cloud_run', '*/15 * * * *', 'UTC', 'main',
  'https://deployment-pending.invalid/run', 'POST', '{"trigger_mode":"safety"}', 1,
  'UK-AIR SOS safety ingest trigger'
);
