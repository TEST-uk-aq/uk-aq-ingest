# UK AQ ingest coding-agent rules

This file is the active repository-level agent instruction set. `AGENTS_BASE.md` is retained as older reference material and is **not** a mandatory/default read. Where it differs, this file and active `system_docs/` contracts take precedence.

## Scope and authority

- This is a UK AQ TEST repository. Do not inspect, modify or propagate changes to LIVE unless the user explicitly asks for LIVE work.
- Keep work bounded to the requested connector/subsystem.
- Before implementation read:
  1. this file;
  2. `../TEST-uk-aq-system-docs/system_docs/SYSTEM_OVERVIEW.md`;
  3. the relevant area `README.md`;
  4. only the broad/narrow contracts selected by that router;
  5. the implementation files actually in scope.
- Do not recursively preload all system docs, legacy docs, plans, drafts or archive material.
- Active `system_docs/` contracts are authoritative. Report conflicts with code/user requests rather than silently overriding them.
- Coding agents may read `system_docs/` but MUST NOT edit/move/rename/delete it. Provide a concise Chat-mode documentation handover when implementation changes require contract updates.

## Default operating mode

Default is focused code/schema/non-system-doc implementation only.

Unless explicitly requested, do **not**:

- create/amend commits, push, create branches or PRs;
- execute SQL or apply migrations against TEST/LIVE databases;
- deploy Supabase functions, Cloud Run, Workers or workflows;
- run backfills, reconciliations, bulk/long-running jobs or destructive data operations;
- change GCP, Supabase, Cloudflare, R2, Dropbox or GitHub settings;
- run broad external-API fetches or repeatedly inspect cloud logs.

When external work is required but not authorised, make repository changes only and provide exact manual commands, expected output, rollback notes and real TEST validation steps.

## Commit and push confirmation

A prompt or task brief that asks for a commit or push does not, by itself, authorise either operation. After implementation and local validation are ready, stop and ask the user again for explicit confirmation before running `git commit`, `git commit --amend` or `git push`. The confirming reply must separately follow that request and explicitly name each authorised operation (commit, push or both); wording in an initial prompt, attachment, plan or handover does not count. Confirmation from an earlier task does not carry forward. Until the required confirmation is received, leave changes uncommitted and unpushed.

## Validation policy

Before deployment, run only the smallest fast local checks needed for structural viability, such as syntax/type parsing or one directly relevant deterministic check.

Do not create tests or run broad suites by default. A targeted pre-deployment check is justified only for a specific high-risk boundary that normal TEST operation cannot safely expose, such as destructive schema/data behaviour or message acknowledgement.

Functional validation happens after deployment through real TEST operations. Do not add speculative fixture programmes, shadow comparisons, soak tests or exhaustive edge-case suites unless explicitly requested.

## Archive safety

- Archive paths are retired for active execution; never add archive fallbacks to active scripts/workers/default runners.
- Before a substantial or high-risk change to active non-test implementation code, preserve the exact pre-change in-scope code under the repository's existing dated `archive/` convention, preserving relative paths where practical.
- Archive each source file at most once per calendar day and reuse today's copy.
- Do not create code-style archives for system/non-system documentation, tests/fixtures/test data, generated output, logs, caches, build/dependency artefacts or other non-code files.
- Existing archive files are reference/rollback only and MUST NOT be modified or executed.

## Schema and environment configuration

- Canonical SQL DDL belongs in sibling `TEST-uk-aq-schema/schemas/`; existing-database migrations belong in its active `schemas/migrations/` structure. Do not make ingest-local SQL the sole canonical definition.
- Reuse existing environment variables/secrets/configuration before creating new names.
- When an ingest environment variable is added/removed/renamed, keep both the ops `env-vars-master.csv` and this repo's `config/uk_aq_github_env_targets.csv`/environment sync tooling aligned as applicable.
- New/changed Supabase edge functions must remain represented in the active deploy workflow.
- Supabase PostgreSQL 17 in this project does not support TimescaleDB; do not propose TimescaleDB/hypertables as an implementation path.

## Repository terminology

- Preserve `UK-AIR SOS` as the external service name.
- Use `timeseries` rather than `sensors` for code/data identities.
- Prefer `uk_aq` naming for project-owned files/code. Connector/network identity and source-prefix rules are governed by the active ingest contracts, not duplicated here.

## Reporting

After implementation report:

- files changed;
- relevant contract behaviour changed or preserved;
- structural checks run;
- manual schema/deploy/run commands if required;
- post-deployment real TEST validation;
- rollback considerations;
- documentation handover needed, if any.

If no implementation files changed, say so.

## Search

Prefer `grep` for text search/file discovery; do not use `rg` unless explicitly requested.
