# UK AQ ingest coding-agent rules

This file is the active repository-level agent instruction set. `AGENTS_BASE.md` is retained as older reference material and is **not** a mandatory/default read. Where it differs, this file and active `system_docs/` contracts take precedence.

## Scope and authority

- This is a UK AQ TEST repository. Do not inspect, modify or propagate changes to LIVE unless the user explicitly asks for LIVE work.
- Keep work bounded to the requested connector/subsystem.
- Before implementation read:
  1. this file;
  2. the `TEST-uk-aq/uk-aq-system-docs` repository in the current multi-repository workspace, starting at `system_docs/SYSTEM_OVERVIEW.md`;
  3. the relevant area `README.md` selected by that router;
  4. only the broad/narrow contracts selected by that router;
  5. the implementation files actually in scope.
- Do not recursively preload all system docs, legacy docs, plans, drafts or archive material.
- Active `system_docs/` contracts are authoritative. Report conflicts with code/user requests rather than silently overriding them.
- Coding agents may read authoritative `system_docs/` in `TEST-uk-aq/uk-aq-system-docs` but MUST NOT edit/move/rename/delete it. If that repository is unavailable in the current workspace, report the missing prerequisite rather than using a copied fallback. Provide a concise Chat-mode documentation handover when implementation changes require contract updates.

## Default operating mode

Default is focused code/schema/non-system-doc implementation only.

Unless permitted by the environment-specific Git rules below or explicitly authorised for that specific operation, do **not**:

- create/amend commits, push, create branches or PRs except as permitted by the environment-specific Git rules below;
- execute SQL or apply migrations against TEST/LIVE databases;
- deploy Supabase functions, Cloud Run, Workers or workflows;
- run backfills, reconciliations, bulk/long-running jobs or destructive data operations;
- change GCP, Supabase, Cloudflare, R2, Dropbox or GitHub settings;
- run broad external-API fetches or repeatedly inspect cloud logs.

When external work is required but not authorised, make repository changes only and provide exact manual commands, expected output, rollback notes and real TEST validation steps.

## Git and pull-request authorisation

The permitted Git workflow depends on where the agent is running.

### Local/editor agents

For agents operating in a local checkout, including VS Code Codex:

- A request to implement, fix, change or update code authorises the required bounded TEST working-tree edits without further confirmation.
- Leave implementation changes uncommitted in the local working tree for the user to inspect.
- Do not create or amend commits.
- Do not push any local commit or branch to GitHub.
- Do not create a pull request from the local checkout.
- Do not push directly to `main`.
- Fetching from remotes, inspecting remote branches and checking out an existing branch are permitted when needed for the requested task, provided unrelated local work is not overwritten or discarded.
- If the user explicitly requests a particular local Git operation in the current task, perform only that named operation. A request to implement code alone never authorises commit or push.

### Codex Cloud agents

For agents operating in Codex Cloud:

- A request to implement, fix, change or update TEST code authorises the bounded cloud working-tree edits needed for that task without further confirmation.
- For completed implementation work, create a working branch, commit the intended changes, push that branch and create a pull request targeting the TEST repository's `main` branch.
- If the task is already associated with an open pull request, update that existing PR branch instead of creating another PR.
- No separate post-implementation confirmation is required to create or update the PR.
- Never push directly to `main`.
- Never merge the pull request.
- Never deploy as a consequence of creating or updating the pull request.
- Stop after the PR has been created or updated and report the PR, branch and commit details.

### ChatGPT in Chat mode

For ChatGPT operating in Chat mode with repository/GitHub tools:

- A request to implement, fix, change or update TEST code, configuration or documentation authorises the bounded repository edits and commits needed for that task without further confirmation.
- For small, bounded, low-risk TEST changes, commit and push directly to the repository's `main` branch by default. No separate confirmation is required.
- Do not create a pull request by default. Create a PR only when the user explicitly asks for one in the current task.
- For larger, substantial, high-risk or broad cross-repository changes, do not write directly to `main` by default. Use a non-`main` branch, commit and push the changes there, then stop and report the branch unless the user explicitly authorises another Git action.
- If it is genuinely unclear whether a change is small and low-risk enough for direct `main`, prefer the non-`main` branch path.
- Never merge a pull request unless the user explicitly asks for the merge in the current task.
- Never deploy or perform remote operational/data mutations merely because repository changes were requested.

### Common restrictions

Local/editor agents and Codex Cloud agents must not push directly to `main` unless the user explicitly authorises that specific action in the current task.

ChatGPT in Chat mode may push small, bounded, low-risk TEST changes directly to `main` under the standing rule above. Direct-`main` writes for larger, substantial, high-risk or broad cross-repository changes still require explicit current-task authorisation.

No agent may merge a PR, deploy, modify LIVE, or perform remote operational/data mutations unless the user explicitly authorises that specific action in the current task.

The user may explicitly narrow the permitted Git behaviour for the current task; a narrower current-task instruction takes precedence over these defaults.

Authorisation from an earlier task does not carry forward for exceptional operations such as larger direct-`main` writes, merging, deployment, LIVE work or remote operational mutation.

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
