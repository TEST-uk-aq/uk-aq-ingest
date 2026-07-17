# Agent Notes

## Required repository rules

The existing repository-specific rules are stored in [`AGENTS_BASE.md`](AGENTS_BASE.md) and are incorporated into this `AGENTS.md`.

Coding agents must read and follow `AGENTS_BASE.md` before making changes in this repository. The TEST validation policy below takes precedence where older wording could be interpreted as requiring broader testing.

## TEST System Validation Policy

- This repository is part of the UK AQ TEST system. It is intended for development and real operational testing before changes are transferred to LIVE.
- Perform as little pre-deployment testing as reasonably possible.
- Before deployment, run only the smallest fast local check needed to establish that changed code or configuration is structurally viable, such as syntax, type checking, parsing or one directly relevant existing check.
- Do not create new automated tests by default.
- Add a targeted test only when it is genuinely needed to protect against a specific high-risk regression that would be difficult to detect through normal TEST operation.
- Do not run broad test suites, exhaustive edge-case testing, large fixture programmes, shadow comparisons, soak tests or extended validation unless the user explicitly requests them.
- Functional testing should normally happen after deployment through real operation on the TEST system.
- For a reversible change, one successful normal operation and one representative output check are generally sufficient.
- Data deletion, schema safety, message acknowledgement and irreversible operations may require one narrowly targeted check before execution.
- Do not expand the task solely to improve test coverage.
