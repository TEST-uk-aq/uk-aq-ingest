# System documentation maintenance contract

## Purpose

This document defines how authoritative UK AQ system documentation is structured, interpreted and maintained.

Its purpose is to prevent implementation drift, accidental behaviour changes, conflicting sources of truth and unnecessary coding-agent context loading.

## Source of truth

Human-readable Markdown under `system_docs/` is the authoritative prose specification.

`system_docs/` is the sole active system-documentation root. A top-level `docs/` directory MUST NOT be recreated. Historical reports and superseded broad documents may be retained under `system_docs_legacy/` or the applicable dated archive, but they MUST carry clear historical status and MUST NOT override an active area contract.

Coding agents MUST NOT create or rely on a separate hidden, compressed or non-human-readable behavioural specification.

Machine-readable schemas, fixtures and deterministic checks may accompany the prose contract, but they MUST implement the same rules and MUST NOT introduce a second behavioural authority.

## Context routing hierarchy

The normal documentation entry point is [`SYSTEM_OVERVIEW.md`](SYSTEM_OVERVIEW.md).

That file is a concise system map and contract router. It MUST NOT become a duplicate detailed behavioural contract.

For a bounded coding task, the normal reading hierarchy is:

1. repository `AGENTS.md` for operating and safety rules;
2. `system_docs/SYSTEM_OVERVIEW.md` for the system map and primary-area route;
3. the primary area's `README.md` for area scope and task-specific reading order;
4. the area's broad `contract.md`;
5. only the narrower contracts, interfaces, operations, recovery, validation or decisions required by the task;
6. implementation files.

[`READING_GUIDE.md`](READING_GUIDE.md) provides known cross-area reading sets. It SHOULD be used when a task crosses one of those boundaries and SHOULD NOT be loaded in full for unrelated bounded tasks.

Coding agents SHOULD NOT recursively read all of `system_docs/`, broadly inventory unrelated areas or load every cross-area contract before the bounded investigation shows that the additional context is needed.

An area README SHOULD remain a router and orientation document. Detailed required behaviour SHOULD live in the area's authoritative contract or a clearly named narrower contract.

## Behavioural language

Area contracts use the following meanings:

- **MUST**: required behaviour.
- **MUST NOT**: prohibited behaviour.
- **SHOULD**: expected behaviour unless a documented reason justifies an exception.
- **MAY**: optional behaviour that does not alter the required contract.

Examples and diagrams explain the contract but do not override an explicit MUST or MUST NOT statement.

## Documentation classes

### Current authoritative system contract

Located in an active area under `system_docs/`.

Defines current required behaviour, interfaces, state transitions and invariants.

### Future implementation contract

May be located in an active area when an agreed future architecture or behavioural boundary must constrain upcoming implementation before that subsystem or replacement runtime is deployed.

A future implementation contract MUST be explicitly labelled as future/not-yet-current in both the contract and its area router. Where the area appears in `SYSTEM_OVERVIEW.md`, its status MUST also distinguish future design authority from current runtime authority.

A future implementation contract is authoritative for the future implementation it describes, but it MUST NOT be treated as evidence that the subsystem is already deployed and MUST NOT silently override an existing current-runtime contract. Replacing or transferring current behaviour requires the necessary detailed contracts plus an explicit implementation/cut-over decision.

Mechanisms deliberately left open by a future implementation contract MUST NOT be inferred and implemented as though they were already specified.

### Operational runbook

Defines commands and procedures for deployment, repair, recovery or inspection.

A runbook MUST link to the contract it operates and MUST NOT redefine the system's behaviour.

### Implementation plan

Defines proposed future work. It is not current behaviour until the relevant contract is updated and the implementation is accepted.

### Architecture Decision Record

Explains why a load-bearing decision was made, what alternatives were rejected and what consequences follow.

The area contract defines what the system does. The decision record explains why.

### Archive or legacy record

Preserves retired files, point-in-time reports and historical decisions. Archive and `system_docs_legacy/` paths MUST NOT be used as active runtime or authoritative documentation paths.

### Design draft

Files under `system_docs/drafts/` are non-authoritative proposed designs. They MUST NOT constrain current implementation unless the user explicitly asks to review, promote or implement a named draft.

Promotion requires the appropriate active contract to be created or updated. A draft does not become authoritative merely because implementation work begins.

## Required area structure

Each substantial system area SHOULD contain only the files needed to express its contract clearly. Common files are:

- `README.md` for small orientation, ownership and reading order;
- `contract.md` for broad required behaviour;
- narrower `*-contract.md` files where one component has distinct authoritative behaviour;
- `data_flow.md` where component/data boundaries need explanation;
- `interfaces.md` for external and cross-component interfaces;
- `operations.md` for routine operation;
- `validation.md` for structural and TEST operational validation;
- `decisions/` for Architecture Decision Records.

Add `state_model.md` when the area owns persistent or load-bearing transient state.

Add `recovery.md` when recovery is complex enough that combining it with operations would obscure the normal runtime contract.

Do not create empty files merely to satisfy this shape. A file should exist only when it has a clear, non-overlapping responsibility.

Do not split one coherent component into many tiny contract fragments merely to reduce individual file size. A split is justified when it lets a normal task avoid a substantial unrelated component without forcing the reader to reconstruct one concept from many fragments.

## No duplicate authority

A behavioural rule MUST have one authoritative home.

Other documents may summarise or link to it, but they MUST identify the authoritative source and MUST NOT copy a second editable version of the rule.

Overview files and routing tables MUST favour links over repeated behavioural detail.

When an existing broad document is replaced:

1. preserve it in the required legacy or dated archive location when applicable;
2. move its still-current content into the appropriate area files;
3. remove the original active document, or use a short redirect only where an active external reference genuinely requires it;
4. record the migration in the area `README.md` or repository documentation index.

Documentation refactors MUST NOT use the coding-agent pre-change code archive mechanism. Documentation history is preserved through Git and the documentation legacy/archive rules above.

## Coding-agent change protocol

Before making implementation changes, an agent MUST report:

- requested behaviour being changed;
- behaviour explicitly required to remain unchanged;
- authoritative documents read;
- implementation files in scope;
- files and systems explicitly out of scope;
- any conflict between code and documentation.

During implementation, an agent MUST NOT:

- perform unrelated refactors;
- rename public fields, routes, object keys or environment variables unless explicitly required;
- change scheduling, retry, caching, error or fallback behaviour merely to simplify code;
- broaden the task to adjacent services without reporting the need first;
- use archived code as a runtime fallback.

After implementation, the change report MUST include:

- files changed;
- contract sections changed, or a statement that the contract was preserved;
- deterministic checks run;
- manual deployment or apply steps;
- post-deployment TEST validation;
- rollback considerations.

Codex and other coding agents are read-only consumers of `system_docs/`. They MUST provide a handover when implementation requires an authoritative documentation update. ChatGPT in Chat mode owns the corresponding `system_docs/` change.

## AGENTS.md boundary

Repository `AGENTS.md` files define operating rules that must apply before task-specific documentation is selected.

They SHOULD remain concise and MAY include or link the load-bearing rules needed before implementation, including:

- TEST/LIVE scope;
- permissions for deployment, SQL, backfills and external operations;
- the archive execution rule;
- the pre-change archive requirement for substantial or high-risk active non-test implementation code;
- the prohibition on code-style pre-change archive copies for documentation, tests and other excluded files;
- repository-specific safety rules that cannot safely wait for task-specific contract discovery.

`AGENTS.md` MUST NOT become a second system architecture or behavioural contract. Detailed component behaviour belongs in active `system_docs/` contracts.

## Contract update triggers

Update an area contract when any of the following changes intentionally:

- value eligibility or filtering;
- state identity or transition rules;
- API, message or object schema;
- field meaning;
- scheduling or ordering that affects observable behaviour;
- fallback or fail-open/fail-closed behaviour;
- retention, deletion or backup safety gates;
- source-of-truth ownership;
- cache key or cache validation contract;
- recovery semantics.

A code reorganisation that demonstrably preserves all of those does not require a contract rewrite.

## Validation policy

Pre-deployment validation should establish only that the implementation and configuration are structurally viable, plus any small deterministic contract check genuinely needed to prevent the known class of regression.

Functional validation is performed after deployment through real operation on the TEST system.

Broad speculative pre-implementation test programmes are not required.

For a documentation-only routing refactor, validation is structural: links, authority ownership and migration completeness. It does not require runtime testing.

## Review checklist

A reviewer should be able to answer:

1. What behaviour is intentionally changing?
2. Which contract authorises that change?
3. What behaviour is protected from change?
4. Are public interfaces byte- and field-compatible where required?
5. Are raw records, derived products and current-state records still clearly separated?
6. Does recovery rebuild the same state defined by the normal runtime contract?
7. Were the relevant system documents updated without creating duplicate authority?
8. Could a bounded coding task identify its authoritative reading set without loading unrelated system documentation?
