# UK AQ system-doc reference mirror

## Purpose

This directory is a read-only reference mirror of selected authoritative files from:

`TEST-uk-aq/uk-aq-system-docs/system_docs/`

It exists so single-repository Codex Cloud work in `TEST-uk-aq/uk-aq-ingest` can read the contracts required for bounded ingest tasks when the sibling system-doc repository is not present in the workspace.

The authoritative source remains `TEST-uk-aq/uk-aq-system-docs`. This mirror is not a second editable contract authority.

## Snapshot identity

Source repository:

`TEST-uk-aq/uk-aq-system-docs`

Source commit:

`773cc13e5f40bb8b2eb39ad7016e5bc2480793c7`

Snapshot date:

`24/09/2026`

## Reading rule

When `../TEST-uk-aq-system-docs/system_docs/` exists, coding agents should use that sibling authoritative tree.

When it is unavailable, use this mirror for the bounded task. Start at:

`ref_docs/system_docs/SYSTEM_OVERVIEW.md`

then follow the mirrored ingest route.

If a routed contract is not present in this mirror, do not infer its contents. Report that the authoritative sibling file is unavailable.

## Mirrored scope

This snapshot intentionally contains the contracts needed for the current TEST Daily Stations and Breathe London reference-resilience implementation work:

- system overview and documentation authority rules;
- ingest area routing and Daily Stations cross-connector orchestration;
- Breathe London Nodes deterministic reference discovery;
- future Breathe London Communities deterministic reference discovery;
- future shared Breathe London missing-reference self-repair and isolated station-failure behaviour;
- station/timeseries metadata ownership;
- the isolated UK-AIR SOS Daily Stations failure/warning contract.

It is intentionally not a complete copy of all UK AQ system documentation.

## Maintenance

Coding agents must not edit files under this mirror as a substitute for changing the authoritative system docs.

ChatGPT in Chat mode owns authoritative system-doc changes. When one of the mirrored source contracts changes, refresh the corresponding mirror file and update the source commit above in the same documentation task.
