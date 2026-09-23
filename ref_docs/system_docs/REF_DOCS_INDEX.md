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

`bc14d60ddee0996a2765d3f84ba69ed9025fa94f`

Snapshot date:

`23/09/2026`

## Reading rule

When `../TEST-uk-aq-system-docs/system_docs/` exists, coding agents should use that sibling authoritative tree.

When it is unavailable, use this mirror for the bounded task. Start at:

`ref_docs/system_docs/SYSTEM_OVERVIEW.md`

then follow the mirrored ingest route.

If a routed contract is not present in this mirror, do not infer its contents. Report that the authoritative sibling file is unavailable.

## Mirrored scope

This snapshot intentionally contains the contracts needed for the current TEST Daily Stations SOS warning work:

- system overview and documentation authority rules;
- ingest area routing;
- Daily Stations cross-connector orchestration;
- the isolated UK-AIR SOS Daily Stations failure/warning contract.

It is intentionally not a complete copy of all UK AQ system documentation.

## Maintenance

Coding agents must not edit files under this mirror as a substitute for changing the authoritative system docs.

ChatGPT in Chat mode owns authoritative system-doc changes. When one of the mirrored source contracts changes, refresh the corresponding mirror file and update the source commit above in the same documentation task.
