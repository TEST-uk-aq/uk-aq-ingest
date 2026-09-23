# UK AQ system overview

## Purpose

This file is the normal top-level entry point for UK AQ system documentation.

It is a concise system map and contract router. It does not replace the authoritative behavioural contracts inside each active `system_docs/` area.

For documentation authority and maintenance rules, see [`documentation_contract.md`](documentation_contract.md).

## System shape

At a high level, UK AQ consists of:

1. source connectors and reference discovery;
2. operational ingestion and current-state data;
3. latest-snapshot products;
4. pruning and historical publication to R2;
5. historical integrity, backup and recovery;
6. derived AQI and WHO products;
7. R2/API/cache delivery;
8. public website and administrative dashboards;
9. shared schema, geography, monitoring, local persistence and scheduling infrastructure.

A normal coding task should enter through the relevant area below rather than reading unrelated contracts.

## Contract router

| Task or subsystem | Start here | Status |
|---|---|---|
| Connector ingest, Daily Stations discovery, Breathe London reference discovery, UK-AIR SOS polling/fallback | [`ingest/README.md`](ingest/README.md) | Active current runtime; also routes explicitly labelled future high-frequency canonicalisation contracts |
| Latest snapshot builder and current snapshot/API boundary | [`latest_snapshot/README.md`](latest_snapshot/README.md) | Active |
| Canonical observation history, timeseries continuity, history indexes and current History Integrity repair | [`r2_history/README.md`](r2_history/README.md) | Active |
| Future dedicated Integrity Factory architecture | [`integrity_factory/README.md`](integrity_factory/README.md) | Future implementation authority; not current Integrity runtime |
| UK AQ Media / AQ in the News discovery, editorial and public-feed subsystem | [`media/README.md`](media/README.md) | Active standalone runtime; also routes future homepage latest-six generation/cache authority; public website cut-over not yet done |
| Calculated hourly DAQI / European AQI and station-history AQI behaviour | [`aqi-levels/README.md`](aqi-levels/README.md) | Active |
| WHO 2021 daily, rolling-year and calendar-year products | [`who_2021/README.md`](who_2021/README.md) | Active |
| Shared station-chart browser architecture and rendering | [`station_charts/README.md`](station_charts/README.md) | Active |
| Public website shell, navigation, footer attribution and responsive presentation | [`website_ui/README.md`](website_ui/README.md) | Active; homepage Media contract also carries explicitly labelled future generation-refresh authority |
| Public website GitHub Pages build and browser asset identity | [`website_deployment/README.md`](website_deployment/README.md) | Active |
| Cache proxy behaviour, including WHO summary and public network metadata routes | [`cache_proxy/README.md`](cache_proxy/README.md) | Partly active; broader migration pending |
| Administrative dashboards | [`dashboards/README.md`](dashboards/README.md) | Active |
| Operator-run scripts: per-invocation work directories, 15-second progress, logs and structured run reports | [`operator_execution/README.md`](operator_execution/README.md) | Future implementation authority; not yet current runtime |
| Geography and postcode products | [`geography/README.md`](geography/README.md) | Active |
| MacBook Pro local MySQL persistence, naming and TEST/LIVE isolation | [`local_mysql/README.md`](local_mysql/README.md) | Active |
| GCP Cloud Billing export, Pro MySQL reporting and Dropbox archive | [`gcp_billing/README.md`](gcp_billing/README.md) | Future implementation authority; not yet deployed |
| TEST GCP Cloud Logging analysis archive to Dropbox | [`gcp_logging_archive/README.md`](gcp_logging_archive/README.md) | Future implementation authority; collector prepared but not yet deployed/accepted |
| Monitoring and Supabase egress attribution | [`monitoring/README.md`](monitoring/README.md) | Partly active; broader migration pending |
| Backup, restore and R2 history Dropbox backup | [`backup_and_recovery/README.md`](backup_and_recovery/README.md) | Partly active; broader migration pending |
| Prune daily and retention outside completed R2-history contracts | `prune_and_retention/` | Area migration pending |
| Observs outbox and partition maintenance | `observs_operations/` | Area migration pending |
| Public/private APIs outside completed history/AQI boundaries | `api_services/` | Area migration pending |
| Cloudflare and GCP scheduling | `scheduling/` | Area migration pending |
| Shared runtime components and cross-area invariants | `shared/` | Area migration pending |

A pending area name is a proposed documentation boundary only. It does not override any existing active contract that currently owns the behaviour.

A route labelled **future implementation authority** constrains that future implementation but is not evidence of deployed runtime behaviour and does not override the current-runtime route it is intended eventually to replace or complement.

## Cross-area work

Some tasks legitimately span more than one area. Do not read every area pre-emptively.

Start with the primary area, then use [`READING_GUIDE.md`](READING_GUIDE.md) when the task crosses a known boundary such as:

- R2 history plus station charts;
- website deployment plus website UI;
- database/schema promotion plus runtime code;
- WHO calculation plus cache proxy;
- monitoring plus dashboards;
- R2 history plus backup/recovery;
- operator execution plus the primary subsystem whose command is being changed.

## Source-of-truth rules

- Human-readable Markdown under active `system_docs/` areas is the authoritative prose specification.
- Plans, `system_docs/drafts/`, archives and `system_docs_legacy/` are non-authoritative unless an active contract explicitly incorporates a decision from them.
- Current-runtime contracts define deployed required behaviour. Explicitly labelled future implementation contracts constrain future implementation without superseding current runtime by implication.
- A detailed behavioural rule must have one authoritative home. Overview/router files may summarise and link but must not create a second editable version of the rule.
- If code, a user request and an active contract disagree, report the conflict rather than silently choosing or weakening one source.
- Codex and other coding agents are read-only consumers of `system_docs/`. ChatGPT in Chat mode owns system-documentation updates.

## Normal coding-agent reading order

For a bounded task:

1. read the repository `AGENTS.md`;
2. read this `SYSTEM_OVERVIEW.md`;
3. read the relevant area's `README.md`;
4. read the broad or narrow contract selected by that README for the task;
5. read only additional interfaces, operations, recovery, validation or decision documents that the selected route requires;
6. inspect the implementation files in scope.

Do not recursively read all of `system_docs/` or broadly inventory unrelated repository areas unless the bounded investigation shows that additional context is necessary.

## Documentation classes

For the full definitions, see [`documentation_contract.md`](documentation_contract.md). In short:

- current authoritative contracts define deployed required behaviour;
- explicitly labelled future implementation contracts constrain agreed future implementation without superseding current runtime by implication;
- runbooks define procedures and do not redefine behaviour;
- plans define proposed work and are not current behaviour;
- drafts are non-authoritative until explicitly promoted;
- archive and legacy material is historical only.

## Archive distinction

Two different archive concepts exist and must not be confused:

1. **documentation legacy/archive** preserves superseded documentation and historical decisions, but never acts as current authority;
2. **pre-change code archive snapshots** are an agent operating rule for substantial or high-risk changes to active non-test implementation code.

Documentation itself, including `system_docs/`, must never receive code-style pre-change archive snapshots.

The detailed coding-agent archive execution and pre-change archive rules belong in repository `AGENTS.md` because they govern what an agent may execute or modify before task-specific contracts are read.
