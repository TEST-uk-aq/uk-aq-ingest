# Node.js October 2026 security remediation — 24 October 2026

Status: proposed TEST repository changes; deployment and real operational acceptance remain outstanding.

The repository change is configuration only. No application source, dependency manifest/lock, secret, variable, schedule, permission, concurrency group, lock, timeout, deletion guard, failure handler or data-writing command is changed. All 60 current workflow files were parsed, and parsed before/after comparisons prove that the only workflow changes are exact Node selections, the 12 new setup steps and the Sensor.Community build flags/digest output. No operational workflow, service, database or cloud mutation was run.

The official October bulletin requires Node 24.22.1 / 22.23.4. Operational workflows now select exact 24.22.1; no Node 22 compatibility exception was needed. Exact setup-node requests cannot silently select an earlier patch: download/cache failure fails the setup step. Setup logs supply the selected version. Advance these pins together after reviewing later supported security releases; they do not automatically follow newer patches.

New setup steps reuse the verified actions/setup-node v7.0.0 commit 820762786026740c76f36085b0efc47a31fe5020 and disable automatic package-manager caching. Existing action references and cache settings are retained. The pinned action and the referenced checkout, Python, artifact, cache, CodeQL, Google and Wrangler action definitions declare node24; Supabase is composite, with pinned nested setup-node/setup-bun. setup-node does not select action-host Node. Actual hosted-runner bundled Node patch and runner identity remain operational evidence requirements. Node-24 actions require a compatible runner (at least 2.327.1 for the reviewed Node-24 migration); current jobs use ubuntu-latest.

Sensor.Community retains the existing four COPY sources, command, work directory, production environment and Dockerfile-specific ignore file. The base changes from node:24-slim to node:24.22.1-slim. A build-time Node assertion requires >=24.22.1 <25 and logs version/platform/architecture before any application code is copied. The deployment build pulls the current base and uses plain progress; after push it prints final RepoDigests. Tags remain mutable for OS rebuilds: retain the base manifest digest from BuildKit and final image digest with the accepted Cloud Run revision. No digest was invented or asserted as deployed.

Official docker-library metadata at nodejs/docker-node commit 80f02a542da739e6a662451d1db66456a0dea253 maps both tags to 24/bookworm-slim, whose Dockerfile sets NODE_VERSION=24.22.1. It supports amd64, arm64v8 and ppc64le; the current ubuntu-latest build uses amd64. No platform override or Debian-family change is introduced. This is supplier source evidence, not a locally built/pulled image observation.

After separate authorization, use the existing uk_aq_scomm_cloud_run_deploy.yml workflow, retain the build gate/version/base/final digests, verify the actual serving revision/traffic, and observe one normal Sensor.Community invocation under existing configuration. Confirm normal claim/source fetch, writes, Pub/Sub/outbox mode, logs, egress reporting and health. Run Daily Stations through its existing TEST workflow and confirm polling recovery and isolated-warning semantics.

Promotion requires the complete Dockerfile, Dockerfile.dockerignore, index.mjs, run_service.mjs, service_egress_metrics.mjs, shared ingestdb_observation_writer.mjs and deployment workflow. LIVE lacks the metrics module and its Docker COPY/ignore rules; TEST index.mjs already includes unrelated egress instrumentation and a pre-existing TEST environment default. Existing deployment configuration also adds metrics secrets/environment bindings. Those differences require their own acceptance and correctly configured environment identity before complete-file promotion; promoting the Dockerfile alone fails when the new COPY source is missing.

Changed configuration files:

| File | Before | After |
|---|---|---|
| `.github/workflows/uk_aq_cloudflare_scheduler_ingest_config_sync.yml` | runner PATH / action host | 24.22.1 |
| `.github/workflows/uk_aq_cloudflare_scheduler_ingest_deploy.yml` | runner PATH / action host | 24.22.1 |
| `.github/workflows/uk_aq_observs_edge_deploy.yml` | runner PATH / action host | 24.22.1 |
| `.github/workflows/uk_aq_stations_daily.yml` | 24 | 24.22.1 |
| `workers/uk_aq_sensorcommunity_cloud_run/Dockerfile` | node:24-slim | node:24.22.1-slim |
| `.github/workflows/uk_aq_scomm_cloud_run_deploy.yml` | runner PATH / action host | runner PATH / action host |

Structural validation: 60 YAML parses; 39 changed workflow semantic comparisons; 38 exact runtime selections in execution order; all four Sensor Docker COPY sources/ignore rules; syntax checks for four Sensor modules and the runtime assertion; the actual development-host 24.19.0 is rejected by that assertion. No application tests, install scripts, Docker build or functional operations were run. No archive is required for these low-risk configuration-only edits; no application code changed.

Rollback: revert this repository commit through a reviewed TEST branch. Do not restore an unsupported runtime into operation. A Cloud Run rollback requires a previously accepted patched immutable image/revision. Reverting workflow source does not undo already performed data operations or migrations. Keep all existing history/backup recovery contracts and activation decisions.

Documentation handover for ChatGPT in Chat mode: record the exact selected runtime floor, image assertion and required digest/runner evidence if current deployment documentation is updated. No authoritative system_docs file is modified and no schema or environment-key change is required.

Sources: https://nodejs.org/en/blog/vulnerability/october-2026-security-releases ; TEST System Docs merged PR #3 and its companion 3,045-row CSV; current pinned GitHub source and upstream action.yml definitions. Current deployment acceptance is not inferred from source versions.
