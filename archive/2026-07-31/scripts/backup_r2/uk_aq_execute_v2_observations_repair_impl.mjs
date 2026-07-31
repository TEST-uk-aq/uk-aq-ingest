#!/usr/bin/env node
// Ordered metadata executor for v2 observations and AQI data.
// It never rewrites parquet data or invokes either data backfill wrapper.
import fs from "node:fs";
import { pathToFileURL } from "node:url";
import {
  parquetMetadataAsync,
  parquetRead,
  parquetSchema,
  compressors,
} from "./lib/uk_aq_parquet_dependencies.mjs";
import { sha256Hex } from "../../workers/shared/r2_sigv4.mjs";
import {
  computeObservationContentHash,
  normalizeCanonicalObservationRow,
  resolveLegacyVerificationStatus,
} from "../../workers/shared/uk_aq_observation_content_hash.mjs";
import {
  buildHistoryV2PollutantManifest,
  buildHistoryV2ConnectorManifest,
  buildHistoryV2DayManifest,
} from "../../workers/shared/uk_aq_r2_history_canonical.mjs";
import {
  combineObservationHistoryPhysicalSchemas,
  observationHistoryPhysicalSchemaForColumns,
} from "../../workers/shared/uk_aq_observation_history_schema.mjs";
import {
  resolveR2HistoryIndexConfig,
  updateR2HistoryIndexesTargeted,
} from "../../workers/shared/uk_aq_r2_history_index.mjs";
import {
  assertV2ObservationsChildManifest,
  classifyRepairableV2ObservationsConnectorManifest,
} from "./lib/uk_aq_v2_observations_manifest_validation.mjs";

const SUPPORTED_ACTIONS = new Set([
  "observation_pollutant_manifest_repair",
  "observation_connector_manifest_repair",
  "observation_day_manifest_repair",
  "observation_index_repair",
  "rebuild_v2_observations_index_only",
  "aqi_pollutant_manifest_repair",
  "aqi_connector_manifest_repair",
  "aqi_day_manifest_repair",
  "aqi_index_repair",
  "rebuild_v2_aqi_index_only",
]);

// Scope is part of the repair-action contract.  In particular, a day
// manifest is above the connector hierarchy: giving it a connector ID would
// rebuild a parent from a subset of its children.  Keep those actions as a
// distinct, connector-less scope so the day builder reads every final child
// manifest from the combined overlay/Dropbox view.
const ACTION_SCOPE_RULES = {
  observation_pollutant_manifest_repair: { connector: "required", pollutant: "required", needsConnector: true, needsDay: true, pollutantRepair: true },
  observation_connector_manifest_repair: { connector: "required", needsConnector: true, needsDay: true },
  observation_day_manifest_repair: { connector: "absent", needsDay: true },
  observation_index_repair: { connector: "required", pollutant: "required" },
  rebuild_v2_observations_index_only: { connector: "required" },
  aqi_pollutant_manifest_repair: { connector: "required", pollutant: "required", needsConnector: true, needsDay: true, pollutantRepair: true },
  aqi_connector_manifest_repair: { connector: "required", needsConnector: true, needsDay: true },
  aqi_day_manifest_repair: { connector: "absent", needsDay: true },
  aqi_index_repair: { connector: "required", pollutant: "required" },
  rebuild_v2_aqi_index_only: { connector: "required" },
};

function parseArgs(argv) {
  const args = { repairPlanJson: null, repairPlanStdin: false, writeR2: false, overlayRoot: null, dropboxRoot: null, runStateJson: null };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--repair-plan-json") args.repairPlanJson = String(argv[++index] || "");
    else if (arg === "--repair-plan-stdin") args.repairPlanStdin = true;
    else if (arg === "--write-r2") args.writeR2 = true;
    else if (arg === "--dry-run") args.writeR2 = false;
    else if (arg === "--overlay-root") args.overlayRoot = String(argv[++index] || "");
    else if (arg === "--dropbox-root") args.dropboxRoot = String(argv[++index] || "");
    else if (arg === "--run-state-json") args.runStateJson = String(argv[++index] || "");
    else throw new Error(`Unknown arg: ${arg}`);
  }
  if (!args.repairPlanJson && !args.repairPlanStdin) throw new Error("--repair-plan-json or --repair-plan-stdin is required");
  if (args.repairPlanJson && args.repairPlanStdin) throw new Error("pass only one repair plan input");
  if (!args.overlayRoot || !args.dropboxRoot || !args.runStateJson) throw new Error("--overlay-root, --dropbox-root and --run-state-json are required for the combined local resolver");
  return args;
}

function jsonObject(object, key) {
  try {
    return JSON.parse(object.body.toString("utf8"));
  } catch (error) {
    throw new Error(`Invalid JSON in ${key}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

export async function readChildren({
  store,
  prefix,
  dayUtc,
  connectorId,
  kind,
  domain = "observations",
  identityOnlyKeys = new Set(),
  allowEmpty = false,
}) {
  const entries = await store.listAllObjects({ prefix });
  const keyPattern = kind === "connector"
    ? /\/connector_id=\d+\/manifest\.json$/
    : /\/pollutant_code=[^/]+\/manifest\.json$/;
  const keys = entries.map((entry) => entry.key).filter((key) => keyPattern.test(key)).sort();
  if (!keys.length && identityOnlyKeys.size === 0 && !allowEmpty) {
    throw new Error(`Blocked dependency: no ${kind} manifests under ${prefix}`);
  }
  const children = [];
  const identities = new Map();
  for (const key of keys) {
    // readChildren is used with the staged combined-local adapter during
    // planning. Its identities are diagnostic provenance, not live guards.
    const object = await store.getObject({ key });
    identities.set(key, {
      content_sha256: object.content_sha256 || sha256Hex(object.body),
      r2_etag: object.r2_etag || object.etag || null,
      source: object.source || "combined_local",
      last_modified: object.last_modified || null,
    });
    // Callers can request identity-only treatment for a child whose canonical
    // staged replacement is already the dependency body.
    if (identityOnlyKeys.has(key)) continue;
    const payload = jsonObject(object, key);
    if (domain === "observations") {
      assertV2ObservationsChildManifest(payload, { key, kind, dayUtc, connectorId });
    } else if (payload?.domain !== "aqilevels" || payload?.manifest_kind !== kind) {
      throw new Error(`Invalid AQI ${kind} manifest: ${key}`);
    }
    children.push(payload);
  }
  return { children, identities };
}

async function stageRepairableObservationConnectorDependencies({
  staged,
  base,
  dayUtc,
  proposalKeys,
}) {
  const entries = await staged.stagedR2.adapter.listAllObjects({
    prefix: `${base}/connector_id=`,
  });
  const connectorKeys = entries
    .map((entry) => entry.key)
    .filter((key) => /\/connector_id=\d+\/manifest\.json$/.test(key))
    .sort();

  for (const key of connectorKeys) {
    if (staged.proposals.has(key)) continue;
    const match = key.match(/\/connector_id=(\d+)\/manifest\.json$/);
    const connectorId = Number(match?.[1]);
    if (!Number.isInteger(connectorId) || connectorId <= 0) {
      throw new Error(`Blocked dependency: invalid connector manifest key ${key}`);
    }
    const existing = await staged.stagedR2.adapter.getObject({ key });
    const existingPayload = jsonObject(existing, key);
    try {
      assertV2ObservationsChildManifest(existingPayload, {
        key,
        kind: "connector",
        dayUtc,
        connectorId,
      });
      continue;
    } catch (error) {
      const classification = classifyRepairableV2ObservationsConnectorManifest(
        existingPayload,
        { key, dayUtc, connectorId },
      );
      if (!classification.repairable) throw error;

      const child = await readChildren({
        store: staged.stagedR2.adapter,
        prefix: `${base}/connector_id=${connectorId}/pollutant_code=`,
        dayUtc,
        connectorId,
        kind: "pollutant",
        domain: "observations",
        allowEmpty: true,
      });
      const connectorPrefix = `${base}/connector_id=${connectorId}/`;
      const discoveredChildKeys = new Set(child.children
        .map((payload) => String(payload?.manifest_key || ""))
        .filter(Boolean));
      const declaredChildKeys = new Set([
        ...(Array.isArray(existingPayload.child_manifests) ? existingPayload.child_manifests : []),
        ...(Array.isArray(existingPayload.pollutant_manifests) ? existingPayload.pollutant_manifests : []),
      ].map((entry) => String(entry?.manifest_key || "")).filter(Boolean));
      const missingDeclaredChildKeys = [...declaredChildKeys]
        .filter((childKey) => !discoveredChildKeys.has(childKey))
        .sort();
      const discoveredParquetKeys = new Set(child.children.flatMap((payload) =>
        Array.isArray(payload?.parquet_object_keys) ? payload.parquet_object_keys.map(String) : []));
      const baselineParquetKeys = entries
        .map((entry) => entry.key)
        .filter((objectKey) => objectKey.startsWith(connectorPrefix) && objectKey.endsWith(".parquet"))
        .sort();
      const unrepresentedBaselineParquetKeys = baselineParquetKeys
        .filter((objectKey) => !discoveredParquetKeys.has(objectKey));
      if (missingDeclaredChildKeys.length || unrepresentedBaselineParquetKeys.length) {
        throw new Error(
          `Blocked dependency: legacy connector manifest ${key} cannot preserve baseline objects; `
          + `missing_declared_children=${missingDeclaredChildKeys.join(",") || "none"}; `
          + `unrepresented_parquet=${unrepresentedBaselineParquetKeys.join(",") || "none"}`,
        );
      }

      const emptyConnectorIsProven = child.children.length === 0
        && Number(existingPayload.source_row_count) === 0
        && Number(existingPayload.row_count) === 0
        && Number(existingPayload.file_count) === 0
        && Number(existingPayload.total_bytes) === 0
        && Array.isArray(existingPayload.files)
        && existingPayload.files.length === 0;
      if (!child.children.length && !emptyConnectorIsProven) {
        throw new Error(
          `Blocked dependency: connector manifest ${key} is legacy but canonical pollutant children are unavailable`,
        );
      }

      const childBackups = child.children
        .map((payload) => payload.backed_up_at_utc)
        .filter((value) => typeof value === "string" && !Number.isNaN(Date.parse(value)))
        .sort();
      const existingBackup = typeof existingPayload.backed_up_at_utc === "string"
          && !Number.isNaN(Date.parse(existingPayload.backed_up_at_utc))
        ? existingPayload.backed_up_at_utc
        : null;
      const runId = typeof existingPayload.run_id === "string" || existingPayload.run_id === null
        ? existingPayload.run_id
        : null;
      const writerGitSha = typeof existingPayload.writer_git_sha === "string"
          || existingPayload.writer_git_sha === null
        ? existingPayload.writer_git_sha
        : null;
      const payload = buildHistoryV2ConnectorManifest({
        domain: "observations",
        grain: null,
        profile: null,
        dayUtc,
        connectorId,
        runId,
        manifestKey: key,
        pollutantManifests: child.children,
        writerGitSha,
        backedUpAtUtc: existingBackup || childBackups.at(-1) || `${dayUtc}T00:00:00.000Z`,
      });
      await staged.stage({
        key,
        body: JSON.stringify(payload, null, 2),
        kind: "connector_manifest",
        dayUtc,
        dependencies: [...child.identities.keys()],
        localDependencySnapshot: localDependencySnapshot({
          child,
          proposals: staged.proposals,
          prefix: `${base}/connector_id=${connectorId}/pollutant_code=`,
          dayUtc,
          connectorId,
          kind: "pollutant",
          domain: "observations",
        }),
        provenance: {
          source: "legacy_connector_normalisation",
          validation_failures: classification.failures,
          immutable_scope_identity_verified: true,
        },
      });
      proposalKeys.push(key);
    }
  }
}

function safeLocalKey(key) {
  const normalized = String(key || "").replace(/^\/+/, "");
  if (!normalized || normalized.split("/").some((part) => part === "..")) throw new Error(`Unsafe local object key: ${key}`);
  return normalized;
}

function walkLocalObjects(root, prefixes = []) {
  const found = new Map();
  if (!root || !fs.existsSync(root)) return found;
  const visit = (directory, relative = "") => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const nextRelative = relative ? `${relative}/${entry.name}` : entry.name;
      const fullPath = `${directory}/${entry.name}`;
      if (entry.isDirectory()) visit(fullPath, nextRelative);
      else if (entry.isFile()) found.set(nextRelative, fullPath);
    }
  };
  for (const prefix of prefixes) {
    const normalized = safeLocalKey(prefix).replace(/\/$/, "");
    const scopedRoot = `${root}/${normalized}`;
    if (fs.existsSync(scopedRoot)) visit(scopedRoot, normalized);
  }
  return found;
}

export function createCombinedLocalStore({ overlayRoot, dropboxRoot, runStateJson, prefixes, exactKeys = [], dynamicExactKeyPrefixes = [] }) {
  const state = JSON.parse(fs.readFileSync(runStateJson, "utf8"));
  const proposedTombstones = new Set(Object.entries(state?.tombstones || {})
    .filter(([, value]) => value?.proposed === true || value?.deleted === true)
    .map(([key]) => safeLocalKey(key)));
  const proposedTombstonePrefixes = (state?.tombstone_prefixes || [])
    .filter((entry) => entry?.proposed === true && typeof entry?.prefix === "string")
    .map((entry) => `${safeLocalKey(entry.prefix).replace(/\/+$/, "")}/`);
  const isProposedAbsent = (key) => proposedTombstones.has(key)
    || proposedTombstonePrefixes.some((prefix) => key.startsWith(prefix));
  const dropboxPaths = walkLocalObjects(dropboxRoot, prefixes);
  for (const rawKey of exactKeys) {
    const key = safeLocalKey(rawKey);
    const candidate = `${dropboxRoot}/${key}`;
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) dropboxPaths.set(key, candidate);
  }
  for (const key of [...dropboxPaths.keys()]) {
    if (isProposedAbsent(key)) dropboxPaths.delete(key);
  }
  const overlayPaths = new Map();
  const overlayOwners = new Map();
  for (const [key, entry] of Object.entries(state?.objects || {})) {
    if (entry?.structurally_validated === true && typeof entry.local_path === "string" && fs.existsSync(entry.local_path)) {
      overlayPaths.set(safeLocalKey(key), entry.local_path);
      if (typeof entry.proposal_owner === "string" && entry.proposal_owner) {
        overlayOwners.set(safeLocalKey(key), entry.proposal_owner);
      } else if (entry.stage === "observations_data") {
        overlayOwners.set(safeLocalKey(key), "source_derived_observation_repair");
      }
    }
  }
  function localObject(key, source) {
    const localPath = (source === "overlay" ? overlayPaths : dropboxPaths).get(key);
    if (!localPath) return null;
    const body = fs.readFileSync(localPath);
    return {
      ...objectFromBody({ key, body, source, content_sha256: sha256Hex(body) }),
      proposal_owner: source === "overlay" ? overlayOwners.get(key) || null : null,
    };
  }

  function objectFor(key) {
    if (isProposedAbsent(key) && !overlayPaths.has(key)) return null;
    if (overlayPaths.has(key)) return localObject(key, "overlay");
    return localObject(key, "dropbox");
  }

  return {
    overlayRoot,
    getObjectFromSourceIfExists(key, source) {
      const normalized = safeLocalKey(key);
      return ["overlay", "dropbox"].includes(source) ? localObject(normalized, source) : null;
    },
    getObject(key) {
      const normalized = safeLocalKey(key);
      let object = objectFor(normalized);
      if (!object && dynamicExactKeyPrefixes.some((prefix) => normalized.startsWith(`${safeLocalKey(prefix)}/`))) {
        const candidate = `${dropboxRoot}/${normalized}`;
        if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
          dropboxPaths.set(normalized, candidate);
          object = objectFor(normalized);
        }
      }
      if (!object) {
        const error = new Error(`Combined local object unavailable: ${normalized}`);
        error.code = "OBJECT_NOT_FOUND";
        throw error;
      }
      return object;
    },
    getObjectIfExists(key) {
      try { return this.getObject(key); } catch (error) {
        if (error?.code === "OBJECT_NOT_FOUND") return null;
        throw error;
      }
    },
    listAllObjects({ prefix }) {
      const keys = new Set([...dropboxPaths.keys(), ...overlayPaths.keys()]);
      return [...keys]
        .filter((key) => key.startsWith(prefix) && (!isProposedAbsent(key) || overlayPaths.has(key)))
        .map((key) => {
          const object = objectFor(key);
          return { key, size: object?.bytes ?? null, source: object?.source ?? null, content_sha256: object?.content_sha256 ?? null, r2_etag: object?.r2_etag ?? null };
        })
        .sort((left, right) => left.key.localeCompare(right.key));
    },
  };
}

function objectFromBody({ key, body, source = "unknown", content_sha256 = null, r2_etag = null, last_modified = null }) {
  const buffer = Buffer.isBuffer(body) ? body : Buffer.from(body, "utf8");
  return {
    key,
    body: buffer,
    bytes: buffer.byteLength,
    source,
    content_sha256: content_sha256 || sha256Hex(buffer),
    r2_etag: r2_etag || null,
    last_modified: last_modified || null,
  };
}

function proposalView(proposal) {
  return {
    key: proposal.key,
    kind: proposal.kind,
    day_utc: proposal.day_utc,
    bytes: proposal.bytes,
    old_sha256: proposal.old_sha256,
    new_sha256: proposal.new_sha256,
    changed: proposal.changed,
    status: proposal.changed ? "planned" : "skipped_unchanged",
    dependencies: proposal.dependencies,
    dependency_identities: proposal.dependency_identities,
    provenance: proposal.provenance || null,
    baseline_source: proposal.baseline_source || null,
    local_dependency_snapshot: proposal.local_dependency_snapshot ? {
      source: proposal.local_dependency_snapshot.source,
      expected_child_keys: proposal.local_dependency_snapshot.expected_children.map((child) => child.key),
    } : null,
    expected_verification: proposal.changed ? "exact_body_and_bytes" : "not_required",
    proposed_body: proposal.body,
  };
}

function localDependencySnapshot({ child, proposals, prefix, dayUtc, connectorId, kind, domain = "observations" }) {
  return {
    prefix,
    dayUtc,
    connectorId,
    kind,
    domain,
    expected_children: child.children.map((payload) => {
      const key = payload.manifest_key;
      const staged = proposals.get(key);
      const identity = child.identities.get(key);
      return {
        key,
        content_sha256: staged?.new_sha256 || identity.content_sha256,
        source: staged ? "planned_overlay" : identity.source,
        staged: Boolean(staged),
      };
    }).sort((left, right) => left.key.localeCompare(right.key)),
    source: "combined_local_snapshot",
  };
}

export function createStagedObjectMap({ r2, store, dropboxSourceKeys = [] }) {
  const proposals = new Map();
  // `dropboxSourceKeys` identifies exact source leaves for targeted index
  // repairs. It must not make a valid combined-local leaf invisible to
  // connector/day discovery: a simultaneous parent rebuild needs every valid
  // sibling. readChildren still validates every manifest before use.
  void dropboxSourceKeys;
  const indexProposalKind = (key) => key.endsWith("_latest.json")
      ? "latest_timeseries_index"
      : "pollutant_timeseries_index";
  const indexDependencies = (key) => {
    const observation = String(key).match(
      /^history\/_index_v2\/observations_timeseries\/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)\/pollutant_code=([a-z0-9_]+)\/manifest\.json$/,
    );
    if (observation) {
      return [`history/v2/observations/day_utc=${observation[1]}/connector_id=${observation[2]}/pollutant_code=${observation[3]}/manifest.json`];
    }
    const aqi = String(key).match(
      /^history\/_index_v2\/aqilevels_hourly_data_timeseries\/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)\/pollutant_code=([a-z0-9_]+)\/manifest\.json$/,
    );
    return aqi
      ? [`history/v2/aqilevels/hourly/data/day_utc=${aqi[1]}/connector_id=${aqi[2]}/pollutant_code=${aqi[3]}/manifest.json`]
      : [];
  };

  function resolveDependencyIdentities(dependencies) {
    return Object.fromEntries(dependencies.map((dependencyKey) => {
      const staged = proposals.get(dependencyKey);
      if (staged) {
        return [dependencyKey, {
          sha256: staged.new_sha256,
          bytes: staged.bytes,
          source: "planned_overlay",
        }];
      }
      const existing = store.getObjectIfExists(dependencyKey);
      if (!existing) {
        throw new Error(`Combined local dependency unavailable: ${dependencyKey}`);
      }
      return [dependencyKey, {
        sha256: existing.content_sha256 || sha256Hex(existing.body),
        bytes: existing.bytes,
        source: existing.source,
      }];
    }));
  }

  async function stage({ key, body, contentType = "application/json", kind, dayUtc = null, dependencies = [], localDependencySnapshot = null, provenance = null }) {
    const bodyText = Buffer.isBuffer(body) ? body.toString("utf8") : String(body);
    const previous = proposals.get(key);
    const proposalKind = previous?.kind || kind;
    // Planning has no live-R2 read path. The verified overlay wins over the
    // Dropbox baseline; staged objects win over both for dependent builders.
    const existing = previous ? null : store.getObjectIfExists(key);
    const oldBody = previous ? previous.old_body : existing?.body?.toString("utf8") ?? null;
    const changed = oldBody !== bodyText;
    const allDependencies = [...new Set([...(previous?.dependencies || []), ...dependencies])].sort();
    const proposal = {
      key,
      kind: proposalKind,
      day_utc: previous?.day_utc || dayUtc,
      content_type: contentType,
      body: bodyText,
      bytes: Buffer.byteLength(bodyText, "utf8"),
      old_body: oldBody,
      old_sha256: oldBody === null ? null : sha256Hex(oldBody),
      new_sha256: sha256Hex(bodyText),
      changed,
      dependencies: allDependencies,
      dependency_identities: resolveDependencyIdentities(allDependencies),
      baseline_source: previous?.baseline_source || existing?.source || null,
      local_dependency_snapshot: previous?.local_dependency_snapshot || localDependencySnapshot,
      provenance: previous?.provenance || provenance || null,
    };
    proposals.set(key, proposal);
    return proposal;
  }

  function stagedObject(key) {
    const proposal = proposals.get(key);
    return proposal ? objectFromBody({ key, body: proposal.body, source: "planned_overlay", content_sha256: proposal.new_sha256 }) : null;
  }

  const stagedR2 = {
    ...r2,
    proposal_sink: async ({ key, body, content_type }) => {
      await stage({
        key,
        body,
        contentType: content_type,
        kind: indexProposalKind(key),
        dependencies: indexDependencies(key),
      });
    },
    adapter: {
      getObject: async ({ key }) => {
        const staged = stagedObject(key);
        if (staged) return staged;
        const object = store.getObjectIfExists(key);
        if (object) return object;
        const error = new Error(`Combined local object unavailable: ${key}`);
        error.code = "OBJECT_NOT_FOUND";
        throw error;
      },
      headObject: async ({ key }) => {
        const staged = stagedObject(key);
        if (staged) return { exists: true, key, bytes: staged.bytes, etag: null, content_sha256: staged.content_sha256 };
        const object = store.getObjectIfExists(key);
        return object ? { exists: true, key, bytes: object.bytes, etag: null, content_sha256: object.content_sha256 } : { exists: false, key };
      },
      listAllObjects: async ({ prefix, max_keys }) => {
        const entries = store.listAllObjects({ prefix, max_keys });
        const byKey = new Map(entries.map((entry) => [entry.key, entry]));
        for (const proposal of proposals.values()) {
          if (proposal.key.startsWith(prefix)) {
            byKey.set(proposal.key, { key: proposal.key, size: proposal.bytes, source: "planned_overlay", content_sha256: proposal.new_sha256, r2_etag: null });
          }
        }
        return [...byKey.values()].sort((left, right) => left.key.localeCompare(right.key));
      },
      listAllCommonPrefixes: async ({ prefix, delimiter, max_keys }) => {
        const entries = await stagedR2.adapter.listAllObjects({ prefix, max_keys });
        const prefixes = new Set();
        for (const entry of entries) {
          const suffix = entry.key.slice(prefix.length);
          const index = suffix.indexOf(delimiter);
          if (index >= 0) prefixes.add(`${prefix}${suffix.slice(0, index + delimiter.length)}`);
        }
        return [...prefixes].sort();
      },
      putObject: async ({ key, body, content_type }) => {
        const proposal = await stage({
          key,
          body,
          contentType: content_type,
          kind: indexProposalKind(key),
          dependencies: indexDependencies(key),
        });
        return { key, bytes: proposal.bytes, etag: proposal.new_sha256 };
      },
    },
  };

  return { proposals, stage, stagedR2, resolveDependencyIdentities };
}

function stableGeneratedAt({ dayUtc, dayManifest }) {
  const backedUpAt = String(dayManifest?.backed_up_at_utc || "");
  return /^\d{4}-\d{2}-\d{2}T/.test(backedUpAt) ? backedUpAt : `${dayUtc}T00:00:00.000Z`;
}

function parquetIso(value) {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString();
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

async function parquetFileEntry({ store, key, domain, pollutantCode }) {
  const object = await store.getObject(key);
  const file = new Uint8Array(object.body).slice().buffer;
  const metadata = await parquetMetadataAsync(file);
  const rowCount = Math.max(0, Number(metadata.num_rows || 0));
  if (!rowCount) throw new Error("parquet_zero_rows");
  const physicalColumns = parquetSchema(metadata).children
    .map((column) => String(column.element.name));
  const schemaColumns = new Set(physicalColumns);
  const timestampCandidates = domain === "observations"
    ? ["observed_at_utc", "observed_at"]
    : ["timestamp_hour_utc"];
  const timestampColumn = timestampCandidates.find((column) => schemaColumns.has(column));
  if (!timestampColumn) throw new Error(`parquet_timestamp_column_missing:${timestampCandidates.join("|")}`);
  let rows = [];
  await parquetRead({
    file,
    metadata,
    columns: ["timeseries_id", timestampColumn],
    rowStart: 0,
    rowEnd: rowCount,
    compressors,
    onComplete: (value) => { rows = Array.isArray(value) ? value : []; },
  });
  const counts = {};
  let minTimeseriesId = null;
  let maxTimeseriesId = null;
  let minTimestamp = null;
  let maxTimestamp = null;
  for (const row of rows) {
    const timeseriesId = Number(Array.isArray(row) ? row[0] : null);
    const timestamp = parquetIso(Array.isArray(row) ? row[1] : null);
    if (!Number.isInteger(timeseriesId) || timeseriesId <= 0 || !timestamp) {
      throw new Error("parquet_required_metadata_invalid");
    }
    const id = Math.trunc(timeseriesId);
    counts[String(id)] = (counts[String(id)] || 0) + 1;
    minTimeseriesId = minTimeseriesId === null ? id : Math.min(minTimeseriesId, id);
    maxTimeseriesId = maxTimeseriesId === null ? id : Math.max(maxTimeseriesId, id);
    minTimestamp = minTimestamp === null || timestamp < minTimestamp ? timestamp : minTimestamp;
    maxTimestamp = maxTimestamp === null || timestamp > maxTimestamp ? timestamp : maxTimestamp;
  }
  if (Object.values(counts).reduce((total, count) => total + count, 0) !== rowCount) {
    throw new Error("parquet_row_count_metadata_mismatch");
  }
  return {
    fileEntry: {
      key,
      row_count: rowCount,
      bytes: object.bytes,
      etag_or_hash: sha256Hex(object.body),
      pollutant_codes: [pollutantCode],
      min_timeseries_id: minTimeseriesId,
      max_timeseries_id: maxTimeseriesId,
      ...(domain === "observations"
        ? { min_observed_at_utc: minTimestamp, max_observed_at_utc: maxTimestamp }
        : { min_timestamp_hour_utc: minTimestamp, max_timestamp_hour_utc: maxTimestamp }),
      timeseries_row_counts: counts,
    },
    physicalSchema: domain === "observations"
      ? observationHistoryPhysicalSchemaForColumns(physicalColumns)
      : null,
  };
}

async function observationContentHashForPartKeys({
  store,
  partKeys,
  connectorId,
}) {
  const canonicalRows = [];
  const sosConnectorId = Number.parseInt(
    process.env.UK_AQ_BACKFILL_SOS_CONNECTOR_ID_FALLBACK || "1",
    10,
  );
  for (const key of partKeys) {
    const object = await store.getObject(key);
    const file = new Uint8Array(object.body).slice().buffer;
    const metadata = await parquetMetadataAsync(file);
    const rowCount = Math.max(0, Number(metadata.num_rows || 0));
    const schemaColumns = new Set(
      parquetSchema(metadata).children.map((column) =>
        String(column.element.name)
      ),
    );
    const required = [
      "connector_id",
      "station_id",
      "timeseries_id",
      "pollutant_code",
      "observed_at_utc",
      "value",
    ];
    const missing = required.filter((column) => !schemaColumns.has(column));
    if (missing.length > 0) {
      throw new Error(
        `parquet_canonical_columns_missing:${missing.join("|")}`,
      );
    }
    const statusColumn = schemaColumns.has("verification_status")
      ? "verification_status"
      : schemaColumns.has("status")
      ? "status"
      : null;
    const columns = [...required, ...(statusColumn ? [statusColumn] : [])];
    let rows = [];
    await parquetRead({
      file,
      metadata,
      columns,
      rowStart: 0,
      rowEnd: rowCount,
      compressors,
      onComplete: (value) => {
        rows = Array.isArray(value) ? value : [];
      },
    });
    for (const values of rows) {
      const statusRow = statusColumn
        ? { [statusColumn]: values[required.length] ?? null }
        : {};
      canonicalRows.push(normalizeCanonicalObservationRow({
        connector_id: values[0],
        station_id: values[1] ?? null,
        timeseries_id: values[2],
        pollutant_code: values[3],
        observed_at_utc: parquetIso(values[4]),
        value: values[5],
        verification_status: resolveLegacyVerificationStatus(statusRow, {
          isSos: Number(connectorId) === sosConnectorId,
        }),
      }));
    }
  }
  const {
    canonical_rows: _canonicalRows,
    ...metadata
  } = computeObservationContentHash(canonicalRows);
  return metadata;
}

function canonicalManifestMetadata(payload, {
  manifestKey,
  domain,
  grain,
  profile,
  dayUtc,
  connectorId,
  pollutantCode,
  manifestKind,
}) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)
    || payload.history_version !== "v2"
    || payload.manifest_kind !== manifestKind
    || payload.domain !== domain
    || payload.grain !== grain
    || payload.profile !== profile
    || payload.manifest_key !== manifestKey
    || payload.day_utc !== dayUtc
    || payload.connector_id !== connectorId
    || payload.pollutant_code !== pollutantCode
    || !Array.isArray(payload.files)
    || !Array.isArray(payload.parquet_object_keys)) return null;
  const { manifest_hash: manifestHash, ...withoutHash } = payload;
  if (typeof manifestHash !== "string" || manifestHash !== sha256Hex(JSON.stringify(withoutHash))) return null;
  const backedUpAtUtc = typeof payload.backed_up_at_utc === "string" && !Number.isNaN(Date.parse(payload.backed_up_at_utc))
    ? payload.backed_up_at_utc
    : null;
  if (!backedUpAtUtc) return null;
  return {
    run_id: typeof payload.run_id === "string" || payload.run_id === null ? payload.run_id : null,
    writer_git_sha: typeof payload.writer_git_sha === "string" || payload.writer_git_sha === null ? payload.writer_git_sha : null,
    backed_up_at_utc: backedUpAtUtc,
  };
}

function sourceManifestMetadata(store, source, label, manifestKey, expectation) {
  const object = store.getObjectFromSourceIfExists(manifestKey, source);
  if (!object) return null;
  try {
    const metadata = canonicalManifestMetadata(jsonObject(object, manifestKey), { manifestKey, ...expectation });
    if (!metadata) return null;
    return {
      payload: metadata,
      provenance: {
        run_id: metadata.run_id === null ? "schema_nullable" : label,
        writer_git_sha: metadata.writer_git_sha === null ? "schema_nullable" : label,
        backed_up_at_utc: label,
        source: object.proposal_owner || label,
      },
    };
  } catch {
    return null;
  }
}

function existingManifestMetadata(store, {
  manifestKey,
  base,
  dayUtc,
  connectorId,
  pollutantCode,
  domain,
}) {
  const grain = domain === "aqilevels" ? "hourly" : null;
  const profile = domain === "aqilevels" ? "data" : null;
  const leafExpectation = { domain, grain, profile, dayUtc, connectorId, pollutantCode, manifestKind: "pollutant" };
  for (const [source, label] of [["overlay", "overlay_manifest"], ["dropbox", "dropbox_manifest"]]) {
    const found = sourceManifestMetadata(store, source, label, manifestKey, leafExpectation);
    if (found) return found;
  }

  // A canonical parent can supply equivalent writer/run provenance, but it
  // cannot supply leaf file metadata. Prefer it only after a valid same-leaf
  // copy was unavailable, and never invent a historical run ID or Git SHA.
  const parentKey = `${base}/connector_id=${connectorId}/manifest.json`;
  const parentExpectation = { domain, grain, profile, dayUtc, connectorId, pollutantCode: null, manifestKind: "connector" };
  for (const [source, label] of [["overlay", "overlay_parent_manifest_metadata"], ["dropbox", "dropbox_parent_manifest_metadata"]]) {
    const found = sourceManifestMetadata(store, source, label, parentKey, parentExpectation);
    if (found) return found;
  }

  return {
    payload: {
      run_id: null,
      writer_git_sha: null,
      // The v2 schema requires a timestamp. This deterministic timestamp is
      // explicitly repair-generated provenance, never a claim about the
      // original writer's historical backup time.
      backed_up_at_utc: `${dayUtc}T00:00:00.000Z`,
    },
    provenance: {
      run_id: "schema_nullable",
      writer_git_sha: "schema_nullable",
      backed_up_at_utc: "repair_generated",
      source: "repair_generated",
    },
  };
}

export async function leafManifestSourceFromCombined({ store, dataPrefix, base, dayUtc, connectorId, pollutantCode, domain }) {
  void dataPrefix;
  const pollutantPrefix = `${base}/connector_id=${connectorId}/pollutant_code=${pollutantCode}`;
  const keyPattern = new RegExp(
    `^${pollutantPrefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}/part-\\d+\\.parquet$`,
  );
  const partKeys = store.listAllObjects({ prefix: `${pollutantPrefix}/` })
    .map((entry) => entry.key)
    .filter((key) => keyPattern.test(key))
    .sort();
  if (!partKeys.length) return { blocked_reason: "canonical_parquet_objects_unavailable" };
  const manifestKey = `${pollutantPrefix}/manifest.json`;
  const metadata = existingManifestMetadata(store, {
    manifestKey, base, dayUtc, connectorId, pollutantCode, domain,
  });
  try {
    const files = [];
    const physicalSchemas = [];
    for (const key of partKeys) {
      const inspected = await parquetFileEntry({
        store,
        key,
        domain,
        pollutantCode,
      });
      files.push(inspected.fileEntry);
      if (inspected.physicalSchema) {
        physicalSchemas.push(inspected.physicalSchema);
      }
    }
    const physicalSchema = domain === "observations"
      ? combineObservationHistoryPhysicalSchemas(physicalSchemas)
      : null;
    if (Array.isArray(physicalSchema?.physical_schemas)) {
      throw new Error("mixed_pollutant_parquet_physical_schemas");
    }
    const observationContentHash = domain === "observations"
      ? await observationContentHashForPartKeys({
        store,
        partKeys,
        connectorId,
      })
      : null;
    return {
      manifestKey,
      payload: metadata.payload,
      provenance: metadata.provenance,
      files,
      observationContentHash,
      physicalSchema,
    };
  } catch (error) {
    return { blocked_reason: `canonical_parquet_metadata_unreadable:${error instanceof Error ? error.message : String(error)}` };
  }
}

const REPAIR_STATUSES = new Set([
  "planned",
  "executing",
  "skipped_unchanged",
  "succeeded",
  "failed",
  "blocked_dependency",
  "not_run",
]);

function collectOutcomeStatuses(value, statuses = []) {
  if (!value || typeof value !== "object") return statuses;
  if (Array.isArray(value)) {
    for (const entry of value) collectOutcomeStatuses(entry, statuses);
    return statuses;
  }
  if (REPAIR_STATUSES.has(value.status)) statuses.push(value.status);
  if (REPAIR_STATUSES.has(value.verification_status)) statuses.push(value.verification_status);
  if (Number(value.blocked_dependency_count || 0) > 0) statuses.push("blocked_dependency");
  if (Number(value.failed_count || value.failure_count || 0) > 0) statuses.push("failed");
  for (const [key, entry] of Object.entries(value)) {
    if (key !== "status" && key !== "verification_status") {
      collectOutcomeStatuses(entry, statuses);
    }
  }
  return statuses;
}

function collectVerificationStatuses(value, statuses = []) {
  if (!value || typeof value !== "object") return statuses;
  if (Array.isArray(value)) {
    for (const entry of value) collectVerificationStatuses(entry, statuses);
    return statuses;
  }
  if (REPAIR_STATUSES.has(value.verification_status)) statuses.push(value.verification_status);
  if (REPAIR_STATUSES.has(value.verification?.status)) statuses.push(value.verification.status);
  for (const [key, entry] of Object.entries(value)) {
    if (key !== "verification_status" && key !== "verification") {
      collectVerificationStatuses(entry, statuses);
    }
  }
  return statuses;
}

function reduceRepairStatus(statuses, fallback = "not_run") {
  const known = statuses.filter((status) => REPAIR_STATUSES.has(status));
  if (known.includes("blocked_dependency")) return "blocked_dependency";
  if (known.includes("failed")) return "failed";
  if (known.includes("succeeded")) return "succeeded";
  if (known.includes("planned") || known.includes("executing")) return "planned";
  if (known.includes("skipped_unchanged")) return "skipped_unchanged";
  return fallback;
}

function assertCanonicalObjectKey(value, label) {
  if (typeof value !== "string" || !value.trim() || value !== value.trim() || value.startsWith("/")) {
    throw new Error(`Invalid canonical R2 ${label}`);
  }
  const parts = value.split("/");
  if (parts.some((part) => !part || part === "." || part === "..")) {
    throw new Error(`Invalid canonical R2 ${label}: ${value}`);
  }
  return value;
}

function assertCanonicalManifestProposal(proposal) {
  const expectedKind = {
    pollutant_manifest: "pollutant",
    connector_manifest: "connector",
    day_manifest: "day",
  }[proposal.kind];
  if (!expectedKind) return;
  let payload;
  try {
    payload = JSON.parse(proposal.body);
  } catch {
    throw new Error(`Invalid ${proposal.kind} JSON proposal: ${proposal.key}`);
  }
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error(`Invalid ${proposal.kind} payload: ${proposal.key}`);
  }
  const isAqi = payload.domain === "aqilevels";
  const expectedGrain = isAqi ? "hourly" : null;
  const expectedProfile = isAqi ? "data" : null;
  if (!['observations', 'aqilevels'].includes(payload.domain)
    || payload.history_version !== "v2"
    || payload.manifest_kind !== expectedKind
    || payload.manifest_key !== proposal.key
    || payload.grain !== expectedGrain
    || payload.profile !== expectedProfile) {
    throw new Error(`Invalid canonical ${proposal.kind} contract: ${proposal.key}`);
  }
  if (typeof payload.backed_up_at_utc !== "string" || Number.isNaN(Date.parse(payload.backed_up_at_utc))) {
    throw new Error(`Invalid canonical ${proposal.kind} backed_up_at_utc: ${proposal.key}`);
  }
  if (!Array.isArray(payload.files) || !Array.isArray(payload.parquet_object_keys)) {
    throw new Error(`Invalid canonical ${proposal.kind} file collection: ${proposal.key}`);
  }
  for (const entry of payload.files) {
    if (!entry || typeof entry !== "object") {
      throw new Error(`Invalid canonical ${proposal.kind} files entry: ${proposal.key}`);
    }
    assertCanonicalObjectKey(entry.key, "files[].key");
  }
  for (const key of payload.parquet_object_keys) assertCanonicalObjectKey(key, "parquet_object_keys entry");
  const { manifest_hash: manifestHash, ...withoutHash } = payload;
  if (typeof manifestHash !== "string" || manifestHash !== sha256Hex(JSON.stringify(withoutHash))) {
    throw new Error(`Invalid canonical ${proposal.kind} manifest_hash: ${proposal.key}`);
  }
}

function assertCanonicalProposal(proposal) {
  if (!proposal || typeof proposal !== "object") throw new Error("Invalid metadata proposal");
  assertCanonicalObjectKey(proposal.key, "proposal key");
  if (typeof proposal.body !== "string" || Buffer.byteLength(proposal.body, "utf8") !== proposal.bytes) {
    throw new Error(`Invalid proposal body or bytes: ${proposal.key}`);
  }
  for (const dependency of proposal.dependencies || []) assertCanonicalObjectKey(dependency, "proposal dependency");
  if (!proposal.dependency_identities || typeof proposal.dependency_identities !== "object"
    || Array.isArray(proposal.dependency_identities)) {
    throw new Error(`Invalid proposal dependency identities: ${proposal.key}`);
  }
  for (const dependency of proposal.dependencies || []) {
    const identity = proposal.dependency_identities[dependency];
    if (!identity || !/^[a-f0-9]{64}$/.test(String(identity.sha256 || ""))
      || !Number.isSafeInteger(identity.bytes) || identity.bytes < 0
      || !["planned_overlay", "overlay", "dropbox"].includes(identity.source)) {
      throw new Error(`Invalid proposal dependency identity: ${proposal.key} -> ${dependency}`);
    }
  }
  if (proposal.local_dependency_snapshot) {
    if (typeof proposal.local_dependency_snapshot !== "object"
      || proposal.local_dependency_snapshot.source !== "combined_local_snapshot"
      || !Array.isArray(proposal.local_dependency_snapshot.expected_children)) {
      throw new Error(`Invalid proposal local dependency snapshot: ${proposal.key}`);
    }
    assertCanonicalObjectKey(proposal.local_dependency_snapshot.prefix, "local dependency snapshot prefix");
    for (const child of proposal.local_dependency_snapshot.expected_children) {
      assertCanonicalObjectKey(child?.key, "local dependency snapshot child key");
      if (!/^[a-f0-9]{64}$/.test(String(child?.content_sha256 || ""))) {
        throw new Error(`Invalid proposal local dependency snapshot hash: ${proposal.key}`);
      }
    }
  }
  assertCanonicalManifestProposal(proposal);
  if (!proposal.kind.endsWith("manifest")) {
    try {
      const payload = JSON.parse(proposal.body);
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
        throw new Error("payload is not an object");
      }
    } catch (error) {
      throw new Error(`Invalid canonical ${proposal.kind} proposal: ${proposal.key} (${error instanceof Error ? error.message : String(error)})`);
    }
  }
}

function assertCanonicalProposalRelationships(proposal, proposals) {
  const guard = proposal.local_dependency_snapshot;
  if (!guard) return;
  const stagedKind = {
    pollutant: "pollutant_manifest",
    connector: "connector_manifest",
  }[guard.kind];
  for (const child of guard.expected_children) {
    if (!child?.staged) continue;
    const staged = proposals.get(child.key);
    if (!stagedKind
      || !child.key.startsWith(guard.prefix)
      || child.source !== "planned_overlay"
      || !staged
      || staged.kind !== stagedKind
      || !/^[a-f0-9]{64}$/.test(String(child.content_sha256 || ""))
      || staged.new_sha256 !== child.content_sha256
      || !(proposal.dependencies || []).includes(child.key)) {
      throw new Error(`Invalid staged child proposal dependency: ${proposal.key} -> ${child?.key || "(missing)"}`);
    }
  }
}

function extractRepairPlan(input) {
  if (input?.history_version === "v2" && ["observations", "aqilevels"].includes(input?.domain) && Array.isArray(input.repair_plan)) {
    return { inputKind: `${input.domain}_repair_plan`, domain: input.domain, actions: input.repair_plan };
  }
  const v2 = input?.history_version_results?.v2;
  const observations = v2?.observations;
  if (input?.history_version_results) {
    if (!(["v2", "both"].includes(input.history_version_mode)
      && Array.isArray(input.checked_versions) && input.checked_versions.includes("v2")
      && v2?.history_version === "v2" && v2.checks_implemented === true
      && typeof observations?.status === "string" && Number.isInteger(observations.checked_partitions)
      && Number.isInteger(observations.gap_count) && Array.isArray(observations.gaps)
      && Array.isArray(observations.repair_plan))) {
      throw new Error("Invalid complete integrity report: expected checked v2 observations results");
    }
    return { inputKind: "integrity_report", domain: "observations", actions: observations.repair_plan };
  }
  throw new Error("Repair input must be a complete integrity report with history_version_results.v2.observations.repair_plan, or { history_version: 'v2', domain: 'observations', repair_plan: [...] }");
}

function validateAction(action) {
  if (!SUPPORTED_ACTIONS.has(action?.kind)) {
    throw new Error(`Unsupported Phase 4 repair action: ${String(action?.kind || "unknown")}`);
  }
  if (action.status !== "planned" || action.executes !== false
    || action.data_changes_required !== false || action.operator_action_required !== false) {
    throw new Error(`Unsafe Phase 4 repair action: ${action.kind}`);
  }
  if (action.history_version !== undefined && action.history_version !== "v2") {
    throw new Error(`Unsupported history version for Phase 4 repair action: ${action.history_version}`);
  }
  if (action.domain !== undefined && !["observations", "aqilevels"].includes(action.domain)) {
    throw new Error(`Unsupported domain for Phase 4 repair action: ${action.domain}`);
  }
  if (typeof action.requires_index_rebuild !== "boolean" || !Array.isArray(action.gap_types)
    || action.gap_types.some((gapType) => typeof gapType !== "string" || !gapType.trim())) {
    throw new Error(`Invalid Phase 4 repair action contract: ${action.kind}`);
  }
  if (action.targeted_replacement_timeseries_ids !== undefined
    && (!Array.isArray(action.targeted_replacement_timeseries_ids)
      || action.targeted_replacement_timeseries_ids.some((value) =>
        !Number.isInteger(Number(value)) || Number(value) <= 0))) {
    throw new Error(`Invalid targeted replacement timeseries IDs: ${action.kind}`);
  }
}

export function normalizePlan(input) {
  const { inputKind, domain, actions } = extractRepairPlan(input);
  const scopes = new Map();
  for (const action of actions) {
    validateAction(action);
    const dayUtc = String(action.day_utc || "");
    const rule = ACTION_SCOPE_RULES[action.kind];
    const hasConnector = action.connector_id !== undefined && action.connector_id !== null;
    const connectorId = hasConnector ? Number(action.connector_id) : null;
    const pollutantCode = typeof action.pollutant_code === "string"
      ? action.pollutant_code.trim().toLowerCase()
      : "";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dayUtc)) {
      throw new Error(`Repair action ${action.kind} must have day_utc`);
    }
    if (rule.connector === "required" && (!Number.isInteger(connectorId) || connectorId <= 0)) {
      throw new Error(`Repair action ${action.kind} must have day_utc and positive connector_id`);
    }
    if (rule.connector === "absent" && hasConnector) {
      throw new Error(`Repair action ${action.kind} must have day_utc and no connector_id`);
    }
    if (rule.pollutant === "required" && !pollutantCode) {
      throw new Error(`Repair action ${action.kind} must have pollutant_code`);
    }
    const key = rule.connector === "absent" ? `${dayUtc}|day` : `${dayUtc}|${connectorId}`;
    const scope = scopes.get(key) || {
      dayUtc,
      connectorId,
      needsConnector: false,
      needsDay: false,
      needsIndex: false,
      pollutantRepair: false,
      pollutantManifestCodes: new Set(),
      indexPollutantCodes: new Set(),
      targetedReplacementTimeseriesIds: new Set(),
      gapTypes: new Set(),
    };
    scope.needsConnector ||= Boolean(rule.needsConnector);
    scope.needsDay ||= Boolean(rule.needsDay);
    scope.needsIndex ||= Boolean(action.requires_index_rebuild) || action.kind.includes("index");
    scope.pollutantRepair ||= Boolean(rule.pollutantRepair);
    // A direct index repair can share a connector/day scope with manifest
    // repairs, but it must never promote its valid pollutant manifest into a
    // manifest rewrite.  Track leaf-manifest and index-only codes separately.
    if (pollutantCode && rule.pollutantRepair) {
      scope.pollutantManifestCodes.add(pollutantCode);
    }
    if (pollutantCode && action.kind.includes("index")) {
      scope.indexPollutantCodes.add(pollutantCode);
    }
    for (const value of action.targeted_replacement_timeseries_ids || []) {
      scope.targetedReplacementTimeseriesIds.add(Number(value));
    }
    for (const gapType of action.gap_types) scope.gapTypes.add(gapType);
    scopes.set(key, scope);
  }
  return {
    inputKind,
    domain,
    scopes: [...scopes.values()]
      .sort((left, right) => left.dayUtc.localeCompare(right.dayUtc) || (left.connectorId ?? -1) - (right.connectorId ?? -1))
      .map(({
        gapTypes,
        pollutantManifestCodes,
        indexPollutantCodes,
        targetedReplacementTimeseriesIds,
        ...scope
      }) => ({
        ...scope,
        gap_types: [...gapTypes].sort(),
        pollutant_codes: [...pollutantManifestCodes].sort(),
        index_pollutant_codes: [...indexPollutantCodes].sort(),
        targeted_replacement_timeseries_ids: [...targetedReplacementTimeseriesIds]
          .sort((left, right) => left - right),
      })),
  };
}

function authoritativeTimeseriesById(input) {
  const bindings = new Map();
  for (const raw of Array.isArray(input?.authoritative_core_timeseries)
    ? input.authoritative_core_timeseries
    : []) {
    const timeseriesId = Number(raw?.timeseries_id);
    const connectorId = Number(raw?.connector_id);
    const pollutantCode = String(raw?.pollutant_code || "").trim().toLowerCase();
    if (!Number.isInteger(timeseriesId) || timeseriesId <= 0
      || !Number.isInteger(connectorId) || connectorId <= 0 || !pollutantCode) continue;
    bindings.set(String(timeseriesId), {
      timeseries_id: timeseriesId,
      connector_id: connectorId,
      pollutant_code: pollutantCode,
      phenomenon_id: raw?.phenomenon_id ?? null,
      observed_property_id: raw?.observed_property_id ?? null,
    });
  }
  return bindings;
}

export async function runV2ObservationsRepair({
  argv = process.argv.slice(2),
  env = process.env,
  repairPlan = null,
  updateIndexes = updateR2HistoryIndexesTargeted,
} = {}) {
  const startedAtMs = Date.now();
  const reportProgress = ({ phase, completed_objects = 0, total_objects = 0, successful_put_count = 0, successful_readback_verification_count = 0, failures = 0, blocked_count = 0 }) => {
    process.stderr.write(`UK_AQ_INTEGRITY_PROGRESS ${JSON.stringify({
      phase,
      completed_objects,
      total_objects,
      successful_put_count,
      successful_readback_verification_count,
      failures,
      blocked_count,
      elapsed_seconds: Math.round((Date.now() - startedAtMs) / 1000),
    })}\n`);
  };
  const args = repairPlan
    ? {
      writeR2: argv.includes("--write-r2"),
      overlayRoot: String(env.UK_AQ_HISTORY_INTEGRITY_OVERLAY_ROOT || ""),
      dropboxRoot: String(env.UK_AQ_R2_HISTORY_DROPBOX_ROOT || ""),
      runStateJson: String(env.UK_AQ_HISTORY_INTEGRITY_RUN_STATE_JSON || ""),
    }
    : parseArgs(argv);
  const input = repairPlan || JSON.parse(
    args.repairPlanStdin
      ? fs.readFileSync(0, "utf8")
      : fs.readFileSync(args.repairPlanJson, "utf8"),
  );
  const { inputKind, domain, scopes } = normalizePlan(input); // Validate all actions before the first R2 request.
  reportProgress({ phase: "metadata_planning_start", total_objects: scopes.length });
  const config = resolveR2HistoryIndexConfig(env);
  if (args.writeR2) {
    throw new Error("metadata executor is proposal-only; use the validated canonical apply executor");
  }
  const byDay = new Map();
  for (const scope of scopes) {
    const dayScopes = byDay.get(scope.dayUtc) || [];
    dayScopes.push(scope);
    byDay.set(scope.dayUtc, dayScopes);
  }
  if (!args.overlayRoot || !args.dropboxRoot || !args.runStateJson) {
    throw new Error("Combined local resolver paths are required for metadata repair");
  }
  const dataPrefix = domain === "observations"
    ? config.observations_prefix_v2
    : config.aqilevels_hourly_data_prefix_v2;
  const indexPrefix = domain === "observations"
    ? config.observations_timeseries_index_prefix_v2
    : config.aqilevels_hourly_data_timeseries_index_prefix_v2;
  // Targeted index rebuilds merge the changed days into this global latest
  // summary.  Read exactly that key from the Dropbox baseline; scanning the
  // whole index tree would make the sparse overlay resolver non-deterministic.
  const latestIndexKey = domain === "observations"
    ? `${config.index_prefix_v2}/observations_timeseries_latest.json`
    : `${config.index_prefix_v2}/aqilevels_hourly_data_timeseries_latest.json`;
  // An explicit index-only action can legitimately target a historical leaf
  // which is absent from the live connector/day hierarchy. Keep that leaf
  // out of parent discovery, but allow its Dropbox manifest as a narrowly
  // scoped index source. The target index itself is still live-only.
  const additionalIndexPollutantTargets = domain === "observations"
    ? scopes.flatMap((scope) => (scope.needsIndex && Number.isInteger(scope.connectorId) && scope.connectorId > 0
      ? (scope.index_pollutant_codes || []).map((pollutantCode) => ({
        day_utc: scope.dayUtc,
        connector_id: scope.connectorId,
        pollutant_code: pollutantCode,
        manifest_key: `${dataPrefix}/day_utc=${scope.dayUtc}/connector_id=${scope.connectorId}/pollutant_code=${pollutantCode}/manifest.json`,
      }))
      : []))
    : [];
  const localStore = createCombinedLocalStore({
    ...args,
    prefixes: [...new Set(scopes.flatMap((scope) => [
      `${dataPrefix}/day_utc=${scope.dayUtc}`,
      `${indexPrefix}/day_utc=${scope.dayUtc}`,
    ]))],
    exactKeys: [latestIndexKey],
    dynamicExactKeyPrefixes: [],
  });
  const staged = createStagedObjectMap({
    r2: config.r2,
    store: localStore,
    dropboxSourceKeys: additionalIndexPollutantTargets.map((target) => target.manifest_key),
  });
  const dayPlans = [];
  const blockedScopes = [];
  const blockedConnectorScopes = new Set();
  const plannedStageStatus = args.writeR2 ? "not_run" : "planned";
  const manifestStageStatus = (proposalKeys) => proposalKeys.some((key) =>
    String(staged.proposals.get(key)?.kind || "").endsWith("manifest")
  ) ? plannedStageStatus : "not_run";

  for (const [dayUtc, dayScopes] of [...byDay.entries()].sort(([left], [right]) => left.localeCompare(right))) {
    const base = `${dataPrefix}/day_utc=${dayUtc}`;
    const proposalKeys = [];
    // Pollutant manifests are the leaf metadata layer.  Rebuild them before
    // connector/day manifests so a malformed child cannot be preserved by a
    // newly generated parent.  The writer records complete per-part metadata
    // in `files`; its parquet objects are the immutable data dependency.
    for (const scope of dayScopes.filter((value) => value.pollutantRepair).sort((left, right) => left.connectorId - right.connectorId)) {
      const wanted = new Set(scope.pollutant_codes || []);
      const availableCodes = [...wanted].sort();
      const selectedCodes = availableCodes.filter((code) => !wanted.size || wanted.has(code));
      const missingRequested = [...wanted].filter((code) => !availableCodes.includes(code));
      for (const pollutantCode of missingRequested) {
        blockedScopes.push({ ...scope, pollutant_code: pollutantCode, status: "blocked_dependency", reason: "final_parquet_objects_unavailable" });
        blockedConnectorScopes.add(`${dayUtc}|${scope.connectorId}`);
      }
      if (!selectedCodes.length) {
        const blocked = { ...scope, status: "blocked_dependency", reason: "final_parquet_objects_unavailable" };
        blockedScopes.push(blocked);
        blockedConnectorScopes.add(`${dayUtc}|${scope.connectorId}`);
        continue;
      }
      for (const pollutantCode of selectedCodes) {
        const source = await leafManifestSourceFromCombined({
          store: localStore,
          dataPrefix,
          base,
          dayUtc,
          connectorId: scope.connectorId,
          pollutantCode,
          domain,
        });
        if (source.blocked_reason) {
          const blocked = { ...scope, pollutant_code: pollutantCode, status: "blocked_dependency", reason: source.blocked_reason };
          blockedScopes.push(blocked);
          blockedConnectorScopes.add(`${dayUtc}|${scope.connectorId}`);
          continue;
        }
        const {
          manifestKey,
          payload,
          provenance,
          files,
          observationContentHash,
          physicalSchema,
        } = source;
        const rebuilt = buildHistoryV2PollutantManifest({
          domain,
          grain: domain === "aqilevels" ? "hourly" : null,
          profile: domain === "aqilevels" ? "data" : null,
          dayUtc,
          connectorId: scope.connectorId,
          pollutantCode,
          runId: payload.run_id || null,
          manifestKey,
          sourceRowCount: files.reduce((total, entry) => total + Number(entry.row_count || 0), 0),
          fileEntries: files,
          writerGitSha: payload.writer_git_sha || null,
          backedUpAtUtc: payload.backed_up_at_utc || `${dayUtc}T00:00:00.000Z`,
          observationContentHash,
          physicalSchema,
        });
        await staged.stage({
          key: manifestKey,
          body: JSON.stringify(rebuilt, null, 2),
          kind: "pollutant_manifest",
          dayUtc,
          dependencies: files.map((entry) => String(entry?.key || "")).filter(Boolean),
          provenance,
        });
        proposalKeys.push(manifestKey);
      }
    }
    for (const scope of dayScopes.filter((value) => value.needsConnector).sort((left, right) => left.connectorId - right.connectorId)) {
      if (blockedConnectorScopes.has(`${dayUtc}|${scope.connectorId}`)) {
        blockedScopes.push({ ...scope, status: "blocked_dependency", reason: "pollutant_manifest_dependency_blocked" });
        continue;
      }
      const child = await readChildren({ store: staged.stagedR2.adapter, prefix: `${base}/connector_id=${scope.connectorId}/pollutant_code=`, dayUtc, connectorId: scope.connectorId, kind: "pollutant", domain, allowEmpty: true });
      const key = `${base}/connector_id=${scope.connectorId}/manifest.json`;
      if (!child.children.length) {
        const existing = staged.stagedR2.adapter.getObjectIfExists(key);
        if (!existing) {
          blockedScopes.push({ ...scope, status: "blocked_dependency", reason: "empty_connector_manifest_unavailable" });
          blockedConnectorScopes.add(`${dayUtc}|${scope.connectorId}`);
          continue;
        }
        const existingPayload = jsonObject(existing, key);
        if (domain === "observations") {
          assertV2ObservationsChildManifest(existingPayload, { key, kind: "connector", dayUtc, connectorId: scope.connectorId });
        } else if (existingPayload?.domain !== "aqilevels" || existingPayload?.manifest_kind !== "connector") {
          throw new Error(`Invalid empty AQI connector manifest: ${key}`);
        }
        if ((existingPayload.child_manifests || []).length || (existingPayload.files || []).length) {
          throw new Error(`Connector manifest has children hidden by the proposed final state: ${key}`);
        }
        proposalKeys.push(key);
        continue;
      }
      const payload = buildHistoryV2ConnectorManifest({ domain, grain: domain === "aqilevels" ? "hourly" : null, profile: domain === "aqilevels" ? "data" : null, dayUtc, connectorId: scope.connectorId, runId: child.children[0].run_id, manifestKey: key, pollutantManifests: child.children, writerGitSha: child.children[0].writer_git_sha, backedUpAtUtc: child.children.map((value) => value.backed_up_at_utc).sort().at(-1) || null });
      await staged.stage({
        key,
        body: JSON.stringify(payload, null, 2),
        kind: "connector_manifest",
        dayUtc,
        dependencies: [...child.identities.keys()],
        localDependencySnapshot: localDependencySnapshot({
          child,
          proposals: staged.proposals,
          prefix: `${base}/connector_id=${scope.connectorId}/pollutant_code=`,
          dayUtc,
          connectorId: scope.connectorId,
          kind: "pollutant",
          domain,
        }),
      });
      proposalKeys.push(key);
    }

    const needsDay = dayScopes.some((scope) => scope.needsDay);
    let dayManifest = null;
    let dayManifestKey = `${base}/manifest.json`;
    const dayBlocked = [...blockedConnectorScopes].some((scopeKey) => scopeKey.startsWith(`${dayUtc}|`));
    if (dayBlocked) {
      blockedScopes.push({ day_utc: dayUtc, status: "blocked_dependency", reason: "connector_manifest_dependency_blocked" });
      dayPlans.push({ day_utc: dayUtc, status: "blocked_dependency", manifest_status: "blocked_dependency", index_status: "blocked_dependency", scopes: dayScopes, blocked_scopes: blockedScopes.filter((scope) => scope.dayUtc === dayUtc || scope.day_utc === dayUtc), proposal_keys: [], index: null });
      continue;
    }
    if (needsDay) {
      if (domain === "observations") {
        await stageRepairableObservationConnectorDependencies({
          staged,
          base,
          dayUtc,
          proposalKeys,
        });
      }
      const child = await readChildren({ store: staged.stagedR2.adapter, prefix: `${base}/connector_id=`, dayUtc, kind: "connector", domain });
      dayManifest = buildHistoryV2DayManifest({ domain, grain: domain === "aqilevels" ? "hourly" : null, profile: domain === "aqilevels" ? "data" : null, dayUtc, runId: child.children[0].run_id, manifestKey: dayManifestKey, connectorManifests: child.children, writerGitSha: child.children[0].writer_git_sha, backedUpAtUtc: child.children.map((value) => value.backed_up_at_utc).sort().at(-1) || null });
      await staged.stage({
        key: dayManifestKey,
        body: JSON.stringify(dayManifest, null, 2),
        kind: "day_manifest",
        dayUtc,
        dependencies: [...child.identities.keys()],
        localDependencySnapshot: localDependencySnapshot({
          child,
          proposals: staged.proposals,
          prefix: `${base}/connector_id=`,
          dayUtc,
          connectorId: null,
          kind: "connector",
          domain,
        }),
      });
      proposalKeys.push(dayManifestKey);
    } else {
      const existingDay = localStore.getObjectIfExists(dayManifestKey);
      dayManifest = existingDay ? jsonObject(existingDay, dayManifestKey) : null;
    }

    let index = null;
    const indexRequested = dayScopes.some((scope) => scope.needsIndex);
    // Parent manifests only describe their already-final children.  Their
    // hash is not a dependency of a pollutant timeseries index, so a
    // connector-less day-manifest repair must never expand into every
    // connector/pollutant index for that day.  Every index-producing action
    // has a connector scope; fail closed if that contract is ever broken.
    const indexConnectorIds = [...new Set(dayScopes
      .filter((scope) => scope.needsIndex)
      .map((scope) => Number(scope.connectorId))
      .filter((connectorId) => Number.isInteger(connectorId) && connectorId > 0))]
      .sort((left, right) => left - right);
    if (indexRequested && !indexConnectorIds.length) {
      blockedScopes.push({
        status: "blocked_dependency",
        day_utc: dayUtc,
        reason: "targeted_index_scope_missing_connector",
      });
      dayPlans.push({
        day_utc: dayUtc,
        status: "blocked_dependency",
        manifest_status: manifestStageStatus(proposalKeys),
        index_status: "blocked_dependency",
        scopes: dayScopes,
        blocked_scopes: blockedScopes.filter((scope) => scope.dayUtc === dayUtc || scope.day_utc === dayUtc),
        proposal_keys: [],
        index: null,
      });
      continue;
    }
    if (indexRequested) {
      const proposalSnapshot = new Map(staged.proposals);
      try {
        const results = [];
        for (const connectorId of indexConnectorIds) {
          results.push(await updateIndexes({
            env,
            r2: staged.stagedR2,
            historyVersion: "v2",
            domains: [domain],
            fromDayUtc: dayUtc,
            toDayUtc: dayUtc,
            connectorId,
            generatedAt: stableGeneratedAt({ dayUtc, dayManifest }),
            strictMissingTimeseriesCounts: true,
            writeR2: false,
            additionalPollutantManifestTargets: additionalIndexPollutantTargets.filter((target) =>
              target.day_utc === dayUtc && target.connector_id === connectorId
            ),
          }));
        }
        index = {
          status: "planned",
          connector_ids: indexConnectorIds,
          results,
        };
      } catch (error) {
        staged.proposals.clear();
        for (const [key, proposal] of proposalSnapshot) staged.proposals.set(key, proposal);
        const message = error instanceof Error ? error.message : String(error);
        const parts = message.split("|");
        blockedScopes.push({
          status: "blocked_dependency",
          day_utc: dayUtc,
          reason: parts[1] || "required_index_child_unreadable",
          path: parts[2] || null,
          detail: message,
        });
        dayPlans.push({ day_utc: dayUtc, status: "blocked_dependency", manifest_status: manifestStageStatus(proposalKeys), index_status: "blocked_dependency", scopes: dayScopes, blocked_scopes: blockedScopes.filter((scope) => scope.dayUtc === dayUtc || scope.day_utc === dayUtc), proposal_keys: [], index: null });
        continue;
      }
      for (const key of staged.proposals.keys()) {
        if (!proposalSnapshot.has(key)) proposalKeys.push(key);
      }
    }
    dayPlans.push({
      day_utc: dayUtc,
      status: "planned",
      manifest_status: manifestStageStatus(proposalKeys),
      index_status: indexRequested ? plannedStageStatus : "not_run",
      scopes: dayScopes,
      blocked_scopes: [],
      proposal_keys: [...new Set(proposalKeys)].sort(),
      index,
    });
  }

  // The shared builder plans a latest summary while it is constructing the
  // targeted merge. Apply it last, after all lower-level index
  // proposals have completed their PUT-and-GET verification.
  const latestKeys = new Set(dayPlans.map((plan) => {
    const domainResult = domain === "observations"
      ? plan.index?.observations_timeseries
      : plan.index?.aqilevels_timeseries;
    return domainResult?.latest_index_key;
  }).filter(Boolean));
  for (const key of latestKeys) {
    const proposal = staged.proposals.get(key);
    if (proposal) {
      staged.proposals.delete(key);
      const dependencies = [...staged.proposals.values()]
        .filter((candidate) => candidate.kind === "pollutant_timeseries_index")
        .map((candidate) => candidate.key)
        .sort();
      staged.proposals.set(key, {
        ...proposal,
        kind: "latest_timeseries_index",
        dependencies: [...new Set([...(proposal.dependencies || []), ...dependencies])].sort(),
      });
    }
  }
  reportProgress({
    phase: "metadata_planning_complete",
    completed_objects: staged.proposals.size,
    total_objects: staged.proposals.size,
    blocked_count: blockedScopes.length,
  });
  for (const proposal of staged.proposals.values()) {
    proposal.dependency_identities = staged.resolveDependencyIdentities(proposal.dependencies || []);
  }
  for (const proposal of staged.proposals.values()) assertCanonicalProposal(proposal);
  for (const proposal of staged.proposals.values()) {
    assertCanonicalProposalRelationships(proposal, staged.proposals);
  }
  const proposalViews = [...staged.proposals.values()]
    .map(proposalView)
    .sort((left, right) => left.key.localeCompare(right.key));
  const results = dayPlans.map((plan) => plan.status === "blocked_dependency"
    ? plan
    : { ...plan, status: "planned", verification: { status: "not_run" }, operations: [] });
  const status = blockedScopes.length ? "blocked_dependency" : "planned";
  const ok = status !== "blocked_dependency";
  return {
    ok,
    status,
    dry_run: true,
    write_r2: false,
    manifest_status: reduceRepairStatus(dayPlans.map((plan) => plan.manifest_status || "not_run"), "not_run"),
    index_status: reduceRepairStatus(dayPlans.map((plan) => plan.index_status || "not_run"), "not_run"),
    bucket: config.r2.bucket,
    planning: { status: "planned", input_kind: inputKind, domain, scopes, days: dayPlans, proposals: proposalViews, blocked_scopes: blockedScopes },
    execution: { status: "not_run" },
    verification: { status: "not_run" },
    application_failure: null,
    r2_operation_counts: {
      r2_get_before_put_count: 0,
      r2_head_before_put_count: 0,
      r2_list_before_put_count: 0,
      r2_put_count: 0,
      r2_get_after_put_verification_count: 0,
    },
    application: {
      status: "planned",
      operations: [],
      failure: null,
    },
    results,
  };
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) runV2ObservationsRepair().then((output) => {
  process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
  if (!output.ok) process.exitCode = 1;
}).catch((error) => { process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`); process.exit(1); });

