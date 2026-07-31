#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  hasRequiredR2Config,
  r2DeleteObjects,
  r2GetObject,
  r2ListAllObjects,
  r2PutObject,
  sha256Hex,
} from "../../workers/shared/r2_sigv4.mjs";
import {
  computeEmptyObservationContentHash,
  computeObservationContentHash,
  encodeCanonicalObservationRow,
  normalizeCanonicalObservationRow,
  resolveLegacyVerificationStatus,
  validateObservationContentHashMetadata,
} from "../../workers/shared/uk_aq_observation_content_hash.mjs";
import {
  runCanonicalConnectorDayWriter,
  runCanonicalDayFinalizer,
  runCanonicalGlobalIndexFinalizer,
  withHistoryWriterClient,
  mergeConnectorManifestReferences,
  readParentManifestForBoundedRecovery,
} from "../../workers/shared/uk_aq_r2_history_writer.mjs";
import {
  buildHistoryV2DayManifest,
  validateCanonicalHistoryV2Manifest,
} from "../../workers/shared/uk_aq_r2_history_canonical.mjs";
import {
  resolveR2HistoryIndexConfig,
  updateR2HistoryIndexesTargeted,
} from "../../workers/shared/uk_aq_r2_history_index.mjs";
import {
  compressors,
  parquetMetadataAsync,
  parquetRead,
  parquetSchema,
} from "./lib/uk_aq_parquet_dependencies.mjs";

const TEST_BUCKET = "uk-aq-history-cic-test";

function parseArgs(argv) {
  const args = { runStateJson: "", writeR2: false };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--run-state-json") args.runStateJson = String(argv[++index] || "");
    else if (arg === "--write-r2") args.writeR2 = true;
    else throw new Error(`Unknown arg: ${arg}`);
  }
  if (!args.runStateJson) throw new Error("--run-state-json is required");
  if (!args.writeR2) throw new Error("canonical apply requires --write-r2");
  return args;
}

function safeKey(rawKey) {
  const key = String(rawKey || "").replace(/^\/+/, "");
  if (!key || key.split("/").some((part) => part === "..")) {
    throw new Error(`Unsafe canonical object key: ${rawKey}`);
  }
  return key;
}

function atomicWriteJson(filePath, value) {
  const temporaryPath = `${filePath}.tmp`;
  fs.writeFileSync(temporaryPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  fs.renameSync(temporaryPath, filePath);
}

function contentTypeForKey(key) {
  if (key.endsWith(".json")) return "application/json; charset=utf-8";
  if (key.endsWith(".parquet")) return "application/vnd.apache.parquet";
  return "application/octet-stream";
}

function objectDomain(key) {
  if (key.includes("/aqilevels_") || key.includes("/aqilevels/")) return "aqilevels";
  return "observations";
}

export function publicationRank(key) {
  const value = String(key || "");
  if (/^history\/v2\/observations\/.+\.parquet$/.test(value)) return 10;
  if (/^history\/v2\/observations\/.+\/pollutant_code=[^/]+\/manifest\.json$/.test(value)) return 20;
  if (/^history\/v2\/observations\/.+\/connector_id=\d+\/manifest\.json$/.test(value)) return 30;
  if (/^history\/_index_v2\/observations_.+/.test(value)) return 40;
  if (/^history\/v2\/aqilevels\/.+\.parquet$/.test(value)) return 50;
  if (/^history\/v2\/aqilevels\/.+\/pollutant_code=[^/]+\/manifest\.json$/.test(value)) return 60;
  if (/^history\/v2\/aqilevels\/.+\/connector_id=\d+\/manifest\.json$/.test(value)) return 70;
  if (/^history\/_index_v2\/aqilevels_.+/.test(value)) return 80;
  if (/^history\/v2\/observations\/day_utc=\d{4}-\d{2}-\d{2}\/manifest\.json$/.test(value)) return 90;
  if (/^history\/v2\/aqilevels\/.+\/day_utc=\d{4}-\d{2}-\d{2}\/manifest\.json$/.test(value)) return 100;
  if (value.includes("latest") || value.startsWith("history/_index_v2/")) return 120;
  return 110;
}

export const VERIFIED_GET_CACHE_MAX_BYTES = 64 * 1024 * 1024;
export const VERIFIED_GET_CACHE_MAX_ENTRIES = 32;

export function createVerifiedGetBodyCache({
  maxBytes = VERIFIED_GET_CACHE_MAX_BYTES,
  maxEntries = VERIFIED_GET_CACHE_MAX_ENTRIES,
} = {}) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0
    || !Number.isSafeInteger(maxEntries) || maxEntries <= 0) {
    throw new Error("Verified GET cache bounds must be positive integers");
  }
  const entries = new Map();
  const events = [];
  let totalBytes = 0;
  let peakBytes = 0;
  let peakEntries = 0;
  const record = (event) => {
    events.push(event);
    if (events.length > 50) events.shift();
  };
  const invalidateKey = (key, reason) => {
    let removed = 0;
    for (const [cacheKey, entry] of entries) {
      if (entry.key !== key) continue;
      entries.delete(cacheKey);
      totalBytes -= entry.body.byteLength;
      removed += 1;
    }
    if (removed) record({ key, reason, invalidated_entries: removed });
    return removed;
  };
  const evictOldest = (reason) => {
    const oldest = entries.entries().next().value;
    if (!oldest) return false;
    const [cacheKey, entry] = oldest;
    entries.delete(cacheKey);
    totalBytes -= entry.body.byteLength;
    record({ key: entry.key, verified_sha256: entry.sha256, reason });
    return true;
  };
  return {
    maxBytes,
    maxEntries,
    store({ key, sha256, body }) {
      const normalizedKey = safeKey(key);
      const normalizedSha = String(sha256 || "").toLowerCase();
      const buffer = Buffer.isBuffer(body) ? body : Buffer.from(body);
      if (!/^[a-f0-9]{64}$/.test(normalizedSha) || sha256Hex(buffer) !== normalizedSha) {
        throw new Error(`Refusing unverified body cache entry: ${normalizedKey}`);
      }
      invalidateKey(normalizedKey, "later_put_same_key");
      if (buffer.byteLength > maxBytes) {
        record({ key: normalizedKey, verified_sha256: normalizedSha, reason: "body_exceeds_cache_bound" });
        return false;
      }
      while (entries.size >= maxEntries || totalBytes + buffer.byteLength > maxBytes) {
        if (!evictOldest("bounded_cache_eviction")) break;
      }
      entries.set(`${normalizedKey}\u0000${normalizedSha}`, {
        key: normalizedKey,
        sha256: normalizedSha,
        body: buffer,
      });
      totalBytes += buffer.byteLength;
      peakBytes = Math.max(peakBytes, totalBytes);
      peakEntries = Math.max(peakEntries, entries.size);
      return true;
    },
    get(key, expectedSha256) {
      const normalizedKey = safeKey(key);
      const normalizedSha = String(expectedSha256 || "").toLowerCase();
      const entry = entries.get(`${normalizedKey}\u0000${normalizedSha}`);
      return entry && sha256Hex(entry.body) === normalizedSha ? entry.body : null;
    },
    invalidateKey,
    invalidatePrefix(prefix, reason) {
      const normalizedPrefix = `${safeKey(prefix).replace(/\/+$/, "")}/`;
      let removed = 0;
      for (const entry of [...entries.values()]) {
        if (entry.key.startsWith(normalizedPrefix)) {
          removed += invalidateKey(entry.key, reason);
        }
      }
      return removed;
    },
    clear(reason = "connector_day_scope_complete") {
      for (const entry of [...entries.values()]) invalidateKey(entry.key, reason);
    },
    snapshot() {
      return {
        max_bytes: maxBytes,
        max_entries: maxEntries,
        current_bytes: totalBytes,
        current_entries: entries.size,
        peak_bytes: peakBytes,
        peak_entries: peakEntries,
        recent_events: [...events],
      };
    },
  };
}

const OBSERVATION_INTEGRITY_POLLUTANTS = new Set(["pm25", "pm10", "no2", "o3"]);
const AQI_INTEGRITY_POLLUTANTS = new Set(["pm25", "pm10", "no2"]);
const CANONICAL_CONNECTOR_DAY_PREFIX_PATTERNS = Object.freeze([
  /^history\/v2\/observations\/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)$/,
  /^history\/v2\/aqilevels\/hourly\/data\/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)$/,
  /^history\/v2\/aqilevels\/hourly\/debug\/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)$/,
]);
const CANONICAL_OBSERVATION_POLLUTANT_PREFIX_PATTERN =
  /^history\/v2\/observations\/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)\/pollutant_code=([a-z0-9_]+)$/;
const CANONICAL_AQI_POLLUTANT_PREFIX_PATTERN =
  /^history\/v2\/aqilevels\/hourly\/(data|debug)\/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)\/pollutant_code=([a-z0-9_]+)$/;
const CANONICAL_OBSERVATION_POLLUTANT_MANIFEST_PATTERN =
  /^history\/v2\/observations\/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)\/pollutant_code=([a-z0-9_]+)\/manifest\.json$/;

function validateDeletionDayConnector({ prefix, dayUtc, connectorIdRaw }) {
  const parsedDay = new Date(`${dayUtc}T00:00:00.000Z`);
  if (Number.isNaN(parsedDay.getTime()) || parsedDay.toISOString().slice(0, 10) !== dayUtc) {
    throw new Error(`Deletion prefix has an invalid UTC day: ${prefix}`);
  }
  const connectorId = Number(connectorIdRaw);
  if (!Number.isSafeInteger(connectorId) || connectorId <= 0 || String(connectorId) !== connectorIdRaw) {
    throw new Error(`Deletion prefix has an invalid connector ID: ${prefix}`);
  }
}

function assertCanonicalDeletionPrefix(prefix, entry) {
  const observationPollutantMatch = prefix.match(CANONICAL_OBSERVATION_POLLUTANT_PREFIX_PATTERN);
  const aqiPollutantMatch = prefix.match(CANONICAL_AQI_POLLUTANT_PREFIX_PATTERN);
  const pollutantMatch = observationPollutantMatch || aqiPollutantMatch;
  if (pollutantMatch) {
    const isObservation = Boolean(observationPollutantMatch);
    const [, ...parts] = pollutantMatch;
    const [dayUtc, connectorIdRaw, pollutant] = isObservation ? parts : parts.slice(1);
    const supportedPollutants = isObservation
      ? OBSERVATION_INTEGRITY_POLLUTANTS
      : AQI_INTEGRITY_POLLUTANTS;
    const domainName = isObservation ? "Observation" : "AQI";
    validateDeletionDayConnector({ prefix, dayUtc, connectorIdRaw });
    if (!supportedPollutants.has(pollutant)) {
      throw new Error(`${domainName} deletion prefix has an unsupported pollutant: ${prefix}`);
    }
    const repairPollutants = Array.isArray(entry?.repair_pollutants)
      ? entry.repair_pollutants.map((value) => String(value || "").trim().toLowerCase()).filter(Boolean).sort()
      : [];
    if (!repairPollutants.includes(pollutant) || repairPollutants.some((value) => !supportedPollutants.has(value))) {
      throw new Error(`${domainName} pollutant deletion prefix is not backed by matching repair_pollutants evidence: ${prefix}`);
    }
    return;
  }
  const match = CANONICAL_CONNECTOR_DAY_PREFIX_PATTERNS
    .map((pattern) => prefix.match(pattern))
    .find(Boolean);
  if (!match) {
    throw new Error(`Deletion prefix is outside the canonical allowlist: ${prefix}`);
  }
  const [, dayUtc, connectorIdRaw] = match;
  validateDeletionDayConnector({ prefix, dayUtc, connectorIdRaw });
  if (Array.isArray(entry?.repair_pollutants) && entry.repair_pollutants.length > 0) {
    throw new Error(`Pollutant-scoped repair cannot delete a connector-day prefix: ${prefix}`);
  }
}

function dependencyIdentity(entry, dependencyKey) {
  const identities = entry?.dependency_identities;
  if (!identities || typeof identities !== "object" || Array.isArray(identities)) return null;
  const identity = identities[dependencyKey];
  if (!identity || typeof identity !== "object") return null;
  const sha256 = String(identity.sha256 || "").trim().toLowerCase();
  const bytes = Number(identity.bytes);
  if (!/^[a-f0-9]{64}$/.test(sha256) || !Number.isSafeInteger(bytes) || bytes < 0) return null;
  const source = String(identity.source || "");
  if (!source) return null;
  return { sha256, bytes, source };
}

export function validateLocalProposal(runState) {
  if (!runState || typeof runState !== "object") throw new Error("run state must be an object");
  if (runState.environment !== "CIC-Test") {
    throw new Error(`Refusing canonical apply outside CIC-Test: ${runState.environment || "(unset)"}`);
  }
  const objects = Object.entries(runState.objects || {}).sort(([left], [right]) => left.localeCompare(right));
  const prefixes = Array.isArray(runState.tombstone_prefixes) ? runState.tombstone_prefixes : [];
  if (!objects.length && !prefixes.length) throw new Error("canonical proposal has no planned operations");
  const normalizedObjects = [];
  const proposedPrefixes = prefixes
    .filter((entry) => entry?.proposed)
    .map((entry) => `${safeKey(entry.prefix).replace(/\/+$/, "")}/`);
  for (const [rawKey, entry] of objects) {
    const key = safeKey(rawKey);
    if (!(key.startsWith("history/v2/") || key.startsWith("history/_index_v2/"))
      || /\/(?:generation(?:=)|transactions\/)/.test(`/${key}`)) {
      throw new Error(`Non-canonical Integrity proposal key: ${key}`);
    }
    if (!entry?.proposed || !entry?.built || !entry?.structurally_validated) {
      throw new Error(`Local structural validation is incomplete: ${key}`);
    }
    const localPath = String(entry.local_path || "");
    if (!localPath || !fs.statSync(localPath, { throwIfNoEntry: false })?.isFile()) {
      throw new Error(`Local proposal body is missing: ${key}`);
    }
    const body = fs.readFileSync(localPath);
    if (body.byteLength !== Number(entry.bytes) || sha256Hex(body) !== entry.sha256) {
      throw new Error(`Local proposal identity changed after validation: ${key}`);
    }
    for (const dependency of entry.dependencies || []) {
      const dependencyKey = safeKey(dependency);
      const stagedDependency = runState.objects?.[dependencyKey];
      if (stagedDependency) {
        if (!stagedDependency.structurally_validated
          || !fs.statSync(String(stagedDependency.local_path || ""), { throwIfNoEntry: false })?.isFile()) {
          throw new Error(`Local proposal dependency is not structurally validated: ${key} -> ${dependencyKey}`);
        }
        const expectedIdentity = dependencyIdentity(entry, dependencyKey);
        const dependencyBody = fs.readFileSync(String(stagedDependency.local_path));
        if (!expectedIdentity
          || !["planned_overlay", "overlay"].includes(expectedIdentity.source)
          || dependencyBody.byteLength !== expectedIdentity.bytes
          || sha256Hex(dependencyBody) !== expectedIdentity.sha256) {
          throw new Error(`Staged current-run dependency identity is invalid: ${key} -> ${dependencyKey}`);
        }
      } else {
        const expectedIdentity = dependencyIdentity(entry, dependencyKey);
        if (!expectedIdentity || expectedIdentity.source !== "dropbox") {
          throw new Error(`Dropbox baseline dependency identity is not pinned: ${key} -> ${dependencyKey}`);
        }
        if (proposedPrefixes.some((prefix) => dependencyKey.startsWith(prefix))) {
          throw new Error(`Proposed deletion would remove an unstaged dependency: ${key} -> ${dependencyKey}`);
        }
        const baselinePath = path.join(String(runState.base_dropbox_root || ""), dependencyKey);
        if (!fs.statSync(baselinePath, { throwIfNoEntry: false })?.isFile()) {
          throw new Error(`Dropbox baseline dependency is unavailable: ${key} -> ${dependencyKey}`);
        }
        const baselineBody = fs.readFileSync(baselinePath);
        if (baselineBody.byteLength !== expectedIdentity.bytes || sha256Hex(baselineBody) !== expectedIdentity.sha256) {
          throw new Error(`Dropbox baseline dependency identity changed after planning: ${key} -> ${dependencyKey}`);
        }
      }
    }
    normalizedObjects.push({ key, entry, localPath, body, domain: objectDomain(key) });
  }
  const normalizedPrefixes = prefixes.map((entry) => {
    const prefix = safeKey(entry?.prefix).replace(/\/+$/, "");
    if (!entry?.proposed) throw new Error(`Deletion prefix is not proposed: ${prefix}`);
    assertCanonicalDeletionPrefix(prefix, entry);
    return { entry, prefix, domain: objectDomain(prefix) };
  });
  const scopedPollutantGroups = new Map();
  for (const item of normalizedPrefixes) {
    const observationMatch = item.prefix.match(CANONICAL_OBSERVATION_POLLUTANT_PREFIX_PATTERN);
    const aqiMatch = item.prefix.match(CANONICAL_AQI_POLLUTANT_PREFIX_PATTERN);
    const match = observationMatch || aqiMatch;
    if (!match) continue;
    const isObservation = Boolean(observationMatch);
    const [, ...parts] = match;
    const [dayUtc, connectorIdRaw, pollutant] = isObservation ? parts : parts.slice(1);
    const scopeName = isObservation ? "observations" : `aqilevels/${parts[0]}`;
    const repairPollutants = Array.isArray(item.entry?.repair_pollutants)
      ? item.entry.repair_pollutants.map((value) => String(value || "").trim().toLowerCase()).filter(Boolean).sort()
      : [];
    const groupKey = `${scopeName}|${dayUtc}|${connectorIdRaw}|${repairPollutants.join(",")}`;
    const group = scopedPollutantGroups.get(groupKey) || { repairPollutants, prefixes: new Map() };
    group.prefixes.set(pollutant, (group.prefixes.get(pollutant) || 0) + 1);
    scopedPollutantGroups.set(groupKey, group);
  }
  for (const [groupKey, group] of scopedPollutantGroups.entries()) {
    for (const pollutant of group.repairPollutants) {
      if (group.prefixes.get(pollutant) !== 1) {
        throw new Error(`Pollutant-scoped repair requires exactly one deletion prefix for ${pollutant}: ${groupKey}`);
      }
    }
  }
  return {
    objects: normalizedObjects.sort((left, right) => publicationRank(left.key) - publicationRank(right.key) || left.key.localeCompare(right.key)),
    prefixes: normalizedPrefixes.sort((left, right) => left.domain.localeCompare(right.domain) || left.prefix.localeCompare(right.prefix)),
  };
}

export function validateDedicatedSosHistoricalProposal({ runState, proposal }) {
  if (runState?.execution_path !== "dedicated_sos_historical_observation_replacement") {
    return { dedicated: false };
  }
  if (runState.environment !== "CIC-Test"
    || JSON.stringify(runState.mutation_connector_ids) !== "[1]"
    || runState.aqi_policy !== "bypassed_observation_history_only") {
    throw new Error("Dedicated SOS proposal has invalid execution-scope evidence");
  }
  const aqiScopeSets = ["AQILEVELS_CHANGED", "AQI_MANIFESTS_CHANGED", "AQI_INDEXES_CHANGED"];
  if (aqiScopeSets.some((scope) => (runState.changed_scopes?.[scope] || []).length > 0)) {
    throw new Error("Dedicated SOS proposal must not contain AQI changed scopes");
  }
  for (const item of proposal.prefixes) {
    const match = item.prefix.match(CANONICAL_OBSERVATION_POLLUTANT_PREFIX_PATTERN);
    if (!match || Number(match[2]) !== 1 || item.domain !== "observations") {
      throw new Error(`Dedicated SOS deletion is outside connector-1 observations: ${item.prefix}`);
    }
  }
  const connectorDayParquet = new Map();
  for (const object of proposal.objects) {
    if (object.domain !== "observations" || object.key.includes("aqilevels")) {
      throw new Error(`Dedicated SOS proposal contains an AQI object: ${object.key}`);
    }
    const connectorMatch = object.key.match(/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)/);
    if (connectorMatch && Number(connectorMatch[2]) !== 1) {
      throw new Error(`Dedicated SOS proposal contains a non-connector-1 mutation: ${object.key}`);
    }
    if (connectorMatch && object.key.endsWith(".parquet")) {
      const groupKey = `${connectorMatch[1]}|${connectorMatch[2]}`;
      const group = connectorDayParquet.get(groupKey) || { bytes: 0, entries: 0 };
      group.bytes += object.body.byteLength;
      group.entries += 1;
      connectorDayParquet.set(groupKey, group);
    }
  }
  for (const [groupKey, group] of connectorDayParquet) {
    if (group.bytes > VERIFIED_GET_CACHE_MAX_BYTES || group.entries > VERIFIED_GET_CACHE_MAX_ENTRIES) {
      throw new Error(
        `Dedicated SOS verified-body cache capacity exceeded before mutation: ${groupKey} bytes=${group.bytes} entries=${group.entries}`,
      );
    }
  }
  return {
    dedicated: true,
    connector_id: 1,
    observation_object_count: proposal.objects.length,
    exact_pollutant_prefix_count: proposal.prefixes.length,
    verified_body_cache_capacity_preflight: "succeeded",
  };
}

async function deleteAndVerifyPrefix({ r2, runState, runStatePath, prefixEntry, adapters, verifiedBodyCache = null }) {
  const prefix = `${prefixEntry.prefix}/`;
  verifiedBodyCache?.invalidatePrefix(prefixEntry.prefix, "delete_prefix");
  prefixEntry.entry.remote_attempted = true;
  prefixEntry.entry.status = "deleting";
  atomicWriteJson(runStatePath, runState);
  try {
    const entries = await adapters.listAllObjects({ r2, prefix, max_keys: 1000 });
    const keys = entries.map((entry) => safeKey(entry.key)).filter((key) => key.startsWith(prefix)).sort();
    for (let index = 0; index < keys.length; index += 1000) {
      const batch = keys.slice(index, index + 1000);
      const result = await adapters.deleteObjects({ r2, keys: batch });
      if (Array.isArray(result?.errors) && result.errors.length) {
        throw new Error(`R2 prefix delete returned errors for ${prefixEntry.prefix}: ${JSON.stringify(result.errors)}`);
      }
    }
    const remaining = await adapters.listAllObjects({ r2, prefix, max_keys: 1000 });
    if (remaining.length) throw new Error(`R2 prefix deletion verification failed: ${prefixEntry.prefix}`);
    Object.assign(prefixEntry.entry, {
      deleted: true,
      deletion_verified: true,
      remote_completed: true,
      completed_at_utc: new Date().toISOString(),
      deleted_object_count: keys.length,
      deleted_object_keys: keys,
      status: "deletion_verified",
    });
    atomicWriteJson(runStatePath, runState);
    return keys.length;
  } catch (error) {
    Object.assign(prefixEntry.entry, {
      status: "failed",
      error: error instanceof Error ? error.message : String(error),
    });
    atomicWriteJson(runStatePath, runState);
    throw error;
  }
}

export async function putAndVerifyObject({ r2, runState, runStatePath, object, adapters, verifiedBodyCache = null }) {
  const entry = object.entry;
  if (Number(entry.post_put_verification_get_attempt_count || 0) !== 0
    || Number(entry.post_put_verification_get_count || 0) !== 0) {
    throw new Error(`Changed object already has post-PUT GET bookkeeping: ${object.key}`);
  }
  verifiedBodyCache?.invalidateKey(object.key, "later_put_same_key");
  Object.assign(entry, { remote_attempted: true, status: "uploading" });
  atomicWriteJson(runStatePath, runState);
  try {
    await adapters.putObject({ r2, key: object.key, body: object.body, content_type: contentTypeForKey(object.key) });
    Object.assign(entry, { uploaded: true, uploaded_at_utc: new Date().toISOString(), status: "uploaded" });
    atomicWriteJson(runStatePath, runState);
    entry.post_put_verification_get_attempt_count = 1;
    atomicWriteJson(runStatePath, runState);
    const fresh = await adapters.getObject({ r2, key: object.key });
    if (Number(fresh.bytes) !== object.body.byteLength || sha256Hex(fresh.body) !== entry.sha256) {
      throw new Error(`R2 GET verification identity mismatch: ${object.key}`);
    }
    Object.assign(entry, {
      r2_verified: true,
      r2_verified_at_utc: new Date().toISOString(),
      remote_completed: true,
      status: "get_verified",
      post_put_verification_get_count: 1,
    });
    if (/^history\/v2\/observations\/.+\.parquet$/.test(object.key)) {
      const cached = verifiedBodyCache?.store({
        key: object.key,
        sha256: entry.sha256,
        body: fresh.body,
      }) || false;
      Object.assign(entry, {
        verified_get_body_cached: cached,
        verified_get_cache_key: cached ? object.key : null,
        verified_get_cache_sha256: cached ? entry.sha256 : null,
      });
    }
    atomicWriteJson(runStatePath, runState);
  } catch (error) {
    Object.assign(entry, {
      status: "failed",
      error: error instanceof Error ? error.message : String(error),
    });
    atomicWriteJson(runStatePath, runState);
    throw error;
  }
}

function parquetIso(value) {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new Error("Invalid observation Parquet timestamp");
    return value.toISOString();
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new Error("Invalid observation Parquet timestamp");
  return parsed.toISOString();
}

async function readCanonicalObservationRows({ body, connectorId }) {
  const file = new Uint8Array(body).slice().buffer;
  const metadata = await parquetMetadataAsync(file);
  const rowCount = Number(metadata.num_rows || 0);
  if (!Number.isSafeInteger(rowCount) || rowCount <= 0) {
    throw new Error("Repaired observation Parquet must contain rows");
  }
  const schemaColumns = new Set(
    parquetSchema(metadata).children.map((column) => String(column.element.name)),
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
  if (missing.length) {
    throw new Error(`Repaired observation Parquet is missing canonical columns: ${missing.join(",")}`);
  }
  const statusColumn = schemaColumns.has("verification_status")
    ? "verification_status"
    : schemaColumns.has("status")
    ? "status"
    : null;
  const columns = [...required, ...(statusColumn ? [statusColumn] : [])];
  let decodedRows = [];
  await parquetRead({
    file,
    metadata,
    columns,
    rowStart: 0,
    rowEnd: rowCount,
    compressors,
    onComplete: (rows) => {
      decodedRows = Array.isArray(rows) ? rows : [];
    },
  });
  if (decodedRows.length !== rowCount) {
    throw new Error("Repaired observation Parquet row count changed while reading");
  }
  const sosConnectorId = Number.parseInt(
    process.env.UK_AQ_BACKFILL_SOS_CONNECTOR_ID_FALLBACK || "1",
    10,
  );
  return decodedRows.map((values) => {
    if (!Array.isArray(values)) throw new Error("Invalid repaired observation Parquet row");
    const statusRow = statusColumn
      ? { [statusColumn]: values[required.length] ?? null }
      : {};
    return normalizeCanonicalObservationRow({
      connector_id: Number(values[0]),
      station_id: values[1] === null || values[1] === undefined
        ? null
        : Number(values[1]),
      timeseries_id: Number(values[2]),
      pollutant_code: values[3],
      observed_at_utc: parquetIso(values[4]),
      value: Number(values[5]),
      verification_status: resolveLegacyVerificationStatus(statusRow, {
        isSos: connectorId === sosConnectorId,
      }),
    });
  });
}

function timeseriesRowCounts(rows) {
  const counts = {};
  for (const row of rows) {
    const key = String(row.timeseries_id);
    counts[key] = (counts[key] || 0) + 1;
  }
  return Object.fromEntries(Object.entries(counts).sort(([left], [right]) => Number(left) - Number(right)));
}

function sameJson(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function semanticMetadata(value) {
  return {
    observation_content_hash: value.observation_content_hash,
    observation_content_hash_algorithm: value.observation_content_hash_algorithm,
    observation_content_hash_contract_version: value.observation_content_hash_contract_version,
    observation_content_hash_row_count: value.observation_content_hash_row_count,
    observation_content_hash_columns: value.observation_content_hash_columns,
    verification_status_counts: value.verification_status_counts,
  };
}

function semanticDifferences(left, right) {
  const fields = [
    "observation_content_hash",
    "observation_content_hash_algorithm",
    "observation_content_hash_contract_version",
    "observation_content_hash_row_count",
    "observation_content_hash_columns",
    "verification_status_counts",
  ];
  return fields.filter((field) => !sameJson(left?.[field], right?.[field]));
}

function proposalOwners(object) {
  const values = [
    object?.entry?.proposal_owner,
    object?.entry?.source,
    object?.entry?.provenance?.source,
  ].map((value) => String(value || "").trim()).filter(Boolean);
  return [...new Set(values.length ? values : ["final_metadata_proposal"])];
}

function finalProposalError({ key, object, differingFields, compatibilitySource = null }) {
  const suffix = compatibilitySource ? ` compatibility_source=${compatibilitySource}` : "";
  return new Error(
    `Final Integrity proposal graph mismatch: key=${key} owners=${proposalOwners(object).join(",")} differing_fields=${[...new Set(differingFields)].join(",")}${suffix}`,
  );
}

function parseFinalJsonBody({ key, object, body, differingField }) {
  try {
    return JSON.parse(body.toString("utf8"));
  } catch {
    throw finalProposalError({ key, object, differingFields: [differingField] });
  }
}

function sourceEvidencePaths(runState, dayUtc, connectorId, pollutantCode) {
  const configuredRoot = String(runState.overlay_root || "").trim();
  if (!configuredRoot) throw new Error("Final proposal graph has no overlay root");
  const root = path.resolve(configuredRoot);
  const identity = `day_utc=${dayUtc}/connector_id=${connectorId}/pollutant_code=${pollutantCode}`;
  const retained = runState.source_evidence_partitions?.[identity];
  if (retained) {
    const expectedDirectory = path.join(
      root,
      "source-evidence",
      `day_utc=${dayUtc}`,
      `connector_id=${connectorId}`,
      `pollutant_code=${pollutantCode}`,
    );
    const evidencePath = path.resolve(String(retained.evidence_path || ""));
    const rowsPath = path.resolve(String(retained.rows_path || ""));
    if (retained.identity !== identity
      || retained.day_utc !== dayUtc
      || Number(retained.connector_id) !== connectorId
      || retained.pollutant_code !== pollutantCode
      || evidencePath !== path.join(expectedDirectory, "source-evidence.json")
      || rowsPath !== path.join(expectedDirectory, "obs_history_rows.json")) {
      throw new Error(`Pollutant-scoped source evidence identity is invalid: ${identity}`);
    }
    return {
      evidencePath,
      rowsPath,
      identity,
      pollutantScoped: true,
      retainedEvidenceSha256: String(retained.evidence_sha256 || ""),
      retainedRowsSha256: String(retained.rows_sha256 || ""),
    };
  }
  if (runState.execution_path === "dedicated_sos_historical_observation_replacement") {
    throw new Error(`Pollutant-scoped source evidence is missing: ${identity}`);
  }
  const directory = path.join(root, `day_utc=${dayUtc}`, `connector_id=${connectorId}`);
  return {
    evidencePath: path.join(directory, "source-evidence.json"),
    rowsPath: path.join(directory, "obs_history_rows.json"),
    identity: `day_utc=${dayUtc}/connector_id=${connectorId}`,
    pollutantScoped: false,
    retainedEvidenceSha256: null,
    retainedRowsSha256: null,
  };
}

function loadImmutableSourcePartition({ runState, dayUtc, connectorId, pollutantCode }) {
  const {
    evidencePath,
    rowsPath,
    identity,
    pollutantScoped,
    retainedEvidenceSha256,
    retainedRowsSha256,
  } = sourceEvidencePaths(runState, dayUtc, connectorId, pollutantCode);
  const evidenceBody = fs.readFileSync(evidencePath);
  const rowsBody = fs.readFileSync(rowsPath);
  if (pollutantScoped && (
    sha256Hex(evidenceBody) !== retainedEvidenceSha256
    || sha256Hex(rowsBody) !== retainedRowsSha256
  )) {
    throw new Error(`Pollutant-scoped source evidence changed after capture: ${identity}`);
  }
  const evidence = JSON.parse(evidenceBody.toString("utf8"));
  const rows = JSON.parse(rowsBody.toString("utf8"));
  if (evidence?.schema_version !== 1
    || evidence?.enumeration_complete !== true
    || evidence?.day_utc !== dayUtc
    || Number(evidence?.connector_id) !== connectorId
    || !Array.isArray(rows)
    || rowsBody.byteLength !== Number(evidence.canonical_rows_bytes)
    || sha256Hex(rowsBody) !== evidence.canonical_rows_sha256
    || rows.length !== Number(evidence.total_rows)) {
    throw new Error(`Immutable source evidence identity is invalid: day=${dayUtc} connector=${connectorId}`);
  }
  if (pollutantScoped && !sameJson(
    [...new Set((evidence.requested_pollutant_set || []).map(String))].sort(),
    [pollutantCode],
  )) {
    throw new Error(`Immutable source evidence selected pollutant is invalid: ${identity}`);
  }
  const reconstructedRows = rows.map((row, index) => {
    if (!row || typeof row !== "object" || Array.isArray(row)) {
      throw new Error(`Immutable source evidence row is invalid: day=${dayUtc} connector=${connectorId} row=${index}`);
    }
    if (Object.hasOwn(row, "connector_id") && Number(row.connector_id) !== connectorId) {
      throw new Error(`Immutable source evidence row has conflicting connector_id: day=${dayUtc} connector=${connectorId} row=${index}`);
    }
    if (typeof row.observed_at !== "string" || !row.observed_at.trim()) {
      throw new Error(`Immutable source evidence observed_at is missing or invalid: day=${dayUtc} connector=${connectorId} row=${index}`);
    }
    const observedAt = new Date(row.observed_at);
    if (Number.isNaN(observedAt.getTime())) {
      throw new Error(`Immutable source evidence observed_at is missing or invalid: day=${dayUtc} connector=${connectorId} row=${index}`);
    }
    const observedAtUtc = observedAt.toISOString();
    if (observedAtUtc.slice(0, 10) !== dayUtc) {
      throw new Error(`Immutable source evidence observed_at is outside selected UTC day: day=${dayUtc} connector=${connectorId} row=${index}`);
    }
    return normalizeCanonicalObservationRow({
      connector_id: connectorId,
      station_id: row.station_id,
      timeseries_id: row.timeseries_id,
      pollutant_code: row.pollutant_code,
      observed_at_utc: observedAtUtc,
      value: row.value,
      verification_status: row.verification_status,
    });
  });
  const reconstructedByPollutant = new Map();
  for (const row of reconstructedRows) {
    const partitionRows = reconstructedByPollutant.get(row.pollutant_code) || [];
    partitionRows.push(row);
    reconstructedByPollutant.set(row.pollutant_code, partitionRows);
  }
  const reconstructedCounts = Object.fromEntries([...reconstructedByPollutant.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([code, partitionRows]) => [code, partitionRows.length]));
  const recordedCounts = Object.fromEntries(Object.entries(evidence?.per_pollutant_counts || {})
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([code, count]) => [code, Number(count)]));
  if (!sameJson(reconstructedCounts, recordedCounts)) {
    throw new Error(`Immutable source evidence per-pollutant counts changed: day=${dayUtc} connector=${connectorId}`);
  }
  const recordedHashCodes = Object.keys(evidence?.observation_content_hashes || {}).sort();
  if (!sameJson(Object.keys(reconstructedCounts), recordedHashCodes)) {
    throw new Error(`Immutable source evidence content-hash partitions changed: day=${dayUtc} connector=${connectorId}`);
  }
  for (const [code, partitionRows] of reconstructedByPollutant) {
    const computedPartition = computeObservationContentHash(partitionRows);
    const recordedPartition = evidence?.observation_content_hashes?.[code];
    validateObservationContentHashMetadata(recordedPartition, { rowCount: partitionRows.length });
    const partitionDifferences = semanticDifferences(computedPartition, recordedPartition);
    if (partitionDifferences.length) {
      throw new Error(
        `Immutable source evidence semantic identity changed: day=${dayUtc} connector=${connectorId} pollutant=${code} differing_fields=${partitionDifferences.join(",")}`,
      );
    }
  }
  const selectedRows = reconstructedByPollutant.get(pollutantCode) || [];
  const missingBindingRows = Number(evidence.missing_binding_rows || 0);
  if (!selectedRows.length && missingBindingRows > 0) {
    throw new Error(`Immutable source evidence is all-unmapped rather than authoritative no-data: ${identity}`);
  }
  const computed = selectedRows.length
    ? computeObservationContentHash(selectedRows)
    : computeEmptyObservationContentHash();
  const recorded = evidence?.observation_content_hashes?.[pollutantCode];
  if (selectedRows.length || recorded != null) {
    validateObservationContentHashMetadata(recorded, { rowCount: selectedRows.length });
    const differingFields = semanticDifferences(computed, recorded);
    if (differingFields.length) {
      throw new Error(
        `Immutable source evidence semantic identity changed: day=${dayUtc} connector=${connectorId} pollutant=${pollutantCode} differing_fields=${differingFields.join(",")}`,
      );
    }
  }
  return {
    evidence,
    evidencePath,
    rowsPath,
    evidenceSha256: sha256Hex(evidenceBody),
    rowsSha256: sha256Hex(rowsBody),
    rows: selectedRows,
    identity,
    encodedRows: selectedRows.map(encodeCanonicalObservationRow).sort(),
    metadata: semanticMetadata(computed),
    timeseriesRowCounts: timeseriesRowCounts(selectedRows),
  };
}

function parseManifestObject(object, expectedKind) {
  const payload = parseFinalJsonBody({
    key: object.key,
    object,
    body: object.body,
    differingField: "manifest_json",
  });
  try {
    validateCanonicalHistoryV2Manifest(payload, {
      history_version: "v2",
      domain: payload.domain,
      grain: payload.grain ?? null,
      profile: payload.profile ?? null,
      manifest_kind: expectedKind,
      day_utc: payload.day_utc,
      connector_id: payload.connector_id ?? undefined,
      pollutant_code: payload.pollutant_code ?? undefined,
      manifest_key: object.key,
    });
  } catch {
    throw finalProposalError({ key: object.key, object, differingFields: ["manifest_contract"] });
  }
  return payload;
}

function validateFinalParentReferences({ proposal, runState }) {
  const objects = new Map(proposal.objects.map((object) => [object.key, object]));
  for (const object of proposal.objects) {
    if (!object.key.endsWith("/manifest.json") || object.key.startsWith("history/_index_v2/")) continue;
    const expectedParentKind = /\/connector_id=\d+\/manifest\.json$/.test(object.key)
      ? "connector"
      : /\/day_utc=\d{4}-\d{2}-\d{2}\/manifest\.json$/.test(object.key)
      ? "day"
      : null;
    if (!expectedParentKind) continue;
    const payload = parseManifestObject(object, expectedParentKind);
    const rawReferences = payload.manifest_kind === "connector"
      ? [...(payload.pollutant_manifests || []), ...(payload.child_manifests || [])]
      : [...(payload.connector_manifests || []), ...(payload.child_manifests || [])];
    const references = new Map();
    for (const reference of rawReferences) {
      const key = String(reference?.manifest_key || "");
      if (!key) continue;
      const prior = references.get(key);
      if (prior && prior.manifest_hash !== reference.manifest_hash) {
        throw finalProposalError({ key: object.key, object, differingFields: [`duplicate_child_identity:${key}`] });
      }
      references.set(key, reference);
    }
    const dependencies = new Set((object.entry.dependencies || []).map(String));
    const expectedChildKeys = object.entry?.local_dependency_snapshot?.expected_child_keys;
    if (Array.isArray(expectedChildKeys)
      && !sameJson([...new Set(expectedChildKeys.map(String))].sort(), [...references.keys()].sort())) {
      throw finalProposalError({
        key: object.key,
        object,
        differingFields: ["preserved_child_accounting"],
      });
    }
    for (const [key, reference] of references) {
      if (!dependencies.has(key)) {
        throw finalProposalError({ key: object.key, object, differingFields: [`missing_child_dependency:${key}`] });
      }
      const child = objects.get(key);
      const identity = dependencyIdentity(object.entry, key);
      let childPayload;
      if (child) {
        childPayload = parseFinalJsonBody({
          key: child.key,
          object: child,
          body: child.body,
          differingField: "child_manifest_json",
        });
      } else {
        const baselinePath = path.join(String(runState.base_dropbox_root || ""), key);
        const baselineBody = fs.readFileSync(baselinePath);
        childPayload = parseFinalJsonBody({
          key,
          object,
          body: baselineBody,
          differingField: `preserved_child_json:${key}`,
        });
        if (!identity || identity.source !== "dropbox"
          || identity.sha256 !== sha256Hex(baselineBody)
          || identity.bytes !== baselineBody.byteLength) {
          throw finalProposalError({ key: object.key, object, differingFields: [`preserved_child_identity:${key}`] });
        }
      }
      if (reference.manifest_hash !== childPayload.manifest_hash) {
        throw finalProposalError({ key: object.key, object, differingFields: [`child_manifest_hash:${key}`] });
      }
      if (child && (!identity || identity.sha256 !== child.entry.sha256 || identity.bytes !== child.body.byteLength
        || !["overlay", "planned_overlay"].includes(identity.source))) {
        throw finalProposalError({ key: object.key, object, differingFields: [`child_dependency_identity:${key}`] });
      }
    }
    for (const dependency of dependencies) {
      if (dependency.endsWith("/manifest.json") && !references.has(dependency)) {
        throw finalProposalError({ key: object.key, object, differingFields: [`unrepresented_child_dependency:${dependency}`] });
      }
    }
  }
  for (const object of proposal.objects.filter((candidate) => candidate.key.startsWith("history/_index_v2/")
    && candidate.key.includes("/day_utc=") && candidate.key.includes("/connector_id=")
    && candidate.key.includes("/pollutant_code="))) {
    const observation = object.key.match(/^history\/_index_v2\/observations_timeseries\/day_utc=([^/]+)\/connector_id=([^/]+)\/pollutant_code=([^/]+)\/manifest\.json$/);
    const aqi = object.key.match(/^history\/_index_v2\/aqilevels_hourly_data_timeseries\/day_utc=([^/]+)\/connector_id=([^/]+)\/pollutant_code=([^/]+)\/manifest\.json$/);
    const match = observation || aqi;
    if (!match) continue;
    const manifestKey = observation
      ? `history/v2/observations/day_utc=${match[1]}/connector_id=${match[2]}/pollutant_code=${match[3]}/manifest.json`
      : `history/v2/aqilevels/hourly/data/day_utc=${match[1]}/connector_id=${match[2]}/pollutant_code=${match[3]}/manifest.json`;
    if (!(object.entry.dependencies || []).includes(manifestKey)) {
      throw finalProposalError({ key: object.key, object, differingFields: [`missing_index_manifest_dependency:${manifestKey}`] });
    }
    const stagedManifest = runState.objects?.[manifestKey];
    const identity = dependencyIdentity(object.entry, manifestKey);
    let manifestBody;
    if (stagedManifest) {
      manifestBody = fs.readFileSync(String(stagedManifest.local_path));
      if (!identity || identity.sha256 !== stagedManifest.sha256
        || identity.bytes !== Number(stagedManifest.bytes)
        || !["overlay", "planned_overlay"].includes(identity.source)) {
        throw finalProposalError({ key: object.key, object, differingFields: [`index_manifest_identity:${manifestKey}`] });
      }
    } else {
      manifestBody = fs.readFileSync(path.join(String(runState.base_dropbox_root || ""), manifestKey));
      if (!identity || identity.source !== "dropbox"
        || identity.sha256 !== sha256Hex(manifestBody)
        || identity.bytes !== manifestBody.byteLength) {
        throw finalProposalError({ key: object.key, object, differingFields: [`index_manifest_identity:${manifestKey}`] });
      }
    }
    const manifest = parseFinalJsonBody({
      key: manifestKey,
      object,
      body: manifestBody,
      differingField: `index_source_manifest_json:${manifestKey}`,
    });
    const indexPayload = parseFinalJsonBody({
      key: object.key,
      object,
      body: object.body,
      differingField: "scoped_index_json",
    });
    const indexDifferences = [];
    const declaredManifestKeys = [
      indexPayload.pollutant_manifest_key,
      indexPayload.connector_pollutant_manifest_key,
    ].filter(Boolean);
    if (!declaredManifestKeys.length || declaredManifestKeys.some((key) => key !== manifestKey)) {
      indexDifferences.push("pollutant_manifest_key");
    }
    if (indexPayload.pollutant_manifest_hash !== manifest.manifest_hash) {
      indexDifferences.push("pollutant_manifest_hash");
    }
    if (Number(indexPayload.source_row_count) !== Number(manifest.source_row_count)
      || !sameJson(indexPayload.timeseries_row_counts, manifest.timeseries_row_counts)) {
      indexDifferences.push("manifest_counts");
    }
    const fileIdentity = (entry) => ({
      key: String(entry?.key || ""),
      row_count: Number(entry?.row_count),
      bytes: Number(entry?.bytes),
      etag_or_hash: String(entry?.etag_or_hash || ""),
    });
    const indexFiles = (indexPayload.files || []).map(fileIdentity).sort((left, right) => left.key.localeCompare(right.key));
    const manifestFiles = (manifest.files || []).map(fileIdentity).sort((left, right) => left.key.localeCompare(right.key));
    if (!sameJson(indexFiles, manifestFiles)) indexDifferences.push("manifest_file_identities");
    if (indexDifferences.length) {
      throw finalProposalError({ key: object.key, object, differingFields: indexDifferences });
    }
  }
}

export async function validateFinalProposalGraph({ runState, proposal, runStatePath = null }) {
  const audit = {
    status: "running",
    started_at_utc: new Date().toISOString(),
    selected_partition_count: 0,
    validated_partition_count: 0,
    partitions: [],
  };
  runState.final_proposal_graph_validation = audit;
  if (runStatePath) atomicWriteJson(runStatePath, runState);
  try {
    const selectedPrefixes = proposal.prefixes.filter((item) =>
      CANONICAL_OBSERVATION_POLLUTANT_PREFIX_PATTERN.test(item.prefix));
    audit.selected_partition_count = selectedPrefixes.length;
    const objects = new Map(proposal.objects.map((object) => [object.key, object]));
    const tombstoneCounts = new Map();
    for (const selected of selectedPrefixes) {
      tombstoneCounts.set(selected.prefix, (tombstoneCounts.get(selected.prefix) || 0) + 1);
    }
    const sourceDerivedPrefixes = new Map();
    for (const object of proposal.objects) {
      if (!object.key.endsWith(".parquet")
        || object.entry?.stage !== "observations_data") continue;
      const partitionPrefix = object.key.slice(0, object.key.lastIndexOf("/"));
      if (!CANONICAL_OBSERVATION_POLLUTANT_PREFIX_PATTERN.test(partitionPrefix)) continue;
      if (!sourceDerivedPrefixes.has(partitionPrefix)) sourceDerivedPrefixes.set(partitionPrefix, object);
    }
    for (const [partitionPrefix, partObject] of sourceDerivedPrefixes) {
      if (tombstoneCounts.get(partitionPrefix) !== 1) {
        throw finalProposalError({
          key: `${partitionPrefix}/manifest.json`,
          object: partObject,
          differingFields: ["matching_pollutant_prefix_tombstone"],
        });
      }
    }
    for (const selected of selectedPrefixes) {
      const manifestKey = `${selected.prefix}/manifest.json`;
      const manifestObject = objects.get(manifestKey);
      const stagedParts = proposal.objects.filter((object) =>
        object.key.startsWith(`${selected.prefix}/`) && object.key.endsWith(".parquet"));
      const dedicatedEmptyAllowed = runState.execution_path
        === "dedicated_sos_historical_observation_replacement";
      if (!manifestObject || (!stagedParts.length && !dedicatedEmptyAllowed)) {
        throw finalProposalError({
          key: manifestKey,
          object: manifestObject,
          differingFields: [
            ...(!manifestObject ? ["matching_staged_pollutant_manifest"] : []),
            ...(!stagedParts.length && !dedicatedEmptyAllowed
              ? ["matching_staged_parquet"] : []),
          ],
        });
      }
    }
    for (const selected of selectedPrefixes) {
      const match = selected.prefix.match(CANONICAL_OBSERVATION_POLLUTANT_PREFIX_PATTERN);
      const [, dayUtc, connectorIdRaw, pollutantCode] = match;
      const connectorId = Number(connectorIdRaw);
      const manifestKey = `${selected.prefix}/manifest.json`;
      const manifestObject = objects.get(manifestKey);
      if (!manifestObject) {
        throw finalProposalError({ key: manifestKey, object: null, differingFields: ["missing_final_pollutant_manifest"] });
      }
      const partObjects = proposal.objects.filter((object) =>
        object.key.startsWith(`${selected.prefix}/`) && object.key.endsWith(".parquet"));
      let source;
      try {
        source = loadImmutableSourcePartition({ runState, dayUtc, connectorId, pollutantCode });
      } catch (error) {
        throw finalProposalError({
          key: manifestKey,
          object: manifestObject,
          differingFields: [`immutable_source_evidence:${error instanceof Error ? error.message : String(error)}`],
        });
      }
      if (!partObjects.length && source.rows.length) {
        throw finalProposalError({
          key: manifestKey,
          object: manifestObject,
          differingFields: ["missing_final_staged_parquet"],
        });
      }
      const stagedRows = [];
      const partRows = new Map();
      for (const part of partObjects) {
        let rows;
        try {
          rows = await readCanonicalObservationRows({ body: part.body, connectorId });
        } catch (error) {
          throw finalProposalError({
            key: part.key,
            object: part,
            differingFields: [`staged_parquet:${error instanceof Error ? error.message : String(error)}`],
          });
        }
        if (rows.some((row) => row.connector_id !== connectorId || row.pollutant_code !== pollutantCode)) {
          throw finalProposalError({ key: part.key, object: part, differingFields: ["parquet_scope_identity"] });
        }
        partRows.set(part.key, rows);
        stagedRows.push(...rows);
      }
      const staged = stagedRows.length
        ? computeObservationContentHash(stagedRows)
        : computeEmptyObservationContentHash();
      const stagedEncodedRows = stagedRows.map(encodeCanonicalObservationRow).sort();
      const stagedDifferences = semanticDifferences(source.metadata, staged);
      if (!sameJson(source.encodedRows, stagedEncodedRows)) stagedDifferences.push("canonical_row_identity_and_duplicate_multiplicity");
      if (stagedDifferences.length) {
        throw finalProposalError({ key: manifestKey, object: manifestObject, differingFields: stagedDifferences });
      }
      const manifest = parseManifestObject(manifestObject, "pollutant");
      const manifestDifferences = semanticDifferences(source.metadata, manifest);
      if (manifest.day_utc !== dayUtc || Number(manifest.connector_id) !== connectorId
        || manifest.pollutant_code !== pollutantCode || manifest.manifest_key !== manifestKey) {
        manifestDifferences.push("manifest_scope_identity");
      }
      if (Number(manifest.source_row_count) !== source.rows.length
        || Number(manifest.row_count) !== source.rows.length) {
        manifestDifferences.push("manifest_row_count");
      }
      const manifestTimeseriesRowCounts = manifest.timeseries_row_counts
        ?? (source.rows.length === 0 ? {} : null);
      if (!sameJson(manifestTimeseriesRowCounts, source.timeseriesRowCounts)) {
        manifestDifferences.push("manifest_timeseries_row_counts");
      }
      const manifestPartKeys = [...new Set((manifest.parquet_object_keys || []).map(String))].sort();
      const stagedPartKeys = partObjects.map((part) => part.key).sort();
      if (!sameJson(manifestPartKeys, stagedPartKeys)) manifestDifferences.push("manifest_parquet_object_keys");
      const fileEntries = new Map((manifest.files || []).map((entry) => [String(entry?.key || ""), entry]));
      if (fileEntries.size !== partObjects.length) manifestDifferences.push("manifest_files");
      for (const part of partObjects) {
        const fileEntry = fileEntries.get(part.key);
        const expectedPartCounts = timeseriesRowCounts(partRows.get(part.key));
        if (!fileEntry || Number(fileEntry.bytes) !== part.body.byteLength
          || String(fileEntry.etag_or_hash || "") !== part.entry.sha256
          || Number(fileEntry.row_count) !== partRows.get(part.key).length
          || !sameJson(fileEntry.timeseries_row_counts, expectedPartCounts)) {
          manifestDifferences.push(`manifest_part_identity:${part.key}`);
        }
      }
      if (manifestDifferences.length) {
        throw finalProposalError({
          key: manifestKey,
          object: manifestObject,
          differingFields: manifestDifferences,
          compatibilitySource: manifestObject.entry?.dependency_identities
            && Object.values(manifestObject.entry.dependency_identities).some((identity) => identity?.source === "dropbox")
            ? "dropbox"
            : null,
        });
      }
      Object.assign(manifestObject.entry, {
        proposal_owner: "source_derived_observation_repair",
        final_proposal_graph_validated: true,
        immutable_source_evidence_path: source.evidencePath,
        immutable_source_rows_path: source.rowsPath,
        immutable_source_evidence_sha256: source.evidenceSha256,
        immutable_source_rows_sha256: source.rowsSha256,
        immutable_source_content_hash: source.metadata.observation_content_hash,
        immutable_source_row_count: source.rows.length,
        immutable_source_verification_status_counts: source.metadata.verification_status_counts,
      });
      for (const part of partObjects) {
        Object.assign(part.entry, {
          proposal_owner: "source_derived_observation_repair",
          final_proposal_graph_validated: true,
        });
      }
      audit.partitions.push({
        manifest_key: manifestKey,
        proposal_owner: "source_derived_observation_repair",
        source_content_hash: source.metadata.observation_content_hash,
        staged_content_hash: staged.observation_content_hash,
        proposed_manifest_content_hash: manifest.observation_content_hash,
        row_count: source.rows.length,
        status: "validated",
      });
      audit.validated_partition_count += 1;
    }
    validateFinalParentReferences({ proposal, runState });
    Object.assign(audit, {
      status: "succeeded",
      finished_at_utc: new Date().toISOString(),
      parent_and_index_dependencies_validated: true,
      tombstones_validated: true,
    });
    if (runStatePath) atomicWriteJson(runStatePath, runState);
    return audit;
  } catch (error) {
    Object.assign(audit, {
      status: "failed",
      finished_at_utc: new Date().toISOString(),
      error: error instanceof Error ? error.message : String(error),
    });
    if (runStatePath) atomicWriteJson(runStatePath, runState);
    throw error;
  }
}

export async function verifyLiveObservationPartition({
  r2,
  runState,
  runStatePath,
  object,
  adapters,
  verifiedBodyCache = null,
}) {
  const match = object.key.match(CANONICAL_OBSERVATION_POLLUTANT_MANIFEST_PATTERN);
  if (!match) return;
  const [, dayUtc, connectorIdRaw, pollutantCode] = match;
  let manifest;
  try {
    manifest = JSON.parse(new TextDecoder().decode(object.body));
  } catch {
    throw new Error(`Proposed observation pollutant manifest is not valid JSON: ${object.key}`);
  }
  const connectorId = Number(connectorIdRaw);
  if (
    manifest?.day_utc !== dayUtc ||
    Number(manifest?.connector_id) !== connectorId ||
    manifest?.pollutant_code !== pollutantCode
  ) {
    throw new Error(`Proposed observation pollutant manifest scope mismatch: ${object.key}`);
  }
  validateObservationContentHashMetadata(manifest, {
    rowCount: Number(manifest.source_row_count),
  });
  const source = loadImmutableSourcePartition({
    runState,
    dayUtc,
    connectorId,
    pollutantCode,
  });
  const partKeys = Array.isArray(manifest.parquet_object_keys)
    ? manifest.parquet_object_keys.map(safeKey)
    : [];
  const dedicatedEmptyAllowed = runState.execution_path
    === "dedicated_sos_historical_observation_replacement";
  if ((!partKeys.length && (source.rows.length > 0 || !dedicatedEmptyAllowed))
    || partKeys.some((key) =>
    !key.startsWith(object.key.slice(0, -"/manifest.json".length) + "/") ||
    !key.endsWith(".parquet")
  )) {
    throw new Error(`Proposed observation pollutant manifest has invalid Parquet keys: ${object.key}`);
  }
  const canonicalRows = [];
  const bodySources = [];
  for (const key of partKeys) {
    const stagedEntry = runState.objects?.[key];
    const expectedSha = String(stagedEntry?.sha256 || "");
    if (!stagedEntry?.r2_verified || !/^[a-f0-9]{64}$/.test(expectedSha)) {
      throw new Error(`Live semantic verification requires a GET-verified staged Parquet: ${key}`);
    }
    let body = verifiedBodyCache?.get(key, expectedSha) || null;
    let sourceKind = "verified_get_cache";
    if (!body) {
      if (runState?.execution_path === "dedicated_sos_historical_observation_replacement") {
        throw new Error(
          `Dedicated SOS semantic verification refuses a second GET for changed Parquet: ${key}`,
        );
      }
      const live = await adapters.getObject({ r2, key });
      body = live.body;
      sourceKind = "fresh_get";
      if (Number(live.bytes) !== Number(stagedEntry.bytes) || sha256Hex(body) !== expectedSha) {
        throw new Error(`Live semantic verification R2 identity mismatch: ${key}`);
      }
    }
    bodySources.push({ key, verified_sha256: expectedSha, source: sourceKind });
    canonicalRows.push(...await readCanonicalObservationRows({
      body,
      connectorId,
    }));
  }
  const liveMetadata = canonicalRows.length
    ? computeObservationContentHash(canonicalRows)
    : computeEmptyObservationContentHash();
  const liveSourceDifferences = semanticDifferences(source.metadata, liveMetadata);
  Object.assign(object.entry, {
    live_observation_body_sources: bodySources,
    live_observation_content_hash: liveMetadata.observation_content_hash,
    live_verification_status_counts: liveMetadata.verification_status_counts,
    live_observation_source_evidence_hash: source.metadata.observation_content_hash,
    live_observation_content_verified_against_source: liveSourceDifferences.length === 0,
  });
  atomicWriteJson(runStatePath, runState);
  if (liveSourceDifferences.length) {
    object.entry.live_observation_failure_classification = "live_observation_content_mismatch";
    atomicWriteJson(runStatePath, runState);
    throw new Error(
      `Live repaired observation content does not match immutable source evidence: day=${dayUtc} connector=${connectorId} pollutant=${pollutantCode} differing_fields=${liveSourceDifferences.join(",")}`,
    );
  }
  const manifestDifferences = semanticDifferences(liveMetadata, manifest);
  if (Number(manifest.source_row_count) !== liveMetadata.observation_content_hash_row_count
    || Number(manifest.row_count) !== liveMetadata.observation_content_hash_row_count) {
    manifestDifferences.push("manifest_row_count");
  }
  if (manifestDifferences.length) {
    Object.assign(object.entry, {
      live_observation_content_verified: true,
      proposed_manifest_matches_live_observation: false,
      live_observation_failure_classification: "proposal_manifest_defect",
      proposal_manifest_differing_fields: [...new Set(manifestDifferences)],
    });
    atomicWriteJson(runStatePath, runState);
    throw new Error(
      `Proposed observation manifest does not match verified live source content: day=${dayUtc} connector=${connectorId} pollutant=${pollutantCode} differing_fields=${[...new Set(manifestDifferences)].join(",")}`,
    );
  }
  Object.assign(object.entry, {
    live_observation_content_verified: true,
    live_observation_content_verified_at_utc: new Date().toISOString(),
    proposed_manifest_matches_live_observation: true,
    live_observation_failure_classification: null,
  });
  for (const key of partKeys) {
    verifiedBodyCache?.invalidateKey(key, "semantic_verification_complete");
  }
  atomicWriteJson(runStatePath, runState);
}

async function prepareMergedDayManifest({ r2, object, adapters }) {
  if (!/\/day_utc=\d{4}-\d{2}-\d{2}\/manifest\.json$/.test(object.key)) return;
  const proposed = JSON.parse(object.body.toString("utf8"));
  const dayPrefix = object.key.slice(0, -"manifest.json".length);
  const currentRead = await readParentManifestForBoundedRecovery({
    getObject: adapters.getObject,
    r2,
    key: object.key,
    validate: (current) => {
      validateCanonicalHistoryV2Manifest(current, {
        history_version: "v2",
        domain: proposed.domain,
        grain: proposed.grain ?? null,
        profile: proposed.profile ?? null,
        manifest_kind: "day",
        day_utc: proposed.day_utc,
        manifest_key: object.key,
      });
      const values = Array.isArray(current?.connector_manifests)
        ? current.connector_manifests
        : Array.isArray(current?.child_manifests) ? current.child_manifests : null;
      if (!values) throw new Error("Current day manifest has no connector references");
      const references = values.map((entry) => ({
        connector_id: Number(entry.connector_id),
        manifest_key: String(entry.manifest_key || ""),
      }));
      if (references.some((entry) =>
        !Number.isInteger(entry.connector_id) || entry.connector_id <= 0 ||
        !entry.manifest_key.startsWith(dayPrefix) ||
        !entry.manifest_key.endsWith(`/connector_id=${entry.connector_id}/manifest.json`)
      )) {
        throw new Error("Current day manifest has invalid connector references");
      }
      return references;
    },
  });
  let currentReferences = currentRead.state === "valid" ? currentRead.value : [];
  if (currentRead.state !== "valid") {
    const discovered = await adapters.listAllObjects({ r2, prefix: dayPrefix, max_keys: 10_000 });
    currentReferences = discovered.flatMap((entry) => {
      const key = String(entry.key || "");
      const match = key.match(/\/connector_id=([1-9]\d*)\/manifest\.json$/);
      return match ? [{ connector_id: Number(match[1]), manifest_key: key }] : [];
    });
  }
  const references = (manifest) => {
    const values = Array.isArray(manifest?.connector_manifests)
      ? manifest.connector_manifests
      : Array.isArray(manifest?.child_manifests) ? manifest.child_manifests : [];
    return values.map((entry) => ({ connector_id: Number(entry.connector_id), manifest_key: String(entry.manifest_key || "") }));
  };
  const mergedReferences = mergeConnectorManifestReferences(currentReferences, references(proposed));
  const connectorManifests = [];
  for (const reference of mergedReferences) {
    const child = JSON.parse((await adapters.getObject({ r2, key: reference.manifest_key })).body.toString("utf8"));
    validateCanonicalHistoryV2Manifest(child, {
      history_version: "v2",
      domain: proposed.domain,
      manifest_kind: "connector",
      day_utc: proposed.day_utc,
      connector_id: reference.connector_id,
      manifest_key: reference.manifest_key,
    });
    connectorManifests.push({ ...child, manifest_key: reference.manifest_key });
  }
  const merged = buildHistoryV2DayManifest({
    domain: proposed.domain,
    grain: proposed.grain ?? null,
    profile: proposed.profile ?? null,
    dayUtc: proposed.day_utc,
    runId: proposed.run_id,
    manifestKey: object.key,
    connectorManifests,
    writerGitSha: proposed.writer_git_sha ?? null,
    backedUpAtUtc: proposed.backed_up_at_utc ?? new Date().toISOString(),
  });
  validateCanonicalHistoryV2Manifest(merged, {
    domain: proposed.domain,
    manifest_kind: "day",
    day_utc: proposed.day_utc,
  });
  object.body = Buffer.from(JSON.stringify(merged, null, 2), "utf8");
  Object.assign(object.entry, {
    bytes: object.body.byteLength,
    sha256: sha256Hex(object.body),
    day_finalizer_regenerated_from_live_connectors: true,
    merged_connector_ids: mergedReferences.map((entry) => entry.connector_id),
  });
}

export function assertPublicationDependenciesVerified({ object, runState }) {
  for (const dependencyKey of object?.entry?.dependencies || []) {
    const dependency = runState?.objects?.[dependencyKey];
    if (dependency?.proposed && dependency?.structurally_validated && dependency.r2_verified !== true) {
      throw new Error(`Publication dependency is not GET-verified: ${object.key} -> ${dependencyKey}`);
    }
  }
}

export async function applyValidatedProposal({ runStatePath, r2, adapters = {} }) {
  const resolvedAdapters = {
    deleteObjects: adapters.deleteObjects || r2DeleteObjects,
    getObject: adapters.getObject || r2GetObject,
    listAllObjects: adapters.listAllObjects || r2ListAllObjects,
    putObject: adapters.putObject || r2PutObject,
  };
  const runState = JSON.parse(fs.readFileSync(runStatePath, "utf8"));
  const proposal = validateLocalProposal(runState);
  const dedicatedSosProposal = validateDedicatedSosHistoricalProposal({
    runState,
    proposal,
  });
  await validateFinalProposalGraph({ runState, proposal, runStatePath });
  const counts = { planned_deletions: proposal.prefixes.length, planned_writes: proposal.objects.length, deleted_objects: 0, completed_deletions: 0, completed_writes: 0, get_verified_writes: 0 };
  runState.apply = {
    status: "running",
    started_at_utc: new Date().toISOString(),
    final_proposal_graph_validation: "succeeded",
    dedicated_sos_historical_proposal: dedicatedSosProposal,
    connector_day_publication: {},
    ...counts,
  };
  runState.writer_locks = [];
  atomicWriteJson(runStatePath, runState);
  try {
    const historyWriterClient = adapters.historyWriterClient;
    if (!historyWriterClient) throw new Error("canonical apply requires one retained PostgreSQL history-writer session");
    const operations = [];
    const applyDomains = dedicatedSosProposal.dedicated
      ? ["observations"]
      : ["observations", "aqilevels"];
    for (const domain of applyDomains) {
      for (const prefixEntry of proposal.prefixes.filter((entry) => entry.domain === domain)) {
        operations.push({ kind: "delete", key: prefixEntry.prefix, prefixEntry });
      }
      for (const object of proposal.objects.filter((entry) => entry.domain === domain)) {
        if (dedicatedSosProposal.dedicated
          && /\/observations_timeseries_latest\.json$/.test(object.key)) {
          Object.assign(object.entry, {
            delegated_global_latest_finalization: true,
            status: "delegated_global_latest_finalization",
          });
          continue;
        }
        operations.push({ kind: "put", key: object.key, object });
      }
    }
    const connectorGroups = new Map();
    const dayGroups = new Map();
    const globalOperations = [];
    for (const operation of operations) {
      const connectorMatch = operation.key.match(/day_utc=(\d{4}-\d{2}-\d{2})\/connector_id=([1-9]\d*)/);
      const dayMatch = operation.key.match(/day_utc=(\d{4}-\d{2}-\d{2})/);
      if (connectorMatch) {
        const groupKey = `${connectorMatch[1]}|${connectorMatch[2]}`;
        if (!connectorGroups.has(groupKey)) connectorGroups.set(groupKey, { day_utc: connectorMatch[1], connector_id: Number(connectorMatch[2]), operations: [] });
        connectorGroups.get(groupKey).operations.push(operation);
      } else if (dayMatch) {
        if (!dayGroups.has(dayMatch[1])) dayGroups.set(dayMatch[1], []);
        dayGroups.get(dayMatch[1]).push(operation);
      } else {
        globalOperations.push(operation);
      }
    }
    const executeOperation = async (operation, verifiedBodyCache = null) => {
      if (operation.kind === "delete") {
        const { prefixEntry } = operation;
        counts.deleted_objects += await deleteAndVerifyPrefix({
          r2,
          runState,
          runStatePath,
          prefixEntry,
          adapters: resolvedAdapters,
          verifiedBodyCache,
        });
        counts.completed_deletions += 1;
      } else {
        const { object } = operation;
        assertPublicationDependenciesVerified({ object, runState });
        await verifyLiveObservationPartition({
          r2,
          runState,
          runStatePath,
          object,
          adapters: resolvedAdapters,
          verifiedBodyCache,
        });
        await putAndVerifyObject({
          r2,
          runState,
          runStatePath,
          object,
          adapters: resolvedAdapters,
          verifiedBodyCache,
        });
        counts.completed_writes += 1;
        counts.get_verified_writes += 1;
      }
      atomicWriteJson(runStatePath, runState);
    };
    for (const group of Array.from(connectorGroups.values()).sort((left, right) =>
      left.day_utc.localeCompare(right.day_utc) || left.connector_id - right.connector_id)) {
      const groupKey = `${group.day_utc}|${group.connector_id}`;
      const verifiedBodyCache = createVerifiedGetBodyCache();
      runState.apply.connector_day_publication[groupKey] = {
        day_utc: group.day_utc,
        connector_id: group.connector_id,
        status: "running",
        completed_publication_level: "none",
      };
      await runCanonicalConnectorDayWriter({
        client: historyWriterClient,
        dayUtc: group.day_utc,
        connectorId: group.connector_id,
        diagnosticEnvironment: runState.environment,
        diagnostics: runState.writer_locks,
        write: async () => {
          try {
            for (const operation of group.operations) {
              await executeOperation(operation, verifiedBodyCache);
              const publication = runState.apply.connector_day_publication[groupKey];
              publication.completed_publication_level = operation.kind === "delete"
                ? "selected_prefix_deletion"
                : publicationRank(operation.key) <= 10
                ? "observation_parquet"
                : publicationRank(operation.key) <= 20
                ? "observation_pollutant_manifest"
                : publicationRank(operation.key) <= 30
                ? "observation_connector_manifest"
                : publicationRank(operation.key) <= 40
                ? "observation_indexes"
                : publicationRank(operation.key) <= 70
                ? "aqi_manifests_and_data"
                : "aqi_indexes";
              atomicWriteJson(runStatePath, runState);
            }
            runState.apply.connector_day_publication[groupKey].status = "succeeded";
            return { operation_count: group.operations.length };
          } catch (error) {
            runState.apply.connector_day_publication[groupKey].status = "failed";
            runState.apply.connector_day_publication[groupKey].error = error instanceof Error ? error.message : String(error);
            throw error;
          } finally {
            verifiedBodyCache.clear(
              runState.apply.connector_day_publication[groupKey].status === "succeeded"
                ? "connector_day_scope_complete"
                : "connector_day_scope_failed",
            );
            runState.apply.connector_day_publication[groupKey].verified_get_cache = verifiedBodyCache.snapshot();
            atomicWriteJson(runStatePath, runState);
          }
        },
        verify: async (written) => ({ ...written, get_verified: true }),
      });
    }
    for (const [dayUtc, dayOperations] of Array.from(dayGroups.entries()).sort(([left], [right]) => left.localeCompare(right))) {
      await runCanonicalDayFinalizer({
        client: historyWriterClient,
        dayUtc,
        diagnosticEnvironment: runState.environment,
        diagnostics: runState.writer_locks,
        finalize: async () => {
        for (const operation of dayOperations) {
          if (operation.kind === "put") {
            await prepareMergedDayManifest({ r2, object: operation.object, adapters: resolvedAdapters });
          }
          await executeOperation(operation);
        }
          return { operation_count: dayOperations.length };
        },
      });
    }
    const affectedDays = Array.from(new Set([
      ...Array.from(connectorGroups.values()).map((group) => group.day_utc),
      ...dayGroups.keys(),
    ])).sort();
    if (globalOperations.length || affectedDays.length) {
      await runCanonicalGlobalIndexFinalizer({
        client: historyWriterClient,
        diagnosticEnvironment: runState.environment,
        diagnostics: runState.writer_locks,
        finalize: async () => {
        for (const operation of globalOperations) await executeOperation(operation);
        if (affectedDays.length && !dedicatedSosProposal.dedicated) {
          runState.global_index_finalization = await updateR2HistoryIndexesTargeted({
            env: process.env,
            r2,
            historyVersion: "v2",
            domains: ["observations", "aqilevels"],
            affectedDaysUtc: affectedDays,
            connectorId: null,
            updateLatestIndex: true,
            strictMissingTimeseriesCounts: true,
            writeR2: true,
          });
        } else if (affectedDays.length) {
          runState.global_index_finalization = await updateR2HistoryIndexesTargeted({
            env: process.env,
            r2,
            historyVersion: "v2",
            domains: ["observations"],
            affectedDaysUtc: affectedDays,
            connectorId: null,
            updateLatestIndex: true,
            writePollutantIndexes: false,
            strictMissingTimeseriesCounts: true,
            writeR2: true,
          });
          const latest = runState.global_index_finalization?.observations_timeseries;
          const latestKey = String(latest?.latest_index_key || "");
          const latestEntry = runState.objects?.[latestKey];
          if (!latestKey || !latestEntry?.delegated_global_latest_finalization
            || latest?.latest_index_verified !== true) {
            throw new Error("Dedicated SOS live observation latest finalization was not verified");
          }
          Object.assign(latestEntry, {
            remote_attempted: !latest.latest_index_put_skipped,
            remote_completed: true,
            uploaded: !latest.latest_index_put_skipped,
            r2_verified: true,
            r2_verified_at_utc: new Date().toISOString(),
            post_put_verification_get_attempt_count: latest.latest_index_put_skipped ? 0 : 1,
            post_put_verification_get_count: latest.latest_index_put_skipped ? 0 : 1,
            skipped_unchanged: Boolean(latest.latest_index_put_skipped),
            final_live_sha256: latest.latest_index_sha256,
            final_live_bytes: latest.latest_index_bytes,
            status: latest.latest_index_put_skipped ? "skipped_unchanged" : "get_verified",
          });
          if (!latest.latest_index_put_skipped) {
            counts.completed_writes += 1;
            counts.get_verified_writes += 1;
          }
          atomicWriteJson(runStatePath, runState);
        }
        },
      });
    }
    runState.apply = { ...runState.apply, ...counts, status: "succeeded", finished_at_utc: new Date().toISOString() };
    atomicWriteJson(runStatePath, runState);
    return { ok: true, status: "succeeded", ...counts };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    runState.apply = { ...runState.apply, ...counts, status: "failed", error: message, finished_at_utc: new Date().toISOString() };
    atomicWriteJson(runStatePath, runState);
    throw error;
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const runStatePath = path.resolve(args.runStateJson);
  const config = resolveR2HistoryIndexConfig(process.env);
  if (!hasRequiredR2Config(config.r2)) throw new Error("canonical apply requires complete R2 configuration");
  if (config.r2.bucket !== TEST_BUCKET) throw new Error(`Refusing canonical apply for non-TEST bucket: ${config.r2.bucket || "(unset)"}`);
  return await withHistoryWriterClient(
    process.env.SUPABASE_DB_URL || process.env.DATABASE_URL,
    async (historyWriterClient) => await applyValidatedProposal({
      runStatePath,
      r2: config.r2,
      adapters: { historyWriterClient },
    }),
    { applicationName: "uk-aq-integrity-history-writer" },
  );
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) main().then((result) => {
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}).catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
});

