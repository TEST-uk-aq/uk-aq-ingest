/usr/local/bin/bash: warning: setlocale: LC_ALL: cannot change locale (C.UTF-8): No such file or directory
#!/bin/bash

set -euo pipefail

# Always provide a valid detached stdin to Python and child processes.
exec </dev/null

INTEGRITY="/Users/mikehinford/uk-aq-history-integrity/bin/uk-aq-history-integrity-sos-light-local-wrapper-v2.sh"
INTEGRITY_ENV="LIVE"
BACKUP_REPOSITORY="UK-AQ/uk-aq-ops"
BACKUP_WORKFLOW="uk_aq_r2_history_dropbox_backup.yml"
BACKUP_ARTIFACT="uk-aq-r2-history-dropbox-backup-report"
BACKUP_REPORT="r2_history_dropbox_backup_report.json"
OBSERVATION_PARQUET_COPY_MODE="reuse_matching"
LOG_ROOT="/Users/mikehinford/uk-aq-history-integrity/state/LIVE/logs/integrity-run-monthly"
PYTHON_BIN="${PYTHON_BIN:-python3}"
DISCOVERY_TIMEOUT_SECONDS="${DISCOVERY_TIMEOUT_SECONDS:-300}"
DISCOVERY_POLL_SECONDS="${DISCOVERY_POLL_SECONDS:-10}"
BACKUP_TIMEOUT_SECONDS="${BACKUP_TIMEOUT_SECONDS:-21600}"
BACKUP_POLL_SECONDS="${BACKUP_POLL_SECONDS:-30}"
DROPBOX_SYNC_TIMEOUT_SECONDS="${DROPBOX_SYNC_TIMEOUT_SECONDS:-7200}"
DROPBOX_SYNC_POLL_SECONDS="${DROPBOX_SYNC_POLL_SECONDS:-15}"

mkdir -p "$LOG_ROOT"

RUN_STARTED="$(date -u +%Y%m%dT%H%M%SZ)"
BATCH_ID="${RUN_STARTED}-$$"
SUMMARY="$LOG_ROOT/run-summary-${RUN_STARTED}.log"
LOCAL_INTEGRITY_ROOT="$(cd -P -- "$(dirname -- "$INTEGRITY")/.." && pwd -P)"
REPOSITORY_SELECTOR="$LOCAL_INTEGRITY_ROOT/env/${INTEGRITY_ENV}.env"
OPS_REPO_ROOT=""
DROPBOX_BACKUP_ROOT=""
OBSERVATION_HISTORY_VERSION=""
LOCAL_CHECKPOINT_ROOT=""

log() {
  echo "$(date -u +%FT%TZ) $*" | tee -a "$SUMMARY"
}

fail() {
  log "ERROR $*"
  return 1
}

json_field() {
  local path="$1"
  local field="$2"
  "$PYTHON_BIN" - "$path" "$field" <<'PY'
import json
import sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    value = value[part]
if isinstance(value, bool):
    print("true" if value else "false")
elif value is None:
    print("")
else:
    print(value)
PY
}

receipt_valid() {
  local path="$1"
  shift
  "$PYTHON_BIN" - "$path" "$@" <<'PY'
import json
import sys
try:
    data = json.load(open(sys.argv[1], encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError):
    raise SystemExit(1)
for requirement in sys.argv[2:]:
    key, expected = requirement.split("=", 1)
    value = data
    for part in key.split("."):
        if not isinstance(value, dict) or part not in value:
            raise SystemExit(1)
        value = value[part]
    if str(value).lower() != expected.lower():
        raise SystemExit(1)
raise SystemExit(0)
PY
}

integrity_receipt_valid() {
  local path="$1"
  local label="$2"
  local from_day="$3"
  local to_day="$4"
  receipt_valid "$path" "phase=integrity_succeeded" "label=$label" "from_day=$from_day" "to_day=$to_day" || return 1
  local recorded_log
  recorded_log="$(json_field "$path" log)" || return 1
  [ -f "$recorded_log" ]
}

month_state_valid() {
  local label="$1"
  local from_day="$2"
  local to_day="$3"
  local integrity_receipt="$4"
  local dispatch_receipt="$5"
  local backup_receipt="$6"
  local sync_receipt="$7"
  integrity_receipt_valid "$integrity_receipt" "$label" "$from_day" "$to_day" || return 1
  receipt_valid "$dispatch_receipt" "phase=backup_dispatched_resolved" "label=$label" \
    "requested_observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE" || return 1
  local caller run_id state_key expected_hash
  caller="$(json_field "$dispatch_receipt" caller_run_id)" || return 1
  run_id="$(json_field "$dispatch_receipt" run_id)" || return 1
  receipt_valid "$backup_receipt" "phase=backup_succeeded" "label=$label" \
    "caller_run_id=$caller" "run_id=$run_id" \
    "observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE" || return 1
  state_key="$(json_field "$backup_receipt" state_root_key)" || return 1
  expected_hash="$(json_field "$backup_receipt" expected_hash)" || return 1
  [[ "$state_key" != /* && "$state_key" != *".."* && "$expected_hash" =~ ^[0-9a-f]{64}$ ]] || return 1
  receipt_valid "$sync_receipt" "phase=local_materialisation_verified" "label=$label" \
    "caller_run_id=$caller" "run_id=$run_id" "generation=$OBSERVATION_HISTORY_VERSION" \
    "state_root_key=$state_key" "expected_hash=$expected_hash" "observed_hash=$expected_hash" \
    "observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE"
}

write_receipt() {
  local path="$1"
  shift
  "$PYTHON_BIN" - "$path" "$@" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
data = {"recorded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")}
for item in sys.argv[2:]:
    key, value = item.split("=", 1)
    data[key] = value
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.replace(tmp, path)
PY
}

resolve_ops_repo_root() {
  "$PYTHON_BIN" - "$REPOSITORY_SELECTOR" <<'PY'
import re
import sys
from pathlib import Path
path = Path(sys.argv[1])
if not path.is_file():
    raise SystemExit(f"Integrity repository selector is unavailable: {path}")
assignments = []
for raw in path.read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line or line.startswith("#"):
        continue
    match = re.fullmatch(r"UK_AQ_OPS_REPO_ROOT\s*=\s*(.*?)\s*(?:#.*)?", line)
    if not match:
        raise SystemExit("Integrity repository selector contains an unsupported entry")
    value = match.group(1).strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    assignments.append(value)
if len(assignments) != 1:
    raise SystemExit("Integrity repository selector must define UK_AQ_OPS_REPO_ROOT exactly once")
root = Path(assignments[0]).expanduser()
if not root.is_absolute() or not root.is_dir() or "archive" in root.parts:
    raise SystemExit(f"selected Integrity repository is invalid: {root}")
print(root.resolve())
PY
}

resolve_dropbox_backup_root() {
  "$PYTHON_BIN" - "$OPS_REPO_ROOT/.env" <<'PY'
import os
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.is_file():
    raise SystemExit(f"repository environment file is unavailable: {path}")
values = dict(os.environ)
for raw in path.read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, value = line.split("=", 1)
    if key.startswith("export "):
        key = key[len("export "):]
    key = key.strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        continue
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    values[key] = value
explicit = values.get("UK_AQ_R2_HISTORY_DROPBOX_ROOT", "").strip()
if explicit:
    root = Path(explicit).expanduser()
    if not root.is_absolute():
        raise SystemExit("UK_AQ_R2_HISTORY_DROPBOX_ROOT must be absolute")
else:
    environment_root = values.get("UK_AQ_DROPBOX_ROOT", "").strip()
    if not environment_root:
        raise SystemExit("UK_AQ_DROPBOX_ROOT is not configured")
    history_dir = values.get("UK_AQ_R2_HISTORY_DROPBOX_DIR", "R2_history_backup").strip()
    app_root = Path(values.get("UK_AQ_DROPBOX_APP_ROOT", "/Users/mikehinford/Dropbox/Apps/github-uk-air-quality-networks")).expanduser()
    environment_path = Path(environment_root).expanduser()
    root = environment_path / history_dir if environment_path.is_absolute() else app_root / environment_path / history_dir
if "archive" in root.parts:
    raise SystemExit("resolved R2 history Dropbox root points into archive")
if not root.is_dir():
    raise SystemExit(f"resolved R2 history Dropbox root is not a directory: {root}")
history_version = values.get("UK_AQ_R2_HISTORY_VERSION", "").strip()
if history_version not in {"v2", "v3"}:
    raise SystemExit("UK_AQ_R2_HISTORY_VERSION must be exactly v2 or v3")
print(f"{root.resolve()}\t{history_version}")
PY
}

preflight() {
  log "PREFLIGHT target=${BACKUP_REPOSITORY} workflow=${BACKUP_WORKFLOW}"
  command -v gh >/dev/null 2>&1 || fail "gh is not available"
  gh auth status >/dev/null 2>&1 || fail "GitHub CLI authentication is unusable"
  gh repo view "$BACKUP_REPOSITORY" --json nameWithOwner --jq .nameWithOwner >/dev/null || fail "cannot access backup repository ${BACKUP_REPOSITORY}"
  gh workflow view "$BACKUP_WORKFLOW" --repo "$BACKUP_REPOSITORY" >/dev/null || fail "cannot access ${BACKUP_WORKFLOW} in ${BACKUP_REPOSITORY}"

  local remote_workflow
  remote_workflow="$(mktemp)"
  if ! gh api -H 'Accept: application/vnd.github.raw+json' \
    "repos/${BACKUP_REPOSITORY}/contents/.github/workflows/${BACKUP_WORKFLOW}" >"$remote_workflow"; then
    rm -f "$remote_workflow"
    fail "cannot read ${BACKUP_WORKFLOW} from ${BACKUP_REPOSITORY}"
    return 1
  fi
  if ! "$PYTHON_BIN" - "$remote_workflow" "$OBSERVATION_PARQUET_COPY_MODE" <<'PY'
import re
import sys
text = open(sys.argv[1], encoding="utf-8").read()
required_mode = sys.argv[2]
has_input = bool(re.search(r"(?m)^\s{6}caller_run_id:\s*$", text))
has_run_name = bool(re.search(r"(?m)^run-name:.*caller_run_id", text))
mode_input = re.search(
    r"(?ms)^\s{6}observation_parquet_copy_mode:\s*\n(?P<body>(?:^\s{8,}.*\n?)*)",
    text,
)
has_required_mode = bool(
    mode_input
    and re.search(rf"(?m)^\s+-\s+{re.escape(required_mode)}\s*$", mode_input.group("body"))
)
raise SystemExit(0 if has_input and has_run_name and has_required_mode else 1)
PY
  then
    rm -f "$remote_workflow"
    if [ "$INTEGRITY_ENV" = "LIVE" ]; then
      fail "LIVE backup workflow lacks caller_run_id correlation or observation_parquet_copy_mode=${OBSERVATION_PARQUET_COPY_MODE} support; promote the required workflow to ${BACKUP_REPOSITORY} before running LIVE Integrity"
    else
      fail "backup workflow lacks required caller_run_id correlation or observation_parquet_copy_mode=${OBSERVATION_PARQUET_COPY_MODE} input support"
    fi
    return 1
  fi
  rm -f "$remote_workflow"

  OPS_REPO_ROOT="$(resolve_ops_repo_root)" || fail "cannot resolve the ${INTEGRITY_ENV} repository selected by the Integrity local wrapper"
  local dropbox_config
  dropbox_config="$(resolve_dropbox_backup_root)" || fail "local Dropbox backup root/checkpoint location cannot be resolved"
  IFS=$'\t' read -r DROPBOX_BACKUP_ROOT OBSERVATION_HISTORY_VERSION <<<"$dropbox_config"
  [ -r "$DROPBOX_BACKUP_ROOT" ] || fail "local Dropbox backup root is not readable: ${DROPBOX_BACKUP_ROOT}"
  LOCAL_CHECKPOINT_ROOT="$DROPBOX_BACKUP_ROOT/_ops/checkpoints/r2_history_backup_state_v2/observation_generation=${OBSERVATION_HISTORY_VERSION}/root.json"
  [ -r "$LOCAL_CHECKPOINT_ROOT" ] || fail "generation-specific local Dropbox checkpoint is not readable: ${LOCAL_CHECKPOINT_ROOT}"
  log "PREFLIGHT OK selected_repository=${OPS_REPO_ROOT} local_backup_root=${DROPBOX_BACKUP_ROOT} observation_history_version=${OBSERVATION_HISTORY_VERSION} checkpoint_location=${LOCAL_CHECKPOINT_ROOT} requested_observation_parquet_copy_mode=${OBSERVATION_PARQUET_COPY_MODE}"
}

append_connector_totals() {
  local log_file="$1"
  "$PYTHON_BIN" - "$log_file" <<'PY' | tee -a "$SUMMARY"
import json
import re
import sys
from pathlib import Path
log_path = Path(sys.argv[1])
try:
    log_text = log_path.read_text(encoding="utf-8", errors="replace")
except OSError:
    print("Connector observation totals unavailable")
    raise SystemExit(0)
def last_existing_report_path(field):
    for candidate in reversed(re.findall(rf"\b{re.escape(field)}=(\S+)", log_text)):
        if Path(candidate).is_file():
            return Path(candidate)
def totals_from_json(path):
    if path is None: return None
    try: report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError): return None
    totals = report.get("connector_observation_totals")
    return totals if isinstance(totals, dict) and totals else None
def totals_from_markdown(path):
    if path is None: return None
    try: lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError): return None
    try: start = lines.index("## Connector observation totals") + 1
    except ValueError: return None
    totals, connector_id = {}, None
    labels = {"Total Observs before": "total_observations_before", "Total Observs added": "total_observations_added", "Total Observs after": "total_observations_after"}
    for line in lines[start:]:
        if line.startswith("## "): break
        match = re.fullmatch(r"### Connector (.+)", line)
        if match:
            connector_id = match.group(1).strip(); totals.setdefault(connector_id, {}); continue
        match = re.fullmatch(r"- (Total Observs (?:before|added|after)): ([0-9,]+)", line)
        if connector_id and match: totals[connector_id][labels[match.group(1)]] = int(match.group(2).replace(",", ""))
    return totals or None
totals = totals_from_json(last_existing_report_path("report_json")) or totals_from_markdown(last_existing_report_path("report_md"))
if not totals:
    print("Connector observation totals unavailable"); raise SystemExit(0)
def sort_key(item):
    try: return (0, int(str(item[0])))
    except ValueError: return (1, str(item[0]))
printed = False
for connector_id, values in sorted(totals.items(), key=sort_key):
    if not isinstance(values, dict): continue
    fields = [values.get(name) for name in ("total_observations_before", "total_observations_added", "total_observations_after")]
    if not all(isinstance(value, int) and value >= 0 for value in fields): continue
    print(f"Connector {connector_id}:")
    print(f"Total Observs before: {fields[0]:,}")
    print(f"Total Observs added: {fields[1]:,}")
    print(f"Total Observs after: {fields[2]:,}")
    printed = True
if not printed: print("Connector observation totals unavailable")
PY
}

find_correlated_run() {
  local caller_run_id="$1"
  local runs_json="$2"
  gh api \
    "repos/${BACKUP_REPOSITORY}/actions/workflows/${BACKUP_WORKFLOW}/runs?event=workflow_dispatch&per_page=100" >"$runs_json" || return 1
  "$PYTHON_BIN" - "$runs_json" "$caller_run_id" <<'PY'
import json
import sys
matches = []
with open(sys.argv[1], encoding="utf-8") as stream:
    decoder = json.JSONDecoder()
    text = stream.read()
    pos = 0
    while pos < len(text):
        while pos < len(text) and text[pos].isspace(): pos += 1
        if pos >= len(text): break
        payload, pos = decoder.raw_decode(text, pos)
        matches.extend(run for run in payload.get("workflow_runs", []) if sys.argv[2] in str(run.get("display_title", "")))
if len(matches) != 1:
    print(len(matches))
    raise SystemExit(2)
run = matches[0]
print(f"{run['id']}\t{run['html_url']}")
PY
}

record_backup_failure() {
  local receipt_path="$1"
  local label="$2"
  local caller_run_id="$3"
  local run_id="$4"
  local run_url="$5"
  local status="$6"
  local conclusion="$7"
  local reason="$8"
  write_receipt "$receipt_path" "phase=backup_failed" "label=$label" \
    "caller_run_id=$caller_run_id" "run_id=$run_id" "run_url=$run_url" \
    "status=$status" "conclusion=$conclusion" "reason=$reason"
  log "BACKUP FAILURE EVIDENCE label=${label} caller_run_id=${caller_run_id} run_id=${run_id} url=${run_url} status=${status} conclusion=${conclusion} receipt=${receipt_path}"
}

resolve_exact_run() {
  local caller_run_id="$1"
  local dispatch_receipt="$2"
  local backup_receipt="$3"
  local sync_receipt="$4"
  local label="$5"
  local deadline=$(( $(date +%s) + DISCOVERY_TIMEOUT_SECONDS ))
  local runs_json result count
  runs_json="$(mktemp)"
  while [ "$(date +%s)" -le "$deadline" ]; do
    if result="$(find_correlated_run "$caller_run_id" "$runs_json")"; then
      local run_id="${result%%$'\t'*}"
      local run_url="${result#*$'\t'}"
      write_receipt "$dispatch_receipt" \
        "phase=backup_dispatched_resolved" "label=$label" "caller_run_id=$caller_run_id" \
        "run_id=$run_id" "run_url=$run_url" \
        "requested_observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE"
      rm -f "$runs_json"
      log "BACKUP RESOLVED caller_run_id=${caller_run_id} run_id=${run_id} url=${run_url}"
      return 0
    else
      count="${result:-query_failed}"
      if [ "$count" != "0" ] && [ "$count" != "query_failed" ]; then
        local ambiguity_receipt="$LOG_ROOT/${label}.backup-dispatch-unresolved-${caller_run_id}.json"
        write_receipt "$ambiguity_receipt" "phase=backup_dispatch_unresolved" "label=$label" \
          "caller_run_id=$caller_run_id" "status=ambiguous" "matches=$count"
        rm -f "$runs_json" "$dispatch_receipt" "$backup_receipt" "$sync_receipt"
        fail "backup run correlation is ambiguous for ${caller_run_id}: matches=${count}; incomplete dispatch state cleared for a later fresh attempt; evidence=${ambiguity_receipt}"
        return 1
      fi
    fi
    log "BACKUP DISCOVERY waiting caller_run_id=${caller_run_id}"
    sleep "$DISCOVERY_POLL_SECONDS"
  done
  local unresolved_receipt="$LOG_ROOT/${label}.backup-dispatch-unresolved-${caller_run_id}.json"
  write_receipt "$unresolved_receipt" "phase=backup_dispatch_unresolved" "label=$label" \
    "caller_run_id=$caller_run_id" "status=not_resolved" "conclusion=unconfirmed"
  rm -f "$runs_json" "$dispatch_receipt" "$backup_receipt" "$sync_receipt"
  fail "timed out resolving exact backup run for ${caller_run_id}; incomplete dispatch state cleared for a later fresh attempt; evidence=${unresolved_receipt}"
}

wait_for_backup_run() {
  local run_id="$1"
  local run_url="$2"
  local caller_run_id="$3"
  local label="$4"
  local dispatch_receipt="$5"
  local backup_receipt="$6"
  local sync_receipt="$7"
  local deadline=$(( $(date +%s) + BACKUP_TIMEOUT_SECONDS ))
  local record status conclusion url
  while [ "$(date +%s)" -le "$deadline" ]; do
    if ! record="$(gh run view "$run_id" --repo "$BACKUP_REPOSITORY" --json status,conclusion,url --jq '[.status, (.conclusion // ""), .url] | join("|")' 2>/dev/null)"; then
      local unavailable_receipt="$LOG_ROOT/${label}.backup-run-unconfirmed-${run_id}.json"
      write_receipt "$unavailable_receipt" "phase=backup_run_unconfirmed" "label=$label" \
        "caller_run_id=$caller_run_id" "run_id=$run_id" "run_url=$run_url" \
        "status=unavailable" "conclusion=unconfirmed" "reason=exact_run_unreadable"
      fail "exact backup run ${run_id} disappeared or cannot be read; resolved exact-run state retained for retry; evidence=${unavailable_receipt}"
      return 1
    fi
    IFS='|' read -r status conclusion url <<<"$record"
    log "BACKUP POLL run_id=${run_id} status=${status} conclusion=${conclusion:-pending} url=${url}"
    if [ "$status" = "completed" ]; then
      if [ "$conclusion" != "success" ]; then
        local failure_receipt="$LOG_ROOT/${label}.backup-failure-${run_id}.json"
        record_backup_failure "$failure_receipt" "$label" "$caller_run_id" "$run_id" "$url" "$status" "${conclusion:-unknown}" "terminal_non_success"
        rm -f "$dispatch_receipt" "$backup_receipt" "$sync_receipt"
        fail "exact backup run ${run_id} concluded ${conclusion:-unknown}; failed backup state cleared for a later fresh attempt"
        return 1
      fi
      return 0
    fi
    sleep "$BACKUP_POLL_SECONDS"
  done
  fail "timed out waiting for exact backup run ${run_id}; resolved run state retained for continued polling on rerun"
}

validate_backup_report() {
  local report_path="$1"
  "$PYTHON_BIN" - "$report_path" "$OBSERVATION_PARQUET_COPY_MODE" <<'PY'
import json
import re
import sys
from pathlib import PurePosixPath
try:
    report = json.load(open(sys.argv[1], encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError) as exc:
    raise SystemExit(f"backup report unreadable: {exc}")
state_key = report.get("state_root_key")
root_hash = (report.get("observations") or {}).get("processed_source_root_hash")
effective_mode = report.get("observation_parquet_copy_mode")
if report.get("ok") is not True or report.get("complete") is not True or report.get("dry_run") is not False:
    raise SystemExit("backup report does not show a successful complete non-dry-run backup")
if report.get("max_days_per_run") != 0:
    raise SystemExit("backup report does not show max_days_per_run=0")
if effective_mode != sys.argv[2]:
    raise SystemExit(
        "backup report observation_parquet_copy_mode is not " + sys.argv[2]
    )
if not isinstance(state_key, str) or not state_key or state_key.startswith("/") or ".." in PurePosixPath(state_key).parts:
    raise SystemExit("backup report state_root_key is invalid")
if not isinstance(root_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", root_hash):
    raise SystemExit("backup report observations.processed_source_root_hash is not SHA-256")
print(f"{state_key}\t{root_hash}\t{effective_mode}")
PY
}

wait_for_local_checkpoint() {
  local state_root_key="$1"
  local expected_hash="$2"
  local label="$3"
  local checkpoint="$DROPBOX_BACKUP_ROOT/$state_root_key"
  local deadline=$(( $(date +%s) + DROPBOX_SYNC_TIMEOUT_SECONDS ))
  local observed=""
  while [ "$(date +%s)" -le "$deadline" ]; do
    observed="$("$PYTHON_BIN" - "$checkpoint" 2>/dev/null <<'PY' || true
import json
import re
import sys
try:
    value = json.load(open(sys.argv[1], encoding="utf-8"))["observations"]["processed_source_root_hash"]
except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError):
    raise SystemExit(1)
if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
    raise SystemExit(1)
print(value)
PY
)"
    if [ "$observed" = "$expected_hash" ]; then
      log "LOCAL CHECKPOINT MATCH label=${label} checkpoint=${checkpoint} processed_source_root_hash=${observed}"
      return 0
    fi
    log "LOCAL CHECKPOINT WAIT label=${label} checkpoint=${checkpoint} expected=${expected_hash} observed=${observed:-unreadable_or_not_synced}"
    sleep "$DROPBOX_SYNC_POLL_SECONDS"
  done
  fail "local Dropbox checkpoint did not reach expected hash ${expected_hash}: ${checkpoint}"
}

wait_for_local_materialisation() {
  local report_path="$1"
  local state_root_key="$2"
  local expected_hash="$3"
  local caller_run_id="$4"
  local run_id="$5"
  local label="$6"
  local sync_receipt="$7"
  local attempt_report="$LOG_ROOT/${label}.materialisation-attempt.json"
  local verifier="$OPS_REPO_ROOT/scripts/backup_r2/verify_local_backup_materialisation.mjs"
  local deadline=$(( $(date +%s) + DROPBOX_SYNC_TIMEOUT_SECONDS ))
  [ -f "$verifier" ] || { fail "local materialisation verifier is unavailable: ${verifier}"; return 1; }
  while [ "$(date +%s)" -le "$deadline" ]; do
    if node "$verifier" \
      --backup-root "$DROPBOX_BACKUP_ROOT" \
      --backup-report "$report_path" \
      --expected-observations-root "$expected_hash" \
      --generation "$OBSERVATION_HISTORY_VERSION" \
      --output "$attempt_report" >/dev/null 2>&1
    then
      local verified_days verified_objects verified_core verified_bindings completed_at
      verified_days="$(json_field "$attempt_report" verified_observation_day_count)"
      verified_objects="$(json_field "$attempt_report" verified_observation_object_count)"
      verified_core="$(json_field "$attempt_report" verified_core_unit_count)"
      verified_bindings="$(json_field "$attempt_report" verified_binding_unit_count)"
      completed_at="$(json_field "$attempt_report" completed_at)"
      write_receipt "$sync_receipt" "phase=local_materialisation_verified" "label=$label" \
        "caller_run_id=$caller_run_id" "run_id=$run_id" \
        "expected_hash=$expected_hash" "observed_hash=$expected_hash" \
        "generation=$OBSERVATION_HISTORY_VERSION" "state_root_key=$state_root_key" \
        "observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE" \
        "verified_observation_day_count=$verified_days" \
        "verified_observation_object_count=$verified_objects" \
        "verified_core_unit_count=$verified_core" \
        "verified_binding_unit_count=$verified_bindings" \
        "verifier_completed_at=$completed_at"
      log "LOCAL MATERIALISATION VERIFIED label=${label} caller_run_id=${caller_run_id} run_id=${run_id} generation=${OBSERVATION_HISTORY_VERSION} observation_days=${verified_days} observation_objects=${verified_objects} core_units=${verified_core} binding_units=${verified_bindings}"
      return 0
    fi
    local pending_error="unavailable"
    if [ -f "$attempt_report" ]; then
      pending_error="$(json_field "$attempt_report" error 2>/dev/null || echo unreadable_verifier_report)"
    fi
    log "LOCAL MATERIALISATION WAIT label=${label} run_id=${run_id} pending=${pending_error}"
    sleep "$DROPBOX_SYNC_POLL_SECONDS"
  done
  fail "local Dropbox materialisation did not authenticate within ${DROPBOX_SYNC_TIMEOUT_SECONDS}s; evidence=${attempt_report}"
}

run_batch() {
  local from_day="$1"
  local to_day="$2"
  local label="$3"
  local log_file="$LOG_ROOT/${label}-${RUN_STARTED}.log"
  local ok_marker="$LOG_ROOT/${label}.ok"
  local integrity_receipt="$LOG_ROOT/${label}.integrity-success.json"
  local dispatch_receipt="$LOG_ROOT/${label}.backup-dispatch.json"
  local backup_receipt="$LOG_ROOT/${label}.backup-success.json"
  local sync_receipt="$LOG_ROOT/${label}.local-sync.json"
  local artifact_dir="$LOG_ROOT/${label}.backup-artifact"

  if [ "${FORCE:-0}" = "1" ]; then
    rm -f "$ok_marker" "$integrity_receipt" "$dispatch_receipt" "$backup_receipt" "$sync_receipt"
    rm -rf "$artifact_dir"
    log "FORCE RESET label=${label} phase state cleared"
  elif [ -f "$ok_marker" ]; then
    if month_state_valid "$label" "$from_day" "$to_day" "$integrity_receipt" "$dispatch_receipt" "$backup_receipt" "$sync_receipt"; then
      log "SKIP label=${label} final_month_complete=true"
      return 0
    fi
    rm -f "$ok_marker"
    fail "stale final marker found without valid local-sync receipt for ${label}; refusing to treat it as complete"
    return 1
  fi

  log "MONTH START label=${label} range=${from_day}..${to_day}"
  if ! integrity_receipt_valid "$integrity_receipt" "$label" "$from_day" "$to_day"; then
    rm -f "$dispatch_receipt" "$backup_receipt" "$sync_receipt" "$ok_marker"
    rm -rf "$artifact_dir"
    log "INTEGRITY START label=${label} log=${log_file}"
    if nice -n 10 "$INTEGRITY" \
      --env "$INTEGRITY_ENV" \
      --profile manual \
      --source sos \
      --from-day "$from_day" \
      --to-day "$to_day" \
      --run-backfill \
      --repair-pollutants pm25,pm10,no2,o3 \
      --verbose \
      </dev/null >"$log_file" 2>&1
    then
      write_receipt "$integrity_receipt" "phase=integrity_succeeded" "label=$label" \
        "from_day=$from_day" "to_day=$to_day" "log=$log_file"
      log "INTEGRITY SUCCESS label=${label}"
      append_connector_totals "$log_file"
    else
      local exit_code=$?
      log "INTEGRITY FAILURE label=${label} exit=${exit_code} final_month_complete=false"
      return "$exit_code"
    fi
  else
    log "INTEGRITY RESUME label=${label} status=success receipt=${integrity_receipt}"
  fi

  local caller_run_id run_id run_url
  if receipt_valid "$dispatch_receipt" "phase=backup_dispatched_resolved" "label=$label" \
    "requested_observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE"; then
    caller_run_id="$(json_field "$dispatch_receipt" caller_run_id)"
    run_id="$(json_field "$dispatch_receipt" run_id)"
    run_url="$(json_field "$dispatch_receipt" run_url)"
    log "BACKUP RESUME label=${label} caller_run_id=${caller_run_id} run_id=${run_id} url=${run_url}"
  else
    if [ -f "$dispatch_receipt" ] && receipt_valid "$dispatch_receipt" \
      "phase=backup_dispatch_requested" \
      "requested_observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE"; then
      caller_run_id="$(json_field "$dispatch_receipt" caller_run_id)"
      log "BACKUP DISCOVERY RESUME label=${label} caller_run_id=${caller_run_id}"
    else
      caller_run_id="integrity-monthly-${INTEGRITY_ENV}-${BATCH_ID}-after-${label}"
      rm -f "$backup_receipt" "$sync_receipt"
      rm -rf "$artifact_dir"
      write_receipt "$dispatch_receipt" "phase=backup_dispatch_requested" "label=$label" \
        "caller_run_id=$caller_run_id" \
        "requested_observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE"
      log "BACKUP DISPATCH label=${label} caller_run_id=${caller_run_id} repository=${BACKUP_REPOSITORY} max_days_per_run=0 observation_parquet_copy_mode=${OBSERVATION_PARQUET_COPY_MODE}"
      if ! gh workflow run "$BACKUP_WORKFLOW" --repo "$BACKUP_REPOSITORY" \
        -f "caller_run_id=${caller_run_id}" -f "max_days_per_run=0" \
        -f "observation_parquet_copy_mode=${OBSERVATION_PARQUET_COPY_MODE}"; then
        rm -f "$dispatch_receipt"
        fail "failed to dispatch backup for ${label}"
        return 1
      fi
    fi
    resolve_exact_run "$caller_run_id" "$dispatch_receipt" "$backup_receipt" "$sync_receipt" "$label" || return 1
    run_id="$(json_field "$dispatch_receipt" run_id)"
    run_url="$(json_field "$dispatch_receipt" run_url)"
  fi

  local state_root_key expected_hash effective_mode backup_status backup_conclusion report_path
  if receipt_valid "$backup_receipt" "phase=backup_succeeded" \
    "caller_run_id=$caller_run_id" "run_id=$run_id" \
    "observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE"; then
    state_root_key="$(json_field "$backup_receipt" state_root_key)"
    expected_hash="$(json_field "$backup_receipt" expected_hash)"
    effective_mode="$(json_field "$backup_receipt" observation_parquet_copy_mode)"
    backup_status="completed"
    backup_conclusion="success"
    [[ "$state_root_key" != /* && "$state_root_key" != *".."* ]] || { fail "persisted backup receipt has invalid state_root_key"; return 1; }
    [[ "$expected_hash" =~ ^[0-9a-f]{64}$ ]] || { fail "persisted backup receipt has invalid expected hash"; return 1; }
    log "BACKUP SUCCESS RESUME label=${label} caller_run_id=${caller_run_id} run_id=${run_id} url=${run_url} status=${backup_status} conclusion=${backup_conclusion} expected_hash=${expected_hash} observation_parquet_copy_mode=${effective_mode}"
  else
    wait_for_backup_run "$run_id" "$run_url" "$caller_run_id" "$label" "$dispatch_receipt" "$backup_receipt" "$sync_receipt" || return 1
    backup_status="completed"
    backup_conclusion="success"
    rm -rf "$artifact_dir"
    mkdir -p "$artifact_dir"
    gh run download "$run_id" --repo "$BACKUP_REPOSITORY" --name "$BACKUP_ARTIFACT" --dir "$artifact_dir" || { fail "failed to download report artifact for exact run ${run_id}"; return 1; }
    report_path="$artifact_dir/$BACKUP_REPORT"
    [ -f "$report_path" ] || { fail "exact run artifact lacks ${BACKUP_REPORT}"; return 1; }
    local report_identity
    if ! report_identity="$(validate_backup_report "$report_path")"; then
      rm -f "$dispatch_receipt" "$backup_receipt" "$sync_receipt"
      fail "exact run ${run_id} backup report is not acceptable; incomplete backup state cleared for a fresh contracted dispatch"
      return 1
    fi
    IFS=$'\t' read -r state_root_key expected_hash effective_mode <<<"$report_identity"
    write_receipt "$backup_receipt" "phase=backup_succeeded" "label=$label" \
      "caller_run_id=$caller_run_id" "run_id=$run_id" "run_url=$run_url" \
      "status=$backup_status" "conclusion=$backup_conclusion" \
      "state_root_key=$state_root_key" "expected_hash=$expected_hash" \
      "observation_parquet_copy_mode=$effective_mode"
    log "BACKUP SUCCESS label=${label} caller_run_id=${caller_run_id} run_id=${run_id} url=${run_url} status=${backup_status} conclusion=${backup_conclusion} state_root_key=${state_root_key} expected_hash=${expected_hash} observation_parquet_copy_mode=${effective_mode}"
  fi

  report_path="$artifact_dir/$BACKUP_REPORT"
  if [ ! -f "$report_path" ]; then
    mkdir -p "$artifact_dir"
    gh run download "$run_id" --repo "$BACKUP_REPOSITORY" --name "$BACKUP_ARTIFACT" --dir "$artifact_dir" || { fail "failed to restore report artifact for exact run ${run_id}"; return 1; }
  fi
  [ -f "$report_path" ] || { fail "exact run artifact lacks ${BACKUP_REPORT}"; return 1; }

  wait_for_local_checkpoint "$state_root_key" "$expected_hash" "$label" || return 1
  if receipt_valid "$sync_receipt" "phase=local_materialisation_verified" "label=$label" \
    "caller_run_id=$caller_run_id" "run_id=$run_id" "generation=$OBSERVATION_HISTORY_VERSION" \
    "state_root_key=$state_root_key" "expected_hash=$expected_hash" "observed_hash=$expected_hash" \
    "observation_parquet_copy_mode=$OBSERVATION_PARQUET_COPY_MODE"; then
    log "LOCAL MATERIALISATION RESUME label=${label} run_id=${run_id} expected_hash=${expected_hash} observation_parquet_copy_mode=${OBSERVATION_PARQUET_COPY_MODE}"
  else
    wait_for_local_materialisation "$report_path" "$state_root_key" "$expected_hash" \
      "$caller_run_id" "$run_id" "$label" "$sync_receipt" || return 1
  fi

  touch "$ok_marker"
  log "MONTH COMPLETE label=${label} integrity=success caller_run_id=${caller_run_id} run_id=${run_id} url=${run_url} backup_status=${backup_status} backup_conclusion=${backup_conclusion} observation_parquet_copy_mode=${OBSERVATION_PARQUET_COPY_MODE} expected_hash=${expected_hash} local_hash=${expected_hash} final_month_complete=true"
}

preflight
run_batch 2025-05-01 2025-05-31 2025-05
run_batch 2025-06-01 2025-06-30 2025-06
run_batch 2025-07-01 2025-07-31 2025-07
run_batch 2025-08-01 2025-08-31 2025-08
run_batch 2025-09-01 2025-09-30 2025-09
run_batch 2025-10-01 2025-10-31 2025-10
run_batch 2025-11-01 2025-11-30 2025-11
run_batch 2025-12-01 2025-12-31 2025-12
run_batch 2026-01-01 2026-01-31 2026-01
log "ALL BATCHES COMPLETED"
echo "Summary: $SUMMARY"

