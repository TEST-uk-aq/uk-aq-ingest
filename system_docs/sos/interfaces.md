# UK-AIR SOS interfaces

## Purpose

This document defines the bounded response and child-result interfaces between:

- `ingest_sos`;
- `workers/uk_aq_sos_cloud_run/run_job.ts`; and
- `workers/uk_aq_sos_cloud_run/run_service.ts`.

The required behavioural interpretation is in [`contract.md`](contract.md).

## Ingest response fields

The Cloud Run path retains only the bounded fields needed by callers and operators.

Recognised fields are:

| Field | Meaning |
|---|---|
| `ok` | Whether the inner HTTP response is a successful 2xx response. Recognised dependency failures use `false` while still being completed child results. |
| `status` | Logical ingest status, such as `ok`, `skipped` or `upstream_unavailable`. |
| `run_status` | Persisted run classification, such as `succeeded`, `partial`, `failed` or `skipped`. |
| `run_message` | Bounded human-readable run summary. |
| `reason` | Bounded machine-readable skip reason where applicable. |
| `partial` | Whether selected work was incomplete. |
| `stopped_reason` | Machine-readable reason for a partial stop. |
| `upstream_status` | Real upstream HTTP status, or `null` when no upstream HTTP response was received. |
| `upstream_failure_kind` | Structured SOS fetch failure kind. |
| `connector_http_status` | Connector-facing HTTP status selected from the failure contract. |
| `runtime_deadline_failure_count` | Number of in-flight timeseries requests that failed with the shared runtime deadline. |
| `runtime_deadline_timeseries_sample` | Bounded sample of affected internal timeseries IDs. |
| `individual_error_count` | Count of failures reported through the individual per-timeseries path. |
| `series_polled` | Number of timeseries successfully polled. |
| `observations_upserted` | Number of observation rows upserted. |
| `connector_id` | Internal connector ID when available. |

Unknown fields are not accepted in the child-result payload.

## Child-result file

The Cloud Run service creates one unique temporary file per invocation and passes its path to the child in `SOS_RUN_RESULT_PATH`.

The child writes one JSON object:

```json
{
  "httpStatus": 200,
  "payload": {
    "ok": true,
    "status": "skipped",
    "run_status": "skipped",
    "run_message": "not_due",
    "reason": "not_due",
    "connector_id": 1
  }
}
```

`httpStatus` is the outer HTTP status that `run_service.ts` returns when the child result is valid.

### Required payload fields

Every valid child-result payload requires:

- boolean `ok`;
- non-empty bounded `status`;
- non-empty bounded `run_status`;
- non-empty bounded `run_message`.

Optional fields must match the types and meanings in the table above.

### Bounds

The validator enforces:

- `status`, `run_status` and `upstream_failure_kind`: maximum 64 characters;
- `reason` and `stopped_reason`: maximum 128 characters;
- `run_message`: maximum 500 characters;
- non-negative integer counters;
- `runtime_deadline_timeseries_sample`: at most 10 non-negative integer IDs;
- `connector_id`: a non-negative integer or a bounded non-empty string.

The result MUST contain only approved payload fields. An object containing only `httpStatus`, an empty payload, an incomplete skipped payload, unbounded strings or an oversized sample is invalid.

## Service decision matrix

| Child process | Result file | Outer result |
|---|---|---|
| Exit code 0 | Valid child result | Return the declared `httpStatus` and bounded payload. |
| Exit code 0 | Empty file | HTTP 500 with `error: missing_child_result`. |
| Exit code 0 | Malformed JSON or invalid schema | HTTP 500 with `error: invalid_child_result`. |
| Non-zero exit | Any file state | Generic HTTP 500 wrapper response. |

The outer service also adds request-level metadata such as:

- `trigger_mode`;
- `current_task_name`;
- child exit `code`.

The service MUST NOT parse arbitrary child stdout or logs as a result fallback.

## HTTP status meanings

| HTTP status | Meaning |
|---|---|
| 200 | Successful complete result or intentional skip. |
| 207 | Partial ingest, including a genuine runtime-budget stop. |
| Real upstream status | Preserved when the failure kind is `http` and the structured dependency contract is valid. HTTP 502 is the common gateway-failure example. |
| 503 | Probe request timeout or runtime deadline where no upstream HTTP response was received. |
| 500 | Wrapper, local, unknown or invalid-result failure. |

An upstream HTTP error or a timeout-derived HTTP 503 is recognised as a dependency result only when the structured body passes the validation rules in [`contract.md`](contract.md).

## Temporary-file lifecycle

`run_service.ts` MUST:

1. create a unique temporary result file for each accepted POST request;
2. pass only that path to the child process;
3. read it only after the child exits successfully;
4. validate the complete object before returning it;
5. remove the file in `finally`;
6. never reuse stale content from a previous invocation.

The file path is per invocation. It is not persistent deployment configuration and must not be added to the project environment-variable inventory.