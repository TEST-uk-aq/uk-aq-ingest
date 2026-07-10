#!/usr/bin/env python3
"""Validate scheduler jobs.toml and generate the D1 sync SQL payload."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import tomllib


DEFAULT_CONFIG_VERSION = 1
DEFAULT_SCHEDULER_NAME = "uk-aq-cron-scheduler-ingest"
DEFAULT_TIMEZONE = "UTC"
DEFAULT_GITHUB_REF = "main"
DEFAULT_CLOUD_RUN_METHOD = "POST"
DEPLOYMENT_PENDING_CLOUD_RUN_URL = "https://deployment-pending.invalid/run"

MONTH_NAME_TO_NUMBER = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

DOW_NAME_TO_NUMBER = {
    "SUN": 0,
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
}

SQL_COLUMNS = [
    "job_key",
    "enabled",
    "target_type",
    "cron_expr",
    "timezone",
    "github_repo",
    "github_workflow_file",
    "github_ref",
    "github_inputs_json",
    "cloud_run_url",
    "cloud_run_method",
    "cloud_run_headers_json",
    "cloud_run_body_json",
    "dry_run",
    "notes",
]


class JobsConfigError(ValueError):
    """Raised when the scheduler jobs.toml file is invalid."""


class SqlExpression(str):
    """Marker for raw SQL fragments that should not be quoted."""


def trim_text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def require_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise JobsConfigError(f"Invalid {field_name}: expected a string")
    text = value.strip()
    if not text:
        raise JobsConfigError(f"Invalid {field_name}: expected a non-empty string")
    return text


def optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def require_bool(value: Any, field_name: str) -> int:
    if not isinstance(value, bool):
        raise JobsConfigError(f"Invalid {field_name}: expected true or false")
    return 1 if value else 0


def require_timezone(value: Any) -> str:
    timezone = optional_string(value) or DEFAULT_TIMEZONE
    if timezone not in {"UTC", "Etc/UTC"}:
        raise JobsConfigError(f"Unsupported timezone {timezone!r}; expected UTC")
    return timezone


def normalize_json_table(value: Any, field_name: str) -> str:
    if value is None:
        return "{}"
    if not isinstance(value, dict):
        raise JobsConfigError(f"Invalid {field_name}: expected a TOML table")
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def normalize_cron_atom(
    token: str,
    field_name: str,
    min_value: int,
    max_value: int,
    names: dict[str, int] | None = None,
) -> int:
    normalized = token.strip().upper()
    if not normalized:
        raise JobsConfigError(f"Invalid {field_name}: empty token")

    if names and normalized in names:
        return names[normalized]

    if field_name == "day-of-week" and normalized == "7":
        return 0

    try:
        parsed = int(normalized)
    except ValueError as exc:
        raise JobsConfigError(f"Invalid {field_name}: {token!r} is not a valid value") from exc

    if parsed < min_value or parsed > max_value:
        raise JobsConfigError(f"Invalid {field_name}: {parsed} outside {min_value}-{max_value}")
    return 0 if field_name == "day-of-week" and parsed == 7 else parsed


def expand_cron_token(
    token: str,
    field_name: str,
    min_value: int,
    max_value: int,
    names: dict[str, int] | None = None,
) -> None:
    normalized = token.strip().upper()
    if not normalized:
        raise JobsConfigError(f"Invalid {field_name}: empty cron token")

    base = normalized
    step = 1
    if "/" in normalized:
        parts = normalized.split("/")
        if len(parts) != 2:
            raise JobsConfigError(f"Invalid {field_name}: {token!r}")
        base, step_text = parts
        try:
            step = int(step_text.strip())
        except ValueError as exc:
            raise JobsConfigError(f"Invalid {field_name}: step must be a positive integer") from exc
        if step <= 0:
            raise JobsConfigError(f"Invalid {field_name}: step must be a positive integer")

    if base == "*":
        start = min_value
        end = max_value
    elif "-" in base:
        range_parts = base.split("-")
        if len(range_parts) != 2:
            raise JobsConfigError(f"Invalid {field_name}: {token!r}")
        start = normalize_cron_atom(range_parts[0], field_name, min_value, max_value, names)
        end = normalize_cron_atom(range_parts[1], field_name, min_value, max_value, names)
    else:
        start = normalize_cron_atom(base, field_name, min_value, max_value, names)
        end = start

    if start > end:
        raise JobsConfigError(f"Invalid {field_name}: range start must be <= range end")

    for _ in range(start, end + 1, step):
        pass


def validate_cron_expression(expr: str, job_key: str) -> str:
    cron_expr = trim_text(expr)
    if not cron_expr:
        raise JobsConfigError(f"Invalid scheduler job {job_key}: missing cron_expr")

    fields = cron_expr.split()
    if len(fields) != 5:
        raise JobsConfigError(f"Invalid scheduler job {job_key}: cron_expr must have five fields")

    expand_cron_token(fields[0], "minute", 0, 59)
    expand_cron_token(fields[1], "hour", 0, 23)
    expand_cron_token(fields[2], "day-of-month", 1, 31)
    expand_cron_token(fields[3], "month", 1, 12, MONTH_NAME_TO_NUMBER)
    expand_cron_token(fields[4], "day-of-week", 0, 7, DOW_NAME_TO_NUMBER)
    return cron_expr


def load_jobs_config(jobs_file: Path) -> dict[str, Any]:
    try:
        with jobs_file.open("rb") as handle:
            config = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise JobsConfigError(f"Failed to parse {jobs_file}: {exc}") from exc

    if not isinstance(config, dict):
        raise JobsConfigError(f"{jobs_file} must contain a TOML table at the top level")
    return config


def validate_job(job_key: str, raw_job: Any) -> dict[str, Any]:
    if not isinstance(raw_job, dict):
        raise JobsConfigError(f"Invalid scheduler job {job_key}: expected a TOML table")

    allowed_keys = {
        "enabled",
        "target_type",
        "cron_expr",
        "timezone",
        "github_repo",
        "github_workflow_file",
        "github_ref",
        "github_inputs",
        "cloud_run_url",
        "cloud_run_url_managed_by_deploy",
        "cloud_run_method",
        "cloud_run_headers",
        "cloud_run_body",
        "dry_run",
        "notes",
    }
    unknown_keys = sorted(set(raw_job) - allowed_keys)
    if unknown_keys:
        joined = ", ".join(unknown_keys)
        raise JobsConfigError(f"Invalid scheduler job {job_key}: unknown fields: {joined}")

    enabled = require_bool(raw_job.get("enabled"), f"scheduler_jobs.enabled for {job_key}")
    target_type = require_string(raw_job.get("target_type"), f"scheduler_jobs.target_type for {job_key}")
    if target_type not in {"github_workflow", "cloud_run"}:
        raise JobsConfigError(f"Invalid scheduler job {job_key}: unsupported target_type {target_type!r}")

    cron_expr = validate_cron_expression(raw_job.get("cron_expr"), job_key)
    timezone = require_timezone(raw_job.get("timezone"))
    dry_run = require_bool(raw_job.get("dry_run"), f"scheduler_jobs.dry_run for {job_key}")
    notes = optional_string(raw_job.get("notes"))

    job = {
        "job_key": trim_text(job_key),
        "enabled": enabled,
        "target_type": target_type,
        "cron_expr": cron_expr,
        "timezone": timezone,
        "github_repo": None,
        "github_workflow_file": None,
        # The D1 schema keeps github_ref NOT NULL for every target type.
        "github_ref": DEFAULT_GITHUB_REF,
        "github_inputs_json": None,
        "cloud_run_url": None,
        "cloud_run_method": DEFAULT_CLOUD_RUN_METHOD,
        "cloud_run_headers_json": None,
        "cloud_run_body_json": None,
        "dry_run": dry_run,
        "notes": notes,
        "cloud_run_url_managed_by_deploy": False,
    }

    if not job["job_key"]:
        raise JobsConfigError("Invalid scheduler job: job_key must not be empty")

    if target_type == "github_workflow":
        forbidden = {
            "cloud_run_url",
            "cloud_run_url_managed_by_deploy",
            "cloud_run_method",
            "cloud_run_headers",
            "cloud_run_body",
        } & set(raw_job)
        if forbidden:
            joined = ", ".join(sorted(forbidden))
            raise JobsConfigError(f"Invalid scheduler job {job_key}: github_workflow does not use {joined}")

        job["github_repo"] = require_string(raw_job.get("github_repo"), f"scheduler_jobs.github_repo for {job_key}")
        job["github_workflow_file"] = require_string(
            raw_job.get("github_workflow_file"),
            f"scheduler_jobs.github_workflow_file for {job_key}",
        )
        job["github_ref"] = optional_string(raw_job.get("github_ref")) or DEFAULT_GITHUB_REF
        job["github_inputs_json"] = normalize_json_table(
            raw_job.get("github_inputs"),
            f"scheduler_jobs.github_inputs for {job_key}",
        )
    else:
        forbidden = {"github_repo", "github_workflow_file", "github_ref", "github_inputs"} & set(raw_job)
        if forbidden:
            joined = ", ".join(sorted(forbidden))
            raise JobsConfigError(f"Invalid scheduler job {job_key}: cloud_run does not use {joined}")

        managed_url = raw_job.get("cloud_run_url_managed_by_deploy", False)
        if not isinstance(managed_url, bool):
            raise JobsConfigError(
                f"Invalid scheduler_jobs.cloud_run_url_managed_by_deploy for {job_key}: expected true or false"
            )
        if managed_url and "cloud_run_url" in raw_job:
            raise JobsConfigError(
                f"Invalid scheduler job {job_key}: set cloud_run_url or cloud_run_url_managed_by_deploy, not both"
            )
        if managed_url:
            job["cloud_run_url"] = DEPLOYMENT_PENDING_CLOUD_RUN_URL
            job["cloud_run_url_managed_by_deploy"] = True
        else:
            job["cloud_run_url"] = require_string(
                raw_job.get("cloud_run_url"),
                f"scheduler_jobs.cloud_run_url for {job_key}",
            )
        job["cloud_run_method"] = optional_string(raw_job.get("cloud_run_method")) or DEFAULT_CLOUD_RUN_METHOD
        job["cloud_run_headers_json"] = normalize_json_table(
            raw_job.get("cloud_run_headers"),
            f"scheduler_jobs.cloud_run_headers for {job_key}",
        )
        job["cloud_run_body_json"] = normalize_json_table(
            raw_job.get("cloud_run_body"),
            f"scheduler_jobs.cloud_run_body for {job_key}",
        )

    return job


def validate_jobs_config(config: dict[str, Any], expected_scheduler_name: str = DEFAULT_SCHEDULER_NAME) -> dict[str, Any]:
    unknown_root_keys = sorted(set(config) - {"config_version", "scheduler_name", "jobs"})
    if unknown_root_keys:
        joined = ", ".join(unknown_root_keys)
        raise JobsConfigError(f"Unknown top-level fields: {joined}")

    config_version = config.get("config_version")
    if config_version != DEFAULT_CONFIG_VERSION:
        raise JobsConfigError(f"Unsupported config_version {config_version!r}; expected {DEFAULT_CONFIG_VERSION}")

    scheduler_name = require_string(config.get("scheduler_name"), "scheduler_name")
    if scheduler_name != expected_scheduler_name:
        raise JobsConfigError(
            f"Unsupported scheduler_name {scheduler_name!r}; expected {expected_scheduler_name!r}"
        )

    jobs = config.get("jobs")
    if not isinstance(jobs, dict):
        raise JobsConfigError("jobs must be a TOML table of job definitions")

    normalized_jobs = [validate_job(job_key, jobs[job_key]) for job_key in sorted(jobs)]
    return {
        "config_version": DEFAULT_CONFIG_VERSION,
        "scheduler_name": scheduler_name,
        "jobs": normalized_jobs,
        "job_count": len(normalized_jobs),
    }


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, SqlExpression):
        return str(value)
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"Unsupported SQL literal value: {value!r}")


def render_upsert_statement(job: dict[str, Any]) -> str:
    columns = SQL_COLUMNS + ["updated_at"]
    values = [job[column] for column in SQL_COLUMNS] + [SqlExpression("current_timestamp")]
    insert_values = ",\n  ".join(sql_literal(value) for value in values)

    cloud_run_url_update = (
        "  cloud_run_url = scheduler_jobs.cloud_run_url"
        if job.get("cloud_run_url_managed_by_deploy")
        else "  cloud_run_url = excluded.cloud_run_url"
    )
    update_lines = [
        "  enabled = excluded.enabled",
        "  target_type = excluded.target_type",
        "  cron_expr = excluded.cron_expr",
        "  timezone = excluded.timezone",
        "  github_repo = excluded.github_repo",
        "  github_workflow_file = excluded.github_workflow_file",
        "  github_ref = excluded.github_ref",
        "  github_inputs_json = excluded.github_inputs_json",
        cloud_run_url_update,
        "  cloud_run_method = excluded.cloud_run_method",
        "  cloud_run_headers_json = excluded.cloud_run_headers_json",
        "  cloud_run_body_json = excluded.cloud_run_body_json",
        "  dry_run = excluded.dry_run",
        "  notes = excluded.notes",
        "  updated_at = current_timestamp",
    ]

    statement_lines = [
        f"-- job_key: {job['job_key']}",
        "insert into scheduler_jobs (",
        "  " + ",\n  ".join(columns),
        ") values (",
        f"  {insert_values}",
        ")",
        "on conflict(job_key) do update set",
        ",\n".join(update_lines),
        ";",
    ]
    return "\n".join(statement_lines)


def render_sync_sql(manifest: dict[str, Any]) -> str:
    statements = [
        "-- Generated from cloudflare/scheduler/jobs.toml",
        "-- Do not edit by hand; update jobs.toml and rerun the sync workflow.",
    ]
    for job in manifest["jobs"]:
        statements.append("")
        statements.append(render_upsert_statement(job))
    statements.append("")
    return "\n".join(statements)


def build_expected_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "config_version": manifest["config_version"],
        "scheduler_name": manifest["scheduler_name"],
        "job_count": manifest["job_count"],
        "jobs": [
            {column: job[column] for column in SQL_COLUMNS}
            for job in manifest["jobs"]
        ],
        "deployment_managed_cloud_run_url_job_keys": [
            job["job_key"]
            for job in manifest["jobs"]
            if job.get("cloud_run_url_managed_by_deploy")
        ],
    }


def write_text_file(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--jobs-file",
        type=Path,
        default=Path("cloudflare/scheduler/jobs.toml"),
        help="Path to the canonical jobs.toml file.",
    )
    parser.add_argument(
        "--sql-file",
        type=Path,
        required=True,
        help="Where to write the generated D1 SQL file.",
    )
    parser.add_argument(
        "--json-file",
        type=Path,
        required=True,
        help="Where to write the canonical expected job manifest as JSON.",
    )
    parser.add_argument(
        "--scheduler-name",
        default=DEFAULT_SCHEDULER_NAME,
        help="Expected scheduler_name value.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = validate_jobs_config(load_jobs_config(args.jobs_file), args.scheduler_name)
    write_text_file(args.sql_file, render_sync_sql(manifest))
    write_text_file(
        args.json_file,
        json.dumps(build_expected_manifest(manifest), indent=2, sort_keys=True) + "\n",
    )
    print(
        f"Validated {manifest['job_count']} jobs from {args.jobs_file} and wrote {args.sql_file}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
