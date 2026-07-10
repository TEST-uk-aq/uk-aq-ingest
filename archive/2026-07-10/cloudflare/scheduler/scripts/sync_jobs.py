#!/usr/bin/env python3
"""Validate ingest scheduler jobs.toml and generate a normalized manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import tomllib


DEFAULT_CONFIG_VERSION = 1
DEFAULT_SCHEDULER_NAME = "uk-aq-scheduler-ingest"
ALLOWED_ROOT_KEYS = {"config_version", "scheduler_name", "jobs"}
ALLOWED_JOB_KEYS = {
    "enabled",
    "label",
    "target_label",
    "state_source",
    "connector_code",
    "cron",
    "interval_minutes",
    "min_gap_minutes",
    "stale_after_minutes",
    "dry_run",
    "safety_only",
    "notes",
}


class JobsConfigError(ValueError):
    """Raised when the ingest scheduler jobs.toml file is invalid."""


def trim_text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def require_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise JobsConfigError(f"Invalid {field_name}: expected a string")
    text = value.strip()
    if not text:
        raise JobsConfigError(f"Invalid {field_name}: expected a non-empty string")
    return text


def require_bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise JobsConfigError(f"Invalid {field_name}: expected true or false")
    return value


def require_positive_int(value: Any, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise JobsConfigError(f"Invalid {field_name}: expected a positive integer")
    if value <= 0:
        raise JobsConfigError(f"Invalid {field_name}: expected a positive integer")
    return value


def validate_cron(expr: Any, job_key: str) -> str:
    cron = require_string(expr, f"jobs.{job_key}.cron")
    if len(cron.split()) != 5:
        raise JobsConfigError(f"Invalid jobs.{job_key}.cron: expected a five-field cron expression")
    return cron


def load_jobs_config(jobs_file: Path) -> dict[str, Any]:
    try:
        with jobs_file.open("rb") as handle:
            config = tomllib.load(handle)
    except tomllib.TOMLDecodeError as exc:
        raise JobsConfigError(f"Failed to parse {jobs_file}: {exc}") from exc

    if not isinstance(config, dict):
        raise JobsConfigError(f"{jobs_file} must contain a TOML table at the top level")
    return config


def normalize_job(job_key: str, raw_job: Any) -> dict[str, Any]:
    if not isinstance(raw_job, dict):
        raise JobsConfigError(f"Invalid jobs.{job_key}: expected a TOML table")

    unknown_keys = sorted(set(raw_job) - ALLOWED_JOB_KEYS)
    if unknown_keys:
        joined = ", ".join(unknown_keys)
        raise JobsConfigError(f"Invalid jobs.{job_key}: unknown fields: {joined}")

    normalized = {
        "job_key": trim_text(job_key),
        "enabled": require_bool(raw_job.get("enabled"), f"jobs.{job_key}.enabled"),
        "label": require_string(raw_job.get("label"), f"jobs.{job_key}.label"),
        "target_label": require_string(raw_job.get("target_label"), f"jobs.{job_key}.target_label"),
        "state_source": require_string(raw_job.get("state_source"), f"jobs.{job_key}.state_source"),
        "connector_code": require_string(raw_job.get("connector_code"), f"jobs.{job_key}.connector_code"),
        "cron": validate_cron(raw_job.get("cron"), job_key),
        "interval_minutes": require_positive_int(raw_job.get("interval_minutes"), f"jobs.{job_key}.interval_minutes"),
        "min_gap_minutes": require_positive_int(raw_job.get("min_gap_minutes"), f"jobs.{job_key}.min_gap_minutes"),
        "stale_after_minutes": require_positive_int(raw_job.get("stale_after_minutes"), f"jobs.{job_key}.stale_after_minutes"),
        "dry_run": require_bool(raw_job.get("dry_run"), f"jobs.{job_key}.dry_run"),
        "safety_only": require_bool(raw_job.get("safety_only"), f"jobs.{job_key}.safety_only")
        if "safety_only" in raw_job
        else False,
        "notes": trim_text(raw_job.get("notes")) or None,
    }
    if not normalized["job_key"]:
        raise JobsConfigError("Invalid jobs entry: job_key must not be empty")
    return normalized


def validate_jobs_config(config: dict[str, Any], expected_scheduler_name: str = DEFAULT_SCHEDULER_NAME) -> dict[str, Any]:
    unknown_root_keys = sorted(set(config) - ALLOWED_ROOT_KEYS)
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

    normalized_jobs = [normalize_job(job_key, raw_job) for job_key, raw_job in jobs.items()]
    return {
        "config_version": DEFAULT_CONFIG_VERSION,
        "scheduler_name": scheduler_name,
        "job_count": len(normalized_jobs),
        "jobs": normalized_jobs,
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
        "--json-file",
        type=Path,
        required=True,
        help="Where to write the normalized job manifest as JSON.",
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
    write_text_file(args.json_file, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(
        f"Validated {manifest['job_count']} jobs from {args.jobs_file} and wrote {args.json_file}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
