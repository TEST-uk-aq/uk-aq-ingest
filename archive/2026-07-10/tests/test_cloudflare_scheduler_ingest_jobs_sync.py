from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "cloudflare/scheduler/scripts/sync_jobs.py"
SPEC = importlib.util.spec_from_file_location("cloudflare_scheduler_ingest_sync_jobs", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
sync_jobs = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = sync_jobs
SPEC.loader.exec_module(sync_jobs)


class CloudflareSchedulerIngestJobsSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.jobs_file = ROOT / "cloudflare/scheduler/jobs.toml"

    def test_jobs_toml_generates_expected_rows(self) -> None:
        manifest = sync_jobs.validate_jobs_config(sync_jobs.load_jobs_config(self.jobs_file))

        self.assertEqual(manifest["config_version"], 1)
        self.assertEqual(manifest["scheduler_name"], "uk-aq-scheduler-ingest")
        self.assertEqual(manifest["job_count"], 5)
        self.assertEqual(
            [job["job_key"] for job in manifest["jobs"]],
            [
                "uk_aq_blondon_communities",
                "uk_aq_blondon_nodes",
                "uk_aq_scomm",
                "uk_aq_sos",
                "uk_aq_openaq_safety",
            ],
        )

        openaq = next(job for job in manifest["jobs"] if job["job_key"] == "uk_aq_openaq_safety")
        self.assertTrue(openaq["dry_run"])
        self.assertTrue(openaq["safety_only"])
        self.assertEqual(openaq["interval_minutes"], 30)

    def test_rendered_manifest_is_stable(self) -> None:
        manifest = sync_jobs.validate_jobs_config(sync_jobs.load_jobs_config(self.jobs_file))
        with tempfile.TemporaryDirectory() as tmpdir:
            json_path = Path(tmpdir) / "ingest_scheduler_jobs.json"
            sync_jobs.write_text_file(json_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
            round_trip = json.loads(json_path.read_text(encoding="utf-8"))

        self.assertEqual(round_trip["job_count"], 5)
        self.assertTrue(all(job["dry_run"] for job in round_trip["jobs"]))

    def test_invalid_cron_expression_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            bad_jobs = tmpdir_path / "jobs.toml"
            bad_jobs.write_text(
                self.jobs_file.read_text(encoding="utf-8").replace("*/15 * * * *", "*/15 * * *", 1),
                encoding="utf-8",
            )

            with self.assertRaises(sync_jobs.JobsConfigError):
                sync_jobs.validate_jobs_config(sync_jobs.load_jobs_config(bad_jobs))


if __name__ == "__main__":
    unittest.main()
