from __future__ import annotations

import importlib.util
import json
import sqlite3
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


JOB_KEYS = [
    "uk_aq_blondon_communities",
    "uk_aq_blondon_nodes",
    "uk_aq_openaq_safety",
    "uk_aq_scomm",
    "uk_aq_sos",
]

DEPLOY_WORKFLOWS = {
    "uk_aq_blondon_communities": "uk_aq_blondon_communities_cloud_run_deploy.yml",
    "uk_aq_blondon_nodes": "uk_aq_blondon_nodes_cloud_run_deploy.yml",
    "uk_aq_openaq_safety": "uk_aq_openaq_cloud_run_deploy.yml",
    "uk_aq_scomm": "uk_aq_scomm_cloud_run_deploy.yml",
    "uk_aq_sos": "uk_aq_sos_cloud_run_deploy.yml",
}

SERVICE_ENTRYPOINTS = {
    "uk_aq_blondon_communities": "workers/uk_aq_blondon_communities_cloud_run/run_service.ts",
    "uk_aq_blondon_nodes": "workers/uk_aq_blondon_nodes_cloud_run/run_service.py",
    "uk_aq_openaq_safety": "workers/uk_aq_openaq_cloud_run/run_service.ts",
    "uk_aq_scomm": "workers/uk_aq_sensorcommunity_cloud_run/run_service.mjs",
    "uk_aq_sos": "workers/uk_aq_sos_cloud_run/run_service.ts",
}


class CloudflareSchedulerIngestJobsSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.jobs_file = ROOT / "cloudflare/scheduler/jobs.toml"

    def manifest(self) -> dict:
        return sync_jobs.validate_jobs_config(sync_jobs.load_jobs_config(self.jobs_file))

    def test_jobs_toml_defines_five_managed_cloud_run_jobs(self) -> None:
        manifest = self.manifest()
        self.assertEqual(manifest["scheduler_name"], "uk-aq-cron-scheduler-ingest")
        self.assertEqual([job["job_key"] for job in manifest["jobs"]], JOB_KEYS)
        self.assertTrue(all(job["target_type"] == "cloud_run" for job in manifest["jobs"]))
        self.assertTrue(all(job["cloud_run_url_managed_by_deploy"] for job in manifest["jobs"]))
        self.assertTrue(all(job["cloud_run_url"] == sync_jobs.DEPLOYMENT_PENDING_CLOUD_RUN_URL for job in manifest["jobs"]))
        self.assertTrue(all(job["cloud_run_method"] == "POST" for job in manifest["jobs"]))
        self.assertTrue(all(job["dry_run"] == 1 for job in manifest["jobs"]))
        self.assertTrue(all(job["github_repo"] is None for job in manifest["jobs"]))
        self.assertTrue(all("x-uk-aq" not in (job["cloud_run_headers_json"] or "") for job in manifest["jobs"]))

    def test_schedules_and_bodies_match_service_contracts(self) -> None:
        jobs = {job["job_key"]: job for job in self.manifest()["jobs"]}
        for job_key in JOB_KEYS:
            expected_cron = "*/30 * * * *" if job_key == "uk_aq_openaq_safety" else "*/15 * * * *"
            self.assertEqual(jobs[job_key]["cron_expr"], expected_cron)
        self.assertEqual(jobs["uk_aq_blondon_nodes"]["cloud_run_body_json"], "{}")
        for job_key in set(JOB_KEYS) - {"uk_aq_blondon_nodes"}:
            self.assertEqual(jobs[job_key]["cloud_run_body_json"], '{"trigger_mode":"safety"}')

    def test_generated_sql_preserves_managed_urls_on_resync(self) -> None:
        manifest = self.manifest()
        sql = sync_jobs.render_sync_sql(manifest)
        self.assertEqual(sql.count("cloud_run_url = scheduler_jobs.cloud_run_url"), 5)

        connection = sqlite3.connect(":memory:")
        connection.executescript((ROOT / "cloudflare/scheduler/migrations/0001_scheduler_schema.sql").read_text())
        connection.executescript(sql)
        deployed_url = "https://example-service.run.app"
        connection.execute(
            "update scheduler_jobs set cloud_run_url = ? where job_key = ?",
            (deployed_url, "uk_aq_sos"),
        )
        connection.executescript(sql)
        stored = connection.execute(
            "select cloud_run_url from scheduler_jobs where job_key = 'uk_aq_sos'"
        ).fetchone()[0]
        self.assertEqual(stored, deployed_url)

    def test_bootstrap_seed_matches_jobs_toml(self) -> None:
        connection = sqlite3.connect(":memory:")
        connection.executescript(
            (ROOT / "cloudflare/scheduler/migrations/0001_scheduler_schema.sql").read_text(encoding="utf-8")
        )
        connection.executescript(
            (ROOT / "cloudflare/scheduler/seeds/0001_cloud_run_jobs.sql").read_text(encoding="utf-8")
        )
        rows = connection.execute(
            "select job_key, cron_expr, cloud_run_url, dry_run from scheduler_jobs order by job_key"
        ).fetchall()
        self.assertEqual([row[0] for row in rows], JOB_KEYS)
        self.assertTrue(all(row[2] == sync_jobs.DEPLOYMENT_PENDING_CLOUD_RUN_URL for row in rows))
        self.assertTrue(all(row[3] == 1 for row in rows))

    def test_main_writes_sql_and_expected_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sql_path = Path(tmpdir) / "jobs.sql"
            json_path = Path(tmpdir) / "jobs.json"
            self.assertEqual(sync_jobs.main([
                "--jobs-file", str(self.jobs_file),
                "--sql-file", str(sql_path),
                "--json-file", str(json_path),
            ]), 0)
            expected = json.loads(json_path.read_text())
        self.assertEqual(expected["deployment_managed_cloud_run_url_job_keys"], JOB_KEYS)

    def test_each_deploy_workflow_reconciles_only_its_job(self) -> None:
        retired_secret = "UK_AQ_CLOUD_RUN_" + "DISPATCH_SECRET"
        for job_key, workflow_name in DEPLOY_WORKFLOWS.items():
            with self.subTest(job_key=job_key):
                text = (ROOT / ".github/workflows" / workflow_name).read_text()
                self.assertIn(f"SCHEDULER_D1_JOB_KEY: {job_key}", text)
                self.assertIn("uk_aq_reconcile_ingest_scheduler_url.sh", text)
                self.assertIn("UK_AQ_EDGE_UPSTREAM_SECRET", text)
                self.assertIn("X-UK-AQ-Upstream-Auth", text)
                self.assertIn("--allow-unauthenticated", text)
                self.assertNotIn(retired_secret, text)

    def test_each_service_requires_shared_application_auth(self) -> None:
        retired_secret = "UK_AQ_CLOUD_RUN_" + "DISPATCH_SECRET"
        for job_key, relative_path in SERVICE_ENTRYPOINTS.items():
            with self.subTest(job_key=job_key):
                text = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn("UK_AQ_EDGE_UPSTREAM_SECRET", text)
                self.assertIn("x-uk-aq-upstream-auth", text)
                self.assertIn("x-uk-aq-dispatch-secret", text)
                self.assertIn("403", text)
                self.assertNotIn(retired_secret, text)

    def test_openaq_keeps_service_side_safety_noop_and_task_auth(self) -> None:
        text = (ROOT / "workers/uk_aq_openaq_cloud_run/run_job.ts").read_text(encoding="utf-8")
        self.assertIn('OPENAQ_TRIGGER_MODE === "safety"', text)
        self.assertIn('logSummary("safety_noop_recent_run"', text)
        self.assertIn('"x-uk-aq-upstream-auth": UK_AQ_EDGE_UPSTREAM_SECRET', text)

    def test_invalid_cron_is_rejected(self) -> None:
        config = sync_jobs.load_jobs_config(self.jobs_file)
        config["jobs"]["uk_aq_sos"]["cron_expr"] = "61 * * * *"
        with self.assertRaises(sync_jobs.JobsConfigError):
            sync_jobs.validate_jobs_config(config)


if __name__ == "__main__":
    unittest.main()
