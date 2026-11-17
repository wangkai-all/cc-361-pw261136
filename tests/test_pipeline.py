"""Split pipeline: fetch writes staging, a separate seal stage writes the vault.

These tests exercise the two stages as separate steps (never with the combined
worker) to prove the architectural claim: the process that touches a source is
not the process that writes chunks, and staging is only trusted if its bytes
still match the digest recorded at fetch time.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest

from tests.support import VaultCase

from app.capture_worker import (
    claim_fetch_job,
    claim_seal_job,
    enqueue_capture,
    run_fetch,
    run_seal,
)
from app.util import sha256_hex
from app.vault import StagingMismatch, get_object, load_manifest, read_object_bytes

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA = (
    b"<html><body>\n" + b"".join(
        f'<article data-seq="{i}"><h2>pipeline / a / b</h2>'
        f'<span class="value">{i}</span></article>\n'.encode()
        for i in range(300)
    ) + b"</body></html>\n"
)


class SplitPipelineTests(VaultCase):
    def _fetch(self, src):
        job_id, cap_id = enqueue_capture(self.conn, src, inline=False)
        job = claim_fetch_job(self.conn, "fetch-test", inline=False)
        self.assertIsNotNone(job)
        result = run_fetch(self.cfg, self.conn, job, worker="fetch-test")
        self.assertEqual(result["status"], "fetched", result)
        return job_id, cap_id, result

    def test_fetch_only_stages_and_writes_no_vault_bytes(self):
        src = self.fresh_source("split-fetch", DATA)
        _, cap_id, result = self._fetch(src)

        # Staging exists, is byte-exact, and is the only artifact produced.
        staged = self.cfg.staging_dir / f"{cap_id}.staged"
        self.assertTrue(staged.exists())
        self.assertEqual(staged.read_bytes(), DATA)
        self.assertEqual(result["sha256"], sha256_hex(DATA))
        self.assertEqual(result["bytes"], len(DATA))
        self.assertEqual(
            list(self.cfg.chunk_dir.iterdir()) if self.cfg.chunk_dir.exists() else [], []
        )
        self.assertEqual(
            list(self.cfg.manifest_dir.iterdir()) if self.cfg.manifest_dir.exists() else [],
            [],
        )
        row = self.conn.execute("SELECT * FROM objects").fetchone()
        self.assertIsNone(row)
        # A seal job was queued, not executed.
        seal_jobs = self.conn.execute("SELECT * FROM seal_jobs").fetchall()
        self.assertEqual([j["status"] for j in seal_jobs], ["queued"])

    def test_seal_stage_produces_the_same_object_as_the_combined_worker(self):
        src = self.fresh_source("split-seal", DATA)
        self._fetch(src)
        seal_job = claim_seal_job(self.conn, "seal-test", split_only=True)
        self.assertIsNotNone(seal_job)
        result = run_seal(self.cfg, self.conn, seal_job, worker="seal-test")
        self.assertEqual(result["status"], "done", result)

        obj = get_object(self.conn, result["object_id"])
        self.assertEqual(obj["stream_digest"], sha256_hex(DATA))
        self.assertEqual(obj["total_bytes"], len(DATA))
        self.assertEqual(read_object_bytes(self.cfg, self.conn, obj), DATA)
        om = load_manifest(self.cfg, obj["manifest_digest"])
        self.assertEqual(om.stream_digest, sha256_hex(DATA))
        # Staging is consumed exactly once.
        self.assertFalse((self.cfg.staging_dir / f"{seal_job['capture_id']}.staged").exists())
        fetch = self.conn.execute("SELECT * FROM fetch_jobs").fetchone()
        self.assertEqual(fetch["status"], "sealed")

    def test_staging_tamper_is_refused_and_never_sealed(self):
        src = self.fresh_source("split-tamper", DATA)
        _, cap_id, _ = self._fetch(src)
        staged = self.cfg.staging_dir / f"{cap_id}.staged"
        tampered = bytearray(staged.read_bytes())
        tampered[10] ^= 0xFF
        staged.write_bytes(bytes(tampered))

        seal_job = claim_seal_job(self.conn, "seal-test", split_only=True)
        result = run_seal(self.cfg, self.conn, seal_job, worker="seal-test")
        self.assertEqual(result["status"], "failed")
        self.assertIn("StagingMismatch", result["error"])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) n FROM objects").fetchone()["n"], 0)
        # The staged file is left for inspection; nothing entered the vault.
        self.assertTrue(staged.exists())

    def test_seal_staged_directly_raises_on_length_change(self):
        from app.vault import seal_staged

        src = self.fresh_source("split-short", DATA)
        _, cap_id, _ = self._fetch(src)
        staged = self.cfg.staging_dir / f"{cap_id}.staged"
        staged.write_bytes(DATA[: len(DATA) // 2])
        capture = self.conn.execute("SELECT * FROM captures WHERE id=?", (cap_id,)).fetchone()
        with self.assertRaises(StagingMismatch):
            seal_staged(self.cfg, self.conn, capture)

    def test_combined_and_split_workers_do_not_steal_each_others_jobs(self):
        src_a = self.fresh_source("inline-a", DATA)
        src_b = self.fresh_source("split-b", DATA + b"<!-- b -->\n")
        inline_id, _ = enqueue_capture(self.conn, src_a, inline=True)
        split_id, _ = enqueue_capture(self.conn, src_b, inline=False)

        self.assertEqual(claim_fetch_job(self.conn, "split", inline=False)["id"], split_id)
        self.assertEqual(claim_fetch_job(self.conn, "combined", inline=True)["id"], inline_id)

    def test_cancel_after_fetch_discards_staging_and_blocks_seal(self):
        from app.capture_worker import cancel_capture

        src = self.fresh_source("split-cancel", DATA)
        job_id, cap_id, _ = self._fetch(src)
        staged = self.cfg.staging_dir / f"{cap_id}.staged"
        self.assertTrue(staged.exists())

        self.assertTrue(cancel_capture(self.conn, job_id))
        self.assertFalse(staged.exists())
        self.assertIsNone(claim_seal_job(self.conn, "seal", split_only=True))
        row = self.conn.execute("SELECT status FROM fetch_jobs WHERE id=?", (job_id,)).fetchone()
        self.assertEqual(row["status"], "canceled")
        cap = self.conn.execute("SELECT status FROM captures WHERE id=?", (cap_id,)).fetchone()
        self.assertEqual(cap["status"], "aborted")
        # A canceled fetch cannot be resurrected by a seal worker.
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) n FROM objects").fetchone()["n"], 0
        )

    def test_cancel_is_refused_once_sealing_has_started(self):
        from app.capture_worker import cancel_capture

        src = self.fresh_source("split-cancel-running", DATA)
        job_id, _, _ = self._fetch(src)
        seal = claim_seal_job(self.conn, "seal", split_only=True)
        self.assertEqual(seal["status"], "running")
        self.assertFalse(cancel_capture(self.conn, job_id))

    def test_seal_retry_after_commit_is_idempotent(self):
        """Sealing twice must not create a second object or fail on missing staging."""
        src = self.fresh_source("split-retry", DATA)
        self._fetch(src)
        seal_job = claim_seal_job(self.conn, "seal-test", split_only=True)
        first = run_seal(self.cfg, self.conn, seal_job, worker="seal-test")
        self.assertEqual(first["status"], "done", first)

        # Simulate a crash after the object committed but before the job was marked.
        self.conn.execute("UPDATE seal_jobs SET status='queued' WHERE id=?", (seal_job["id"],))
        self.conn.execute("UPDATE fetch_jobs SET status='fetched'")
        self.conn.commit()
        retry = claim_seal_job(self.conn, "seal-test-2", split_only=True)
        result = run_seal(self.cfg, self.conn, retry, worker="seal-test-2")
        self.assertEqual(result["status"], "done", result)
        self.assertTrue(result.get("recovered"))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) n FROM objects").fetchone()["n"], 1)

    def test_missing_staging_fails_the_seal_job_not_the_fetch_record(self):
        src = self.fresh_source("split-missing", DATA)
        _, cap_id, _ = self._fetch(src)
        (self.cfg.staging_dir / f"{cap_id}.staged").unlink()
        seal_job = claim_seal_job(self.conn, "seal-test", split_only=True)
        result = run_seal(self.cfg, self.conn, seal_job, worker="seal-test")
        self.assertEqual(result["status"], "failed")
        fetch = self.conn.execute("SELECT * FROM fetch_jobs").fetchone()
        self.assertEqual(fetch["status"], "failed")
        self.assertEqual(fetch["stream_sha256"], sha256_hex(DATA))

    def test_fetch_worker_process_imports_without_the_web_app(self):
        """The workers are startable as standalone modules."""
        for mod in ("app.fetch_worker", "app.seal_worker", "app.capture_worker",
                    "app.parse_worker"):
            proc = subprocess.run(
                [sys.executable, "-c", f"import {mod}; print({mod}.__name__)"],
                cwd=ROOT, capture_output=True, text=True,
                env={**os.environ, "PYTHONPATH": ROOT},
            )
            self.assertEqual(proc.returncode, 0, f"{mod}: {proc.stderr}")

    def test_combined_worker_recovers_an_orphaned_seal(self):
        """A crash between fetch and seal is finished by a later combined worker."""
        from app.capture_worker import claim_combined, run_combined

        src = self.fresh_source("inline-orphan", DATA)
        enqueue_capture(self.conn, src, inline=True)
        job = claim_combined(self.conn, "combined-1")
        self.assertEqual(job[0], "fetch")
        fetched = run_fetch(self.cfg, self.conn, job[1], worker="combined-1")
        self.assertEqual(fetched["status"], "fetched")
        # Simulate the crash: the fetch committed, the process died before sealing.
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) n FROM objects").fetchone()["n"], 0
        )
        self.conn.execute("UPDATE seal_jobs SET lease_until='2000-01-01T00:00:00'")
        self.conn.commit()

        recovered = claim_combined(self.conn, "combined-2")
        self.assertEqual(recovered[0], "seal")
        result = run_combined(self.cfg, self.conn, recovered, worker="combined-2")
        self.assertEqual(result["status"], "done", result)
        self.assertEqual(
            self.conn.execute("SELECT status FROM fetch_jobs").fetchone()["status"], "sealed"
        )

    def test_stages_run_as_two_separate_processes(self):
        """The real process handoff: fetch process stages, seal process seals."""
        src = self.fresh_source("split-proc", DATA)
        job_id, cap_id = enqueue_capture(self.conn, src, inline=False)
        env = {**os.environ, "PYTHONPATH": ROOT}

        fetch = subprocess.run(
            [sys.executable, "-m", "app.fetch_worker", "--once", "--poll", "0.05"],
            cwd=ROOT, env=env, capture_output=True, text=True, timeout=90,
        )
        self.assertEqual(fetch.returncode, 0, fetch.stderr)
        self.assertTrue((self.cfg.staging_dir / f"{cap_id}.staged").exists())
        # The fetch process must not have created any vault artifact.
        self.assertEqual(
            self.conn.execute("SELECT COUNT(*) n FROM objects").fetchone()["n"], 0
        )
        self.assertEqual(
            self.conn.execute("SELECT status FROM fetch_jobs WHERE id=?", (job_id,)
                              ).fetchone()["status"],
            "fetched",
        )

        seal = subprocess.run(
            [sys.executable, "-m", "app.seal_worker", "--once", "--poll", "0.05"],
            cwd=ROOT, env=env, capture_output=True, text=True, timeout=90,
        )
        self.assertEqual(seal.returncode, 0, seal.stderr)
        obj = self.conn.execute("SELECT * FROM objects").fetchone()
        self.assertIsNotNone(obj)
        self.assertEqual(obj["stream_digest"], sha256_hex(DATA))
        self.assertEqual(
            self.conn.execute("SELECT status FROM fetch_jobs WHERE id=?", (job_id,)
                              ).fetchone()["status"],
            "sealed",
        )
        self.assertFalse((self.cfg.staging_dir / f"{cap_id}.staged").exists())

    def test_pipeline_records_are_visible_in_state(self):
        from app import web
        from app.supervisor import Supervisors

        src = self.fresh_source("split-state", DATA)
        job_id, cap_id, _ = self._fetch(src)
        saved = dict(web.server_state)
        web.server_state["cfg"] = self.cfg
        web.server_state["supervisors"] = Supervisors()
        try:
            state = web.api_state()
        finally:
            web.server_state.clear()
            web.server_state.update(saved)
        jobs = state["jobs"]
        job = next(j for j in jobs if j["id"] == job_id)
        self.assertEqual(job["status"], "fetched")
        self.assertEqual(job["inline"], 0)
        self.assertEqual(job["staged_path"], str(self.cfg.staging_dir / f"{cap_id}.staged"))
        meta = json.loads(self.conn.execute(
            "SELECT staged_meta_json FROM captures WHERE id=?", (cap_id,)
        ).fetchone()["staged_meta_json"])
        self.assertEqual(meta["sha256"], sha256_hex(DATA))
        self.assertEqual(meta["source_name"], "split-state")


if __name__ == "__main__":
    unittest.main()
