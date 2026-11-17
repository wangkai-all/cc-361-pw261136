"""Web API tests over real HTTP (server started per test class in a thread)."""
from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

from http.server import ThreadingHTTPServer

ROOT = Path(__file__).resolve().parent.parent


class WebApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix="vault-web-"))
        cls._saved = {k: os.environ.get(k) for k in ("VAULT_DATA_DIR", "VAULT_FIXTURES_DIR")}
        os.environ["VAULT_DATA_DIR"] = str(cls.tmp / "data")
        os.environ["VAULT_FIXTURES_DIR"] = str(cls.tmp / "fixtures")
        from app import web

        cls.web = web
        cls.cfg = web.bootstrap()
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), web.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        from app.db import connect

        try:
            conn = connect(cls.cfg.db_path)
            conn.close()
        except Exception:
            pass
        shutil.rmtree(cls.tmp, ignore_errors=True)
        for k, v in cls._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    # ---- helpers ----
    def url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def get(self, path):
        with urllib.request.urlopen(self.url(path), timeout=20) as r:
            return r.status, json.loads(r.read().decode())

    def post(self, path, body=None):
        data = json.dumps(body or {}).encode()
        req = urllib.request.Request(self.url(path), data=data, method="POST",
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.status, json.loads(r.read().decode())
        except urllib.error.HTTPError as err:
            return err.code, json.loads(err.read().decode())

    def state(self):
        return self.get("/api/state")[1]

    def source(self, name):
        return next(s for s in self.state()["sources"] if s["name"] == name)

    def capture_sync(self, name):
        """Run capture in-process (no worker subprocess) through the same code path."""
        from app.capture_worker import claim_job, run_capture
        from app.db import connect

        conn = connect(self.cfg.db_path)
        try:
            job = claim_job(conn, "test")
            if job is None:
                raise AssertionError("no queued job")
            while job["source_id"] != self.source(name)["id"]:
                result = run_capture(self.cfg, conn, job, worker="test")
                job = claim_job(conn, "test")
                if job is None:
                    raise AssertionError("job for source not found")
            result = run_capture(self.cfg, conn, job, worker="test")
            assert result["status"] == "done", result
            return result
        finally:
            conn.close()

    # ---- tests ----
    def test_health_and_index(self):
        code, data = self.get("/healthz")
        self.assertEqual(code, 200)
        self.assertTrue(data["ok"])
        with urllib.request.urlopen(self.url("/"), timeout=10) as r:
            html = r.read().decode()
        self.assertIn("Sealed Wire Vault", html)
        for asset in ("/static/app.js", "/static/style.css"):
            with urllib.request.urlopen(self.url(asset), timeout=10) as r:
                self.assertEqual(r.status, 200)

    def test_state_lists_seeded_sources_and_rules(self):
        st = self.state()
        names = {s["name"] for s in st["sources"]}
        self.assertEqual(names, {"ticker-news", "port-authority", "market-csv"})
        ids = {r["id"] for r in st["rules"]}
        self.assertIn("html_records@1", ids)
        self.assertIn("html_records@2", ids)
        self.assertIn("csv_rows@1", ids)

    def test_capture_then_parse_then_export(self):
        src = self.source("port-authority")
        code, job = self.post(f"/api/sources/{src['id']}/capture", {"auto_start_worker": False})
        self.assertEqual(code, 200, job)
        result = self.capture_sync("port-authority")
        obj_id = result["object_id"]
        code, detail = self.get(f"/api/objects/{obj_id}")
        self.assertEqual(code, 200)
        self.assertGreater(detail["object"]["chunk_count"], 0)
        self.assertTrue(detail["manifest"]["stream"]["sha256"])

        code, started = self.post("/api/objects/parse",
                                  {"object": obj_id, "rule_id": "html_records@2",
                                   "auto_start_worker": False})
        self.assertEqual(code, 200, started)
        from app.parse_engine import run_parse

        conn = self.web._conn()
        try:
            run_parse(self.cfg, conn, started["run_id"], worker="test")
        finally:
            conn.close()
        code, view = self.get(f"/api/runs/{started['run_id']}/view")
        self.assertEqual(code, 200)
        self.assertEqual(view["run"]["status"], "done")
        self.assertTrue(view["records"])
        self.assertTrue(all(r["span"]["start"] <= r["span"]["end"] for r in view["records"]))

        # Export via HTTP and verify offline.
        req = urllib.request.Request(self.url(f"/api/objects/{obj_id}/export"),
                                     data=b"{}", method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as r:
            self.assertEqual(r.headers.get_content_type(), "application/zip")
            payload = r.read()
        dest = self.tmp / "pkg"
        shutil.rmtree(dest, ignore_errors=True)
        dest.mkdir()
        zipfile.ZipFile(io.BytesIO(payload)).extractall(dest)
        import subprocess
        import sys

        proc = subprocess.run([sys.executable, "-I", "verify_package.py", "."],
                              cwd=str(dest), capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_bytes_endpoint_returns_exact_original(self):
        src = self.source("market-csv")
        self.post(f"/api/sources/{src['id']}/capture", {"auto_start_worker": False})
        result = self.capture_sync("market-csv")
        with urllib.request.urlopen(self.url(f"/api/objects/{result['object_id']}/bytes"),
                                    timeout=20) as r:
            body = r.read()
        from app.config import load_config
        from app.fixtures import fixture_path

        expected = fixture_path(load_config(), "market-csv").read_bytes()
        self.assertEqual(body, expected)

    def test_withdraw_blocks_capture_but_not_reads(self):
        src = self.source("ticker-news")
        self.post(f"/api/sources/{src['id']}/capture", {"auto_start_worker": False})
        result = self.capture_sync("ticker-news")
        code, _ = self.post(f"/api/sources/{src['id']}/withdraw", {})
        self.assertEqual(code, 200)
        code, err = self.post(f"/api/sources/{src['id']}/capture", {"auto_start_worker": False})
        self.assertEqual(code, 409, err)
        self.assertIn("withdrawn", err["error"])
        # Object still verifies and its bytes still download.
        code, report = self.get(f"/api/objects/{result['object_id']}/verify")
        self.assertEqual(report["status"], "ok")
        with urllib.request.urlopen(self.url(f"/api/objects/{result['object_id']}/bytes"),
                                    timeout=20) as r:
            self.assertTrue(r.read())
        self.post(f"/api/sources/{src['id']}/reactivate", {})

    def test_damage_endpoint_and_chunk_metadata(self):
        src = self.source("ticker-news")
        self.post(f"/api/sources/{src['id']}/capture", {"auto_start_worker": False})
        result = self.capture_sync("ticker-news")
        obj_id = result["object_id"]
        code, detail = self.get(f"/api/objects/{obj_id}")
        seq = detail["chunks"][0]["seq"]
        digest = detail["chunks"][0]["digest"]
        code, res = self.post(f"/api/objects/{obj_id}/damage",
                              {"seq": seq, "mode": "corrupt", "force": True})
        self.assertEqual(code, 200, res)
        self.assertEqual(res["report"]["status"], "damaged")
        code, users = self.get(f"/api/chunks/{digest}")
        self.assertIn(obj_id, [o["id"] for o in users["objects"]])
        # Damaged chunk cannot be fetched raw.
        try:
            urllib.request.urlopen(self.url(f"/api/objects/{obj_id}/chunks/{seq}"), timeout=10)
            self.fail("expected 410")
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 410)
        code, _ = self.post(f"/api/chunks/{digest}", {})
        self.assertEqual(code, 200)
        code, report = self.get(f"/api/objects/{obj_id}/verify")
        self.assertEqual(report["status"], "ok")

    def test_rule_switch_endpoint(self):
        code, res = self.post("/api/rules/switch",
                              {"family": "html_records", "rule_id": "html_records@1"})
        self.assertEqual(code, 200)
        st = self.state()
        active = [r for r in st["rules"] if r["name"] == "html_records" and r["is_active"]]
        self.assertEqual([r["id"] for r in active], ["html_records@1"])
        self.post("/api/rules/switch", {"family": "html_records", "rule_id": "html_records@2"})
        code, res = self.post("/api/rules/switch", {"family": "html_records",
                                                    "rule_id": "csv_rows@1"})
        self.assertEqual(code, 404)

    def test_unknown_routes(self):
        try:
            urllib.request.urlopen(self.url("/api/nope"), timeout=10)
            self.fail("expected 404")
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 404)
        code, err = self.post("/api/objects/parse", {"object": "obj_missing",
                                                     "rule_id": "html_records@1"})
        self.assertEqual(code, 404)

    def test_split_pipeline_via_api_and_cancel_before_seal(self):
        src = self.source("market-csv")
        code, queued = self.post(f"/api/sources/{src['id']}/capture",
                                 {"pipeline": "split", "auto_start_worker": False})
        self.assertEqual(code, 200, queued)
        self.assertEqual(queued["pipeline"], "split")

        job = next(j for j in self.state()["jobs"] if j["id"] == queued["job_id"])
        self.assertEqual(job["status"], "queued")
        self.assertEqual(job["inline"], 0)

        # Cancel before anything ran: the fetch job and its capture are closed.
        code, res = self.post(f"/api/jobs/{queued['job_id']}/cancel", {})
        self.assertEqual(code, 200, res)
        job = next(j for j in self.state()["jobs"] if j["id"] == queued["job_id"])
        self.assertEqual(job["status"], "canceled")
        cap = next(c for c in self.web.api_state()["jobs"] if c["id"] == queued["job_id"])
        self.assertEqual(cap["status"], "canceled")

        # A second split capture completes through fetch + seal separately.
        code, queued = self.post(f"/api/sources/{src['id']}/capture",
                                 {"pipeline": "split", "auto_start_worker": False})
        self.assertEqual(code, 200, queued)
        from app.capture_worker import claim_fetch_job, claim_seal_job, run_fetch, run_seal
        from app.db import connect

        conn = connect(self.cfg.db_path)
        try:
            fetch = claim_fetch_job(conn, "test-fetch", inline=False)
            self.assertEqual(fetch["id"], queued["job_id"])
            fetched = run_fetch(self.cfg, conn, fetch, worker="test-fetch")
            self.assertEqual(fetched["status"], "fetched", fetched)
            # The API surfaces the waiting-for-seal state.
            st = self.state()
            self.assertEqual(st["pipeline"]["fetched_waiting_seal"], 1)
            self.assertEqual(len(st["pipeline"]["staged_files"]), 1)
            # Nothing was published by the fetch stage alone.
            self.assertFalse([o for o in st["objects"]
                              if o.get("capture_id") == queued["capture_id"]])

            seal = claim_seal_job(conn, "test-seal", split_only=True)
            sealed = run_seal(self.cfg, conn, seal, worker="test-seal")
            self.assertEqual(sealed["status"], "done", sealed)
        finally:
            conn.close()
        detail = self.get(f"/api/objects/{sealed['object_id']}")[1]
        self.assertEqual(detail["object"]["stream_digest"], fetched["sha256"])
        st = self.state()
        self.assertEqual(st["pipeline"]["staged_files"], [])

    def test_capture_rejects_unknown_pipeline_and_worker(self):
        src = self.source("ticker-news")
        code, err = self.post(f"/api/sources/{src['id']}/capture",
                              {"pipeline": "teleport", "auto_start_worker": False})
        self.assertEqual(code, 400)
        code, err = self.post("/api/workers", {"action": "start", "worker": "nope"})
        self.assertEqual(code, 400)
        code, err = self.post("/api/workers", {"action": "status"})
        self.assertEqual(code, 200)
        for key in ("capture_worker", "fetch_worker", "seal_worker", "parse_worker"):
            self.assertIn(key, err)


if __name__ == "__main__":
    unittest.main()
