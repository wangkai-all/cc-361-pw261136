"""Shared fixtures for the test suite: one isolated vault per test."""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import load_config  # noqa: E402
from app.db import connect, init_db  # noqa: E402


class VaultCase(unittest.TestCase):
    """Each test gets a fresh data dir, fixtures dir, database and rules sync."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="vault-test-"))
        self._saved_env = {
            k: os.environ.get(k)
            for k in ("VAULT_DATA_DIR", "VAULT_FIXTURES_DIR", "VAULT_CHUNK_MIN",
                      "VAULT_CHUNK_AVG", "VAULT_CHUNK_MAX", "VAULT_DEMO_TOOLS")
        }
        os.environ["VAULT_DATA_DIR"] = str(self.tmp / "data")
        os.environ["VAULT_FIXTURES_DIR"] = str(self.tmp / "fixtures")
        os.environ["VAULT_CHUNK_MIN"] = "128"
        os.environ["VAULT_CHUNK_AVG"] = "512"
        os.environ["VAULT_CHUNK_MAX"] = "2048"
        os.environ["VAULT_DEMO_TOOLS"] = "1"
        self.cfg = load_config()
        init_db(self.cfg.db_path)
        self.conn = connect(self.cfg.db_path)
        from app.rules import sync_rules

        sync_rules(self.conn, self.cfg.rules_dir)
        self._counter = 0

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmp, ignore_errors=True)
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def fresh_source(self, name: str, data: bytes, *, status="active", mode="static",
                     update: bool = False):
        from app.util import new_id, utcnow

        path = self.cfg.fixtures_dir / f"{name}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        existing = self.conn.execute("SELECT * FROM sources WHERE name=?", (name,)).fetchone()
        if existing:
            self.conn.execute(
                "UPDATE sources SET static_path=?, mode=?, status=? WHERE id=?",
                (str(path), mode, status, existing["id"]),
            )
            self.conn.commit()
            return self.conn.execute("SELECT * FROM sources WHERE id=?", (existing["id"],)).fetchone()
        sid = new_id("src")
        self.conn.execute(
            "INSERT INTO sources(id,name,origin,url,headers_json,mode,static_path,status,"
            "created_at,notes) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (sid, name, f"demo://{name}", f"demo://{name}", "{}", mode, str(path),
             status, utcnow(), "test fixture"),
        )
        self.conn.commit()
        return self.conn.execute("SELECT * FROM sources WHERE id=?", (sid,)).fetchone()

    def capture(self, source_row):
        from app.capture_worker import claim_job, enqueue_capture, run_capture
        from app.vault import get_object

        job_id, _ = enqueue_capture(self.conn, source_row)
        job = claim_job(self.conn, "test")
        result = run_capture(self.cfg, self.conn, job, worker="test")
        self.assertEqual(result.get("status"), "done", result)
        return get_object(self.conn, result["object_id"])

    def parse(self, obj, rule_id: str, *, stop=None, progress=None):
        from app.parse_engine import create_run, run_parse

        run_id = create_run(self.conn, obj, rule_id)
        result = run_parse(self.cfg, self.conn, run_id, stop, progress=progress, worker="test")
        return run_id, result
