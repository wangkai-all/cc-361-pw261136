"""Parsing: provenance, reproducibility, rule switching, stop/resume, damage gating."""
from __future__ import annotations

import unittest

from tests.support import VaultCase

from app.parse_engine import (
    StopSignal,
    compare_runs,
    create_run,
    result_view,
    run_parse,
)
from app.vault import object_chunks_rows, restore_chunk, simulate_damage

SHARED = b"prefix\n" + b"x" * 3000 + b"\n"
DATA = (
    b"<html><body>\n" + b"".join(
        f'<article data-seq="{i}" data-rev="0"><h2>feed / topic{i % 3} / region{i % 2}</h2>'
        f'<p class="note">entry {i}</p><span class="value">{10 + i}</span></article>\n'.encode()
        for i in range(120)
    ) + b"</body></html>\n"
)


class ParseTests(VaultCase):
    def setUp(self):
        super().setUp()
        self.src = self.fresh_source("feed", DATA)
        self.obj = self.capture(self.src)

    def test_records_have_exact_spans_and_deps(self):
        run_id, res = self.parse(self.obj, "html_records@2")
        self.assertEqual(res["status"], "done", res)
        view = result_view(self.conn, run_id)
        self.assertEqual(len(view["records"]), 120)
        chunks = object_chunks_rows(self.conn, self.obj["id"])
        for rec in view["records"]:
            self.assertLessEqual(rec["span"]["start"], rec["span"]["end"])
            self.assertEqual(rec["value"]["seq"], rec["ordinal"])
            # Every dependency named is a real chunk of this object.
            self.assertTrue(rec["deps"])
            self.assertFalse(rec["blocked"])
            for dep in rec["deps"]:
                hit = next(c for c in chunks if c["digest"] == dep)
                c_start, c_end = hit["offset"], hit["offset"] + hit["length"]
                self.assertLess(c_start, rec["span"]["end"])
                self.assertGreater(c_end, rec["span"]["start"])

    def test_spans_point_at_real_bytes(self):
        run_id, _ = self.parse(self.obj, "html_records@2")
        view = result_view(self.conn, run_id)
        for rec in view["records"][:20]:
            a, b = rec["span"]["start"], rec["span"]["end"]
            piece = DATA[a:b]
            self.assertIn(b"<article", piece)
            self.assertIn(b"</article>", piece)
            self.assertIn(str(rec["value"]["value"]).encode(), piece)

    def test_parse_never_reads_online_source(self):
        run_id, _ = self.parse(self.obj, "html_records@2")
        # Change the source bytes on disk; the sealed object is untouched.
        self.fresh_source("feed", b"completely different bytes now", mode="static")
        view = result_view(self.conn, run_id)
        self.assertEqual(len(view["records"]), 120)
        new_run, res = self.parse(self.obj, "html_records@2")
        self.assertEqual(res["result_digest"],
                         self.conn.execute("SELECT result_digest FROM parse_runs WHERE id=?",
                                           (run_id,)).fetchone()[0])

    def test_result_is_reproducible(self):
        r1, a = self.parse(self.obj, "html_records@2")
        r2, b = self.parse(self.obj, "html_records@2")
        self.assertEqual(a["result_digest"], b["result_digest"])
        self.assertNotEqual(r1, r2)

    def test_rule_versions_differ_and_compare_locates_fields(self):
        r1, _ = self.parse(self.obj, "html_records@1")
        r2, _ = self.parse(self.obj, "html_records@2")
        cmp = compare_runs(self.conn, r1, r2)
        self.assertTrue(cmp["same_object"])
        self.assertEqual(cmp["summary"]["changed"], 120)
        first = cmp["changed"][0]
        self.assertIn("value", first["fields"])
        self.assertIn("topic", first["fields"])
        self.assertEqual(first["span_a"], first["span_b"])  # same byte positions

    def test_stop_then_resume_is_identical_to_uninterrupted(self):
        ref_id, ref = self.parse(self.obj, "html_records@2")
        run_id = create_run(self.conn, self.obj, "html_records@2")
        stop = StopSignal()

        def progress(ev):
            stop.requested = True
            stop.reason = "test pause"

        paused = run_parse(self.cfg, self.conn, run_id, stop, progress=progress, worker="t")
        self.assertEqual(paused["status"], "paused", paused)
        resumed = run_parse(self.cfg, self.conn, run_id, StopSignal(), worker="t")
        self.assertEqual(resumed["status"], "done", resumed)
        got = result_view(self.conn, run_id)
        want = result_view(self.conn, ref_id)
        self.assertEqual(got["run"]["result_digest"], want["run"]["result_digest"])
        self.assertEqual([r["ordinal"] for r in got["records"]],
                         [r["ordinal"] for r in want["records"]])
        self.assertEqual(len(got["records"]), 120)

    def test_stop_mid_region_resumes_without_duplicates(self):
        # Force a tiny batch size so a checkpoint lands inside one region.
        import app.parse_engine as pe
        old = pe.BATCH_RECORDS
        pe.BATCH_RECORDS = 7
        try:
            ref_id, _ = self.parse(self.obj, "html_records@2")
            run_id = create_run(self.conn, self.obj, "html_records@2")
            stop = StopSignal()
            seen = {"n": 0}

            def progress(ev):
                seen["n"] += 1
                if seen["n"] >= 2:
                    stop.requested = True
                    stop.reason = "mid-region"

            run_parse(self.cfg, self.conn, run_id, stop, progress=progress, worker="t")
            run_parse(self.cfg, self.conn, run_id, StopSignal(), worker="t")
            got = result_view(self.conn, run_id)
            want = result_view(self.conn, ref_id)
            self.assertEqual(len(got["records"]), 120)
            self.assertEqual(got["run"]["result_digest"], want["run"]["result_digest"])
        finally:
            pe.BATCH_RECORDS = old

    def test_damage_blocks_only_dependent_records(self):
        run_id, _ = self.parse(self.obj, "html_records@2")
        rows = object_chunks_rows(self.conn, self.obj["id"])
        target = rows[len(rows) // 2]
        sim = simulate_damage(self.cfg, self.conn, self.obj, target["seq"])
        view = result_view(self.conn, run_id)
        blocked = [r for r in view["records"] if r["blocked"]]
        served = [r for r in view["records"] if not r["blocked"]]
        self.assertTrue(blocked)
        self.assertTrue(served)
        self.assertEqual(view["blocked_records"], len(blocked))
        for rec in blocked:
            self.assertIn(sim["digest"], rec["blocked_by"])
        for rec in served:
            self.assertNotIn(sim["digest"], rec["deps"])
        # Repair unblocks instantly, no re-run needed.
        restore_chunk(self.cfg, self.conn, sim["digest"])
        view2 = result_view(self.conn, run_id)
        self.assertEqual(view2["blocked_records"], 0)

    def test_damage_produces_gap_and_partial_parse(self):
        rows = object_chunks_rows(self.conn, self.obj["id"])
        sim = simulate_damage(self.cfg, self.conn, self.obj, rows[0]["seq"])
        run_id, res = self.parse(self.obj, "html_records@2")
        self.assertEqual(res["status"], "done", res)
        view = result_view(self.conn, run_id)
        self.assertEqual(len(view["gaps"]), 1)
        self.assertLess(len(view["records"]), 120)
        self.assertTrue(all(not r["blocked"] for r in view["records"]))
        restore_chunk(self.cfg, self.conn, sim["digest"])

    def test_damage_then_repair_then_parse_matches_clean(self):
        ref_id, _ = self.parse(self.obj, "html_records@2")
        rows = object_chunks_rows(self.conn, self.obj["id"])
        sim = simulate_damage(self.cfg, self.conn, self.obj, rows[1]["seq"])
        damaged_id, _ = self.parse(self.obj, "html_records@2")
        restore_chunk(self.cfg, self.conn, sim["digest"])
        healed_id, _ = self.parse(self.obj, "html_records@2")
        a = result_view(self.conn, ref_id)
        b = result_view(self.conn, healed_id)
        self.assertEqual(a["run"]["result_digest"], b["run"]["result_digest"])
        d = result_view(self.conn, damaged_id)
        self.assertNotEqual(d["run"]["result_digest"], a["run"]["result_digest"])

    def test_rule_source_change_is_refused(self):
        from app.rules import RuleError, load_rule_by_id

        rule = load_rule_by_id(self.conn, "html_records@1")
        # Registered digest is immutable even if the file changes later.
        import shutil
        path = self.cfg.rules_dir / "html_records_v1.py"
        backup = self.tmp / "backup_v1.py"
        shutil.copy(path, backup)
        try:
            path.write_text(path.read_text() + "\n# tampered\n")
            with self.assertRaises(RuleError):
                load_rule_by_id(self.conn, "html_records@1")
        finally:
            shutil.copy(backup, path)

    def test_withdrawn_source_keeps_results_readable(self):
        run_id, _ = self.parse(self.obj, "html_records@2")
        self.conn.execute("UPDATE sources SET status='withdrawn' WHERE id=?", (self.src["id"],))
        self.conn.commit()
        view = result_view(self.conn, run_id)
        self.assertEqual(len(view["records"]), 120)
        # And no new capture can start.
        from app.capture_worker import enqueue_capture

        with self.assertRaises(PermissionError):
            enqueue_capture(self.conn, self.conn.execute(
                "SELECT * FROM sources WHERE id=?", (self.src["id"],)).fetchone())


if __name__ == "__main__":
    unittest.main()
