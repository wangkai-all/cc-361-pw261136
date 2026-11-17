"""Vault: sealing, exactness, sharing, damage localisation, export packages."""
from __future__ import annotations

import io
import json
import shutil
import subprocess
import sys
import unittest
import zipfile

from tests.support import VaultCase

from app.util import sha256_hex
from app.vault import (
    ObjectDamageError,
    check_object,
    chunk_share_summary,
    export_package,
    content_key,
    get_object,
    iter_object_chunks,
    load_manifest,
    object_chunks_rows,
    read_object_bytes,
    restore_chunk,
    simulate_damage,
)

DATA = (
    b"<html><body>\n" + b"".join(
        f'<article data-seq="{i}" data-rev="0"><h2>s / t{i % 4} / r{i % 3}</h2>'
        f'<p class="note">note {i}</p><span class="value">{100 + i}</span></article>\n'.encode()
        for i in range(400)
    ) + b"</body></html>\n"
)


class VaultTests(VaultCase):
    def test_seal_is_exact_and_self_describing(self):
        src = self.fresh_source("exact", DATA)
        obj = self.capture(src)
        self.assertEqual(obj["total_bytes"], len(DATA))
        self.assertEqual(obj["stream_digest"], sha256_hex(DATA))
        om = load_manifest(self.cfg, obj["manifest_digest"])
        self.assertEqual(om.stream_digest, sha256_hex(DATA))
        offsets = [c["offset"] for c in om.manifest["chunks"]]
        self.assertEqual(offsets, sorted(offsets))
        self.assertEqual(offsets[0], 0)
        self.assertEqual(offsets[-1] + om.manifest["chunks"][-1]["length"], len(DATA))
        self.assertEqual(read_object_bytes(self.cfg, self.conn, obj), DATA)

    def test_identical_recapture_shares_every_chunk(self):
        src = self.fresh_source("share", DATA)
        a = self.capture(src)
        b = self.capture(src)
        self.assertNotEqual(a["id"], b["id"])
        # Distinct object identities (each pins its own capture attempt), but
        # the same packet under the same bytes: content key and stream digest agree.
        self.assertNotEqual(a["manifest_digest"], b["manifest_digest"])
        self.assertEqual(a["stream_digest"], b["stream_digest"])
        self.assertEqual(content_key(self.conn, a), content_key(self.conn, b))
        sa = chunk_share_summary(self.conn, a["id"])
        self.assertEqual(sa["unique_chunks"], 0)
        self.assertEqual(sa["shared_chunks"], sa["chunks"])
        digests = {r["digest"] for r in object_chunks_rows(self.conn, a["id"])}
        counts = [
            self.conn.execute("SELECT refcount FROM chunks WHERE digest=?", (d,)).fetchone()[0]
            for d in digests
        ]
        self.assertTrue(all(c >= 2 for c in counts), counts)

    def test_small_change_shares_most_chunks(self):
        src = self.fresh_source("smallchange", DATA)
        a = self.capture(src)
        changed = DATA.replace(b"note 200<", b"NOTE 200<")
        src_updated = self.fresh_source("smallchange", changed, mode="static")
        b = self.capture(src_updated)
        da = {r["digest"] for r in object_chunks_rows(self.conn, a["id"])}
        db = {r["digest"] for r in object_chunks_rows(self.conn, b["id"])}
        self.assertGreater(len(da & db), 0.7 * max(len(da), len(db)), f"{len(da & db)}/{len(da)}")
        self.assertNotEqual(da, db)

    def test_damage_is_localised_to_one_object_chunk(self):
        src = self.fresh_source("damage", DATA)
        obj = self.capture(src)
        other_src = self.fresh_source("damage-other", b"unrelated bytes " * 50)
        other = self.capture(other_src)
        rows = object_chunks_rows(self.conn, obj["id"])
        mid = rows[len(rows) // 2]["seq"]
        res = simulate_damage(self.cfg, self.conn, obj, mid)
        report = res["report"]
        self.assertEqual(report["status"], "damaged")
        self.assertEqual(len(report["damaged"]), 1)
        self.assertEqual(report["damaged"][0]["seq"], mid)
        self.assertEqual(report["chunks_ok"], report["chunks_total"] - 1)
        # Only the affected chunks fail; the other object is untouched.
        self.assertEqual(check_object(self.cfg, self.conn, other)["status"], "ok")
        # Every other chunk of this object is still individually readable.
        good = [cv for cv in iter_object_chunks(self.cfg, self.conn, obj) if cv.status == "ok"]
        self.assertEqual(len(good), len(rows) - 1)
        # Whole-object reassembly must fail rather than silently return partial.
        with self.assertRaises(ObjectDamageError):
            read_object_bytes(self.cfg, self.conn, obj)
        restore_chunk(self.cfg, self.conn, res["digest"])
        self.assertEqual(check_object(self.cfg, self.conn, obj)["status"], "ok")
        self.assertEqual(read_object_bytes(self.cfg, self.conn, obj), DATA)

    def test_corruption_same_size_detected(self):
        src = self.fresh_source("corrupt", DATA)
        obj = self.capture(src)
        row = object_chunks_rows(self.conn, obj["id"])[1]
        from app.vault import chunk_path
        import os
        p = chunk_path(self.cfg, row["digest"])
        raw = bytearray(p.read_bytes())
        raw[0] ^= 0xFF
        os.chmod(p, 0o644)
        p.write_bytes(bytes(raw))
        report = check_object(self.cfg, self.conn, obj)
        self.assertEqual([d["status"] for d in report["damaged"]], ["corrupt"])
        # Repair manually: the bytes were never quarantined by the demo tool.
        import os as _os
        _os.chmod(p, 0o644)
        p.write_bytes(DATA[row["offset"]:row["offset"] + row["length"]])
        report = check_object(self.cfg, self.conn, obj)
        self.assertEqual(report["status"], "ok", report)

    def test_shared_chunk_damage_affects_both_objects(self):
        src = self.fresh_source("shared-damage", DATA)
        a = self.capture(src)
        b = self.capture(src)
        rows = object_chunks_rows(self.conn, a["id"])
        target = rows[0]
        # A chunk shared by two objects: damaging it flags both objects, which
        # is the honest answer, and each object reports the same exact chunk.
        users = self.conn.execute(
            "SELECT COUNT(DISTINCT object_id) n FROM object_chunks WHERE digest=?",
            (target["digest"],)).fetchone()["n"]
        self.assertEqual(users, 2)
        simulate_damage(self.cfg, self.conn, a, target["seq"])
        self.assertEqual(check_object(self.cfg, self.conn, b)["status"], "damaged")
        restore_chunk(self.cfg, self.conn, target["digest"])
        self.assertEqual(check_object(self.cfg, self.conn, b)["status"], "ok")

    def test_export_refuses_damage_and_partial_declares_it(self):
        src = self.fresh_source("export", DATA)
        obj = self.capture(src)
        seq = object_chunks_rows(self.conn, obj["id"])[0]["seq"]
        sim = simulate_damage(self.cfg, self.conn, obj, seq)
        with self.assertRaises(ObjectDamageError):
            export_package(self.cfg, self.conn, obj)
        payload, summary = export_package(self.cfg, self.conn, obj, partial=True)
        zf = zipfile.ZipFile(io.BytesIO(payload))
        self.assertIn("damage.json", zf.namelist())
        dmg = json.loads(zf.read("damage.json"))
        self.assertEqual(dmg["status"], "partial")
        self.assertEqual(len(dmg["damaged_chunks"]), 1)
        restore_chunk(self.cfg, self.conn, sim["digest"])

    def test_export_package_verifies_standalone(self):
        src = self.fresh_source("pkg", DATA)
        obj = self.capture(src)
        payload, _ = export_package(self.cfg, self.conn, obj)
        dest = self.tmp / "pkgout"
        shutil.rmtree(dest, ignore_errors=True)
        dest.mkdir()
        zipfile.ZipFile(io.BytesIO(payload)).extractall(dest)
        proc = subprocess.run([sys.executable, "-I", "verify_package.py", "."],
                              cwd=str(dest), capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertIn("OK", proc.stdout)
        # A tampered chunk must fail verification.
        chunk_file = next((dest / "chunks").glob("*.bin"))
        import os
        os.chmod(chunk_file, 0o644)
        raw = bytearray(chunk_file.read_bytes())
        raw[-1] ^= 0x01
        chunk_file.write_bytes(bytes(raw))
        proc = subprocess.run([sys.executable, "-I", "verify_package.py", "."],
                              cwd=str(dest), capture_output=True, text=True)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("FAILED", proc.stdout)

    def test_manifest_is_immutable(self):
        src = self.fresh_source("immutable", DATA)
        obj = self.capture(src)
        from app.vault import manifest_path
        p = manifest_path(self.cfg, obj["manifest_digest"])
        self.assertEqual(sha256_hex(p.read_bytes()), obj["manifest_digest"])
        self.assertEqual(p.stat().st_mode & 0o222, 0, "manifest should be read-only")

    def test_get_object_by_prefix(self):
        src = self.fresh_source("prefix", DATA)
        obj = self.capture(src)
        self.assertIsNotNone(get_object(self.conn, obj["id"][:10]))
        self.assertIsNotNone(get_object(self.conn, obj["manifest_digest"]))
        self.assertIsNone(get_object(self.conn, "zzzzzzzzzz"))


if __name__ == "__main__":
    unittest.main()
