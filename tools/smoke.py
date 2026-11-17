#!/usr/bin/env python3
"""End-to-end smoke test of the whole pipeline (no web server required).

Run: python3 tools/smoke.py
"""
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

tmp = Path(tempfile.mkdtemp(prefix="vault-smoke-"))
os.environ["VAULT_DATA_DIR"] = str(tmp / "data")
os.environ["VAULT_FIXTURES_DIR"] = str(tmp / "fixtures")

from app.capture_worker import claim_job, enqueue_capture, run_capture  # noqa: E402
from app.config import load_config  # noqa: E402
from app.db import connect, init_db  # noqa: E402
from app.fixtures import bump_fixture, seed_demo_sources  # noqa: E402
from app.parse_engine import build_regions, compare_runs, create_run, result_view, run_parse  # noqa: E402
from app.rules import sync_rules  # noqa: E402
from app.vault import check_object, export_package, get_object, load_manifest  # noqa: E402

failures = []


def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        failures.append((name, detail))


cfg = load_config()
init_db(cfg.db_path)
conn = connect(cfg.db_path)
seed_demo_sources(cfg, conn, origin_base="demo://")
res = sync_rules(conn, cfg.rules_dir)
check("rules registered", "html_records@1" in res["rules"] and "html_records@2" in res["rules"], str(res))

src = conn.execute("SELECT * FROM sources WHERE name='ticker-news'").fetchone()
job_id, cap_id = enqueue_capture(conn, src)
job = claim_job(conn, "smoke")
result = run_capture(cfg, conn, job, worker="smoke")
check("capture sealed", result.get("status") == "done", str(result))
obj = get_object(conn, result["object_id"])
check("object stored", obj is not None)
om = load_manifest(cfg, obj["manifest_digest"])
orig = (cfg.fixtures_dir / "ticker-news.bin").read_bytes()
check("manifest stream digest matches fixture", om.stream_digest == __import__("hashlib").sha256(orig).hexdigest())
check("chunk count > 1", om.manifest["chunks"] and len(om.manifest["chunks"]) > 1, str(len(om.manifest["chunks"])))

rep = check_object(cfg, conn, obj)
check("fresh object verifies", rep["status"] == "ok", str(rep))
regions, gaps = build_regions(cfg, conn, obj)
check("no gaps on intact object", len(gaps) == 0, str(gaps))

# parse with v1 then v2
run1 = create_run(conn, obj, "html_records@1")
r1 = run_parse(cfg, conn, run1, worker="smoke")
check("v1 parse done", r1.get("status") == "done", str(r1))
run2 = create_run(conn, obj, "html_records@2")
r2 = run_parse(cfg, conn, run2, worker="smoke")
check("v2 parse done", r2.get("status") == "done", str(r2))
v1 = result_view(conn, run1)
v2 = result_view(conn, run2)
check("v1 records", len(v1["records"]) > 0, str(len(v1["records"])))
check("v2 records", len(v2["records"]) == len(v1["records"]))
check("spans exact", all(v1["records"][i]["span"] == v2["records"][i]["span"] for i in range(len(v1["records"]))))
cmp = compare_runs(conn, run1, run2)
check("compare detects changes", cmp["summary"]["changed"] > 0, str(cmp["summary"]))

# determinism: repeat v1 on same object, digest must match
run1b = create_run(conn, obj, "html_records@1")
r1b = run_parse(cfg, conn, run1b, worker="smoke")
check("parse reproducible", r1b["result_digest"] == r1["result_digest"], f"{r1b} vs {r1}")

# source changes -> new capture, old object still exact
bump_fixture(cfg, "ticker-news")
job2, _ = enqueue_capture(conn, src)
res2 = run_capture(cfg, conn, claim_job(conn, "smoke2"), worker="smoke2")
check("re-capture after change", res2.get("status") == "done", str(res2))
obj2 = get_object(conn, res2["object_id"])
old = check_object(cfg, conn, obj)
check("old object after source change", old["status"] == "ok")
new_bytes = (cfg.fixtures_dir / "ticker-news.bin").read_bytes()
check("new bytes differ", new_bytes != orig)
check("new object different digest", obj2["manifest_digest"] != obj["manifest_digest"])

# withdrawal blocks new capture but not reads
conn.execute("UPDATE sources SET status='withdrawn' WHERE id=?", (src["id"],))
conn.commit()
try:
    enqueue_capture(conn, conn.execute("SELECT * FROM sources WHERE id=?", (src["id"],)).fetchone())
    check("withdrawn blocks capture", False)
except PermissionError:
    check("withdrawn blocks capture", True)
reread = result_view(conn, run1)
check("results readable after withdrawal", len(reread["records"]) > 0)

# damage localisation
detail = [c["seq"] for c in __import__("app.vault", fromlist=["object_chunks_rows"]).object_chunks_rows(conn, obj["id"])]
damage_seq = detail[len(detail) // 2]
from app.vault import simulate_damage, restore_chunk  # noqa: E402
dmg = simulate_damage(cfg, conn, obj, damage_seq)
check("damage detected", dmg["report"]["status"] == "damaged", str(dmg["report"]["status"]))
check("only one damaged chunk", len(dmg["report"]["damaged"]) == 1, str(dmg["report"]["damaged"]))
view = result_view(conn, run1)
blocked = sum(1 for r in view["records"] if r["blocked"])
check("some records blocked", blocked > 0, str(blocked))
check("some records still readable", blocked < len(view["records"]), f"{blocked}/{len(view['records'])}")
# object2 unaffected
rep2 = check_object(cfg, conn, obj2)
check("other object unaffected", rep2["status"] == "ok", str(rep2["status"]))

# export refuses damaged, allows partial
try:
    export_package(cfg, conn, obj)
    check("export refuses damaged", False)
except Exception:
    check("export refuses damaged", True)
payload, summary = export_package(cfg, conn, obj, partial=True)
check("partial export works", len(payload) > 100 and summary["report"]["status"] == "damaged")

restore_chunk(cfg, conn, dmg["digest"])
view2 = result_view(conn, run1)
check("restore unblocks records", all(not r["blocked"] for r in view2["records"]))
rep3 = check_object(cfg, conn, obj)
check("object ok after repair", rep3["status"] == "ok", str(rep3["status"]))

# standalone package verifies
payload, summary = export_package(cfg, conn, obj, run_ids=[run1])
pkgdir = tmp / "pkg"
shutil.rmtree(pkgdir, ignore_errors=True)
pkgdir.mkdir()
import zipfile  # noqa: E402
zipfile.ZipFile(__import__("io").BytesIO(payload)).extractall(pkgdir)
import subprocess  # noqa: E402
proc = subprocess.run([sys.executable, "-I", str(pkgdir / "verify_package.py"), str(pkgdir)],
                      capture_output=True, text=True)
check("standalone package verifies", proc.returncode == 0 and "OK" in proc.stdout,
      proc.stdout + proc.stderr)

# split pipeline: fetch stages, a separate seal stage commits the vault
from app.capture_worker import claim_fetch_job, claim_seal_job, run_fetch, run_seal  # noqa: E402
from app.util import sha256_hex  # noqa: E402
mkt = conn.execute("SELECT * FROM sources WHERE name='market-csv'").fetchone()
split_job, split_cap = enqueue_capture(conn, mkt, inline=False)
fetched = run_fetch(cfg, conn, claim_fetch_job(conn, "smoke-fetch", inline=False),
                    worker="smoke-fetch")
check("split fetch stages only", fetched.get("status") == "fetched", str(fetched))
check("no vault bytes before seal",
      conn.execute("SELECT COUNT(*) n FROM objects WHERE capture_id=?",
                   (split_cap,)).fetchone()["n"] == 0)
staged_file = Path(fetched["staged_path"])
check("staged file is byte-exact",
      staged_file.read_bytes() == (cfg.fixtures_dir / "market-csv.bin").read_bytes())
check("staged digest recorded", fetched["sha256"] == sha256_hex(staged_file.read_bytes()))
sealed = run_seal(cfg, conn, claim_seal_job(conn, "smoke-seal", split_only=True),
                  worker="smoke-seal")
check("split seal commits object", sealed.get("status") == "done", str(sealed))
check("staging consumed", not staged_file.exists())
split_obj = get_object(conn, sealed["object_id"])
check("split object stream digest", split_obj["stream_digest"] == fetched["sha256"])

# staging tamper is refused, so a bad handoff can never be sealed silently
job_bad, cap_bad = enqueue_capture(conn, mkt, inline=False)
run_fetch(cfg, conn, claim_fetch_job(conn, "smoke-fetch", inline=False), worker="smoke-fetch")
bad_path = cfg.staging_dir / f"{cap_bad}.staged"
raw = bytearray(bad_path.read_bytes())
raw[5] ^= 0xFF
bad_path.write_bytes(bytes(raw))
bad = run_seal(cfg, conn, claim_seal_job(conn, "smoke-seal", split_only=True),
               worker="smoke-seal")
check("tampered staging refused", bad.get("status") == "failed"
      and "StagingMismatch" in bad.get("error", ""), str(bad))
check("no object from tampered staging",
      conn.execute("SELECT COUNT(*) n FROM objects WHERE capture_id=?",
                   (cap_bad,)).fetchone()["n"] == 0)
bad_path.unlink(missing_ok=True)

# chunk sharing across two captures of unchanged sources
src2 = conn.execute("SELECT * FROM sources WHERE name='port-authority'").fetchone()
job3, _ = enqueue_capture(conn, src2)
run_capture(cfg, conn, claim_job(conn, "smoke3"), worker="smoke3")
job4, _ = enqueue_capture(conn, src2)
run_capture(cfg, conn, claim_job(conn, "smoke4"), worker="smoke4")
shared = conn.execute("SELECT COUNT(*) n FROM chunks WHERE refcount>1").fetchone()["n"]
check("chunk sharing across identical captures", shared > 0, str(shared))

print()
print(f"{len(failures)} failure(s)")
if failures:
    for name, detail in failures:
        print(f" - {name}: {detail}")
    sys.exit(1)
print("temp dir:", tmp)
shutil.rmtree(tmp, ignore_errors=True)
