#!/usr/bin/env python3
"""Pause/resume and hard-kill resilience tests.

Run: python3 -I tools/resume_test.py
"""
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
tmp = Path(tempfile.mkdtemp(prefix="vault-resume-"))
env = dict(os.environ)
env["VAULT_DATA_DIR"] = str(tmp / "data")
env["VAULT_FIXTURES_DIR"] = str(tmp / "fixtures")
env["PYTHONPATH"] = str(ROOT)
# The in-process API reads the same environment the subprocesses will use.
os.environ.update({"VAULT_DATA_DIR": str(tmp / "data"),
                   "VAULT_FIXTURES_DIR": str(tmp / "fixtures")})

sys.path.insert(0, str(ROOT))
from app.capture_worker import claim_job, enqueue_capture, run_capture  # noqa: E402
from app.config import load_config  # noqa: E402
from app.db import connect, init_db  # noqa: E402
from app.fixtures import seed_demo_sources  # noqa: E402
from app.parse_engine import create_run, result_view, run_parse  # noqa: E402
from app.rules import sync_rules  # noqa: E402
from app.parse_engine import StopSignal  # noqa: E402

failures = []


def check(name, cond, detail=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        failures.append((name, detail))


cfg = load_config()
init_db(cfg.db_path)
conn = connect(cfg.db_path)
conn.execute("DELETE FROM events")
conn.commit()
seed_demo_sources(cfg, conn, origin_base="demo://")
sync_rules(conn, cfg.rules_dir)
src = conn.execute("SELECT * FROM sources WHERE name='ticker-news'").fetchone()
enqueue_capture(conn, src)
run_capture(cfg, conn, claim_job(conn, "t"), worker="t")
obj = conn.execute("SELECT * FROM objects LIMIT 1").fetchone()

# ---- 1. cooperative stop mid-run, then resume with same result digest ----
ref_run = create_run(conn, obj, "html_records@2")
ref = run_parse(cfg, conn, ref_run, worker="t")
ref_view = result_view(conn, ref_run)


class StopAfterN(StopSignal):
    def __init__(self, n):
        super().__init__()
        self.n = n
        self.count = 0

    def install(self):
        pass


run_a = create_run(conn, obj, "html_records@2")
# Stop after the first checkpoint by using a progress callback that flips the flag.
stop = StopSignal()
seen = {"n": 0}


def progress(ev):
    seen["n"] += 1
    stop.requested = True
    stop.reason = "test-stop"


res_a = run_parse(cfg, conn, run_a, stop, progress=progress, worker="t")
check("stopped run paused", res_a.get("status") == "paused", str(res_a))
paused_row = conn.execute("SELECT * FROM parse_runs WHERE id=?", (run_a,)).fetchone()
check("pause checkpoint stored", paused_row["cursor"] >= 0 and paused_row["status"] == "paused")
partial = conn.execute("SELECT COUNT(*) n FROM parse_records WHERE run_id=?", (run_a,)).fetchone()["n"]
res_b = run_parse(cfg, conn, run_a, StopSignal(), worker="t")
check("resume completes", res_b.get("status") == "done", str(res_b))
view_b = result_view(conn, run_a)
check("resume result identical to uninterrupted", view_b["run"]["result_digest"] == ref["result_digest"],
      f"{view_b['run']['result_digest']} vs {ref['result_digest']}")
check("no duplicate records after resume",
      len(view_b["records"]) == len(ref_view["records"]),
      f"{len(view_b['records'])} vs {len(ref_view['records'])}")

# ---- 2. real SIGKILL of the parse worker process mid-run ----
# Make a bigger object so the run has time to be killed.
big = b"<html><body>\n" + b"".join(
    f'<article data-seq="{i}" data-rev="0"><h2>big / a / b</h2>'
    f'<p class="note">row {i}</p><span class="value">{i}</span></article>\n'.encode()
    for i in range(4000)
) + b"</body></html>\n"
bigpath = tmp / "fixtures" / "big.bin"
bigpath.parent.mkdir(parents=True, exist_ok=True)
bigpath.write_bytes(big)
conn.execute(
    "INSERT INTO sources(id,name,origin,url,headers_json,mode,static_path,status,created_at,notes) "
    "VALUES('src_big','big','demo://big','demo://big','{}','static',?, 'active', '2020', '')",
    (str(bigpath),),
)
conn.commit()
bigsrc = conn.execute("SELECT * FROM sources WHERE id='src_big'").fetchone()
enqueue_capture(conn, bigsrc)
run_capture(cfg, conn, claim_job(conn, "t"), worker="t")
bigobj = conn.execute(
    "SELECT * FROM objects WHERE source_id='src_big' ORDER BY created_at DESC LIMIT 1").fetchone()
check("big object captured", bigobj is not None and bigobj["chunk_count"] > 10, str(bigobj["chunk_count"]) if bigobj else "none")

bigrun = create_run(conn, bigobj, "html_records@2")
# Launch worker as a real process.
proc = subprocess.Popen(
    [sys.executable, "-m", "app.parse_worker", "--once"],
    cwd=str(ROOT), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
)
# Wait until it starts making progress, then SIGKILL.
deadline = time.time() + 10
while time.time() < deadline:
    row = conn.execute("SELECT status, processed_chunks, result_count FROM parse_runs WHERE id=?",
                       (bigrun,)).fetchone()
    if row["status"] == "running" and row["result_count"] > 0:
        break
    time.sleep(0.05)
proc.send_signal(signal.SIGKILL)
proc.wait(timeout=10)
row = conn.execute("SELECT * FROM parse_runs WHERE id=?", (bigrun,)).fetchone()
check("killed run left resumable", row["status"] == "running", row["status"])
killed_cursor = row["cursor"]
killed_records = conn.execute(
    "SELECT COUNT(*) n FROM parse_records WHERE run_id=?", (bigrun,)).fetchone()["n"]
check("kill kept committed progress", killed_records > 0, str(killed_records))

# A second worker reclaims after lease expiry (simulate by clearing lease).
conn.execute("UPDATE parse_runs SET lease_until='2000-01-01T00:00:00' WHERE id=?", (bigrun,))
conn.commit()
proc2 = subprocess.run(
    [sys.executable, "-m", "app.parse_worker", "--once", "--poll", "0.1"],
    cwd=str(ROOT), env=env, capture_output=True, text=True, timeout=120,
)
row = conn.execute("SELECT * FROM parse_runs WHERE id=?", (bigrun,)).fetchone()
check("killed run resumed by new worker", row["status"] == "done", f"{row['status']} {proc2.stdout[-500:]}")
check("resumed run has full record count", row["result_count"] == 4000, str(row["result_count"]))
check("resumed cursor past kill point", row["cursor"] >= killed_cursor)
view = result_view(conn, bigrun)
check("no gap introduced by kill", len(view["gaps"]) == 0, str(view["gaps"]))
check("resumed parse reports no duplicate ordinals",
      len({r["ordinal"] for r in view["records"]}) == len(view["records"]))

# Reference: uninterrupted run over the big object must match digest.
refbig = create_run(conn, bigobj, "html_records@2")
refbig_res = run_parse(cfg, conn, refbig, worker="t")
check("resumed digest equals fresh digest", row["result_digest"] == refbig_res["result_digest"],
      f"{row['result_digest']} vs {refbig_res.get('result_digest')}")

# ---- 3. damage during a done run blocks only dependent records ----
from app.vault import simulate_damage, restore_chunk  # noqa: E402
rows = conn.execute("SELECT seq FROM object_chunks WHERE object_id=? ORDER BY seq", (bigobj["id"],)).fetchall()
mid = rows[len(rows) // 2]["seq"]
dmg = simulate_damage(cfg, conn, bigobj, mid)
check("big damage detected", dmg["report"]["status"] == "damaged")
v = result_view(conn, bigrun)
blocked = [r for r in v["records"] if r["blocked"]]
ok = [r for r in v["records"] if not r["blocked"]]
check("damage blocks a subset", 0 < len(blocked) < len(v["records"]), f"{len(blocked)}/{len(v['records'])}")
check("majority still readable", len(ok) > 3000, str(len(ok)))
blockset = set()
for r in blocked:
    blockset.update(r["blocked_by"])
check("blocked_by names exact chunk", blockset == {dmg["digest"]}, str(blockset))
restore_chunk(cfg, conn, dmg["digest"])
v2 = result_view(conn, bigrun)
check("all readable after repair", all(not r["blocked"] for r in v2["records"]))

print()
print(f"{len(failures)} failure(s)")
if failures:
    for n, d in failures:
        print(" -", n, d)
    sys.exit(1)
import shutil
shutil.rmtree(tmp, ignore_errors=True)
