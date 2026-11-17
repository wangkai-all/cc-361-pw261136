"""Parse worker: an independent process that executes queued/resumed parse runs.

Run it separately from the web UI and the capture worker:

    python3 -m app.parse_worker            # loop
    python3 -m app.parse_worker --once     # one run then exit

Stopping: send SIGTERM/SIGINT (or press "Stop" in the UI). The run checkpoints
after every batch and exits as `paused` with its cursor persisted; the same or a
different worker can then resume it. SIGKILL is also safe: the lease on the run
expires and the next worker reclaims it, resuming from the last committed cursor
without duplicating or losing records.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sqlite3
import time
from datetime import datetime, timedelta, timezone

from .config import load_config
from .db import connect, transaction
from .parse_engine import StopSignal, run_parse
from .util import utcnow

RUN_LEASE_SECONDS = 60


def worker_name() -> str:
    return f"parse-{socket.gethostname()}-{os.getpid()}"


def _lease() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=RUN_LEASE_SECONDS)).isoformat()


def claim_run(conn: sqlite3.Connection, name: str):
    """Claim a queued/paused/orphaned parse run atomically."""
    now = utcnow()
    with transaction(conn):
        conn.execute(
            "UPDATE parse_runs SET status='paused', stopped_by='lease-expired', "
            "error='worker lease expired; resumable from last checkpoint' "
            "WHERE status='running' AND lease_until IS NOT NULL AND lease_until < ?",
            (now,),
        )
        row = conn.execute(
            "SELECT * FROM parse_runs WHERE status IN ('queued','paused') AND stop_requested=0 "
            "ORDER BY created_at LIMIT 1"
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE parse_runs SET status='running', started_at=COALESCE(started_at,?), "
            "lease_until=?, worker=? WHERE id=?",
            (utcnow(), _lease(), name, row["id"]),
        )
    return conn.execute("SELECT * FROM parse_runs WHERE id=?", (row["id"],)).fetchone()


def renew_lease(conn, run_id: str):
    conn.execute("UPDATE parse_runs SET lease_until=? WHERE id=?", (_lease(), run_id))
    conn.commit()


def insert_run(conn, object_id: str, rule_id: str, *, requested_by="cli") -> str:
    from .parse_engine import create_run

    obj = conn.execute("SELECT * FROM objects WHERE id=?", (object_id,)).fetchone()
    if not obj:
        raise ValueError(f"no such object {object_id}")
    return create_run(conn, obj, rule_id, requested_by=requested_by)


def run_worker(cfg, *, once: bool = False, idle_exit: bool = False, poll: float = 0.5):
    conn = connect(cfg.db_path)
    name = worker_name()
    stop = {"flag": False}

    def handler(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    print(f"[{name}] parse worker ready (data={cfg.data_dir})", flush=True)
    idle_since = time.time()
    try:
        while not stop["flag"]:
            run = claim_run(conn, name)
            if run:
                idle_since = time.time()
                print(f"[{name}] run {run['id']} rule={run['rule_id']} "
                      f"from cursor={run['cursor']}", flush=True)
                stop_signal = StopSignal()
                stop_signal.requested = stop["flag"]

                def progress(ev, _run_id=run["id"]):
                    renew_lease(conn, _run_id)

                # A stop requested between claims is honored immediately.
                if stop["flag"]:
                    stop_signal.requested = True
                result = run_parse(cfg, conn, run["id"], stop_signal,
                                   progress=progress, worker=name)
                print(f"[{name}] run {run['id']} -> {result.get('status')}", flush=True)
                if once:
                    return result
                continue
            if once:
                return None
            if idle_exit and time.time() - idle_since > 3:
                return None
            time.sleep(poll)
    finally:
        conn.close()
    return None


def main(argv=None):
    parser = argparse.ArgumentParser(description="sealed vault parse worker")
    parser.add_argument("--once", action="store_true", help="claim one run then exit")
    parser.add_argument("--idle-exit", action="store_true", help="exit when no run is queued")
    parser.add_argument("--poll", type=float, default=0.5)
    parser.add_argument("--object", help="with --rule, enqueue a run before working")
    parser.add_argument("--rule", help="rule id, e.g. html_records@1")
    parser.add_argument("--enqueue-only", action="store_true")
    args = parser.parse_args(argv)
    cfg = load_config()
    if args.object and args.rule:
        conn = connect(cfg.db_path)
        try:
            run_id = insert_run(conn, args.object, args.rule, requested_by="cli")
            print(f"queued run {run_id}")
        finally:
            conn.close()
        if args.enqueue_only:
            return
    result = run_worker(cfg, once=args.once, idle_exit=args.idle_exit, poll=args.poll)
    if args.once:
        print(json.dumps(result, indent=2) if result else "no run queued")


if __name__ == "__main__":
    main()
