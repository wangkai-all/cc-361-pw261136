"""Capture pipeline: fetch to staging, then seal from staging.

Two stages, deliberately separated:

* **fetch** (`run_fetch`) is the only stage that may touch a network origin or
  the live fixture files. Its sole durable output is a staging file plus a
  recorded length and SHA-256. It never writes chunks or manifests.
* **seal** (`run_seal`) reads only the staging file, re-hashes it against the
  fetch record, and performs every vault write. It never opens a source.

`app.capture_worker` runs both stages in one process (the default).
`app.fetch_worker` and `app.seal_worker` run them in separate processes, which
is the deployment shape the brief calls for: fetcher, sealed store and parser
all run independently.

Claiming is lease-based so multiple workers of the same kind are safe. A hard
kill leaves the lease to expire. Combined jobs are `fetch_jobs.inline=1` and
split jobs are `inline=0`, so the two worker styles never consume each other's
work. The seal stage is idempotent: if a worker dies after `seal_stream` commits
the object but before the seal job is marked done, the expired lease re-queues
the job, and `run_seal` finds the object by capture id, finishes the bookkeeping
and removes staging instead of creating a second object.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sqlite3
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .config import Config, load_config
from .db import connect, log_event, transaction
from .fixtures import read_fixture
from .util import new_id, utcnow
from .vault import (
    SealedObject,
    SizeLimitExceeded,
    StagingMismatch,
    seal_staged,
    stage_stream,
)

LEASE_SECONDS = 120


class Canceled(Exception):
    pass


def worker_name(kind: str = "capture") -> str:
    return f"{kind}-{socket.gethostname()}-{os.getpid()}"


def _lease() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=LEASE_SECONDS)).isoformat()


# --------------------------------------------------------------------------
# enqueue / cancel
# --------------------------------------------------------------------------

def enqueue_capture(conn, source_row, *, requested_by: str = "web",
                    inline: bool = True) -> tuple[str, str]:
    """Create a capture attempt and a queued fetch job. Returns (job id, capture id).

    `inline=True` marks the job for the combined worker, which fetches and seals
    in one process. `inline=False` marks it for the split pipeline: a fetch
    worker writes staging and a separate seal worker performs the vault writes.
    Either worker ignores jobs marked for the other, so both can run at once.

    Rejects withdrawn sources: withdrawal means no new capture may start, while
    every already-sealed object stays readable.
    """
    src = conn.execute("SELECT * FROM sources WHERE id=?", (source_row["id"],)).fetchone()
    if src["status"] == "withdrawn":
        raise PermissionError(
            f"source {src['name']} is withdrawn; no new capture can be started, "
            "but existing sealed objects remain readable"
        )
    job_id = new_id("fch")
    cap_id = new_id("cap")
    now = utcnow()
    with transaction(conn):
        conn.execute(
            "INSERT INTO captures(id, source_id, status, started_at, job_id) "
            "VALUES(?,?,?,?,?)",
            (cap_id, src["id"], "pending", now, job_id),
        )
        conn.execute(
            "INSERT INTO fetch_jobs(id, source_id, capture_id, status, inline, "
            "created_at) VALUES(?,?,?,?,?,?)",
            (job_id, src["id"], cap_id, "queued", 1 if inline else 0, now),
        )
        log_event(conn, "capture.queued", "capture", cap_id,
                  {"job_id": job_id, "source_id": src["id"]}, actor=requested_by)
    return job_id, cap_id


def cancel_capture(conn, job_id: str) -> bool:
    """Cancel a fetch that has not been sealed yet.

    A running fetch is asked to stop at the next block; a queued one is closed
    immediately. If the seal stage has already claimed the staged bytes, cancel
    is refused (it would report a cancellation that did not happen) and the
    caller can decide to let the seal finish.
    """
    with transaction(conn):
        row = conn.execute("SELECT * FROM fetch_jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            return False
        if row["status"] in ("sealed", "failed", "canceled"):
            return False
        if row["status"] == "running":
            conn.execute("UPDATE fetch_jobs SET cancel_requested=1 WHERE id=?", (job_id,))
        elif row["status"] == "fetched":
            seal = conn.execute(
                "SELECT status FROM seal_jobs WHERE fetch_job_id=?", (job_id,)
            ).fetchone()
            if seal and seal["status"] != "queued":
                return False
            conn.execute(
                "UPDATE seal_jobs SET status='failed', finished_at=?, error='canceled' "
                "WHERE fetch_job_id=? AND status='queued'",
                (utcnow(), job_id),
            )
            conn.execute(
                "UPDATE fetch_jobs SET status='canceled', finished_at=? WHERE id=?",
                (utcnow(), job_id),
            )
            conn.execute(
                "UPDATE captures SET status='aborted', finished_at=?, error=? WHERE id=?",
                (utcnow(), "canceled after fetch, before seal", row["capture_id"]),
            )
            if row["staged_path"]:
                Path(row["staged_path"]).unlink(missing_ok=True)
        else:  # queued
            conn.execute(
                "UPDATE fetch_jobs SET status='canceled', finished_at=? WHERE id=?",
                (utcnow(), job_id),
            )
            conn.execute(
                "UPDATE captures SET status='aborted', finished_at=?, error=? WHERE id=?",
                (utcnow(), "canceled before sealing", row["capture_id"]),
            )
        log_event(conn, "capture.cancel", "fetch_job", job_id,
                  {"status": row["status"]}, actor="web")
    return True


def fetch_is_canceled(conn, job_id: str) -> bool:
    row = conn.execute(
        "SELECT cancel_requested, status FROM fetch_jobs WHERE id=?", (job_id,)
    ).fetchone()
    return bool(row and (row["cancel_requested"] or row["status"] == "canceled"))


# --------------------------------------------------------------------------
# fetch stage
# --------------------------------------------------------------------------

def _open_source(cfg: Config, source_row) -> tuple[Iterable[bytes], dict, str]:
    """Return (byte-block iterator, lazy metadata box, fetched url).

    Metadata is filled in by the HTTP path as soon as response headers arrive,
    so the staged record describes the exact response that produced the bytes.
    """
    headers = json.loads(source_row["headers_json"] or "{}")
    mode = source_row["mode"]
    url = source_row["url"]
    meta_box: dict = {"headers": {}, "status": None, "ctype": None}

    if mode == "static":
        data = read_fixture(cfg, source_row["name"])
        meta_box.update(status=200, ctype="application/octet-stream",
                        headers={"x-source-mode": "static-fixture"})

        def static_blocks():
            for i in range(0, len(data), 4096):
                yield data[i:i + 4096]

        return static_blocks(), meta_box, f"file:{source_row['static_path']}"

    def http_blocks():
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=cfg.fetch_timeout) as resp:
            meta_box["headers"] = {k.lower(): v for k, v in resp.getheaders()}
            meta_box["status"] = resp.status
            meta_box["ctype"] = resp.headers.get_content_type() if resp.headers else None
            while True:
                block = resp.read(4096)
                if not block:
                    break
                yield block

    return http_blocks(), meta_box, url


def claim_fetch_job(conn: sqlite3.Connection, name: str, *, inline: bool | None = None):
    """Claim a queued fetch job. `inline` selects the combined or split pipeline."""
    now = utcnow()
    with transaction(conn):
        # Recover jobs whose worker died mid-fetch (lease expired).
        conn.execute(
            "UPDATE fetch_jobs SET status='queued', worker=NULL, lease_until=NULL "
            "WHERE status='running' AND lease_until IS NOT NULL AND lease_until < ?",
            (now,),
        )
        if inline is None:
            row = conn.execute(
                "SELECT * FROM fetch_jobs WHERE status='queued' ORDER BY created_at LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM fetch_jobs WHERE status='queued' AND inline=? "
                "ORDER BY created_at LIMIT 1",
                (1 if inline else 0,),
            ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE fetch_jobs SET status='running', started_at=?, lease_until=?, worker=? "
            "WHERE id=?",
            (now, _lease(), name, row["id"]),
        )
        conn.execute(
            "UPDATE captures SET status='running' WHERE id=? AND status='pending'",
            (row["capture_id"],),
        )
    return conn.execute("SELECT * FROM fetch_jobs WHERE id=?", (row["id"],)).fetchone()


def run_fetch(cfg: Config, conn, fetch_row, *, worker: str) -> dict:
    """Fetch one source into staging. Never writes chunk or manifest files."""
    source = conn.execute("SELECT * FROM sources WHERE id=?", (fetch_row["source_id"],)).fetchone()
    capture = conn.execute("SELECT * FROM captures WHERE id=?", (fetch_row["capture_id"],)).fetchone()
    if not source or not capture:
        _fail_fetch(conn, fetch_row["id"], "source or capture disappeared")
        return {"job_id": fetch_row["id"], "status": "failed"}
    try:
        if source["status"] == "withdrawn":
            raise PermissionError("source is withdrawn")
        blocks, meta_box, url = _open_source(cfg, source)

        def on_progress(total):
            conn.execute("UPDATE captures SET bytes_captured=? WHERE id=?",
                         (total, capture["id"]))
            conn.commit()

        def watched():
            for block in blocks:
                if fetch_is_canceled(conn, fetch_row["id"]):
                    raise Canceled()
                yield block

        staged = stage_stream(cfg, capture["id"], watched(), on_progress=on_progress)
    except Canceled:
        _fail_fetch(conn, fetch_row["id"], "canceled by user", canceled=True)
        return {"job_id": fetch_row["id"], "status": "canceled"}
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, SizeLimitExceeded,
            PermissionError, ValueError, RuntimeError) as exc:
        _fail_fetch(conn, fetch_row["id"], f"{type(exc).__name__}: {exc}")
        return {"job_id": fetch_row["id"], "status": "failed", "error": str(exc)}

    meta = {
        "source_name": source["name"],
        "source_origin": source["origin"],
        "source_url": source["url"],
        "fetched_url": url,
        "http_status": meta_box.get("status"),
        "content_type": meta_box.get("ctype"),
        "headers": meta_box.get("headers", {}),
        "request_headers": json.loads(source["headers_json"] or "{}"),
        "started_at": capture["started_at"],
        "fetched_at": utcnow(),
    }
    seal_id = new_id("sel")
    with transaction(conn):
        conn.execute(
            "UPDATE fetch_jobs SET status='fetched', finished_at=?, staged_path=?, "
            "meta_json=?, stream_sha256=?, bytes=?, http_status=? WHERE id=?",
            (utcnow(), staged["path"], json.dumps(meta), staged["sha256"],
             staged["bytes"], meta.get("http_status"), fetch_row["id"]),
        )
        conn.execute(
            "INSERT INTO seal_jobs(id, fetch_job_id, capture_id, status, created_at) "
            "VALUES(?,?,?,?,?)",
            (seal_id, fetch_row["id"], capture["id"], "queued", utcnow()),
        )
        conn.execute(
            "UPDATE captures SET status='fetched', content_type=?, http_status=?, "
            "fetched_url=?, headers_json=?, bytes_captured=?, staged_path=?, "
            "staged_meta_json=?, staged_at=? WHERE id=?",
            (meta.get("content_type"), meta.get("http_status"), url,
             json.dumps(meta.get("headers", {})), staged["bytes"], staged["path"],
             json.dumps({"bytes": staged["bytes"], "sha256": staged["sha256"], **meta}),
             utcnow(), capture["id"]),
        )
        log_event(conn, "capture.fetched", "capture", capture["id"],
                  {"job_id": fetch_row["id"], "bytes": staged["bytes"],
                   "sha256": staged["sha256"], "seal_job": seal_id}, actor=worker)
    return {
        "job_id": fetch_row["id"],
        "status": "fetched",
        "capture_id": capture["id"],
        "bytes": staged["bytes"],
        "sha256": staged["sha256"],
        "seal_job_id": seal_id,
        "staged_path": staged["path"],
    }


def _fail_fetch(conn, job_id: str, error: str, *, canceled: bool = False):
    row = conn.execute("SELECT * FROM fetch_jobs WHERE id=?", (job_id,)).fetchone()
    with transaction(conn):
        conn.execute(
            "UPDATE fetch_jobs SET status=?, finished_at=?, error=? WHERE id=?",
            ("canceled" if canceled else "failed", utcnow(), error, job_id),
        )
        if row and row["capture_id"]:
            conn.execute(
                "UPDATE captures SET status=?, finished_at=?, error=? WHERE id=?",
                ("aborted" if canceled else "failed", utcnow(), error, row["capture_id"]),
            )
        log_event(conn, "capture.cancel" if canceled else "capture.failed", "fetch_job",
                  job_id, {"error": error}, actor="fetch-worker")


# --------------------------------------------------------------------------
# seal stage
# --------------------------------------------------------------------------

def claim_seal_job(conn: sqlite3.Connection, name: str, *, fetch_job_id: str | None = None,
                   split_only: bool = False, inline_only: bool = False):
    """Claim a queued seal job.

    `split_only` restricts the claim to jobs produced by the split fetch worker,
    and `inline_only` to jobs produced by the combined fetch+seal worker, so a
    standalone seal worker never races the combined worker for its just-fetched
    staging file.
    """
    now = utcnow()
    with transaction(conn):
        conn.execute(
            "UPDATE seal_jobs SET status='queued', worker=NULL, lease_until=NULL "
            "WHERE status='running' AND lease_until IS NOT NULL AND lease_until < ?",
            (now,),
        )
        if fetch_job_id:
            row = conn.execute(
                "SELECT * FROM seal_jobs WHERE fetch_job_id=? AND status='queued'",
                (fetch_job_id,),
            ).fetchone()
        else:
            query = (
                "SELECT s.* FROM seal_jobs s JOIN fetch_jobs f ON f.id=s.fetch_job_id "
                "WHERE s.status='queued'"
            )
            params: tuple = ()
            if split_only:
                query += " AND f.inline=0"
            if inline_only:
                query += " AND f.inline=1"
            row = conn.execute(query + " ORDER BY s.created_at LIMIT 1", params).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE seal_jobs SET status='running', started_at=?, lease_until=?, worker=? "
            "WHERE id=?",
            (now, _lease(), name, row["id"]),
        )
    return conn.execute("SELECT * FROM seal_jobs WHERE id=?", (row["id"],)).fetchone()


def run_seal(cfg: Config, conn, seal_row, *, worker: str) -> dict:
    """Seal one staged fetch into the vault. Reads only the staging file.

    Idempotent: if a previous attempt committed the object but died before
    recording the seal job, the retry recognises the object instead of failing
    on the staged file it already consumed.
    """
    done = conn.execute(
        "SELECT * FROM objects WHERE capture_id=? ORDER BY created_at DESC LIMIT 1",
        (seal_row["capture_id"],),
    ).fetchone()
    if done:
        # The previous attempt committed the object but died before recording
        # it. Finish the bookkeeping and clear any staging file it left behind.
        fetch = conn.execute(
            "SELECT staged_path FROM fetch_jobs WHERE id=?", (seal_row["fetch_job_id"],)
        ).fetchone()
        if fetch and fetch["staged_path"]:
            Path(fetch["staged_path"]).unlink(missing_ok=True)
        with transaction(conn):
            conn.execute(
                "UPDATE seal_jobs SET status='done', finished_at=?, object_id=?, error=NULL "
                "WHERE id=?",
                (utcnow(), done["id"], seal_row["id"]),
            )
            conn.execute(
                "UPDATE fetch_jobs SET status='sealed', finished_at=? WHERE id=?",
                (utcnow(), seal_row["fetch_job_id"]),
            )
            log_event(conn, "seal.recovered", "object", done["id"],
                      {"seal_job_id": seal_row["id"]}, actor=worker)
        return {"seal_job_id": seal_row["id"], "status": "done", "object_id": done["id"],
                "recovered": True}

    capture = conn.execute("SELECT * FROM captures WHERE id=?", (seal_row["capture_id"],)).fetchone()
    if not capture:
        _fail_seal(conn, seal_row["id"], "capture disappeared")
        return {"seal_job_id": seal_row["id"], "status": "failed"}
    try:
        sealed: SealedObject = seal_staged(cfg, conn, capture, worker=worker)
    except (StagingMismatch, OSError, ValueError, RuntimeError) as exc:
        message = f"{type(exc).__name__}: {exc}"
        _fail_seal(conn, seal_row["id"], message)
        return {"seal_job_id": seal_row["id"], "status": "failed", "error": message}

    with transaction(conn):
        conn.execute(
            "UPDATE seal_jobs SET status='done', finished_at=?, object_id=?, error=NULL "
            "WHERE id=?",
            (utcnow(), sealed.object_id, seal_row["id"]),
        )
        conn.execute(
            "UPDATE fetch_jobs SET status='sealed', finished_at=? WHERE id=?",
            (utcnow(), seal_row["fetch_job_id"]),
        )
        log_event(conn, "object.sealed", "object", sealed.object_id,
                  {"fetch_job_id": seal_row["fetch_job_id"],
                   "capture_id": seal_row["capture_id"],
                   "manifest_digest": sealed.manifest_digest,
                   "bytes": sealed.total_bytes, "chunks": sealed.chunk_count},
                  actor=worker)
    return {
        "seal_job_id": seal_row["id"],
        "status": "done",
        "object_id": sealed.object_id,
        "manifest_digest": sealed.manifest_digest,
        "total_bytes": sealed.total_bytes,
        "chunk_count": sealed.chunk_count,
        "duplicate_bytes": sealed.duplicate_bytes,
    }


def _fail_seal(conn, seal_id: str, error: str):
    row = conn.execute("SELECT * FROM seal_jobs WHERE id=?", (seal_id,)).fetchone()
    with transaction(conn):
        conn.execute(
            "UPDATE seal_jobs SET status='failed', finished_at=?, error=? WHERE id=?",
            (utcnow(), error, seal_id),
        )
        if row:
            conn.execute(
                "UPDATE fetch_jobs SET status='failed', finished_at=?, error=? WHERE id=?",
                (utcnow(), error, row["fetch_job_id"]),
            )
            conn.execute(
                "UPDATE captures SET status='failed', finished_at=?, error=? WHERE id=?",
                (utcnow(), error, row["capture_id"]),
            )
        log_event(conn, "seal.failed", "seal_job", seal_id, {"error": error},
                  actor="seal-worker")


# --------------------------------------------------------------------------
# combined worker (fetch then seal in one process)
# --------------------------------------------------------------------------

def claim_job(conn: sqlite3.Connection, name: str):
    """Claim a queued fetch job for the combined (fetch+seal) worker."""
    return claim_fetch_job(conn, name, inline=True)


def claim_combined(conn: sqlite3.Connection, name: str):
    """Claim work for the combined worker: an orphaned seal, else a new fetch.

    A combined worker that dies between its fetch and seal stages leaves an
    inline fetch job at `fetched` with a queued seal job. Picking that seal up
    first lets any later combined worker finish the capture from staging instead
    of leaving it stuck.
    """
    seal = claim_seal_job(conn, name, inline_only=True)
    if seal:
        return ("seal", seal)
    row = claim_fetch_job(conn, name, inline=True)
    return ("fetch", row) if row else None


def run_capture(cfg: Config, conn, fetch_row, *, worker: str) -> dict:
    """Fetch then seal one job in this process. Used by `app.capture_worker`."""
    fetched = run_fetch(cfg, conn, fetch_row, worker=worker)
    if fetched.get("status") != "fetched":
        return fetched
    seal_row = claim_seal_job(conn, worker, fetch_job_id=fetch_row["id"])
    if not seal_row:
        # Another worker may already have sealed this capture (the seal queue is
        # shared), in which case the job is done, not failed.
        row = conn.execute("SELECT * FROM fetch_jobs WHERE id=?", (fetch_row["id"],)).fetchone()
        if row and row["status"] == "sealed":
            return {**fetched, "status": "done",
                    "object_id": conn.execute(
                        "SELECT id FROM objects WHERE capture_id=? ORDER BY created_at DESC "
                        "LIMIT 1", (fetch_row["capture_id"],)
                    ).fetchone()["id"]}
        return {**fetched, "status": "failed", "error": "could not claim seal job"}
    return run_seal(cfg, conn, seal_row, worker=worker)


def run_combined(cfg: Config, conn, item, *, worker: str) -> dict:
    kind, row = item
    if kind == "seal":
        return run_seal(cfg, conn, row, worker=worker)
    return run_capture(cfg, conn, row, worker=worker)


# --------------------------------------------------------------------------
# worker loops
# --------------------------------------------------------------------------

def _loop(cfg: Config, claim, execute, kind: str, once: bool, idle_exit: bool, poll: float):
    conn = connect(cfg.db_path)
    name = worker_name(kind)
    stop = {"flag": False}

    def handler(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)
    print(f"[{name}] {kind} worker ready (data={cfg.data_dir})", flush=True)
    idle_since = time.time()
    try:
        while not stop["flag"]:
            row = claim(conn, name)
            if row:
                idle_since = time.time()
                result = execute(cfg, conn, row, worker=name)
                label = row[1]["id"] if isinstance(row, tuple) else row["id"]
                print(f"[{name}] {label} -> {result.get('status')}", flush=True)
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


def run_capture_worker(cfg, *, once=False, idle_exit=False, poll=1.0):
    return _loop(cfg, claim_combined, run_combined, "capture", once, idle_exit, poll)


def run_fetch_worker(cfg, *, once=False, idle_exit=False, poll=1.0):
    return _loop(cfg, lambda conn, name: claim_fetch_job(conn, name, inline=False),
                 run_fetch, "fetch", once, idle_exit, poll)


def run_seal_worker(cfg, *, once=False, idle_exit=False, poll=1.0):
    return _loop(cfg, lambda conn, name: claim_seal_job(conn, name, split_only=True),
                 run_seal, "seal", once, idle_exit, poll)


def main(argv=None):
    parser = argparse.ArgumentParser(description="sealed vault capture pipeline worker")
    parser.add_argument("--stage", choices=["combined", "fetch", "seal"], default="combined",
                        help="combined = fetch+seal; fetch/seal run one stage only")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--idle-exit", action="store_true")
    parser.add_argument("--poll", type=float, default=1.0)
    args = parser.parse_args(argv)
    cfg = load_config()
    fn = {"combined": run_capture_worker, "fetch": run_fetch_worker,
          "seal": run_seal_worker}[args.stage]
    result = fn(cfg, once=args.once, idle_exit=args.idle_exit, poll=args.poll)
    if args.once:
        print(json.dumps(result, indent=2) if result else "no job")


if __name__ == "__main__":
    main()
