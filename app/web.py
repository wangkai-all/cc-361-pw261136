"""Web console: stdlib HTTP server exposing the vault API + a single-page UI.

The server never captures or parses bytes itself. It:
  * records intent (queue a capture, queue a parse run, request a stop),
  * manages worker processes (start/stop, including a real SIGKILL demo),
  * serves read-only views computed from the catalog + sealed bytes.

That separation is what makes "the parse service cannot silently read a newer
online source" true by construction: the parse worker's only input is the
manifest-listed chunks.
"""
from __future__ import annotations

import json
import mimetypes
import re
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from . import fixtures as fixtures_mod
from .capture_worker import cancel_capture, enqueue_capture
from .config import Config, load_config, origin_base
from .db import connect, log_event, row_to_dict, transaction
from .parse_engine import compare_runs, create_run, result_view
from .rules import sync_rules
from .supervisor import Supervisors
from .util import canonical_json_text, sha256_hex, utcnow
from .vault import (
    ObjectDamageError,
    check_object,
    chunk_share_summary,
    export_package,
    get_object,
    load_manifest,
    object_chunks_rows,
    read_chunk,
    read_object_bytes,
    restore_chunk,
    simulate_damage,
)

STATIC_DIR = Path(__file__).parent / "static"
MAX_JSON_BODY = 1 << 20

server_state = {
    "cfg": None,
    "supervisors": None,
    "started_at": utcnow(),
}


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def cfg() -> Config:
    return server_state["cfg"]


def supers() -> Supervisors:
    return server_state["supervisors"]


class ApiError(Exception):
    def __init__(self, status: int, message: str, extra: dict | None = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.extra = extra or {}


def _conn():
    return connect(cfg().db_path)


def _source_by_any(conn, key: str):
    row = conn.execute("SELECT * FROM sources WHERE id=? OR name=?", (key, key)).fetchone()
    if not row and len(key) >= 6:
        rows = conn.execute("SELECT * FROM sources WHERE id LIKE ?", (key + "%",)).fetchall()
        if len(rows) == 1:
            row = rows[0]
    return row


def _require_object(conn, key: str):
    row = get_object(conn, key)
    if not row:
        raise ApiError(404, f"no object matching {key!r}")
    return row


def _parse_json_body(handler) -> dict:
    length = int(handler.headers.get("Content-Length") or 0)
    if length <= 0:
        return {}
    if length > MAX_JSON_BODY:
        raise ApiError(413, "request body too large")
    raw = handler.rfile.read(length)
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ApiError(400, f"invalid JSON body: {exc}")
    if not isinstance(body, dict):
        raise ApiError(400, "JSON body must be an object")
    return body


def _ensure_seed(conn) -> dict:
    base = origin_base()
    created = fixtures_mod.seed_demo_sources(cfg(), conn, origin_base=base)
    rules_result = sync_rules(conn, cfg().rules_dir)
    return {"sources_created": created, "rules": rules_result}


def _object_public(row, conn=None) -> dict:
    d = row_to_dict(row)
    if conn is not None:
        d["sharing"] = chunk_share_summary(conn, row["id"])
    return d


# --------------------------------------------------------------------------
# API implementation
# --------------------------------------------------------------------------

def api_state() -> dict:
    conn = _conn()
    try:
        sources = []
        for s in conn.execute("SELECT * FROM sources ORDER BY created_at").fetchall():
            latest = conn.execute(
                "SELECT * FROM captures WHERE source_id=? ORDER BY started_at DESC LIMIT 1",
                (s["id"],),
            ).fetchone()
            objs = conn.execute(
                "SELECT COUNT(*) AS n FROM objects WHERE source_id=?", (s["id"],)
            ).fetchone()["n"]
            sources.append(
                {
                    "id": s["id"],
                    "name": s["name"],
                    "origin": s["origin"],
                    "url": s["url"],
                    "mode": s["mode"],
                    "status": s["status"],
                    "withdrawn_at": s["withdrawn_at"],
                    "withdrawn_reason": s["withdrawn_reason"],
                    "notes": s["notes"],
                    "objects": objs,
                    "latest_capture": row_to_dict(latest) if latest else None,
                }
            )
        recent_objects = conn.execute(
            "SELECT o.*, s.name AS source_name FROM objects o JOIN sources s ON s.id=o.source_id "
            "ORDER BY o.created_at DESC LIMIT 20"
        ).fetchall()
        runs = conn.execute("SELECT * FROM parse_runs ORDER BY created_at DESC LIMIT 25").fetchall()
        # `jobs` is the capture pipeline seen as one queue: fetch rows carry the
        # pipeline shape (inline=1 combined worker, inline=0 split fetch+seal).
        jobs = conn.execute(
            "SELECT f.*, (SELECT COUNT(*) FROM seal_jobs s WHERE s.fetch_job_id=f.id) AS seal_jobs "
            "FROM fetch_jobs f ORDER BY f.created_at DESC LIMIT 25"
        ).fetchall()
        counts = {
            "chunks": conn.execute("SELECT COUNT(*) n FROM chunks").fetchone()["n"],
            "chunk_bytes": conn.execute(
                "SELECT COALESCE(SUM(size),0) n FROM chunks").fetchone()["n"],
            "shared_chunks": conn.execute(
                "SELECT COUNT(*) n FROM chunks WHERE refcount>1").fetchone()["n"],
            "damaged_chunks": conn.execute(
                "SELECT COUNT(*) n FROM chunks WHERE missing=1").fetchone()["n"],
            "objects": conn.execute("SELECT COUNT(*) n FROM objects").fetchone()["n"],
            "events": conn.execute("SELECT COUNT(*) n FROM events").fetchone()["n"],
        }
        pipeline = {
            "fetch_queued": conn.execute(
                "SELECT COUNT(*) n FROM fetch_jobs WHERE status='queued'").fetchone()["n"],
            "fetch_running": conn.execute(
                "SELECT COUNT(*) n FROM fetch_jobs WHERE status='running'").fetchone()["n"],
            "fetched_waiting_seal": conn.execute(
                "SELECT COUNT(*) n FROM fetch_jobs WHERE status='fetched'").fetchone()["n"],
            "seal_queued": conn.execute(
                "SELECT COUNT(*) n FROM seal_jobs WHERE status='queued'").fetchone()["n"],
            "staged_files": sorted(
                p.name for p in cfg().staging_dir.glob("*.staged")
            ) if cfg().staging_dir.exists() else [],
        }
        rules = conn.execute(
            "SELECT * FROM rules ORDER BY name, version DESC").fetchall()
        return {
            "ok": True,
            "started_at": server_state["started_at"],
            "now": utcnow(),
            "sources": sources,
            "objects": [_object_public(o, conn) for o in recent_objects],
            "runs": [row_to_dict(r) for r in runs],
            "jobs": [row_to_dict(j) for j in jobs],
            "rules": [row_to_dict(r) for r in rules],
            "counts": counts,
            "pipeline": pipeline,
            "workers": supers().status(),
            "demo": {
                "tools": cfg().demo_tools,
                "data_dir": str(cfg().data_dir),
                "origin_base": origin_base(),
                "chunking": {"min": cfg().chunk_min, "avg": cfg().chunk_avg,
                             "max": cfg().chunk_max},
            },
        }
    finally:
        conn.close()


def api_source_bump(source_key: str) -> dict:
    conn = _conn()
    try:
        src = _source_by_any(conn, source_key)
        if not src:
            raise ApiError(404, "unknown source")
        if src["status"] == "withdrawn":
            raise ApiError(409, "source is withdrawn; reactivate it first")
    finally:
        conn.close()
    rev = fixtures_mod.bump_fixture(cfg(), src["name"])
    return {"source": src["name"], "revision": rev}


def api_source_withdraw(source_key: str, body: dict) -> dict:
    conn = _conn()
    try:
        src = _source_by_any(conn, source_key)
        if not src:
            raise ApiError(404, "unknown source")
        withdrawn = bool(body.get("withdrawn", True))
        reason = str(body.get("reason") or ("retracted by origin (demo)" if withdrawn else ""))
        with transaction(conn):
            conn.execute(
                "UPDATE sources SET status=?, withdrawn_at=?, withdrawn_reason=? WHERE id=?",
                ("withdrawn" if withdrawn else "active",
                 utcnow() if withdrawn else None,
                 reason if withdrawn else None,
                 src["id"]),
            )
            log_event(conn, "source.withdrawn" if withdrawn else "source.reactivated",
                      "source", src["id"], {"reason": reason}, actor="web")
        return {"source": src["name"], "status": "withdrawn" if withdrawn else "active"}
    finally:
        conn.close()


def api_capture(source_key: str, body: dict) -> dict:
    conn = _conn()
    try:
        src = _source_by_any(conn, source_key)
        if not src:
            raise ApiError(404, "unknown source")
        pipeline = str(body.get("pipeline") or "combined")
        if pipeline not in ("combined", "split"):
            raise ApiError(400, "pipeline must be 'combined' or 'split'")
        try:
            job_id, cap_id = enqueue_capture(conn, src, requested_by="web",
                                             inline=(pipeline == "combined"))
        except PermissionError as exc:
            raise ApiError(409, str(exc))
        auto = bool(body.get("auto_start_worker", True))
        workers = {}
        if auto:
            if pipeline == "split":
                workers["fetch"] = supers().start("fetch")
                workers["seal"] = supers().start("seal")
            else:
                workers["capture"] = supers().start("capture")
        return {
            "job_id": job_id,
            "capture_id": cap_id,
            "source": src["name"],
            "pipeline": pipeline,
            "worker": workers.get("capture", {"started": False}),
            "workers": workers,
            "mode": src["mode"],
            "url": src["url"],
        }
    finally:
        conn.close()


def api_capture_cancel(job_id: str) -> dict:
    conn = _conn()
    try:
        ok = cancel_capture(conn, job_id)
        if not ok:
            raise ApiError(409, "job is not cancelable")
        return {"job_id": job_id, "cancel_requested": True}
    finally:
        conn.close()


def api_object_detail(object_key: str) -> dict:
    conn = _conn()
    try:
        obj = _require_object(conn, object_key)
        om = load_manifest(cfg(), obj["manifest_digest"])
        rows = object_chunks_rows(conn, obj["id"])
        runs = conn.execute(
            "SELECT r.*, u.name AS rule_name FROM parse_runs r JOIN rules u ON u.id=r.rule_id "
            "WHERE r.object_id=? ORDER BY r.created_at DESC",
            (obj["id"],),
        ).fetchall()
        damaged = {r["digest"] for r in conn.execute(
            "SELECT digest FROM chunks WHERE missing=1").fetchall()}
        chunks = []
        for row in rows:
            chunks.append(
                {
                    "seq": row["seq"],
                    "digest": row["digest"],
                    "offset": row["offset"],
                    "length": row["length"],
                    "status": "damaged" if row["digest"] in damaged else "ok",
                    "refcount": conn.execute(
                        "SELECT refcount FROM chunks WHERE digest=?", (row["digest"],)
                    ).fetchone()["refcount"],
                }
            )
        return {
            "object": _object_public(obj, conn),
            "manifest": om.manifest,
            "chunks": chunks,
            "runs": [row_to_dict(r) for r in runs],
            "chunk_availability": {
                "locally_stored": len(rows),
                "distinct": len({c["digest"] for c in chunks}),
            },
        }
    finally:
        conn.close()


def api_object_verify(object_key: str) -> dict:
    conn = _conn()
    try:
        obj = _require_object(conn, object_key)
        report = check_object(cfg(), conn, obj)
        return report
    finally:
        conn.close()


def api_object_bytes(object_key: str) -> bytes:
    conn = _conn()
    try:
        obj = _require_object(conn, object_key)
        return read_object_bytes(cfg(), conn, obj)
    except ObjectDamageError as exc:
        raise ApiError(409, f"object cannot be reassembled: {exc}", exc.report)
    finally:
        conn.close()


def api_object_chunk(object_key: str, seq: int, *, raw: bool = True):
    conn = _conn()
    try:
        obj = _require_object(conn, object_key)
        row = conn.execute(
            "SELECT * FROM object_chunks WHERE object_id=? AND seq=?", (obj["id"], seq)
        ).fetchone()
        if not row:
            raise ApiError(404, "no such chunk")
        data = read_chunk(cfg(), row["digest"])
        if data is None:
            raise ApiError(410, f"chunk {row['digest']} is missing on disk")
        if sha256_hex(data) != row["digest"]:
            raise ApiError(410, f"chunk {row['digest']} is corrupt (digest mismatch)")
        return data, dict(row)
    finally:
        conn.close()


def api_object_chunk_force(object_key: str, seq: int, *, mode: str, force: bool) -> dict:
    conn = _conn()
    try:
        obj = _require_object(conn, object_key)
        if not cfg().demo_tools:
            raise ApiError(403, "demo tools disabled")
        if not force:
            raise ApiError(400, "pass force to confirm this is a demo action")
        try:
            res = simulate_damage(cfg(), conn, obj, seq, mode=mode)
        except ValueError as exc:
            raise ApiError(400, str(exc))
        return res
    finally:
        conn.close()


def api_chunk_restore(digest: str) -> dict:
    conn = _conn()
    try:
        try:
            return restore_chunk(cfg(), conn, digest)
        except FileNotFoundError as exc:
            raise ApiError(404, str(exc))
        except ValueError as exc:
            raise ApiError(409, str(exc))
    finally:
        conn.close()


def api_chunk_users(digest: str) -> dict:
    conn = _conn()
    try:
        row = conn.execute("SELECT * FROM chunks WHERE digest=?", (digest,)).fetchone()
        if not row:
            raise ApiError(404, "unknown chunk")
        objs = conn.execute(
            "SELECT o.id, o.source_id, s.name AS source_name FROM objects o "
            "JOIN sources s ON s.id=o.source_id "
            "WHERE o.id IN (SELECT object_id FROM object_chunks WHERE digest=?)",
            (digest,),
        ).fetchall()
        runs = conn.execute(
            "SELECT r.id, r.status, r.object_id FROM parse_runs r WHERE EXISTS ("
            "SELECT 1 FROM parse_records p JOIN json_each(p.dep_digests) j "
            "WHERE p.run_id=r.id AND j.value=?)", (digest,)
        ).fetchall()
        return {
            "chunk": row_to_dict(row),
            "objects": [row_to_dict(o) for o in objs],
            "runs": [row_to_dict(r) for r in runs],
        }
    finally:
        conn.close()


def api_object_export(object_key: str, body: dict) -> tuple[bytes, str]:
    conn = _conn()
    try:
        obj = _require_object(conn, object_key)
        run_ids = body.get("run_ids")
        if run_ids is not None and not isinstance(run_ids, list):
            raise ApiError(400, "run_ids must be a list")
        try:
            payload, summary = export_package(
                cfg(), conn, obj, run_ids=run_ids, partial=bool(body.get("partial", False))
            )
        except ObjectDamageError as exc:
            raise ApiError(409, str(exc), exc.report)
        name = f"sealed-{obj['id']}-{obj['manifest_digest'][:8]}.zip"
        conn.execute(
            "INSERT INTO events(at, kind, entity, entity_id, detail_json, actor) "
            "VALUES(?,?,?,?,?,?)",
            (utcnow(), "object.exported", "object", obj["id"],
             canonical_json_text({"runs": [r["id"] for r in summary["runs"]],
                                  "bytes": len(payload)}), "web"),
        )
        conn.commit()
        return payload, name
    finally:
        conn.close()


def api_parse_start(object_key: str, body: dict) -> dict:
    conn = _conn()
    try:
        obj = _require_object(conn, object_key)
        rule_id = body.get("rule_id")
        if not rule_id:
            raise ApiError(400, "rule_id is required")
        try:
            run_id = create_run(conn, obj, str(rule_id), requested_by="web")
        except Exception as exc:
            raise ApiError(400, str(exc))
        auto = bool(body.get("auto_start_worker", True))
        started = supers().start_parse() if auto else {"started": False}
        return {"run_id": run_id, "object_id": obj["id"], "rule_id": rule_id, "worker": started}
    finally:
        conn.close()


def api_parse_stop(run_id: str, body: dict) -> dict:
    conn = _conn()
    try:
        run = conn.execute("SELECT * FROM parse_runs WHERE id=?", (run_id,)).fetchone()
        if not run:
            raise ApiError(404, "no such run")
        hard = bool(body.get("kill_worker", False))
        with transaction(conn):
            conn.execute("UPDATE parse_runs SET stop_requested=1 WHERE id=?", (run_id,))
            log_event(conn, "run.stop_requested", "parse_run", run_id,
                      {"kill_worker": hard}, actor="web")
        result = {"run_id": run_id, "stop_requested": True, "kill_worker": hard}
        if hard:
            # A real process kill: the run resumes from its last checkpoint.
            result["worker"] = supers().stop("parse", hard=True)
        return result
    finally:
        conn.close()


def api_parse_resume(run_id: str, body: dict) -> dict:
    conn = _conn()
    try:
        run = conn.execute("SELECT * FROM parse_runs WHERE id=?", (run_id,)).fetchone()
        if not run:
            raise ApiError(404, "no such run")
        if run["status"] == "done":
            raise ApiError(409, "run already finished")
        with transaction(conn):
            conn.execute(
                "UPDATE parse_runs SET status='paused', stop_requested=0, error=NULL WHERE id=?",
                (run_id,),
            )
            log_event(conn, "run.resume_requested", "parse_run", run_id, {}, actor="web")
        started = supers().start_parse()
        return {"run_id": run_id, "queued": True, "worker": started}
    finally:
        conn.close()


def api_parse_cancel(run_id: str) -> dict:
    conn = _conn()
    try:
        run = conn.execute("SELECT * FROM parse_runs WHERE id=?", (run_id,)).fetchone()
        if not run:
            raise ApiError(404, "no such run")
        with transaction(conn):
            conn.execute(
                "UPDATE parse_runs SET status='canceled', stop_requested=1, finished_at=? WHERE id=?",
                (utcnow(), run_id),
            )
            log_event(conn, "run.canceled", "parse_run", run_id, {}, actor="web")
        return {"run_id": run_id, "status": "canceled"}
    finally:
        conn.close()


def api_run_view(run_id: str, query: dict) -> dict:
    conn = _conn()
    try:
        view = result_view(conn, run_id)
        if "error" in view:
            raise ApiError(404, view["error"])
        return view
    finally:
        conn.close()


def api_run_compare(query: dict) -> dict:
    a = (query.get("a") or [""])[0]
    b = (query.get("b") or [""])[0]
    if not a or not b:
        raise ApiError(400, "query params a and b (run ids) are required")
    conn = _conn()
    try:
        out = compare_runs(conn, a, b)
        if "error" in out:
            raise ApiError(404, out["error"])
        return out
    finally:
        conn.close()


def api_rules() -> dict:
    conn = _conn()
    try:
        res = sync_rules(conn, cfg().rules_dir)
        rows = conn.execute("SELECT * FROM rules ORDER BY name, version DESC").fetchall()
        return {"sync": res, "rules": [row_to_dict(r) for r in rows]}
    finally:
        conn.close()


def api_rule_activate(rule_id: str) -> dict:
    conn = _conn()
    try:
        rule = conn.execute("SELECT * FROM rules WHERE id=?", (rule_id,)).fetchone()
        if not rule:
            raise ApiError(404, "unknown rule")
        with transaction(conn):
            conn.execute("UPDATE rules SET is_active=0 WHERE name=?", (rule["name"],))
            conn.execute("UPDATE rules SET is_active=1 WHERE id=?", (rule_id,))
            log_event(conn, "rule.activated", "rule", rule_id, {"name": rule["name"]},
                      actor="web")
        return {"active": rule_id}
    finally:
        conn.close()


def api_rules_switch(body: dict) -> dict:
    """Switch the default rule for a source family and report old vs new runs."""
    family = body.get("family")
    rule_id = body.get("rule_id")
    if not family or not rule_id:
        raise ApiError(400, "family and rule_id are required")
    conn = _conn()
    try:
        rule = conn.execute(
            "SELECT * FROM rules WHERE id=? AND name=?", (rule_id, family)
        ).fetchone()
        if not rule:
            raise ApiError(404, "rule not found for that family")
        with transaction(conn):
            conn.execute("UPDATE rules SET is_active=0 WHERE name=?", (family,))
            conn.execute("UPDATE rules SET is_active=1 WHERE id=?", (rule_id,))
            log_event(conn, "rule.switched", "rule", rule_id, {"family": family}, actor="web")
        return {"family": family, "active": rule_id}
    finally:
        conn.close()


def api_workers(body: dict) -> dict:
    action = body.get("action")
    which = body.get("worker", "parse")
    if which not in ("capture", "fetch", "seal", "parse"):
        raise ApiError(400, "worker must be 'capture', 'fetch', 'seal' or 'parse'")
    if action == "start":
        return supers().start(which)
    if action == "stop":
        return supers().stop(which, hard=bool(body.get("hard", False)))
    if action == "status":
        return supers().status()
    raise ApiError(400, "action must be start/stop/status")


def api_events(query: dict) -> dict:
    limit = int((query.get("limit") or ["80"])[0])
    limit = max(1, min(limit, 500))
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            d = row_to_dict(r)
            try:
                d["detail"] = json.loads(d.pop("detail_json"))
            except json.JSONDecodeError:
                d["detail"] = {}
            out.append(d)
        return {"events": out}
    finally:
        conn.close()


def api_health() -> dict:
    conn = _conn()
    try:
        conn.execute("SELECT 1").fetchone()
        return {
            "ok": True,
            "workers": supers().status(),
            "data_dir": str(cfg().data_dir),
            "origin": origin_base(),
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------
# HTTP plumbing
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "SealedWire/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        if self.path.startswith("/static/"):
            return
        sys.stderr.write("%s %s\n" % (self.command, self.path))

    def _send(self, status: int, body: bytes, ctype="application/octet-stream",
              headers: dict | None = None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, status: int, obj):
        self._send(status, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8")

    def _error(self, status: int, message: str, extra: dict | None = None):
        payload = {"ok": False, "error": message, "status": status}
        if extra:
            payload["detail"] = extra
        self._json(status, payload)

    # ---- routing ----

    def do_GET(self):
        self._dispatch("GET")

    def do_HEAD(self):
        self._dispatch("HEAD")

    def do_POST(self):
        self._dispatch("POST")

    def _dispatch(self, method: str):
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        query = parse_qs(parsed.query)
        try:
            if path.startswith("/api/") or path == "/healthz":
                self._api(method, path, query)
            elif path == "/" or path.startswith("/static/"):
                self._static(path)
            elif path.startswith("/pkg/"):
                self._download(path, query)
            else:
                self._error(404, "not found")
        except ApiError as exc:
            self._error(exc.status, exc.message, exc.extra)
        except BrokenPipeError:
            pass
        except Exception as exc:  # keep the console usable, log the traceback
            traceback.print_exc()
            self._error(500, f"{type(exc).__name__}: {exc}")

    def _api(self, method: str, path: str, query: dict):
        body = _parse_json_body(self) if method == "POST" else {}
        if method == "GET" and path == "/healthz":
            return self._json(200, api_health())
        if method == "GET" and path == "/api/state":
            return self._json(200, api_state())
        if method == "GET" and path == "/api/events":
            return self._json(200, api_events(query))
        if method == "GET" and path == "/api/rules":
            return self._json(200, api_rules())
        if method == "GET" and path == "/api/compare":
            return self._json(200, api_run_compare(query))
        if method == "GET" and path.startswith("/api/chunks/"):
            digest = path.rsplit("/", 1)[-1]
            return self._json(200, api_chunk_users(digest))
        if method == "POST" and path.startswith("/api/chunks/"):
            digest = path.rsplit("/", 1)[-1]
            return self._json(200, api_chunk_restore(digest))
        m = re.fullmatch(r"/api/sources/([^/]+)/(capture|withdraw|bump|reactivate)", path)
        if m:
            key, action = m.groups()
            if method == "POST":
                if action == "capture":
                    return self._json(200, api_capture(key, body))
                if action == "withdraw":
                    return self._json(200, api_source_withdraw(key, {**body, "withdrawn": True}))
                if action == "reactivate":
                    return self._json(200, api_source_withdraw(key, {**body, "withdrawn": False}))
                if action == "bump":
                    return self._json(200, api_source_bump(key))
        m = re.fullmatch(r"/api/jobs/([^/]+)/cancel", path)
        if m and method == "POST":
            return self._json(200, api_capture_cancel(m.group(1)))
        m = re.fullmatch(r"/api/objects/([^/]+)/chunks/(\d+)", path)
        if m and method == "GET":
            key, seq = m.group(1), int(m.group(2))
            data, row = api_object_chunk(key, seq)
            return self._send(200, data, "application/octet-stream",
                              {"X-Chunk-Digest": row["digest"],
                               "X-Chunk-Seq": str(row["seq"]),
                               "X-Chunk-Offset": str(row["offset"])})
        m = re.fullmatch(r"/api/objects/([^/]+)(?:/(verify|bytes|export|damage))?", path)
        if m:
            key, sub = m.groups()
            if method == "GET":
                if sub is None:
                    return self._json(200, api_object_detail(key))
                if sub == "verify":
                    return self._json(200, api_object_verify(key))
                if sub == "bytes":
                    data = api_object_bytes(key)
                    return self._send(200, data, "application/octet-stream",
                                      {"Content-Disposition":
                                       f'attachment; filename="{key}.orig"',
                                       "X-Sealed-Object": key})
                if sub == "export":
                    return self._error(405, "POST to export")
            else:
                if sub == "export":
                    payload, name = api_object_export(key, body)
                    return self._send(200, payload, "application/zip",
                                      {"Content-Disposition": f'attachment; filename="{name}"'})
                if sub == "damage":
                    if not str(body.get("seq", "")).isdigit():
                        raise ApiError(400, "seq must be an integer")
                    return self._json(200, api_object_chunk_force(
                        key, int(body["seq"]),
                        mode=str(body.get("mode", "corrupt")),
                        force=bool(body.get("force"))))
        m = re.fullmatch(r"/api/runs/([^/]+)/(stop|resume|cancel|view)", path)
        if m:
            run_id, action = m.groups()
            if action == "view" and method == "GET":
                return self._json(200, api_run_view(run_id, query))
            if method == "POST":
                if action == "stop":
                    return self._json(200, api_parse_stop(run_id, body))
                if action == "resume":
                    return self._json(200, api_parse_resume(run_id, body))
                if action == "cancel":
                    return self._json(200, api_parse_cancel(run_id))
        m = re.fullmatch(r"/api/rules/([^/]+)/activate", path)
        if m and method == "POST":
            return self._json(200, api_rule_activate(m.group(1)))
        if path == "/api/rules/switch" and method == "POST":
            return self._json(200, api_rules_switch(body))
        if path == "/api/objects/parse" and method == "POST":
            if not body.get("object"):
                raise ApiError(400, "object is required")
            return self._json(200, api_parse_start(body["object"], body))
        if path == "/api/workers" and method == "POST":
            return self._json(200, api_workers(body))
        if path == "/api/demo/seed" and method == "POST":
            conn = _conn()
            try:
                res = _ensure_seed(conn)
            finally:
                conn.close()
            return self._json(200, res)
        raise ApiError(404, f"no API route for {path}")

    def _static(self, path: str):
        if path == "/":
            path = "/static/index.html"
        rel = path[len("/static/"):]
        target = (STATIC_DIR / rel).resolve()
        if STATIC_DIR.resolve() not in target.parents and target != STATIC_DIR.resolve():
            return self._error(403, "invalid path")
        if not target.exists() or not target.is_file():
            return self._error(404, "not found")
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data = target.read_bytes()
        self._send(200, data, ctype)

    def _download(self, path: str, query: dict):
        """Serve a previously built export from memory-less re-export."""
        m = re.fullmatch(r"/pkg/([^/]+)", path)
        if not m:
            return self._error(404, "not found")
        payload, name = api_object_export(m.group(1), {})
        self._send(200, payload, "application/zip",
                   {"Content-Disposition": f'attachment; filename="{name}"'})


def bootstrap() -> Config:
    config = load_config()
    from .db import init_db

    init_db(config.db_path)
    server_state["cfg"] = config
    server_state["supervisors"] = Supervisors()
    conn = connect(config.db_path)
    try:
        res = _ensure_seed(conn)
        if res["sources_created"]:
            print(f"seeded {len(res['sources_created'])} fictional sources")
        if res["rules"]["errors"]:
            for err in res["rules"]["errors"]:
                print(f"rule load error: {err}", file=sys.stderr)
    finally:
        conn.close()
    return config


def serve(host: str = "127.0.0.1", port: int = 8787):
    bootstrap()
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.daemon_threads = True
    url = f"http://{host}:{port}/"
    print(f"sealed vault console on {url}", flush=True)
    print(f"data dir: {cfg().data_dir}", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        supers().shutdown()
        httpd.server_close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="sealed vault web console")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()
    serve(args.host, args.port)
