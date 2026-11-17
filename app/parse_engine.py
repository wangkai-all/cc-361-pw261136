"""Parse engine: turn sealed bytes into records with exact provenance.

Rules of the game
-----------------
* Input is assembled **only** from the sealed manifest's chunks. Nothing here
  can reach a network source, so a recomputation after the source changed (or
  was withdrawn) still reads exactly the captured packet.
* A region is a maximal run of intact chunks. Damaged chunks are not guessed
  at: they become gaps, and records whose byte span overlaps them are marked
  blocked at read time (see `result_view`), never silently dropped.
* Progress is checkpointed by absolute byte offset after every batch, so a
  worker killed with SIGTERM/SIGKILL resumes from the last committed cursor.
* Every record stores the chunk digests its span touches, which is what lets a
  later damaged chunk block exactly the dependent records.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .config import Config
from .db import log_event, transaction
from .rules import RuleContext, RuleError, load_rule_by_id
from .util import canonical_json_text, digest_of, new_id, utcnow
from .vault import (
    CHUNK_OK,
    ObjectDamageError,
    iter_object_chunks,
    load_manifest,
)

BATCH_RECORDS = 200
REGION_CHUNK_GROUP = 8  # chunks grouped per checkpoint boundary


# --------------------------------------------------------------------------
# stop coordination
# --------------------------------------------------------------------------

class StopSignal:
    """Cooperative stop flag set by SIGTERM/SIGINT or a DB cancel request."""

    def __init__(self):
        self.requested = False
        self.reason = ""

    def install(self):
        def handler(signum, frame):
            self.requested = True
            self.reason = signal.Signals(signum).name

        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)


# --------------------------------------------------------------------------
# regions
# --------------------------------------------------------------------------

@dataclass
class Region:
    index: int
    chunk_start_seq: int
    chunk_end_seq: int
    byte_start: int
    byte_end: int
    chunks: list  # list[ChunkView] with data


@dataclass
class Gap:
    start: int
    end: int
    reason: str


def build_regions(cfg: Config, conn, object_row) -> tuple[list[Region], list[Gap]]:
    regions: list[Region] = []
    gaps: list[Gap] = []
    current: list = []
    current_start = None
    gap_start = gap_end = None

    def close_region():
        nonlocal current, current_start
        if current:
            regions.append(
                Region(
                    index=len(regions),
                    chunk_start_seq=current[0].seq,
                    chunk_end_seq=current[-1].seq,
                    byte_start=current[0].offset,
                    byte_end=current[-1].offset + current[-1].length,
                    chunks=list(current),
                )
            )
        current = []
        current_start = None

    for cv in iter_object_chunks(cfg, conn, object_row):
        if cv.status == CHUNK_OK:
            if current_start is None:
                current_start = cv.offset
                if gap_start is not None:
                    gaps.append(
                        Gap(gap_start, gap_end, "damaged chunk(s) skipped; bytes unknown")
                    )
                    gap_start = gap_end = None
            current.append(cv)
        else:
            close_region()
            if gap_start is None:
                gap_start = cv.offset
            gap_end = cv.offset + cv.length
    close_region()
    if gap_start is not None:
        gaps.append(Gap(gap_start, gap_end, "damaged chunk(s) skipped; bytes unknown"))
    return regions, gaps


def region_text(region: Region) -> str:
    data = b"".join(c.data for c in region.chunks)
    # surrogateescape makes text -> bytes round-trip exact even for invalid
    # UTF-8, which keeps byte spans truthful for any captured payload.
    return data.decode("utf-8", errors="surrogateescape")


def char_to_byte(text: str, index: int) -> int:
    """Exact character offset -> UTF-8 byte offset for the region text."""
    index = max(0, min(index, len(text)))
    return len(text[:index].encode("utf-8", "surrogateescape"))


# --------------------------------------------------------------------------
# run creation / worker loop
# --------------------------------------------------------------------------

def create_run(conn, object_row, rule_id: str, *, job_id: str | None = None,
               requested_by: str = "web") -> str:
    rule = conn.execute("SELECT id FROM rules WHERE id=?", (rule_id,)).fetchone()
    if not rule:
        raise RuleError(f"unknown rule {rule_id}")
    run_id = new_id("run")
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM object_chunks WHERE object_id=?", (object_row["id"],)
    ).fetchone()["n"]
    with transaction(conn):
        conn.execute(
            """INSERT INTO parse_runs(id, object_id, rule_id, status, created_at,
                    cursor, total_chunks, processed_chunks, result_count, job_id)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (run_id, object_row["id"], rule_id, "queued", utcnow(), 0, total, 0, 0, job_id),
        )
        log_event(conn, "run.queued", "parse_run", run_id,
                  {"object_id": object_row["id"], "rule_id": rule_id, "requested_by": requested_by})
    return run_id


def _result_path(cfg: Config, run_id: str) -> Path:
    return cfg.data_dir / "results" / f"{run_id}.json"


def _absolute_span(record: dict, region: Region, text: str) -> tuple[int, int] | None:
    """Convert a rule's character span into an absolute *byte* span."""
    span = record.get("_span")
    if span is None:
        return None
    try:
        a, b = int(span[0]), int(span[1])
    except Exception:
        return None
    if b < a:
        a, b = b, a
    start = region.byte_start + char_to_byte(text, a)
    end = region.byte_start + char_to_byte(text, b)
    return start, end


def _chunks_for_span(chunks: list, start: int, end: int) -> list[dict]:
    out = []
    for c in chunks:
        c_end = c.offset + c.length
        if c.offset < end and c_end > start:
            out.append({"digest": c.digest, "seq": c.seq})
    if not out:
        # Zero-width or boundary span: attach to the chunk containing the start.
        for c in chunks:
            if c.offset <= start < c.offset + max(1, c.length):
                out.append({"digest": c.digest, "seq": c.seq})
                break
    return out


def stop_requested_in_db(conn, run_id: str) -> bool:
    row = conn.execute(
        "SELECT stop_requested, status FROM parse_runs WHERE id=?", (run_id,)
    ).fetchone()
    return bool(row and (row["stop_requested"] or row["status"] == "canceled"))


def run_parse(
    cfg: Config,
    conn: sqlite3.Connection,
    run_id: str,
    stop: StopSignal | None = None,
    *,
    progress: Callable[[dict], None] | None = None,
    worker: str = "parse-worker",
) -> dict:
    """Execute or resume one parse run. Safe to call again after a stop."""
    stop = stop or StopSignal()
    run = conn.execute("SELECT * FROM parse_runs WHERE id=?", (run_id,)).fetchone()
    if not run:
        raise ValueError(f"no such run {run_id}")
    if run["status"] == "done":
        return {"run_id": run_id, "status": "done", "resumed": False, "records": run["result_count"]}
    if run["status"] == "canceled":
        return {"run_id": run_id, "status": "canceled", "resumed": False, "records": 0}

    object_row = conn.execute("SELECT * FROM objects WHERE id=?", (run["object_id"],)).fetchone()
    if not object_row:
        _fail(conn, run_id, "object no longer exists")
        return {"run_id": run_id, "status": "failed"}

    rule = load_rule_by_id(conn, run["rule_id"])
    om = load_manifest(cfg, object_row["manifest_digest"])
    regions, gaps = build_regions(cfg, conn, object_row)

    resumed_from = run["cursor"]
    with transaction(conn):
        conn.execute(
            "UPDATE parse_runs SET status='running', started_at=COALESCE(started_at,?), "
            "worker=?, error=NULL, stop_requested=0 WHERE id=?",
            (utcnow(), worker, run_id),
        )
        conn.execute("DELETE FROM parse_gaps WHERE run_id=?", (run_id,))
        for g in gaps:
            conn.execute(
                "INSERT OR REPLACE INTO parse_gaps(run_id, start, end, reason) VALUES(?,?,?,?)",
                (run_id, g.start, g.end, g.reason),
            )
        log_event(conn, "run.started", "parse_run", run_id,
                  {"rule_id": rule.id, "resumed_from": resumed_from, "regions": len(regions),
                   "gaps": len(gaps)}, actor=worker)

    written = 0
    ordinal_row = conn.execute(
        "SELECT COALESCE(MAX(ordinal), -1) AS m FROM parse_records WHERE run_id=?", (run_id,)
    ).fetchone()
    ordinal = ordinal_row["m"] + 1
    cursor = resumed_from
    # A region may contain more records than one batch. Checkpoint within a
    # region at the byte offset just past the last persisted record plus the
    # record index, so a resume replays the rule from the region start and skips
    # exactly the records already committed (see `_resume_skip`).
    try:
        resume_state = json.loads(run["resume_state"] or "{}")
    except json.JSONDecodeError:
        resume_state = {}
    processed_chunks = run["processed_chunks"]
    stop_reason = ""

    try:
        for region in regions:
            # Skip regions entirely before the cursor (already committed).
            if region.byte_end <= cursor:
                continue
            text = region_text(region)
            ctx = RuleContext(
                object_id=object_row["id"],
                stream_sha256=om.stream_digest,
                text_length=len(text),
                chunk_offset=region.byte_start,
                chunk_length=region.byte_end - region.byte_start,
                chunk_digest=region.chunks[0].digest,
                extras={"region_index": region.index, "region_offset": region.byte_start},
            )
            produced = list(rule.parse(text, ctx))
            skip_remaining = 0
            if resume_state.get("region_index") == region.index:
                skip_remaining = int(resume_state.get("skip_records", 0))
            persisted_in_region = max(0, int(resume_state.get("skip_records", 0))) \
                if resume_state.get("region_index") == region.index else 0
            batch = []
            for rec in produced:
                if skip_remaining > 0:
                    skip_remaining -= 1
                    continue
                if stop.requested or stop_requested_in_db(conn, run_id):
                    stop_reason = stop.reason or "stop requested via UI"
                    break
                if not isinstance(rec, dict):
                    raise RuleError(f"rule {rule.id} yielded {type(rec).__name__}, expected dict")
                rec = dict(rec)
                span = _absolute_span(rec, region, text)
                if span is None:
                    # No span: conservatively depend on the whole region.
                    span = (region.byte_start, region.byte_end)
                    span_exact = False
                else:
                    span_exact = True
                start, end = span
                if span_exact:
                    deps = _chunks_for_span(region.chunks, start, end)
                else:
                    deps = [{"digest": c.digest, "seq": c.seq} for c in region.chunks]
                payload = {k: v for k, v in rec.items() if not k.startswith("_")}
                batch.append(
                    {
                        "ordinal": ordinal,
                        "span_start": start,
                        "span_end": end,
                        "deps": [d["digest"] for d in deps],
                        "chunk_digest": deps[0]["digest"] if deps else "",
                        "chunk_seq": deps[0]["seq"] if deps else -1,
                        "value": canonical_json_text(payload),
                    }
                )
                ordinal += 1
                persisted_in_region += 1
                if len(batch) >= BATCH_RECORDS:
                    next_from = persisted_in_region
                    written += _commit_batch(
                        conn, run_id, batch, cursor, region.chunk_end_seq,
                        {"region_index": region.index, "skip_records": next_from},
                    )
                    resume_state = {"region_index": region.index, "skip_records": next_from}
                    batch = []
                    if progress:
                        progress({"run_id": run_id, "cursor": cursor, "records": written})
            if stop.requested or stop_requested_in_db(conn, run_id):
                if batch:
                    written += _commit_batch(conn, run_id, batch, cursor,
                                             region.chunk_end_seq, resume_state)
                stop.requested = True
                stop_reason = stop_reason or "stop requested via UI"
                break
            if batch:
                written += _commit_batch(conn, run_id, batch, cursor,
                                         region.chunk_end_seq, resume_state)
            # Region finished: advance the cursor past all of its bytes and clear
            # the in-region resume state.
            cursor = max(cursor, region.byte_end)
            processed_chunks = max(processed_chunks, region.chunk_end_seq + 1)
            resume_state = {}
            with transaction(conn):
                conn.execute(
                    "UPDATE parse_runs SET cursor=?, processed_chunks=?, resume_state='{}' "
                    "WHERE id=?",
                    (cursor, processed_chunks, run_id),
                )
            if progress:
                progress({"run_id": run_id, "cursor": cursor, "records": written,
                          "region": region.index})
    except (RuleError, ObjectDamageError) as exc:
        _fail(conn, run_id, str(exc))
        return {"run_id": run_id, "status": "failed", "error": str(exc), "records": written}
    except Exception as exc:  # unexpected: mark failed, keep committed records
        _fail(conn, run_id, f"{type(exc).__name__}: {exc}")
        return {"run_id": run_id, "status": "failed", "error": str(exc), "records": written}

    if stop.requested:
        with transaction(conn):
            conn.execute(
                "UPDATE parse_runs SET status='paused', stopped_by=?, cursor=?, error=?, "
                "stop_requested=1 WHERE id=?",
                (stop_reason, cursor, "stopped: " + stop_reason, run_id),
            )
            log_event(conn, "run.paused", "parse_run", run_id,
                      {"cursor": cursor, "records": written, "reason": stop_reason},
                      actor=worker)
        return {"run_id": run_id, "status": "paused", "records": written, "cursor": cursor}

    return _finalize(cfg, conn, run_id, rule, om, object_row, gaps, written, worker)


def _commit_batch(conn, run_id: str, batch: list[dict], cursor: int,
                  chunk_seq_done: int, resume_state: dict) -> int:
    with transaction(conn):
        for rec in batch:
            conn.execute(
                """INSERT OR REPLACE INTO parse_records(run_id, ordinal, chunk_digest,
                        chunk_seq, value_json, span_start, span_end, dep_digests)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (
                    run_id,
                    rec["ordinal"],
                    rec["chunk_digest"],
                    rec["chunk_seq"],
                    rec["value"],
                    rec["span_start"],
                    rec["span_end"],
                    json.dumps(sorted(set(rec["deps"]))),
                ),
            )
        conn.execute(
            "UPDATE parse_runs SET cursor=MAX(cursor, ?), processed_chunks=MAX(processed_chunks, ?), "
            "resume_state=?, "
            "result_count=(SELECT COUNT(*) FROM parse_records WHERE run_id=?) WHERE id=?",
            (cursor, chunk_seq_done + 1, json.dumps(resume_state), run_id, run_id),
        )
    return len(batch)


RESULT_FORMAT = "sealed-wire-result/1"


def canonical_result_digest(result: dict) -> str:
    """Digest of the reproducible part of a result document.

    Deliberately excludes run id, creation time and anything else that varies
    when the same rule is re-run over the same sealed object. Everything that
    identifies *what was published* is included: object, manifest, stream, rule
    version, record values, byte spans, chunk dependencies and gaps.
    """
    canonical = {
        "format": result.get("format", RESULT_FORMAT),
        "object_id": result["object_id"],
        "object_manifest_digest": result["object_manifest_digest"],
        "object_stream_sha256": result["object_stream_sha256"],
        "rule_id": result["rule_id"],
        "rule_source_sha256": result["rule_source_sha256"],
        "records": result["records"],
        "gaps": result.get("gaps", []),
        "counts": result.get("counts"),
    }
    return digest_of(canonical)


def _finalize(cfg, conn, run_id, rule, om, object_row, gaps, written, worker) -> dict:
    with transaction(conn):
        rows = conn.execute(
            "SELECT ordinal, chunk_digest, chunk_seq, value_json, span_start, span_end, dep_digests "
            "FROM parse_records WHERE run_id=? ORDER BY ordinal",
            (run_id,),
        ).fetchall()
        records = [
            {
                "ordinal": r["ordinal"],
                "chunk_digest": r["chunk_digest"],
                "chunk_seq": r["chunk_seq"],
                "value": json.loads(r["value_json"]),
                "span": {"start": r["span_start"], "end": r["span_end"]},
                "deps": json.loads(r["dep_digests"]),
            }
            for r in rows
        ]
        result = {
            "format": RESULT_FORMAT,
            "run_id": run_id,
            "object_id": object_row["id"],
            "object_manifest_digest": object_row["manifest_digest"],
            "object_stream_sha256": om.stream_digest,
            "rule_id": rule.id,
            "rule_source_sha256": rule.source_sha256,
            "created_at": utcnow(),
            "records": records,
            "gaps": [{"start": g.start, "end": g.end, "reason": g.reason} for g in gaps],
            "counts": {"records": len(records), "gaps": len(gaps)},
        }
        # The published digest covers only reproducible content (see
        # canonical_result_digest): re-running the same rule over the same
        # sealed object yields the same digest.
        result_digest = canonical_result_digest(result)
        result["result_digest"] = result_digest
        payload = json.dumps(result, sort_keys=True, indent=1, ensure_ascii=False).encode("utf-8")
        rp = _result_path(cfg, run_id)
        rp.parent.mkdir(parents=True, exist_ok=True)
        from .util import atomic_write

        atomic_write(rp, payload)
        conn.execute(
            "UPDATE parse_runs SET status='done', finished_at=?, error=NULL, "
            "result_digest=?, result_path=?, result_count=?, total_chunks=? WHERE id=?",
            (
                utcnow(),
                result_digest,
                str(rp),
                len(records),
                object_row["chunk_count"],
                run_id,
            ),
        )
        log_event(conn, "run.done", "parse_run", run_id,
                  {"rule_id": rule.id, "records": len(records), "result_digest": result_digest,
                   "gaps": len(gaps)}, actor=worker)
    return {
        "run_id": run_id,
        "status": "done",
        "records": len(records),
        "result_digest": result_digest,
        "gaps": len(gaps),
        "cursor": om.total_bytes,
    }


def _fail(conn, run_id: str, error: str):
    with transaction(conn):
        conn.execute(
            "UPDATE parse_runs SET status='failed', finished_at=?, error=? WHERE id=?",
            (utcnow(), error, run_id),
        )
        log_event(conn, "run.failed", "parse_run", run_id, {"error": error}, actor="parse-worker")


# --------------------------------------------------------------------------
# result serving with dynamic damage gating
# --------------------------------------------------------------------------

def blocked_digests(conn) -> set[str]:
    return {
        r["digest"]
        for r in conn.execute("SELECT digest FROM chunks WHERE missing=1").fetchall()
    }


def result_view(conn, run_id: str, *, include_blocked: bool = True) -> dict:
    """Load a finished run and mark records blocked by currently-damaged chunks.

    Blocking is computed at read time from chunk health, so repairing a chunk
    instantly unblocks its records without re-running the parse.
    """
    run = conn.execute("SELECT * FROM parse_runs WHERE id=?", (run_id,)).fetchone()
    if not run:
        return {"error": "no such run"}
    damaged = blocked_digests(conn)
    records = conn.execute(
        "SELECT * FROM parse_records WHERE run_id=? ORDER BY ordinal", (run_id,)
    ).fetchall()
    gaps = conn.execute(
        "SELECT start, end, reason FROM parse_gaps WHERE run_id=? ORDER BY start", (run_id,)
    ).fetchall()
    out = []
    blocked = 0
    for r in records:
        deps = set(json.loads(r["dep_digests"]))
        hit = sorted(deps & damaged)
        is_blocked = bool(hit)
        if is_blocked:
            blocked += 1
        if is_blocked and not include_blocked:
            continue
        out.append(
            {
                "ordinal": r["ordinal"],
                "value": json.loads(r["value_json"]),
                "span": {"start": r["span_start"], "end": r["span_end"]},
                "chunk": {"seq": r["chunk_seq"], "digest": r["chunk_digest"]},
                "deps": sorted(deps),
                "blocked": is_blocked,
                "blocked_by": hit,
            }
        )
    return {
        "run": {
            "id": run["id"],
            "object_id": run["object_id"],
            "rule_id": run["rule_id"],
            "status": run["status"],
            "cursor": run["cursor"],
            "result_digest": run["result_digest"],
            "result_count": run["result_count"],
            "stopped_by": run["stopped_by"],
            "error": run["error"],
            "started_at": run["started_at"],
            "finished_at": run["finished_at"],
        },
        "records": out,
        "blocked_records": blocked,
        "total_records": len(records),
        "gaps": [dict(g) for g in gaps],
    }


def compare_runs(conn, run_a: str, run_b: str) -> dict:
    """Field-level comparison of two runs over the same object."""
    a = result_view(conn, run_a, include_blocked=True)
    b = result_view(conn, run_b, include_blocked=True)
    if "error" in a or "error" in b:
        return {"error": "one or both runs are missing"}
    key = lambda rec: json.dumps(rec["value"], sort_keys=True, ensure_ascii=False)
    # Align records by a caller-visible identity when present, else by position.
    def index(view):
        by_key = {}
        for rec in view["records"]:
            ident = rec["value"].get("id") if isinstance(rec["value"], dict) else None
            k = f"id:{ident}" if ident is not None else f"pos:{rec['ordinal']}"
            by_key.setdefault(k, rec)
        return by_key

    ia, ib = index(a), index(b)
    only_a = sorted(set(ia) - set(ib))
    only_b = sorted(set(ib) - set(ia))
    changed = []
    for k in sorted(set(ia) & set(ib)):
        va, vb = ia[k]["value"], ib[k]["value"]
        if isinstance(va, dict) and isinstance(vb, dict):
            fields = {}
            for f in sorted(set(va) | set(vb)):
                if va.get(f) != vb.get(f):
                    fields[f] = {"a": va.get(f), "b": vb.get(f)}
            if fields:
                changed.append(
                    {
                        "key": k,
                        "fields": fields,
                        "span_a": ia[k]["span"],
                        "span_b": ib[k]["span"],
                        "blocked_a": ia[k]["blocked"],
                        "blocked_b": ib[k]["blocked"],
                    }
                )
        elif va != vb:
            changed.append({"key": k, "fields": {"_": {"a": va, "b": vb}}})
    return {
        "run_a": {"id": run_a, "rule_id": a["run"]["rule_id"],
                  "digest": a["run"]["result_digest"], "count": a["total_records"]},
        "run_b": {"id": run_b, "rule_id": b["run"]["rule_id"],
                  "digest": b["run"]["result_digest"], "count": b["total_records"]},
        "same_object": a["run"]["object_id"] == b["run"]["object_id"],
        "only_in_a": only_a,
        "only_in_b": only_b,
        "changed": changed,
        "unchanged": len(set(ia) & set(ib)) - len(changed),
        "summary": {"only_in_a": len(only_a), "only_in_b": len(only_b), "changed": len(changed)},
    }
