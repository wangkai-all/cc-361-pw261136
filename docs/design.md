# Sealed Wire Vault — design notes

This document records the two storage strategies that were on the table, why
content-defined chunking was chosen, and how every guarantee in the brief maps
onto code. It is intentionally explicit about failure modes.

---

## 1. The two strategies

| | A. Whole-object retention | B. Content-defined chunk store (**chosen**) |
|---|---|---|
| Unit on disk | one blob per capture | one file per content-defined chunk, named by SHA-256 |
| Exact original | trivially: the file *is* the object | reassembled from the manifest’s ordered chunk list |
| Re-capture of a changed page | stores a second full copy | unchanged regions dedupe to existing chunks |
| Re-capture of an unchanged page | stores a second full copy | creates **zero** new chunk bytes, one new manifest |
| Source withdrawn | bytes remain; nothing to do | bytes remain; chunk refcounts are untouched |
| One byte corrupted | the whole object is unreadable, and the failure site is unknown | one chunk fails; only records whose spans overlap it are blocked |
| Damage forensics | compare whole blob to a known-good copy | chunk digest + offset + seq + Merkle proof name the exact site |
| Reference sharing between sources | only by copying | by digest; refcounts show who references what |
| Cost | storage grows with every capture | storage grows with novel content |

Whole-object retention is simpler, and it is tempting because “the original
packet” sounds like one file. It fails three requirements directly:

1. **Damage localisation.** A single flipped byte makes the entire object
   unusable, so *every* dependent result is blocked. The brief requires that
   damage block only the results that depend on the damaged block.
2. **Reference sharing / dedupe.** Publishing the same bytes from two feeds, or
   re-capturing an unchanged page, doubles storage; there is no shared
   reference to audit.
3. **Fine-grained provenance with partial recovery.** With whole blobs you can
   only say “the object is bad”. With chunks you can say “chunk #17 at offset
   34 816–36 102 of object `obj_…`, digest `…`, is bad; the 12 records that
   overlap those bytes are blocked and the other 3 988 are served”.

**Decision: B.** The “original packet” is preserved exactly through the
manifest: concatenating the manifest’s chunks in order reproduces the captured
stream byte for byte, and `stream.sha256` proves it. The standalone export
package re-verifies this offline with only the standard library.

### Why content-defined boundaries

Fixed-size blocks shift every boundary after an insertion, so a one-byte change
at the top of a page invalidates every block. A gear-hash rolling window picks
boundaries from local content, so unchanged regions keep identical chunk
digests across captures. `tests/test_chunking.py` checks that a prefix insertion
preserves a majority of chunks, and `tests/test_vault.py` checks that a
single-character edit keeps >70% of chunks shared.

### Storage layout

```
data/
  vault.db                     catalog: sources, captures, objects, chunks, runs, records, events
  chunks/ab/cd/<sha256>        immutable, read-only chunk files (shared)
  manifests/<sha256>.json      immutable manifests (shared by identical content)
  results/<run_id>.json        published result documents
  reports/objects/<id>.json    last verification report per object
  quarantine/<digest>.orig     demo-only copies of deliberately damaged chunks
  logs/                        worker and origin logs
  pids/                        dev tool PID files
  staging/<capture_id>.staged  fetched bytes awaiting an independent seal
```

Chunk files are written with temp-file + `rename` and then `chmod 444`.
Manifests are canonical JSON (sorted keys, no whitespace) so their SHA-256 is
stable and their contents are diffable.

### Capture runs as two stages

The brief requires the fetch side, the sealed store and the parse worker to run
separately. Capture is therefore split into two stages with an explicit,
hash-checked handoff:

| stage | process | may touch | writes |
|---|---|---|---|
| fetch | `app/fetch_worker.py` → `capture_worker.run_fetch` | source (HTTP or fixture) | `staging/<capture>.staged` + `{bytes, sha256}` in `fetch_jobs`/`captures`, then a queued `seal_jobs` row |
| seal | `app/seal_worker.py` → `capture_worker.run_seal` | staging file only | chunks, manifest, `objects` row |
| parse | `app/parse_worker.py` | manifest-listed chunks only | records, gaps, results |

`app/capture_worker.py` can run both stages back to back (the default demo,
`inline=1`). Jobs queued with `"pipeline": "split"` are marked `inline=0` and are
claimed only by the standalone fetch and seal workers, so a combined worker
never steals a split job (and vice versa). `tests/test_pipeline.py` runs the two
stages as two real subprocesses.

The staging handoff is not a trusted back channel: `vault.seal_staged`
re-hashes the staged bytes while streaming them into the chunker and raises
`StagingMismatch` if either the length or the SHA-256 differs from what the
fetch stage recorded. So the sealed object provably equals the fetched
response, and a fetch can never partially seal. Staging files are removed only
after a successful seal; a failed seal leaves them for inspection, and a cancel
before seal removes them because those bytes will never be published.

Sealing is idempotent across crashes: `run_seal` first looks for an object
already committed for the capture, and if one exists it finishes the job
bookkeeping instead of sealing a second copy. That covers the window between
`seal_stream`'s commit and the seal-job status update.

---

## 2. How the brief’s guarantees map to code

### “Recomputation cannot silently read an updated online source”

The parse worker (`app/parse_worker.py` → `app/parse_engine.py`) is a separate
process with no fetch code on its import path. Its only byte source is
`vault.iter_object_chunks`, which reads `object_chunks` rows and chunk files.
The manifest pins the fetch: URL, HTTP status, request/response headers and
timestamps. `tests/test_parse.py::test_parse_never_reads_online_source` changes
the on-disk fixture after a run and proves the re-run digest is unchanged.

### “Every published result points at the exact original, parse version, field position”

A run pins:

* `object_id` + `object_manifest_digest` + `object_stream_sha256`;
* `rule_id` (name@version) + `rule_source_sha256`;
* one record per parsed item with `span {start,end}` in **absolute byte**
  offsets of the object, the first chunk digest/seq, and the full list of chunk
  digests the span overlaps (`dep_digests`);
* `result_digest`, a canonical hash of that reproducible content only (run id
  and timestamps are metadata, so re-running yields the same digest).

The UI shows these; the export package embeds them; `verify_package.py`
recomputes them offline.

### “Sources can be withdrawn”

`app/web.py::api_source_withdraw` marks the source `withdrawn`, records time and
reason, and logs an event. `enqueue_capture` and `run_capture` both refuse a
withdrawn source. Nothing is deleted: objects, chunks, manifests, runs and
exports remain. The demo origin returns **410 Gone** for a withdrawn feed, so an
online re-capture fails honestly rather than silently fetching something else.

### “References are shared”

Chunks are deduped by digest across objects and sources. `chunks.refcount`
counts referencing objects; `object_chunks` is the reference edge. The object
view shows shared vs unique bytes; `/api/chunks/<digest>` lists every object and
run that depends on a chunk. Damaging a shared chunk correctly flags every
object that references it (`test_shared_chunk_damage_affects_both_objects`) —
the honest answer, since the bytes really are the same.

### “Damage can be located”

`vault.check_object` never repairs silently. It hashes every referenced chunk,
reports `{seq, digest, offset, length, status, actual_size}` for failures,
distinguishes `missing` from `corrupt` (same-size corruption is caught by
digest), and maps damaged digests to blocked records across runs. The manifest
also carries a Merkle root over chunk digests, and `vault.merkle_proof` produces
a sibling path, so a light client can verify one chunk without the whole object.

### “A damaged chunk blocks only results that depend on it”

Two mechanisms:

* **At parse time**, damaged chunks are not guessed at. `build_regions` splits
  the object into maximal runs of intact chunks; each run is parsed
  independently, and damaged regions become recorded `gaps`. Records whose
  spans overlap a gap are never invented.
* **At read time**, `parse_engine.result_view` re-checks chunk health and marks
  exactly the records whose `dep_digests` intersect currently-damaged chunks.
  Repairing a chunk unblocks its records immediately, with no re-run.

### “Web can seal, switch rules, compare results, and continue after a mid-run stop”

`app/web.py` exposes the actions; `app/static/*` is a zero-dependency console.

* **Seal**: `POST /api/sources/<id>/capture` queues a capture. With
  `{"pipeline":"combined"}` one worker fetches and seals; with
  `{"pipeline":"split"}` the fetch worker stages and a separate seal worker
  commits the vault.
* **Switch rules**: `POST /api/rules/switch` flips the active version for a
  family; existing runs keep their pinned version.
* **Compare**: `GET /api/compare?a=&b=` aligns records by `id` (falling back to
  position) and reports changed fields with both byte spans.
* **Stop/resume**: `POST /api/runs/<id>/stop` sets `stop_requested` (graceful)
  and optionally SIGKILLs the parse worker. The run checkpoints after each
  batch; resume re-queues it and a worker continues from `cursor` (plus
  `resume_state` for a mid-region stop).

`tools/dev.py` runs web + origin; the console can start/stop the combined
capture worker, the fetch and seal workers, and the parse worker, or they can be
started by hand. The web process never fetches or parses.

### “Stop mid-process and continue”

* Graceful (`stop_requested`): the current batch commits, `status=paused`,
  cursor persists. `tests/test_parse.py` proves the resumed digest equals an
  uninterrupted run.
* Hard (SIGKILL): committed batches are durable; the run lease expires;
  `claim_run` reclaims it and resumes. `tools/resume_test.py` kills a real
  worker process with SIGKILL mid-run and checks the resumed digest matches a
  fresh run, with no duplicate ordinals and no gaps.

### “Download an independent, re-checkable package”

`vault.export_package` builds a deterministic zip:

```
object.json          object id, manifest digest, stream digest, pinned runs
manifest.json        exact canonical manifest (hashes to manifest_digest)
chunks/<digest>.bin  only the distinct chunks, content-addressed
results/<run>.json   published result documents
verify_package.py    stdlib-only verifier
VERIFY.md            how to run it
```

`verify_package.py` checks manifest hash, every chunk’s length/digest/offset,
stream SHA-256, Merkle root, and for each run re-checks it pins this object, its
rule, and recomputes the result digest. A damaged object is refused unless
`partial=true`, in which case excluded chunks are listed in `damage.json` and
the verifier reports `PARTIAL` and skips the whole-stream check.

---

## 3. Deliberate non-goals / simplifications

* **Demo damage tools** (`simulate_damage`, `restore_chunk`) are gated behind
  `VAULT_DEMO_TOOLS` and always quarantine the original first. They exist to
  make the guarantees observable; they are not a production repair path.
* **Rule sandboxing**: rules are trusted files shipped with the project. Loading
  uses `compile`/`exec` and pins the source digest; rules are not user uploads.
* **Concurrency**: SQLite WAL with `BEGIN IMMEDIATE` transactions and
  lease-based job/run claiming. Multiple workers are safe; throughput is not
  the goal.
* **Deletion/GC**: there is none. Removing chunks would break the withdrawal and
  reference-sharing guarantees; a real deployment would add mark-and-sweep over
  `object_chunks` with an explicit retention policy.
* **Staging directory** holds exactly the bytes a fetch stage wrote and a seal
  stage has not yet committed. It is not a cache and not an input to parsing:
  the seal stage deletes the file after committing, and the parser never looks
  there. Size is bounded by the number of in-flight captures.
