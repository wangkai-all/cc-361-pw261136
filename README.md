# Sealed Wire Vault

A capture → seal → recompute pipeline where **recomputation can only read the
original packet**, every result names its exact bytes, parse version and field
positions, and a damaged block blocks only the results that depend on it.

Built from an empty directory with the Python standard library only — no
frameworks, no build step, no network needed for recompute. Real files are read
and written; the demo sources are fictional.

```
   ┌──────────────┐        ┌───────────────────┐        ┌─────────────────┐
   │ demo origin  │  HTTP  │   fetch worker    │ write  │    staging/     │
   │ (fixtures)   │───────▶│ stream + SHA-256  │───────▶│ capture.staged  │
   └──────────────┘        └───────────────────┘        └────────┬────────┘
            │ withdrawn → 410                                    │ re-hash, only
            ▼                                                    │ if it matches
   new captures refused                             ┌─────────────▼──────────┐
   old objects untouched                            │      seal worker       │
                                                    │ chunker → chunks +     │
                                                    │ manifests (immutable)  │
                                                    └─────────────┬──────────┘
                                                                  │ read-only
                                                     ┌────────────▼───────────┐
                                                     │      parse worker      │
                                                     │ rule → records + spans │
                                                     └────────────┬───────────┘
                                                                  ▼
                                            results pinned to object + rule + bytes
```

Capture is two stages. **Fetch** is the only code that touches a source; its
only output is a staging file plus its length and SHA-256. **Seal** reads just
that file, re-hashes it, and only then writes chunks and a manifest. A
`capture_worker` process can run both stages back to back for convenience; the
deployment shape the design argues for runs `fetch_worker` and `seal_worker` as
separate processes, and then neither of them is the parser.

All roles run separately: the web console records intent, a fetch process owns
source access, a seal process owns vault writes, the parse worker owns
recomputation. The console can start/stop each.

## Quick start

Requires Python 3.11+ (no third-party packages).

```bash
# terminal 1 — fictional origin + web console
python3 tools/dev.py up
# => console at http://127.0.0.1:8787/ , origin at http://127.0.0.1:8811/

# terminal 2 (optional) — run workers by hand instead of from the console
PYTHONPATH=. python3 -m app.capture_worker   # combined: fetch + seal
PYTHONPATH=. python3 -m app.fetch_worker     # split stage 1
PYTHONPATH=. python3 -m app.seal_worker      # split stage 2
PYTHONPATH=. python3 -m app.parse_worker
```

Both worker styles are safe to run at once: a job queued as `combined` is only
claimed by the capture worker, and a job queued as `split` only by fetch+seal.

Then in the console:

1. **Sources → Capture & seal** on `ticker-news`. The capture worker starts
   automatically and a sealed object appears under **Objects**. **Fetch → seal
   (split)** instead routes the same capture through the two-stage pipeline.
2. **Objects → Open** the object, pick `html_records@2`, **Start parse run**.
3. **Parse runs → View results**. Then start a run with `html_records@1` and use
   **Compare** to see field-level changes with byte spans.
4. **Sources → Mutate fixture**, then capture again: a new object appears; the
   old one still verifies.
5. **Sources → Withdraw**: new captures are refused (the origin returns 410);
   old objects and results stay readable and exportable.
6. **Objects → Export package (.zip)**: a self-contained directory that
   `python3 verify_package.py .` re-checks offline.
7. **Parse runs → Stop & kill worker** mid-run, then **Resume**: the run
   continues from its checkpoint and its result digest matches an uninterrupted
   run.
8. **Objects → Chunk #N → Simulate corruption**: only records overlapping that
   chunk become blocked; **Restore** unblocks them.

## What is where

| path | role |
|---|---|
| `app/web.py` | HTTP API + static console; never fetches or parses |
| `app/capture_worker.py` | pipeline core: fetch stage, seal stage, combined loop |
| `app/fetch_worker.py` | process entry point: source → staging only |
| `app/seal_worker.py` | process entry point: staging → chunks + manifest only |
| `app/parse_worker.py` | claims parse runs; resumable process |
| `app/vault.py` | staging, chunk storage, manifests, verification, export |
| `app/chunking.py` | FastCDC-style content-defined chunking |
| `app/parse_engine.py` | regions, spans, dependency mapping, stop/resume, compare |
| `app/rules.py` | rule discovery and pinned loading |
| `app/origin.py` | fictional demo origin (feeds, mutate, withdraw) |
| `app/fixtures.py` | deterministic fictional fixtures |
| `rules/` | `html_records@1`, `html_records@2`, `csv_rows@1` |
| `tools/dev.py` | start/stop web + origin |
| `tools/smoke.py` | end-to-end checks without a browser |
| `tools/resume_test.py` | pause/resume + real SIGKILL resilience |
| `tests/` | unit + HTTP integration suite |

## Guarantees (and where they are proven)

* **Exact original** — manifest + stream SHA-256; `verify_package.py` reproduces
  the bytes offline. `tests/test_vault.py`.
* **Fetch and seal are separate stages** — the fetch stage writes only staging;
  the seal stage re-hashes staging against the fetch digest before any vault
  write. `tests/test_pipeline.py` (including a two-process handoff and a tamper
  case that must raise `StagingMismatch`).
* **No silent online re-read** — the parse worker has no fetch path;
  `tests/test_parse.py::test_parse_never_reads_online_source`.
* **Withdrawal keeps history** — `tests/test_parse.py`,
  `tests/test_web.py::test_withdraw_blocks_capture_but_not_reads`.
* **Chunk sharing** — identical re-captures add zero chunk bytes;
  `tests/test_vault.py::test_identical_recapture_shares_every_chunk`.
* **Damage localisation** — one chunk, exact seq/offset/digest;
  `tests/test_vault.py`.
* **Only dependent results blocked** —
  `tests/test_parse.py::test_damage_blocks_only_dependent_records`.
* **Stop and continue** — graceful and SIGKILL paths; `tests/test_parse.py`,
  `tools/resume_test.py`.
* **Standalone re-checkable package** — `tests/test_vault.py`,
  `tests/test_web.py`.

Design rationale, including the whole-object vs chunk-store comparison:
[`docs/design.md`](docs/design.md).

## Tests

```bash
python3 -m unittest discover -s tests -v     # 53 tests
python3 -I tools/smoke.py                    # end-to-end pipeline
python3 -I tools/resume_test.py               # pause/resume + SIGKILL
```

## Configuration

All processes read the same environment:

| variable | default | meaning |
|---|---|---|
| `VAULT_DATA_DIR` | `./data` | catalog, chunks, manifests, results |
| `VAULT_FIXTURES_DIR` | `./fixtures` | fictional source bytes |
| `VAULT_RULES_DIR` | `./rules` | parse rule files |
| `VAULT_ORIGIN_BASE` | `http://127.0.0.1:8811` | demo origin URL |
| `VAULT_CHUNK_MIN/AVG/MAX` | `512/2048/8192` | chunking bounds |
| `VAULT_MAX_CAPTURE_BYTES` | `5242880` | capture size cap |
| `VAULT_DEMO_TOOLS` | `1` | enables damage/restore demo actions |

## API sketch

```
GET  /healthz
GET  /api/state                      sources, objects, runs, rules, workers, counts
POST /api/demo/seed                  register fixtures + rules (idempotent)
POST /api/sources/<id>/capture       queue a capture        {pipeline: combined|split, auto_start_worker}
POST /api/sources/<id>/withdraw      mark withdrawn         {reason}
POST /api/sources/<id>/reactivate
POST /api/sources/<id>/bump          advance the demo fixture
GET  /api/objects/<id|digest>        manifest, chunks, sharing, runs
GET  /api/objects/<id>/verify        per-chunk damage report
GET  /api/objects/<id>/bytes         exact original stream
GET  /api/objects/<id>/chunks/<seq>  one chunk, verified before serving
POST /api/objects/<id>/export        standalone zip             {run_ids, partial}
POST /api/objects/<id>/damage        demo: corrupt/lose a chunk {seq, mode, force}
POST /api/objects/parse              queue a run                {object, rule_id}
GET  /api/runs/<id>/view             records with span/deps/blocked flags
POST /api/runs/<id>/stop             graceful or kill_worker
POST /api/runs/<id>/resume
GET  /api/compare?a=&b=              field-level diff of two runs
GET  /api/rules                      registered versions
POST /api/rules/switch               {family, rule_id}
POST /api/workers                    {action, worker: capture|fetch|seal|parse, hard}
GET  /api/chunks/<digest>            which objects/runs reference it
POST /api/chunks/<digest>            demo: restore from quarantine
GET  /api/events                     audit log
```
