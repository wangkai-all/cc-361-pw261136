"""Process supervisor for the demo web console.

Workers are separate OS processes (not threads): the web server only enqueues
jobs and can start/stop worker processes. Stopping a worker in the middle of a
run is therefore a real process kill, and resume really re-attaches from the
database checkpoint rather than from in-memory state.

Four kinds exist:

* ``capture``  - one process fetches *and* seals (the default, simplest demo).
* ``fetch``    - fetches to staging only.
* ``seal``     - seals from staging only.
* ``parse``    - recomputes results from sealed chunks only.

``fetch`` + ``seal`` together are the split pipeline the brief describes: the
process that touches a network origin is not the process that writes the
sealed store.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from .config import PROJECT_ROOT, load_config

WORKER_KINDS = {
    "capture": ("app.capture_worker", "capture-worker"),
    "fetch": ("app.fetch_worker", "fetch-worker"),
    "seal": ("app.seal_worker", "seal-worker"),
    "parse": ("app.parse_worker", "parse-worker"),
}


class Supervisors:
    def __init__(self):
        self.procs: dict[str, subprocess.Popen | None] = {k: None for k in WORKER_KINDS}

    def _env(self):
        env = dict(os.environ)
        cfg = load_config()
        env["VAULT_DATA_DIR"] = str(cfg.data_dir)
        env.setdefault("PYTHONPATH", str(PROJECT_ROOT))
        return env

    def _log_path(self, name: str) -> Path:
        cfg = load_config()
        cfg.log_dir.mkdir(parents=True, exist_ok=True)
        return cfg.log_dir / f"{name}.log"

    def start(self, which: str) -> dict:
        if which not in WORKER_KINDS:
            raise ValueError(f"unknown worker {which!r}")
        proc = self.procs.get(which)
        if self._alive(proc):
            return {"started": False, "pid": proc.pid}
        module, log_name = WORKER_KINDS[which]
        log_path = self._log_path(log_name)
        log = open(log_path, "ab")
        proc = subprocess.Popen(
            [sys.executable, "-m", module],
            cwd=str(PROJECT_ROOT),
            env=self._env(),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.procs[which] = proc
        return {"started": True, "pid": proc.pid, "log": str(log_path)}

    # ---- compatibility shims used by the existing web handlers ----
    def start_capture(self) -> dict:
        return self.start("capture")

    def start_parse(self) -> dict:
        return self.start("parse")

    @staticmethod
    def _alive(proc: subprocess.Popen | None) -> bool:
        return bool(proc and proc.poll() is None)

    def stop(self, which: str, *, hard: bool = False) -> dict:
        proc = self.procs.get(which)
        if not self._alive(proc):
            return {"stopped": False, "reason": "not running"}
        pid = proc.pid
        sig = signal.SIGKILL if hard else signal.SIGTERM
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return {"stopped": False, "reason": "already exited"}
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if not hard:
                os.kill(pid, signal.SIGKILL)
        return {"stopped": True, "pid": pid, "signal": "SIGKILL" if hard else "SIGTERM"}

    def status(self) -> dict:
        out = {}
        for which, (_, log_name) in WORKER_KINDS.items():
            proc = self.procs.get(which)
            if self._alive(proc):
                out[f"{which}_worker"] = {
                    "running": True,
                    "pid": proc.pid,
                    "log": str(self._log_path(log_name)),
                }
            else:
                out[f"{which}_worker"] = {"running": False,
                                          "pid": proc.pid if proc else None}
        return out

    def shutdown(self, timeout: float = 3.0):
        live = [p for p in self.procs.values() if self._alive(p)]
        for proc in live:
            try:
                os.kill(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.time() + timeout
        for proc in live:
            if proc.poll() is None:
                try:
                    proc.wait(timeout=max(0.1, deadline - time.time()))
                except subprocess.TimeoutExpired:
                    try:
                        os.kill(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
