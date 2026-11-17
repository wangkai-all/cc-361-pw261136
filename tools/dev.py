#!/usr/bin/env python3
"""Start/stop the demo processes (web, origin, workers) for local use.

    python3 tools/dev.py up          # web + origin in background
    python3 tools/dev.py down
    python3 tools/dev.py status
    python3 tools/dev.py logs [name]
    python3 tools/dev.py online      # flip demo sources to HTTP mode

Processes are tracked by PID files under $VAULT_DATA_DIR/pids so this never
touches unrelated processes. The web console can also start workers itself.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = Path(os.environ.get("VAULT_DATA_DIR", ROOT / "data")).resolve()
PIDS = DATA / "pids"
LOGS = DATA / "logs"
PY = sys.executable


def env():
    e = dict(os.environ)
    e["PYTHONPATH"] = str(ROOT)
    e.setdefault("VAULT_DATA_DIR", str(DATA))
    return e


def pid_path(name: str) -> Path:
    return PIDS / f"{name}.pid"


def start(name: str, args: list[str]):
    PIDS.mkdir(parents=True, exist_ok=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    if is_running(name):
        print(f"{name}: already running (pid {read_pid(name)})")
        return
    log = open(LOGS / f"{name}.log", "ab")
    proc = subprocess.Popen([PY, "-m", *args], cwd=str(ROOT), env=env(),
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    pid_path(name).write_text(str(proc.pid))
    print(f"{name}: started pid {proc.pid} -> {LOGS / (name + '.log')}")


def read_pid(name: str):
    try:
        return int(pid_path(name).read_text().strip())
    except (OSError, ValueError):
        return None


def is_running(name: str) -> bool:
    pid = read_pid(name)
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def stop(name: str, *, hard: bool = False):
    pid = read_pid(name)
    if not pid or not is_running(name):
        print(f"{name}: not running")
        pid_path(name).unlink(missing_ok=True)
        return
    os.kill(pid, signal.SIGKILL if hard else signal.SIGTERM)
    for _ in range(50):
        if not is_running(name):
            break
        time.sleep(0.1)
    pid_path(name).unlink(missing_ok=True)
    print(f"{name}: stopped (pid {pid})")


def main(argv):
    cmd = argv[1] if len(argv) > 1 else "up"
    if cmd == "up":
        start("origin", ["app.origin", "--port", "8811"])
        start("web", ["app.web", "--port", "8787"])
        time.sleep(1.0)
        for name in ("origin", "web"):
            print(f"{name}: {'running' if is_running(name) else 'FAILED'}")
    elif cmd == "down":
        for name in ("web", "origin", "capture-worker", "fetch-worker", "seal-worker",
                     "parse-worker"):
            stop(name)
    elif cmd == "status":
        for name in ("origin", "web", "capture-worker", "fetch-worker", "seal-worker",
                     "parse-worker"):
            print(f"{name}: {'running pid ' + str(read_pid(name)) if is_running(name) else 'stopped'}")
    elif cmd == "logs":
        name = argv[2] if len(argv) > 2 else "web"
        path = LOGS / f"{name}.log"
        print(path)
        if path.exists():
            sys.stdout.write(path.read_text()[-8000:])
    elif cmd == "online":
        sys.path.insert(0, str(ROOT))
        from app.config import load_config
        from app.db import connect

        cfg = load_config()
        conn = connect(cfg.db_path)
        conn.execute("UPDATE sources SET mode='online'")
        conn.commit()
        print("sources now use HTTP from", os.environ.get("VAULT_ORIGIN_BASE", "http://127.0.0.1:8811"))
        conn.close()
    else:
        print(__doc__)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
