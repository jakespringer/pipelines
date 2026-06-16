"""Run discovery: a tiny on-disk registry mapping a *run id* to a live run.

Every run writes ``<cache>/pipelines/runs/<run_id>.json`` describing itself; ``pipelines attach``
(no port) reads the registry to find the newest *live* run. Each entry carries a ``kind`` that
selects how liveness is determined:

* ``"parallel"`` — a local ``RunServer`` whose TCP ``port`` is the run id; the connection is the
  source of truth (the json is just a discovery hint, liveness-checked by connecting). Used only by
  ``pipelines attach`` now — the dashboard discovers runs via the ``"remote"`` path below.
* ``"slurm"`` — a ``SlurmExecutor`` run whose foreground monitor writes a shared-filesystem run dir;
  there is no port, so liveness is "the monitor is attached" — a ``heartbeat`` file it touches each
  poll, with a ``done`` marker once the run is finalized.
* ``"remote"`` — a run that ships its ``events.log`` to *this* machine over SSH (see
  :mod:`pipelines.reporting`). Its files are ``rsync``'d into ``<registry_dir>/<run_id>/``, so its
  ``logdir`` is derived (the shipping job can't know this machine's home) and its liveness is the
  same heartbeat/``done`` scheme as slurm.

Parallel runs default to ``run_id == str(port)`` and ``kind == "parallel"``, so the historical
port-keyed behavior is unchanged.
"""

from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path

# How long after the last heartbeat touch a slurm monitor is still considered attached.
_HEARTBEAT_STALE = 45.0


def registry_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")
    d = Path(base) / "pipelines" / "runs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def run_id_of(info: dict) -> str:
    """The run id for an entry, defaulting to ``str(port)`` for pre-``run_id`` entries."""
    return str(info.get("run_id") or info.get("port") or "")


def _path(run_id) -> Path:
    return registry_dir() / f"{run_id}.json"


def write(info: dict) -> Path:
    """Persist a registry entry, defaulting ``run_id``/``kind`` for parallel callers."""
    rid = run_id_of(info)
    info = {**info, "run_id": rid, "kind": info.get("kind", "parallel")}
    p = _path(rid)
    p.write_text(json.dumps(info, indent=2))
    return p


def remove(run_id) -> None:
    try:
        _path(run_id).unlink()
    except OSError:
        pass


def port_alive(port: int, host: str = "127.0.0.1", timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def logdir_of(info: dict) -> str | None:
    """The directory holding a run's ``events.log``/``heartbeat``/``done``, as seen on *this* machine.

    A ``remote`` entry omits ``logdir`` (the shipping job can't know this machine's home), so it is
    derived as ``<registry_dir>/<run_id>`` — exactly where the Shipper ``rsync``'d the files.
    """
    ld = info.get("logdir")
    if ld:
        return ld
    if info.get("kind") == "remote":
        return str(registry_dir() / run_id_of(info))
    return None


def _heartbeat_alive(info: dict) -> bool:
    """A run is live while its writer is attached: ``done`` absent and a fresh ``heartbeat``.

    Liveness compares *this* machine's clock against the heartbeat file's mtime — both local (the
    file is touched here, by the slurm monitor or by the remote Shipper's ``rsync``/``touch``), so it
    is immune to clock skew with the job machine.
    """
    logdir = logdir_of(info)
    if not logdir:
        return False
    rundir = Path(logdir)
    if (rundir / "done").exists():
        return False
    try:
        last = (rundir / "heartbeat").stat().st_mtime
    except OSError:
        return False
    stale = float(info.get("heartbeat_stale", _HEARTBEAT_STALE))
    return (time.time() - last) < stale


def alive(info: dict) -> bool:
    """Whether a registered run is currently live, dispatched on its ``kind``."""
    if info.get("kind") in ("slurm", "remote"):
        return _heartbeat_alive(info)
    try:                                         # parallel (default): TCP-probe the run server port
        return port_alive(int(info.get("port", 0)))
    except (TypeError, ValueError):
        return False


def list_runs(only_alive: bool = True) -> list[dict]:
    """All registered runs, newest first. Dead *parallel* hints are pruned when ``only_alive``.

    Slurm and remote entries are never pruned: their json is the only summary handle the dashboard
    has to a finished run (project/store/node/started_at), and for remote runs it also names the
    rsync'd ``<run_id>/`` dir — deleting it would drop a perfectly browsable past run. Only a dead
    parallel entry is pruned: it is just a stale ``pipelines attach`` discovery hint.
    """
    runs: list[dict] = []
    for f in registry_dir().glob("*.json"):
        try:
            info = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        info["_alive"] = alive(info)
        if only_alive and not info["_alive"]:
            if info.get("kind", "parallel") == "parallel":
                f.unlink(missing_ok=True)         # prune dead parallel discovery hint
            continue
        runs.append(info)
    runs.sort(key=lambda r: r.get("started_at", 0), reverse=True)
    return runs


def newest_alive() -> dict | None:
    runs = list_runs(only_alive=True)
    return runs[0] if runs else None
