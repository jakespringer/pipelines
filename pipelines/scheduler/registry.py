"""Run discovery: a tiny on-disk registry mapping a port to a live run.

``pipelines runparallel`` writes ``<cache>/pipelines/runs/<port>.json`` describing itself;
``pipelines attach`` (no port) reads the registry to find the newest *live* run. The TCP
connection is the source of truth — registry entries are just a discovery hint and are
liveness-checked by attempting a connection.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path


def registry_dir() -> Path:
    base = os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")
    d = Path(base) / "pipelines" / "runs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _path(port: int) -> Path:
    return registry_dir() / f"{port}.json"


def write(info: dict) -> Path:
    p = _path(int(info["port"]))
    p.write_text(json.dumps(info, indent=2))
    return p


def remove(port: int) -> None:
    try:
        _path(port).unlink()
    except OSError:
        pass


def port_alive(port: int, host: str = "127.0.0.1", timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def list_runs(only_alive: bool = True) -> list[dict]:
    """All registered runs, newest first. Stale entries are pruned when ``only_alive``."""
    runs: list[dict] = []
    for f in registry_dir().glob("*.json"):
        try:
            info = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        info["_alive"] = port_alive(int(info.get("port", 0)))
        if only_alive and not info["_alive"]:
            f.unlink(missing_ok=True)         # prune dead entry
            continue
        runs.append(info)
    runs.sort(key=lambda r: r.get("started_at", 0), reverse=True)
    return runs


def newest_alive() -> dict | None:
    runs = list_runs(only_alive=True)
    return runs[0] if runs else None
