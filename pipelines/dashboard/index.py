"""Run discovery and event-log replay for the dashboard.

The dashboard never talks to a run server over its socket. Instead it reads the same
append-only ``events.log`` that every parallel run writes (see :mod:`pipelines.scheduler.events`).
That file is the complete, replayable record of a run, so one code path serves a *live* run and a
*finished* one identically — and the dashboard keeps working after a run (or the dashboard itself)
has exited.

* :class:`RunView` replays one run's ``events.log`` into a queryable state, incrementally: each
  :meth:`RunView.refresh` reads only the bytes appended since the last call and returns the new
  records, so a streaming endpoint can forward them verbatim.
* :class:`RunIndex` discovers every run under the registry directory, keeps a cache of views, and
  renders the JSON payloads the HTTP layer serves. Liveness is decided by the registry (a run owns
  a ``<port>.json`` entry while serving) so we never probe a port that can't be alive.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from ..identity import slug as _slug
from ..scheduler import registry

# Display ordering for job states — running work first, terminal states last. Mirrors the
# precedence the curses monitor uses so the two surfaces read the same way.
STATE_ORDER = ["running", "yielding", "queued", "held", "blocked",
               "completed", "failed", "cancelled"]
_STATE_RANK = {s: i for i, s in enumerate(STATE_ORDER)}


class RunView:
    """Replays a single run's ``events.log`` into state, advancing incrementally.

    Not thread-safe: each consumer (the index cache, or one streaming connection) owns its own
    instance and refreshes it from its own thread.
    """

    def __init__(self, port: int, logdir):
        self.port = int(port)
        self.logdir = Path(logdir)
        self.events_path = self.logdir / "events.log"
        self._offset = 0          # bytes of events.log already consumed
        self._tail = b""          # trailing bytes past the last newline (an unfinished line)
        self._reset_state()

    def _reset_state(self) -> None:
        self.started_at = None
        self.ended_at = None
        self.done = False
        self.ok = None
        self.pool: dict = {}
        self.jobs: dict[str, dict] = {}     # relpath -> latest snapshot (minus envelope keys)
        self.order: list[str] = []          # relpaths in first-seen (declaration) order
        self.declared = 0                   # n_jobs reported by server_start
        self.project = None
        self.store = None
        self.base_path = None

    # ------------------------------------------------------------------ #
    # Ingest
    # ------------------------------------------------------------------ #
    def refresh(self) -> list[dict]:
        """Apply records appended since the last call; return the list of new records."""
        try:
            size = self.events_path.stat().st_size
        except OSError:
            return []
        if size < self._offset:                 # truncated / rotated: replay from scratch
            self._offset, self._tail = 0, b""
            self._reset_state()
        if size <= self._offset:
            return []
        try:
            with self.events_path.open("rb") as fh:
                fh.seek(self._offset)
                chunk = fh.read()
                self._offset = fh.tell()
        except OSError:
            return []

        data = self._tail + chunk
        cut = data.rfind(b"\n")
        if cut == -1:                            # no complete line yet; keep buffering
            self._tail = data
            return []
        self._tail = data[cut + 1:]

        applied: list[dict] = []
        for raw in data[:cut].split(b"\n"):
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw.decode("utf-8", "replace"))
            except json.JSONDecodeError:
                continue
            self._apply(rec)
            applied.append(rec)
        return applied

    def _apply(self, rec: dict) -> None:
        kind = rec.get("type")
        ts = rec.get("ts")
        if self.started_at is None and ts is not None:
            self.started_at = ts

        if kind == "server_start":
            if ts is not None:
                self.started_at = ts
            self.declared = max(self.declared, int(rec.get("n_jobs", 0) or 0))
            if rec.get("pool"):
                self.pool = rec["pool"]
            self.project = rec.get("project", self.project)
            self.store = rec.get("store", self.store)
            self.base_path = rec.get("base_path", self.base_path)
            for relpath in rec.get("jobs", []):  # seed every declared job as queued
                self._ensure(relpath)
        elif kind == "job_state":
            relpath = rec.get("relpath")
            if not relpath:
                return
            self._ensure(relpath)
            self.jobs[relpath] = {k: v for k, v in rec.items() if k != "type"}
        elif kind == "pool":
            self.pool = {k: rec[k] for k in ("gpus", "cpus", "memory_mb") if k in rec}
        elif kind == "server_done":
            self.done = True
            self.ok = bool(rec.get("ok"))
            if ts is not None:
                self.ended_at = ts

    def _ensure(self, relpath: str) -> None:
        if relpath not in self.jobs:
            self.order.append(relpath)
            self.jobs[relpath] = {"relpath": relpath, "state": "queued"}

    # ------------------------------------------------------------------ #
    # Derived reads
    # ------------------------------------------------------------------ #
    def n_jobs(self) -> int:
        return max(self.declared, len(self.order))

    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for relpath in self.order:
            state = self.jobs.get(relpath, {}).get("state", "queued")
            out[state] = out.get(state, 0) + 1
        return out

    def labels(self) -> dict[str, str]:
        """Map each relpath to a short display name: artifact class, ``#i`` when it repeats.

        Indices follow declaration order so they stay stable as states change — the same scheme
        the curses monitor uses.
        """
        totals: dict[str, int] = {}
        for relpath in self.order:
            cls = self._cls(relpath)
            totals[cls] = totals.get(cls, 0) + 1
        seen: dict[str, int] = {}
        labels: dict[str, str] = {}
        for relpath in self.order:
            cls = self._cls(relpath)
            i = seen.get(cls, 0)
            seen[cls] = i + 1
            labels[relpath] = cls if totals[cls] <= 1 else f"{cls}#{i}"
        return labels

    def _cls(self, relpath: str) -> str:
        cls = self.jobs.get(relpath, {}).get("cls")
        if cls:
            return cls
        return relpath.split("/")[0] or relpath   # before the first event: best-effort from path


class RunIndex:
    """Discovers runs, caches their views, and renders dashboard payloads."""

    def __init__(self):
        self._lock = threading.Lock()
        self._views: dict[int, RunView] = {}

    # ------------------------------------------------------------------ #
    # Discovery
    # ------------------------------------------------------------------ #
    def discover(self) -> dict[int, Path]:
        """Every run we can find as ``{port: logdir}`` (live and historical)."""
        root = registry.registry_dir()
        dirs: dict[int, Path] = {}
        try:
            for child in root.iterdir():
                if child.is_dir() and child.name.isdigit() and (child / "events.log").exists():
                    dirs[int(child.name)] = child
        except OSError:
            pass
        # A live run may keep its log dir elsewhere (custom runs_root); its registry entry records
        # the real path, so fold those in too.
        try:
            for f in root.glob("*.json"):
                try:
                    info = json.loads(f.read_text())
                except (OSError, json.JSONDecodeError):
                    continue
                port = int(info.get("port", 0) or 0)
                logdir = info.get("logdir")
                if port and logdir and (Path(logdir) / "events.log").exists():
                    dirs.setdefault(port, Path(logdir))
        except OSError:
            pass
        return dirs

    def _live(self) -> dict[int, dict]:
        """``{port: registry_info}`` for runs that are currently serving.

        Reuses :func:`registry.list_runs`, which TCP-checks each registered port and prunes dead
        entries — so we only ever probe ports that own a registry file (never the full history).
        """
        out: dict[int, dict] = {}
        for info in registry.list_runs(only_alive=True):
            try:
                out[int(info["port"])] = info
            except (KeyError, TypeError, ValueError):
                continue
        return out

    def logdir_for(self, port: int) -> Path | None:
        return self.discover().get(int(port))

    # ------------------------------------------------------------------ #
    # View cache
    # ------------------------------------------------------------------ #
    def _refresh(self) -> tuple[dict[int, RunView], dict[int, dict]]:
        dirs = self.discover()
        live = self._live()
        with self._lock:
            for port in list(self._views):           # forget runs whose dir disappeared
                if port not in dirs:
                    del self._views[port]
            for port, logdir in dirs.items():
                view = self._views.get(port)
                if view is None or view.logdir != logdir:
                    view = RunView(port, logdir)
                    self._views[port] = view
                view.refresh()
            return dict(self._views), live

    # ------------------------------------------------------------------ #
    # Payloads
    # ------------------------------------------------------------------ #
    def index_payload(self) -> dict:
        views, live = self._refresh()
        now = time.time()
        runs = [self._summary(v, p in live, live.get(p), now) for p, v in views.items()]
        # Live runs first, then most-recently-started.
        runs.sort(key=lambda r: (not r["live"], -(r["started_at"] or 0)))
        return {"now": now, "runs": runs}

    def detail_payload(self, port: int) -> dict | None:
        views, live = self._refresh()
        view = views.get(int(port))
        if view is None:
            return None
        return self._detail(view, int(port) in live, live.get(int(port)), time.time())

    def detail_from_view(self, view: RunView, is_live: bool) -> dict:
        """Render a detail payload from a caller-owned view (used by the streaming endpoint)."""
        return self._detail(view, is_live, None, time.time())

    # --- builders ---------------------------------------------------------- #
    def _status(self, view: RunView, is_live: bool) -> str:
        if is_live:
            return "live"
        if view.done:
            return "completed" if view.ok else "failed"
        return "interrupted"                          # serving ended without a server_done record

    def _summary(self, view: RunView, is_live: bool, info: dict | None, now: float) -> dict:
        info = info or {}
        started = view.started_at if view.started_at is not None else info.get("started_at")
        ended = view.ended_at
        elapsed = ((ended if ended is not None else now) - started) if started else None
        return {
            "port": view.port,
            "project": view.project or info.get("project"),
            "store": view.store or info.get("store"),
            "base_path": view.base_path or info.get("base_path"),
            "logdir": str(view.logdir),
            "status": self._status(view, is_live),
            "live": bool(is_live),
            "started_at": started,
            "ended_at": ended,
            "elapsed": elapsed,
            "n_jobs": view.n_jobs(),
            "counts": view.counts(),
            "pool": view.pool,
        }

    def _detail(self, view: RunView, is_live: bool, info: dict | None, now: float) -> dict:
        payload = self._summary(view, is_live, info, now)
        labels = view.labels()
        payload["jobs"] = [
            self._job(view.jobs.get(rp, {"relpath": rp, "state": "queued"}),
                      labels.get(rp, rp), now)
            for rp in view.order
        ]
        return payload

    def _job(self, job: dict, name: str, now: float) -> dict:
        relpath = job.get("relpath")
        started = job.get("started_at")
        ended = job.get("ended_at")
        elapsed = ((ended if ended is not None else now) - started) if started else None
        return {
            "relpath": relpath,
            "slug": _slug(relpath) if relpath else None,
            "name": name,
            "cls": job.get("cls"),
            "state": job.get("state", "queued"),
            "gpus": job.get("gpus", []),
            "pid": job.get("pid"),
            "exit_code": job.get("exit_code"),
            "reason": job.get("reason", ""),
            "deps": job.get("deps", []),
            "req": job.get("req", {}),
            "held": bool(job.get("held", False)),
            "started_at": started,
            "ended_at": ended,
            "elapsed": elapsed,
        }
