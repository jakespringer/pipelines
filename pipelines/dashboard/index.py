"""Run discovery and event-log replay for the dashboard.

The dashboard never talks to a run server over its socket. Instead it reads the same
append-only ``events.log`` that every parallel run writes (see :mod:`pipelines.scheduler.events`).
That file is the complete, replayable record of a run, so one code path serves a *live* run and a
*finished* one identically — and the dashboard keeps working after a run (or the dashboard itself)
has exited.

* :class:`RunView` replays one run's ``events.log`` into a queryable state, incrementally: each
  :meth:`RunView.refresh` reads only the bytes appended since the last call and returns the new
  records, so a streaming endpoint can forward them verbatim. It tracks *every* artifact in the
  run's plan — the ones it built and the ones it skipped as already committed (``cached``).
* :class:`RunIndex` discovers every run under the registry directory, keeps a cache of views, and
  renders the JSON payloads. The detail payload is grouped **by pipeline step** (artifact type,
  ordered by depth) to mirror ``pipelines plan``.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

from ..identity import slug as _slug
from ..scheduler import registry

# Display ordering for states — active work first, then done (completed/cached), then failures.
STATE_ORDER = ["running", "yielding", "queued", "held", "blocked",
               "completed", "cached", "failed", "cancelled"]


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
        self.arts: dict[str, dict] = {}     # relpath -> {relpath, cls, deps, cached, state, ...}
        self.order: list[str] = []          # relpaths in plan (topological) / first-seen order
        self.has_plan = False               # did server_start carry the full plan manifest?
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
            # A server_start begins a (re)run. A re-run that reuses the port appends to this same
            # events.log, so reset per-run state here: the view reflects the *latest* run, and a
            # re-queued job shows as queued again rather than keeping its previous terminal state.
            self.arts, self.order = {}, []
            self.done, self.ok, self.ended_at = False, None, None
            self.has_plan, self.pool = False, {}
            if ts is not None:
                self.started_at = ts
            if rec.get("pool"):
                self.pool = rec["pool"]
            self.project = rec.get("project", self.project)
            self.store = rec.get("store", self.store)
            self.base_path = rec.get("base_path", self.base_path)
            plan = rec.get("plan")
            if plan:                              # full manifest: seed every artifact with its type
                self.has_plan = True
                for entry in plan:
                    self._seed(entry)
            else:                                 # older log: only the to-build relpaths are known
                for relpath in rec.get("jobs", []):
                    self._ensure(relpath)
        elif kind == "job_state":
            relpath = rec.get("relpath")
            if not relpath:
                return
            self._ensure(relpath)
            prior = self.arts[relpath]
            merged = {k: v for k, v in rec.items() if k != "type"}
            merged.setdefault("deps", prior.get("deps", []))
            merged["cls"] = merged.get("cls") or prior.get("cls")
            merged["cached"] = False              # a job that emits state ran this session
            self.arts[relpath] = merged
        elif kind == "pool":
            self.pool = {k: rec[k] for k in ("gpus", "cpus", "memory_mb") if k in rec}
        elif kind == "server_done":
            self.done = True
            self.ok = bool(rec.get("ok"))
            if ts is not None:
                self.ended_at = ts

    def _seed(self, entry: dict) -> None:
        relpath = entry.get("relpath")
        if not relpath:
            return
        cached = bool(entry.get("cached"))
        if relpath not in self.arts:
            self.order.append(relpath)
        self.arts[relpath] = {                    # fresh: server_start reset cleared any prior state
            "relpath": relpath,
            "cls": entry.get("cls"),
            "deps": entry.get("deps") or [],
            "cached": cached,
            "state": "cached" if cached else "queued",
        }

    def _ensure(self, relpath: str) -> None:
        if relpath not in self.arts:
            self.order.append(relpath)
            self.arts[relpath] = {"relpath": relpath, "state": "queued", "cached": False, "deps": []}

    # ------------------------------------------------------------------ #
    # Derived reads
    # ------------------------------------------------------------------ #
    def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for relpath in self.order:
            state = self.arts.get(relpath, {}).get("state", "queued")
            out[state] = out.get(state, 0) + 1
        return out

    def n_cached(self) -> int:
        return sum(1 for rp in self.order if self.arts.get(rp, {}).get("cached"))


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

    def registry_info(self, port: int) -> dict | None:
        """The registry entry for ``port`` if it still exists (started_at / project / store …)."""
        try:
            return json.loads((registry.registry_dir() / f"{int(port)}.json").read_text())
        except (OSError, json.JSONDecodeError, ValueError):
            return None

    # ------------------------------------------------------------------ #
    # View cache
    # ------------------------------------------------------------------ #
    def _sync_locked(self, dirs: dict[int, Path]) -> None:
        """Create/drop/refresh cached views to match ``dirs``. Caller must hold ``_lock``.

        Filesystem/TCP discovery happens *before* the lock; only the in-memory view update and
        payload build run under it, so a payload is always read from views no other request is
        mutating concurrently.
        """
        for port in list(self._views):              # forget runs whose dir disappeared
            if port not in dirs:
                del self._views[port]
        for port, logdir in dirs.items():
            view = self._views.get(port)
            if view is None or view.logdir != logdir:
                view = RunView(port, logdir)
                self._views[port] = view
            view.refresh()

    # ------------------------------------------------------------------ #
    # Payloads
    # ------------------------------------------------------------------ #
    def index_payload(self) -> dict:
        dirs = self.discover()
        live = self._live()
        now = time.time()
        with self._lock:
            self._sync_locked(dirs)
            runs = [self._summary(v, p in live, live.get(p), now) for p, v in self._views.items()]
        runs.sort(key=lambda r: (not r["live"], -(r["started_at"] or 0)))
        return {"now": now, "runs": runs}

    def overview_payload(self) -> dict:
        """Every run's *full* detail (steps + instances) — backs the expanded "all runs" view."""
        dirs = self.discover()
        live = self._live()
        now = time.time()
        with self._lock:
            self._sync_locked(dirs)
            runs = [self._detail(v, p in live, live.get(p), now) for p, v in self._views.items()]
        runs.sort(key=lambda r: (not r["live"], -(r["started_at"] or 0)))
        return {"now": now, "runs": runs}

    def detail_payload(self, port: int) -> dict | None:
        port = int(port)
        dirs = self.discover()
        live = self._live()
        now = time.time()
        with self._lock:
            self._sync_locked(dirs)
            view = self._views.get(port)
            return self._detail(view, port in live, live.get(port), now) if view else None

    def detail_from_view(self, view: RunView, is_live: bool, info: dict | None = None) -> dict:
        """Render a detail payload from a caller-owned view (used by the streaming endpoint).

        ``info`` is the run's registry entry, if any — it backstops ``started_at`` for the brief
        window where the snapshot is built before the (large) ``server_start`` line is parsed.
        """
        return self._detail(view, is_live, info, time.time())

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
            "n_artifacts": len(view.order),
            "n_cached": view.n_cached(),
            "counts": view.counts(),
            "pool": view.pool,
        }

    def _detail(self, view: RunView, is_live: bool, info: dict | None, now: float) -> dict:
        payload = self._summary(view, is_live, info, now)
        arts = [view.arts[rp] for rp in view.order if rp in view.arts]
        payload["steps"] = _build_steps(arts, now)
        return payload


# --------------------------------------------------------------------------- #
# Step grouping — mirror `pipelines plan`: group by artifact type, order by depth.
# Pure functions over the replayed artifact dicts.
# --------------------------------------------------------------------------- #
def _build_steps(arts: list[dict], now: float) -> list[dict]:
    by_rp = {a["relpath"]: a for a in arts}

    depth: dict[str, int] = {}
    def depth_of(relpath: str) -> int:
        if relpath in depth:
            return depth[relpath]
        depth[relpath] = 0                            # tentative value breaks any cycle safely
        best = 0
        for dep in by_rp.get(relpath, {}).get("deps", []):
            if dep in by_rp:
                best = max(best, 1 + depth_of(dep))
        depth[relpath] = best
        return best
    for relpath in by_rp:
        depth_of(relpath)

    groups: dict[str, list[dict]] = {}
    for a in arts:
        groups.setdefault(_type_of(a), []).append(a)

    steps = []
    for cls, members in groups.items():
        from_types = sorted({
            _type_of(by_rp[dep]) for m in members for dep in m.get("deps", [])
            if dep in by_rp and _type_of(by_rp[dep]) != cls
        })
        names = _trim_common([m["relpath"] for m in members])
        steps.append({
            "type": cls,
            "tier": max(depth[m["relpath"]] for m in members),
            "from_types": from_types,
            "total": len(members),
            "instances": [_instance(m, names[m["relpath"]], now) for m in members],
        })
    steps.sort(key=lambda s: (s["tier"], s["type"]))     # upstream (inputs) first
    return steps


def _type_of(art: dict) -> str:
    return art.get("cls") or art["relpath"].split("/")[0] or art["relpath"]


def _instance(art: dict, name: str, now: float) -> dict:
    relpath = art["relpath"]
    started = art.get("started_at")
    ended = art.get("ended_at")
    elapsed = ((ended if ended is not None else now) - started) if started else None
    return {
        "relpath": relpath,
        "slug": _slug(relpath),
        "name": name,
        "state": art.get("state", "queued"),
        "cached": bool(art.get("cached")),
        "gpus": art.get("gpus", []),
        "pid": art.get("pid"),
        "exit_code": art.get("exit_code"),
        "reason": art.get("reason", ""),
        "held": bool(art.get("held", False)),
        "req": art.get("req", {}),
        "started_at": started,
        "ended_at": ended,
        "elapsed": elapsed,
    }


def _trim_common(relpaths: list[str]) -> dict[str, str]:
    """Strip the longest common leading path segments so instance names show only what differs."""
    if len(relpaths) <= 1:
        return {rp: rp for rp in relpaths}
    segs = [rp.split("/") for rp in relpaths]
    common = 0
    for i in range(min(len(s) for s in segs)):
        if len({s[i] for s in segs}) == 1:
            common = i + 1
        else:
            break
    out = {}
    for rp, s in zip(relpaths, segs):
        rest = s[common:]
        out[rp] = "/".join(rest) if rest else s[-1]   # never blank: fall back to the last segment
    return out
