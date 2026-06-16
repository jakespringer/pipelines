"""Run discovery and event-log replay for the dashboard.

The dashboard never talks to a run server over its socket. Instead it reads the same
append-only ``events.log`` that every run writes (see :mod:`pipelines.scheduler.events`) — for a
remote run, the copy ``rsync``'d into ``<registry_dir>/<run_id>/`` over SSH (see
:mod:`pipelines.reporting`). That file is the complete, replayable record of a run, so one code path
serves a *live* run and a *finished* one identically — and the dashboard keeps working after a run
(or the dashboard itself) has exited.

* :class:`RunView` replays one run's ``events.log`` into a queryable state, incrementally: each
  :meth:`RunView.refresh` reads only the bytes appended since the last call and returns the new
  records, so a streaming endpoint can forward them verbatim. It tracks *every* artifact in the
  run's plan — the ones it built, the ones it skipped as already committed (``cached``), and
  unneeded transients pruned because nothing running depended on them (``skipped``).
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

# Display ordering for states — active work first, then done (completed/cached/skipped),
# then failures. "skipped" is a transient artifact pruned because nothing running needed it.
STATE_ORDER = ["running", "yielding", "queued", "held", "blocked",
               "completed", "cached", "skipped", "failed", "cancelled"]


class RunView:
    """Replays a single run's ``events.log`` into state, advancing incrementally.

    Not thread-safe: each consumer (the index cache, or one streaming connection) owns its own
    instance and refreshes it from its own thread.
    """

    def __init__(self, run_id, logdir):
        self.run_id = str(run_id)
        self.port = self.run_id        # back-compat alias: payloads still expose this as "port"
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
        skipped = bool(entry.get("skipped"))      # transient pruned: no running consumer
        if relpath not in self.arts:
            self.order.append(relpath)
        self.arts[relpath] = {                    # fresh: server_start reset cleared any prior state
            "relpath": relpath,
            "cls": entry.get("cls"),
            "deps": entry.get("deps") or [],
            "cached": cached,
            "skipped": skipped,
            "state": "cached" if cached else "skipped" if skipped else "queued",
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
        self._views: dict[str, RunView] = {}
        self._announced: set[str] = set()       # run_ids already logged as connected (once each)

    # ------------------------------------------------------------------ #
    # Discovery
    # ------------------------------------------------------------------ #
    def discover(self) -> dict[str, Path]:
        """Every run we can find as ``{run_id: logdir}`` (live and historical).

        One source: each run's ``<run_id>.json`` registry entry. ``registry.logdir_of`` resolves the
        directory holding ``events.log`` — for a remote run that is ``<registry_dir>/<run_id>/``
        (rsync'd from the job machine); for a slurm run the shared-filesystem rundir. A run whose
        ``events.log`` hasn't landed yet (entry written, first rsync pending) is skipped until it does.
        """
        root = registry.registry_dir()
        dirs: dict[str, Path] = {}
        try:
            for f in root.glob("*.json"):
                try:
                    info = json.loads(f.read_text())
                except (OSError, json.JSONDecodeError):
                    continue
                run_id = registry.run_id_of(info)
                logdir = registry.logdir_of(info)
                if run_id and logdir and (Path(logdir) / "events.log").exists():
                    dirs[run_id] = Path(logdir)
        except OSError:
            pass
        return dirs

    def _live(self) -> dict[str, dict]:
        """``{run_id: registry_info}`` for runs that are currently live.

        Reuses :func:`registry.list_runs`, which liveness-checks each registered run (heartbeat for
        slurm/remote, TCP probe for a parallel attach hint) and prunes dead parallel hints — so we
        only ever check runs that own a registry file (never the full history).
        """
        out: dict[str, dict] = {}
        for info in registry.list_runs(only_alive=True):
            run_id = registry.run_id_of(info)
            if run_id:
                out[run_id] = info
        return out

    def logdir_for(self, run_id) -> Path | None:
        return self.discover().get(str(run_id))

    def registry_info(self, run_id) -> dict | None:
        """The registry entry for ``run_id`` if it still exists (started_at / project / store …)."""
        try:
            return json.loads((registry.registry_dir() / f"{run_id}.json").read_text())
        except (OSError, json.JSONDecodeError, ValueError):
            return None

    def is_live(self, run_id) -> bool:
        """Whether ``run_id`` is currently live — kind-aware (heartbeat for slurm/remote, TCP port
        for a parallel attach hint). Reads only this run's registry entry, cheap for stream polls."""
        info = self.registry_info(run_id)
        return registry.alive(info) if info is not None else False

    # ------------------------------------------------------------------ #
    # View cache
    # ------------------------------------------------------------------ #
    def _sync_locked(self, dirs: dict[str, Path], live: dict[str, dict]) -> None:
        """Create/drop/refresh cached views to match ``dirs``, and log newly-connected runs.

        Filesystem/TCP discovery happens *before* the lock; only the in-memory view update and
        payload build run under it, so a payload is always read from views no other request is
        mutating concurrently. ``live`` is ``{run_id: registry_info}`` for the currently-live runs
        (from the caller's :meth:`_live`), used to announce a run the first time it shows up.
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
        # Announce each run once, the first time it appears live in the index — i.e. its
        # events.log has landed (it's in ``dirs``) and it is currently alive. This is a run
        # "connecting" to the dashboard (a remote job's logs reaching us, or a local run starting).
        for run_id in dirs:
            if run_id in live and run_id not in self._announced:
                self._announced.add(run_id)
                node = (live.get(run_id) or {}).get("node")
                where = f" (node {node})" if node else ""
                print(f"pipelines dashboard: run {run_id} connected{where}", flush=True)

    # ------------------------------------------------------------------ #
    # Payloads
    # ------------------------------------------------------------------ #
    def index_payload(self) -> dict:
        dirs = self.discover()
        live = self._live()
        now = time.time()
        with self._lock:
            self._sync_locked(dirs, live)
            runs = [self._summary(v, p in live, live.get(p), now) for p, v in self._views.items()]
        runs.sort(key=lambda r: (not r["live"], -(r["started_at"] or 0)))
        return {"now": now, "runs": runs}

    def overview_payload(self) -> dict:
        """Every run's *full* detail (steps + instances) — backs the expanded "all runs" view."""
        dirs = self.discover()
        live = self._live()
        now = time.time()
        with self._lock:
            self._sync_locked(dirs, live)
            runs = [self._detail(v, p in live, live.get(p), now) for p, v in self._views.items()]
        runs.sort(key=lambda r: (not r["live"], -(r["started_at"] or 0)))
        return {"now": now, "runs": runs}

    def detail_payload(self, run_id) -> dict | None:
        run_id = str(run_id)
        dirs = self.discover()
        live = self._live()
        now = time.time()
        with self._lock:
            self._sync_locked(dirs, live)
            view = self._views.get(run_id)
            return self._detail(view, run_id in live, live.get(run_id), now) if view else None

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
            "node": info.get("node"),                 # the job's host (remote runs); None otherwise
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
        "skipped": bool(art.get("skipped")),
        "gpus": art.get("gpus", []),
        "pid": art.get("pid"),
        "job_id": art.get("job_id"),       # slurm job id (parallel runs leave this unset)
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
