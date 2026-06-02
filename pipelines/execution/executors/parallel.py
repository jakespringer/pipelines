"""``ParallelExecutor`` — local, resource-aware concurrency (``pipelines runparallel``).

Builds the immutable plan once (same graph/freshness pass as every executor), then hands the
unsatisfied artifacts to a :class:`~pipelines.scheduler.server.RunServer`. The server runs the
scheduler loop — launch an artifact whose dependencies are met and whose ``annotations``
(``gpus``/``cpus``/``memory``) fit the free pool, each in a worker subprocess pinned via
``CUDA_VISIBLE_DEVICES`` — while serving the monitor/control API that ``pipelines attach``
connects to. The worker subprocess is the ordinary ``materialize`` primitive in strict mode,
re-entered through the hidden ``pipelines _worker`` CLI verb.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from ... import runtime
from ...project import Project
from ...runtime import RuntimeContext
from ...scheduler.resources import detect_pool
from ...scheduler.server import RunServer
from ..graph import build_plan

log = logging.getLogger("pipelines")


class ParallelExecutor:
    """Run a graph concurrently on one host, respecting per-artifact resource intent.

    ``store``/``base_path`` mirror the other executors (the CLI passes the configured
    executor's values through for ``runparallel``). ``gpus``/``cpus``/``memory_mb`` override
    host auto-detection when given.
    """

    def __init__(self, store=None, base_path=None, *, gpus: int | None = None,
                 cpus: int | None = None, memory_mb: int | None = None,
                 runfile: str | None = None):
        self.store = str(store) if store is not None else None
        self.base_path = base_path
        self.gpus = gpus
        self.cpus = cpus
        self.memory_mb = memory_mb
        # The worker subprocess re-runs this project's run.py via the CLI; the path is
        # stashed by cli.main when the command is dispatched.
        self.runfile = runfile or os.environ.get("_PIPELINES_RUNFILE")

    # ------------------------------------------------------------------ #
    def run(self, targets, *, force: bool = False, dry: bool = False) -> int:
        if not self.store or self.base_path is None:
            raise SystemExit(
                "ParallelExecutor needs a store and base_path; the configured executor in "
                "run.py did not provide them.")
        if not self.runfile:
            raise SystemExit(
                "runparallel must be launched via the `pipelines` CLI (no project run.py known).")

        base_path = _as_path(self.base_path)
        plan = self._plan(targets, base_path)
        universe = {a.relpath: a for a in plan.ordered}
        satisfied = set() if force else {a.relpath for a in plan.satisfied}

        def deps_of(a):
            return [d.relpath for d in a.dependencies() if d.relpath in universe]

        specs = [(a, deps_of(a)) for a in plan.ordered if a.relpath not in satisfied]
        # The full plan in topological order — every artifact, its type, its in-plan deps, and
        # whether it was already committed. The run server records this so a monitor can show the
        # experiment's structure (grouped by step) and what was skipped as already cached.
        manifest = [
            {"relpath": a.relpath, "cls": type(a).__name__, "deps": deps_of(a),
             "cached": a.relpath in satisfied}
            for a in plan.ordered
        ]
        pool = detect_pool(self.gpus, self.cpus, self.memory_mb)

        if dry:
            return self._dryrun(specs, satisfied, pool, base_path)

        if not specs:
            print(f"pipelines: nothing to build ({len(satisfied)} already committed)")
            return 0

        worker_argv_prefix = [
            sys.executable, "-m", "pipelines", "--project", str(self.runfile),
            "_worker", "--store", self.store, "--base-path", str(base_path),
        ]
        server = RunServer(
            specs=specs, done=satisfied, pool=pool,
            worker_argv_prefix=worker_argv_prefix,
            store=self.store, base_path=str(base_path),
            project=getattr(Project, "name", None), manifest=manifest,
        )
        return server.run()

    def dryrun(self, targets) -> int:
        return self.run(targets, dry=True)

    # ------------------------------------------------------------------ #
    def _plan(self, targets, base_path: Path):
        # build_plan's freshness pass calls exists(), which needs an active context to
        # resolve each artifact's store. Set one just for planning.
        rc = RuntimeContext(base_path=base_path, executor_store=self.store, log=log)
        token = runtime._CTX.set(rc)
        try:
            return build_plan(targets)
        finally:
            runtime._CTX.reset(token)

    def _dryrun(self, specs, satisfied, pool, base_path: Path) -> int:
        print(f"# parallel plan: {len(specs)} to build, {len(satisfied)} committed")
        print(f"# host pool: {pool.snapshot()}")
        for a, deps in specs:
            from ...scheduler.resources import ResourceRequest
            req = ResourceRequest.from_annotations(getattr(a, "annotations", {}) or {})
            fit = "ok" if pool.can_ever_fit(req) else "UNSCHEDULABLE (host too small)"
            dep_note = f"  deps={deps}" if deps else ""
            print(f"  [build] {a.relpath}  ({req.describe()}) {fit}{dep_note}")
        for rp in sorted(satisfied):
            print(f"  [skip ] {rp}")
        return 0


def _as_path(base_path) -> Path:
    return Path(str(base_path).replace("file://", "")) if base_path else Path.cwd()
