"""``LocalExecutor`` — sequential, in-process materialization (autonomous mode).

Ideal for development and for running the test example: ``construct`` bodies run in
the current process, so tracebacks point at user code and ``pdb`` just works.
See ``docs/06-execution.md`` §4.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ... import runtime
from ...runtime import RuntimeContext
from ...store import Store
from ..graph import build_plan
from ..materialize import materialize

log = logging.getLogger("pipelines")


class LocalExecutor:
    def __init__(self, store, base_path):
        self.store = str(store)
        self.base_path = _as_path(base_path)
        Store.from_uri(self.store)              # validate scheme early

    # ------------------------------------------------------------------ #
    def run(self, targets, *, dry: bool = False, force: bool = False) -> int:
        rc = RuntimeContext(base_path=self.base_path, executor_store=self.store, log=log)
        token = runtime._CTX.set(rc)
        cache_token = runtime._RESULT_CACHE.set({})
        sessions: dict = {}
        failed: list[str] = []
        try:
            plan = build_plan(targets)
            if dry:
                _print_plan(plan, self.base_path)
                return 0
            for a in plan.ordered:
                if a in plan.satisfied and not force:
                    log.info("skip (committed) %s", a.relpath)
                    continue
                rc.relpath = a.relpath
                rc.annotations = dict(a.annotations or {})
                rc.session = _ensure_session(a, sessions, rc)
                try:
                    materialize(a, scheduler=not force, strict=False)
                    log.info("built %s", a.relpath)
                except Exception:
                    log.exception("FAILED %s", a.relpath)
                    failed.append(a.relpath)
        finally:
            for s in sessions.values():
                try:
                    s.close()
                except Exception:
                    log.exception("session close failed")
            runtime._RESULT_CACHE.reset(cache_token)
            runtime._CTX.reset(token)
        if failed:
            log.error("%d artifact(s) failed: %s", len(failed), ", ".join(failed))
            return 1
        return 0

    def dryrun(self, targets) -> int:
        return self.run(targets, dry=True)


def _ensure_session(a, sessions: dict, rc: RuntimeContext):
    cls = a.__pipelines__.session
    if cls is None:
        return None
    probe = cls()
    key = (cls, tuple(probe.group_key(a)))
    if key not in sessions:
        probe.open(a, rc)
        sessions[key] = probe
    return sessions[key]


def _print_plan(plan, base_path: Path) -> None:
    print(f"# plan: {len(plan.to_build)} to build, {len(plan.satisfied)} committed")
    for a in plan.ordered:
        state = "skip" if a in plan.satisfied else "build"
        print(f"  [{state}] {a.relpath}  ->  {base_path / a.relpath}")


def _as_path(base_path) -> Path:
    return Path(str(base_path).replace("file://", "")) if base_path else Path.cwd()
