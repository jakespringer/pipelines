"""The one execution primitive: ``materialize(artifact)``.

Every executor is a policy for who calls this and how many at once. Assumes the
runtime context (``_CTX``) is already set by the executor. See ``docs/06-execution.md`` §1.
"""

from __future__ import annotations

import logging
import os

from ..artifact import has_construct
from ..futures import resolve_future_fields

log = logging.getLogger("pipelines")


def _maybe_autoyield() -> None:
    """If this job opted into ``PIPELINES_YIELD=1``, free its resources before commit.

    A commit is typically the save/upload phase, so yielding here lets the parallel
    scheduler reclaim GPUs/CPUs while this job finishes publishing. No-op elsewhere.
    """
    val = os.environ.get("PIPELINES_YIELD", "").strip().lower()
    if val in ("1", "true", "yes", "on"):
        from ..runtime import yield_now
        yield_now()


def materialize(a, *, scheduler: bool = False, strict: bool = False) -> None:
    p = a.__pipelines__
    log.info("materialize %r (scheduler=%s, strict=%s, cache=%s)",
             a.relpath, scheduler, strict, p.cache)
    if scheduler and p.cache:
        log.info("materialize %r: checking cache before building", a.relpath)
        if a.exists():
            log.info("materialize %r: cache hit -> skip/resume", a.relpath)
            return                              # skip / resume (only when cache=True)
        log.info("materialize %r: cache miss -> build", a.relpath)

    if has_construct(a):
        target = a
        if p.automaterialize:
            deps = a.dependencies()
            log.info("materialize %r: readying %d dependency(ies)", a.relpath, len(deps))
            for d in deps:
                _ready_dep(d, strict=strict)
            target = resolve_future_fields(a)   # future-valued fields -> concrete clone
        log.info("materialize %r: running construct()", a.relpath)
        target.construct()                      # writes to self.path
        if p.autocommit:
            _maybe_autoyield()                  # PIPELINES_YIELD=1 -> free GPUs before upload
            log.info("materialize %r: committing", a.relpath)
            a.commit()                          # atomic publish to selected store
    else:                                       # external / given
        log.info("materialize %r: no construct() -> retrieve external/given", a.relpath)
        a.retrieve()


def _ready_dep(d, *, strict: bool) -> None:
    if strict:
        log.info("dependency %r: strict mode -> retrieve (assumed committed)", d.relpath)
        d.retrieve()                            # ordering guaranteed it is committed
    elif d.__pipelines__.cache and d.exists():
        log.info("dependency %r: already committed -> retrieve", d.relpath)
        d.retrieve()
    else:
        log.info("dependency %r: not committed -> build upstream", d.relpath)
        materialize(d, scheduler=True, strict=False)   # autonomous: build the upstream
