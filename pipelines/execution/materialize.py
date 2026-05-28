"""The one execution primitive: ``materialize(artifact)``.

Every executor is a policy for who calls this and how many at once. Assumes the
runtime context (``_CTX``) is already set by the executor. See ``docs/06-execution.md`` §1.
"""

from __future__ import annotations

from ..artifact import has_construct
from ..futures import resolve_future_fields


def materialize(a, *, scheduler: bool = False, strict: bool = False) -> None:
    p = a.__pipelines__
    if scheduler and p.cache and a.exists():
        return                                  # skip / resume (only when cache=True)

    if has_construct(a):
        target = a
        if p.automaterialize:
            for d in a.dependencies():
                _ready_dep(d, strict=strict)
            target = resolve_future_fields(a)   # future-valued fields -> concrete clone
        target.construct()                      # writes to self.path
        if p.autocommit:
            a.commit()                          # atomic publish to selected store
    else:                                       # external / given
        a.retrieve()


def _ready_dep(d, *, strict: bool) -> None:
    if strict:
        d.retrieve()                            # ordering guaranteed it is committed
    elif d.__pipelines__.cache and d.exists():
        d.retrieve()
    else:
        materialize(d, scheduler=True, strict=False)   # autonomous: build the upstream
