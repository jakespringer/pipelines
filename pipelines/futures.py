"""Derived values, futures, combinators, and the dependency scan.

A ``@derived`` is a cheap, pure read of an artifact's own output, returned as a lazy
``Future``. Combinators build new futures from existing ones. Future-valued config
fields make a dependency data-dependent. See ``docs/05-futures-derived.md``.

Artifacts are duck-typed here (``hasattr(v, "__pipelines__")``) to avoid a circular
import with ``artifact.py``.
"""

from __future__ import annotations

import dataclasses
import logging
from typing import Callable, Generic, TypeVar

from . import runtime

T = TypeVar("T")


def _is_artifact(v) -> bool:
    return hasattr(v, "__pipelines__") and hasattr(v, "relpath")


def _value(x):
    """Resolve ``x`` if it is a Future, else return it."""
    return x.result() if isinstance(x, Future) else x


# --------------------------------------------------------------------------- #
# Future
# --------------------------------------------------------------------------- #

class Future(Generic[T]):
    """A lazy promise of a value (or artifact) that is a function of one or more outputs.

    Lazy while wiring the graph; eager-feeling in analysis via coercion. ``__eq__`` and
    ``__hash__`` are *structural* (by key) so a Future can sit in a frozen artifact field
    without triggering I/O during graph construction.
    """

    def __init__(self, sources: list, thunk: Callable[[], T], key: tuple):
        self._sources = list(sources)
        self._thunk = thunk
        self._key = key

    def result(self) -> T:
        cache = runtime._RESULT_CACHE.get()
        if cache is None:
            return self._thunk()
        if self._key not in cache:
            cache[self._key] = self._thunk()
        return cache[self._key]

    def sources(self) -> list:
        """The artifacts this future depends on (flattened, for dependency ordering)."""
        out = []
        for s in self._sources:
            out += s.sources() if isinstance(s, Future) else [s]
        return out

    # structural identity (no I/O) ----------------------------------------- #
    def _fingerprint_key(self) -> str:
        return str(self._key)

    def __eq__(self, other) -> bool:
        return isinstance(other, Future) and self._key == other._key

    def __hash__(self) -> int:
        return hash(self._key)

    def __repr__(self) -> str:
        return f"Future({self._key})"

    # eager-feeling coercion for analysis (never used by dataclass __eq__) -- #
    def __float__(self): return float(self.result())
    def __int__(self): return int(self.result())
    def __bool__(self): return bool(self.result())
    def __lt__(self, o): return self.result() < _value(o)
    def __le__(self, o): return self.result() <= _value(o)
    def __gt__(self, o): return self.result() > _value(o)
    def __ge__(self, o): return self.result() >= _value(o)


# --------------------------------------------------------------------------- #
# @derived
# --------------------------------------------------------------------------- #

def derived(reads: str | list[str] | None = None):
    """Declare a cheap, pure read of the artifact's output, exposed as a ``Future``."""
    def deco(fn):
        return _Derived(fn, reads)
    return deco


class _Derived:
    def __init__(self, fn, reads):
        self.fn = fn
        self.reads = [reads] if isinstance(reads, str) else reads
        self.name = fn.__name__

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        key = ("derived", instance.relpath, self.name)
        return Future(sources=[instance], thunk=lambda: self._read(instance), key=key)

    def _read(self, instance):
        if instance.__pipelines__.automaterialize:
            instance.retrieve(only=self.reads)     # partial retrieve (full if reads is None)
        return self.fn(instance)


# --------------------------------------------------------------------------- #
# Combinators
# --------------------------------------------------------------------------- #

def fmap(future: Future, fn) -> Future:
    return Future([future], lambda: fn(future.result()), ("fmap", future._key, id(fn)))


def gather(futures: list[Future]) -> Future:
    fut = Future(list(futures), lambda: [f.result() for f in futures],
                 ("gather", tuple(f._key for f in futures)))

    def to_frame():
        import pandas as pd
        return pd.DataFrame(fut.result())

    fut.to_frame = to_frame   # type: ignore[attr-defined]
    return fut


def _select_key(items, key) -> tuple:
    rels = tuple(getattr(i, "relpath", repr(i)) for i in items)
    return (rels, id(key))


def argmax(items, key) -> Future:
    items = list(items)
    return Future(items, lambda: max(items, key=lambda it: _value(key(it))),
                  ("argmax",) + _select_key(items, key))


def argmin(items, key) -> Future:
    items = list(items)
    return Future(items, lambda: min(items, key=lambda it: _value(key(it))),
                  ("argmin",) + _select_key(items, key))


def select(items, key, reduce) -> Future:
    """General reduce over ``(item, key-value)`` pairs -> ``Future[item]``."""
    items = list(items)
    return Future(items,
                  lambda: reduce([(it, _value(key(it))) for it in items]),
                  ("select",) + _select_key(items, key) + (id(reduce),))


# --------------------------------------------------------------------------- #
# Dependency scan + future-field resolution
# --------------------------------------------------------------------------- #

def scan_dependencies(a) -> list:
    """Direct dependency artifacts in ``a``'s fields, incl. inside tuples/dicts/futures."""
    out: list = []
    for f in dataclasses.fields(a):
        out += _edges_of(getattr(a, f.name))
    seen, deduped = set(), []
    for d in out:
        if d.relpath not in seen:
            seen.add(d.relpath)
            deduped.append(d)
    return deduped


def _edges_of(v) -> list:
    if _is_artifact(v):
        return [v]
    if isinstance(v, Future):
        return list(v.sources())
    if isinstance(v, (tuple, list)):
        return [e for x in v for e in _edges_of(x)]
    if isinstance(v, dict):
        return [e for x in v.values() for e in _edges_of(x)]
    return []


def resolve_future_fields(a):
    """Resolve any ``Future``-valued field into a concrete clone (automaterialize path)."""
    log = runtime._CTX.get().log if runtime._CTX.get() else logging.getLogger("pipelines")
    repl = {}
    for f in dataclasses.fields(a):
        v = getattr(a, f.name)
        if isinstance(v, Future):
            log.info("future: resolving field %r of %r", f.name, a.relpath)
            chosen = v.result()
            if _is_artifact(chosen):
                log.info("future: field %r resolved to artifact %r; retrieving",
                         f.name, chosen.relpath)
                chosen.retrieve()
            else:
                log.info("future: field %r resolved to value %r", f.name, chosen)
            repl[f.name] = chosen
    return dataclasses.replace(a, **repl) if repl else a
