"""The ``@artifact`` decorator: turns a class into a frozen dataclass with
framework-injected members (``path``, ``dependencies``, default storage lifecycle,
``session``, ``materialize``), records its settings, and guards footguns.

See ``docs/01-core-artifact.md``.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Callable, Type

from . import futures, runtime
from .identity import (fingerprint, is_fingerprintable_type, validate_relpath,
                       validate_under_base)
from .store import Store

StoreSpec = "str | Store | Callable[[], object]"

_RESERVED = {"relpath", "path", "dependencies", "retrieve", "exists", "commit",
             "materialize", "session"}


class ArtifactDefinitionError(TypeError):
    pass


@dataclasses.dataclass(frozen=True)
class ArtifactSettings:
    automaterialize: bool = True
    autocommit: bool = True
    cache: bool = True
    store: object | None = None
    session: Type | None = None
    retries: int = 0
    env: dict | None = None
    transient: bool = False


def artifact(cls=None, /, *, automaterialize: bool = True, autocommit: bool = True,
             cache: bool = True, store=None, session=None, retries: int = 0, env=None,
             transient: bool = False):
    """Class decorator. Use bare (``@artifact``) or with args (``@artifact(cache=False)``).

    ``transient=True`` marks an on-demand intermediate: it is scheduled only when some
    artifact that will actually run this invocation depends on it. If nothing running
    needs it, it is skipped even when missing — unless ``--force`` is given. See the
    freshness/pruning pass in ``execution/graph.py`` and ``docs/06-execution.md`` §2.
    """
    settings = ArtifactSettings(automaterialize, autocommit, cache, store, session,
                                retries, env, transient)

    def wrap(klass):
        return _build_artifact(klass, settings)

    return wrap if cls is None else wrap(cls)


def _build_artifact(klass, settings: ArtifactSettings):
    _guard_reserved_names(klass)
    _validate_field_types(klass)
    klass = dataclasses.dataclass(frozen=True, kw_only=True)(klass)
    klass.__pipelines__ = settings
    _inject_members(klass)
    return klass


def _guard_reserved_names(klass) -> None:
    for name in getattr(klass, "__annotations__", {}):
        if name in _RESERVED:
            raise ArtifactDefinitionError(
                f"{klass.__name__}.{name} is reserved; declare it as a @property/method, "
                f"not a dataclass field")


def _validate_field_types(klass) -> None:
    for name, tp in getattr(klass, "__annotations__", {}).items():
        if not is_fingerprintable_type(tp):
            raise ArtifactDefinitionError(
                f"{klass.__name__}.{name}: field type {tp!r} is not fingerprintable; "
                f"use an immutable type (e.g. tuple instead of list)")


# --------------------------------------------------------------------------- #
# Member injection
# --------------------------------------------------------------------------- #

def _lookup(klass, name):
    for base in klass.__mro__:
        if base is object:
            continue
        if name in base.__dict__:
            return base.__dict__[name]
    return None


def _inject_members(klass) -> None:
    if _lookup(klass, "relpath") is None:
        _set(klass, "relpath", property(_default_relpath))
    if _lookup(klass, "annotations") is None:
        _set(klass, "annotations", property(lambda self: {}))
    for name, func in (
        ("path", property(_path)),
        ("session", property(_session)),
        ("dependencies", _dependencies),
        ("materialize", _materialize),
        ("retrieve", _default_retrieve),
        ("exists", _default_exists),
        ("commit", _default_commit),
    ):
        if _lookup(klass, name) is None:
            _set(klass, name, func)


def _set(klass, name, value) -> None:
    if not isinstance(value, property):
        value._pipelines_injected = True
    setattr(klass, name, value)


def _default_relpath(self) -> str:
    return f"{type(self).__name__}/{fingerprint(self)}"


def _path(self) -> Path:
    cur = runtime._CTX.get()
    if cur is None:
        raise runtime.RuntimeContextError(
            f"self.path requires an active executor (relpath={self.relpath!r})")
    return validate_under_base(cur.base_path / validate_relpath(self.relpath),
                               cur.base_path, follow_symlinks=False)


def _session(self):
    cur = runtime._CTX.get()
    return cur.session if cur else None


def _dependencies(self) -> list:
    return futures.scan_dependencies(self)


def _resolve_store(self) -> Store:
    spec = self.__pipelines__.store
    if spec is None:
        cur = runtime._CTX.get()
        if cur is None or cur.executor_store is None:
            raise runtime.RuntimeContextError("no default store; run under an executor")
        return Store.from_uri(cur.executor_store)
    resolved = spec() if callable(spec) and not isinstance(spec, Store) else spec
    return Store.from_uri(resolved)


def _default_retrieve(self, *, only=None) -> None:
    _resolve_store(self).get_dir(self.relpath, self.path, only=only)


def _default_exists(self) -> bool:
    return _resolve_store(self).exists(self.relpath)


def _default_commit(self) -> None:
    _resolve_store(self).put_dir(self.path, self.relpath)


def _materialize(self, *, scheduler: bool = False, strict: bool = False) -> None:
    from .execution.materialize import materialize
    materialize(self, scheduler=scheduler, strict=strict)


def has_construct(a) -> bool:
    fn = getattr(type(a), "construct", None)
    return callable(fn)
