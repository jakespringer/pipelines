"""The ``Store`` interface and the URI-scheme registry.

A Store is a thin, uniform interface over a backend selected by a store-root URI.
Artifacts' default lifecycle delegates here. See ``docs/03-storage-backends.md``.
"""

from __future__ import annotations

import abc
from pathlib import Path
from urllib.parse import urlsplit

_REGISTRY: dict[str, type["Store"]] = {}


def register(*schemes: str):
    def deco(cls):
        for s in schemes:
            _REGISTRY[s] = cls
        return cls
    return deco


class ReadOnlyStoreError(RuntimeError):
    pass


class Store(abc.ABC):
    """Directory-granular durable storage addressed by ``relpath``."""

    root: str

    # --- existence (scheduler-only; may be expensive) ---------------------- #
    @abc.abstractmethod
    def exists(self, relpath: str) -> bool: ...

    def exists_many(self, relpaths: list[str]) -> dict[str, bool]:
        return {r: self.exists(r) for r in relpaths}

    # --- directory transfer ------------------------------------------------ #
    @abc.abstractmethod
    def get_dir(self, relpath: str, dest: Path, only: list[str] | None = None) -> None:
        """Materialize the committed ``relpath`` (or selected files) into ``dest``."""

    @abc.abstractmethod
    def put_dir(self, src: Path, relpath: str) -> None:
        """Atomically publish ``src`` at ``relpath`` (stage -> finalize)."""

    @abc.abstractmethod
    def delete(self, relpath: str) -> None: ...

    @classmethod
    def from_uri(cls, uri) -> "Store":
        if isinstance(uri, Store):
            return uri
        scheme = urlsplit(str(uri)).scheme or "file"
        try:
            backend = _REGISTRY[scheme]
        except KeyError:
            raise ValueError(
                f"no store backend for scheme {scheme!r} (have {sorted(_REGISTRY)})")
        return backend(str(uri))
