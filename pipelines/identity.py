"""Identity and paths: ``relpath`` validation, ``slug``, config fingerprint, collisions.

Pure functions, no I/O. See ``docs/02-identity-paths.md``.
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib
import os
import re
from pathlib import Path
from typing import Iterable


class RelpathError(ValueError):
    """A ``relpath`` is absolute, escapes the base, or has an empty segment."""


class CollisionError(ValueError):
    """Two non-equal artifacts resolved to the same ``relpath`` in one graph."""

    def __init__(self, relpath: str, a, b):
        super().__init__(
            f"relpath collision at {relpath!r}: "
            f"{type(a).__name__}{_fields(a)} != {type(b).__name__}{_fields(b)}. "
            f"Encode every output-varying field in relpath."
        )


def _fields(a) -> str:
    try:
        return "(" + ", ".join(f"{f.name}={getattr(a, f.name)!r}"
                               for f in dataclasses.fields(a)) + ")"
    except TypeError:
        return ""


def validate_relpath(rp: str) -> str:
    """Return ``rp`` unchanged, or raise. ``/`` is a real separator (kept)."""
    if not isinstance(rp, str) or rp.strip() == "":
        raise RelpathError(f"empty relpath: {rp!r}")
    if rp.startswith("/"):
        raise RelpathError(f"absolute relpath not allowed: {rp!r}")
    for seg in rp.split("/"):
        if seg in ("", ".", "..") or seg.strip() != seg:
            raise RelpathError(f"illegal segment {seg!r} in relpath {rp!r}")
    return rp


def validate_under_base(path: Path, base: Path) -> Path:
    """Defense in depth: the resolved path must stay under ``base``."""
    base, path = Path(base).resolve(), Path(path).resolve()
    if os.path.commonpath([str(base), str(path)]) != str(base):
        raise RelpathError(f"path {path} escapes base {base}")
    return path


_SLUG_KEEP = re.compile(r"[^A-Za-z0-9._-]+")


def slug(value: str) -> str:
    """Flatten separators into a single safe path segment.

    >>> slug("Qwen/Qwen2.5-14B")
    'Qwen_Qwen2.5-14B'
    """
    out = _SLUG_KEEP.sub("_", str(value)).strip("_")
    return re.sub(r"_+", "_", out)


def _canonical(obj) -> str:
    """Deterministic, recursive serialization for fingerprinting."""
    if obj is None or isinstance(obj, (bool, int, str)):
        return repr(obj)
    if isinstance(obj, float):
        return repr(obj)
    if isinstance(obj, enum.Enum):
        return f"{type(obj).__name__}.{obj.name}"
    if hasattr(obj, "__pipelines__"):              # an artifact: identity is its relpath
        return f"@{obj.relpath}"
    if hasattr(obj, "_fingerprint_key"):           # a Future
        return f"~{obj._fingerprint_key()}"
    if isinstance(obj, (tuple, list)):
        return "(" + ",".join(_canonical(x) for x in obj) + ")"
    if isinstance(obj, frozenset):
        return "{" + ",".join(sorted(_canonical(x) for x in obj)) + "}"
    if dataclasses.is_dataclass(obj):
        parts = [f"{f.name}={_canonical(getattr(obj, f.name))}"
                 for f in sorted(dataclasses.fields(obj), key=lambda f: f.name)]
        return f"{type(obj).__name__}(" + ",".join(parts) + ")"
    raise TypeError(f"unfingerprintable value of type {type(obj).__name__}: {obj!r}")


def fingerprint(obj, n: int = 12) -> str:
    return hashlib.sha256(_canonical(obj).encode()).hexdigest()[:n]


_FINGERPRINTABLE = (bool, int, float, str, tuple, frozenset, enum.Enum)


def is_fingerprintable_type(tp) -> bool:
    """Whether a declared field type is stable to fingerprint/hash."""
    origin = getattr(tp, "__origin__", None)
    if origin in (tuple, frozenset):
        return True
    if origin in (list, dict, set):
        return False
    if tp in (list, dict, set):
        return False
    return True   # primitives, artifacts, futures, frozen dataclasses, unions, Literals, None


def check_collisions(artifacts: Iterable) -> None:
    """Graph-local guardrail: non-equal artifacts sharing a relpath is an error."""
    seen: dict[str, object] = {}
    for a in artifacts:
        rp = validate_relpath(a.relpath)
        prev = seen.get(rp)
        if prev is not None and prev != a:
            raise CollisionError(rp, prev, a)
        seen[rp] = a
