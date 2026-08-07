"""Storage backends and publication helpers."""

from __future__ import annotations

import json
from pathlib import Path

from . import file as _file   # noqa: F401  (registers file://)
from . import gs as _gs       # noqa: F401  (registers gs://)
from . import link as _link   # noqa: F401  (registers link://, symlink-on-materialize)
from . import mirror as _mirror  # noqa: F401  (registers mirror://, local-first + replica)
from .base import ReadOnlyStoreError, Store

__all__ = ["Store", "ReadOnlyStoreError", "publish_atomic", "write_meta"]


def publish_atomic(src: Path, dest: str) -> None:
    """Stage-and-finalize ``src`` at an explicit destination URI (e.g. for a custom commit)."""
    from urllib.parse import urlsplit
    parts = urlsplit(str(dest))
    root = f"{parts.scheme}://{parts.netloc}" if parts.scheme else ""
    relpath = parts.path.lstrip("/")
    Store.from_uri(dest if not relpath else f"{root}/").put_dir(Path(src), relpath)


def write_meta(artifact, into: Path) -> None:
    """Write opt-in provenance metadata into ``into`` (never automatic)."""
    import dataclasses
    meta = {
        "relpath": artifact.relpath,
        "class": type(artifact).__name__,
        "config": {f.name: _jsonable(getattr(artifact, f.name))
                   for f in dataclasses.fields(artifact)},
    }
    Path(into).parent.mkdir(parents=True, exist_ok=True)
    Path(into).write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")


def _jsonable(v):
    if hasattr(v, "relpath"):
        return v.relpath
    if isinstance(v, (tuple, list)):
        return [_jsonable(x) for x in v]
    try:
        json.dumps(v)
        return v
    except TypeError:
        return repr(v)
