"""Local-filesystem store backend (``file://``).

Atomicity is provided by ``os.replace`` of a fully-staged directory: the committed
``relpath`` directory either does not exist or is complete. See ``docs/03 §3``.
"""

from __future__ import annotations

import fnmatch
import os
import shutil
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from .base import Store, register


def _uri_to_path(uri: str) -> Path:
    parts = urlsplit(uri)
    if parts.scheme not in ("file", ""):
        raise ValueError(f"FileStore got non-file URI: {uri!r}")
    # file:///abs/path -> netloc empty, path "/abs/path"; also accept a bare path.
    return Path(parts.path if parts.scheme == "file" else uri)


@register("file", "")
class FileStore(Store):
    def __init__(self, root: str):
        self.root = str(root)
        self._root = _uri_to_path(root)

    def _final(self, relpath: str) -> Path:
        return self._root / relpath

    def exists(self, relpath: str) -> bool:
        return self._final(relpath).is_dir()

    def get_dir(self, relpath: str, dest: Path, only: list[str] | None = None) -> None:
        src = self._final(relpath)
        if not src.is_dir():
            raise FileNotFoundError(f"not committed at {relpath!r} in {self.root}")
        _copy_tree(src, Path(dest), only)

    def put_dir(self, src: Path, relpath: str) -> None:
        src = Path(src)
        final = self._final(relpath)
        final.parent.mkdir(parents=True, exist_ok=True)
        staging = final.parent / f".staging-{final.name}-{uuid.uuid4().hex}"
        _copy_tree(src, staging, None)
        # Finalize atomically. Clobbering an existing publication is intentional.
        if final.exists():
            trash = final.parent / f".trash-{final.name}-{uuid.uuid4().hex}"
            os.replace(final, trash)
            shutil.rmtree(trash, ignore_errors=True)
        os.replace(staging, final)

    def delete(self, relpath: str) -> None:
        shutil.rmtree(self._final(relpath), ignore_errors=True)


def _copy_tree(src: Path, dst: Path, only: list[str] | None) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for path in sorted(src.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(src)
        if only is not None and not _matches(rel, only):
            continue
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def _matches(rel: Path, only: list[str]) -> bool:
    name, full = rel.name, str(rel)
    return any(fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(full, pat) for pat in only)
