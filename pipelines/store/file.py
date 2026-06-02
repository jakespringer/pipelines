"""Local-filesystem store backend (``file://``).

Atomicity is provided by ``os.replace`` of a fully-staged directory: the committed
``relpath`` directory either does not exist or is complete. See ``docs/03 §3``.
"""

from __future__ import annotations

import fnmatch
import logging
import os
import shutil
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from .base import Store, register

log = logging.getLogger("pipelines")


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
        final = self._final(relpath)
        log.info("file store: checking whether %r is committed (dir %s)", relpath, final)
        result = final.is_dir()
        log.info("file store: %r %s", relpath,
                 "exists (committed)" if result else "does not exist (not committed)")
        return result

    def get_dir(self, relpath: str, dest: Path, only: list[str] | None = None) -> None:
        src = self._final(relpath)
        log.info("file store: retrieving %r from %s into %s%s", relpath, src, dest,
                 f" (only {only})" if only else "")
        if not src.is_dir():
            log.info("file store: %r is not committed; cannot retrieve", relpath)
            raise FileNotFoundError(f"not committed at {relpath!r} in {self.root}")
        _copy_tree(src, Path(dest), only)
        log.info("file store: retrieved %r into %s", relpath, dest)

    def put_dir(self, src: Path, relpath: str) -> None:
        src = Path(src)
        final = self._final(relpath)
        log.info("file store: publishing %s to %r (final %s)", src, relpath, final)
        final.parent.mkdir(parents=True, exist_ok=True)
        staging = final.parent / f".staging-{final.name}-{uuid.uuid4().hex}"
        _copy_tree(src, staging, None)
        # Finalize atomically. Clobbering an existing publication is intentional.
        if final.exists():
            log.info("file store: %r already exists; clobbering prior publication", relpath)
            trash = final.parent / f".trash-{final.name}-{uuid.uuid4().hex}"
            os.replace(final, trash)
            shutil.rmtree(trash, ignore_errors=True)
        os.replace(staging, final)
        log.info("file store: published %r", relpath)

    def delete(self, relpath: str) -> None:
        final = self._final(relpath)
        log.info("file store: deleting %r (dir %s, exists=%s)", relpath, final, final.is_dir())
        shutil.rmtree(final, ignore_errors=True)


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
