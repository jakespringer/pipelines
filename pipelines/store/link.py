"""Local store that materializes by symlink instead of copy (``link://``).

For very large artifacts — model checkpoints — copying the directory into every
consumer's working tree (each eval / generation / chained-finetune that loads the
model) wastes disk and time. :class:`LinkStore` keeps the single durable copy
under its root and exposes it at the consumer's ``dest`` through a symlink:

* ``get_dir`` symlinks ``dest -> root/relpath`` instead of copying.
* ``put_dir`` *moves* the freshly built directory into the store — a rename when
  the working path and the store share a filesystem, so nothing is duplicated —
  then leaves a symlink behind at the original working path.

It is a filesystem backend, so an artifact stored here is **never uploaded**; it
is the right home for checkpoints kept off the (possibly remote) default store.
``exists`` / ``delete`` are inherited from :class:`FileStore` (the durable copy is
an ordinary directory under the root). See ``docs/03-storage-backends.md``.
"""

from __future__ import annotations

import logging
import os
import shutil
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from .base import register
from .file import FileStore

log = logging.getLogger("pipelines")


def _uri_to_path(uri: str) -> Path:
    parts = urlsplit(str(uri))
    if parts.scheme not in ("link", ""):
        raise ValueError(f"LinkStore got non-link URI: {uri!r}")
    # link:///abs/path -> netloc empty, path "/abs/path"; also accept a bare path.
    return Path(parts.path if parts.scheme == "link" else str(uri))


@register("link")
class LinkStore(FileStore):
    def __init__(self, root: str):
        self.root = str(root)
        self._root = _uri_to_path(root)

    def get_dir(self, relpath: str, dest: Path, only: list[str] | None = None) -> None:
        # ``only`` is ignored: a symlink exposes the whole committed directory, and
        # the only consumers of partial retrieval (@derived reads) target small
        # copied artifacts, not the big checkpoints stored here.
        src = self._final(relpath)
        log.info("link store: materializing %r from %s as a symlink at %s",
                 relpath, src, dest)
        if not src.is_dir():
            log.info("link store: %r is not committed; cannot materialize", relpath)
            raise FileNotFoundError(f"not committed at {relpath!r} in {self.root}")
        _symlink(src, Path(dest))

    def put_dir(self, src: Path, relpath: str) -> None:
        src = Path(src)
        final = self._final(relpath)
        log.info("link store: publishing %s to %r (final %s)", src, relpath, final)
        final.parent.mkdir(parents=True, exist_ok=True)
        # Already in place — skip the move/symlink (which would otherwise delete the
        # data; see _symlink). Two cases resolve `src` to `final`: (a) the leftover
        # symlink from a prior publish (idempotent re-commit), or (b) a chained model
        # whose relpath nests under a parent link:// model, so its work path resolved
        # *through* the parent's symlink straight into the store. Guard on `final`
        # being a real directory so a normal first commit (final absent) still moves.
        if final.is_dir() and src.resolve() == final.resolve():
            log.info("link store: %r already in place in the store; nothing to do", relpath)
            return
        # Move the built directory into the store (rename on a shared filesystem),
        # finalize atomically, then leave a symlink where it used to be.
        staging = final.parent / f".staging-{final.name}-{uuid.uuid4().hex}"
        shutil.move(str(src), str(staging))         # rename, else copy + remove src
        if final.exists() or final.is_symlink():
            log.info("link store: %r already exists; clobbering prior publication", relpath)
            trash = final.parent / f".trash-{final.name}-{uuid.uuid4().hex}"
            os.replace(final, trash)
            shutil.rmtree(trash, ignore_errors=True)
        os.replace(staging, final)
        _symlink(final, src)
        log.info("link store: published %r and linked %s -> %s", relpath, src, final)


def _symlink(target: Path, link: Path) -> None:
    """Atomically point ``link`` at ``target`` (an absolute symlink)."""
    target = target.resolve()
    # Defense in depth: if ``link`` already resolves to ``target`` (e.g. a chained
    # model whose work/consumer path runs *through* a parent symlink into the store,
    # so link and target are the same directory) there is nothing to do — and the
    # rmtree below would delete the very data we mean to expose. Bail out first.
    if link.exists() and link.resolve() == target:
        return
    link.parent.mkdir(parents=True, exist_ok=True)
    if link.is_symlink():
        try:
            if Path(os.readlink(link)) == target:
                return                              # already linked correctly
        except OSError:
            pass
    elif link.is_dir():
        shutil.rmtree(link, ignore_errors=True)     # replace a stale real directory
    tmp = link.parent / f".lnk-{link.name}-{uuid.uuid4().hex}"
    tmp.symlink_to(target, target_is_directory=True)
    os.replace(tmp, link)                           # atomic; replaces an existing symlink
