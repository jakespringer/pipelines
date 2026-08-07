"""External-input helpers: ``source.hf / url / gs / local``.

Plain functions that fetch into a local directory; called inside an external
artifact's ``retrieve`` override. No ``Source`` type, no ``locate()`` hook.
See ``docs/04-sources.md``.
"""

from __future__ import annotations

import fnmatch
import os
import shutil
import uuid
from pathlib import Path


def _copy_atomic(src: Path, dst: Path) -> None:
    """Copy ``src`` to ``dst`` atomically; skip when ``dst`` already matches.

    Concurrent consumers can retrieve the same given input into one shared
    working path. A plain ``copy2`` rewrites the destination in place on every
    retrieval, so a reader racing a writer sees a torn file (observed as
    mid-line ``JSONDecodeError``s fanning out to hundreds of blocked jobs).
    Writing to a temp name and ``os.replace``-ing makes each copy one atomic
    rename, and the size+mtime match (``copy2`` preserves mtime) makes repeat
    retrievals no-ops instead of rewrite storms.
    """
    try:
        s, d = src.stat(), dst.stat()
        if s.st_size == d.st_size and int(s.st_mtime) == int(d.st_mtime):
            return
    except FileNotFoundError:
        pass
    tmp = dst.with_name(f".{dst.name}.tmp-{uuid.uuid4().hex}")
    try:
        shutil.copy2(src, tmp)
        os.replace(tmp, dst)
    finally:
        tmp.unlink(missing_ok=True)


def local(path, *, into, only: list[str] | None = None) -> None:
    """Copy a local file or directory into ``into`` (read-only treatment of source)."""
    src, into = Path(path), Path(into)
    into.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        for f in sorted(src.rglob("*")):
            if not f.is_file():
                continue
            rel = f.relative_to(src)
            if only is not None and not _matches(rel, only):
                continue
            (into / rel).parent.mkdir(parents=True, exist_ok=True)
            _copy_atomic(f, into / rel)
    elif src.is_file():
        if only is None or _matches(Path(src.name), only):
            _copy_atomic(src, into / src.name)
    else:
        raise FileNotFoundError(f"source.local: no such path {src}")


def hf(repo: str, revision: str = "main", *, into, only: list[str] | None = None) -> None:
    from huggingface_hub import snapshot_download
    snapshot_download(repo_id=repo, revision=revision, local_dir=str(into),
                      allow_patterns=only)


def url(url: str, *, into, only=None, sha256: str | None = None) -> None:
    import hashlib
    import urllib.request
    into = Path(into)
    into.mkdir(parents=True, exist_ok=True)
    dest = into / Path(url.split("?", 1)[0]).name
    urllib.request.urlretrieve(url, dest)
    if sha256 is not None:
        got = hashlib.sha256(dest.read_bytes()).hexdigest()
        if got != sha256:
            raise ValueError(f"source.url checksum mismatch for {url}: {got} != {sha256}")


def gs(gs_uri: str, *, into, only=None) -> None:
    """Download every object under a ``gs://<bucket>/<prefix>`` directory into ``into``.

    Delegates to :class:`~pipelines.store.gs.GSStore` rooted at the bucket, so it
    inherits the store's retries, connection pooling, and rsync-style size-match
    skipping. Works on any object prefix — the directory need not have been
    published by a pipelines store (no manifest required); store-internal
    ``.pipelines/`` bookkeeping is never materialized.
    """
    from urllib.parse import urlsplit

    from .store.base import Store

    parts = urlsplit(gs_uri)
    if parts.scheme != "gs" or not parts.netloc or not parts.path.strip("/"):
        raise ValueError(f"source.gs expects gs://<bucket>/<prefix>; got {gs_uri!r}")
    Store.from_uri(f"gs://{parts.netloc}").get_dir(
        parts.path.strip("/"), Path(into), only=only)


def _matches(rel: Path, only: list[str]) -> bool:
    name, full = rel.name, str(rel)
    return any(fnmatch.fnmatch(name, p) or fnmatch.fnmatch(full, p) for p in only)
