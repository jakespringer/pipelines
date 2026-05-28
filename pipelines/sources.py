"""External-input helpers: ``source.hf / url / gs / local``.

Plain functions that fetch into a local directory; called inside an external
artifact's ``retrieve`` override. No ``Source`` type, no ``locate()`` hook.
See ``docs/04-sources.md``.
"""

from __future__ import annotations

import fnmatch
import shutil
from pathlib import Path


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
            shutil.copy2(f, into / rel)
    elif src.is_file():
        if only is None or _matches(Path(src.name), only):
            shutil.copy2(src, into / src.name)
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
    raise NotImplementedError("source.gs requires the gs:// backend (not yet implemented)")


def _matches(rel: Path, only: list[str]) -> bool:
    name, full = rel.name, str(rel)
    return any(fnmatch.fnmatch(name, p) or fnmatch.fnmatch(full, p) for p in only)
