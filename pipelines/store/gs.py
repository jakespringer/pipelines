"""Google Cloud Storage store backend (``gs://``).

GCS has no atomic directory rename, so a publication is finalized by a **manifest
object** written *last*: ``<relpath>/.pipelines/manifest.json``. ``exists()`` keys
off that manifest, so a half-uploaded directory (data objects present, no manifest
yet) reads as *not committed* — the core correctness property of ``put_dir`` (see
``docs/03 §3``).

``google-cloud-storage`` is imported lazily, only when a ``gs://`` store is actually
used (first store operation), never at package import time. Install it with
``pip install google-cloud-storage`` (or ``pip install pipelines[gs]``).
"""

from __future__ import annotations

import fnmatch
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlsplit

from .base import Store, register

log = logging.getLogger("pipelines")

_MAX_WORKERS = 16          # concurrency for bulk file transfer (up/download)


def _exists_workers() -> int:
    """Concurrency for ``exists_many`` HEAD probes.

    Kept below ``_MAX_WORKERS`` by default: each probe is one tiny request, so the
    win is in avoiding throttling-backoff and TLS-handshake thrash, not in raw fan-out.
    Override with ``PIPELINES_GS_EXISTS_WORKERS`` (e.g. when checking thousands).
    """
    try:
        n = int(os.environ.get("PIPELINES_GS_EXISTS_WORKERS", "8"))
    except ValueError:
        n = 8
    return max(1, n)


_MANIFEST = ".pipelines/manifest.json"


@register("gs")
class GSStore(Store):
    def __init__(self, root: str):
        self.root = str(root)
        parts = urlsplit(self.root)
        if parts.scheme != "gs":
            raise ValueError(f"GSStore got non-gs URI: {root!r}")
        if not parts.netloc:
            raise ValueError(f"gs:// URI missing bucket: {root!r}")
        self._bucket_name = parts.netloc
        self._prefix = parts.path.strip("/")
        self._client = None
        self._bucket = None
        self._tm = None

    # --- lazy client (the only place google.cloud is imported) ------------- #
    def _ensure(self):
        if self._bucket is None:
            try:
                from google.cloud import storage
                from google.cloud.storage import transfer_manager
            except ImportError as e:
                raise ImportError(
                    "the gs:// store backend requires google-cloud-storage; install "
                    "it with `pip install google-cloud-storage` (or "
                    "`pip install pipelines[gs]`)"
                ) from e
            self._client = storage.Client()
            self._size_connection_pool(self._client)
            self._bucket = self._client.bucket(self._bucket_name)
            self._tm = transfer_manager
        return self._bucket

    @staticmethod
    def _size_connection_pool(client) -> None:
        """Grow the client's HTTP keep-alive pool to our worker count.

        The default ``requests`` pool holds 10 connections; fanning out 16 transfer
        threads (or many ``exists`` probes) past that makes urllib3 open and then
        *discard* extra connections, so each one pays a fresh TLS handshake — which
        shows up as a long silent stall before any per-object log appears. Sizing the
        pool to the max concurrency we ever use lets those calls reuse connections.
        """
        try:
            from requests.adapters import HTTPAdapter
            n = max(_MAX_WORKERS, _exists_workers())
            adapter = HTTPAdapter(pool_connections=n, pool_maxsize=n)
            client._http.mount("https://", adapter)
            client._http.mount("http://", adapter)
        except Exception:   # best-effort tuning; never block store init on it
            log.info("gs store: could not enlarge HTTP connection pool; continuing",
                     exc_info=True)

    # --- relpath -> object-name helpers ------------------------------------ #
    def _base(self, relpath: str) -> str:
        relpath = relpath.strip("/")
        return "/".join(p for p in (self._prefix, relpath) if p)

    def _manifest_name(self, relpath: str) -> str:
        return f"{self._base(relpath)}/{_MANIFEST}"

    # --- existence --------------------------------------------------------- #
    def exists(self, relpath: str) -> bool:
        self._ensure()
        # Object writes are atomic, so the manifest is present only once the
        # publication is complete; its mere existence means committed.
        manifest = self._manifest_name(relpath)
        log.info("gs store: checking whether %r is committed (manifest gs://%s/%s)",
                 relpath, self._bucket_name, manifest)
        result = self._bucket.blob(manifest).exists()
        log.info("gs store: %r %s", relpath,
                 "exists (manifest present)" if result else "does not exist (no manifest)")
        return result

    def exists_many(self, relpaths: list[str]) -> dict[str, bool]:
        self._ensure()
        workers = min(_exists_workers(), len(relpaths)) or 1
        log.info("gs store: checking %d relpaths for commitment (%d workers)",
                 len(relpaths), workers)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(self.exists, relpaths))
        out = dict(zip(relpaths, results))
        log.info("gs store: %d/%d relpaths are committed", sum(results), len(relpaths))
        return out

    # --- directory transfer ------------------------------------------------ #
    def get_dir(self, relpath: str, dest: Path, only: list[str] | None = None) -> None:
        self._ensure()
        prefix = self._base(relpath) + "/"
        log.info("gs store: retrieving %r from gs://%s/%s into %s%s", relpath,
                 self._bucket_name, prefix, dest, f" (only {only})" if only else "")
        # transfer_manager forms each object name as blob_name_prefix + name and
        # writes it to dest/name, so pass relpath-relative names (prefix stripped).
        rels = [
            b.name[len(prefix):]
            for b in self._client.list_blobs(self._bucket, prefix=prefix)
            if not b.name.startswith(prefix + ".pipelines/")
        ]
        if not rels and not self._bucket.blob(self._manifest_name(relpath)).exists():
            log.info("gs store: %r is not committed; cannot retrieve", relpath)
            raise FileNotFoundError(f"not committed at {relpath!r} in {self.root}")
        if only is not None:
            rels = [r for r in rels if _matches(Path(r), only)]
        dest = Path(dest)
        dest.mkdir(parents=True, exist_ok=True)
        if not rels:
            log.info("gs store: %r committed but has no data files to download", relpath)
            return
        log.info("gs store: downloading %d files for %r", len(rels), relpath)
        self._tm.download_many_to_path(
            self._bucket, rels,
            destination_directory=str(dest),
            blob_name_prefix=prefix,
            create_directories=True,
            worker_type="thread",
            max_workers=_MAX_WORKERS,
            raise_exception=True,
        )

    def put_dir(self, src: Path, relpath: str) -> None:
        self._ensure()
        src = Path(src)
        rels = [p.relative_to(src).as_posix() for p in sorted(src.rglob("*")) if p.is_file()]
        base = self._base(relpath)
        log.info("gs store: publishing %d files from %s to %r (gs://%s/%s)",
                 len(rels), src, relpath, self._bucket_name, base)
        # Clobber any prior publication; deleting the manifest first makes
        # exists() report not-committed for the whole re-publish window.
        self._delete_prefix(base + "/")
        if rels:
            self._tm.upload_many_from_filenames(
                self._bucket, rels,
                source_directory=str(src),
                blob_name_prefix=base + "/",
                worker_type="thread",
                max_workers=_MAX_WORKERS,
                raise_exception=True,
            )
        # Manifest last == the atomic commit point.
        manifest = {
            "completed": True,
            "relpath": relpath,
            "files": [{"path": r, "size": (src / r).stat().st_size} for r in rels],
        }
        self._bucket.blob(self._manifest_name(relpath)).upload_from_string(
            json.dumps(manifest, indent=2), content_type="application/json")
        log.info("gs store: published %r (manifest written == commit point)", relpath)

    def delete(self, relpath: str) -> None:
        self._ensure()
        log.info("gs store: deleting %r (prefix gs://%s/%s)", relpath,
                 self._bucket_name, self._base(relpath))
        self._delete_prefix(self._base(relpath) + "/")

    # --- internals --------------------------------------------------------- #
    def _delete_prefix(self, prefix: str) -> None:
        from google.api_core.exceptions import NotFound

        def _drop(blob):
            try:
                blob.delete()
            except NotFound:
                pass

        # Remove the manifest first so exists() flips to False before the
        # (slower) data deletion runs.
        _drop(self._bucket.blob(prefix + _MANIFEST))
        rest = list(self._client.list_blobs(self._bucket, prefix=prefix))
        if rest:
            with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
                list(pool.map(_drop, rest))


def _matches(rel: Path, only: list[str]) -> bool:
    name, full = rel.name, str(rel)
    return any(fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(full, pat) for pat in only)
