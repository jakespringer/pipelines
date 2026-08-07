"""Local-first mirrored store backend (``mirror://``).

``mirror:/abs/local/root?to=<replica-uri>`` composes two stores:

* a **local** ``FileStore`` at ``/abs/local/root`` — typically fast, node-local
  *scratch*. Every ``put_dir`` lands here first and this commit **always
  succeeds** independent of the replica (network, credentials, outages).
* a **replica** store (typically ``gs://``) that each commit is mirrored to
  **immediately afterwards** — but *best-effort*: if the mirror attempt fails
  (expired gcloud credentials being the motivating case), the failure is logged
  loudly, the relpath stays in a durable **journal of pending mirrors**, and the
  commit still reports success. The job's work is safe on scratch and the
  replica catches up later.

Reconciliation ("later") happens through three routes, any of which may fire
first — all idempotent:

1. **Opportunistic drain**: after any *successful* replica operation in the same
   process, pending journal entries are replayed (so the first commit after a
   reauth also flushes the backlog on that node).
2. ``pipelines mirror sync <uri>`` — replays the journal on demand; with
   ``--scan`` it additionally walks the *index* of everything ever committed
   locally and re-mirrors any relpath the replica is missing (ground truth,
   independent of journal bookkeeping).
3. Job wrappers may invoke (2) at job end.

Because the local root is expendable scratch, a pending mirror whose local copy
has vanished (node reimage, tmp cleaner) is dropped with a warning — by design:
scratch expiring before a sync is an accepted loss, never an error.

Reads are local-first: ``exists``/``get_dir`` consult scratch before the
replica, so same-node re-reads are fast and credential-free. NOTE the inherent
visibility caveat: an un-mirrored commit is visible only where its scratch is
mounted (for node-local /tmp: that node alone) until a sync runs.
"""

from __future__ import annotations

import fcntl
import json
import logging
import threading
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .base import Store, register
from .file import FileStore

log = logging.getLogger("pipelines")

_STATE_DIR = ".pipelines-mirror"     # lives at the local root, outside relpath space
_JOURNAL = "journal.jsonl"           # pending replica ops (put/delete), dedup by relpath
_INDEX = "index.jsonl"               # every relpath ever committed locally (for --scan)
_LOCK = "lock"


@register("mirror")
class MirrorStore(Store):
    def __init__(self, root: str):
        self.root = str(root)
        parts = urlsplit(self.root)
        if parts.scheme != "mirror":
            raise ValueError(f"MirrorStore got non-mirror URI: {root!r}")
        local = parts.path
        if not local or not local.startswith("/"):
            raise ValueError(
                f"mirror: URI needs an absolute local path, got {root!r} "
                "(expected mirror:/abs/scratch/root?to=<replica-uri>)")
        to = parse_qs(parts.query).get("to", [""])[0]
        if not to:
            raise ValueError(f"mirror: URI missing ?to=<replica-uri>: {root!r}")
        if urlsplit(to).scheme == "mirror":
            raise ValueError(f"mirror: replica may not itself be a mirror store: {to!r}")
        self.local = FileStore(local)
        # The replica is resolved lazily: ``Store.from_uri`` memoizes instances
        # under a non-reentrant lock, and THIS constructor runs inside that very
        # lock — resolving the replica here would re-enter it and deadlock.
        self._to = to
        self._replica = None
        self._state = Path(local) / _STATE_DIR
        self._tlock = threading.Lock()

    @property
    def replica(self) -> Store:
        if self._replica is None:
            self._replica = Store.from_uri(self._to)
        return self._replica

    # ------------------------------------------------------------------ reads
    def exists(self, relpath: str) -> bool:
        if self.local.exists(relpath):
            return True
        return self.replica.exists(relpath)

    def exists_many(self, relpaths: list[str]) -> dict[str, bool]:
        out = {r: self.local.exists(r) for r in relpaths}
        misses = [r for r, ok in out.items() if not ok]
        if misses:
            out.update(self.replica.exists_many(misses))
        return out

    def get_dir(self, relpath: str, dest: Path, only: list[str] | None = None) -> None:
        try:
            self.local.get_dir(relpath, dest, only)
            return
        except FileNotFoundError:
            pass
        self.replica.get_dir(relpath, dest, only)

    # ----------------------------------------------------------------- writes
    def put_dir(self, src: Path, relpath: str) -> None:
        # 1. Local commit: the durable-enough source of truth. Never blocked by
        #    the replica.
        self.local.put_dir(src, relpath)
        self._index_add(relpath)
        # 2. Journal BEFORE attempting the mirror: if this process dies mid-
        #    upload, the pending entry survives and a later sync finishes the job.
        self._journal_add("put", relpath)
        # 3. Immediate best-effort mirror.
        try:
            self.replica.put_dir(self.local._final(relpath), relpath)
        except Exception:
            log.warning(
                "mirror store: commit of %r is safe locally but mirroring to %s "
                "FAILED — left in the pending journal for `pipelines mirror sync`",
                relpath, self.replica.root, exc_info=True)
            return
        self._journal_remove("put", relpath)
        log.info("mirror store: %r mirrored to %s", relpath, self.replica.root)
        self._drain(blocking=False)

    def delete(self, relpath: str) -> None:
        self.local.delete(relpath)
        self._journal_add("delete", relpath)
        try:
            self.replica.delete(relpath)
        except Exception:
            log.warning("mirror store: delete of %r done locally but FAILED on %s — "
                        "journaled for later sync", relpath, self.replica.root,
                        exc_info=True)
            return
        self._journal_remove("delete", relpath)
        self._drain(blocking=False)

    # ------------------------------------------------------- reconciliation
    def sync(self, scan: bool = False) -> tuple[int, int]:
        """Replay pending journal ops (and, with ``scan``, re-mirror anything the
        replica is missing from the all-time local index). Returns
        ``(done, still_pending)``."""
        done = self._drain(blocking=True)
        if scan:
            done += self._scan()
        return done, len(self.pending())

    def pending(self) -> list[dict]:
        return self._journal_read()

    def _drain(self, blocking: bool) -> int:
        """Replay journal entries against the replica. Non-blocking mode bails
        immediately if another process/thread is already draining."""
        got = self._tlock.acquire(blocking=blocking)
        if not got:
            return 0
        try:
            with self._flock(blocking=blocking) as locked:
                if not locked:
                    return 0
                done = 0
                for entry in self._journal_read():
                    op, rel = entry["op"], entry["relpath"]
                    try:
                        if op == "put":
                            final = self.local._final(rel)
                            if not final.is_dir():
                                log.warning(
                                    "mirror store: pending mirror of %r dropped — local "
                                    "copy expired before it could be uploaded", rel)
                                self._journal_remove(op, rel, locked=True)
                                continue
                            self.replica.put_dir(final, rel)
                        elif op == "delete":
                            self.replica.delete(rel)
                        else:                      # unknown op: drop, don't wedge
                            log.warning("mirror store: dropping unknown journal op %r", entry)
                            self._journal_remove(op, rel, locked=True)
                            continue
                    except Exception:
                        log.warning("mirror store: replay of %s %r failed; keeping in "
                                    "journal", op, rel, exc_info=True)
                        break                       # replica likely still unreachable
                    self._journal_remove(op, rel, locked=True)
                    done += 1
                return done
        finally:
            self._tlock.release()

    def _scan(self) -> int:
        """Ground-truth pass: every relpath ever committed locally (the index)
        that still has a local copy must exist on the replica; upload stragglers."""
        rels = [r for r in self._index_read() if self.local.exists(r)]
        if not rels:
            return 0
        missing = [r for r, ok in self.replica.exists_many(rels).items() if not ok]
        done = 0
        for rel in missing:
            try:
                self.replica.put_dir(self.local._final(rel), rel)
                done += 1
                log.info("mirror store: scan re-mirrored %r", rel)
            except Exception:
                log.warning("mirror store: scan re-mirror of %r failed", rel, exc_info=True)
                self._journal_add("put", rel)      # make sure it's tracked for next time
        return done

    # ------------------------------------------------------- journal / index
    def _flock(self, blocking: bool = True):
        state = self._state
        state.mkdir(parents=True, exist_ok=True)

        class _Lock:
            def __enter__(inner):
                inner.fh = open(state / _LOCK, "w")
                try:
                    fcntl.flock(inner.fh, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
                    return True
                except BlockingIOError:
                    inner.fh.close()
                    inner.fh = None
                    return False

            def __exit__(inner, *exc):
                if inner.fh is not None:
                    fcntl.flock(inner.fh, fcntl.LOCK_UN)
                    inner.fh.close()
                return False

        return _Lock()

    def _journal_path(self) -> Path:
        return self._state / _JOURNAL

    def _journal_read(self) -> list[dict]:
        try:
            lines = self._journal_path().read_text().splitlines()
        except FileNotFoundError:
            return []
        seen: dict[tuple, dict] = {}
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
                seen[(e["op"], e["relpath"])] = e   # last write wins per (op, relpath)
            except (ValueError, KeyError):
                log.warning("mirror store: skipping corrupt journal line %r", line[:200])
        return list(seen.values())

    def _journal_add(self, op: str, relpath: str) -> None:
        entry = json.dumps({"op": op, "relpath": relpath, "ts": time.time()})
        with self._flock():
            with open(self._journal_path(), "a") as fh:
                fh.write(entry + "\n")

    def _journal_remove(self, op: str, relpath: str, locked: bool = False) -> None:
        def _rewrite():
            keep = [e for e in self._journal_read()
                    if not (e["op"] == op and e["relpath"] == relpath)]
            tmp = self._journal_path().with_suffix(".tmp")
            tmp.write_text("".join(json.dumps(e) + "\n" for e in keep))
            tmp.replace(self._journal_path())
        if locked:                                  # caller already holds the flock
            _rewrite()
        else:
            with self._flock():
                _rewrite()

    def _index_path(self) -> Path:
        return self._state / _INDEX

    def _index_read(self) -> list[str]:
        try:
            lines = self._index_path().read_text().splitlines()
        except FileNotFoundError:
            return []
        return list(dict.fromkeys(l.strip() for l in lines if l.strip()))

    def _index_add(self, relpath: str) -> None:
        with self._flock():
            if relpath in self._index_read():
                return
            with open(self._index_path(), "a") as fh:
                fh.write(relpath + "\n")


# ------------------------------------------------------------------- CLI verb
def mirror_main(argv: list[str]) -> int:
    """``pipelines mirror <sync|status> <mirror-uri> [--scan]`` — project-free."""
    import sys
    args = [a for a in argv if not a.startswith("--")]
    flags = {a.lstrip("-") for a in argv if a.startswith("--")}
    if len(args) != 2 or args[0] not in ("sync", "status"):
        print("usage: pipelines mirror sync   <mirror-uri> [--scan]\n"
              "       pipelines mirror status <mirror-uri>", file=sys.stderr)
        return 2
    cmd, uri = args
    store = Store.from_uri(uri)
    if not isinstance(store, MirrorStore):
        print(f"not a mirror:// store URI: {uri!r}", file=sys.stderr)
        return 2
    if cmd == "status":
        pending = store.pending()
        print(f"local root : {store.local.root}\nreplica    : {store.replica.root}\n"
              f"indexed    : {len(store._index_read())} committed relpaths\n"
              f"pending    : {len(pending)}")
        for e in pending:
            print(f"  {e['op']:6s} {e['relpath']}")
        return 0 if not pending else 1
    done, left = store.sync(scan="scan" in flags)
    print(f"synced {done} item(s); {left} still pending")
    return 0 if left == 0 else 1
