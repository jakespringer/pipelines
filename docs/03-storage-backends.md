# Storage Backends — `pipelines/store/`

**Specifies:** `design/04-retrieval-and-storage.md` (the `retrieve`/`exists`/`commit` contract, partial
retrieve, atomic publish, per-artifact Store selection, backends, GC).
**Modules:** `pipelines/store/{__init__,base,file,gs,wandb,http}.py`.
**Depends on:** [02 identity](02-identity-paths.md) (relpath), [08 runtime](08-runtime-helpers.md)
(gs blob helpers, retries).
**Milestone:** M1 (`base` + `file`), M3 (`gs`, `wandb`, `http`).

A `Store` is a thin, uniform interface over a backend selected by **store-root URI**. Artifacts'
default lifecycle ([01 §3](01-core-artifact.md)) delegates here. Implements cross-cutting mechanic
**M-5** (store selection is policy, not identity).

---

## 1. The `Store` ABC (`store/base.py`)

```python
class Store(abc.ABC):
    root: str                                   # the store-root URI, e.g. "gs://bucket/exp"

    # --- existence (scheduler-only; may be expensive) ---
    @abc.abstractmethod
    def exists(self, relpath: str) -> bool: ...
    def exists_many(self, relpaths: list[str]) -> dict[str, bool]:
        return {r: self.exists(r) for r in relpaths}     # backends override with one prefix-list

    # --- directory transfer (the directory output model) ---
    @abc.abstractmethod
    def get_dir(self, relpath: str, dest: Path, only: list[str] | None = None) -> None: ...
    @abc.abstractmethod
    def put_dir(self, src: Path, relpath: str) -> None:  # ATOMIC publish (§3)
        ...

    # --- blob I/O (escape hatch; used by gs.* helper and logs) ---
    def read_bytes(self, uri_or_rel: str) -> bytes: ...
    def write_bytes(self, uri_or_rel: str, data: bytes) -> None: ...
    def list(self, prefix: str) -> list[str]: ...
    def delete(self, relpath: str) -> None: ...           # for `rm`/`gc` (09 §3)

    @classmethod
    def from_uri(cls, uri: str | "Store") -> "Store":
        if isinstance(uri, Store): return uri
        return _REGISTRY[urlsplit(uri).scheme](uri)
```

- **`only=`** is part of the contract: a backend may satisfy it by fetching just the listed files/globs,
  or fall back to the full directory if it cannot select efficiently (`design/04 §2`). Used by partial
  retrieve for `@derived(reads=...)` ([05 §1](05-futures-derived.md)).
- **`exists` must never be on the hot path**: `retrieve`/`get_dir` assume existence and raise if the
  directory was never committed; only the scheduler calls `exists`/`exists_many` ([06 §2](06-execution.md)).
- **Registry:** `_REGISTRY = {"file": FileStore, "gs": GSStore, "link": LinkStore, "wandb": WandbStore,
  "http": HttpStore, "https": HttpStore, "hf": HttpStore}`. Adding a backend = "write one class + register".
- **`link://` (`store/link.py`)** is a local `FileStore` subclass for very large artifacts (model
  checkpoints): `get_dir` materializes by **symlink** (`dest -> root/relpath`) instead of copying, and
  `put_dir` **moves** the built directory into the store (a rename on a shared filesystem) then leaves a
  symlink at the working path — so a checkpoint is stored exactly once and never copied per consumer. It
  is a filesystem backend, so anything stored under `link://` is never uploaded. `only=` is ignored (the
  symlink exposes the whole directory); `exists`/`delete` are inherited from `FileStore`.

---

## 2. URI semantics and relpath joining

- The store root + `relpath` joins with `/`. For `file://`, root is a local path; for `gs://`, root is
  `bucket/prefix`.
- **One explicit path rule** (also used by `gs.*`, [08 §3](08-runtime-helpers.md)): a trailing `/` on a
  blob path means a directory prefix; otherwise a single object. No `/.` magic, no probing.

---

## 3. Atomic publish (`put_dir`) — the core correctness property

`commit` must never leave a half-published output that `exists()` mistakes for done (`design/04 §3`).
Protocol:

1. **Stage** — upload/copy `src`'s contents to a staging location `…/<relpath>/.staging-<uuid>/`.
2. **Verify** — byte counts / checksums of staged objects.
3. **Finalize atomically** — make the completed `relpath` visible per backend (below).
4. **Idempotent race** — if the final publication already exists (another worker won), discard staging
   and accept theirs.

> **No `/tmp` flocks.** This explicitly replaces `experiments`' fragile lock mechanism. Correctness is
> staging + backend-defined finalization, which is also what makes preemption/requeue safe
> ([06 §7](06-execution.md)): a crash during `construct` leaves only a partial *local* `self.path`,
> harmless because `exists()` consults the committed Store, not local bytes.

### `FileStore` (`store/file.py`) — M1
- root = `file:///abs/path`. `exists(relpath)` = the published dir exists **and** carries the commit
  marker (a `.committed` sentinel inside a store-private sibling, *not* inside the artifact dir, so the
  user's contents stay clean — or use a `<relpath>/.pipelines-committed` marker; choose one and document).
- `put_dir`: write into `…/<relpath>/.staging-<uuid>/`, then `os.replace` (atomic rename) the staging
  dir onto `…/<relpath>/`. If the destination exists and is committed, treat as idempotent win.
- `get_dir`: if `src` and `dest` are on the **same filesystem**, hardlink/`reflink`/copy (the
  `local_model_store` fast path, `design/04 §5`); else copy. `only=` filters by filename/glob.
- `exists_many`: one `scandir` over the parent prefix → membership set.

### `GSStore` (`store/gs.py`) — M3
- root = `gs://bucket/prefix`. GCS has **no atomic directory rename**, so finalize writes a
  **publication manifest object** `…/<relpath>/.pipelines/manifest.json` (object count + checksums +
  completed=true) **after** all data objects are copied from staging. `exists(relpath)` = the manifest
  object exists and reports `completed`. Half-copied data without a manifest reads as *not committed*.
- `put_dir`: copy `src` → `…/<relpath>/` data objects (concurrent), write manifest last, delete
  staging. (Copy-then-manifest is the atomic point; an interrupted copy never has a manifest.)
- `get_dir`/`exists_many`/blob ops wrap the `gs.*` helpers ([08 §3](08-runtime-helpers.md)) which carry
  retry/backoff/concurrency.
- **Port from `experiments/runlib.py`:** `upload_to_gs:700`, `download_from_gs:565`, `sync_to_gs:782`,
  `sync_from_gs:891`, `gs_list:1301`, `gs_read_text:1172`, `gs_write_text:1222`. Adapt: drop the
  ad-hoc locking, add the manifest finalize.

### `WandbStore` (`store/wandb.py`) — M3
- A **publish mirror only**, not a full filesystem store. `put_dir` logs a W&B artifact; `get_dir`/
  `exists` are best-effort (W&B is not the system-of-record). Used when an artifact's `commit` opts to
  mirror; not a default executor Store.

### `HttpStore` (`store/http.py`) — M3
- `http(s)://`, `hf://` — **read-only**, used by `source.*` ([04](04-sources.md)). `put_dir`/`exists`
  for commit raise `ReadOnlyStoreError`.

---

## 4. Overriding `commit`; opt-in metadata helpers (`store/__init__.py`)

The default `commit` gives **atomic publication only** — no metadata added to the output. Overriding
`commit` transfers full responsibility (the framework does **not** silently wrap an override —
consistent with `autocommit=False`). Helpers support explicit publication and optional metadata
(`design/04 §4`):

```python
def publish_atomic(src: Path, dest: str) -> None:      # stage->verify->finalize at an explicit dest
    Store.from_uri(_root_of(dest)).put_dir(src, _rel_of(dest))

def write_meta(artifact, into: Path) -> None:          # opt-in user provenance (design/02 §8)
    into.write_text(json.dumps(_meta_dict(artifact), indent=2))
```

`examples/test` `PublishedBundle` uses `autocommit=False` + `self.commit()` and writes its own
`meta.json`; `design/04 §4`'s `ToWandb` example uses `write_meta` + `publish_atomic` +
`wandb_log_artifact`. Both must work.

---

## 5. Per-artifact Store selection (M-5 mechanics)

Resolved by `_resolve_store(artifact)` in [01 §3](01-core-artifact.md): `__pipelines__.store` is a URI,
`Store`, or zero-arg callable (resolved after `Project.init`). `examples` use the callable form
(`local_index_store`, `local_model_store`) so `Project.config` is read lazily — never at import time,
never into identity. A `file://` selected Store must be reachable by every job that retrieves the
artifact; if node-private, a placement policy must co-locate producer/consumers ([06](06-execution.md));
a shared mount needs no special scheduling. For large outputs, configuring `base_path` on the same
filesystem as the `file://` Store lets `put_dir` finalize with a rename and `get_dir` hardlink.

---

## 6. Garbage collection (`gc` support)

Outputs are `relpath`-addressed, so current-graph GC needs no stored metadata (`design/04 §8`). The
planner groups reachable artifacts by selected Store and:
- `--keep-reachable`: keep everything reachable from the project's groups; offer to delete the rest
  (dry-run by default).
- `--older-than D` / `--match GLOB`: prune by age / `relpath` glob.
Historical, dependency-aware GC is only possible when artifacts opted into provenance metadata. CLI
wiring in [09 §3](09-cli.md).

---

## 7. Conformance hook

- `FileStore.put_dir` is atomic under concurrent writers (two workers, one wins, both observe a
  complete dir).
- `get_dir(only=["metrics.json"])` fetches just that file (partial retrieve for `@derived`).
- `WordIndex` (selected `local_index_store`) commits/retrieves to `file://` while `Preview` (default
  store) uses the executor Store — interop in one graph.
- `em` `FinetunedModel` (file store) → `ModelGenerations` (gs store) cross-store dependency retrieval.
- `gs://` round-trip with manifest finalize; half-copied dir reads as not-committed.

Next: [04-sources.md](04-sources.md).
