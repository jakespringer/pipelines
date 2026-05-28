# Retrieval and Storage — `retrieve` / `exists` / `commit`

This defines the storage contract — the three functions every Artifact has (with working defaults),
partial retrieve for derived reads, atomic publish, per-artifact Store selection, the `source.*`
helper library for external inputs, the Store backends, and the local-materialization + selected
Store model. Artifact contents are user-owned: the default storage implementation does not insert
a provenance file.

> **One output shape: a directory.** An Artifact's output is the contents of `self.path`. The
> framework never serializes your Python objects — it syncs the directory you wrote and fetches it
> back. There is **no codec registry**; values live as files inside the directory and are read with
> `@derived` ([05](05-derived-and-futures.md)).

---

## 1. The contract (with defaults)

| Function | Role | Default implementation |
|----------|------|------------------------|
| `retrieve(self, *, only=None)` | make all or selected committed contents local at `self.path`; **error if never created** | download the full `relpath` dir or selected files from the artifact's selected Store into `base_path` |
| `exists(self) -> bool` | is the output committed? **scheduler-only; may be expensive** | selected Store reports a completed publish at `relpath` |
| `commit(self)` | persist `self.path` | **atomic** publication of `self.path` at `relpath` in the selected Store |

A typical constructed artifact overrides **none** of these — it writes `construct` + `relpath` and
the defaults (driven by `relpath` + its Store selection) do the rest. By default the selected Store
is the executor Store; `@artifact(store=...)` changes that Store without replacing the lifecycle
methods. The hooks exist for externals and unusual backends.

**Contract invariants:**

- `retrieve` is the cheap **consumer** path; it must **not** call `exists()` (which may be costly).
  It either succeeds (output now local) or raises ("not yet created").
- `exists` is the **planner's** tool; only the scheduler calls it, during planning. Batchable via
  `exists_many(artifacts)` (the planner groups artifacts by selected Store; each backend can do one
  prefix `list` + membership) so large sweeps don't do N round-trips.
- `commit` is **user-overridable**; see §4.

---

## 2. Partial retrieve (for derived reads)

A `@derived` that reads a 2 KB `metrics.json` must not pull a 70 GB checkpoint. `retrieve` accepts an
**`only=`** selector, and `@derived(reads=...)` declares which files it touches so the framework
partial-retrieves just those ([05](05-derived-and-futures.md) §1):

```python
@derived(reads="metrics.json")
def unsafe_rate(self) -> float:
    return json.loads((self.path / "metrics.json").read_text())["unsafe"]
# framework: self.retrieve(only=["metrics.json"]) before the body  (when automaterialize=True)
```

`retrieve(only=[...])` materializes only the listed files/globs into `self.path`. This parameter is
part of the override contract: custom `retrieve` implementations must accept it, but may retrieve
the whole artifact when their backing source cannot efficiently select files. A `@derived` with no
`reads` falls back to the full output. Gated by `automaterialize`
([01](01-artifacts.md) §6): if `False`, the body calls `self.retrieve(only=...)` itself.

---

## 3. Atomic publish

`commit` must never leave a half-published output that `exists()` mistakes for done. The default
protocol:

1. Upload `self.path`'s contents to a **staging** prefix (`…/<relpath>/.staging-<uuid>/`).
2. Verify (byte counts / checksums).
3. **Finalize**: atomically make the completed `relpath` publication visible according to the
   backend's Store protocol. A local store can rename the staged directory. An object store may use
   private publication bookkeeping or a manifest outside the artifact's user-owned contents.
4. If the final prefix is already committed (another worker won) → discard staging, accept theirs
   (idempotent).

> GCS has no atomic directory rename; its Store implementation therefore needs an atomic
> publication record or manifest after the final objects are copied. This is Store-private
> bookkeeping, not a required `meta.json` artifact output. Local uses real `rename`. There are
> **no `/tmp` flocks** (v1/`experiments`' fragile mechanism): correctness is staging plus
> backend-defined finalization.

Note (per [02](02-identity-and-storage.md) §3): durable atomicity is `commit`'s job; a crash *during
`construct`* may leave a partial *local* `self.path`, which is harmless because `exists()` checks the
**committed** store, not local bytes.

---

## 4. Overriding `commit` and opting into metadata

The default gives atomic publication only. It does **not** add metadata to the artifact output.
**Overriding `commit` transfers responsibility** to you (push to W&B, a registry, a specific
prefix), and the framework does **not** silently wrap your override (consistent with the
`autocommit=False` / by-hand philosophy). Helpers support explicit publication and optional
metadata:

```python
from pipelines.store import publish_atomic, write_meta

@artifact
class ToWandb:
    def commit(self) -> None:
        write_meta(self, into=self.path / "meta.json")                            # opt-in user output
        publish_atomic(self.path, dest=f"gs://bucket/exports/{self.relpath}")   # opt-in atomicity
        wandb_log_artifact(self.path)
```

Here `meta.json` exists only because this artifact chose to write it. If present, tooling may
display it for **lineage / `inspect` / audit**; it never gates a skip
([02](02-identity-and-storage.md) §3, §8).

---

## 5. Selecting a Store per artifact

Most artifacts publish to the executor's Store, for example a project GCS prefix. Some output
classes need a different persistence policy without bespoke lifecycle methods: large finetuned
checkpoints may stay on a mounted filesystem, while their metrics/generations/judgements go to
GCS. `@artifact(store=...)` makes that ordinary:

```python
from pipelines import Project, artifact

def local_model_store() -> str:
    return Project.config.local_model_store       # e.g. file:///data/project/em-models

@artifact(store=local_model_store)
class FinetunedModel:
    def construct(self) -> None:
        train(..., out=self.path)
    # default commit/exists/retrieve now use local_model_store atomically

@artifact
class ModelGenerations:
    model: FinetunedModel
    # default Store is still the executor's gs:// store; automatic dependency
    # materialization retrieves `model` through its own file:// Store.
```

`store=` accepts a Store or URI, or a zero-argument callable returning one. A callable is resolved
in the runtime/project context, after `Project.init`, so machine-specific `Project.config` values
do not contaminate graph identity. Store selection is **not** a dataclass field and does **not**
enter `relpath`.

A `file://` Store must be stable and reachable by every job that may retrieve its artifacts. If it
is node-private storage, a placement policy must co-locate producer and consumers; merely selecting
a local Store does not create scheduling affinity. For large outputs, configure `base_path` on the
same filesystem as the `file://` Store: the backend can finalize with an atomic rename (and
materialize locally with filesystem-native linking/copying) rather than uploading or performing a
cross-filesystem checkpoint copy.

---

## 6. `source.*` — external inputs (no `Source` type, no `locate()`)

External/given artifacts ([01](01-artifacts.md) §5.2) override `retrieve` and call **`source.*`
helpers** — plain functions that fetch into a local dir. There is **no framework `Source` type and
no `locate()` hook** (v1 had both); an external artifact is just an artifact with no `construct`:

```python
from pipelines import source

@artifact
class HFModel:
    repo: str
    revision: str = "main"
    @property
    def relpath(self) -> str: return f"hf/{self.repo}@{self.revision}"
    def retrieve(self, *, only=None) -> None:
        source.hf(self.repo, self.revision, into=self.path, only=only)
```

Available helpers (a utility library, not a framework concept):

```python
source.hf(repo, revision, into=..., only=None)     # Hugging Face snapshot
source.url(url, into=..., only=None)                # download (+ cache); optional checksum
source.gs(gs_uri, into=..., only=None)             # copy from an external GCS path
source.local(path, into=..., only=None)            # link/copy a local file or dir
```

Identity for externals is just their `relpath` (e.g. `hf/{repo}@{revision}`) — there's no
fingerprint for a `source` to contribute. `source.*` lives in the same plain-Python spirit as a
project's `steps.py`.

---

## 7. Store backends and local materialization

A `Store` is a thin, uniform interface over a backend, selected by the **store-root URI**:

| Scheme | Backend | Notes |
|--------|---------|-------|
| `file://` | local filesystem | laptop runs; also the local cache tier |
| `gs://` | Google Cloud Storage | primary cluster backend; retry-wrapped, concurrent-safe (§3) |
| `wandb://` | W&B artifacts | a **publish mirror** target (not a full filesystem store) |
| `http(s)://`, `hf://` | web / HF | read-only, used by `source.*` |

Adding a backend is "write one class"; artifacts, construction, scheduling are untouched.

**Local materialization plus selected persistence.** On a typical cluster, the executor default
Store is remote (`gs://…`) while compute runs on fast local disk. An individual artifact class may
instead select another persistent Store such as a mounted `file://` location. The local
**`base_path`** (`/dev/shm`, `/scratch`) remains where `self.path` lives during construction and
consumption:

```
default Store (gs://…)             ← datasets/results normally publish here
alternate Store (file://…/models) ← selected checkpoint classes publish here
        ▲ commit            │ retrieve        (each artifact selects one)
        │                    ▼
local base_path (/dev/shm or /scratch)  ← where construct writes & deps materialize
```

`exists()` consults that artifact's selected Store (authoritative). Laptop runs can collapse stores
and materialization to local roots. (`base_path` and project Store URIs can be supplied through
`Project.config` or explicit executor args — [10](10-configuration.md).)

---

## 8. Garbage collection

Outputs are addressed by `relpath`, so GC over the current graph is simple. The planner groups
reachable artifacts by selected Store, just as it does for freshness checks:

- `pipelines gc --keep-reachable run.py` — keep everything reachable from the current project's
  groups; offer to delete the rest (dry-run by default).
- `pipelines gc --older-than 30d --match 'pretrain/*'` — prune by age / `relpath` glob.

Current-graph reachability requires no stored metadata. Historical dependency-aware GC is available
only when artifacts opted into storing suitable provenance.

---

## 9. Summary

- The contract is **`retrieve(only=...)` / `exists` / `commit`** with working **defaults**
  (selected-Store-backed via `relpath`); a typical artifact overrides none.
- `retrieve` is the cheap consumer path (never calls `exists`); `exists` is scheduler-only and
  batchable; `commit` defaults to **atomic publication without metadata** and is overridable.
- **Partial retrieve** (`retrieve(only=...)`, `@derived(reads=...)`) avoids pulling whole outputs.
- **`source.*`** plain helpers bring external inputs in; **no `Source` type, no `locate()`**.
- One `Store` interface, backends by URI; `@artifact(store=...)` chooses an alternate Store without
  losing default atomic lifecycle behavior; local `base_path` is separate from selected
  persistence; GC is `relpath`-addressed and current-graph aware.

Next: [05-derived-and-futures.md](05-derived-and-futures.md).
