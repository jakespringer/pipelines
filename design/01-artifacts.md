# Artifacts — The Fundamental Unit

This defines the `Artifact`: the `@artifact` decorator, how fields double as configuration and
dependency edges, the `relpath` anchor, the four-function storage contract, constructed vs.
external artifacts, and annotations. Everything else is defined in terms of this.

---

## 1. An Artifact is an `@artifact` frozen dataclass

```python
from pathlib import Path
from pipelines import artifact

@artifact
class PretrainedModel:
    lr: float
    epochs: int
    batch_size: int = 32

    @property
    def relpath(self) -> str:
        return f"pretrain/lr{self.lr:.0e}_ep{self.epochs}_bs{self.batch_size}"

    def construct(self) -> None:
        train(lr=self.lr, epochs=self.epochs, batch_size=self.batch_size, out=self.path)
```

`@artifact` (replacing v1's `class X(Artifact)`) does three things:

1. **Applies `@dataclass(frozen=True, kw_only=True)`.** One decorator, not two. `frozen` makes the
   instance immutable/hashable; `kw_only` removes the "required field after a defaulted field"
   problem (so a subtype can add required fields without fake `= None` defaults).
2. **Guards footguns.** It raises at class-creation if a reserved name (`relpath`, `path`, the
   storage members) is declared as an annotated dataclass *field* (those must be `@property`/method,
   never config). It rejects non-frozen / non-fingerprintable field types.
3. **Carries settings and provides defaults** (§5, §6): the keyword args `automaterialize`,
   `autocommit`, `cache`, optional `store`, and the default `retrieve`/`exists`/`commit`/`path`
   machinery.

Instantiating an Artifact **does nothing** but create a description:

```python
m = PretrainedModel(lr=1e-3, epochs=10)   # no training; just a config object
```

---

## 2. Fields are the configuration; `relpath` is the identity

The dataclass **fields** are the full configuration. The artifact's identity is its **`relpath`**
(see [02](02-identity-and-storage.md)): a human-readable relative path, a pure function of the
fields, declared as a plain attribute or a `@property`. Two artifacts with the same `relpath` are
the same artifact.

```python
m = PretrainedModel(lr=1e-3, epochs=10)
m.relpath    # "pretrain/lr1e-03_ep10_bs32"   — pure, no I/O, available always
m.path       # "<base_path>/pretrain/lr1e-03_ep10_bs32"  — absolute local, needs an executor
```

There is **no content hash**. The author is responsible for putting every field they want the
output to vary by into `relpath`; fields left out share storage across runs (intentional clobber,
or a bug). Within one graph, non-equal configs producing the same `relpath` are a graph-build
**error**. Full rules in [02](02-identity-and-storage.md).

---

## 3. Dependencies are Artifact-valued fields

A dependency is expressed by **holding another Artifact in your config** — the only way to make an
edge, and it builds the DAG:

```python
@artifact
class FinetunedModel:
    base: PretrainedModel        # ← dependency edge
    data: Dataset                # ← dependency edge
    lr: float
    @property
    def relpath(self) -> str:
        return f"finetune/{self.data.name}/lr{self.lr:.0e}"
    def construct(self) -> None:
        finetune(self.base.path, self.data.path, lr=self.lr, out=self.path)
```

- The framework discovers dependencies by scanning the config for Artifact-valued fields
  (including those nested in `tuple`/`dict`). This is the DAG — the `experiments` model, kept.
- Inside `construct`, a dependency's materialized output is at **`self.dep.path`** (the framework
  has retrieved it first — see [03](03-construction.md) §2, and `automaterialize` in §6).
- Use **immutable containers** (`tuple`, not `list`) for collection fields so the artifact stays
  hashable:

```python
@artifact
class Ensemble:
    members: tuple[PretrainedModel, ...]      # every member is a dependency edge
    def construct(self) -> None:
        combine([m.path for m in self.members], out=self.path)
```

> A field may also hold a **`Future`** ("the best of a sweep") — a data-dependent dependency.
> With `automaterialize=True` it is resolved before `construct` at the consumer's runtime; with
> `automaterialize=False` the author resolves/materializes it explicitly. See
> [05](05-derived-and-futures.md).

---

## 4. The four-function storage contract

Beyond `relpath` and `construct`, an Artifact has a storage contract. The framework supplies
working **defaults** (backed by the artifact's selected Store via `relpath`), so the common case
overrides none of them:

| Member | Role | Default |
|--------|------|---------|
| `construct(self)` | build the output locally at `self.path` | none — author writes it (omit for external) |
| `retrieve(self, *, only=None)` | make all or selected committed contents local at `self.path`; **error if never created** | download from selected Store at `relpath` |
| `exists(self)` | is the output already committed? **scheduler-only; may be expensive** | selected Store has a completed publication at `relpath` |
| `commit(self)` | persist `self.path` to storage | atomic publish to selected Store |

The full semantics (defaults, partial retrieve, atomic publish, backends) are in
[04](04-retrieval-and-storage.md). The key contract points:

- `retrieve` is the **cheap consumer path** — it must not call `exists()` (which may be costly); it
  either succeeds or raises.
- `exists` is the **planner's** tool — only the scheduler calls it, during planning, to decide
  skips. It is never on the hot path.
- `commit` is **user-overridable** (push to W&B, a registry, a specific prefix); the default is
  atomic publication, and overriding transfers responsibility ([04](04-retrieval-and-storage.md)
  §4). The framework does not create `meta.json`; an artifact may choose to write metadata as
  ordinary output.

---

## 5. Constructed vs. external (given) artifacts

`construct` is **optional**:

### 5.1 Constructed (the common case)
Has a `construct` that writes the output. Defaults handle storage.

### 5.2 External / given
No `construct` — already exists somewhere; only `retrieve` is overridden, usually via a `source.*`
helper ([04](04-retrieval-and-storage.md) §6). There is **no separate `Source` type or `locate()`
hook** (v1 had these) — an external artifact is just an artifact with no `construct`:

```python
from pipelines import artifact, source

@artifact
class HFModel:
    repo: str
    revision: str = "main"
    @property
    def relpath(self) -> str: return f"hf/{self.repo}@{self.revision}"
    def retrieve(self, *, only=None) -> None:
        source.hf(self.repo, self.revision, into=self.path, only=only)  # plain helper, not a hook
    # exists() default (present locally / in store) is usually fine; override to check the hub

@artifact
class PublicCorpus:
    url: str
    @property
    def relpath(self) -> str: return f"corpus/{slug(self.url)}"
    def retrieve(self, *, only=None) -> None:
        source.url(self.url, into=self.path, only=only)
```

> **The contract:** every Artifact must be *retrievable* — a constructed artifact becomes so by
> being built and committed; an external one because `retrieve` fetches it. An artifact with
> neither a usable `construct` nor a `retrieve` (and no committed output) is an error the executor
> reports at planning time.

> **OPEN (minor):** the default `exists()` for a *never-committed* external (pure passthrough from
> HF) — "present in store/local" vs "present on the hub." Default is the cheap local/store check;
> override for hub semantics.

---

## 6. Settings: `automaterialize`, `autocommit`, `cache`, `store`

`@artifact(...)` accepts three lifecycle booleans (all default `True`) plus an optional Store
selection; none affect identity:

| Setting | Default | Meaning |
|---------|---------|---------|
| `automaterialize` | `True` | Before `construct`, the framework materializes the **direct deps** and resolves future-valued fields (so ordinary dependency access is ready); it also gates whether a `@derived` read auto-(partial-)retrieves the output it reads ([05](05-derived-and-futures.md)). `False` ⇒ the author retrieves and resolves/materializes by hand in `construct`/`@derived` (`self.dep.retrieve()`, `self.retrieve(only=...)`, `future.result()`) — an escape hatch for lazy/conditional loading. **Dependency *ordering* is always derived from fields regardless** ([06](06-execution.md)). |
| `autocommit` | `True` | After `construct`, the framework calls `commit()`. `False` ⇒ the author commits by hand. |
| `cache` | `True` | `False` ⇒ always rebuild: the scheduler skips the `exists()` check and always runs `construct`. |
| `store` | executor default Store | URI/Store, or zero-argument callable returning one, used by the default `retrieve`/`exists`/`commit`. Use a callable for `Project.config` values resolved after `Project.init`; e.g. large local checkpoints. |

```python
@artifact(cache=False)            # cheap, always-fresh
class QuickEval:
    ...

@artifact(automaterialize=False)  # I'll fetch only the deps I actually need
class ConditionalThing:
    ...

from pipelines import Project

@artifact(store=lambda: Project.config.local_model_store)
class LargeCheckpoint:
    """Publish atomically to configured file:// storage, not default gs://."""
    ...
```

An alternate `file://` Store is a durable selected Store, not scratch space: it must be visible to
any later job that retrieves the artifact. If it names node-private storage, the executor must
co-locate dependent jobs; a shared/persistent mounted path needs no special scheduling.

> **OPEN (minor):** decorator-with-args ergonomics — support both bare `@artifact` and
> `@artifact(cache=False)` (standard pattern: the decorator returns a decorator when called with
> kwargs).

---

## 7. Annotations

An Artifact may declare **`annotations`** — an open, namespaced bag of execution hints/metadata that
executors consume as they please (portable intent like `gpus`/`cpus`/`memory`, per-backend
namespaces like `slurm`, and free metadata). It is **not** a decorator setting — keep it a class
attribute or `@property` (config-dependent), always read as `artifact.annotations`. It is **never
part of identity** (identity is `relpath`; annotations don't appear there). Details in
[08](08-runtime-and-cluster.md) §6.

```python
@artifact
class FinetunedModel:
    num_gpus: int = 1
    @property
    def annotations(self) -> dict:
        return {"gpus": self.num_gpus, "cpus": self.num_gpus * 8,
                "memory": f"{self.num_gpus * 96}G", "slurm": {"partition": "general"}}
```

A small helper (`gpu_annotations(gpus, partition=...)`) covers the common GPU-budget shape so the
boilerplate isn't copy-pasted.

---

## 8. The Artifact API surface (summary)

A subclass may define:

| Member | Required? | Purpose |
|--------|-----------|---------|
| dataclass fields | yes | configuration = parameters + dependency edges |
| `relpath` (attr or `@property`) | recommended | identity + local location ([02](02-identity-and-storage.md)) |
| `construct(self) -> None` | optional | build the output into `self.path`; omit for external |
| `retrieve(self, *, only=None) -> None` | optional | override fetch (externals; custom backends); may fall back to full fetch |
| `exists(self) -> bool` | optional | override existence check |
| `commit(self) -> None` | optional | override persistence |
| `@derived` methods | optional | cheap reads of the output as `Future` ([05](05-derived-and-futures.md)) |
| `annotations` (attr/`@property`) | optional | executor hints/metadata; never identity |

The framework provides on every Artifact: `self.relpath`, `self.path`, `dependencies()`, and the
storage defaults. `@artifact(store=...)` chooses the Store those defaults operate against without
requiring custom lifecycle methods. Any `meta.json` is ordinary, user-authored output rather than
framework state.

---

## 9. Summary

- An Artifact is an **`@artifact` frozen `kw_only` dataclass**; fields are config + dependency edges.
- **`relpath`** (pure function of config) is the identity and local location; **no content hash**.
- Dependencies are **Artifact-valued fields**, read inside `construct` via `self.dep.path`.
- The storage contract is **`retrieve(only=...)`/`exists`/`commit`** with working defaults against
  either the executor Store or an `@artifact(store=...)` override;
  **`construct(self)` writes to `self.path`**; external artifacts omit `construct` and override
  `retrieve` (via `source.*`).
- `@artifact(automaterialize=, autocommit=, cache=, store=)` tunes lifecycle/storage policy;
  **annotations** are separate and never identity.

Next: [02-identity-and-storage.md](02-identity-and-storage.md).
