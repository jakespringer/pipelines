# Pipelines v2 — Design Overview

> A Pythonic library for describing experiments as **Artifacts** — configurations that know
> their human-readable location, how to construct themselves, and how to be retrieved and
> committed — and running their dependency graph effortlessly, locally or on a Slurm cluster.

This is the entry point to the **v2** design. v2 keeps the proven core of `experiments`
(config-keyed artifacts, dependency-as-field, content skip/resume, multi-backend execution) and
the v1 redesign's wins (plain-Python construction, directory outputs, a real storage abstraction),
but reworks identity and the artifact contract around a single idea: **`relpath`**.

> v1 (the previous design) lives in [`../v1/`](../v1). The rationale for every change from v1 is in
> [11-comparison-and-migration.md](11-comparison-and-migration.md).

---

## 1. The motivating problem

We run large experimental pipelines — pretrain, prepare data, finetune, generate, judge, analyze —
whose stages form a dependency graph. We want to: describe each stage as a configuration; wire
stages by referencing one another; write construction logic as ordinary Python; not think about
where outputs live or how they move; checkpoint and resume; run independent stages in parallel,
locally or across a cluster; keep intermediate state interpretable (browsable, hand-editable); and
be fast (wiring the graph is instant; slow work runs only when needed).

---

## 2. The core idea: an Artifact, anchored on `relpath`

Everything is an **Artifact**: a frozen dataclass (declared with `@artifact`) whose fields *are*
its configuration. An Artifact has one anchor and four behaviors:

```
                       ┌─────────────────────────────────────────┐
                       │                ARTIFACT                  │
                       │        @artifact frozen dataclass          │
                       ├─────────────────────────────────────────┤
   anchor   ──────────▶│ relpath   the human-readable relative    │  identity + local location
                       │           path it materializes at        │
                       ├─────────────────────────────────────────┤
   build    ──────────▶│ construct(self)   write output to        │  plain Python; omit for
                       │                   self.path              │  external/given artifacts
                       ├─────────────────────────────────────────┤
   fetch    ──────────▶│ retrieve(self, *, only=None) make output │  default: download all or
                       │                   local at self.path     │  selected files from store
                       ├─────────────────────────────────────────┤
   check    ──────────▶│ exists(self)      is it committed?       │  scheduler-only; may be costly
                       │                   (skip decision)        │
                       ├─────────────────────────────────────────┤
   persist  ──────────▶│ commit(self)      upload self.path to    │  default: atomic publish
                       │                   durable storage        │
                       └─────────────────────────────────────────┘
```

- **`relpath` is the anchor and the identity.** It's a human-readable relative path (a plain
  attribute or `@property`, a pure function of config) where the artifact is *promised to
  materialize locally*. It also organizes durable storage by default. Two artifacts with the same
  `relpath` are the same artifact. There is **no content hash / fingerprint** (see
  [02](02-identity-and-storage.md)).
- **`construct(self)`** is plain Python that writes the output into **`self.path`** (the absolute
  local resolution of `relpath`, supplied by the executor). It returns nothing. Absent ⇒ the
  artifact is *external/given* (e.g. a HF model) and only needs `retrieve`.
- **`retrieve` / `exists` / `commit`** are the storage contract; the framework provides working
  **defaults** (backed by the executor's default Store, or an artifact-selected Store) so the
  common case writes only `construct` + `relpath`.
  `retrieve(only=...)` enables partial materialization; an override may satisfy that request by
  retrieving the whole artifact if its source cannot fetch subsets efficiently.

A concrete taste:

```python
from pathlib import Path
from pipelines import Project, artifact, source

@artifact
class HFModel:                                   # external/given: no construct
    repo: str
    revision: str = "main"
    @property
    def relpath(self) -> str: return f"hf/{self.repo}@{self.revision}"
    def retrieve(self, *, only=None) -> None:
        source.hf(self.repo, self.revision, into=self.path, only=only)

@artifact(store=lambda: Project.config.local_model_store)
class FinetunedModel:
    base: HFModel                                # ← dependency edge
    data: Dataset                                # ← dependency edge
    lr: float
    @property
    def relpath(self) -> str:
        return f"model/{self.base.repo}/{self.data.name}/lr{self.lr:.0e}"
    def construct(self) -> None:                 # plain Python; writes into self.path
        finetune(self.base.path, self.data.path, self.lr, out=self.path)
# This class commits to its configured file:// Store; ordinary outputs can use
# the executor's default gs:// Store. Both retain atomic commit/retrieve defaults.
```

You build a pipeline by instantiating Artifacts that reference one another, then handing the final
ones to an executor. Instantiation runs nothing — it's a description.

### 2.1 Derived values & "best of a set"

An Artifact may expose **derived values** with `@derived`: cheap reads of its own output, returned
as a lazy `Future[T]` that auto-materializes when used. You can select over a set
(`argmax(candidates, key=lambda m: m.metric)`), and a `Future` may be a config field
("finetune the best candidate"). With the default `automaterialize=True`, future-valued fields
resolve **at the consumer's runtime**, just before `construct`; with `automaterialize=False`, the
author explicitly resolves/materializes them in the body. Full treatment in
[05](05-derived-and-futures.md).

---

## 3. Design principles

1. **An Artifact is a description, not an action.** Instantiating one runs nothing; building the
   graph is pure and fast.
2. **`relpath` is the one anchor.** Identity, local materialization, and default storage layout all
   key off the same human-readable path. No second artifact-address index; a backend may keep
   private publication state needed for atomic commit detection.
3. **Construction is plain Python.** `construct(self)` writes files into `self.path`. No DSL.
4. **Storage is a contract with defaults.** `retrieve`/`exists`/`commit` have working defaults;
   most artifacts use the executor's Store, while `@artifact(store=...)` selects an alternate
   Store for a class (for example local large checkpoints while evaluation outputs use GCS).
5. **Clobbering is intentional; no staleness magic.** Re-running overwrites the output at the same
   `relpath`, including after code changes. There is no fingerprint and no auto-invalidation; you
   refresh by deleting or varying `relpath`. (See [02](02-identity-and-storage.md) §3.)
   Two non-equal artifacts in the same graph may not claim that same path: this is a graph-build
   error, not an overwrite.
6. **One dependency model, computed once.** The DAG comes from config composition (artifact-valued
   fields). Scheduling, batching, sessions, and selection are *layers over* it, never forks.
7. **The cluster is an executor, not a rewrite.** Laptop → Slurm changes the executor, not the
   Artifact definitions.

---

## 4. What's new vs v1 (one-line each)

| Area | v1 | v2 |
|------|----|----|
| Identity | content hash (`key`) + optional `name` template | **`relpath`** is the identity (human-readable, author-controlled) |
| Staleness | versioning + freshness | **none** — clobbering is intentional ([02](02-identity-and-storage.md)) |
| Base class | `class X(Artifact)` | **`@artifact`** decorator (frozen `kw_only` dataclass + footgun guards) |
| Contract | `construct(self, out)` + auto Store | **`construct(self)→self.path`** + `retrieve(only=...)`/`exists`/`commit` (defaults provided) |
| Per-artifact storage | manual storage code | **`@artifact(store=...)`** keeps defaults while selecting `file://`/`gs://` per class |
| Externals | `Source` + `locate()` hook | **`source.*` plain helpers** inside `retrieve()` ([04](04-retrieval-and-storage.md)) |
| Selection | `Future` w/ symbolic keys + resolver jobs | `Future` resolved **at consumer runtime when automaterialized** ([05](05-derived-and-futures.md)) |
| Shared setup | `@lru_cache` + accidental co-location | first-class **`Session`** (server per machine) ([07](07-sessions.md)) |
| CLI targets | `cli(default_target=...)` | **`cli(groups={...})`** — groups are aliases for artifact lists ([09](09-cli-and-observability.md)) |
| Job tracking | `launched_jobs.json` | stateless — **`--job-name = hash(project, relpath)`** ([06](06-execution.md), [09](09-cli-and-observability.md)) |
| Config | `pipelines.toml` | **`Project.config.<key>`** from layered TOML, including a per-project system file, + escape hatch ([10](10-configuration.md)) |

---

## 5. Glossary

- **Artifact** — a frozen dataclass (`@artifact`) whose fields are its configuration; the unit.
- **`relpath`** — the human-readable relative path that is the artifact's identity and local
  materialization location (and default storage layout). A pure function of config.
- **`self.path`** — the absolute local path = `<base_path>/<relpath>`; `<base_path>` is supplied by
  the executor. Where `construct` writes, `retrieve` downloads to, `commit` uploads from.
- **`construct(self)`** — plain-Python build step; writes the output into `self.path`. Omitted for
  external artifacts.
- **`retrieve(only=...)` / `exists` / `commit`** — the storage contract (defaults provided): make
  all or selected contents local / is-it-committed (scheduler-only) / persist atomically.
- **`@artifact`** — the class decorator: applies `@dataclass(frozen=True, kw_only=True)`, guards
  footguns, carries `automaterialize`/`autocommit`/`cache`/optional `store`, and provides the
  four defaults.
- **`Project.config`** — an arbitrary-key attribute view loaded from layered TOML; system-local
  project values such as bucket and filesystem roots live in
  `~/.config/pipelines/projects/<project>.toml`.
- **External / given artifact** — no `construct`; overrides `retrieve` (often via `source.*`).
- **`@derived`** — a cheap read of an artifact's output, returned as a lazy `Future[T]`.
- **`Future[T]`** — a lazy promise of a value/artifact; resolves on use (analysis) or, when
  `automaterialize=True`, before `construct` for a future-valued config field.
- **Session** — a first-class shared resource (e.g. a server, one per machine) opened once per
  batch job and queried by member jobs ([07](07-sessions.md)).
- **Group** — a *named alias for a list of artifacts*; overlapping/composable; the CLI selection
  unit ([09](09-cli-and-observability.md)).
- **Executor** — materializes Artifacts and supplies `base_path`: `LocalExecutor`,
  `ParallelExecutor`, `SlurmExecutor`.
- **Optional metadata file** — an artifact may write `meta.json` (or anything else) into its output
  for provenance. The framework neither writes nor requires it; committed existence comes from the
  store contract, never from a user metadata file.

---

## 6. Document map

| # | Document | What it nails down |
|---|----------|--------------------|
| 00 | **overview.md** (this) | the Artifact, `relpath` anchor, principles, v1→v2, glossary |
| 01 | [artifacts.md](01-artifacts.md) | `@artifact`, fields-as-config, dependencies, the four-function contract, externals, annotations |
| 02 | [identity-and-storage.md](02-identity-and-storage.md) | `relpath`=identity, no fingerprint, collision errors, paths, layout, optional metadata |
| 03 | [construction.md](03-construction.md) | `construct(self)→self.path`, `automaterialize`/`autocommit`, workspaces, escape hatches, testing |
| 04 | [retrieval-and-storage.md](04-retrieval-and-storage.md) | `retrieve`/`exists`/`commit` + defaults, partial retrieve, atomic publish, `source.*`, backends |
| 05 | [derived-and-futures.md](05-derived-and-futures.md) | `@derived` (lazy + coercion, `reads=`), `Future`, combinators, future-valued fields |
| 06 | [execution.md](06-execution.md) | the `materialize` primitive, executors, ordering, batching, job-name hashing, failure, dryrun |
| 07 | [sessions.md](07-sessions.md) | shared server per machine, `group_key`/`open`/`close`, node-alloc-per-group |
| 08 | [runtime-and-cluster.md](08-runtime-and-cluster.md) | `workspace()`, `ctx`, `source.*`, `free_port`/`torchrun`/`sh`, annotations |
| 09 | [cli-and-observability.md](09-cli-and-observability.md) | `cli(groups=)`, selectors, `status`/`cancel`, stateless job mapping, lineage, viz |
| 10 | [configuration.md](10-configuration.md) | `Project.config`, layered project/system TOML, project→sub-project discovery, escape hatch |
| 11 | [comparison-and-migration.md](11-comparison-and-migration.md) | vs `experiments`, vs v1, migration |

> **Status:** design documents, not an implementation. Code shows intended API; signatures may
> shift. A handful of minor open items are flagged **OPEN**.
