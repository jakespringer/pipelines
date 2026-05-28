# Implementation Overview

> **What this is.** `design/` answers *what the user writes and why*. `docs/` (this set) answers
> *how we build it* — a buildable blueprint organized by the modules an engineer creates. Each doc
> cites the design it satisfies (`design/NN`) and never restates rationale; read the paired design
> doc for the contract, then this for the construction.

**Specifies:** all of `design/00-11` (entry).
**Modules:** the whole `pipelines/` package.
**Depends on:** nothing.
**Milestone:** spans M1–M5 (see [11](11-roadmap-and-conformance.md)).

---

## 1. Reading order

| Doc | Module(s) | Milestone |
|-----|-----------|-----------|
| [01-core-artifact](01-core-artifact.md) | `pipelines/artifact.py` | M1 |
| [02-identity-paths](02-identity-paths.md) | `pipelines/identity.py` | M1 |
| [03-storage-backends](03-storage-backends.md) | `pipelines/store/` | M1 (`file`), M3 (`gs`/`wandb`/`http`) |
| [04-sources](04-sources.md) | `pipelines/sources.py` | M1 (`local`), M3 (`hf`/`url`/`gs`) |
| [05-futures-derived](05-futures-derived.md) | `pipelines/futures.py` | M1 (scan), M2 (`@derived`/combinators) |
| [06-execution](06-execution.md) | `pipelines/execution/`, `worker.py` | M1 (`materialize`/`graph`/local), M4 (parallel/slurm) |
| [07-sessions](07-sessions.md) | `pipelines/session.py` | M3 |
| [08-runtime-helpers](08-runtime-helpers.md) | `pipelines/runtime.py` | M1 (`ctx`/`workspace`/`slug`), M3+ (`run`/`gs`/`torchrun`) |
| [09-cli](09-cli.md) | `pipelines/cli.py` | M1 (`run`/`dryrun`), M4 (`status`/`cancel`), M5 (rest) |
| [10-config-project-packaging](10-config-project-packaging.md) | `pipelines/project.py`, packaging | M1 (project), M5 (packaging) |
| [11-roadmap-and-conformance](11-roadmap-and-conformance.md) | — | all |

---

## 2. Package layout

```
pipelines/
  __init__.py            # public re-exports (§4)
  artifact.py            # @artifact decorator, ArtifactSettings, injected members
  identity.py            # relpath validation, slug, config fingerprint, collision check
  futures.py             # Future, @derived, combinators, dependency scan
  sources.py             # source.hf / url / gs / local
  session.py             # Session base class
  project.py             # Project.init, layered TOML, Project.config
  runtime.py             # ctx, workspace, run, sh, torchrun, free_port, gs, fetch, slug, gpu_annotations
  cli.py                 # cli(), verbs, selectors, stateless reconcile
  worker.py              # python -m pipelines.worker
  store/
    __init__.py          # from_uri(), publish_atomic, write_meta re-exports
    base.py              # Store ABC, registry
    file.py              # file:// backend
    gs.py                # gs:// backend
    wandb.py             # wandb:// publish-mirror
    http.py              # http(s):// / hf:// read-only
  execution/
    __init__.py
    materialize.py       # the one primitive
    graph.py             # collect / toposort / freshness
    batch.py             # BatchPolicy, array grouping
    annotations.py       # 3-level resolution, portable->backend mapping
    executors/
      __init__.py
      local.py           # LocalExecutor
      parallel.py        # ParallelExecutor
      slurm.py           # SlurmExecutor
```

### Module dependency graph (lower depends on higher; no cycles)

```
identity  ──┐
futures   ──┼──> artifact ──> store ──> execution ──> cli
runtime   ──┘                   ▲          ▲   ▲
project ────────────────────────┘          │   │
sources ───────────────────────────────────┘   │
session ────────────────────────────────────────┘
```

- `identity`, `runtime`, `futures` have no intra-package deps (futures imports `identity` lazily for
  the dependency scan; keep the import inside functions to avoid a cycle with `artifact`).
- `artifact` imports `identity`, `futures`, `runtime`, `store` (for default lifecycle).
- `execution` imports everything below it; `cli` is the top.
- **Cycle-avoidance rule:** `artifact ↔ futures` would cycle (a field may be a `Future`; a `@derived`
  lives on an artifact). Break it by having `futures` reference artifacts only via duck-typing
  (`.relpath`, `.retrieve`, `.path`) and import `artifact` lazily inside the few functions that need
  `isinstance`. `dependencies()` lives in `futures.py` and is attached to artifacts by the decorator.

---

## 3. Cross-cutting mechanics (stated once; every doc references these)

These six decisions are non-obvious and, if left implicit, make the docs inconsistent. Each is
defined in the listed home doc; everywhere else cites it.

### M-1 · `self.path` resolution via a runtime contextvar  *(home: [01](01-core-artifact.md) §3)*
`relpath` is a pure function of config and always available. `self.path` is a **framework-injected
`@property`** that resolves `relpath` against the **active runtime context's `base_path`**, held in a
`contextvar` `pipelines.runtime._CTX`. The executor (local) or `worker.py` (cluster) sets `_CTX`
around each `materialize` call. Accessing `self.path` with no active context raises
`RuntimeContextError` ("self.path requires an active executor; relpath=…"). This is the documented
"needs an executor" dependency of `design/02 §2` — `relpath` stays pure; only the absolute
resolution needs the executor. A dependency's output is read the same way: `self.dep.path` resolves
against the same `_CTX.base_path`.

### M-2 · Future-valued field resolution clones the artifact  *(home: [05](05-futures-derived.md) §4)*
Artifacts are frozen, so resolution cannot mutate a field. When `automaterialize=True`, before
`construct` the framework walks the instance's fields, resolves any `Future` value (`.result()`),
retrieves the chosen artifact, then produces a **resolved clone** via
`dataclasses.replace(self, field=resolved_artifact)` and runs `construct`/`@derived` on the clone — so
`self.best.path` is a concrete directory (the `WinnerReport` case). When `automaterialize=False`, the
field keeps its `Future`; the body calls `self.best.result()` itself (the `ManualWinnerReport` case).
`relpath` is computed from the **original** instance (stable identity); only `construct` sees the
clone.

### M-3 · `@derived` is a per-instance lazy `Future`  *(home: [05](05-futures-derived.md) §1–2)*
`@derived(reads=...)` is a descriptor; `instance.method` returns a `Future[T]` bound to
`(artifact, reads, body)`. Resolving it ensures the output is local — when `automaterialize=True` it
calls `self.retrieve(only=reads)` first — then runs the body; the value is memoized per run in a
contextvar cache keyed by `(relpath, method)`. `Future` implements `__float__`, `__int__`, `__bool__`,
the rich comparisons, and `__repr__` for eager-feeling analysis; `.result()` forces explicitly.
Combinators (`fmap`/`gather`/`argmax`/`argmin`/`select`) build new futures and **never hash the key
callable** (v2 has no fingerprint).

### M-4 · Settings carrier + decorator-with-args  *(home: [01](01-core-artifact.md) §1–2)*
`@artifact(...)` records `store`, `session`, `automaterialize`, `autocommit`, `cache`, `retries`,
`env` on a class attribute `__pipelines__: ArtifactSettings` (a frozen dataclass of defaults).
Supports both bare `@artifact` and `@artifact(cache=False)` via the standard
decorator-returning-decorator pattern. A class-creation guard scans `__annotations__` and raises if a
reserved name (`relpath`, `path`, `dependencies`, `retrieve`, `exists`, `commit`, `materialize`,
`annotations`, `session`) is declared as a **field** (those must be `@property`/method, never config).

### M-5 · Store selection is policy, never identity  *(home: [03](03-storage-backends.md) §1, §5; design/04)*
`store=` accepts a URI string, a `Store` instance, or a **zero-argument callable** returning either
(resolved lazily in the runtime/project context, after `Project.init`, so machine-specific
`Project.config` values never contaminate the graph). It is **not** a dataclass field and never enters
`relpath`. Default lifecycle (`exists`/`retrieve`/`commit`) operates against the selected Store; the
local `base_path` (scratch where `construct` writes and deps materialize) is separate from the durable
Store.

### M-6 · Stateless job identity  *(home: [06](06-execution.md) §5, [09](09-cli.md) §4)*
`--job-name = <hash(project)>:<hash(project, relpath)>` (opaque hex; SHA-256 truncated). Array groups
use `<hash(project)>:<hash(array-group-id)>`, where `array-group-id` is derived from the **member set**
so a stale array cannot be misread; array index → `relpath` is recovered by re-deriving the
deterministic member ordering. There is **no stored job map**: `status`/`cancel` re-import the
project, recompute hashes, hold the reverse map in memory, and reconcile against `squeue`/`sacct`.

---

## 4. Public API surface (what `__init__` re-exports)

```python
# pipelines/__init__.py
from .project import Project
from .artifact import artifact
from .futures import derived, Future, fmap, gather, argmax, argmin, select
from .sources import source
from .session import Session
from .runtime import workspace            # also importable from pipelines.runtime
from .cli import cli
from .execution.executors.local import LocalExecutor
from .execution.executors.parallel import ParallelExecutor
from .execution.executors.slurm import SlurmExecutor
from .execution.batch import BatchPolicy
```

```python
# pipelines/runtime.py exports
ctx, run, sh, torchrun, free_port, gs, fetch, slug, gpu_annotations, workspace
# pipelines/store/__init__.py exports
from_uri, publish_atomic, write_meta
```

> **Source-of-truth check.** Every symbol imported by `examples/test` and `examples/em` must appear
> here with a specified home. The import lines to satisfy:
> `from pipelines import Project, Session, artifact, derived, source, workspace`,
> `from pipelines import argmax`, `from pipelines import LocalExecutor, Project, cli`,
> `from pipelines import Project, cli, SlurmExecutor`,
> `from pipelines.runtime import slug, run`. The full per-symbol checklist is in
> [11 §3](11-roadmap-and-conformance.md).

---

## 5. Environment and dependencies

- **Python ≥ 3.11** (`tomllib` in stdlib; `typing.dataclass_transform`; `dataclasses(kw_only=)`).
- **Required runtime deps:** `google-cloud-storage` (gs backend), `huggingface_hub` (`source.hf`).
- **Optional extras:** `wandb` (`wandb://` mirror), `rich` (CLI tables/logs), `networkx` + a Graphviz
  binary (`viz`), `pandas` (`gather(...).to_frame()`), `tomli` only if a 3.10 fallback is ever needed
  (default target is 3.11+, so stdlib `tomllib`).
- **No DSL dependency, no flock files.** Atomicity is store-native (rename / manifest); see
  [03 §3](03-storage-backends.md).

Packaging details (pyproject, console script `pipelines`, `python -m pipelines.worker`, `src/` vs flat
layout) are in [10 §3](10-config-project-packaging.md).

---

## 6. Conventions used across these docs

- **Pseudocode** is Python-shaped but elides error handling unless the error is part of the contract.
- **"Port from `experiments`"** means adapt the cited `experiments/<file>:<line>` logic, not copy
  verbatim; the porting table is in the plan and repeated per module.
- Signatures shown are the **committed** public surface (must match the examples); private helpers are
  named with a leading `_` and may change.
- Each doc ends with a **conformance hook** listing which `examples/` features it must satisfy.
