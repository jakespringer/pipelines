# Roadmap, Conformance, and Traceability

**Specifies:** the build sequence, the examples-as-conformance suite, the test strategy, the
design→implementation traceability table, and resolutions of the design's OPEN items.
**Modules:** none (cross-cutting).
**Depends on:** all of `docs/00-10`.

---

## 1. Phased implementation roadmap

Each milestone names a concrete pass/fail bar tied to an example group. Build in order.

### M1 — Local core
**Build:** `artifact.py`, `identity.py`, `futures.py` (`scan_dependencies` only),
`store/{base,file}.py`, `runtime.py` (`_CTX`/`ctx`, `workspace`, `slug`, `free_port`, `sh`),
`execution/{materialize,graph}.py`, `execution/executors/local.py`, `worker.py`, `project.py`,
`cli.py` (`run`/`dryrun` only).
**Bar:** `examples/test` `variants` and `merge` groups run end-to-end under `LocalExecutor` against a
temp `file://` store; `dryrun` prints deterministic `relpath`s; re-running skips committed artifacts;
`AuditMarker`-style `cache=False` rebuild path exercised at least by a unit test.

### M2 — Futures & derived
**Build:** `@derived`, `Future` (+ coercion + memoization), combinators
(`fmap`/`gather`/`argmax`/`argmin`/`select`), `resolve_future_fields` (M-2 clone).
**Bar:** `examples/test` `selection` group passes: `argmax` picks the most-unique `WordIndex`;
`WinnerReport` (auto) sees a concrete `self.best`; `ManualWinnerReport` (`automaterialize=False`)
resolves by hand with partial retrieve; `PublishedBundle` (`autocommit=False`) commits by hand with a
user `meta.json`; `AuditMarker` (`cache=False`) always rebuilds.

### M3 — Sessions & remote storage
**Build:** `session.py`, `store/{gs,wandb,http}.py` (gs atomic manifest), `sources.py`
(`hf`/`url`/`gs`), `runtime.gs`/`fetch`.
**Bar:** `examples/test` `Preview`/`PreviewSession` shares one open session across a run; a `gs://`
round-trip commits/retrieves with manifest finalize (half-copied dir reads not-committed); `examples/em`
`JudgeEngine` in-process session path validated (members reuse one loaded model); cross-store
dependency (file→gs) retrieves correctly.

### M4 — Cluster
**Build:** `execution/executors/{parallel,slurm}.py`, `execution/{batch,annotations}.py`, stateless
`status`/`cancel` in `cli.py`, full `dryrun` sbatch plan.
**Bar:** `examples/em` `dryrun baseline`/`scaling` emits correct topological tiers, `afterok` edges,
array jobs per `(class, resource-profile)`, and opaque `hash(project, relpath)` job-names; a
future-field consumer (if added) wires `afterok` to all candidates with no resolver job;
`ParallelExecutor(slots=...)` builds independent `examples/test` artifacts concurrently;
`status` reconstructs job↔artifact purely from graph + `squeue` with no stored map.

### M5 — Full CLI & polish
**Build:** `logs`/`ls`/`inspect`/`lineage`/`rm`/`gc`/`viz`, `--json`, exit codes, packaging + console
script, docs/tests.
**Bar:** `examples/em` runs end-to-end on Slurm (or a documented dry path on machines without Slurm);
the full conformance checklist (§2) is green; `pipelines` console script and `python -m pipelines.worker`
work from an installed package.

---

## 2. Conformance suite — examples as the spec of record

`examples/test` is the integration suite (tiny inputs, fast, hermetic). Its README lists 13 features;
each maps to the module(s) that must satisfy it:

| `examples/test` feature | Appears in | Satisfying module(s) |
|---|---|---|
| Given-artifact retrieval + `retrieve(only=...)` | `LocalDocument` | [04 sources](04-sources.md), [03 store](03-storage-backends.md) |
| Basic dependency construction + annotations | `NormalizedText`, `WordIndex` | [01 artifact](01-core-artifact.md), [06 annotations](06-execution.md) |
| Temporary scratch workspace | `MergedText` | [08 workspace](08-runtime-helpers.md) |
| Artifact-level storage override | `WordIndex` (`local_index_store`) | [01 §3](01-core-artifact.md), [03 §5](03-storage-backends.md) |
| Derived values + `argmax` futures | `WordIndex.unique_words`, selection | [05 futures](05-futures-derived.md) |
| Default automatic future materialization | `WinnerReport` | [05 §5](05-futures-derived.md), [06 §1](06-execution.md) |
| Explicit future resolution + partial materialization | `ManualWinnerReport` | [05 §5](05-futures-derived.md), [03 §1](03-storage-backends.md) |
| Shared session state | `PreviewSession`/`Preview` | [07 sessions](07-sessions.md) |
| Explicit commit + user `meta.json` | `PublishedBundle` | [03 §4](03-storage-backends.md), [02 §7](02-identity-paths.md) |
| Cache bypass | `AuditMarker` (`cache=False`) | [06 §1](06-execution.md) |
| System TOML project configuration | `Project.config` | [10 config](10-config-project-packaging.md) |
| Automatic GCS upload/retrieve | `remote_store` profile | [03 gs](03-storage-backends.md) |
| Integration target for the framework | whole project | all |

`examples/em` is the realistic end-to-end target and exercises, additionally: HF external source
(`HFModel` → `source.hf`); `@artifact(store=)` for large checkpoints alongside GCS outputs; `run()`
dict-arg generation (`ModelGenerations` → `inference.scripts.generate`); in-process `Session` judge
(`JudgeEngine`); `kw_only` inheritance (`Prompts` → `RawPrompts`/`SplitPrompts`); tuple fan-in
(`MisalignmentReport`); `cli(groups=)` + `SlurmExecutor`; opaque job-names.

---

## 3. Public-symbol checklist (every import in the examples has a home)

| Symbol | Imported as | Home doc |
|--------|-------------|----------|
| `Project` | `from pipelines import Project` | [10](10-config-project-packaging.md) |
| `artifact` | `from pipelines import artifact` | [01](01-core-artifact.md) |
| `derived` | `from pipelines import derived` | [05](05-futures-derived.md) |
| `source` | `from pipelines import source` | [04](04-sources.md) |
| `Session` | `from pipelines import Session` | [07](07-sessions.md) |
| `workspace` | `from pipelines import workspace` | [08](08-runtime-helpers.md) |
| `argmax` | `from pipelines import argmax` | [05](05-futures-derived.md) |
| `cli` | `from pipelines import cli` | [09](09-cli.md) |
| `LocalExecutor` | `from pipelines import LocalExecutor` | [06](06-execution.md) |
| `SlurmExecutor` | `from pipelines import SlurmExecutor` | [06](06-execution.md) |
| `slug` | `from pipelines.runtime import slug` | [02 §3](02-identity-paths.md)/[08 §9](08-runtime-helpers.md) |
| `run` | `from pipelines.runtime import run` | [08 §5](08-runtime-helpers.md) |
| `Future`, `fmap`, `gather`, `argmin`, `select`, `ParallelExecutor`, `BatchPolicy`, `ctx`, `sh`, `torchrun`, `free_port`, `gs`, `fetch`, `gpu_annotations`, `publish_atomic`, `write_meta` | (design docs) | as in [00 §4](00-implementation-overview.md) |

---

## 4. Test strategy

- **Unit (no executor):** pure `steps.py` modules (already framework-agnostic in both examples);
  `identity` (`slug`, `validate_relpath`, `fingerprint`, `check_collisions`); `run()` arg-formatting
  rules ([08 §5](08-runtime-helpers.md)) as a table-driven test; `FileStore` atomicity (concurrent
  `put_dir`); annotations resolution levels.
- **Integration:** temp `file://` store + `LocalExecutor` running each `examples/test` group; assert on
  output files and `relpath`/`path` determinism (diff `dryrun` across two imports).
- **Cluster (M4/M5):** `dryrun` golden-file tests for `examples/em` sbatch headers / `afterok` / array
  grouping / job-name hashing; `status` reconciliation against a fake `squeue`.
- The examples are the **spec of record**: a change that breaks an example's documented behavior is a
  spec violation, not just a test failure.

---

## 5. Design OPEN items — spec resolutions

| OPEN (design) | Resolution in this spec |
|---|---|
| `design/01 §5.2` external `exists()` semantics ("in store" vs "on hub") | Default `exists()` checks the selected Store, then local `base_path`; an external may override to probe the hub. ([01 §4](01-core-artifact.md)) |
| `design/01 §6` decorator-with-args ergonomics | `artifact(cls=None, /, **settings)` returns a decorator when called with kwargs; bare and `@artifact(...)` both work. ([01 §1](01-core-artifact.md)) |
| `design/08 §6` multi-node training as one Artifact | `annotations` request `nnodes>1`; `SlurmExecutor` emits `--nodes`; `torchrun` reads `nnodes` from `ctx`. Graph stays one-artifact-per-stage. ([08 §6](08-runtime-helpers.md)) |

---

## 6. Traceability — design → implementation

| design/ | docs/ home |
|---------|------------|
| 00 overview | [00](00-implementation-overview.md) |
| 01 artifacts | [01](01-core-artifact.md) |
| 02 identity-and-storage | [02](02-identity-paths.md) |
| 03 construction | [01 §3](01-core-artifact.md) (`self.path`), [08](08-runtime-helpers.md) (helpers), [06 §1](06-execution.md) (brackets) |
| 04 retrieval-and-storage | [03](03-storage-backends.md), [04](04-sources.md) |
| 05 derived-and-futures | [05](05-futures-derived.md) |
| 06 execution | [06](06-execution.md) |
| 07 sessions | [07](07-sessions.md) |
| 08 runtime-and-cluster | [08](08-runtime-helpers.md), [06 annotations](06-execution.md) |
| 09 cli-and-observability | [09](09-cli.md) |
| 10 configuration | [10](10-config-project-packaging.md) |
| 11 comparison-and-migration | covered by the "port from `experiments`" notes in each module doc + [00 §3](00-implementation-overview.md) |

Every design claim has a *how* in some `docs/` doc; no design feature is unspecified.

Back to [00-implementation-overview.md](00-implementation-overview.md).
