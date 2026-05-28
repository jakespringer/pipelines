# Comparison and Migration

v2 has two reference points: **`experiments`** (the ancestor library) and **v1** (the previous
design in [`../v1/`](../v1)). This document maps both, states what changed and why, and sketches
migration.

---

## 1. v1 → v2: what changed and why

v2 keeps v1's best bets — plain-Python construction, directory-only outputs, no codec registry,
multi-backend execution, the "cluster is an executor" model — and reworks the rest around
**`relpath`**.

| Topic | v1 | v2 | Why |
|-------|----|----|-----|
| **Identity** | content hash `key` + optional `name` template | **`relpath`** is the identity (human-readable, author-controlled) | v1's key-vs-name split was contradictory (Store keyed by `key` but laid out by `name`, "no index" yet needs one); collision-suffixing made paths nondeterministic. One anchor fixes all of it. ([02](02-identity-and-storage.md)) |
| **Staleness** | version tags / freshness | **none** — clobbering intentional | v1's `version` didn't track the *imported code* a `construct` calls, so it silently reused stale cache anyway. v2 makes overwrite the explicit model; refresh by delete / vary `relpath`. ([02](02-identity-and-storage.md) §3) |
| **Base class** | `class X(Artifact)` | **`@artifact`** decorator | one decorator (frozen `kw_only` dataclass), guards the `ClassVar`/required-field footguns, carries settings. ([01](01-artifacts.md)) |
| **Contract** | `construct(self, out)` + automatic Store | **`construct(self)→self.path`** + `retrieve(only=...)`/`exists`/`commit` (defaults) | explicit, overridable lifecycle anchored on `relpath`; `self.path` resolves the v1 ambient-`.path()` ambiguity into pure `relpath` + executor-supplied `base_path`. ([01](01-artifacts.md), [03](03-construction.md)) |
| **Externals** | `Source` type + `locate()` hook | **`source.*` plain helpers** in `retrieve()` | one fewer framework concept; an external is just an artifact with no `construct`. ([04](04-retrieval-and-storage.md) §6) |
| **`commit`** | automatic only | default = atomic publication; optional user metadata; **overridable** (helpers) | supports W&B/registry targets without forcing provenance files into every output. ([04](04-retrieval-and-storage.md) §4) |
| **Store policy** | one automatic Store path or hand-coded upload behavior | executor default plus **`@artifact(store=...)`** alternate Store | large local checkpoints and GCS-backed result files keep the same default atomic lifecycle. ([04](04-retrieval-and-storage.md) §5) |
| **Derived/selection** | `Future` w/ symbolic keys, `resolved_futures` freshness, Slurm resolver jobs | `@derived` lazy + coercion; future-fields resolve before `construct` when `automaterialize=True` | no-fingerprint identity removes the need to hash selection lambdas, to split identity, and to dynamically submit a resolver job. Much lighter. ([05](05-derived-and-futures.md)) |
| **Shared setup** | `@lru_cache` + accidental same-process | first-class **`Session`** (server per machine) | guarantees the amortization the `lru_cache` trick silently needed; adds isolation + parallelism. ([07](07-sessions.md)) |
| **CLI targets** | `cli(default_target=...)` (one set) | **`cli(groups={...})`** — groups are aliases for artifact lists | a real project has many overlapping target sets; the unified positional also takes a `relpath` glob. ([09](09-cli-and-observability.md)) |
| **Job tracking** | (deleted `launched_jobs.json`, no replacement) | stateless **`--job-name = hash(project, relpath)`** (opaque) | maps `squeue`→artifact with no stored state, and hides the project from other cluster users. ([06](06-execution.md) §5) |
| **Config** | repo `pipelines.toml` | **`Project.config.<key>`** from layered TOML, including system per-project overlays, + escape hatch | preserves arbitrary project config ergonomics while adding versioned defaults/sub-project inheritance. ([10](10-configuration.md)) |
| **`automaterialize`/`autocommit`** | (implicit/automatic) | **explicit `@artifact` toggles** | lets authors hand-roll dep fetch / commit when needed, without losing the default automation. ([01](01-artifacts.md) §6) |

What v1 → v2 **kept unchanged:** fields-as-config + Artifact-valued dependency edges; plain-Python
`construct`; directory-only outputs and no codec registry; optional user provenance; atomic publish;
local materialization plus selected Stores; `annotations`; multi-backend executors + array-job
grouping; `dryrun`.

---

## 2. `experiments` → v2: concept mapping

| `experiments` | v2 | Notes |
|---------------|----|-------|
| `Artifact` (frozen dataclass) | `@artifact` frozen dataclass | fields = config + deps |
| `construct(self, builder: Task)` | `construct(self) -> None` writing `self.path` | **plain Python**; the builder DSL is gone |
| `builder.run_command(...)` | call your code / `subprocess` / `sh()` | no format-spec string assembly |
| `builder.create_yaml_file/...` | `Path.write_text` / `yaml.dump` into `self.path` | ordinary file writes |
| `builder.upload_to_gs / rsync_to_gs` | `commit()` (default atomic) / `autocommit` | persistence is default, overridable |
| artifact-specific `storage="local"` | `@artifact(store=lambda: Project.config.local_model_store)` | keep large checkpoints in configured `file://` Store while other outputs use GCS |
| `builder.download_* / download_hf_model` | `self.dep.path`; `source.url/hf/gs`; `fetch()` | retrieval is default; read files from a dep's dir |
| `Artifact.relpath` (`ClassName/hash`) | `relpath` (human-readable, the identity) | now the identity, not just a path |
| `Artifact.exists` / `should_skip()` | `exists()` (default: committed at `relpath`) | scheduler-only; default usually suffices |
| dependency = `Artifact` field | dependency = `Artifact` field | **unchanged** |
| (none) | `@derived` / `Future` / `argmax` | data-dependent deps, "best of a sweep" ([05](05-derived-and-futures.md)) |
| `ArtifactSet.from_product/...` | `itertools.product` + comprehensions | plain Python |
| `ArtifactBatch` (+ remapping) | `BatchPolicy` + `Session` (scheduler-only) | graph stays clean ([06](06-execution.md), [07](07-sessions.md)) |
| `BatchedJudgedResponses` hack | a `Session` (server per machine) | the motivating shared-setup case ([07](07-sessions.md)) |
| `executor.stage(...)` / `stage_group(...)` | `cli(groups={...})` (aliases for lists) | overlapping/composable; the targets are the graph |
| `get_requirements()` dict | `annotations` (attr/`@property`) | open, namespaced, never identity |
| `EXPERIMENTS_*_CONF` env vars | `pipelines.runtime.ctx` | explicit, documented |
| `Project.config.<key>` / per-project JSON | `Project.config.<key>` / layered TOML | same arbitrary-key authoring API; system file is `~/.config/pipelines/projects/<name>.toml` |
| `~/.experiments/...jobs.json` | committed-output state in the store protocol; stateless job-names | no required metadata file or machine-global drift |
| CLI `launch/drylaunch/print/...` | `run/dryrun/status/logs/cancel` (+ `inspect/lineage/gc/viz`) | unified selectors |

---

## 3. Migration sketch (an `experiments` stage → v2)

**Before:**
```python
@dataclass(frozen=True)
class PretrainedModel(Artifact):
    learning_rate: float
    num_epochs: int
    def get_requirements(self):
        return {'partition': 'array', 'gpus': 'A6000:4', 'cpus': '8'}
    def construct(self, builder: Task):
        builder.create_yaml_file(f'{builder.artifact_path}/{self.relpath}/c.yaml',
                                 {'lr': self.learning_rate, 'epochs': self.num_epochs})
        builder.run_command(f'python pretrain.py --config {builder.artifact_path}/{self.relpath}/c.yaml')
        builder.upload_to_gs(f'{builder.artifact_path}/{self.relpath}', f'{builder.gs_path}/{self.relpath}')
```

**After:**
```python
@artifact
class PretrainedModel:
    learning_rate: float
    num_epochs: int
    @property
    def relpath(self) -> str:
        return f"pretrain/lr{self.learning_rate:.0e}_ep{self.num_epochs}"
    @property
    def annotations(self) -> dict:
        return {"gpus": "A6000:4", "cpus": 8, "slurm": {"partition": "array"}}
    def construct(self) -> None:
        (self.path / "c.yaml").write_text(yaml.safe_dump(
            {"lr": self.learning_rate, "epochs": self.num_epochs, "out": str(self.path)}))
        sh(f"python pretrain.py --config {self.path / 'c.yaml'}")
    # no exists(), no upload: defaults commit self.path atomically; metadata is opt-in
```

Steps:

1. `class X(Artifact)` → `@artifact`; `@dataclass(frozen=True)` is applied for you (`kw_only`).
2. **`relpath`**: replace `ClassName/hash` with a readable `@property` encoding the
   identity-bearing fields.
3. **`get_requirements()` → `annotations`** (attr or `@property`); Slurm-only keys into a `"slurm"`
   namespace, portable intent (`gpus`/`cpus`/`memory`) at top level.
4. **`construct` body → plain Python** writing into **`self.path`**: `create_*_file` → file writes;
   `run_command(...)` → a call or `sh(...)`; **delete `upload_to_gs`** (default `commit`); read deps
   via `self.dep.path` instead of reconstructing `relpath` strings.
5. **Delete `exists`/`relpath`-string plumbing**; skipping is automatic (existence at `relpath`).
6. **Factor pure logic into `steps.py`** (framework-agnostic, unit-testable); `construct` just calls
   it.
7. **`from_product`/`stage(...)`** → comprehensions building target lists + `cli(groups={...})`.
8. **Config** → `Project.init(...)` + `Project.config.<key>` from layered TOML, or explicit
   executor args in `run.py` ([10](10-configuration.md)).
9. **Selective persistence** → use `@artifact(store=...)` for classes such as local finetuned
   checkpoints; leave ordinary GCS-backed artifacts on the executor Store.
10. **(Optional) new features:** `@derived` metrics, `argmax`-based selection, `Session` for shared
   servers.

---

## 4. Summary

v2 keeps `experiments`' proven core and v1's plain-Python/directory/no-codec wins, and reworks
identity around **`relpath`** — which in turn simplifies storage (one anchor, no artifact-address
index), staleness (none — intentional clobber), selection (automatic resolution when materialized,
no symbolic keys/resolver jobs),
shared setup (first-class `Session`), CLI (`groups` as aliases + `relpath` globs), and job tracking
(stateless opaque job-names). The previous design is preserved in [`../v1/`](../v1).

See [00-overview.md](00-overview.md) to start from the top.
