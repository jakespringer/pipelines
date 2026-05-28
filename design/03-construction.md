# Construction — Building an Artifact in Plain Python

`construct` is the build step and the single biggest ergonomic win over `experiments`: it is an
**ordinary Python method**, not a shell-emitting builder DSL. This document defines how `construct`
is written, where it writes, how it reads dependencies, how `automaterialize`/`autocommit` frame it,
the scratch helper, escape hatches, discipline, and testing.

---

## 1. `construct(self)` writes to `self.path`

```python
@artifact
class PretrainedModel:
    lr: float
    epochs: int
    @property
    def relpath(self) -> str: return f"pretrain/lr{self.lr:.0e}_ep{self.epochs}"

    def construct(self) -> None:
        from myproject.training import train          # plain Python: import and call your code
        train(lr=self.lr, epochs=self.epochs, out=self.path)
```

- **No `out` parameter.** The output location is **`self.path`** — the absolute local resolution of
  `relpath` under the executor's `base_path` ([02](02-identity-and-storage.md) §2). One anchor:
  `construct` writes to `self.path`, `commit` publishes from it to the artifact's selected Store,
  and `retrieve` downloads from that selected Store to it.
- **Returns nothing.** The output *is* the contents of `self.path` after `construct` returns.
- **Plain Python.** Type-checked, real tracebacks pointing at *your* code, debuggable with `pdb`,
  unit-testable (§7). No `builder.run_command(...)`, no quoting strategies.

`construct` runs **only under an executor** ([06](06-execution.md)), on the device assigned to build
this artifact, after its dependencies are ready (§2).

### Surfacing a value
`construct` returns nothing. If a stage conceptually produces a *value* (a metric, a score), write
it as a file into `self.path` and expose it with a `@derived` read ([05](05-derived-and-futures.md)):

```python
def construct(self) -> None:
    metrics = evaluate(...)
    (self.path / "metrics.json").write_text(json.dumps(metrics))
```

There is no return-value channel and no codec registry — heavy serialization
(`model.save_pretrained(self.path)`, `np.save`, `df.to_parquet`) is your plain Python writing into
`self.path`; the framework only syncs the directory.

---

## 2. Reading dependencies — `self.dep.path`

A dependency is an Artifact-valued field ([01](01-artifacts.md) §3). Inside `construct`, its
materialized output is at **`self.dep.path`**:

```python
@artifact
class FinetunedModel:
    base: PretrainedModel
    data: Dataset
    lr: float
    @property
    def relpath(self) -> str: return f"finetune/{self.data.name}/lr{self.lr:.0e}"
    def construct(self) -> None:
        finetune(base=self.base.path, data=self.data.path, lr=self.lr, out=self.path)
```

**The guarantee (when `automaterialize=True`, the default):** before `construct` runs, the framework
materializes every direct dependency, so `self.dep.path` is a real local directory. You never write
a download, never check existence. Reading a value out of a dependency is just reading its file:
`json.loads((self.base.path / "metrics.json").read_text())`.

---

## 3. `automaterialize` and `autocommit` frame `construct`

The two lifecycle toggles ([01](01-artifacts.md) §6) bracket `construct`:

```
if automaterialize:                # default True
    for dep in direct_deps(self): dep.retrieve()    # ready deps locally
    resolve_future_fields(self)    # future-valued fields become concrete
construct(self)                    # you write to self.path
if autocommit:                     # default True
    commit(self)                   # publish self.path atomically to selected Store
```

- **`automaterialize=False`** hands the dep-readying step to you — call `self.dep.retrieve()` (or
  `self.dep.materialize()`) by hand inside `construct`, for lazy/conditional/custom loading. It
  also means a future-valued field is not automatically resolved: call its resolution method and
  materialize the selected artifact yourself before using it. The dependency **ordering** is still
  enforced (deps are built first regardless — [06](06-execution.md)); only local preparation is
  yours.
- **`autocommit=False`** leaves the output local at `self.path`; you call `self.commit()` yourself
  (e.g. after inspecting it, or to commit incrementally).
- **`@artifact(store=...)`** changes where the same default atomic `commit`/`retrieve`/`exists`
  operate. This is suitable for a large model kept in a configured `file://` Store while small
  downstream outputs use the executor's `gs://` Store; it does not change how `construct` is
  written.

---

## 4. Workspaces — extra scratch

`self.path` is the published output. For *ephemeral* intermediate files you don't want to persist,
use `workspace()` ([08](08-runtime-and-cluster.md) §1):

```python
from pipelines import workspace
def construct(self) -> None:
    with workspace() as tmp:                  # auto-cleaned, prefers /dev/shm / node-local
        preprocess(self.data.path, tmp)
        train(data=tmp, out=self.path)        # only self.path is committed
```

---

## 5. Escape hatches — shelling out

Plain Python is the default, not a cage:

```python
import subprocess, yaml
def construct(self) -> None:
    (self.path / "train.yaml").write_text(yaml.safe_dump({"lr": self.lr, "out": str(self.path)}))
    subprocess.run(["python", "pretrain.py", "--config", str(self.path / "train.yaml")], check=True)
```

Conveniences (real subprocesses, real tracebacks — [08](08-runtime-and-cluster.md) §5):

```python
from pipelines.runtime import sh, torchrun, free_port, ctx
def construct(self) -> None:
    torchrun("train.py", "--out", str(self.path),
             nproc_per_node=ctx.annotations["gpus"], port=free_port())
```

---

## 6. The construction discipline

1. **All output goes through `self.path`.** Only its contents are committed; writing elsewhere
   produces untracked output.
2. **Be restart-safe.** A job may be preempted/requeued; `construct` writes the output fresh each
   run (clobber is fine — [02](02-identity-and-storage.md) §3). Don't assume leftover state.
3. **Keep `relpath` (identity/graph) deterministic.** Randomness/wall-clock *inside* the body is
   fine (it affects bytes, not `relpath`). Identity must not depend on ambient inputs
   ([02](02-identity-and-storage.md) §9).
4. **Read deps via `self.dep.path`**, not raw path-guessing.
5. **Don't mutate inputs.** `dep.path` may be a shared local-cache directory; treat read-only (copy
   into `self.path`/`workspace()` to modify).

---

## 7. Testing

Because `construct` is a method writing to `self.path`, you test it under a lightweight executor
that points `base_path` at a tmp dir (and a local store):

```python
def test_pretrain(tmp_path):
    with local_executor(base_path=tmp_path, store=f"file://{tmp_path}/store"):
        m = PretrainedModel(lr=1e-3, epochs=1)
        m.construct()                          # writes to m.path under tmp_path
        assert (m.path / "checkpoint.pt").exists()
```

For graph-level tests, run the real pipeline against a temp local store and assert on outputs —
fast and hermetic, no cluster, no mocking. (Logic factored into a framework-agnostic `steps.py`
module — [09](09-cli-and-observability.md)/authoring — is unit-testable with no executor at all.)

---

## 8. Summary

- `construct(self) -> None` is a **normal method** writing the output into **`self.path`**; returns
  nothing; no DSL, no codecs.
- Dependencies are read at **`self.dep.path`**; dependencies and future-valued fields are prepared
  automatically only when `automaterialize=True`.
- `automaterialize`/`autocommit` bracket `construct` and can be turned off for hand-rolled I/O.
- `workspace()` for ephemeral scratch; `sh`/`subprocess`/`torchrun` escape hatches are plain Python.
- Discipline: output via `self.path` only, restart-safe, deterministic identity, read deps via
  `.path`, don't mutate inputs.

Next: [04-retrieval-and-storage.md](04-retrieval-and-storage.md).
