# Execution — `pipelines/execution/` + `worker.py`

**Specifies:** `design/06-execution.md` (the `materialize` primitive, graph collection/ordering,
executors, batching, stateless job mapping, failure, dryrun).
**Modules:** `pipelines/execution/{materialize,graph,batch,annotations}.py`,
`pipelines/execution/executors/{local,parallel,slurm}.py`, `pipelines/worker.py`.
**Depends on:** [01 artifact](01-core-artifact.md), [02 identity](02-identity-paths.md),
[03 store](03-storage-backends.md), [05 futures](05-futures-derived.md), [07 sessions](07-sessions.md),
[08 runtime](08-runtime-helpers.md).
**Milestone:** M1 (`materialize`, `graph`, `LocalExecutor`, `worker`), M4 (`ParallelExecutor`,
`SlurmExecutor`, `batch`, `annotations`, stateless status).

There is exactly **one** primitive — "materialize this Artifact" — and every executor is a policy for
*who* calls it and *how many at once*. Implements cross-cutting mechanic **M-6** (stateless job
identity).

---

## 1. The primitive — `materialize.py`

```python
def materialize(a, *, scheduler: bool = False, strict: bool = False):
    # `scheduler`: this call is allowed to skip via exists() (planning context)
    # `strict`:    deps must already be committed; a missing dep is an error, not a trigger to build
    with _runtime_context(a):                      # sets _CTX (base_path, relpath, annotations, ...) — M-1
        if scheduler and a.__pipelines__.cache and a.exists():
            return                                  # skip / resume (only when cache=True)
        if has_construct(a):
            target = a
            if a.__pipelines__.automaterialize:     # default
                for d in a.dependencies():
                    _ready_dep(d, strict=strict)    # strict: require committed; else recurse (autonomous)
                target = resolve_future_fields(a)   # future fields -> concrete clone (05 §5 / M-2)
            target.construct()                      # writes to self.path
            if a.__pipelines__.autocommit:
                a.commit()                          # atomic publish to selected Store (03 §3)
        else:                                       # external/given
            a.retrieve()                            # via source.* (04)

def _ready_dep(d, *, strict):
    if strict:
        d.retrieve()                                # raises if not committed (ordering guaranteed it)
    elif not (d.__pipelines__.cache and d.exists()):
        materialize(d, scheduler=True, strict=False)  # autonomous: build the missing upstream
    else:
        d.retrieve()
```

- **Autonomous mode** (`run()` locally): missing deps are recursively materialized — "build the whole
  pipeline up to the target."
- **Strict mode** (cluster worker `--only <relpath> --strict`): deps must already be committed; the
  scheduler's ordering guaranteed it. Each Slurm job does exactly one artifact's work.
- `commit` uses `a` (original), but `construct` ran on the resolved clone `target`; both write the same
  `self.path` (resolved against the same `_CTX`). `relpath` is identical for `a` and the clone (M-2).
- Session-bound artifacts: `_runtime_context` also exposes the open `Session` as `_CTX.session`; the
  session lifecycle is owned by the executor ([07](07-sessions.md)), not the primitive.

---

## 2. Graph collection and ordering — `graph.py`

Given target artifacts:

1. **`collect(targets)`** — walk `dependencies()` (fields + `Future` source sets, [05 §3](05-futures-derived.md))
   transitively. Pointer-chasing, no I/O. Returns the reachable set.
2. **`check_collisions(reachable)`** ([02 §4](02-identity-paths.md)) and `validate_relpath` each.
3. **`toposort(reachable)`** → tiers (list of lists). Cycle ⇒ build-time error naming the classes.
   **Port** `compute_topological_ordering` / `_build_dependency_graph` / `_kahn_layered_sort` from
   `experiments/executor.py:1456/1489/1543` (Kahn layered sort, preserves insertion order within a
   tier for deterministic array indexing).
4. **`freshness(reachable)`** — one batched pass: group artifacts by **selected Store** (M-5), call
   `store.exists_many([relpaths])` per group; mark committed-and-`cache=True` artifacts *satisfied*,
   prune from the "to build" set but **retain for ordering** (a consumer still `retrieve`s them).
5. **`_prune_transient(unsatisfied)`** — drop `@artifact(transient=True)` intermediates that no
   artifact-that-will-run depends on (see *Transient artifacts* below). Like satisfied artifacts,
   they are **retained for ordering** but moved out of `to_build` into `transient_skipped`.

```python
@dataclass
class Plan:
    ordered: list              # topological order; satisfied/skipped retained for ordering
    to_build: set              # will build (excludes satisfied and transient_skipped)
    satisfied: set             # already committed
    transient_skipped: set     # transient, no running consumer (kept if a forced artifact needs it)
    forced: set                # rebuilt unconditionally (--force ⇒ targets, --force-all ⇒ all)
```

### Transient artifacts (`@artifact(transient=True)`)

A *transient* artifact is an on-demand intermediate (a large scratch corpus, an ephemeral
projection) that should materialize **only when something that will actually run this invocation
consumes it** — not merely because it is missing and reachable. The freshness pass computes the
"will run" set by closure: seed it with every **non-transient** unsatisfied artifact (those always
build), then walk *down* dependency edges within the unsatisfied set — each will-run artifact pulls
in every unsatisfied input it needs (transient or not), transitively. Any transient artifact the
closure never reaches has **no running consumer** and is moved to `transient_skipped`; it is skipped
even though it is missing.

This is exactly the "the final result is already committed, so don't rebuild the scratch it was
derived from" case: if a non-transient consumer `C` is *satisfied*, it does not run, so it never pulls
its transient upstream `T` into the closure — `T` is skipped. The prune is **safe** precisely because
a skipped artifact has no running dependent, so no `_ready_dep` (autonomous mode) or scheduler edge
(parallel mode) ever tries to resurrect it.

`build_plan(targets, force=…, force_all=…)` records which artifacts are forced. `--force` forces
**only the selected targets** (the group members / glob matches named on the command line) — they are
dropped from `satisfied` (so they rebuild even if committed) and seed the prune (so any transient they
need is retained), while their *dependencies* keep normal freshness (a committed input is retrieved,
not rebuilt). `--force-all` forces the **whole reachable graph**: nothing is treated as satisfied and
no transient is skipped. Executors read `Plan.forced` to bypass the materialize-time cache check for
those artifacts (Slurm additionally omits `--skip-committed` for them). The `plan`/`dryrun` output and
the dashboard surface `transient_skipped` as a distinct *skipped* state, separate from *committed*.

**Ordering is always derived from fields**, independent of `automaterialize` ([01 §6](01-core-artifact.md)
note): even with `automaterialize=False`, upstreams build first; only local retrieval/resolution is the
author's. The graph is collected/ordered/freshness-checked **once**; executors consume the immutable
`Plan`.

---

## 3. Annotations resolution — `annotations.py`

Three explicit levels, last wins (`design/08 §7`):
```
executor.defaults  <  artifact.annotations (incl. its "slurm")  <  CLI --annotate k=v
```
- **Portable keys** (`gpus`, `cpus`, `memory`, `runtime`) map to each backend's directives (one
  canonical key per concept; Slurm maps `gpus`→`--gpus`, etc.).
- **Namespaced sections** (`slurm`, `k8s`) merge only into the matching backend.
- **No silent drop:** each executor declares consumed keys; `dryrun` warns about keys nothing consumed.
- Computed `annotations` must be pure functions of config (so `dryrun` and the launcher agree).
- `resolve_annotations(artifact, executor, cli_overrides) -> dict`.

---

## 4. `LocalExecutor` — `executors/local.py` (M1)

Materialize each unsatisfied artifact in topological order, **in-process**, autonomous mode. `construct`
runs in the current process → trivial `pdb`, real tracebacks. This is what `run(target)` and
`--executor local` use. Sets `_CTX` per artifact (`base_path` from the executor/config). Sequential;
no resource scheduling.

Constructor: `LocalExecutor(store, base_path)` (matches `examples/test/run.py`).

---

## 5. `ParallelExecutor` — `executors/parallel.py` (M4)

```python
ParallelExecutor(store, base_path, slots={"gpu": 8, "cpu": 32})
```
A scheduler loop keeps a ready-set (artifacts whose deps are satisfied) and launches those whose
resource intent (`gpus`/`cpus` from resolved annotations) fits free slots. Each builds in a **worker
subprocess** (isolation), pinned via `CUDA_VISIBLE_DEVICES` from allocated gpu ids. The worker entry is
the same primitive in strict mode (`python -m pipelines.worker --only <relpath> --strict`). Independent
branches run concurrently; a failure blocks only its dependents (§7).

---

## 6. `SlurmExecutor` — `executors/slurm.py` (M4)

```python
SlurmExecutor(store, base_path, setup=None, defaults=None, batch=None, retries=0, env=None)
```
Each unsatisfied artifact (or batch group) emits one `sbatch` job whose payload is the primitive in
**strict** mode:

```bash
#SBATCH --job-name <hash(project)>:<hash(project,relpath)>  --gpus=4 --time=24:00:00 ...
<setup>                                                  # e.g. source ~/.bashrc && conda activate llm
python -m pipelines.worker --only 'finetune/cifar10/lr1e-04' --store <uri> --strict
```

- **Dependency wiring:** topological tiers → `--dependency=afterok:<ids>`. Independent artifacts in a
  tier run in parallel. Satisfied upstreams contribute **no job**; the downstream's strict `retrieve`
  just succeeds. A **future-valued field** wires the consumer `afterok` to **all** candidate jobs (the
  static source set); the winner is chosen inside the consumer at runtime (M-2) — **no resolver job**.
- **Port** sbatch-header / `afterok` / job-name emission from `experiments/executor.py:2359-2419` and
  Slurm-arg normalization from `experiments/executor.py:998` (`_normalize_slurm_config`), fed from
  resolved annotations' `slurm` section + `executor.defaults`. **Replace** the stored job map with M-6.

### Stateless job ↔ artifact mapping (M-6)
- `--job-name = <hash(project)>:<hash(project, relpath)>` (opaque hex). `hash(project)` is a derivable
  **namespace prefix** so the framework filters its own jobs (`squeue --name '<hash(project)>:*'`)
  without revealing the name to other cluster users.
- **No stored job map** (no `launched_jobs.json`). `status`/`cancel` re-import the project, recompute
  each artifact's hash, hold the reverse map in memory, reconcile against `squeue`/`sacct`
  ([09 §4](09-cli.md)). Nothing drifts.
- **Array jobs (sweeps):** like-resourced artifacts of one class submit as a Slurm **array**;
  `--job-name = <hash(project)>:<hash(array-group-id)>`, where `array-group-id` incorporates the
  member set; array index → `relpath` recovered by re-deriving the deterministic member order
  (toposort preserves insertion order, §2).

---

## 7. Batching, sessions, failure, retries — `batch.py`

- **Like-resource grouping** → one Slurm array per `(class, resolved-resource-profile)`. A pure function
  of the ordered graph + resolved annotations.
- **`BatchPolicy(select=ClassOrPredicate, max_per_job=N)`** packs many cheap artifacts into one job, or
  shards a session-group across nodes. Lives **only in the scheduler**, over the immutable `Plan` — no
  graph wrapper, no member→producer remapping (the thing that bloated `experiments`).
- **Sessions** ([07](07-sessions.md)) handle "load the expensive shared thing once" (one server per
  machine). `Session.group_key` co-locates members; `BatchPolicy` controls fan-out. Subsumes
  `experiments`' `BatchedJudgedResponses`.
- **Failure isolation:** a failure fails its dependents (blocked) but independent branches continue; the
  run reports succeeded/failed/blocked. **Retries:** `retries=` on class (`__pipelines__.retries`) or
  executor; transient infra errors (preemption, GCS 5xx) retry with backoff; exceptions in `construct`
  don't auto-retry by default. **Preemption-safe:** atomic `commit` ([03 §3](03-storage-backends.md))
  means a requeued job either finds the output committed (skip) or rebuilds into fresh staging.
- **No run-state to corrupt:** re-running re-derives the graph; the freshness pass discovers what's
  committed. Just `run` again.

---

## 8. `worker.py`

```bash
python -m pipelines.worker --only <relpath> --store <uri> [--base-path P] [--strict] [--project run.py]
```
Steps: re-import the project (cheap, pure graph build — instantiates all configs in memory), set the
runtime `_CTX` (`base_path`, resolved `annotations`, granted `gpu_ids`, logger, session), look up the
artifact by `relpath`, and call `materialize(a, scheduler=False, strict=strict)`. Deps are retrieved
through **each dependency's own selected Store** (so an `em` generation job reads its model from
`file://` while committing JSONL to `gs://`). The project entry is the same `run.py` the user wrote;
the worker imports it without invoking `cli()` (guard via `if __name__ == "__main__"` in `run.py`, as
the examples do).

---

## 9. Dry run

`run(targets, dry=True)` / CLI `dryrun` does collection + ordering + freshness + future-field planning,
then prints **without building**: artifacts to build (grouped into tiers/jobs) with resolved
annotations and concrete directives; artifacts that will be skipped (committed) and why; each
artifact's `relpath` and resolved `path` (eyeball determinism); automatically-resolved selections or
user-managed future fields; and for Slurm, the exact `sbatch` headers and `afterok` wiring. Output
format detail in [09 §3](09-cli.md).

---

## 10. Conformance hook

- `LocalExecutor` builds `examples/test` `variants`/`merge`/`selection` end-to-end against a temp
  `file://` store, autonomous mode, deterministic `relpath`s.
- `automaterialize=False` (`ManualWinnerReport`), `autocommit=False` (`PublishedBundle`), `cache=False`
  (`AuditMarker`) take the documented branches of the primitive.
- `ParallelExecutor(slots=...)` builds independent `test` artifacts concurrently.
- `SlurmExecutor` `dryrun` for `examples/em` emits correct tiers, `afterok` edges, array jobs for the
  model sweep, and opaque job-names; future-field consumers wire `afterok` to all candidates with no
  resolver job.
- `status`/`cancel` reconcile fresh from graph + `squeue`/`sacct` + per-artifact Store; no stored map.

Next: [07-sessions.md](07-sessions.md).
