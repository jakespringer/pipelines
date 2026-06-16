# Execution — The Primitive and the Executors

There is exactly **one** primitive — "materialize this Artifact" — and every executor (local
sequential, local parallel, Slurm) is a different policy for *who* calls it and *how many at once*.
This document defines the primitive, graph collection/ordering, the executors, batching + sessions,
the stateless job-name mapping, failure handling, and dry-run.

---

## 1. The one primitive: `materialize(artifact)`

Run by a worker that has imported the project (so all Artifact configs exist in memory) under an
executor that has set `base_path`:

```python
def materialize(a, *, scheduler):
    if scheduler and a.cache and a.exists():       # exists() is scheduler-only, may be expensive
        return                                     # skip / resume (only when cache=True)
    if has_construct(a):
        if a.automaterialize:                      # default: ready direct deps + resolve futures
            for d in direct_deps(a): d.retrieve()
            resolve_future_fields(a)               # future-valued fields -> concrete (05 §4)
        a.construct()                              # writes to a.path
        if a.autocommit:
            a.commit()                             # atomic publish to a's selected Store; no default metadata
    else:                                          # external/given
        a.retrieve()                               # via source.* (04 §6)
```

Local execution calls this for every artifact in dependency order; a cluster job calls it once.
Nothing else needs to know how an artifact is built.

### Strict vs. autonomous
- **Autonomous** (`run()` locally): if a dependency's output is missing, recursively materialize it
  first — "build the whole pipeline up to the target."
- **Strict** (cluster worker, `--only <relpath>`): dependencies are *required* to already be
  committed (the scheduler's ordering guaranteed it). A missing dep is an error, not a trigger to
  build. Each Slurm job does exactly one artifact's work.

---

## 2. Collecting and ordering the graph

Given target Artifacts, the executor:

1. **Walks dependency fields** (and `Future` source sets — [05](05-derived-and-futures.md) §4) from
   the targets to collect the reachable sub-graph. Configs are in memory — pointer-chasing, no I/O.
2. **Topologically sorts** into tiers (cycles are a build-time error naming the classes).
3. **Runs one batched freshness pass** (`exists_many` — [04](04-retrieval-and-storage.md) §1),
   grouping artifacts by selected Store: artifacts already committed (and `cache=True`) are marked
   *satisfied* and pruned from the "to build" set, but retained for ordering (a consumer still
   retrieves them through its own Store).

The graph is collected, ordered, and freshness-checked **once**; executors consume the immutable
result. **Dependency ordering is always derived from fields** — independent of `automaterialize`
([01](01-artifacts.md) §6): even with `automaterialize=False`, upstreams are still built first and
wired; the author takes over local retrieval and resolution/materialization of future-valued fields
inside the body.

---

## 3. `LocalExecutor` — sequential

Materialize each unsatisfied artifact in topological order, in-process. `construct` bodies run in the
current process → trivial `pdb`, real tracebacks. Ideal for development. This is what `run(target)`
does by default, and what running a script's `cli(...)` with `--executor local` gives you.

---

## 4. `ParallelExecutor` — local, resource-aware  *(implemented)*

Build independent artifacts concurrently on one machine, respecting resources:

```bash
pipelines runparallel baseline                  # auto-detects host GPUs/CPUs/memory
pipelines runparallel scaling --gpus 8 --cpus 32 --memory 256G   # explicit caps
pipelines runparallel all --dry                 # plan + per-artifact resources, no build
```

A **run server** keeps a ready-set and launches each artifact whose dependencies are met and whose
resource intent (the `gpus`/`cpus`/`memory` keys from `annotations` — [08](08-runtime-and-cluster.md)
§7) fits the free pool. Each builds in a worker subprocess (isolation), pinned via
`CUDA_VISIBLE_DEVICES` to concrete GPU ids the scheduler hands out. The worker is the same
`materialize` primitive in **strict** mode, re-entered through the hidden `pipelines _worker --only
<relpath>` verb (it re-imports the project, so the graph is rebuilt cheaply per job).

The server also serves a monitor/control API on a TCP port (16400+), writes an agent-parsable
JSON-lines event log, and per-job captured output. **Cooperative yielding:** a job can call
`pipelines.runtime.yield_now()` (or set `PIPELINES_YIELD=1`, which auto-yields right before its
commit/upload) to hand its GPUs/CPUs back to the scheduler while it finishes saving/uploading — it
shows as *yielding* until it exits. See [09](09-cli-and-observability.md) for `pipelines attach`, the
tmux-style live monitor.

---

## 5. `SlurmExecutor` — cluster

Build each artifact (or group) as a Slurm job, wired by dependency order:

```python
run(targets, executor=SlurmExecutor(
    store="gs://bucket/exp/projX",
    setup="source ~/.bashrc && conda activate llm",
    defaults={"partition": "general", "time": "2-00:00:00", "cpus": 4},
))
```

Each unsatisfied artifact emits an `sbatch` job whose payload is the primitive in **strict** mode:

```bash
#SBATCH --job-name <hash(project,relpath)>  --gpus=4 --time=24:00:00 ...
source ~/.bashrc && conda activate llm                       # setup
python -m pipelines.worker --only 'finetune/cifar10/lr1e-04' --store gs://bucket/exp/projX --strict
```

On start the job re-imports the project (cheap, pure graph build), looks up the artifact by
`relpath`, retrieves deps through each dependency's selected Store (guaranteed present by
ordering), runs `construct`, and commits to the artifact's selected Store. Thus an EM generation
job may read its model from configured `file://` storage while committing its JSONL to `gs://`.

**Dependency wiring.** Topological tiers become Slurm `--dependency=afterok:<ids>`. Independent
artifacts in a tier run in parallel. Satisfied (already-committed) upstreams contribute **no job**;
the downstream's strict retrieval just succeeds. A **future-valued field** wires the consumer
`afterok` to *all* its candidate jobs (the static source set); when the consumer uses automatic
materialization, the winner is chosen inside that consumer at runtime
([05](05-derived-and-futures.md) §4) — **no resolver job**.

### Stateless job ↔ artifact mapping
`--job-name = <hash(project)>:<hash(project, relpath)>` — an **opaque hash**, so other cluster users
can't tell from `squeue` what the project is or what's running. Properties:

- `hash(project)` is a derivable **namespace prefix**, so the framework can filter its own jobs
  (`squeue --name '<hash(project)>:*'`) without revealing the name.
- **No stored job map** (no `launched_jobs.json`). `status`/`cancel` **re-import the project**,
  recompute each expected artifact's hash, hold the reverse map in memory, and reconcile against
  `squeue`/`sacct`. Nothing can drift — names are re-derived from the deterministic graph each time.
- **Array jobs (sweeps):** like-resourced artifacts of one class submit as a Slurm **array**;
  `--job-name = <hash(project)>:<hash(array-group-id)>`, and **array index → `relpath`** is recovered
  by re-deriving the deterministic member ordering. The `array-group-id` incorporates the member
  set, so a stale array can't be misread.

(See [09](09-cli-and-observability.md) for how `status` uses this.)

> **Implementation status.** v1 (`pipelines/execution/executors/slurm.py`) emits **one job per
> artifact**, `afterok`-wired, with the opaque stateless job names above. A foreground/detachable
> monitor (`pipelines/scheduler/slurm_monitor.py`) polls `squeue`/`sacct` and writes the same
> `events.log` the dashboard replays. Array jobs (this section's sweep optimization) are the planned
> next step: the single-job submission loop is the seam they attach to. Driven by the `pipelines
> slurm` command group ([09](09-cli-and-observability.md) §3).

---

## 6. Batching and sessions

- **Like-resource grouping** → a Slurm **array job** per (class, resolved-resource-profile). The
  efficient bulk path for sweeps; a pure function of the ordered graph + resolved annotations.
- **`BatchPolicy`** packs many cheap artifacts into one job (or shards a group across nodes):

  ```python
  run(targets, executor=SlurmExecutor(..., batch=[BatchPolicy(select=QuickEval, max_per_job=16)]))
  ```

  Batching lives **only in the scheduler**, over the immutable graph — no graph wrapper, no
  member→producer remapping.
- **Sessions** ([07](07-sessions.md)) handle "load the expensive shared thing once" (e.g. one vLLM
  server per machine that member jobs query). A `Session.group_key` co-locates members; `BatchPolicy`
  controls fan-out. This subsumes `experiments`' `BatchedJudgedResponses` hack.

---

## 7. Failure, retries, idempotency

- **Per-artifact isolation:** a failure fails its dependents (blocked) but independent branches
  continue; the run prints succeeded/failed/blocked.
- **Retries:** `retries=` on the class or executor; transient infra errors (preemption, GCS 5xx)
  retry with backoff. Exceptions in `construct` don't auto-retry by default.
- **Preemption/requeue:** safe — `commit` is atomic ([04](04-retrieval-and-storage.md) §3), so a
  requeued job either finds its output committed (skip) or rebuilds into fresh staging.
- **No run-state to corrupt:** re-running re-derives the graph; the freshness pass discovers exactly
  what's committed. Just `run` again.

---

## 8. Dry run

`run(targets, dry=True)` / CLI `dryrun` does collection + ordering + the freshness pass +
future-field planning, then prints the plan **without building**: artifacts to build (grouped into tiers/jobs)
with resolved annotations and concrete directives; artifacts that will be skipped (committed) and
why; each artifact's `relpath` and resolved `path` (eyeball determinism); automatically
to-be-resolved selections or user-managed future fields; and for Slurm, the exact `sbatch` headers
and `afterok` wiring. The safety check before spending cluster hours.

---

## 9. Summary

- One primitive: **`materialize(artifact)`** — skip if committed in its selected Store (and
  `cache`), else, when
  `automaterialize=True`, ready deps + resolve futures, then `construct` + atomic `commit` (or
  `retrieve` for externals).
- Graph collected/ordered/freshness-checked **once**; ordering always from fields.
- `LocalExecutor` (debug), `ParallelExecutor` (resource-aware subprocesses), `SlurmExecutor` (one
  strict job per artifact, `afterok`-wired, array jobs for sweeps, scheduler-only batching +
  sessions).
- Job mapping is **stateless** — `--job-name = hash(project, relpath)` (opaque), array index →
  `relpath` re-derived; `status`/`cancel` reconcile `squeue`/`sacct` fresh.
- Failure isolated; atomic commit makes preemption/concurrency safe; **no run-state to corrupt**;
  `dryrun` prints the full plan.

Next: [07-sessions.md](07-sessions.md).
