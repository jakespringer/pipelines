# CLI and Observability — `pipelines/cli.py`

**Specifies:** `design/09-cli-and-observability.md` (`cli(groups=)`, selectors, verbs, stateless
status/cancel, inspect/lineage, debugging workflow).
**Modules:** `pipelines/cli.py`.
**Depends on:** [06 execution](06-execution.md) (Plan, executors, M-6 hashing), [03 store](03-storage-backends.md)
(`exists`/`ls`/`rm`/`gc`), [10 project](10-config-project-packaging.md) (`Project`).
**Milestone:** M1 (`run`/`dryrun`), M4 (`status`/`cancel`), M5 (`logs`/`ls`/`inspect`/`lineage`/`rm`/`gc`/`viz`,
`--json`).

The guiding rule: **the CLI operates on the same graph the script builds** — no second source of truth.

---

## 1. `cli(...)` — the entry point

```python
def cli(groups: dict[str, list], executor, argv: list[str] | None = None) -> int: ...
```
- A **group is just an alias for a list of artifacts** — a `name → list` entry, not a framework type,
  no behavior. Groups overlap and compose freely (plain-Python list concatenation in `run.py`). Because
  identity is `relpath`, selecting overlapping groups builds shared targets **once** (union, deduped).
  This one mechanism replaces `experiments`' separate `stage` vs `stage_group`.
- `cli` parses `argv`, resolves the selector to a target set (§2), builds the `Plan` via
  [06 graph](06-execution.md), and dispatches the verb (§3) against the given `executor`.
- Called under `if __name__ == "__main__":` (the examples do this) so the worker can import `run.py`
  without triggering the CLI ([06 §8](06-execution.md)).

`examples/test/run.py` groups: `variants`, `merge`, `selection`, `release`, `audit`, `all-previews`,
`smoke`. `examples/em/run.py` groups: `baseline`, `scaling`, `layerft`, `layerft-smoke`, `all_inference`.

---

## 2. Selectors

Two orthogonal selectors resolve to a target set (`design/09 §2`):
- **group name(s)** — declared aliases, plus the implicit **`all`** = the whole instantiated universe
  (the union of everything the declared groups reach).
- **`relpath` selector** — an exact `relpath` (one artifact) or a `*`-glob over `relpath` segments
  (`'model/*32B*'`, `'judged/*/insecure'`).

Resolution rule for the `run`/`dryrun` positional: if it exactly matches a declared group (or `all`),
use that group; otherwise treat it as a `relpath` selector. Multiple positionals **union**. `--target
GLOB` narrows a selected group by a glob.

```bash
pipelines run baseline                        # exact group
pipelines run baseline scaling                # union of two groups (deduped by relpath)
pipelines run all                             # whole universe
pipelines run 'model/*32B*'                   # not a group -> relpath glob over the universe
pipelines run baseline --target 'model/*32B*' # group, narrowed by a glob
```

A **bare glob** matches the universe of artifacts the project **instantiates on import** (concrete
objects are needed to build). Pure `inspect`/`retrieve`/`rm` of an already-committed output can work
from a bare `relpath` against its selected Store (nothing is rebuilt).

---

## 3. Commands

Backend-specific verbs are **namespaced** `pipelines <backend> <verb>` (`local`/`parallel`/`slurm`),
dispatched in `cli._dispatch` (`_parallel_group`/`_local_group`, and `execution/executors/slurm_cli.py`
for `slurm`). Backend-independent verbs stay top-level. `runparallel`/`attach` are kept as deprecated
aliases for `parallel run`/`parallel attach`.

| Command | Behavior |
|---------|----------|
| `run [SEL…] [--force/--force-all]` | Build the selection with the **configured** executor ([06](06-execution.md)). `--force` rebuilds **only the selected targets** (the named group members / glob matches) even if committed — their dependencies keep normal freshness (a committed input is retrieved, not rebuilt). `--force-all` rebuilds the **whole reachable graph** and also builds `@artifact(transient=True)` intermediates otherwise skipped ([06 §2](06-execution.md)). |
| `local run [SEL…] [--force/--force-all]` | Sequential, in-process `LocalExecutor` (debug/reproduction). |
| `parallel run [SEL…] [--gpus/--cpus/--memory N] [--force/--force-all] [--dry]` · `parallel attach [PORT]` | Resource-aware local parallel build via the run server; tmux-style live monitor. |
| `slurm run [SEL…] [--force/--force-all] [--detach] [--dry]` | Submit one `afterok`-wired `sbatch` job per artifact (`execution/executors/slurm.py`), then a foreground (detachable) monitor (`scheduler/slurm_monitor.py`) feeds the dashboard. `--skip-committed` is passed to ordinary workers so requeued tasks skip committed work (omitted for `--force`-d artifacts, which must rebuild). |
| `slurm ls\|status [SEL…] [--expand] [--watch]` · `slurm cancel [SEL…] [--dry]` | Stateless `squeue`/`sacct` reconciliation by class; `scancel` the matching jobs (§4). |
| `slurm sendcommand '<tmpl>' [SEL…] [--dry]` | Per-job command with `{jobid}`/`{relpath}`/`{name}`/`{state}`/`{cls}`/`{partition}` substitution. |
| `slurm attach [RUN_ID]` · `slurm logs SEL` · `slurm cat JOBID` | Resume a detached/finished run's monitor; print a job's captured log (by selector); print a job's captured log located by Slurm job id across the run registry (project-independent — runs from any directory). |
| `dryrun [SEL…]` | Plan + order + freshness + future planning; print plan, `relpath`s, resolved paths, auto-resolved/user-managed future fields, and (Slurm) `sbatch`/`afterok` wiring — without building. |
| `dashboard [--port 7000] [--host H] [--open]` | Serve the web monitor for all runs (live + past); see [12-dashboard.md](12-dashboard.md). Project-independent. |
| `ls [SEL]` | List store contents by `relpath` with size, age. |
| `inspect SEL` | Print current graph/config and, if the artifact stored metadata, its provenance (§5). |
| `lineage SEL [--up/--down]` | Trace ancestors/descendants over the imported graph. |
| `rm SEL [--down]` | Delete outputs (optionally cascade) — the safe wrapper around hand-`rm`. |
| `gc [--keep-reachable] [--older-than D] [--match GLOB]` | Prune using current-graph reachability ([03 §6](03-storage-backends.md)). |
| `viz [SEL…] [-o dag.svg]` | Render the DAG, colored by state (optional `networkx`+Graphviz). |

Cluster job state/cancel are surfaced under the `slurm` group (`slurm ls`/`slurm cancel`, §4);
`ls`/`inspect`/`lineage`/`rm`/`gc`/`viz` are graph/store verbs (planned top-level).

`run` exits **non-zero** if any targeted artifact ended `failed` (CI-friendly). `dryrun --json` /
`status --json` emit machine-readable plans/state.

---

## 4. `slurm ls` / `slurm cancel` — stateless reconciliation (M-6)

Surfaced by `execution/executors/slurm_cli.py`'s `reconcile()` (`slurm ls`/`status`, `cancel`,
`sendcommand`). It reconciles three sources, computed **fresh** each call, with **no stored job map**:
1. **The graph** — re-import the project; expected artifacts + `relpath`s.
2. **The scheduler** — `squeue`/`sacct`, filtered to this project via the `hash(project)` namespace
   prefix; matched to artifacts by recomputing each `hash(project, relpath)` (reverse map held in
   memory). Array index → `relpath` via the deterministic member ordering ([06 §6](06-execution.md)).
3. **The selected Store per artifact** — committed = that Store reports a completed publication at
   `relpath` ([03 §3](03-storage-backends.md)); an artifact's `meta.json` does **not** determine state.

State precedence per artifact: `committed` (store) > `running`/`queued` (scheduler) > `failed`
(scheduler terminal) > `blocked` (unsatisfied dep, no job) > `pending`. Output is a table
(`relpath`, state, job id, age); `--json` emits the structured form. `cancel` recomputes the same
job-name mapping and `scancel`s matching `squeue` entries. There is no `launched_jobs.json` to drift.

---

## 5. `inspect` / `lineage`

The imported graph always provides current configuration and dependency lineage (no stored metadata
needed). A committed artifact may additionally contain user-authored `meta.json` ([02 §7](02-identity-paths.md));
when present, `inspect` shows historical run provenance and, for a future-valued field, the recorded
resolved selection ([05 §5](05-futures-derived.md)) — **only** when the artifact chose to store it.
`lineage --down` ("what currently depends on this") makes safe invalidation possible: see what would
rebuild before you `rm` a model.

---

## 6. Debugging workflow (`design/09 §6`)

1. `status` → find the failed artifact.
2. `logs SEL` → read the traceback (plain Python → points at *your* code).
3. `pipelines run <relpath> --executor local` → build exactly that artifact in-process (deps fetched
   from their selected Stores) — `pdb` on your laptop with real cluster inputs.
4. Fix; delete the stale output (or `--force`, since there is no fingerprint, [02 §5](02-identity-paths.md));
   rebuild just it; resume the rest.

---

## 7. Conformance hook

- `examples/test`: `pipelines dryrun smoke`, `run variants/merge/selection/audit`, `run all-previews`;
  overlapping groups dedup; `smoke` mixes `previews[0]` + `selection.bundle`.
- `examples/em`: `dryrun baseline`, `run scaling --target 'model/*32B*'`, `run all_inference`,
  `run 'judged/*/insecure'` (bare glob), `run layerft-smoke`.
- `status` reconstructs job↔artifact purely from the graph + `squeue`; no stored map; opaque job-names.
- `run` exit code non-zero on a failed target; `--json` parses.

Next: [10-config-project-packaging.md](10-config-project-packaging.md).
