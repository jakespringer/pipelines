# CLI and Observability

A pipeline script is runnable Python, but day-to-day you drive it from the command line: build a
group or a `relpath` glob, preview a plan, watch progress, read logs, inspect and prune, trace
lineage. The guiding rule: **the CLI operates on the same graph your script builds** — no second
source of truth.

---

## 1. `cli(groups={...})` — the entry point

A project's `run.py` is a config table. The framework owns the verbs and selection:

```python
# run.py
from pipelines import Project, cli, SlurmExecutor
Project.init("em", from_file=__file__)
from . import experiment_baseline, experiment_scaling, experiment_layerft

cli(
    groups={
        "baseline":      experiment_baseline.targets,
        "scaling":       experiment_scaling.targets,
        "layerft":       experiment_layerft.targets,
        "layerft-smoke": experiment_layerft.smoke_targets,
        "all_inference": (experiment_baseline.generations
                          + experiment_scaling.generations
                          + experiment_layerft.generations),
    },
    executor=SlurmExecutor(store=Project.config.remote_store,
                           setup=Project.config.slurm_setup),
)
```

A **group is just an alias for a list of artifacts** — a `name → list` entry, not a framework type,
no behavior. Groups **overlap and compose** freely (a whole experiment is one alias; a cross-cutting
`all_inference` is another); composition is plain-Python list concatenation. Because identity is
`relpath`, selecting overlapping groups builds shared targets **once** (union, deduped). This one
mechanism replaces `experiments`' separate **stage** vs **stage_group** registration.

---

## 2. Selectors — group name and/or `relpath` glob

Two orthogonal selectors resolve to a target set:

- **group name(s)** — the aliases above (plus the implicit **`all`** = the whole instantiated
  universe);
- **`relpath` selector** — an exact `relpath` (one artifact) or a `*`-glob over `relpath` segments
  (`'model/*32B*'`, `'judged/*/insecure'`).

The `run`/`dryrun` **positional accepts either**: if it exactly matches a declared group (or `all`),
that group is used; otherwise it's treated as a `relpath` selector. Multiple positionals union. A
glob may also narrow a group via `--target`.

```bash
pipelines run baseline                       # exact group -> the group
pipelines run baseline scaling               # union of two groups (deduped by relpath)
pipelines run all                            # implicit 'all' = whole universe
pipelines run 'model/*32B*'                  # not a group -> relpath glob over the universe
pipelines run baseline --target 'model/*32B*'# group, narrowed by a glob
```

**What a bare glob matches:** the universe of artifacts the project **instantiates on import** (the
union of everything the declared groups reach — the implicit `all`); a glob needs concrete artifact
*objects* to build. (Pure `inspect`/`retrieve`/`rm` of an already-committed output can work from a
bare `relpath` against its selected Store, since nothing is rebuilt.)

---

## 3. Commands

| Command | What it does |
|---------|--------------|
| `run [SELECTOR…] [--target G] [--force/--force-all] [--head/--tail N] [--executor X]` | Build the selection ([06](06-execution.md)). |
| `dryrun [SELECTOR…]` | Plan + order + freshness + future planning; print plan, `relpath`s, paths, automatically resolved selections or user-managed future fields, and (Slurm) `sbatch`/`afterok` wiring — without building. |
| `status [SELECTOR…]` | Per-artifact state: committed / running / queued / failed / blocked, with job ids. |
| `logs SELECTOR` | Stream/tail an artifact's log (or its Slurm job log). |
| `ls [SELECTOR]` | List store contents by `relpath` with size, age. |
| `inspect SELECTOR` | Print current graph/config and, if the artifact opted into stored metadata, its recorded provenance. |
| `lineage SELECTOR [--up/--down]` | Trace ancestors / descendants. |
| `rm SELECTOR [--down]` | Delete outputs (optionally cascade) — the safe wrapper around hand-`rm`. |
| `gc [--keep-reachable] [--older-than D] [--match GLOB]` | Prune unreachable/old outputs using current-graph reachability; optional metadata can support historical analysis ([04](04-retrieval-and-storage.md) §8). |
| `cancel [SELECTOR…]` | Cancel running/queued Slurm jobs for the selection. |
| `viz [SELECTOR…] [-o dag.svg]` | Render the DAG, colored by state. |

---

## 4. `status` — stateless reconciliation

`status` reconciles three sources, computed **fresh** each call, with **no stored job map**
([06](06-execution.md) §5):

1. **The graph** — re-import the project; the expected artifacts and their `relpath`s.
2. **The scheduler** — `squeue`/`sacct`, filtered to this project via the `hash(project)` namespace
   prefix; matched to artifacts by recomputing each expected `hash(project, relpath)` (the framework
   holds the reverse map in memory). Array index → `relpath` via the deterministic member ordering.
3. **The selected Store for each artifact** — committed = that Store reports a completed
   publication at `relpath`; artifact files such as an optional `meta.json` do not determine state.

```
$ pipelines status scaling
RELPATH                                STATE     JOB        AGE
model/Qwen/Qwen2.5-7B/.../lr1e-05      committed —          2h
model/Qwen/Qwen2.5-14B/.../lr1e-05     running   8123_4     12m
model/Qwen/Qwen2.5-32B/.../lr5e-05     queued    8123_7     —
judged/.../insecure                    blocked   —          (waits on generations)
─────────────────────────────────────────────────────────────
8 committed · 1 running · 5 queued · 1 failed · 9 blocked
```

There is **no `launched_jobs.json` to go stale**: "committed" is the artifact-selected Store's
published state for the `relpath`, and job ids are re-derived from the deterministic graph. Job
names are opaque hashes in `squeue` (other cluster users learn nothing about the project —
[06](06-execution.md) §5).

---

## 5. `inspect` / `lineage` — graph inspection plus optional provenance

The imported graph always provides current configuration and dependency lineage. A committed
artifact may additionally contain user-authored metadata such as `meta.json`
([02](02-identity-and-storage.md) §8); when present, `inspect` can show historical run provenance:

```
$ pipelines inspect 'finetune/cifar10*'
relpath:      finetune/cifar10/lr1e-04        class: FinetunedModel
config:       lr=0.0001
dependencies: base=pretrain/lr1e-03_ep10   data=dataset/cifar10
resolved:     (no future-valued fields)
produced:     2026-05-22T14:03Z  on gpu-node-07  in 1h22m
code:         git 8c30de8 (clean)

$ pipelines inspect 'production/best*'        # an artifact with a Future field
resolved:     base ← argmax(PretrainedModel×4 by val_accuracy) = lr=0.0003 (acc 0.918)

$ pipelines lineage 'pretrain/lr1e-03_ep10' --down
pretrain/lr1e-03_ep10
├── finetune/cifar10/lr1e-04
└── finetune/cifar100/lr1e-04
```

`inspect` surfaces **resolved selections** only when the artifact chose to store them
([05](05-derived-and-futures.md)). `--down` ("what currently depends on this") uses the imported
graph and makes safe invalidation possible: before you `rm` a model, see what would rebuild.

---

## 6. Debugging workflow

1. `status` → find the failed artifact.
2. `logs SELECTOR` → read the traceback. Because `construct` is plain Python, it points at *your*
   code.
3. Reproduce locally: `pipelines run <relpath> --executor local` builds exactly that artifact
   in-process (deps fetched from their selected Stores) — `pdb` it on your laptop with real cluster
   inputs.
4. Fix; delete the stale output (or `--force`) since there's no fingerprint
   ([02](02-identity-and-storage.md) §3); rebuild just it; resume the rest.

---

## 7. Exit codes & scripting

- `run` exits non-zero if any targeted artifact ended `failed` (CI-friendly).
- `dryrun --json` / `status --json` emit machine-readable plans/state for automation.

---

## 8. Summary

- One CLI over the **same graph the script builds**; **`cli(groups={...})`** where a group is just an
  alias for a list of artifacts (overlapping/composable).
- **Unified positional selector**: a group name **or** a `relpath` glob; plus the implicit `all`.
- Verbs: `run`, `dryrun`, `status`, `logs`, `ls`, `inspect`, `lineage`, `rm`, `gc`, `cancel`, `viz`.
- `status`/`cancel` are **stateless** — reconcile graph + `squeue`/`sacct` (matched by opaque
  `hash(project, relpath)` job-names) + each artifact's selected Store; no `launched_jobs.json` to
  drift.
- Current-graph inspection, lineage, and DAG `viz` require no stored metadata; historical
  provenance and resolved selections are available when artifacts opt into recording them.

Next: [10-configuration.md](10-configuration.md).
