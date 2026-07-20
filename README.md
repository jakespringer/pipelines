# pipelines

A small, Pythonic library for describing experiments as **Artifacts** — frozen
configurations that know their human-readable location, how to construct
themselves, and how to be retrieved and committed — and for running their
dependency graph effortlessly, locally or (eventually) on a cluster.

Instantiating an Artifact runs nothing; it is a *description*. You wire stages by
referencing one another as fields, hand the final ones to an executor, and only
the work that is missing actually runs.

```python
from pipelines import Project, artifact, source

@artifact
class HFModel:                                   # external/given: no construct
    repo: str
    revision: str = "main"
    @property
    def relpath(self) -> str:
        return f"hf/{self.repo}@{self.revision}"
    def retrieve(self, *, only=None) -> None:
        source.hf(self.repo, self.revision, into=self.path, only=only)

@artifact(store=lambda: Project.config.local_model_store)
class FinetunedModel:
    base: HFModel                                # ← dependency edge
    lr: float
    @property
    def relpath(self) -> str:
        return f"model/{self.base.repo}/lr{self.lr:.0e}"
    def construct(self) -> None:                 # plain Python; writes into self.path
        finetune(self.base.path, self.lr, out=self.path)
```

## Core ideas

Everything is an **Artifact**: a frozen dataclass (declared with `@artifact`)
whose fields *are* its configuration. It has one anchor and four behaviors:

| Member | Role |
| --- | --- |
| `relpath` | The human-readable relative path that **is** the artifact's identity and local materialization location. A pure function of config — no content hash. |
| `construct(self)` | Plain Python that writes the output into `self.path`. Omit it for external/given artifacts. |
| `retrieve(self, *, only=None)` | Make the output local at `self.path` (defaults to downloading from the store; `only=` enables partial retrieval). |
| `exists(self)` | Is it already committed? The scheduler's skip decision. |
| `commit(self)` | Persist `self.path` to durable storage atomically. |

The framework provides working defaults for `retrieve`/`exists`/`commit`, so the
common case is just `relpath` + `construct`.

- **`self.path`** is `<base_path>/<relpath>`, where `base_path` is supplied by the
  executor at run time.
- **`@derived`** exposes a cheap read of an artifact's output as a lazy
  `Future[T]`, which combinators (`argmax`, `argmin`, `gather`, `fmap`, `select`)
  compose. A `Future` may even be a config field ("finetune the *best*
  candidate").
- **`Session`** is a first-class shared resource (e.g. one server per machine)
  opened once per batch and queried by member jobs.
- **`Project.config.<key>`** is an attribute view over layered TOML for runtime
  values that are *not* artifact identity (store URIs, paths, credentials).

See [`design/`](design/) for the full API rationale and [`docs/`](docs/) for the
implementation specification.

## Installation

Requires **Python ≥ 3.10**. The core is pure standard library, except on Python 3.10
where the `tomli` backport (auto-installed) backfills stdlib `tomllib`.

```bash
pip install .                 # from a checkout
pip install -e .              # editable / development install
pip install -e ".[all]"       # include optional extras (see below)
```

### Optional extras

| Extra | Pulls in | Enables |
| --- | --- | --- |
| `hf` | `huggingface_hub` | `source.hf(...)` external retrieval |
| `analysis` | `pandas` | `gather(...).to_frame()` |
| `all` | both of the above | everything optional |

## Usage

A project's `run.py` initializes the project, imports its experiment wiring, and
hands named groups of artifacts plus an executor to `cli(...)`:

```python
from pipelines import LocalExecutor, Project, cli

Project.init("myproj", from_file=__file__)

from . import experiment_a as a

if __name__ == "__main__":
    cli(
        groups={"a": a.targets, "all-previews": a.previews},
        executor=LocalExecutor(
            store=Project.config.remote_store,
            base_path=Project.config.base_path,
        ),
    )
```

Installing the package puts a `pipelines` command on your PATH. It finds the
project's `run.py` and dispatches the verb to it:

```bash
pipelines dryrun all                  # from a dir with run.py + pipelines.toml
pipelines run a                       # build group "a"
pipelines run "model/*"               # build by relpath glob
pipelines -p path/to/proj run a       # or point at the project explicitly
```

`pipelines` discovers the project by walking up from the current directory for a
`run.py` beside a `pipelines.toml`; `-p/--project DIR` (or a path to a `run.py`)
overrides discovery. Selectors are a group name, `all`, or an `fnmatch` glob over
`relpath`.

### Running and monitoring in parallel

`runparallel` schedules the graph across the host's GPUs/CPUs (one worker subprocess
per artifact). Two monitors read the run's event log, so they work from anywhere and
keep working after the run ends:

```bash
pipelines runparallel all                 # resource-aware local parallel build
pipelines attach                          # tmux-style terminal monitor (newest run)
pipelines dashboard                        # web monitor at http://localhost:7000
```

`dashboard` is project-independent: it watches the run registry, so every run —
live or finished, one or many — shows up on its own. The home page lists runs; open
one to see its jobs grouped by state and resource use; open a job for its live-tailed
log. Override the port with `--port`, bind beyond localhost with `--all-interfaces`,
or auto-open a browser with `--open`.

Equivalently, if your project is an importable package you can run its module
directly — `python -m myproj.run run a` — which is what the `pipelines` command
does under the hood. The cluster worker entry point is `python -m
pipelines.worker`.

### Configuration

`Project.init(name, from_file=__file__)` discovers versioned `pipelines.toml`
defaults by walking upward from the caller, then overlays machine-local files,
last-wins:

```
built-in defaults
  < repo/pipelines.toml                          (versioned, portable, carries [project].name)
  < ~/.config/pipelines/config.toml              (machine-wide)
  < ~/.config/pipelines/projects/<name>.toml     (machine, per-project: buckets, paths, secrets)
  < explicit run.py / executor arguments         (final override)
```

Reading a missing key raises an error that names the system TOML path to edit.

## Examples

[`examples/test/`](examples/test/) is a small, self-contained file-processing
pipeline (normalize → index → preview, merge, select-winner, publish, audit) that
exercises most features against tiny checked-in inputs. [`examples/em/`](examples/em/)
is a larger finetuning/eval shape. Each example is a *consumer* project, not part
of the installed package.

## Status

This is an in-progress implementation of the design. Implemented today:

- `@artifact` (frozen dataclass + footgun guards), `relpath` identity, and the
  default `retrieve`/`exists`/`commit` lifecycle.
- `file://` and `gs://` store backends; `source.local` / `source.hf` /
  `source.url`. The `gs://` backend (extra: `pipelines[gs]`) finalizes commits
  with a manifest object, so half-uploaded directories read as not-committed.
- `@derived` and the `Future` combinators.
- Layered-TOML `Project.config`.
- `LocalExecutor`, in-process `Session`, the `run` / `dryrun` CLI verbs, and the
  `pipelines` console launcher (project discovery + dispatch).
- `ParallelExecutor` (`runparallel`): a resource-aware local scheduler — one worker
  subprocess per artifact, GPUs pinned via `CUDA_VISIBLE_DEVICES` — plus the `attach`
  curses monitor and the `dashboard` web monitor (both read the run's event log).

Not yet implemented (registered as stubs that raise a clear error if used):
`wandb://` / `http(s)://` store backends and `source.gs`, `SlurmExecutor` (and the
cluster `worker`), and the `status` / `cancel` CLI verbs.
See [`docs/11-roadmap-and-conformance.md`](docs/11-roadmap-and-conformance.md).

## Repository layout

```
pipelines/      the installable package
  artifact.py   @artifact, settings, injected members
  identity.py   relpath validation, slug, collision checks
  futures.py    Future, @derived, combinators
  sources.py    source.local / hf / url / gs
  session.py    Session base class
  project.py    Project.init, layered TOML, Project.config
  runtime.py    ctx, workspace, run/sh, free_port, slug, ...
  cli.py        cli(groups=..., executor=...)
  worker.py     python -m pipelines.worker
  store/        Store ABC + file:// and gs:// backends
  execution/    materialize, graph, batch, executors/
  scheduler/    runparallel: run server, registry, event log, attach TUI
  dashboard/    web monitor: run index (event-log replay) + HTTP/SSE server + assets/
design/         what the user writes and why
docs/           how it is built (implementation spec)
examples/       standalone consumer projects (test, em)
```

## License

MIT
