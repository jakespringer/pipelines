# Configuration — `Project.config` and Layered TOML

Configuration covers runtime values that are not artifact identity: Store URIs, local paths,
cluster setup, resource defaults, credentials, and arbitrary project-specific settings. The
authoring surface intentionally mirrors `experiments`:

```python
from pipelines import Project

Project.init("em", from_file=__file__)
Project.config.remote_store
Project.config.local_model_store
Project.config.data_dir
```

`Project.config.<arbitrary_key>` is the committed interface. Projects may add keys without a
framework change.

---

## 1. `Project.init` and arbitrary configuration

`Project.init(name, *, from_file=...)` loads the project configuration before experiment modules
instantiate artifacts. It:

1. Discovers checked-in `pipelines.toml` files by walking upward from `from_file`, merging ancestor
   project defaults followed by nearer sub-project defaults.
2. Loads system-local TOML overlays for the current machine and named project.
3. Exposes the merged `[config]` table as an attribute view at `Project.config`.

```python
# run.py
from pipelines import Project, SlurmExecutor, cli

Project.init("em", from_file=__file__)

from . import experiment_baseline as baseline       # may read Project.config.data_dir

cli(
    groups={"baseline": baseline.targets},
    executor=SlurmExecutor(
        store=Project.config.remote_store,
        base_path=Project.config.base_path,
        setup=Project.config.slurm_setup,
        defaults=Project.config.slurm_defaults,
    ),
)
```

`Project.config.to_dict()` exposes the merged mapping for optional tests/branching. Reading a
missing attribute raises an error that names the system project TOML path to edit, rather than
silently yielding `None`.

This is configuration only, not saved run state: pipelines does not write project config, job maps,
or artifact completion records into the system config directory.

---

## 2. TOML locations

There are three deliberate layers:

```
repo/pipelines.toml                          # versioned project defaults and project name
repo/subproject/pipelines.toml               # optional versioned overrides
~/.config/pipelines/config.toml              # optional machine-wide defaults
~/.config/pipelines/projects/em.toml         # machine-local arbitrary settings for project "em"
```

Versioned TOML carries portable defaults and identifies the project:

```toml
# examples/em/pipelines.toml
[project]
name = "em"

[config]
slurm_setup = "source ~/.bashrc && conda activate llm"
slurm_defaults = { partition = "general" }
```

The system per-project TOML carries paths, buckets, secrets, and any arbitrary settings that vary
by installation:

```toml
# ~/.config/pipelines/projects/em.toml
[config]
remote_store = "gs://USER/projects/em"
base_path = "/data/user_data/USER/em-work" # same filesystem as local_model_store
local_model_store = "file:///data/user_data/USER/em-models"
data_dir = "/home/USER/projects/flexibility/data/em"
wandb_api_key = "..."
```

In particular, `local_model_store` is not the transient `base_path`: it is a persistent
`file://` Store for checkpoint classes that use `@artifact(store=...)`
([04](04-retrieval-and-storage.md) §5). It must be visible to consuming jobs, or used with an
executor policy that ensures node affinity. Putting `base_path` on the same mounted filesystem
allows the file Store to publish a completed checkpoint with an atomic rename instead of a
cross-filesystem copy.

---

## 3. Precedence

Layers merge recursively, last wins:

```
built-in defaults
  < versioned ancestor pipelines.toml
  < versioned nearest/sub-project pipelines.toml
  < ~/.config/pipelines/config.toml
  < ~/.config/pipelines/projects/<project>.toml
  < explicit run.py / executor arguments
```

System TOML therefore supplies or overrides installation-specific values while checked-in files
remain useful for portable defaults. Explicit Python arguments remain the final escape hatch:

```python
SlurmExecutor(store="file:///tmp/debug-store", base_path="/tmp/debug-cache")
```

---

## 4. Store policy through configuration

The executor Store is the default for constructed artifacts. A class can retain all default
storage guarantees while selecting a different configured Store:

```python
from pipelines import Project, artifact

@artifact(store=lambda: Project.config.local_model_store)
class FinetunedModel:
    def construct(self) -> None:
        train(out=self.path)                 # checkpoint atomically commits to file://...

@artifact
class ModelGenerations:
    model: FinetunedModel
    def construct(self) -> None:
        generate(model=self.model.path, out=self.path)  # output commits to executor gs://...
```

This expresses the real EM storage policy cleanly: data/evaluation outputs publish to GCS by
default, but large finetuned models remain on configured local storage.

---

## 5. Relationship to `experiments`

`experiments` provides `Project.config.<key>` through per-project JSON under `~/.experiments`.
Pipelines keeps the ergonomic attribute API and replaces the format/layout with layered TOML:

- safe declarative TOML instead of generated/editable JSON;
- versioned defaults and optional sub-project inheritance;
- a system per-project file for arbitrary keys such as GCS and local checkpoint roots;
- no job-history or completion state mixed into configuration.

---

## 6. Summary

- Initialize with **`Project.init(name, from_file=...)`** before importing wiring modules that use
  configuration.
- Use **`Project.config.<arbitrary_key>`** for per-project values from layered TOML.
- Checked-in `pipelines.toml` holds portable defaults; system TOML at
  `~/.config/pipelines/projects/<project>.toml` holds machine/project values.
- Explicit executor arguments override configuration.
- The executor Store is the default; **`@artifact(store=...)`** selects a configured alternate
  Store, such as local finetuned models alongside GCS-backed evaluation outputs.
- Config files hold settings only; committed-output state remains in each selected Store.

Next: [11-comparison-and-migration.md](11-comparison-and-migration.md).
