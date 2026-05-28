# Configuration, Project, and Packaging — `pipelines/project.py` + build

**Specifies:** `design/10-configuration.md` (`Project.config`, layered TOML, discovery, precedence,
store-via-config) plus the packaging the rest of the spec assumes.
**Modules:** `pipelines/project.py`; `pyproject.toml`.
**Depends on:** [03 store](03-storage-backends.md) (store URIs from config).
**Milestone:** M1 (`project.py`), M5 (packaging polish).

`Project.config.<arbitrary_key>` is the committed interface: a merged view over layered TOML for
runtime values that are **not** artifact identity (Store URIs, paths, cluster setup, credentials,
arbitrary project settings). Implements cross-cutting mechanic **M-5**'s config source.

---

## 1. `Project` — singleton context

```python
class Project:
    name: str
    config: "_ConfigView"

    @classmethod
    def init(cls, name: str, *, from_file: str) -> None: ...
```

`Project.init(name, from_file=__file__)` is called at the **top of `run.py` before importing wiring
modules** (the examples do this; `experiment_baseline` reads `Project.config.data_dir` at import). It:
1. Discovers checked-in `pipelines.toml` by walking **upward** from `from_file`, merging ancestor
   project defaults then nearer sub-project defaults.
2. Loads system-local TOML overlays for this machine and named project.
3. Exposes the merged `[config]` table as `Project.config` (attribute view).

`Project` holds module-level singleton state (`name`, merged config). `Project.config.to_dict()`
exposes the merged mapping for tests/branching. Reading a **missing** attribute raises an error that
**names the system project TOML path to edit** (`~/.config/pipelines/projects/<name>.toml`), never
silently yielding `None`. `Project.init` does **not** write any state — config is read-only; no job
maps or completion records go into the config directory.

```python
class _ConfigView:
    def __getattr__(self, key):
        try: return self._data[key]
        except KeyError: raise ConfigKeyError(key, self._system_path)
    def to_dict(self) -> dict: return dict(self._data)
```

---

## 2. TOML locations and precedence

Layers merge recursively, **last wins** (`design/10 §3`):
```
built-in defaults
  < versioned ancestor   repo/pipelines.toml
  < versioned nearest    repo/subproject/pipelines.toml
  < machine-wide         ~/.config/pipelines/config.toml
  < machine per-project  ~/.config/pipelines/projects/<name>.toml
  < explicit run.py / executor arguments      (final escape hatch)
```

- **Versioned `pipelines.toml`** carries portable defaults + the project name:
  ```toml
  [project]
  name = "em"
  [config]
  slurm_setup = "source ~/.bashrc && conda activate llm"
  slurm_defaults = { partition = "general" }
  ```
- **System per-project TOML** carries installation-specific paths/buckets/secrets:
  ```toml
  # ~/.config/pipelines/projects/em.toml
  [config]
  remote_store      = "gs://jspringe/projects/em"
  base_path         = "/data/user_data/jspringe/em-work"     # same fs as local_model_store
  local_model_store = "file:///data/user_data/jspringe/em-models"
  data_dir          = "/home/jspringe/projects/flexibility/data/em"
  wandb_api_key     = "..."
  ```

Parse with stdlib `tomllib` (Python ≥ 3.11). Discovery walks up from `from_file`'s directory collecting
every `pipelines.toml` until filesystem root (or a marker), applying ancestor→nearer order. The
`[project].name` in the versioned file must match the `Project.init` name (warn/error on mismatch).

`local_model_store` is **not** the transient `base_path`: it is a persistent `file://` Store for
checkpoint classes using `@artifact(store=...)` ([03 §5](03-storage-backends.md)). Putting `base_path`
on the same filesystem lets the file Store finalize with an atomic rename instead of a cross-filesystem
copy.

---

## 3. Store policy through configuration

The executor Store is the default for constructed artifacts; a class selects a configured alternate via
`@artifact(store=lambda: Project.config.local_model_store)` without losing default atomic lifecycle
(M-5). This expresses the real EM policy: evaluation outputs → GCS by default; large finetuned models →
configured local storage. Explicit executor args remain the final override:
`SlurmExecutor(store="file:///tmp/debug-store", base_path="/tmp/debug-cache")`.

---

## 4. Packaging (`pyproject.toml`)

```toml
[project]
name = "pipelines"
requires-python = ">=3.11"
dependencies = ["google-cloud-storage", "huggingface_hub"]

[project.optional-dependencies]
wandb = ["wandb"]
viz   = ["networkx"]            # plus a Graphviz binary on PATH
cli   = ["rich"]
analysis = ["pandas"]           # gather(...).to_frame()

[project.scripts]
pipelines = "pipelines.cli:main"     # console entry; also `python -m pipelines.worker`

[tool.setuptools.packages.find]
where = ["src"]                       # src/ layout decision (below)
```

- **Layout decision:** `src/pipelines/...` (src-layout) to avoid accidental import of the package from
  the repo root during tests, and to keep `examples/` importable as separate projects. (`examples/test`
  and `examples/em` import `from pipelines import ...` and `from . import steps` — they are *consumer*
  projects, not part of the package; tests install `pipelines` and run the examples against a temp
  store.)
- **Console script** `pipelines` → `cli:main`, which locates the project's `run.py` (via `--project` or
  CWD discovery), imports it, and dispatches. The examples also run as `python -m examples.test.run`
  style modules; both paths call `cli(...)`.
- **Worker** is `python -m pipelines.worker` ([06 §8](06-execution.md)).

---

## 5. Relationship to `experiments`

`experiments` provided `Project.config.<key>` via per-project JSON under `~/.experiments`. Pipelines
keeps the ergonomic attribute API and replaces format/layout with layered TOML: safe declarative TOML
instead of generated JSON; versioned defaults + optional sub-project inheritance; a system per-project
file for arbitrary keys (GCS/local roots); and **no job-history or completion state mixed into
configuration** (that lives in each selected Store, M-6).

---

## 6. Conformance hook

- `Project.init("test", from_file=__file__)` then `Project.config.document_names` / `remote_store` /
  `local_index_store` / `base_path` / `preview_prefix` resolve from `examples/test/pipelines.toml`,
  overridable by `~/.config/pipelines/projects/test.toml` (the `project.gcs.toml.example` content).
- `Project.init("em", ...)` then `Project.config.{remote_store,base_path,local_model_store,data_dir,
  slurm_setup,slurm_defaults}`.
- Missing key raises an error naming the system TOML path.
- `store=lambda: Project.config.local_index_store` / `local_model_store` resolves lazily after `init`.

Next: [11-roadmap-and-conformance.md](11-roadmap-and-conformance.md).
