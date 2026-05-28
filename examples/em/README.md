# `em` — Emergent Misalignment (a `pipelines` **v2** wiring mockup)

A re-creation of the `flexibility_stages/em` experiment group as it would look
under the current **v2** design ([`../../design/`](../../design)).

**It does not run** — the `pipelines` framework isn't built yet. It shows the
*authoring experience*: the artifacts, the construction logic, and the wiring.
Code shows the intended v2 API; signatures may shift.

## The experiment

Per cell, a 4-stage pipeline studies whether finetuning on subtly-bad advice
induces broadly misaligned behavior:

```
RawJsonl ─┬─> JsonDataset ──> FinetunedModel ──> ModelGenerations ──> JudgedResponses ─┐
          │     (HFModel base) ^                  (× eval sets)        (rubric / set)   │
          └─> SplitPrompts / RawPrompts ──────────^                                     ▼
                                                                          MisalignmentReport
```

Three FT regimes, each its own wiring file, all sharing one artifact layer:

| file | regime | swept axis |
|------|--------|-----------|
| `experiment_baseline.py` | LoRA on Qwen2.5-14B | learning rate |
| `experiment_scaling.py`  | full-FT             | model size 0.5B→32B (+ LR) |
| `experiment_layerft.py`  | 3-layer-block FT    | which block (+ LR) |

## Files

| file | what it holds |
|------|---------------|
| `steps.py` | **framework-agnostic pure logic** — split / extract / rewrite / judge-input / join / unsafe-rate. No `pipelines`, no `inference`, no vLLM. |
| `artifacts.py` | all `@artifact` definitions + the `JudgeEngine` `Session` |
| `judge_prompts.py` | rubric stubs, keyed by eval set |
| `experiment_*.py` | wiring (instantiate artifacts, build the target lists) |
| `run.py` | entry point — `cli(groups={...})` |
| `pipelines.toml` | checked-in project name and portable defaults |
| `project.local.toml.example` | shape of `~/.config/pipelines/projects/em.toml` |

## What v2 looks like here

### 1. `relpath` is the identity *and* the location
Every artifact has a human-readable `relpath` (`@property`) — its identity, its
local materialization path (`self.path = base_path / relpath`), and its default
durable layout. **No content hash.** There's no `key`, no `name` template, no
`exists`/`upload_to_gs` boilerplate. Non-equal artifacts that render the same
`relpath` in one graph are an error. (See [`design/02`](../../design/02-identity-and-storage.md).)

### 2. The four-function contract, with defaults
`construct(self)` writes the output into `self.path`; `retrieve`/`exists`/`commit`
have working store-backed defaults. External inputs (`HFModel`, `RawJsonl`)
override only `retrieve(*, only=None)`, via `source.*` helpers — no `Source`
type, no `locate()`. The `only` parameter is the partial-materialization
contract; a source may fall back to fetching all contents. Default `commit`
publishes exactly the artifact's output directory atomically; it does not add
a `meta.json` or any other provenance file.

### 3. Storage matches the real EM experiment
The real `flexibility_stages/em` pipeline uploads its prepared data and
evaluation outputs to Google Cloud Storage, but configures every
`FinetunedModel` with `storage="local"` to avoid uploading large checkpoints.
This example represents that directly:

| Artifact kind | Selected Store |
|---------------|----------------|
| `JsonDataset`, prompts, generations, judgements, reports | executor default: `Project.config.remote_store` (`gs://...`) |
| `FinetunedModel` | `@artifact(store=local_model_store)`: `Project.config.local_model_store` (`file://...`) |
| `HFModel`, `RawJsonl` | external `source.*` fetches; no constructed-output commit |

The file Store still gets the normal atomic `commit`/`exists`/`retrieve`
semantics; it is just not GCS. Its path must be visible to both training and
generation jobs. A shared persistent filesystem satisfies that directly; a
node-private path requires executor co-location. Configure `base_path` on the
same filesystem as `local_model_store` so the file Store can atomically rename
the completed checkpoint into place rather than copying it or sending it over
the network.

### 4. Project configuration is arbitrary-key TOML
Like `experiments`' `Project.config.<key>`, the example initializes a project
before importing its wiring:

```python
Project.init("em", from_file=__file__)
Project.config.remote_store
Project.config.local_model_store
Project.config.data_dir
```

Checked-in [`pipelines.toml`](pipelines.toml) supplies portable defaults.
Machine/project settings are overlaid from
`~/.config/pipelines/projects/em.toml`; see
[`project.local.toml.example`](project.local.toml.example). Any key under
`[config]` is available as `Project.config.<key>`, so a project may add paths,
bucket URIs, service settings, or secrets without expanding the framework API.
For example, `experiment_baseline.py` now gets its input directory from
`Project.config.data_dir`.

### 5. Big tasks call the `inference` library — not re-implemented
- **`ModelGenerations`** shells out to `inference.scripts.generate` through the
  `run` helper: args as a **dict, formatted `--key value`** (dict values like
  `sampling_params` are JSON-encoded automatically). This is the easy, readable
  way to run a command with arguments.
- **`JudgedResponses`** uses the **`JudgeEngine` `Session`**, which holds one
  `inference.GenerationModel`: the 27B judge loads **once per machine** and every
  co-located cell calls it. `steps.py` only owns the small transforms around it
  (build judge conversations, join judgements).

### 6. Shared judge = a `Session`, not a graph hack
The original's single most complex piece — `BatchedJudgedResponses` +
`make_batched_judged_responses` + `_BATCH_SHARED_FIELDS` + `_round_robin_shards`
(~260 lines) — existed solely to load the 27B judge once for many cells. Here
that's the `JudgeEngine` `Session` ([`design/07`](../../design/07-sessions.md)):
its `group_key` co-locates cells that share an engine; `open()` loads the model
once; members query `self.session.model`. The rubric travels per-row (in each
conversation's `system` message), so cells with different `rubric_key`s still
share one load. `JudgedResponses` stays a clean, individually-addressable unit.

> Because the `inference` library has no server, the `Session` is the
> **in-process variant** (members run in one process and call the in-memory
> model). If `inference` grows a server, only `JudgeEngine.open` changes — the
> members keep querying `self.session`.

### 7. Wiring is plain Python; `cli(groups={...})`
No `executor.stage(...)`/`stage_group(...)`. A **group is just an alias for a
list of artifacts**; groups overlap and compose (`all_inference` is a union over
the experiments). Launch by group name *or* a `relpath` glob:

```bash
pipelines dryrun baseline
pipelines run scaling --target 'model/*32B*'
pipelines run 'judged/*/insecure'   # bare relpath glob
```

### 8. Values flow through `@derived`
`construct` returns nothing. The misalignment metric is
`JudgedResponses.unsafe_rate` — a `@derived(reads=...)` that **partial-retrieves
just the judged file** and auto-materializes on access (great for post-hoc
analysis on a laptop). The same channel is what an `argmax(models, key=...)`
selection would use.

## Differences from v1 (the previous mockup)

This example was rewritten from the v1 design. The visible shifts: `@artifact`
instead of `class X(Artifact)`; `relpath`/`self.path` instead of `name`/`key` +
ambient `.path()`; `Session` instead of an `@lru_cache`d engine + a batch
artifact; `cli(groups=...)` + `relpath` globs instead of `default_target`; and
generation/judging now call the `inference` library (CLI via `run`, Python API
via the `Session`) instead of a local vLLM wrapper in `steps.py`. The full
rationale is in [`design/11`](../../design/11-comparison-and-migration.md).
