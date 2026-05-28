# Runtime Library and Cluster Helpers

Inside `construct` (and `@derived`, `Session.open`) you write plain Python — but ML-on-Slurm has
recurring chores: scratch space, the runtime context, store I/O, external fetches, free ports and
distributed launch, execution annotations, environment/setup, logging. This is the small, sharp
helper library that covers them. Everything here is **optional** — a `construct` that needs none of
it imports none of it.

---

## 1. Workspaces — fast scratch beyond `self.path`

`self.path` is the published output. For *ephemeral* scratch, `workspace()`:

```python
from pipelines import workspace
def construct(self) -> None:
    with workspace() as tmp:               # fresh, auto-cleaned; prefers /dev/shm, then node-local
        preprocess(self.data.path, tmp)
        train(data=tmp, out=self.path)     # only self.path is committed
```

`workspace(where="shm"|"local"|"auto", keep=False)`. Fetched deps, `self.path`, and `workspace()`
share the fast local tier ([04](04-retrieval-and-storage.md) §7).

---

## 2. The runtime context — `ctx`

When `construct`/`@derived`/`Session.open` run, they can read the current materialization context:

```python
from pipelines.runtime import ctx

ctx.base_path     # the local base path for this device (self.path = base_path / relpath)
ctx.relpath       # this artifact's relpath
ctx.annotations   # resolved annotations, e.g. {"gpus": 4, ...}
ctx.gpu_ids       # concrete device ids granted (parallel/cluster)
ctx.log           # a configured per-artifact logger
ctx.session       # the open Session for this batch, if any (07)
```

`ctx` is an explicit, documented object populated by the worker entry point (replacing
`experiments`' implicit `EXPERIMENTS_*_CONF` env-var contract). Env vars are still exported for
child processes (subprocess/torchrun), but the Python-facing API is `ctx`.

---

## 3. Store / blob I/O helpers — `gs`

For direct blob I/O beyond the directory model (streaming logs, sharded writes). One explicit rule —
**trailing `/` ⇒ directory prefix; otherwise ⇒ a single object** — no `/.` magic, no probing:

```python
from pipelines.runtime import gs
gs.read_text("gs://b/f.txt"); gs.write_bytes("gs://b/raw.bin", data)
gs.list("gs://b/prefix/"); gs.exists(["gs://b/a", "gs://b/c"])     # batched -> {path: bool}
gs.download("gs://b/dir/", local, recursive=True); gs.upload(local, "gs://b/dir/")
```

Retries on transient errors (5xx/rate-limit/timeout) with backoff; concurrent directory transfers.
Most `construct` bodies never touch this — writing into `self.path` is the normal path.

---

## 4. External inputs — `source.*` and `fetch`

External inputs are usually **external Artifacts** that override `retrieve` with `source.*`
([04](04-retrieval-and-storage.md) §6), arriving as `self.dep.path`. For ad-hoc fetches inside a
body:

```python
from pipelines.runtime import fetch
local = fetch("https://example.com/data.tar.gz")     # cached
hf    = fetch("hf://meta-llama/Llama-3-8B", revision="main")
```

Prefer the declarative external-Artifact form (a cached, addressable graph node) over ad-hoc `fetch`.

---

## 5. Running commands — `run`

Calling another tool (a training script, `inference.scripts.generate`, …) should be **easy to read
and Pythonic**. `run` takes a base command plus arguments as **either a raw string or a dict**, and
formats a dict as `--key value` by default:

```python
from pipelines.runtime import run

# raw-string form
run("python -m inference.scripts.generate --backend vllm --model-path /ckpt --input-path in.jsonl")

# dict form — formats "--key value" by default; keys map _ -> -
run("python -m inference.scripts.generate", {
    "backend":         "vllm",
    "model_path":      model_dir,                       # -> --model-path <model_dir>
    "input_path":      prompts_file,
    "output_path":     out_file,
    "sampling_params": {"temperature": 0.7, "max_tokens": 512},   # dict -> JSON-encoded value
    "n_samples":       4,
    "enable_thinking": False,                            # False/None -> omitted
})
```

`run(cmd, args=None, *, fmt="--{key} {value}", check=True, env=None, cwd=None) -> CompletedProcess`:

- **`cmd`** — the base command (a string or a list); the executable + any fixed args.
- **`args`** — `None` | a **string** (appended verbatim) | a **dict** (formatted via `fmt`) | a list
  (appended item-by-item).
- **Dict value rules** (so the common cases read cleanly): keys map `_`→`-`; `None`/`False` ⇒ the
  flag is omitted; `True` ⇒ a bare `--flag` (argparse `store_true`); a `dict` value ⇒
  **JSON-encoded** string (`--sampling-params '{"temperature":0.7}'`); a `list`/`tuple` ⇒ repeated
  values after the flag (`--input-path a.jsonl b.jsonl`, for `nargs="+"`); everything else ⇒
  `--key value`. All values are shell-quoted.
- **`fmt`** — the per-arg template; default `"--{key} {value}"`, override e.g. `"--{key}={value}"`.
- Real `subprocess`: `check=True` raises `CalledProcessError` with a real traceback; logs route to
  `ctx.log`; `env` merges into the runtime environment.

`run` is the easy path for the **escape hatch** of [03 §5](03-construction.md) and for calling big
external tools — e.g. the `em` example invokes `inference.scripts.generate` through it (see
[`../../examples/em/`](../../examples/em)). Prefer calling a library's **Python API** when one
exists (e.g. holding an `inference.GenerationModel` in a `Session` — [07](07-sessions.md)); use
`run` when a CLI is the natural interface.

## 6. Free port and distributed launch

```python
from pipelines.runtime import free_port, torchrun, ctx

def construct(self) -> None:
    torchrun("train.py", "--out", str(self.path),
             nproc_per_node=ctx.annotations["gpus"], port=free_port())
```

- `free_port()` — grab an unused TCP port for rendezvous.
- `torchrun(script, *args, nproc_per_node, nnodes=1, rdzv=...)` — wraps `torchrun`, wiring
  rendezvous from `ctx` on multi-node Slurm.
- `sh(cmd, *, check=True, env=...)` — the low-level raw-string subprocess wrapper `run` builds on
  (logs to `ctx.log`).

> **OPEN — multi-node training as one Artifact:** modeled as a single Artifact whose `annotations`
> request `nnodes>1`; `srun`/`torchrun` fans out within the job. The graph stays
> one-Artifact-per-stage; multi-node is an annotation, not extra graph structure.

---

## 7. Annotations — environment-neutral hints

Artifacts describe execution needs through **`annotations`**, an open, namespaced bag — not
Slurm-specific. Each executor consumes the keys it understands and ignores the rest. Always read as
`artifact.annotations`; **never part of identity** (identity is `relpath`, [02](02-identity-and-storage.md)).

```python
@artifact
class FinetunedModel:
    num_layers: int = 24
    @property
    def annotations(self) -> dict:
        big = self.num_layers > 24
        return {
            "gpus": 8 if big else 4, "cpus": 32, "memory": "128G" if big else "64G",  # portable
            "slurm": {"partition": "general", "qos": "high"},                          # namespaced
            "owner": "jake", "tags": ["pretrain"],                                     # free metadata
        }
```

- **Portable intent** (`gpus`, `cpus`, `memory`, `runtime`) is mapped by each executor to its own
  directives — one canonical key per concept; backends translate internally.
- **Namespaced sections** (`slurm`, `k8s`, …) are merged only by the matching backend.
- **No silent drop.** Each executor declares which keys it consumes; `dryrun` warns about keys
  nothing consumed.
- **Resolution is three explicit levels (last wins):**
  `executor.defaults < artifact.annotations (incl. its "slurm") < CLI --annotate k=v`.
- **Computed annotations must be pure functions of config** — `dryrun` and the launcher both
  evaluate `artifact.annotations` and must agree.
- A `gpu_annotations(gpus, partition=...)` helper covers the common GPU-budget shape.

---

## 8. Environment and setup

A job's environment comes from explicit, inspectable places (last wins):

```
executor.setup     # shell run before the worker, e.g. "conda activate llm"
executor.env       # dict merged into the job environment
artifact env=      # rare per-artifact overrides
```

`setup` is the one place we intentionally use shell (to enter the right Python). `env` is a
structured dict, inspectable in `dryrun`. `ctx` exposes the resolved environment for child processes.
The default store root, any per-artifact Store roots, `base_path`, setup, and defaults come from
`Project.config` or explicit executor args — see [10-configuration.md](10-configuration.md).

---

## 9. Logging

- `ctx.log` — a per-artifact logger; output goes to a `relpath`-scoped log (committed next to the
  output) **and** the executor's aggregated stream.
- `ctx.metric(name, value, step=...)` — optional lightweight metric emission, mirrored to a
  `wandb://` mirror if configured ([04](04-retrieval-and-storage.md) §7). An artifact may also write
  emitted metrics into its own metadata output.
- Timing/host/exit information is available to logs and optional metadata helpers; it is not
  inserted into artifact outputs automatically ([02](02-identity-and-storage.md) §8).

---

## 10. Summary

- `workspace()` — fast auto-cleaned scratch beyond `self.path`.
- `ctx` — explicit runtime context (`base_path`, `relpath`, annotations, gpu ids, logger, session).
- `gs.*` — direct store I/O with one path rule, retries, concurrency (an escape hatch).
- **`run(cmd, args)`** — Pythonic command runner; `args` as a string or a dict (`--key value` by
  default), JSON-encoding dict values, repeating list values; prefer a library's Python API when one
  exists.
- `source.*` / `fetch` — external inputs; `free_port`/`torchrun`/`sh` — launch helpers.
- `annotations` — open, namespaced, environment-neutral; three-level resolution; never identity.
- `setup`/`env` explicit and inspectable; config comes from `Project.config`/executor args
  ([10](10-configuration.md)).

Next: [09-cli-and-observability.md](09-cli-and-observability.md).
