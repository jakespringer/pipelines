# Runtime Helpers — `pipelines/runtime.py`

**Specifies:** `design/08-runtime-and-cluster.md` (`workspace`, `ctx`, `gs`, `source`/`fetch`, `run`,
`free_port`/`torchrun`/`sh`, annotations, env/setup, logging).
**Modules:** `pipelines/runtime.py`.
**Depends on:** [02 identity](02-identity-paths.md) (`slug`), [03 store](03-storage-backends.md)
(`gs` blob ops). The `_CTX` contextvar defined here is read by [01 §3](01-core-artifact.md) (M-1).
**Milestone:** M1 (`ctx`, `workspace`, `slug`, `free_port`, `sh`), M3+ (`run`, `gs`, `fetch`,
`torchrun`, `gpu_annotations`).

A small, sharp helper library for the recurring chores of ML-on-Slurm. **Everything here is optional** —
a `construct` that needs none of it imports none of it.

---

## 1. The runtime context — `ctx` and `_CTX` (M-1)

```python
@dataclass
class RuntimeContext:
    base_path: Path
    relpath: str
    annotations: dict
    gpu_ids: list[int]
    log: logging.Logger          # per-artifact logger
    session: "Session | None"
    env: dict
    executor_store: "Store"      # the executor default Store (used by _resolve_store, 01 §3)

_CTX: contextvars.ContextVar[RuntimeContext | None] = contextvars.ContextVar("pipelines_ctx", default=None)

class _CtxProxy:                 # `ctx` is a proxy reading _CTX.get() at access time
    @property
    def base_path(self): return _require().base_path
    ...                          # relpath, annotations, gpu_ids, log, session, env
    def metric(self, name, value, step=None): ...     # optional lightweight metric (design/08 §9)

ctx = _CtxProxy()
```

`_CTX` is set by the executor/worker around each `materialize` ([06 §1](06-execution.md)); it powers
`self.path` and `self.dep.path` ([01 §3](01-core-artifact.md)). `ctx` is the explicit, documented
replacement for `experiments`' implicit `EXPERIMENTS_*_CONF` env contract. Env vars are still exported
for child processes (subprocess/torchrun); the Python-facing API is `ctx`.

---

## 2. `workspace()` — fast scratch beyond `self.path`

```python
@contextmanager
def workspace(where: Literal["auto","shm","local"] = "auto", keep: bool = False) -> Iterator[Path]:
    ...   # fresh dir, auto-cleaned (unless keep); prefers /dev/shm then node-local
```
**Port** `temporary_workspace` from `experiments/runlib.py:207` (the `/dev/shm` preference). Only
`self.path` is committed; `workspace()` is ephemeral. Used by `examples/test` `MergedText` and
`examples/em` `ModelGenerations` (`with workspace() as tmp:`).

---

## 3. `gs` — direct blob I/O (escape hatch)

```python
class _GS:
    def read_text(self, uri, encoding="utf-8") -> str: ...
    def write_text(self, uri, text, encoding="utf-8") -> None: ...
    def read_bytes(self, uri) -> bytes: ...
    def write_bytes(self, uri, data) -> None: ...
    def list(self, prefix) -> list[str]: ...
    def exists(self, uris: list[str]) -> dict[str, bool]: ...     # batched
    def download(self, uri, local, recursive=False) -> None: ...
    def upload(self, local, uri) -> None: ...
gs = _GS()
```
**One explicit path rule:** trailing `/` ⇒ directory prefix; otherwise ⇒ a single object. Retries on
transient errors (5xx/rate-limit/timeout) with backoff; concurrent directory transfers. **Port** the
transfer/retry logic from `experiments/runlib.py` (`download_from_gs:565`, `upload_to_gs:700`,
`gs_read_text:1172`, `gs_write_text:1222`, `gs_list:1301`). Most `construct` bodies never touch this —
writing into `self.path` is the normal path; `gs.*` is for streaming logs / sharded writes.

---

## 4. `source.*` and `fetch`

External inputs are usually external Artifacts overriding `retrieve` with `source.*`
([04](04-sources.md), arriving as `self.dep.path`). For ad-hoc fetches inside a body:
```python
def fetch(uri: str, *, revision: str | None = None) -> Path:   # cached; returns local path
```
Prefer the declarative external-Artifact form (a cached, addressable graph node) over `fetch`.

---

## 5. `run` — the Pythonic command runner

```python
def run(cmd: str | list[str], args=None, *, fmt: str = "--{key} {value}",
        check: bool = True, env: dict | None = None, cwd=None) -> subprocess.CompletedProcess: ...
```
- **`cmd`** — base command (string or list): executable + fixed args.
- **`args`** — `None` | a **string** (appended verbatim) | a **dict** (formatted via `fmt`) | a **list**
  (appended item-by-item).
- **Dict value rules** (the exact contract — `design/08 §5`): keys map `_`→`-`; `None`/`False` ⇒ flag
  omitted; `True` ⇒ bare `--flag` (argparse `store_true`); a `dict` value ⇒ **JSON-encoded** string
  (`--sampling-params '{"temperature":0.7}'`); a `list`/`tuple` value ⇒ repeated values after the flag
  (`--input-path a.jsonl b.jsonl`, for `nargs="+"`); everything else ⇒ `--key value`. **All values are
  shell-quoted.**
- **`fmt`** — per-arg template; default `"--{key} {value}"`; override e.g. `"--{key}={value}"`.
- Real `subprocess`: `check=True` raises `CalledProcessError` with a real traceback; logs route to
  `ctx.log`; `env` merges into the runtime environment.

This is the easy path for the `design/03 §5` escape hatch and for big external tools.
`examples/em` `ModelGenerations` calls `run("python -m inference.scripts.generate", {...})` with a dict
including a nested `sampling_params` dict (JSON-encoded) and `enable_thinking=False` (omitted). Prefer a
library's Python API (held in a `Session`, [07](07-sessions.md)) when one exists; use `run` when a CLI is
the natural interface.

---

## 6. `free_port`, `torchrun`, `sh`

```python
def free_port() -> int: ...                                   # grab an unused TCP port for rendezvous
def torchrun(script, *args, nproc_per_node, nnodes=1, rdzv=None, port=None) -> CompletedProcess: ...
def sh(cmd: str, *, check=True, env=None) -> CompletedProcess: ...   # low-level raw-string subprocess
```
`torchrun` wraps the `torchrun` launcher, wiring rendezvous from `ctx` on multi-node Slurm; `run` builds
on `sh` (both log to `ctx.log`).

> **OPEN — multi-node training as one Artifact** (`design/08 §6`): a single Artifact whose
> `annotations` request `nnodes>1`; `srun`/`torchrun` fans out within the job. The graph stays
> one-Artifact-per-stage; multi-node is an annotation, not extra graph structure. The spec's resolution:
> `SlurmExecutor` reads `annotations["slurm"]["nodes"]`/portable `nnodes` and emits `--nodes`; `torchrun`
> reads `nnodes` from `ctx.annotations`.

---

## 7. Annotations helpers

`gpu_annotations(gpus, partition=None, cpus_per_gpu=8, mem_per_gpu="96G")` returns the common GPU-budget
dict so the boilerplate in `FinetunedModel`/`ModelGenerations`/`JudgedResponses` isn't copy-pasted.
Annotations resolution (3-level, portable→backend mapping) lives in
[06 annotations](06-execution.md); this module only provides the construction helper. Annotations are
**never identity** ([02](02-identity-paths.md)).

---

## 8. Environment, setup, logging

- Setup/env precedence (last wins, `design/08 §8`): `executor.setup` (shell, e.g. `conda activate llm`)
  < `executor.env` (dict) < artifact `env=` (rare). `setup` is the one intentional shell use (to enter
  the right Python); `env` is structured and inspectable in `dryrun`. `ctx` exposes the resolved env for
  child processes.
- `ctx.log` — per-artifact logger; output goes to a `relpath`-scoped log (committed next to the output)
  and the executor's aggregated stream. `ctx.metric(name, value, step=...)` — optional, mirrored to a
  `wandb://` mirror if configured ([03](03-storage-backends.md)). Timing/host/exit info is available to
  logs and optional metadata helpers; never auto-inserted into artifact outputs ([02 §7](02-identity-paths.md)).

---

## 9. `slug`

Re-exported here (`from pipelines.runtime import slug` is what the examples use) but **defined in**
[02 §3](02-identity-paths.md) to avoid an import inversion: `runtime.slug = identity.slug`.

---

## 10. Conformance hook

- `workspace()` yields an auto-cleaned dir; `MergedText`/`ModelGenerations` use it; only `self.path`
  persists.
- `run("python -m inference.scripts.generate", {...})` formats the `examples/em` dict exactly: nested
  `sampling_params`/`backend_kwargs` → JSON args, `model_path`→`--model-path`, booleans handled.
- `slug`, `free_port`, `sh` available; `ctx.base_path`/`ctx.relpath`/`ctx.session` populated during
  `materialize`; `self.path` raises cleanly outside a context.

Next: [09-cli.md](09-cli.md).
