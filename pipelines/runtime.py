"""Runtime context and helpers available inside ``construct``/``@derived``/``Session``.

The ``_CTX`` contextvar carries the active executor's ``base_path`` (so ``self.path``
resolves) plus the selected default store, session, annotations, and logger. Everything
here is optional — a ``construct`` that needs none of it imports none of it.
See ``docs/08-runtime-helpers.md``.
"""

from __future__ import annotations

import contextlib
import contextvars
import dataclasses
import json
import logging
import os
import shlex
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path
from typing import Iterator

from .identity import slug   # re-exported: `from pipelines.runtime import slug`

__all__ = ["ctx", "workspace", "run", "sh", "free_port", "gpu_annotations",
           "slug", "RuntimeContext", "configure_logging", "yield_now",
           "python", "python_prefix"]


# --------------------------------------------------------------------------- #
# Verbosity / narration
# --------------------------------------------------------------------------- #
# The whole framework logs to the single ``logging.getLogger("pipelines")``
# logger. By default that logger is unconfigured, so the narration below is
# silent — a library should not print uninvited. Setting ``PIPELINES_VERBOSITY``
# (e.g. ``PIPELINES_VERBOSITY=info``) attaches a stderr handler at that level so
# the framework narrates what it is doing: which files it checks for, whether
# those checks resolve to existing, what it builds, retrieves, and publishes.

_VERBOSITY_ENV = "PIPELINES_VERBOSITY"
_LOGGING_CONFIGURED = False


def configure_logging() -> None:
    """Configure the ``pipelines`` logger from ``$PIPELINES_VERBOSITY``.

    The value names a standard level (``debug``/``info``/``warning``/``error``/
    ``critical``, case-insensitive). When set, the ``pipelines`` logger is put at
    that level and given a dedicated stderr handler that narrates framework
    activity. Unset, empty, or unrecognized values leave logging untouched, so
    the default behavior stays quiet. Idempotent: safe to call repeatedly.
    """
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
    raw = os.environ.get(_VERBOSITY_ENV, "").strip()
    if not raw:
        return
    level = getattr(logging, raw.upper(), None)
    if not isinstance(level, int):           # unknown value: don't break import
        return
    logger = logging.getLogger("pipelines")
    logger.setLevel(level)
    if not any(getattr(h, "_pipelines_narrator", False) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setLevel(level)
        handler.setFormatter(logging.Formatter("[pipelines %(levelname)s] %(message)s"))
        handler._pipelines_narrator = True   # mark so we attach exactly one
        logger.addHandler(handler)
        logger.propagate = False             # don't double-print via the root logger
    _LOGGING_CONFIGURED = True
    logger.info("verbosity enabled at level %s (from %s)", raw.lower(), _VERBOSITY_ENV)


# --------------------------------------------------------------------------- #
# The runtime context (set by the executor/worker around each materialize).
# --------------------------------------------------------------------------- #

@dataclasses.dataclass
class RuntimeContext:
    base_path: Path
    relpath: str = ""
    annotations: dict = dataclasses.field(default_factory=dict)
    gpu_ids: list = dataclasses.field(default_factory=list)
    log: logging.Logger = dataclasses.field(
        default_factory=lambda: logging.getLogger("pipelines"))
    session: object | None = None
    env: dict = dataclasses.field(default_factory=dict)
    # The argv prefix subprocess shell-outs use for ``python`` (see ``python``
    # below). The executor sets this from its conda-env config; the default
    # ``["python"]`` inherits the orchestrator's environment.
    python: list = dataclasses.field(default_factory=lambda: ["python"])
    executor_store: object | None = None


_CTX: contextvars.ContextVar[RuntimeContext | None] = \
    contextvars.ContextVar("pipelines_ctx", default=None)
# Per-run memoization cache for resolved derived/future values.
_RESULT_CACHE: contextvars.ContextVar[dict | None] = \
    contextvars.ContextVar("pipelines_result_cache", default=None)


class RuntimeContextError(RuntimeError):
    """Raised when context-dependent state (``self.path``, ``ctx``) is read with no executor."""


def _require() -> RuntimeContext:
    cur = _CTX.get()
    if cur is None:
        raise RuntimeContextError(
            "no active executor; self.path / ctx require running under an executor")
    return cur


class _CtxProxy:
    """Reads the active :class:`RuntimeContext` at attribute-access time."""

    @property
    def base_path(self) -> Path: return _require().base_path
    @property
    def relpath(self) -> str: return _require().relpath
    @property
    def annotations(self) -> dict: return _require().annotations
    @property
    def gpu_ids(self) -> list: return _require().gpu_ids
    @property
    def log(self) -> logging.Logger: return _require().log
    @property
    def session(self): return _require().session
    @property
    def env(self) -> dict: return _require().env
    @property
    def python(self) -> list: return _require().python

    def metric(self, name: str, value, step: int | None = None) -> None:
        _require().log.info("metric %s=%s%s", name, value,
                            f" step={step}" if step is not None else "")


ctx = _CtxProxy()


# --------------------------------------------------------------------------- #
# Scratch space
# --------------------------------------------------------------------------- #

@contextlib.contextmanager
def workspace(where: str = "auto", keep: bool = False) -> Iterator[Path]:
    """A fresh, auto-cleaned scratch directory. Prefers /dev/shm, then node-local."""
    roots = {
        "shm": [Path("/dev/shm")],
        "local": [Path(tempfile.gettempdir())],
        "auto": [Path("/dev/shm"), Path(tempfile.gettempdir())],
    }[where]
    root = next((r for r in roots if r.is_dir()), Path(tempfile.gettempdir()))
    path = Path(tempfile.mkdtemp(prefix="pipelines-ws-", dir=root))
    try:
        yield path
    finally:
        if not keep:
            shutil.rmtree(path, ignore_errors=True)


# --------------------------------------------------------------------------- #
# Subprocess helpers
# --------------------------------------------------------------------------- #

def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def python_prefix(conda_env=None, python_bin=None,
                  conda_run_args=("--no-capture-output",)) -> list[str]:
    """Resolve a conda-env / interpreter spec into an argv prefix for ``python``.

    ``python_bin`` (an explicit interpreter path) wins when set; else ``conda_env``
    activates the env via ``conda run`` (full activation: ``CONDA_PREFIX``,
    ``LD_LIBRARY_PATH``); else bare ``["python"]`` (inherits the orchestrator's
    environment — today's behavior). Executors call this once and stash the result
    on :class:`RuntimeContext.python`.
    """
    if python_bin:
        return [str(python_bin)]
    if conda_env:
        return ["conda", "run", *conda_run_args, "-n", str(conda_env), "python"]
    return ["python"]


def python(*args) -> list[str]:
    """Argv for invoking python in the active executor's configured environment.

    Reads the resolved prefix from :data:`ctx.python` (set by the executor from its
    conda-env config) and appends ``args``. Falls back to bare ``python`` when no
    executor is active. Use it where a ``construct`` would otherwise hardcode the
    interpreter — ``run(python("-m", "pkg.mod"), ...)`` — so the subprocess lands in
    the chosen conda env. (``run``/``sh`` already accept a list ``cmd``.)
    """
    rc = _CTX.get()
    prefix = list(rc.python) if rc and rc.python else ["python"]
    return [*prefix, *(str(a) for a in args)]


def sh(cmd: str, *, check: bool = True, env: dict | None = None,
       cwd=None) -> subprocess.CompletedProcess:
    """Low-level raw-string subprocess wrapper; logs to ``ctx.log`` when available."""
    log = _CTX.get().log if _CTX.get() else logging.getLogger("pipelines")
    log.info("$ %s", cmd)
    full_env = None
    if env is not None:
        import os
        full_env = {**os.environ, **env}
    return subprocess.run(cmd, shell=True, check=check, env=full_env, cwd=cwd)


def _format_args(args, fmt: str) -> list[str]:
    """Format a dict/list/str of arguments per the ``run`` contract (docs/08 §5)."""
    if args is None:
        return []
    if isinstance(args, str):
        return shlex.split(args)
    if isinstance(args, (list, tuple)):
        return [str(a) for a in args]
    if not isinstance(args, dict):
        raise TypeError(f"args must be None|str|list|dict, got {type(args).__name__}")

    out: list[str] = []
    for key, value in args.items():
        flag = "--" + str(key).replace("_", "-")
        if value is None or value is False:
            continue
        if value is True:
            out.append(flag)
            continue
        if isinstance(value, dict):
            rendered = json.dumps(value, separators=(",", ":"))
            out += _split_fmt(fmt, flag, rendered)
        elif isinstance(value, (list, tuple)):
            out.append(flag)
            out += [str(v) for v in value]
        else:
            out += _split_fmt(fmt, flag, str(value))
    return out


def _split_fmt(fmt: str, flag: str, value: str) -> list[str]:
    # Render "--{key} {value}" -> ["--flag", "value"]; "--{key}={value}" -> ["--flag=value"].
    rendered = fmt.format(key=flag[2:], value=value)
    return rendered.split(" ", 1) if " " in fmt else [rendered]


def run(cmd, args=None, *, fmt: str = "--{key} {value}", check: bool = True,
        env: dict | None = None, cwd=None) -> subprocess.CompletedProcess:
    """Pythonic command runner. ``args`` may be a string, list, or dict (``--key value``)."""
    base = shlex.split(cmd) if isinstance(cmd, str) else [str(c) for c in cmd]
    argv = base + _format_args(args, fmt)
    log = _CTX.get().log if _CTX.get() else logging.getLogger("pipelines")
    log.info("$ %s", " ".join(shlex.quote(a) for a in argv))
    full_env = None
    if env is not None:
        import os
        full_env = {**os.environ, **env}
    return subprocess.run(argv, check=check, env=full_env, cwd=cwd)


# --------------------------------------------------------------------------- #
# Cooperative resource yielding (ParallelExecutor)
# --------------------------------------------------------------------------- #
_YIELDED = False


def yield_now() -> None:
    """Tell the parallel run server this job has freed its heavy resources.

    Call this from ``construct`` once the GPU/CPU-intensive work is done and only
    lightweight finishing remains (saving final results, uploading). The scheduler
    reclaims this job's GPUs/CPUs for other work while the job keeps running until it
    exits — the "yielding" state in ``pipelines attach``.

    No-op unless running under :class:`pipelines.ParallelExecutor` (the server address and a
    per-job token arrive via ``PIPELINES_SERVER_PORT``/``PIPELINES_JOB_TOKEN``). Idempotent.
    Setting ``PIPELINES_YIELD=1`` for a job makes the worker call this automatically right
    before its commit/upload step.
    """
    global _YIELDED
    if _YIELDED:
        return
    port = os.environ.get("PIPELINES_SERVER_PORT")
    token = os.environ.get("PIPELINES_JOB_TOKEN")
    log = logging.getLogger("pipelines")
    if not port or not token:
        return                                  # not under a parallel run; nothing to yield to
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=5.0) as s:
            s.sendall((json.dumps({"cmd": "yield", "token": token}) + "\n").encode("utf-8"))
        _YIELDED = True
        log.info("yielded resources back to the parallel scheduler")
    except OSError as e:
        log.info("yield_now: could not reach scheduler (%s)", e)


def gpu_annotations(gpus: int, *, partition: str | None = None,
                    cpus_per_gpu: int = 8, mem_per_gpu: str = "96G") -> dict:
    """The common GPU-budget annotations shape."""
    g = max(1, gpus)
    ann: dict = {"gpus": gpus, "cpus": g * cpus_per_gpu,
                 "memory": f"{g * int(mem_per_gpu.rstrip('G'))}G"}
    if partition:
        ann["slurm"] = {"partition": partition}
    return ann
