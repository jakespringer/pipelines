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
import shlex
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path
from typing import Iterator

from .identity import slug   # re-exported: `from pipelines.runtime import slug`

__all__ = ["ctx", "workspace", "run", "sh", "free_port", "gpu_annotations",
           "slug", "RuntimeContext"]


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


def gpu_annotations(gpus: int, *, partition: str | None = None,
                    cpus_per_gpu: int = 8, mem_per_gpu: str = "96G") -> dict:
    """The common GPU-budget annotations shape."""
    g = max(1, gpus)
    ann: dict = {"gpus": gpus, "cpus": g * cpus_per_gpu,
                 "memory": f"{g * int(mem_per_gpu.rstrip('G'))}G"}
    if partition:
        ann["slurm"] = {"partition": partition}
    return ann
