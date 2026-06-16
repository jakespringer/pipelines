"""Build the ``ssh`` / ``rsync`` argv used to ship a run's events to the dashboard machine.

Pure command construction (no I/O) so it is unit-testable. Every small ``ssh`` call and the
``rsync`` transfers share **one** multiplexed TCP connection via ``ControlMaster``/``ControlPersist``
— so a per-tick heartbeat + a couple of rsyncs cost one round-trip, not several handshakes.

``BatchMode=yes`` is load-bearing: a missing key or an unknown host must fail *immediately* (→ the
Shipper backs off), never block the thread waiting for a password or a host-key prompt.
"""

from __future__ import annotations

import shlex
import tempfile
from pathlib import Path

from .config import DashboardConfig

_CONTROL_DIR = Path(tempfile.gettempdir()) / "pipelines-ssh"


def control_dir() -> Path:
    _CONTROL_DIR.mkdir(parents=True, exist_ok=True)
    return _CONTROL_DIR


def base_ssh_opts(cfg: DashboardConfig) -> list[str]:
    """The ``-o`` options shared by every ssh invocation and embedded in rsync's ``-e`` string."""
    control_path = str(control_dir() / "%C")          # %C: short hash of (host, port, user, ...)
    opts = [
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", f"ConnectTimeout={int(cfg.connect_timeout)}",
        "-o", "ServerAliveInterval=15",
        "-o", "ServerAliveCountMax=3",
        "-o", "ControlMaster=auto",
        "-o", f"ControlPath={control_path}",
        "-o", f"ControlPersist={int(cfg.control_persist)}",
    ]
    if cfg.port and int(cfg.port) != 22:
        opts += ["-p", str(int(cfg.port))]
    if cfg.identity:
        opts += ["-i", str(Path(cfg.identity).expanduser())]
    return opts


def _target(cfg: DashboardConfig) -> str:
    return f"{cfg.user}@{cfg.host}" if cfg.user else str(cfg.host)


def ssh_argv(cfg: DashboardConfig, remote_cmd: str) -> list[str]:
    """``ssh <opts> <target> <remote_cmd>`` — ``remote_cmd`` is a single shell string."""
    return ["ssh", *base_ssh_opts(cfg), _target(cfg), remote_cmd]


def ssh_exit_argv(cfg: DashboardConfig) -> list[str]:
    """Drop the shared ControlMaster connection (best-effort cleanup on shutdown)."""
    return ["ssh", *base_ssh_opts(cfg), "-O", "exit", _target(cfg)]


def rsync_e(cfg: DashboardConfig) -> str:
    """The ``-e`` transport string for rsync: ``ssh`` plus the shared options, shell-quoted."""
    return " ".join(shlex.quote(tok) for tok in ["ssh", *base_ssh_opts(cfg)])


def rsync_argv(cfg: DashboardConfig, local_path, remote_path: str, *,
               append: bool, recursive: bool = False) -> list[str]:
    """``rsync`` argv. ``append`` ⇒ ``--append --inplace`` (correct only for append-only files).

    ``remote_path`` is left unquoted so a leading ``~`` expands on the remote shell. For a recursive
    transfer the source gets a trailing slash so its *contents* land under ``remote_path``.
    """
    flags = ["-q"]
    if recursive:
        flags.append("-r")
    if append:
        flags += ["--append", "--inplace"]
    src = f"{str(local_path).rstrip('/')}/" if recursive else str(local_path)
    return ["rsync", *flags, "-e", rsync_e(cfg), src, f"{_target(cfg)}:{remote_path}"]
