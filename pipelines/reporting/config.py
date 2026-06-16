"""``DashboardConfig`` — the ``[config.dashboard]`` table that turns on SSH reporting.

Off by default: a run ships its events to a dashboard machine over SSH only when the project's
``pipelines.toml`` (or a system overlay) provides a ``host`` (or sets ``enabled = true``). With no
``[config.dashboard]`` at all the run behaves exactly as before — a purely local ``events.log`` and
no network. See ``docs/12-dashboard.md``.

The connection details live in the per-project system overlay
(``~/.config/pipelines/projects/<name>.toml``) so secrets/hostnames stay out of the repo, while a
versioned ``pipelines.toml`` can carry the switches (``enabled``/``metrics``). Both merge through
``Project.config``.
"""

from __future__ import annotations

import logging
import re
import socket
from dataclasses import dataclass

log = logging.getLogger("pipelines")

# ``runs_dir`` is interpolated **unquoted** into a remote shell command (so a leading ``~`` expands
# to the remote home). Restrict it to a safe charset to keep that injection-free — it is trusted
# config, not user input, but a stray space/metachar would still break the command.
_SAFE_REMOTE_PATH = re.compile(r"^[\w./~@+-]+$")


@dataclass(frozen=True)
class DashboardConfig:
    """Where and how to ship a run's events to the dashboard machine. Disabled unless ``host`` set."""

    host: str | None = None
    port: int = 22
    user: str | None = None
    identity: str | None = None                 # path to an SSH private key; omit to use ssh-agent
    runs_dir: str = "~/.cache/pipelines/runs"   # the *remote* registry dir (rsync/cat destination)
    metrics: bool = False                       # also sample + ship this host's system metrics
    interval: float = 5.0                       # Shipper tick (s); drives heartbeat_stale
    backoff_max: float = 300.0                  # cap on the exponential retry interval (s)
    connect_timeout: float = 10.0               # ssh ConnectTimeout (s)
    control_persist: float = 60.0               # ssh ControlPersist (s)
    node: str = ""                              # reported node id; blank ⇒ this host's short name
    master_enabled: bool = True                 # explicit ``enabled = false`` forces off

    @property
    def enabled(self) -> bool:
        return bool(self.host) and self.master_enabled

    @property
    def node_id(self) -> str:
        return self.node or (socket.gethostname().split(".")[0] or "localhost")

    @property
    def remote_metrics_dir(self) -> str:
        """``<remote>/metrics`` — the sibling of ``runs_dir`` (mirrors ``metrics.metrics_dir()``)."""
        rd = self.runs_dir.rstrip("/")
        parent, _, _ = rd.rpartition("/")
        return f"{parent}/metrics" if parent else "metrics"

    # ------------------------------------------------------------------ #
    @classmethod
    def from_config(cls, table) -> "DashboardConfig":
        """Build from the ``[config.dashboard]`` table; disabled on absence or malformed input."""
        if not isinstance(table, dict) or not table:
            return cls()
        try:
            cfg = cls(
                host=_str_or_none(table.get("host")),
                port=int(table.get("port", 22)),
                user=_str_or_none(table.get("user")),
                identity=_str_or_none(table.get("identity")),
                runs_dir=str(table.get("runs_dir") or "~/.cache/pipelines/runs"),
                metrics=bool(table.get("metrics", False)),
                interval=float(table.get("interval", 5.0)),
                backoff_max=float(table.get("backoff_max", 300.0)),
                connect_timeout=float(table.get("connect_timeout", 10.0)),
                control_persist=float(table.get("control_persist", 60.0)),
                node=str(table.get("node") or ""),
                master_enabled=bool(table.get("enabled", True)),
            )
        except (TypeError, ValueError) as exc:
            log.warning("pipelines: ignoring malformed [config.dashboard]: %s", exc)
            return cls()
        if cfg.host and not _SAFE_REMOTE_PATH.match(cfg.runs_dir):
            log.warning("pipelines: [config.dashboard].runs_dir %r has unsafe characters; "
                        "disabling dashboard reporting", cfg.runs_dir)
            return cls()
        return cfg

    @classmethod
    def from_project(cls) -> "DashboardConfig":
        """Read ``Project.config[dashboard]``; disabled (never raises) if unset/unavailable."""
        try:
            from ..project import Project
            if Project.config is None:
                return cls()
            return cls.from_config(Project.config.get("dashboard"))
        except Exception as exc:                          # config must never crash a job
            log.warning("pipelines: could not read [config.dashboard]: %s", exc)
            return cls()


def _str_or_none(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
