"""Remote run reporting: ship a run's ``events.log`` to a dashboard machine over SSH.

The dashboard runs on a local server; jobs run anywhere and report back over SSH. The :class:`Reporter`
is a drop-in for the scheduler's ``EventLog`` — it writes the same local ``events.log`` and, when
``[config.dashboard]`` is configured, a background :class:`~pipelines.reporting.shipper.Shipper`
``rsync``s that file (and the per-job logs, and optionally node metrics) to the dashboard machine.
Off by default. See ``docs/12-dashboard.md``.
"""

from __future__ import annotations

from .config import DashboardConfig
from .reporter import Reporter, short_hostname

__all__ = ["DashboardConfig", "Reporter", "short_hostname"]
