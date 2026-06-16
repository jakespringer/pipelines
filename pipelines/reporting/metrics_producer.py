"""``MetricsProducer`` — the per-host system-metrics sampler for SSH reporting.

When ``[config.dashboard].metrics`` is set, the *first* job on a host samples node metrics and its
Shipper mirrors the resulting ``<node>.jsonl`` to the dashboard. A host-level ``flock`` guarantees
exactly **one** producer per node even when co-located jobs don't know about each other — the losers
ship events only. ``flock`` is released by the kernel when the owning process dies, so a later job
picks the sampler up on its next tick.

The sampler itself is the dashboard's :class:`~pipelines.dashboard.metrics.SystemSampler`, imported
lazily so a job that doesn't enable metrics never pulls the dashboard package in.
"""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger("pipelines")


class MetricsProducer:
    """Holds a host-wide lock; the winner samples this node's metrics to ``<node>.jsonl``."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        self._lockf = None
        self.sampler = None
        self.path: Path | None = None

    def start(self) -> Path | None:
        """Try to become this host's sampler. Returns the metrics file path if we won, else ``None``."""
        import fcntl
        from ..dashboard.metrics import SystemSampler, metrics_dir
        try:
            self._lockf = open(metrics_dir() / ".sampler.lock", "w")
            fcntl.flock(self._lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._close_lock()                    # another job on this host already owns the sampler
            return None
        try:
            self.sampler = SystemSampler(node_id=self.node_id).start()
            self.path = self.sampler.path
            return self.path
        except Exception as exc:
            log.debug("pipelines: metrics sampler failed to start: %s", exc)
            self.stop()
            return None

    def stop(self) -> None:
        if self.sampler is not None:
            try:
                self.sampler.stop()
            except Exception:
                pass
            self.sampler = None
        self._close_lock()

    def _close_lock(self) -> None:
        if self._lockf is None:
            return
        import fcntl
        try:
            fcntl.flock(self._lockf, fcntl.LOCK_UN)
            self._lockf.close()
        except OSError:
            pass
        self._lockf = None
