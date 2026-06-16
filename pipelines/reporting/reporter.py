"""``Reporter`` — a run's event sink: the local ``EventLog`` plus an optional SSH ``Shipper``.

A drop-in replacement for :class:`~pipelines.scheduler.events.EventLog` in ``RunServer`` and
``SlurmMonitor``. :meth:`emit` writes the local ``events.log`` (durable, eagerly flushed) and calls
an optional ``on_record`` hook (the run server's in-process TCP broadcast) — and **never** touches
the network. When ``[config.dashboard]`` is configured, a background :class:`Shipper` mirrors that
file to the dashboard machine; when it isn't (the default), no thread starts and the Reporter is
just the local log.

The local ``events.log`` *is* the queue: ``emit`` only appends to it, and the Shipper picks up the
growth on its own cadence. So a stalled/unreachable dashboard can never slow down a job.
"""

from __future__ import annotations

import logging
import re
import socket
from pathlib import Path

from ..scheduler.events import EventLog
from .config import DashboardConfig
from .shipper import Shipper

log = logging.getLogger("pipelines")


def short_hostname() -> str:
    return socket.gethostname().split(".")[0] or "localhost"


def _safe_run_id(run_id) -> str:
    """A run id usable as a remote directory/file name (and shell-safe when interpolated)."""
    return re.sub(r"[^\w.-]", "-", str(run_id)) or "run"


class Reporter:
    """Owns the local ``events.log`` and, when configured, the SSH shipper that mirrors it."""

    def __init__(self, *, events_path, run_id, meta: dict, on_record=None,
                 config: DashboardConfig | None = None):
        self.events_path = Path(events_path)
        self.eventlog = EventLog(self.events_path)
        self._on_record = on_record
        cfg = config if config is not None else DashboardConfig.from_project()
        self.shipper: Shipper | None = None
        if cfg.enabled:
            try:
                self.shipper = Shipper(cfg, events_path=self.events_path,
                                       run_id=_safe_run_id(run_id), meta=meta)
                self.shipper.start()
            except Exception as exc:                  # reporting is best-effort; never break the run
                log.warning("pipelines: could not start dashboard reporting: %s", exc)
                self.shipper = None

    @classmethod
    def open(cls, *, events_path, run_id, meta: dict, on_record=None,
             config: DashboardConfig | None = None) -> "Reporter":
        return cls(events_path=events_path, run_id=run_id, meta=meta,
                   on_record=on_record, config=config)

    def emit(self, type: str, **fields) -> dict:
        """Append one record locally (durable) and fan it out to the in-process hook. Non-blocking."""
        record = self.eventlog.emit(type, **fields)
        if self._on_record is not None:
            try:
                self._on_record(record)
            except Exception:
                log.debug("pipelines: reporter on_record hook failed", exc_info=True)
        return record

    def close(self, *, ok: bool | None = None, counts: dict | None = None) -> None:
        """Close the local log and stop the shipper. ``ok is None`` ⇒ stop without a ``done`` marker
        (used on detach, where the run continues and a later attach resumes shipping)."""
        self.eventlog.close()
        if self.shipper is not None:
            self.shipper.request_stop(ok=ok, counts=counts)
            self.shipper = None
