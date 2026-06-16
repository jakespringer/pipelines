"""The append-only JSON-lines event log.

Every interesting scheduler action appends one self-describing JSON object (``{"ts", "type",
...}``) to ``<logdir>/events.log``. The format is intentionally flat and stable so an agent
can ``tail -f`` and parse it without any framework knowledge. The server also broadcasts the
same records to attached clients (see :mod:`server`).

Event ``type`` values (and their salient fields):

* ``server_start``  — ``port``, ``logdir``, ``pool`` (resource snapshot), ``n_jobs``, ``jobs``
                       (the relpaths), the run identity ``project`` / ``store`` / ``base_path``,
                       and ``plan`` (the full topological plan: every artifact's ``relpath`` /
                       ``cls`` / in-plan ``deps`` / ``cached`` / ``skipped`` flags). So the log is
                       self-describing for tools that replay it after the run ends — including which
                       artifacts the run skipped because they were already committed (``cached``) or
                       were unneeded transients with no running consumer (``skipped``).
* ``job_state``     — ``relpath``, ``state``, plus whatever changed (``gpus``, ``pid``,
                       ``exit_code``, ``reason``)
* ``pool``          — current :meth:`ResourcePool.snapshot`
* ``server_done``   — ``ok`` (bool), per-state ``counts``
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path


class EventLog:
    """Thread-safe writer of one JSON object per line, flushed eagerly."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._fh = self.path.open("a", encoding="utf-8")

    def emit(self, type: str, **fields) -> dict:
        record = {"ts": round(time.time(), 3), "type": type, **fields}
        line = json.dumps(record)
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
        return record

    def close(self) -> None:
        with self._lock:
            try:
                self._fh.close()
            except OSError:
                pass
