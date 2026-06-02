"""The ``pipelines dashboard`` web monitor.

A dependency-free web UI for parallel runs (``pipelines runparallel``). It watches the registry
directory and replays each run's ``events.log``, so live runs appear on their own and finished runs
stay browsable — no run-server connection required.

* :mod:`index`  — discover runs and replay their event logs into queryable views.
* :mod:`server` — the stdlib HTTP server (JSON + SSE) and the ``dashboard`` CLI entry point.
* ``assets/``   — the single-page UI (one HTML shell, one stylesheet, one script).

Nothing here is imported at package import time unless you actually start the dashboard.
"""

from __future__ import annotations

from .server import dashboard_main, serve

__all__ = ["dashboard_main", "serve"]
