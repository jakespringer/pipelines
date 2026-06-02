"""Local resource-aware parallel scheduling: the run server, its control/monitor
protocol, and the ``pipelines attach`` terminal UI.

This subpackage backs :class:`pipelines.ParallelExecutor` (``pipelines runparallel``).
The pieces are deliberately small and decoupled:

* :mod:`resources` — parse ``annotations`` into a resource request, detect the host's
  GPUs/CPUs/memory, and hand out concrete GPU ids (a :class:`ResourcePool`).
* :mod:`events`   — the append-only JSON-lines event log (agent-parsable).
* :mod:`protocol` — newline-delimited JSON framing over a TCP socket.
* :mod:`server`   — the long-running process that *schedules + runs* jobs and serves a
  control/monitor API on a TCP port (16400+).
* :mod:`client`   — the thin client the TUI (or a script) uses to subscribe + send commands.
* :mod:`tui`      — the tmux-inspired curses monitor for ``pipelines attach``.

Nothing here is imported at package import time unless you actually run the parallel
executor or attach, so the core stays dependency-free.
"""
