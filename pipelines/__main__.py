"""``python -m pipelines`` — same entry point as the installed ``pipelines`` command.

Used by :class:`pipelines.ParallelExecutor` to launch worker subprocesses
(``python -m pipelines --project <run.py> _worker --only <relpath> ...``).
"""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
