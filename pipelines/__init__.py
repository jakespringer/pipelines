"""pipelines — describe experiments as Artifacts and run their dependency graph.

See ``design/`` for the intended API and ``docs/`` for the implementation spec.
"""

from __future__ import annotations

from . import runtime, sources
from .artifact import artifact
from .cli import cli
from .execution import (BatchPolicy, LocalExecutor, ParallelExecutor,
                        SlurmExecutor)
from .futures import Future, argmax, argmin, derived, fmap, gather, select
from .project import Project
from .runtime import workspace
from .session import Session

source = sources   # `from pipelines import source` -> the source.* helper namespace

__all__ = [
    "Project", "artifact", "derived", "source", "Session", "workspace", "cli",
    "Future", "fmap", "gather", "argmax", "argmin", "select",
    "LocalExecutor", "ParallelExecutor", "SlurmExecutor", "BatchPolicy",
    "runtime",
]
