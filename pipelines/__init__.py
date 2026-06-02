"""pipelines — describe experiments as Artifacts and run their dependency graph.

See ``design/`` for the intended API and ``docs/`` for the implementation spec.
"""

from __future__ import annotations

import warnings

from . import runtime, sources
from .artifact import artifact
from .cli import cli
from .execution import (BatchPolicy, LocalExecutor, ParallelExecutor,
                        SlurmExecutor)
from .futures import Future, argmax, argmin, derived, fmap, gather, select
from .project import Project
from .runtime import configure_logging, workspace
from .session import Session

# Honor $PIPELINES_VERBOSITY as soon as the package is imported, so framework
# narration is active for everything that follows (see runtime.configure_logging).
configure_logging()

# Silence the noisy gcloud "authenticated using end user credentials" warning that
# the google-* clients emit on every import; it's expected for our auth setup.
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

source = sources   # `from pipelines import source` -> the source.* helper namespace

__all__ = [
    "Project", "artifact", "derived", "source", "Session", "workspace", "cli",
    "Future", "fmap", "gather", "argmax", "argmin", "select",
    "LocalExecutor", "ParallelExecutor", "SlurmExecutor", "BatchPolicy",
    "runtime", "configure_logging",
]
