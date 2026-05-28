"""Batching policy for the cluster executors. Scheduler-only; not yet implemented."""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class BatchPolicy:
    select: object
    max_per_job: int = 1
