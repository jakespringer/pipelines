"""``SlurmExecutor`` — one strict job per artifact, afterok-wired. Not yet implemented.

The constructor is intentionally lightweight (it just records config) so a project's
``run.py`` can be imported — and other verbs like ``runparallel``/``attach`` can run — even
on a machine where the Slurm submission path is not available. Only ``run``/``dryrun`` raise.
"""

from __future__ import annotations


class SlurmExecutor:
    def __init__(self, store=None, base_path=None, *, setup=None, defaults=None,
                 env=None, batch=None, **kwargs):
        self.store = str(store) if store is not None else None
        self.base_path = base_path
        self.setup = setup
        self.defaults = defaults
        self.env = env
        self.batch = batch
        self.extra = kwargs

    def run(self, targets, *, force: bool = False) -> int:
        raise NotImplementedError(
            "SlurmExecutor.run is not implemented yet; use `runparallel` for local "
            "resource-aware parallelism, or LocalExecutor for sequential runs")

    def dryrun(self, targets) -> int:
        raise NotImplementedError("SlurmExecutor.dryrun is not implemented yet")
