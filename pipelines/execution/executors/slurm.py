"""``SlurmExecutor`` — one strict job per artifact, afterok-wired. Not yet implemented."""

from __future__ import annotations


class SlurmExecutor:
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "SlurmExecutor is not implemented yet; use LocalExecutor for local runs")
