"""First-class per-job shared setup: a ``Session`` amortizes an expensive shared
resource (most often a server, one per machine) across many member jobs.

A subclass defines ``group_key``/``open``/``close``; the executor owns the lifecycle
and the teardown guarantee. The Session instance is the handle: members read
attributes that ``open`` set via ``self.session.<attr>``. See ``docs/07-sessions.md``.
"""

from __future__ import annotations


class Session:
    def group_key(self, a) -> tuple:
        """Which members may share one open session. A pure function of config."""
        return ()

    def open(self, a, ctx) -> None:
        """Start + health-check the shared resource; set attributes on ``self``."""

    def close(self) -> None:
        """Tear down. Guaranteed to run (success/failure/preemption)."""
