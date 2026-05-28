"""Google Cloud Storage store backend (``gs://``).

Not yet implemented. The design (``docs/03 §3``) calls for staging-then-manifest
finalization since GCS has no atomic directory rename. Registered so that a clear
error is raised only if a ``gs://`` store is actually used; local runs use ``file://``.
"""

from __future__ import annotations

from pathlib import Path

from .base import Store, register

_MSG = ("gs:// store backend is not implemented yet; configure a file:// store "
        "(e.g. via ~/.config/pipelines/projects/<name>.toml) for local runs")


@register("gs")
class GSStore(Store):
    def __init__(self, root: str):
        raise NotImplementedError(_MSG)

    def exists(self, relpath: str) -> bool: raise NotImplementedError(_MSG)
    def get_dir(self, relpath: str, dest: Path, only=None) -> None: raise NotImplementedError(_MSG)
    def put_dir(self, src: Path, relpath: str) -> None: raise NotImplementedError(_MSG)
    def delete(self, relpath: str) -> None: raise NotImplementedError(_MSG)
