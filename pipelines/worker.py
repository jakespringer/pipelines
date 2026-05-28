"""Cluster worker entry point: materialize one artifact in strict mode.

Used by the cluster executors (not yet implemented). Re-imports the project, sets the
runtime context, looks up the artifact by relpath, and materializes it.
See ``docs/06-execution.md`` §8.
"""

from __future__ import annotations


def main(argv=None) -> int:
    raise NotImplementedError(
        "pipelines.worker is used by the cluster executors, which are not implemented yet")


if __name__ == "__main__":
    raise SystemExit(main())
