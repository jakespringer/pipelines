"""The CLI entry point: ``cli(groups=..., executor=...)``.

A group is just an alias for a list of artifacts; selectors are a group name, ``all``,
or a ``relpath`` glob. The CLI operates on the same graph the script builds.
See ``docs/09-cli.md``.
"""

from __future__ import annotations

import fnmatch
import sys

from .execution.graph import collect


def cli(groups: dict[str, list], executor, argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        return _usage(groups)

    verb, rest = argv[0], argv[1:]
    selectors = [a for a in rest if not a.startswith("-")]
    flags = {a for a in rest if a.startswith("-")}

    if verb in ("help", "-h", "--help"):
        return _usage(groups)

    targets = _resolve(groups, selectors)
    if not targets:
        print(f"no targets matched {selectors or ['(none)']}", file=sys.stderr)
        return 2

    if verb == "run":
        return executor.run(targets, force="--force" in flags or "--force-all" in flags)
    if verb == "dryrun":
        return executor.dryrun(targets)
    print(f"unknown command {verb!r} (have: run, dryrun)", file=sys.stderr)
    return 2


def _resolve(groups: dict[str, list], selectors: list[str]) -> list:
    if not selectors:
        selectors = ["all"]
    universe = _universe(groups)
    out: dict[str, object] = {}
    for sel in selectors:
        if sel == "all":
            chosen = universe.values()
        elif sel in groups:
            chosen = groups[sel]
        else:
            chosen = [a for rp, a in universe.items() if fnmatch.fnmatch(rp, sel)]
        for a in chosen:
            out[a.relpath] = a
    return list(out.values())


def _universe(groups: dict[str, list]) -> dict:
    everything: list = []
    for artifacts in groups.values():
        everything.extend(artifacts)
    return collect(everything)


def _usage(groups: dict[str, list]) -> int:
    print("usage: <run|dryrun> [SELECTOR ...]")
    print("groups:", ", ".join(sorted(groups)) or "(none)")
    return 0
