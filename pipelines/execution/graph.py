"""Graph collection, topological ordering, and the freshness pass.

Collected, ordered, and freshness-checked once; executors consume the immutable plan.
See ``docs/06-execution.md`` §2.
"""

from __future__ import annotations

import dataclasses

from ..identity import check_collisions, validate_relpath


@dataclasses.dataclass
class Plan:
    ordered: list                       # topological order; satisfied retained for ordering
    to_build: set
    satisfied: set


def collect(targets) -> dict:
    """Reachable artifacts by relpath (walk dependency fields + future source sets)."""
    universe: dict = {}
    stack = list(targets)
    while stack:
        a = stack.pop()
        rp = validate_relpath(a.relpath)
        if rp in universe:
            continue
        universe[rp] = a
        stack.extend(a.dependencies())
    return universe


def toposort(universe: dict) -> list:
    """Kahn's algorithm; deterministic order; cycles are a build-time error."""
    deps = {rp: [d.relpath for d in a.dependencies()] for rp, a in universe.items()}
    indegree = {rp: 0 for rp in universe}
    dependents: dict[str, list] = {rp: [] for rp in universe}
    for rp, ds in deps.items():
        for d in ds:
            indegree[rp] += 1
            dependents[d].append(rp)

    ready = sorted(rp for rp, n in indegree.items() if n == 0)
    ordered = []
    while ready:
        rp = ready.pop(0)
        ordered.append(universe[rp])
        for child in sorted(dependents[rp]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
        ready.sort()
    if len(ordered) != len(universe):
        cyclic = sorted(set(universe) - {a.relpath for a in ordered})
        raise ValueError(f"dependency cycle among: {cyclic}")
    return ordered


def build_plan(targets) -> Plan:
    """Collect, validate, order, and run the freshness pass (under an active context)."""
    universe = collect(targets)
    check_collisions(universe.values())
    ordered = toposort(universe)
    satisfied = {a for a in ordered if a.__pipelines__.cache and _safe_exists(a)}
    to_build = {a for a in ordered if a not in satisfied}
    return Plan(ordered=ordered, to_build=to_build, satisfied=satisfied)


def _safe_exists(a) -> bool:
    try:
        return a.exists()
    except Exception:
        return False
