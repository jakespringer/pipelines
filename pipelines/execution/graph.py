"""Graph collection, topological ordering, and the freshness pass.

Collected, ordered, and freshness-checked once; executors consume the immutable plan.
See ``docs/06-execution.md`` §2.
"""

from __future__ import annotations

import contextvars
import dataclasses
import logging
from concurrent.futures import ThreadPoolExecutor

from ..identity import check_collisions, validate_relpath

log = logging.getLogger("pipelines")

# Outer fan-out for the freshness pass: per-store ``exists_many`` batches and
# custom-``exists`` artifacts run concurrently here. Each gs:// batch itself
# parallelizes internally, so a small outer pool already saturates the network.
_FRESHNESS_WORKERS = 8


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
    targets = list(targets)
    universe = collect(targets)
    log.info("graph: collected %d reachable artifact(s) from %d target(s)",
             len(universe), len(targets))
    check_collisions(universe.values())
    ordered = toposort(universe)
    log.info("graph: freshness pass over %d artifact(s)", len(ordered))
    satisfied = _freshness(ordered)
    to_build = {a for a in ordered if a not in satisfied}
    log.info("graph: %d already committed, %d to build", len(satisfied), len(to_build))
    return Plan(ordered=ordered, to_build=to_build, satisfied=satisfied)


def _freshness(ordered) -> set:
    """Decide which cache-enabled artifacts are already committed, in parallel.

    Artifacts using the framework's default store-backed ``exists`` are grouped by
    their resolved store and checked with one ``store.exists_many`` batch per store
    (gs:// batches parallelize internally). Artifacts with a custom ``exists`` are
    evaluated individually. Every group runs concurrently in a thread pool; because
    worker threads start with an empty :mod:`contextvars` state, each task carries
    its own copy of the active runtime context so store resolution still sees the
    executor (see ``_resolve_store``).
    """
    from ..artifact import _resolve_store

    candidates = [a for a in ordered if a.__pipelines__.cache]
    if not candidates:
        return set()

    batched: dict[str, list] = {}        # store.root -> artifacts using default exists
    stores: dict[str, object] = {}       # store.root -> one Store instance for the batch
    individual: list = []                # custom exists() or unresolvable store
    for a in candidates:
        if not getattr(type(a).exists, "_pipelines_injected", False):
            individual.append(a)
            continue
        try:
            store = _resolve_store(a)    # resolved here, under the live context
        except Exception:
            individual.append(a)
            continue
        batched.setdefault(store.root, []).append(a)
        stores.setdefault(store.root, store)

    satisfied: set = set()

    def _run_batch(root: str) -> None:
        arts = batched[root]
        try:
            result = stores[root].exists_many([a.relpath for a in arts])
        except Exception:
            log.info("graph: batch freshness check against store %r raised; "
                     "treating its %d artifact(s) as not committed", root, len(arts),
                     exc_info=True)
            return
        satisfied.update(a for a in arts if result.get(a.relpath))

    def _run_one(a) -> None:
        if _safe_exists(a):
            satisfied.add(a)

    # (context_copy, callable) per unit of work; each copy isolates the worker so a
    # single Context is never .run() from two threads at once.
    tasks = [(contextvars.copy_context(), _run_batch, root) for root in batched]
    tasks += [(contextvars.copy_context(), _run_one, a) for a in individual]
    if not tasks:
        return satisfied
    with ThreadPoolExecutor(max_workers=min(len(tasks), _FRESHNESS_WORKERS)) as pool:
        # set() membership and update() are GIL-atomic, so no extra lock is needed.
        list(pool.map(lambda t: t[0].run(t[1], t[2]), tasks))
    return satisfied


def _safe_exists(a) -> bool:
    try:
        return a.exists()
    except Exception:
        log.info("graph: freshness check for %r raised; treating as not committed",
                 a.relpath, exc_info=True)
        return False
