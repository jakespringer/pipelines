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
    ordered: list                       # topological order; satisfied/skipped retained for ordering
    to_build: set                       # will build (excludes satisfied and transient_skipped)
    satisfied: set                      # already committed
    # transient artifacts (``@artifact(transient=True)``) pruned because nothing that
    # will actually run depends on them; a forced artifact that consumes one keeps it.
    transient_skipped: set = dataclasses.field(default_factory=set)
    # Artifacts rebuilt unconditionally (``--force`` ⇒ the selected targets, ``--force-all`` ⇒ the
    # whole reachable graph). Dropped from ``satisfied`` so they land in ``to_build``; executors also
    # read this to bypass the materialize-time cache check for these artifacts.
    forced: set = dataclasses.field(default_factory=set)


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


def build_plan(targets, *, force=(), force_all: bool = False) -> Plan:
    """Collect, validate, order, and run the freshness pass (under an active context).

    ``force`` is the set of selected-target relpaths to rebuild unconditionally (``--force``);
    ``force_all`` rebuilds every reachable artifact (``--force-all``). Forced artifacts are
    treated as not-committed (dropped from ``satisfied``, so they land in ``to_build``) and seed
    the transient prune so any transient intermediate a forced artifact needs is retained.
    """
    targets = list(targets)
    log.info("Building job dependency graph")
    universe = collect(targets)
    log.info("graph: collected %d reachable artifact(s) from %d target(s)",
             len(universe), len(targets))
    check_collisions(universe.values())
    ordered = toposort(universe)
    if force_all:
        forced = set(universe.values())
    else:
        force = set(force)
        forced = {a for a in universe.values() if a.relpath in force}
    log.info("Checking if objects are committed (%d artifact(s))", len(ordered))
    satisfied = _freshness(ordered) - forced       # forced artifacts always rebuild
    unsatisfied = [a for a in ordered if a not in satisfied]
    transient_skipped = _prune_transient(unsatisfied, forced)
    to_build = {a for a in unsatisfied if a not in transient_skipped}
    forced_note = f", {len(forced)} forced" if forced else ""
    if transient_skipped:
        log.info("graph: %d already committed, %d to build%s, %d transient skipped "
                 "(no running consumer)", len(satisfied), len(to_build), forced_note,
                 len(transient_skipped))
    else:
        log.info("graph: %d already committed, %d to build%s",
                 len(satisfied), len(to_build), forced_note)
    return Plan(ordered=ordered, to_build=to_build, satisfied=satisfied,
                transient_skipped=transient_skipped, forced=forced)


def _prune_transient(unsatisfied, forced=()) -> set:
    """Transient artifacts in the to-build set that no artifact-that-will-run requires.

    A ``@artifact(transient=True)`` is an on-demand intermediate: it should build only
    when some artifact that will actually run *this* invocation depends on it. Seed the
    "will run" set with every *non-transient* unsatisfied artifact (those always build)
    plus every ``forced`` artifact (rebuilt unconditionally even if normally transient),
    then walk down dependency edges within the unsatisfied set — each will-run artifact
    pulls in every unsatisfied input it needs (transient or not), transitively. Any
    transient artifact not reached this way has no running consumer and is skipped.

    Pruning is safe precisely because a skipped artifact has no running dependent, so no
    ``materialize`` ``_ready_dep`` ever tries to resurrect it. ``--force-all`` forces the
    whole graph, so every artifact is seeded and nothing is pruned.
    """
    unsat = set(unsatisfied)
    if not any(a.__pipelines__.transient for a in unsat):
        return set()                              # no transient artifacts: common fast path
    forced = set(forced)
    by_relpath = {a.relpath: a for a in unsat}
    needed: set = set()
    stack = [a for a in unsat if not a.__pipelines__.transient or a in forced]
    while stack:
        a = stack.pop()
        if a in needed:
            continue
        needed.add(a)
        for d in a.dependencies():
            dep = by_relpath.get(d.relpath)       # only unsatisfied deps still need building
            if dep is not None and dep not in needed:
                stack.append(dep)
    return {a for a in unsat if a.__pipelines__.transient and a not in needed}


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
