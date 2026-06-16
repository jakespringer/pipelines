"""The CLI entry point: ``cli(groups=..., executor=...)``.

A group is just an alias for a list of artifacts; selectors are a group name, ``all``,
or a ``relpath`` glob. The CLI operates on the same graph the script builds.
See ``docs/09-cli.md``.
"""

from __future__ import annotations

import fnmatch
import os
import runpy
import sys
import types
from pathlib import Path

from .execution.graph import collect

# Options that take no value (everything else of the form ``--k v`` consumes ``v``).
_FLAG_OPTS = {"force", "force-all", "dry", "expand", "nofresh", "skip-committed",
              "detach", "watch", "color", "no-color", "dark", "light"}

# A ``run.py`` calls ``cli(...)`` without ``sys.exit``-ing its result, so the verb's exit
# code would otherwise be lost (every worker subprocess would look successful). ``cli``
# records it here and ``main`` propagates it as the process exit status.
LAST_EXIT_CODE = 0


def _parse(rest: list[str]) -> tuple[list[str], dict[str, str], set[str]]:
    """Split argv tail into positionals, ``--key value`` opts, and bare ``--flag`` flags."""
    pos: list[str] = []
    opts: dict[str, str] = {}
    flags: set[str] = set()
    i = 0
    while i < len(rest):
        a = rest[i]
        if a.startswith("--"):
            key = a[2:]
            if "=" in key:
                k, v = key.split("=", 1)
                opts[k] = v
            elif key in _FLAG_OPTS:
                flags.add(key)
            elif i + 1 < len(rest) and not rest[i + 1].startswith("--"):
                opts[key] = rest[i + 1]
                i += 1
            else:
                flags.add(key)
        else:
            pos.append(a)
        i += 1
    return pos, opts, flags


def cli(groups: dict[str, list], executor, argv: list[str] | None = None) -> int:
    """Dispatch a verb and record the exit code so ``main`` can propagate it."""
    global LAST_EXIT_CODE
    LAST_EXIT_CODE = _dispatch(groups, executor, argv)
    return LAST_EXIT_CODE


def _dispatch(groups: dict[str, list], executor, argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        return _usage(groups)

    verb, rest = argv[0], argv[1:]
    if verb in ("help", "-h", "--help"):
        return _usage(groups)

    # `dashboard` serves a web monitor for every run; it needs no targets/executor.
    if verb == "dashboard":
        from .dashboard.server import dashboard_main
        return dashboard_main(rest)

    # Hidden worker verb: build exactly one artifact (used by Parallel/Slurm executor jobs).
    if verb == "_worker":
        _, opts, flags = _parse(rest)
        return _run_worker(groups, opts, flags)

    # Backend command groups: `pipelines <backend> <subcommand> ...`.
    if verb == "slurm":
        from .execution.executors.slurm_cli import main as slurm_main
        return slurm_main(groups, executor, rest)
    if verb == "parallel":
        return _parallel_group(groups, executor, rest)
    if verb == "local":
        return _local_group(groups, executor, rest)

    # Deprecated flat aliases (one release): the old top-level verbs map onto the new groups.
    if verb == "runparallel":
        _deprecated("runparallel", "parallel run")
        return _parallel_group(groups, executor, ["run", *rest])
    if verb == "attach":
        _deprecated("attach", "parallel attach")
        return _parallel_group(groups, executor, ["attach", *rest])

    # Top-level project verbs over the configured executor / graph.
    positionals, opts, flags = _parse(rest)
    if verb in ("run", "dryrun", "plan"):
        targets = _resolve(groups, positionals)
        if not targets:
            print(f"no targets matched {positionals or ['(none)']}", file=sys.stderr)
            return 2
        if verb == "plan":
            return _run_plan(executor, targets, flags)
        if verb == "dryrun":
            return executor.dryrun(targets)
        return executor.run(targets, force="force" in flags, force_all="force-all" in flags)

    print(f"unknown command {verb!r} (have verbs: run, dryrun, plan, dashboard; "
          f"backends: local, parallel, slurm)", file=sys.stderr)
    return 2


def _parallel_group(groups: dict[str, list], executor, argv: list[str]) -> int:
    """`pipelines parallel <run|attach>` — local resource-aware execution and its live monitor."""
    if not argv:
        print("usage: pipelines parallel <run|attach> [...]", file=sys.stderr)
        return 2
    sub, rest = argv[0], argv[1:]
    positionals, opts, flags = _parse(rest)
    if sub == "attach":                          # connects to a running run server; no targets
        from .scheduler.tui import attach_main
        return attach_main(positionals)
    if sub == "run":
        targets = _resolve(groups, positionals)
        if not targets:
            print(f"no targets matched {positionals or ['(none)']}", file=sys.stderr)
            return 2
        return _run_parallel(executor, targets, opts, flags)
    print(f"unknown parallel subcommand {sub!r} (have: run, attach)", file=sys.stderr)
    return 2


def _local_group(groups: dict[str, list], executor, argv: list[str]) -> int:
    """`pipelines local run` — sequential, in-process materialization (debug-friendly)."""
    if not argv:
        print("usage: pipelines local run [SELECTOR ...] [--force]", file=sys.stderr)
        return 2
    sub, rest = argv[0], argv[1:]
    if sub not in ("run", "dryrun"):
        print(f"unknown local subcommand {sub!r} (have: run, dryrun)", file=sys.stderr)
        return 2
    positionals, opts, flags = _parse(rest)
    targets = _resolve(groups, positionals)
    if not targets:
        print(f"no targets matched {positionals or ['(none)']}", file=sys.stderr)
        return 2
    lx = _local_executor(executor)
    if sub == "dryrun":
        return lx.dryrun(targets)
    return lx.run(targets, force="force" in flags, force_all="force-all" in flags)


def _local_executor(executor):
    """The configured executor if it is a LocalExecutor, else one built from its store/base_path."""
    from .execution.executors.local import LocalExecutor
    if isinstance(executor, LocalExecutor):
        return executor
    store = getattr(executor, "store", None)
    base_path = getattr(executor, "base_path", None)
    if not store or base_path is None:
        raise SystemExit("local run needs a store and base_path from the configured executor.")
    return LocalExecutor(store=store, base_path=base_path,
                         conda_env=getattr(executor, "conda_env", None),
                         python_bin=getattr(executor, "python_bin", None))


def _deprecated(old: str, new: str) -> None:
    print(f"pipelines: `{old}` is deprecated; use `{new}`", file=sys.stderr)


def _run_plan(executor, targets, flags: set) -> int:
    """Render the condensed scheduler plan as an ASCII tree (collapsed by type + repetition)."""
    from . import runtime
    from .execution.graph import Plan, collect, toposort, build_plan
    from .plan_render import render_plan
    from .runtime import RuntimeContext

    store = getattr(executor, "store", None)
    base_path = getattr(executor, "base_path", None)

    plan = None
    if store is not None and base_path is not None and "nofresh" not in flags:
        # Freshness (committed vs to-build) needs a context to resolve each store.
        rc = RuntimeContext(base_path=_as_path(base_path), executor_store=str(store))
        token = runtime._CTX.set(rc)
        try:
            plan = build_plan(targets)
        except Exception:                       # store unreachable etc. → fall back to structure
            plan = None
        finally:
            runtime._CTX.reset(token)
    if plan is None:                            # structure-only: everything shown as to-build
        universe = collect(targets)
        ordered = toposort(universe)
        plan = Plan(ordered=ordered, to_build=set(ordered), satisfied=set())

    color = _want_color(flags)
    print(render_plan(plan, expand="expand" in flags, color=color, dark=_dark_bg(flags)))
    return 0


def _want_color(flags: set) -> bool:
    """Color when writing to a TTY, unless overridden by flags or $NO_COLOR."""
    if "no-color" in flags:
        return False
    if "color" in flags:
        return True
    return sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def _dark_bg(flags: set) -> bool:
    """Whether the *terminal* background is dark (→ light neutral text).

    Honors --light/--dark, else sniffs $COLORFGBG (``fg;bg``; bg<7 or 8 ⇒ dark), and
    defaults to **light** (dark text). The highlighted role-qualifier chip always uses
    very light text on its own dark background, independent of this.
    """
    if "light" in flags:
        return False
    if "dark" in flags:
        return True
    parts = os.environ.get("COLORFGBG", "").split(";")
    if parts and parts[-1].isdigit():
        bg = int(parts[-1])
        return bg < 7 or bg == 8
    return False


def _as_path(base_path):
    from pathlib import Path
    return Path(str(base_path).replace("file://", "")) if base_path else Path.cwd()


def _run_parallel(executor, targets, opts: dict, flags: set) -> int:
    """Build a ParallelExecutor from the configured executor's store/base_path + CLI opts."""
    from .execution.executors.parallel import ParallelExecutor
    from .scheduler.resources import parse_memory_mb

    def _int(v):
        return int(v) if v is not None else None

    pexec = ParallelExecutor(
        store=getattr(executor, "store", None),
        base_path=getattr(executor, "base_path", None),
        gpus=_int(opts.get("gpus")),
        cpus=_int(opts.get("cpus")),
        memory_mb=parse_memory_mb(opts["memory"]) if opts.get("memory") else None,
    )
    return pexec.run(targets, force="force" in flags, force_all="force-all" in flags,
                     dry="dry" in flags)


def _run_worker(groups: dict[str, list], opts: dict, flags: set | None = None) -> int:
    """Materialize a single artifact in strict mode (one Parallel/Slurm executor job).

    ``--skip-committed`` runs the primitive with ``scheduler=True`` so an already-committed output
    is skipped instead of rebuilt — what a requeued/preempted Slurm task wants. The parallel
    scheduler never resubmits a committed job, so it leaves the flag off.
    """
    import logging
    import os
    import traceback
    from pathlib import Path

    flags = flags or set()

    from . import runtime
    from .execution.materialize import materialize
    from .runtime import RuntimeContext

    # Surface ctx.log narration into this job's captured stdout (visible in `attach`),
    # unless verbosity was already configured (which installs its own handler).
    wl = logging.getLogger("pipelines")
    if not wl.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(message)s"))
        wl.addHandler(h)
        wl.setLevel(logging.INFO)

    relpath = opts.get("only")
    store = opts.get("store")
    base_path = opts.get("base-path")
    if not (relpath and store and base_path):
        print("_worker requires --only, --store, and --base-path", file=sys.stderr)
        return 2

    universe = _universe(groups)
    artifact = universe.get(relpath)
    if artifact is None:
        print(f"_worker: no artifact with relpath {relpath!r} in this project", file=sys.stderr)
        return 2

    gpu_ids = [g for g in os.environ.get("PIPELINES_GPU_IDS", "").split(",") if g]
    rc = RuntimeContext(
        base_path=Path(str(base_path).replace("file://", "")),
        relpath=relpath,
        executor_store=store,
        annotations=dict(getattr(artifact, "annotations", {}) or {}),
        gpu_ids=gpu_ids,
    )
    token = runtime._CTX.set(rc)
    cache_token = runtime._RESULT_CACHE.set({})
    session = None
    try:
        session = _open_session(artifact, rc)
        rc.session = session
        materialize(artifact, scheduler="skip-committed" in flags, strict=True)
        return 0
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        if session is not None:
            try:
                session.close()
            except Exception:
                traceback.print_exc()
        runtime._RESULT_CACHE.reset(cache_token)
        runtime._CTX.reset(token)


def _open_session(artifact, rc):
    """Open the artifact's Session (per-process; parallel jobs don't share one)."""
    cls = artifact.__pipelines__.session
    if cls is None:
        return None
    probe = cls()
    probe.open(artifact, rc)
    return probe


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
    print("usage: pipelines <verb|backend> ... [SELECTOR ...]")
    print("verbs (configured executor / graph):")
    print("  run|dryrun [SEL...] [--force]   build with the executor configured in run.py")
    print("  plan [SEL...] [--expand] [--nofresh] [--light|--dark] [--no-color]")
    print("  dashboard [--port 7000]         web monitor for all runs (live + past)")
    print("backends:")
    print("  local run [SEL...] [--force]                       sequential, in-process")
    print("  parallel run [SEL...] [--gpus/--cpus/--memory N] [--force] [--dry]   local, parallel")
    print("  parallel attach [PORT]                             tmux-style live monitor")
    print("  slurm run|ls|cancel|sendcommand|attach|logs ...    cluster (see `slurm help`)")
    print("groups:", ", ".join(sorted(groups)) or "(none)")
    return 0


# --- console entry point -------------------------------------------------------
#
# A project's ``run.py`` builds the groups + executor and calls ``cli(...)`` under
# ``if __name__ == "__main__":``. The installed ``pipelines`` command finds that
# ``run.py`` and executes it as ``__main__`` (so its ``cli(...)`` call fires) with
# the remaining argv. ``run.py`` uses *relative* imports (``from . import steps``),
# so it must run inside a package: we register the project directory under a
# synthetic package name — unique, so it never collides with an installed package
# of the same directory name (the reason a bare ``python -m`` fails for the
# examples).

_SYNTH_PKG = "_pipelines_project"


def _split_project_flag(argv: list[str]) -> tuple[str | None, list[str]]:
    """Pull a leading ``-p DIR`` / ``--project DIR`` / ``--project=DIR`` off argv."""
    out: list[str] = []
    project: str | None = None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-p", "--project"):
            if i + 1 >= len(argv):
                raise SystemExit(f"{a} requires a path argument")
            project, i = argv[i + 1], i + 2
            continue
        if a.startswith("--project="):
            project, i = a.split("=", 1)[1], i + 1
            continue
        out.append(a)
        i += 1
    return project, out


def _discover_run_file(project: str | None) -> Path:
    if project:
        p = Path(project).expanduser().resolve()
        if p.is_file():
            return p
        if p.is_dir() and (p / "run.py").is_file():
            return p / "run.py"
        raise SystemExit(f"--project: no run.py at {p}")
    here = Path.cwd().resolve()
    for d in (here, *here.parents):
        if (d / "run.py").is_file() and (d / "pipelines.toml").is_file():
            return d / "run.py"
    if (here / "run.py").is_file():
        return here / "run.py"
    raise SystemExit(
        "no project found: run from a directory containing run.py and "
        "pipelines.toml, or pass --project DIR"
    )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    project, rest = _split_project_flag(argv)

    # `dashboard` is project-independent: it watches the registry directory and serves a web
    # monitor for every run. Its own flags (--port/--host) pass through verbatim.
    if rest and rest[0] == "dashboard":
        from .dashboard.server import dashboard_main
        return dashboard_main(rest[1:])

    # `parallel attach` (and the deprecated bare `attach`) just connects to a running run server,
    # so handle it before discovering a run.py — it works from anywhere.
    attach_args = None
    if rest and rest[0] == "attach":
        _deprecated("attach", "parallel attach")
        attach_args = rest[1:]
    elif rest[:2] == ["parallel", "attach"]:
        attach_args = rest[2:]
    if attach_args is not None:
        from .scheduler.tui import attach_main
        return attach_main([a for a in attach_args if not a.startswith("-")])

    run_file = _discover_run_file(project)
    project_dir = run_file.parent
    # Worker subprocesses (ParallelExecutor) re-invoke this same run.py via --project.
    os.environ["_PIPELINES_RUNFILE"] = str(run_file)

    pkg = types.ModuleType(_SYNTH_PKG)
    pkg.__path__ = [str(project_dir)]      # relative imports in run.py resolve here
    pkg.__package__ = _SYNTH_PKG
    sys.modules[_SYNTH_PKG] = pkg
    sys.path.insert(0, str(project_dir))   # match `python run.py` script semantics

    sys.argv = [str(run_file), *rest]
    global LAST_EXIT_CODE
    LAST_EXIT_CODE = 0
    # alter_sys=False: keep sys.modules["__main__"] as the real, import-guarded
    # ``pipelines.__main__`` rather than the synthetic ``_pipelines_project.run``.
    # In-process libraries that spawn workers (vLLM tensor-parallel forces the
    # 'spawn' start method under CUDA) re-import the parent's __main__ in each child;
    # a fresh interpreter cannot import the synthetic package and dies with
    # ModuleNotFoundError('_pipelines_project'). The real __main__ is guarded, so
    # re-importing it in a child is a harmless no-op, while run.py still runs as
    # __main__ and its relative imports still resolve via the registered package.
    runpy.run_module(f"{_SYNTH_PKG}.run", run_name="__main__", alter_sys=False)
    # run.py's ``cli(...)`` recorded the verb's exit code; propagate it as the process status.
    return LAST_EXIT_CODE
