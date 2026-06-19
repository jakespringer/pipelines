"""``SlurmExecutor`` — one strict ``sbatch`` job per artifact, ``afterok``-wired.

Every executor builds the same immutable plan (``build_plan`` — the graph/freshness pass) and is
just a policy for *who* runs the ``materialize`` primitive. This one emits one Slurm job per
unsatisfied artifact, in topological order, so each consumer can depend (``--dependency=afterok``)
on the concrete job ids of the upstreams it needs. The job payload is the ordinary worker verb in
strict mode (``pipelines _worker --only <relpath> [--skip-committed]``): it re-imports the project,
retrieves committed deps from their stores, runs ``construct``, and commits. ``--skip-committed`` is
passed for ordinary jobs (so a requeue skips finished work) but omitted for ``--force``-d artifacts
so their committed output is rebuilt.

Identity is **stateless**: a job is named ``<hash(project)>:<hash(project, relpath)>`` — an opaque
hash that reveals nothing in ``squeue`` while still letting the framework filter its own jobs by the
``hash(project)`` namespace prefix. There is no persisted job map; ``pipelines slurm ls``/``cancel``
re-derive the names from the graph and reconcile against ``squeue``/``sacct``.

Observability is handled by a single-writer :class:`~pipelines.scheduler.slurm_monitor.SlurmMonitor`
(the cluster analogue of the parallel ``RunServer``) that polls Slurm and writes the same
``events.log`` the dashboard already replays. The constructor stays lightweight so a project's
``run.py`` imports — and ``slurm ls``/``dryrun`` work — even where ``sbatch`` is unavailable; only
the real submission path needs it.

v1 emits **one job per artifact**; the ``_units`` seam is where array grouping (``--array``) attaches
later (see ``design/06-execution.md`` §5–§6).
"""

from __future__ import annotations

import logging
import os
import shlex
import shutil
import subprocess
import time
from pathlib import Path

from ... import runtime
from ...identity import fingerprint
from ...project import Project
from ...runtime import RuntimeContext
from ...scheduler.resources import ResourceRequest
from ...scheduler.server import job_log_path
from ..graph import build_plan

log = logging.getLogger("pipelines")

# Portable resource keys, mapped to canonical Slurm directives. In an artifact's annotations any
# other top-level key is free metadata; in the executor's (backend-specific) defaults, any other
# top-level key is itself a Slurm directive (e.g. partition/time/qos).
_PORTABLE = {"gpus", "cpus", "memory", "runtime"}
_CONSUMED_KEYS = _PORTABLE | {"slurm"}


# --------------------------------------------------------------------------- #
# Stateless identity (shared with slurm_cli / slurm_monitor)
# --------------------------------------------------------------------------- #
def project_prefix(project: str | None) -> str:
    """The opaque per-project namespace prefix used to filter our own jobs in ``squeue``."""
    return fingerprint(project or "pipelines", n=8)


def job_name(project: str | None, relpath: str) -> str:
    """The opaque, re-derivable ``--job-name`` for one artifact's job."""
    return f"{project_prefix(project)}:{fingerprint((project or 'pipelines', relpath), n=12)}"


def as_slurm_time(value) -> str:
    """A portable ``runtime`` annotation → a Slurm ``--time`` string.

    Strings pass through (already Slurm time syntax, e.g. ``"2-00:00:00"``); a bare number is read
    as whole minutes (Slurm accepts a plain integer as a minute count).
    """
    if isinstance(value, (int, float)):
        return str(int(value))
    return str(value)


# --------------------------------------------------------------------------- #
class SlurmExecutor:
    def __init__(self, store=None, base_path=None, *, setup=None, defaults=None,
                 env=None, batch=None, run_dir=None, poll_interval=None, runfile=None,
                 overrides=None, **kwargs):
        self.store = str(store) if store is not None else None
        self.base_path = base_path
        self.setup = setup
        self.defaults = dict(defaults or {})
        # CLI-supplied Slurm directives (e.g. `slurm run --partition cpu`): highest precedence,
        # winning over both `defaults` and per-artifact annotations (see `_resolve`).
        self.overrides = dict(overrides or {})
        self.env = dict(env or {})
        self.batch = batch
        self.run_dir = run_dir
        self.poll_interval = poll_interval
        # The worker re-invokes this project's run.py via --project; cli.main stashes the path.
        self.runfile = runfile or os.environ.get("_PIPELINES_RUNFILE")
        self.extra = kwargs

    # ------------------------------------------------------------------ #
    # Public entry points
    # ------------------------------------------------------------------ #
    def run(self, targets, *, force: bool = False, force_all: bool = False,
            dry: bool = False, detach: bool = False) -> int:
        if not self.store or self.base_path is None:
            raise SystemExit(
                "SlurmExecutor needs a store and base_path; the configured executor in "
                "run.py did not provide them.")

        base_path = _as_path(self.base_path)
        plan = self._plan(targets, base_path, force=force, force_all=force_all)
        universe = {a.relpath: a for a in plan.ordered}
        # Forced artifacts are already dropped from satisfied/transient_skipped by build_plan (cf.
        # parallel); they submit jobs that omit ``--skip-committed`` so a committed output rebuilds.
        forced = {a.relpath for a in plan.forced}
        satisfied = {a.relpath for a in plan.satisfied}
        skipped = {a.relpath for a in plan.transient_skipped}
        omit = satisfied | skipped

        def deps_of(a):                       # in-universe deps (manifest/dashboard tiering)
            return [d.relpath for d in a.dependencies() if d.relpath in universe]

        def job_deps_of(a):                   # deps that produce a job this run (afterok wiring)
            return [rp for rp in deps_of(a) if rp not in omit]

        to_submit = [a for a in plan.ordered if a.relpath not in omit]
        # The full plan in topological order for the monitor's server_start (same shape parallel
        # emits): every artifact, its type, its in-plan deps, and whether it was skipped.
        manifest = [
            {"relpath": a.relpath, "cls": type(a).__name__, "deps": deps_of(a),
             "cached": a.relpath in satisfied, "skipped": a.relpath in skipped}
            for a in plan.ordered
        ]

        if dry:
            return self._dryrun(to_submit, job_deps_of, satisfied, skipped, base_path)

        if not to_submit:
            extra = f", {len(skipped)} transient skipped" if skipped else ""
            print(f"pipelines: nothing to submit ({len(satisfied)} already committed{extra})")
            return 0

        if not self.runfile:
            raise SystemExit(
                "slurm run must be launched via the `pipelines` CLI (no project run.py known).")
        if shutil.which("sbatch") is None:
            raise SystemExit(
                "SlurmExecutor.run needs `sbatch` on PATH. Preview with `pipelines slurm run "
                "--dry`, or run locally with `pipelines parallel run`.")

        rundir = self._make_rundir(targets)
        log.info("Submitting %d job(s) to Slurm (run dir %s)", len(to_submit), rundir)

        # One job per artifact (the array-grouping seam wraps this loop later). Submit in
        # topological order so every dependency's job id is known before its consumer is submitted.
        jobids: dict[str, str] = {}
        try:
            for a in to_submit:
                dep_ids = [jobids[d] for d in job_deps_of(a)]   # topo order ⇒ all already submitted
                jobids[a.relpath] = self._submit_one(
                    a, rundir, dep_ids, base_path, forced=a.relpath in forced)
        except Exception:
            if jobids:
                log.warning("submission failed mid-run; cancelling %d already-queued job(s)",
                            len(jobids))
                _scancel(list(jobids.values()))
            raise

        from ...scheduler.slurm_monitor import SlurmMonitor
        monitor = SlurmMonitor(
            rundir=rundir, run_id=rundir.name, project=Project.name,
            store=self.store, base_path=str(base_path), manifest=manifest,
            jobids=jobids, poll_interval=self.poll_interval,
        )
        if detach:
            monitor.start_detached()
            print(f"pipelines: submitted {len(jobids)} job(s); detached.")
            print(f"pipelines: re-attach with `pipelines slurm attach {rundir.name}`")
            return 0
        return monitor.run()

    def dryrun(self, targets) -> int:
        return self.run(targets, dry=True)

    def run_dir_path(self) -> Path:
        """Resolve the shared-filesystem run-dir root (kwarg or ``slurm_run_dir`` config)."""
        rd = self.run_dir
        if rd is None and Project.config is not None:
            rd = Project.config.get("slurm_run_dir")
        if rd is None:
            raise SystemExit(
                "SlurmExecutor needs a shared-filesystem run_dir (set run_dir=... on the executor "
                "or `slurm_run_dir` in a pipelines.toml [config] table). It holds the event log and "
                "per-job logs the dashboard reads, and must be reachable from compute nodes.")
        return Path(str(rd).replace("file://", "")).expanduser()

    # ------------------------------------------------------------------ #
    # Planning (mirrors ParallelExecutor._plan)
    # ------------------------------------------------------------------ #
    def _plan(self, targets, base_path: Path, *, force: bool = False,
              force_all: bool = False):
        force_relpaths = [a.relpath for a in targets] if force else ()
        rc = RuntimeContext(base_path=base_path, executor_store=self.store, log=log)
        token = runtime._CTX.set(rc)
        try:
            return build_plan(targets, force=force_relpaths, force_all=force_all)
        finally:
            runtime._CTX.reset(token)

    # ------------------------------------------------------------------ #
    # Annotation resolution → sbatch directives
    # ------------------------------------------------------------------ #
    def _resolve(self, a) -> tuple[dict, dict]:
        """Resolve an artifact to ``(portable_resources, slurm_directives)``.

        Levels, last wins: executor ``defaults`` < artifact ``annotations`` (incl. its ``"slurm"``
        subdict) < CLI ``overrides`` (e.g. ``slurm run --partition cpu``). The executor's defaults
        and the CLI overrides are backend-specific, so a non-portable top-level key in either
        (``partition``, ``time``, …) is itself a Slurm directive; in an artifact's annotations the
        same position is free metadata, and Slurm-specific keys live under its ``"slurm"`` subdict.
        """
        portable: dict = {}
        slurm_d: dict = {}

        def _absorb(d):   # defaults/overrides: a non-portable top-level key *is* a Slurm directive
            for key, value in (d or {}).items():
                if key in _PORTABLE:
                    portable[key] = value
                elif key == "slurm":
                    slurm_d.update(value or {})
                else:
                    slurm_d[key] = value

        _absorb(self.defaults)                        # backend defaults (lowest precedence)
        ann = dict(getattr(a, "annotations", {}) or {})   # artifact annotations (middle)
        for key in _PORTABLE:
            if key in ann:
                portable[key] = ann[key]
        slurm_d.update(ann.get("slurm") or {})
        _absorb(self.overrides)                       # CLI overrides (`--partition` …): win over all
        return portable, slurm_d

    def _directives(self, portable: dict, slurm_d: dict) -> list[str]:
        """The ``#SBATCH`` directive args (without the ``#SBATCH `` prefix) for a resolved artifact."""
        req = ResourceRequest.from_annotations(portable)
        out = [f"--cpus-per-task={req.cpus}"]
        if req.gpus:
            out.append(f"--gpus={req.gpus}")          # a gpu_directive config could switch to --gres
        if req.memory_mb:
            out.append(f"--mem={req.memory_mb}M")
        if portable.get("runtime") and "time" not in slurm_d:    # explicit slurm.time wins
            out.append(f"--time={as_slurm_time(portable['runtime'])}")
        for key, value in slurm_d.items():
            flag = "--" + str(key).replace("_", "-")
            out.append(flag if value is True else f"{flag}={value}")
        return out

    # ------------------------------------------------------------------ #
    # Submission
    # ------------------------------------------------------------------ #
    def _make_rundir(self, targets) -> Path:
        rels = sorted(str(t.relpath) for t in targets)
        run_id = "slurm-" + fingerprint((Project.name, tuple(rels), int(time.time())), n=10)
        rundir = self.run_dir_path() / run_id
        (rundir / "jobs").mkdir(parents=True, exist_ok=True)
        (rundir / "scripts").mkdir(parents=True, exist_ok=True)
        return rundir

    def _submit_one(self, a, rundir: Path, dep_ids: list[str], base_path: Path,
                    *, forced: bool = False) -> str:
        portable, slurm_d = self._resolve(a)
        script = self._render_sbatch(
            name=job_name(Project.name, a.relpath),
            log_path=job_log_path(rundir, a.relpath),
            directives=self._directives(portable, slurm_d),
            dep_ids=dep_ids,
            relpath=a.relpath,
            base_path=base_path,
            forced=forced,
        )
        script_path = rundir / "scripts" / f"{_slug(a.relpath)}.sbatch"
        script_path.write_text(script)
        out = subprocess.run(["sbatch", "--parsable", str(script_path)],
                             capture_output=True, text=True, check=True).stdout
        jobid = out.strip().splitlines()[-1].split(";")[0]   # "--parsable" → "<jobid>[;<cluster>]"
        log.info("submitted %s as job %s%s", a.relpath, jobid,
                 f" (afterok {':'.join(dep_ids)})" if dep_ids else "")
        return jobid

    def _render_sbatch(self, *, name: str, log_path: Path, directives: list[str],
                       dep_ids: list[str], relpath: str, base_path: Path,
                       forced: bool = False) -> str:
        lines = ["#!/bin/bash",
                 f"#SBATCH --job-name={name}",
                 f"#SBATCH --output={log_path}",
                 "#SBATCH --open-mode=append"]        # requeued tasks append, not truncate
        lines += [f"#SBATCH {d}" for d in directives]
        if dep_ids:
            lines.append(f"#SBATCH --dependency=afterok:{':'.join(dep_ids)}")
            lines.append("#SBATCH --kill-on-invalid-dep=yes")   # dep failed ⇒ auto-cancel dependent
        lines.append("set -euo pipefail")
        if self.setup:
            lines.append(self.setup)
        for key, value in self.env.items():
            lines.append(f"export {key}={shlex.quote(str(value))}")
        worker = ["python", "-m", "pipelines", "--project", str(self.runfile),
                  "_worker", "--only", relpath, "--store", self.store,
                  "--base-path", str(base_path)]
        # A requeued/preempted task skips an already-committed output (--skip-committed); a
        # forced artifact must rebuild it, so omit the flag and let the worker clobber.
        if not forced:
            worker.append("--skip-committed")
        lines.append("exec " + " ".join(shlex.quote(tok) for tok in worker))
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------ #
    # Dry run
    # ------------------------------------------------------------------ #
    def _dryrun(self, to_submit, job_deps_of, satisfied, skipped, base_path: Path) -> int:
        extra = f", {len(skipped)} transient skipped" if skipped else ""
        print(f"# slurm plan: {len(to_submit)} job(s) to submit, {len(satisfied)} committed{extra}")
        print(f"# store={self.store}  base_path={base_path}")
        if self.setup:
            print(f"# setup: {self.setup}")
        if self.defaults:
            print(f"# defaults: {self.defaults}")
        if self.overrides:
            print(f"# overrides (CLI): {self.overrides}")
        for a in to_submit:
            portable, slurm_d = self._resolve(a)
            directives = " ".join(self._directives(portable, slurm_d))
            print(f"[job ] {a.relpath}")
            print(f"         name={job_name(Project.name, a.relpath)}")
            print(f"         #SBATCH {directives}")
            deps = job_deps_of(a)
            if deps:
                wiring = ":".join(f"<job:{d}>" for d in deps)
                print(f"         #SBATCH --dependency=afterok:{wiring} --kill-on-invalid-dep=yes")
            print(f"         path={base_path / a.relpath}")
            unconsumed = sorted(set(getattr(a, "annotations", {}) or {}) - _CONSUMED_KEYS)
            if unconsumed:
                print(f"         ! annotations not consumed by slurm: {', '.join(unconsumed)}")
        for rp in sorted(satisfied):
            print(f"[skip] {rp}  (committed)")
        for rp in sorted(skipped):
            print(f"[trans] {rp}  (transient, no running consumer)")
        return 0


# --------------------------------------------------------------------------- #
def _scancel(jobids: list[str]) -> None:
    if not jobids or shutil.which("scancel") is None:
        return
    try:
        subprocess.run(["scancel", *jobids], check=False)
    except OSError as e:
        log.warning("scancel failed: %s", e)


def _as_path(base_path) -> Path:
    return Path(str(base_path).replace("file://", "")) if base_path else Path.cwd()


def _slug(relpath: str) -> str:
    from ...identity import slug
    return slug(relpath)
