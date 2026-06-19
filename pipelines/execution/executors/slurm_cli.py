"""``pipelines slurm <subcommand>`` — manage a project's Slurm jobs as a group.

Jobs are submitted individually (one per artifact), but you interact with them as a project group.
Every subcommand is backed by the **stateless reconciliation** from ``design/06`` §5: re-derive each
artifact's opaque job name from the graph (``<hash(project)>:<hash(project, relpath)>``), query the
user's ``squeue``/``sacct`` jobs, keep the ones in our namespace, and join by name. There is no
stored job map, so nothing can drift.

Subcommands:
  run [SEL] [--force] [--detach] [--dry]   submit + foreground monitor (the SlurmExecutor)
  ls/status [SEL] [--expand] [--watch]     reconciled status, combining like jobs by artifact class
  cancel [SEL] [--dry]                     scancel the matching jobs
  sendcommand '<tmpl>' [SEL] [--dry]       run a per-job command, substituting {jobid}/{relpath}/…
  attach [RUN_ID]                          resume the monitor for a detached/finished run
  logs SEL                                 print a job's captured log from the run dir
  cat JOBID                                print a job's captured log, located by Slurm job id
"""

from __future__ import annotations

import json
import logging
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

from ...project import Project
from ...scheduler import registry
from ...scheduler.server import job_log_path
from .slurm import SlurmExecutor, job_name, project_prefix

log = logging.getLogger("pipelines")

# Columns for the grouped `ls` table: (label, event-state). ``None`` = artifact with no live job.
_LS_COLS = [("RUN", "running"), ("PEND", "queued"), ("DONE", "completed"),
            ("FAIL", "failed"), ("CXL", "cancelled"), ("BLOCK", "blocked"), ("—", None)]

# CLI opts on `slurm run` that map straight to Slurm submit directives and win over the executor's
# defaults and per-artifact annotations (see SlurmExecutor._resolve). Resource sizing
# (cpus/memory/gpus) deliberately stays on the portable annotation path, not overridable here.
_SLURM_OPT_KEYS = ("partition", "account", "qos", "time", "constraint",
                   "nodelist", "exclude", "reservation")


def _slurm_overrides(opts: dict) -> dict:
    """The Slurm-directive overrides (e.g. ``--partition cpu``) picked out of the parsed CLI opts."""
    return {k: opts[k] for k in _SLURM_OPT_KEYS if k in opts}


def main(groups: dict[str, list], executor, argv: list[str]) -> int:
    """Dispatch a ``slurm`` subcommand. ``argv`` is everything after ``slurm``."""
    from ...cli import _parse, _resolve, _universe   # local import: cli is the caller

    if not argv or argv[0] in ("help", "-h", "--help"):
        return _usage()
    sub, rest = argv[0], argv[1:]
    positionals, opts, flags = _parse(rest)

    if sub == "run":
        targets = _resolve(groups, positionals)
        if not targets:
            print(f"no targets matched {positionals or ['(none)']}", file=sys.stderr)
            return 2
        return _slurm_executor(executor, _slurm_overrides(opts)).run(
            targets, force="force" in flags, force_all="force-all" in flags,
            dry="dry" in flags, detach="detach" in flags)

    if sub in ("ls", "status"):
        return _ls(groups, executor, positionals, flags, _resolve, _universe)
    if sub == "cancel":
        return _cancel(groups, executor, positionals, flags, _resolve, _universe)
    if sub == "sendcommand":
        return _sendcommand(groups, executor, positionals, flags, _resolve, _universe)
    if sub in ("attach", "monitor"):
        return _attach(executor, positionals)
    if sub == "logs":
        return _logs(groups, executor, positionals, _resolve)
    if sub == "cat":
        return cat_main(rest)

    print(f"unknown slurm subcommand {sub!r} "
          f"(have: run, ls, status, cancel, sendcommand, attach, logs, cat)",
          file=sys.stderr)
    return 2


# --------------------------------------------------------------------------- #
# Stateless reconciliation: artifact graph ⋈ squeue/sacct, joined by job name.
# --------------------------------------------------------------------------- #
def reconcile(universe: dict, executor) -> list[dict]:
    """One record per artifact (+ any stale jobs in our namespace), joined by opaque job name."""
    from ...scheduler.slurm_monitor import _STATE_MAP

    project = Project.name
    prefix = project_prefix(project) + ":"
    by_name = {job_name(project, rp): rp for rp in universe}

    found: dict[str, dict] = {}
    for jid, name, raw, part in _squeue_mine():            # active jobs (most current)
        if name.startswith(prefix):
            found[name] = {"jobid": jid, "raw": raw, "partition": part, "exit": None}
    for jid, name, raw, exit_ in _sacct_mine():            # recent/terminal (don't override active)
        if name.startswith(prefix) and name not in found:
            found[name] = {"jobid": jid, "raw": raw, "partition": "", "exit": exit_}

    rows: list[dict] = []
    for name, rp in by_name.items():
        cls = type(universe[rp]).__name__
        rec = found.pop(name, None)
        rows.append(_row(rp, cls, name, rec, _STATE_MAP))
    for name, rec in found.items():                        # our jobs absent from the current graph
        row = _row(None, "(stale)", name, rec, _STATE_MAP)
        row["stale"] = True
        rows.append(row)
    return rows


def _row(relpath, cls, name, rec, state_map) -> dict:
    if rec is None:
        return {"relpath": relpath, "cls": cls, "name": name, "jobid": None,
                "state": None, "raw_state": None, "partition": "", "exit": None}
    return {"relpath": relpath, "cls": cls, "name": name, "jobid": rec["jobid"],
            "state": state_map.get(rec["raw"], "queued"), "raw_state": rec["raw"],
            "partition": rec["partition"], "exit": rec["exit"]}


# --------------------------------------------------------------------------- #
# Subcommands
# --------------------------------------------------------------------------- #
def _ls(groups, executor, positionals, flags, _resolve, _universe) -> int:
    universe = _universe(groups)
    sel = {t.relpath for t in _resolve(groups, positionals)}
    sx = _slurm_executor(executor)

    def render():
        recs = [r for r in reconcile(universe, sx) if r["relpath"] in sel]
        _print_ls(recs, expand="expand" in flags)

    if "watch" in flags:
        try:
            while True:
                print("\033[2J\033[H", end="")                 # clear screen
                print(f"# pipelines slurm ls  ({time.strftime('%H:%M:%S')})")
                render()
                time.sleep(5.0)
        except KeyboardInterrupt:
            return 0
    render()
    return 0


def _cancel(groups, executor, positionals, flags, _resolve, _universe) -> int:
    universe = _universe(groups)
    sel = {t.relpath for t in _resolve(groups, positionals)}
    sx = _slurm_executor(executor)
    recs = [r for r in reconcile(universe, sx)
            if r["relpath"] in sel and r["jobid"] and r["state"] in ("queued", "running")]
    if not recs:
        print("pipelines: no running or queued jobs match the selection")
        return 0
    jobids = [r["jobid"] for r in recs]
    for r in recs:
        print(f"  cancel {r['jobid']:>10}  {r['state']:<8} {r['relpath']}")
    if "dry" in flags:
        return 0
    if shutil.which("scancel") is None:
        print("pipelines: scancel not on PATH", file=sys.stderr)
        return 1
    subprocess.run(["scancel", *jobids], check=False)
    print(f"pipelines: cancelled {len(jobids)} job(s)")
    return 0


def _sendcommand(groups, executor, positionals, flags, _resolve, _universe) -> int:
    if not positionals:
        print("usage: pipelines slurm sendcommand '<template with {jobid}>' [SELECTOR ...]",
              file=sys.stderr)
        return 2
    template, selectors = positionals[0], positionals[1:]
    universe = _universe(groups)
    sel = {t.relpath for t in _resolve(groups, selectors)}
    sx = _slurm_executor(executor)
    recs = [r for r in reconcile(universe, sx) if r["relpath"] in sel and r["jobid"]]
    if not recs:
        print("pipelines: no jobs match the selection")
        return 0
    rc = 0
    for r in recs:
        fields = {"jobid": r["jobid"], "relpath": r["relpath"], "name": r["name"],
                  "state": r["state"] or "", "cls": r["cls"], "partition": r["partition"]}
        try:
            cmd = template.format(**fields)
        except (KeyError, IndexError) as e:
            print(f"pipelines: bad template field {e}; available: {', '.join(fields)}",
                  file=sys.stderr)
            return 2
        if "dry" in flags:
            print(cmd)
            continue
        result = subprocess.run(shlex.split(cmd), check=False)
        rc = rc or result.returncode
    return rc


def _attach(executor, positionals) -> int:
    from ...scheduler.slurm_monitor import SlurmMonitor

    run_id = positionals[0] if positionals else None
    logdir = _find_run(run_id)
    if logdir is None:
        which = f"run {run_id!r}" if run_id else "any slurm run"
        print(f"pipelines: could not find {which} in the registry", file=sys.stderr)
        return 2
    return SlurmMonitor.from_rundir(logdir).attach()


def _logs(groups, executor, positionals, _resolve) -> int:
    targets = _resolve(groups, positionals)
    if not targets:
        print(f"no targets matched {positionals or ['(none)']}", file=sys.stderr)
        return 2
    logdir = _find_run(None)
    if logdir is None:
        print("pipelines: no slurm run found", file=sys.stderr)
        return 2
    for t in targets:
        _print_log(t.relpath, job_log_path(logdir, t.relpath))
    return 0


def cat_main(argv: list[str]) -> int:
    """``pipelines slurm cat JOBID`` — print a job's captured log, located by Slurm job id.

    Project-independent: the log is found purely from the run registry (the same default location
    pipelines records every run under), so this runs from any directory — no ``run.py`` needed.
    """
    from ...cli import _parse                          # local import: cli is the caller

    positionals, _opts, _flags = _parse(argv)
    if not positionals:
        print("usage: pipelines slurm cat JOBID", file=sys.stderr)
        return 2
    jobid = positionals[0]
    found = _find_job(jobid)
    if found is None:
        print(f"pipelines: no slurm job {jobid!r} found in the run registry", file=sys.stderr)
        return 1
    relpath, logdir = found
    _print_log(relpath, job_log_path(logdir, relpath))
    return 0


def _print_log(relpath: str, path: Path) -> None:
    """Print one job's captured log with a header, or a placeholder if it isn't there yet."""
    print(f"==> {relpath}  ({path})")
    try:
        print(path.read_text())
    except OSError:
        print("  (no log yet)")


def _find_job(jobid: str) -> tuple[str, Path] | None:
    """Find ``(relpath, logdir)`` for a Slurm job id by scanning run dirs in the registry.

    Each slurm run dir's ``meta.json`` holds the ``relpath -> job id`` map the monitor submitted;
    we invert it. Runs are scanned newest first, so a reused job id resolves to its latest run.
    """
    target = str(jobid)
    for info in registry.list_runs(only_alive=False):     # newest first
        if info.get("kind") != "slurm":
            continue
        logdir = info.get("logdir")
        if not logdir:
            continue
        try:
            meta = json.loads((Path(logdir) / "meta.json").read_text())
        except (OSError, ValueError):
            continue
        for relpath, jid in (meta.get("jobids") or {}).items():
            if str(jid) == target:
                return relpath, Path(logdir)
    return None


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _slurm_executor(executor, overrides=None) -> SlurmExecutor:
    """The configured executor if it is a SlurmExecutor, else one built from ``Project.config``.

    ``overrides`` are CLI-supplied Slurm directives (e.g. ``--partition cpu``) that win over both
    the executor ``defaults`` and per-artifact annotations (see ``SlurmExecutor._resolve``).
    """
    overrides = overrides or {}
    if isinstance(executor, SlurmExecutor):
        if overrides:
            executor.overrides = {**getattr(executor, "overrides", {}), **overrides}
        return executor
    cfg = Project.config
    get = (lambda k, d=None: cfg.get(k, d)) if cfg is not None else (lambda k, d=None: d)
    return SlurmExecutor(
        store=getattr(executor, "store", None) or get("remote_store") or get("store"),
        base_path=getattr(executor, "base_path", None) or get("base_path"),
        setup=get("slurm_setup"), defaults=get("slurm_defaults") or {},
        env=get("slurm_env") or {}, run_dir=get("slurm_run_dir"),
        overrides=overrides,
    )


def _find_run(run_id) -> Path | None:
    """The run dir for a slurm run id (newest if ``run_id`` is None), from the registry."""
    for info in registry.list_runs(only_alive=False):     # newest first
        if info.get("kind") != "slurm":
            continue
        if run_id is None or registry.run_id_of(info) == run_id:
            logdir = info.get("logdir")
            if logdir:
                return Path(logdir)
    return None


def _squeue_mine() -> list[tuple]:
    if shutil.which("squeue") is None:
        return []
    try:
        out = subprocess.run(["squeue", "--me", "-h", "-o", "%i|%j|%T|%P"],
                             capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError) as e:
        log.debug("squeue failed: %s", e)
        return []
    res = []
    for line in out.splitlines():
        p = line.split("|")
        if len(p) >= 3 and "_" not in p[0]:               # skip array tasks (not used in v1)
            res.append((p[0].strip(), p[1].strip(), p[2].strip(),
                        p[3].strip() if len(p) > 3 else ""))
    return res


def _sacct_mine() -> list[tuple]:
    if shutil.which("sacct") is None:
        return []
    import getpass
    from datetime import datetime, timedelta

    from ...scheduler.slurm_monitor import _exit_code
    start = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        out = subprocess.run(
            ["sacct", "-u", getpass.getuser(), "-n", "-P", "-X", "-S", start,
             "--format=JobID,JobName,State,ExitCode"],
            capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError) as e:
        log.debug("sacct failed: %s", e)
        return []
    res = []
    for line in out.splitlines():
        c = line.split("|")
        if len(c) >= 4 and "." not in c[0] and "_" not in c[0]:
            raw = c[2].strip().split()[0] if c[2].strip() else ""
            res.append((c[0].strip(), c[1].strip(), raw, _exit_code(c[3].strip())))
    return res


def _print_ls(recs: list[dict], expand: bool) -> None:
    groups: dict[str, list] = {}
    for r in recs:
        groups.setdefault(r["cls"], []).append(r)
    width = max([len("STEP (class)"), *(len(c) for c in groups)] or [12])
    print(f"{'STEP (class)':<{width}}  TOTAL  " + "  ".join(f"{lbl:>5}" for lbl, _ in _LS_COLS))
    for cls in sorted(groups):
        members = groups[cls]
        counts: dict = {}
        for r in members:
            counts[r["state"]] = counts.get(r["state"], 0) + 1
        cells = "  ".join(f"{counts.get(state, 0):>5}" for _, state in _LS_COLS)
        print(f"{cls:<{width}}  {len(members):>5}  {cells}")
    if expand:
        print()
        for r in sorted(recs, key=lambda r: (r["cls"], r["relpath"] or "")):
            print(f"  {(r['state'] or '—'):<10} {(r['jobid'] or '—'):>12}  "
                  f"{r['relpath'] or r['name']}")


def _usage() -> int:
    print("usage: pipelines slurm <run|ls|status|cancel|sendcommand|attach|logs|cat> [SELECTOR ...]")
    print("  run [SEL...] [--partition P] [--account A] [--qos Q] [--time T] [--force] [--detach] "
          "[--dry]   submit + monitor (CLI Slurm directives override config/annotations)")
    print("  ls|status [SEL...] [--expand] [--watch]      reconciled status by artifact class")
    print("  cancel [SEL...] [--dry]                      scancel matching running/queued jobs")
    print("  sendcommand '<tmpl>' [SEL...] [--dry]        per-job cmd; {jobid}/{relpath}/{name}/…")
    print("  attach [RUN_ID]                              resume the monitor (newest if omitted)")
    print("  logs SEL...                                  print a job's captured log (by selector)")
    print("  cat JOBID                                    print a job's captured log (by job id)")
    return 0
