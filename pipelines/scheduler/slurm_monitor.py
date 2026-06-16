"""``SlurmMonitor`` — the single writer of a Slurm run's ``events.log``.

A Slurm run has no long-lived central process and no TCP port: ``sbatch`` queues the jobs and the
submitting process can exit. So observability is provided by a foreground (detachable) monitor — the
cluster analogue of the parallel :class:`~pipelines.scheduler.server.RunServer`. It is the **sole
writer** of the run's append-only ``events.log`` (reusing :class:`~pipelines.scheduler.events.EventLog`
verbatim), which is exactly what the dashboard already replays — so a Slurm run shows up there with
no change to the replay path.

The monitor polls ``squeue``/``sacct``, maps Slurm job states onto the existing event vocabulary
(``queued``/``running``/``completed``/``failed``/``cancelled``, plus ``blocked`` computed from the
in-plan dependency graph), and emits a ``job_state`` only on change. Liveness is file-based: it
touches ``heartbeat`` each poll and writes a ``done`` marker when the run finalizes — the registry's
``alive()`` reads those (no socket), so the dashboard keeps working after the monitor exits.

Detach (Ctrl-C or ``start_detached``) leaves the jobs running; ``pipelines slurm attach`` resumes by
replaying the existing log to rebuild state and continuing the poll loop, backfilling any transitions
missed while detached.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path

from . import registry
from ..reporting import Reporter

log = logging.getLogger("pipelines")

# Slurm job state → the dashboard's existing event-state vocabulary.
_STATE_MAP = {
    "PENDING": "queued", "CONFIGURING": "queued", "REQUEUED": "queued",
    "REQUEUE_HOLD": "queued", "REQUEUE_FED": "queued", "RESV_DEL_HOLD": "queued",
    "SUSPENDED": "queued", "PREEMPTED": "queued",      # preempted ⇒ usually requeued ⇒ not done
    "RUNNING": "running", "COMPLETING": "running", "STAGE_OUT": "running",
    "COMPLETED": "completed",
    "FAILED": "failed", "TIMEOUT": "failed", "OUT_OF_MEMORY": "failed",
    "NODE_FAIL": "failed", "BOOT_FAIL": "failed", "DEADLINE": "failed",
    "CANCELLED": "cancelled",
}
_TERMINAL = {"completed", "failed", "cancelled", "blocked"}
_FAILED_DEP = {"failed", "cancelled", "blocked"}


class SlurmMonitor:
    """Polls Slurm for one submitted run and records it to ``<rundir>/events.log``."""

    def __init__(self, *, rundir, run_id: str, project, store, base_path,
                 manifest: list[dict], jobids: dict[str, str], poll_interval=None):
        self.rundir = Path(rundir)
        self.run_id = run_id
        self.project = project
        self.store = store
        self.base_path = base_path
        self.manifest = manifest
        self.jobids = dict(jobids)                  # relpath -> slurm job id (jobs we submitted)
        self.poll = float(poll_interval) if poll_interval else 10.0
        self.started_at = time.time()

        self.order = [e["relpath"] for e in manifest]
        self.cls = {e["relpath"]: e.get("cls") for e in manifest}
        self.deps = {e["relpath"]: list(e.get("deps") or []) for e in manifest}
        self.last_state = {
            e["relpath"]: ("cached" if e.get("cached") else
                           "skipped" if e.get("skipped") else "queued")
            for e in manifest
        }
        # The monitor keeps writing its shared-FS events.log/heartbeat/done (for FS-sharing
        # dashboards); the Reporter additionally ships the same events.log to a dashboard machine
        # over SSH when [config.dashboard] is set. The slurm run_id is already globally unique.
        self.reporter = Reporter.open(
            events_path=self.rundir / "events.log", run_id=self.run_id,
            meta={"project": self.project, "store": self.store,
                  "base_path": self.base_path, "started_at": self.started_at})
        self._lockf = None

    # ------------------------------------------------------------------ #
    # Re-attach
    # ------------------------------------------------------------------ #
    @classmethod
    def from_rundir(cls, rundir) -> "SlurmMonitor":
        """Reconstruct a monitor for an existing run from its ``meta.json`` (for ``slurm attach``)."""
        rundir = Path(rundir)
        meta = json.loads((rundir / "meta.json").read_text())
        mon = cls(rundir=rundir, run_id=meta["run_id"], project=meta.get("project"),
                  store=meta.get("store"), base_path=meta.get("base_path"),
                  manifest=meta["manifest"], jobids=meta.get("jobids", {}),
                  poll_interval=meta.get("poll_interval"))
        mon.started_at = meta.get("created_at", mon.started_at)
        return mon

    # ------------------------------------------------------------------ #
    # Entry points
    # ------------------------------------------------------------------ #
    def run(self) -> int:
        """Fresh run: write the plan, then poll to completion (foreground)."""
        self._acquire_lock()
        try:
            self._setup(emit_start=True)
            print(f"pipelines: slurm run {self.run_id} — {len(self.jobids)} job(s)")
            print(f"pipelines: events -> {self.rundir / 'events.log'}")
            print(f"pipelines: detach with Ctrl-C (jobs keep running); "
                  f"re-attach with `pipelines slurm attach {self.run_id}`")
            return self._loop()
        finally:
            self._release_lock()

    def start_detached(self) -> None:
        """Fresh run, fire-and-forget: write the plan/registry once and return (no poll loop)."""
        self._acquire_lock()
        try:
            self._setup(emit_start=True)
            # No poll loop will run here, so flush server_start to the dashboard and stop shipping
            # (no `done`: the run continues). A later `slurm attach` starts a fresh Reporter.
            self.reporter.close(ok=None)
        finally:
            self._release_lock()

    def attach(self) -> int:
        """Resume monitoring an already-submitted run, backfilling missed transitions."""
        self._acquire_lock()
        try:
            self._setup(emit_start=False)        # keep the original server_start; just continue
            self._replay_last_state()
            return self._loop()
        finally:
            self._release_lock()

    # ------------------------------------------------------------------ #
    # Setup / teardown
    # ------------------------------------------------------------------ #
    def _setup(self, *, emit_start: bool) -> None:
        meta = {
            "run_id": self.run_id, "project": self.project, "store": self.store,
            "base_path": self.base_path, "created_at": self.started_at,
            "poll_interval": self.poll, "jobids": self.jobids, "manifest": self.manifest,
        }
        (self.rundir / "meta.json").write_text(json.dumps(meta, indent=2))
        registry.write({
            "run_id": self.run_id, "kind": "slurm", "logdir": str(self.rundir),
            "base_path": self.base_path, "store": self.store, "project": self.project,
            "started_at": self.started_at, "heartbeat_stale": max(45.0, self.poll * 3),
        })
        self._beat()
        if emit_start:
            self.reporter.emit(
                "server_start", run_id=self.run_id, logdir=str(self.rundir), pool={},
                n_jobs=len(self.jobids),
                jobs=[rp for rp in self.order if rp in self.jobids],
                project=self.project, store=self.store, base_path=self.base_path,
                plan=self.manifest)

    def _beat(self) -> None:
        try:
            (self.rundir / "heartbeat").touch()
        except OSError:
            pass

    def _acquire_lock(self) -> None:
        import fcntl
        self._lockf = open(self.rundir / "monitor.lock", "w")
        try:
            fcntl.flock(self._lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._lockf.close()
            self._lockf = None
            raise SystemExit(
                f"pipelines: another monitor is already attached to {self.run_id}")
        self._lockf.write(str(os.getpid()))
        self._lockf.flush()

    def _release_lock(self) -> None:
        if self._lockf is None:
            return
        import fcntl
        try:
            fcntl.flock(self._lockf, fcntl.LOCK_UN)
            self._lockf.close()
        except OSError:
            pass
        self._lockf = None

    # ------------------------------------------------------------------ #
    # Poll loop
    # ------------------------------------------------------------------ #
    def _loop(self) -> int:
        if shutil.which("squeue") is None and shutil.which("sacct") is None:
            log.warning("neither squeue nor sacct on PATH; cannot monitor %s", self.run_id)
            return 1
        try:
            while not self._all_terminal():
                self._beat()
                self._emit_changes(self._poll())
                if self._all_terminal():
                    break
                time.sleep(self.poll)
        except KeyboardInterrupt:
            print("\npipelines: detached; Slurm jobs keep running. "
                  f"Re-attach with `pipelines slurm attach {self.run_id}`")
            self.reporter.close(ok=None)        # flush what we have, stop shipping, no `done`
            return 0
        ok = self._finalize()
        return 0 if ok else 1

    def _all_terminal(self) -> bool:
        return all(self.last_state.get(rp) in _TERMINAL for rp in self.jobids)

    def _emit_changes(self, states: dict) -> None:
        computed: dict[str, str] = {}
        for rp in self.order:
            if rp not in self.jobids:                       # cached / skipped: keep the seed
                computed[rp] = self.last_state.get(rp)
                continue
            computed[rp] = self._event_state(states.get(self.jobids[rp]))
        # "blocked" overrides a non-success state when an in-plan dep failed. Manifest order is
        # topological, so a dep's computed state is already settled (and blocked propagates).
        for rp in self.order:
            if rp not in self.jobids or computed[rp] == "completed":
                continue
            if any(computed.get(d) in _FAILED_DEP for d in self.deps.get(rp, [])):
                computed[rp] = "blocked"
        for rp in self.order:
            if rp not in self.jobids:
                continue
            new = computed[rp]
            if new and new != self.last_state.get(rp):
                raw = states.get(self.jobids[rp], {})
                self.reporter.emit(
                    "job_state", relpath=rp, cls=self.cls.get(rp), state=new,
                    job_id=self.jobids[rp], deps=self.deps.get(rp, []),
                    exit_code=raw.get("exit_code"), reason=raw.get("reason", ""),
                    started_at=raw.get("started_at"), ended_at=raw.get("ended_at"))
                self.last_state[rp] = new

    @staticmethod
    def _event_state(raw: dict | None) -> str:
        if not raw:
            return "queued"                                 # not yet visible to squeue/sacct
        return _STATE_MAP.get(raw.get("state", ""), "queued")

    def _finalize(self) -> bool:
        counts: dict[str, int] = {}
        for rp in self.order:
            s = self.last_state.get(rp, "queued")
            counts[s] = counts.get(s, 0) + 1
        ok = not any(self.last_state.get(rp) in _FAILED_DEP for rp in self.jobids)
        self.reporter.emit("server_done", ok=ok, counts=counts)
        try:
            (self.rundir / "done").write_text(json.dumps({"ok": ok, "counts": counts}))
        except OSError:
            pass
        self.reporter.close(ok=ok, counts=counts)   # final flush + remote `done` if shipping
        print(f"pipelines: slurm run {self.run_id} finished — {'ok' if ok else 'FAILED'} {counts}")
        return ok

    # ------------------------------------------------------------------ #
    # Slurm queries
    # ------------------------------------------------------------------ #
    def _poll(self) -> dict:
        jids = list(self.jobids.values())
        states = self._sacct(jids)                          # terminal authority + exit/timestamps
        for jid, st in self._squeue(jids).items():          # active jobs: more current, override
            states[jid] = {**states.get(jid, {}), **st}
        return states

    @staticmethod
    def _squeue(jids: list[str]) -> dict:
        if not jids or shutil.which("squeue") is None:
            return {}
        try:
            out = subprocess.run(
                ["squeue", "-h", "-o", "%i|%T|%R", "-j", ",".join(jids)],
                capture_output=True, text=True, timeout=30).stdout
        except (OSError, subprocess.SubprocessError) as e:
            log.debug("squeue failed: %s", e)
            return {}
        res = {}
        for line in out.splitlines():
            parts = line.split("|")
            if len(parts) < 2 or "_" in parts[0]:           # skip array tasks (not used in v1)
                continue
            res[parts[0].strip()] = {"state": parts[1].strip(),
                                     "reason": parts[2].strip() if len(parts) > 2 else ""}
        return res

    @staticmethod
    def _sacct(jids: list[str]) -> dict:
        if not jids or shutil.which("sacct") is None:
            return {}
        try:
            out = subprocess.run(
                ["sacct", "-n", "-P", "-j", ",".join(jids),
                 "--format=JobID,State,ExitCode,Start,End"],
                capture_output=True, text=True, timeout=30).stdout
        except (OSError, subprocess.SubprocessError) as e:
            log.debug("sacct failed: %s", e)
            return {}
        res = {}
        for line in out.splitlines():
            cols = line.split("|")
            if len(cols) < 5 or "." in cols[0]:             # skip .batch / .extern job steps
                continue
            raw_state = cols[1].strip()
            res[cols[0].strip()] = {
                "state": raw_state.split()[0] if raw_state else "",   # "CANCELLED by 1" -> "CANCELLED"
                "reason": raw_state,
                "exit_code": _exit_code(cols[2].strip()),
                "started_at": _parse_ts(cols[3].strip()),
                "ended_at": _parse_ts(cols[4].strip()),
            }
        return res

    # ------------------------------------------------------------------ #
    def _replay_last_state(self) -> None:
        """Rebuild ``last_state`` from the existing events.log so attach emits only new changes."""
        try:
            text = (self.rundir / "events.log").read_text()
        except OSError:
            return
        for line in text.splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if rec.get("type") == "job_state" and rec.get("relpath"):
                self.last_state[rec["relpath"]] = rec.get("state", self.last_state.get(rec["relpath"]))


def _exit_code(value: str):
    """sacct ``ExitCode`` is ``code:signal`` (e.g. ``0:0``) → the integer code, or ``None``."""
    if not value:
        return None
    try:
        return int(value.split(":")[0])
    except (ValueError, IndexError):
        return None


def _parse_ts(value: str):
    """A sacct ISO-local timestamp → epoch seconds; ``None`` for Unknown/None/blank."""
    if not value or value in ("Unknown", "None", "N/A"):
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return None
