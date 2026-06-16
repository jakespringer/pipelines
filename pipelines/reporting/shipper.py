"""``Shipper`` — a daemon thread that mirrors one run's logs to the dashboard machine over SSH.

Robustness contract — nothing here ever raises into the job. Every ``ssh``/``rsync`` call is wrapped
and bounded by a timeout; on *any* failure the thread sleeps (exponential backoff, capped) and
retries. The local ``events.log`` is the durable source of truth, so ``rsync --append`` ships only
the byte delta and a reconnect after any outage replays exactly what the dashboard missed — no custom
replay code, no acknowledgements.

Per tick (≈ ``cfg.interval`` s), over the one shared SSH connection:

* ``rsync --append`` the ``events.log`` (and the per-job ``jobs/`` logs — also append-only),
* ``touch`` the remote ``heartbeat`` (liveness, covers idle periods with no new events),
* if this process owns the host metrics lock, ``rsync`` (plain, not append) the ``<node>.jsonl``.

The registry entry is written once at bootstrap by piping JSON over plain ``ssh``; the dashboard
machine therefore needs only ``sshd`` + ``rsync`` + coreutils to ingest (no pipelines install).
"""

from __future__ import annotations

import json
import logging
import random
import subprocess
import threading
from pathlib import Path, PurePosixPath

from . import ssh
from .config import DashboardConfig

log = logging.getLogger("pipelines")


class _ShipError(Exception):
    """A transient transport failure; the loop catches it and backs off."""


class Backoff:
    """Exponential backoff with a hard cap and light jitter (so many hosts don't sync in lockstep)."""

    def __init__(self, base: float = 2.0, factor: float = 2.0, cap: float = 300.0):
        self.base, self.factor, self.cap = base, factor, max(base, cap)
        self._n = 0

    def reset(self) -> None:
        self._n = 0

    def next(self) -> float:
        delay = min(self.cap, self.base * (self.factor ** self._n))
        self._n += 1
        return min(self.cap, delay + random.uniform(0.0, delay * 0.1))


class Shipper(threading.Thread):
    """Owns the remote mirror of one run. Reads the local logs; never writes to them."""

    _SUBPROC_TIMEOUT = 30.0     # hard cap per ssh/rsync call — a slow network can't wedge the thread

    def __init__(self, cfg: DashboardConfig, *, events_path, run_id: str, meta: dict):
        super().__init__(name=f"pipelines-shipper-{run_id}", daemon=True)
        self.cfg = cfg
        self.events_path = Path(events_path)
        self.jobs_dir = self.events_path.parent / "jobs"
        self.run_id = run_id
        self.meta = dict(meta or {})
        self._stop_event = threading.Event()
        self._final: tuple | None = None        # (ok, counts) once close() requests shutdown
        self._disabled = False                   # ssh/rsync binary missing: stop trying
        self._bootstrapped = False
        self._last_size = -1                     # last local events.log size (truncation guard)
        self._metrics = None                     # MetricsProducer once we win the host lock
        self._abs_run_dir: str | None = None     # remote run dir, resolved to an absolute path

    # -- remote paths --------------------------------------------------- #
    @property
    def _run_dir(self) -> str:
        return f"{self.cfg.runs_dir}/{self.run_id}"

    @property
    def _entry_path(self) -> str:
        return f"{self.cfg.runs_dir}/{self.run_id}.json"

    def _rd(self) -> str:
        """The run dir for an ssh *shell* command (``~`` ok) — absolute once bootstrap resolved it."""
        return self._abs_run_dir or self._run_dir

    # -- public control (called from the scheduler thread) -------------- #
    def request_stop(self, *, ok=None, counts=None, timeout: float = 30.0) -> None:
        """Ask the thread to do a final flush (with a ``done`` marker if ``ok`` is not None) and exit."""
        self._final = (ok, counts)
        self._stop_event.set()
        self.join(timeout=timeout)

    # -- thread body ---------------------------------------------------- #
    def run(self) -> None:
        backoff = Backoff(cap=self.cfg.backoff_max)
        while not self._stop_event.is_set():
            if self._disabled:
                return
            try:
                self._tick()
                backoff.reset()
                self._stop_event.wait(self.cfg.interval)
            except _ShipError as exc:
                log.debug("pipelines: dashboard ship failed (retrying): %s", exc)
                self._stop_event.wait(backoff.next())
            except Exception as exc:              # belt-and-suspenders: nothing escapes the thread
                log.debug("pipelines: dashboard shipper error (retrying): %s", exc)
                self._stop_event.wait(backoff.next())
        self._final_flush()

    def _tick(self) -> None:
        if not self._bootstrapped:
            self._remote_bootstrap()
            self._bootstrapped = True
        self._ensure_metrics()
        self._ship_events()
        self._ship_logs()
        self._heartbeat()
        self._ship_metrics()

    # -- steps ---------------------------------------------------------- #
    def _registry_entry(self) -> dict:
        return {
            "run_id": self.run_id,
            "kind": "remote",
            "node": self.cfg.node_id,
            "project": self.meta.get("project"),
            "store": self.meta.get("store"),
            "base_path": self.meta.get("base_path"),
            "started_at": self.meta.get("started_at"),
            "heartbeat_stale": max(45.0, self.cfg.interval * 3),
        }

    def _remote_bootstrap(self) -> None:
        dirs = self._run_dir
        if self.cfg.metrics:
            dirs += " " + self.cfg.remote_metrics_dir
        body = json.dumps(self._registry_entry(), indent=2).encode("utf-8")
        # Create the dirs, write the registry entry, and resolve the run dir to an absolute path:
        # rsync transfer paths must not depend on the remote shell expanding `~` (modern rsync
        # protects remote args). `~` in this *shell* command still expands fine.
        cmd = f"mkdir -p {dirs} && cat > {self._entry_path} && cd {self._run_dir} && pwd"
        out = (self._run(ssh.ssh_argv(self.cfg, cmd), input=body) or b"")
        lines = out.decode("utf-8", "replace").strip().splitlines()
        if not lines or not lines[-1].startswith("/"):
            raise _ShipError("could not resolve remote run dir")
        self._abs_run_dir = lines[-1]

    def _ship_events(self) -> None:
        if self._abs_run_dir is None:
            return
        try:
            size = self.events_path.stat().st_size
        except OSError:
            return                                # not written yet; nothing to ship
        append = not (0 <= size < self._last_size)   # local shrank ⇒ re-mirror to re-establish prefix
        self._run(ssh.rsync_argv(self.cfg, self.events_path, f"{self._abs_run_dir}/events.log",
                                 append=append))
        self._last_size = size

    def _ship_logs(self) -> None:
        if self._abs_run_dir is None or not self.jobs_dir.is_dir():
            return
        self._run(ssh.rsync_argv(self.cfg, self.jobs_dir, f"{self._abs_run_dir}/jobs",
                                 append=True, recursive=True))

    def _heartbeat(self) -> None:
        self._run(ssh.ssh_argv(self.cfg, f"touch {self._rd()}/heartbeat"))

    def _ensure_metrics(self) -> None:
        """Lazily become this host's metrics producer (one per node) once the lock is free."""
        if not self.cfg.metrics or self._metrics is not None:
            return
        from .metrics_producer import MetricsProducer
        prod = MetricsProducer(self.cfg.node_id)
        if prod.start() is not None:              # won the host lock; sampler is running
            self._metrics = prod

    def _ship_metrics(self) -> None:
        if self._metrics is None or self._metrics.path is None or self._abs_run_dir is None:
            return
        path = self._metrics.path
        if not path.exists():
            return
        # The metrics file is rewritten on each flush (trimmed ring), so it is NOT a growing prefix:
        # ship it with plain rsync (atomic rename on the destination), never --append. The dest is the
        # absolute "metrics" sibling of the runs dir (= parent of the abs run dir).
        abs_metrics = str(PurePosixPath(self._abs_run_dir).parent.parent / "metrics")
        self._run(ssh.rsync_argv(self.cfg, path, f"{abs_metrics}/{path.name}", append=False))

    def _final_flush(self) -> None:
        ok, counts = self._final or (None, None)
        try:
            if not self._bootstrapped:
                self._remote_bootstrap()
                self._bootstrapped = True
            self._ship_events()
            self._ship_logs()
            self._ship_metrics()
            if ok is not None:                    # a real completion: write the done marker
                done = json.dumps({"ok": bool(ok), "counts": counts or {}}).encode("utf-8")
                self._run(ssh.ssh_argv(self.cfg, f"cat > {self._rd()}/done"), input=done)
            self._heartbeat()
        except Exception as exc:
            log.debug("pipelines: dashboard final flush failed: %s", exc)
        finally:
            if self._metrics is not None:
                self._metrics.stop()
            try:
                subprocess.run(ssh.ssh_exit_argv(self.cfg), capture_output=True, timeout=5.0)
            except Exception:
                pass

    # -- subprocess plumbing ------------------------------------------- #
    def _run(self, argv: list[str], *, input: bytes | None = None) -> bytes:
        try:
            proc = subprocess.run(argv, input=input, capture_output=True,
                                  timeout=self._SUBPROC_TIMEOUT)
        except FileNotFoundError as exc:
            self._disabled = True
            log.warning("pipelines: %s not found; dashboard reporting disabled for this run",
                        argv[0])
            raise _ShipError(str(exc)) from None
        except subprocess.TimeoutExpired:
            raise _ShipError(f"{argv[0]} timed out") from None
        except OSError as exc:
            raise _ShipError(str(exc)) from None
        if proc.returncode != 0:
            err = (proc.stderr or b"").decode("utf-8", "replace").strip()
            raise _ShipError(f"{argv[0]} exit {proc.returncode}: {err[:200]}")
        return proc.stdout or b""
