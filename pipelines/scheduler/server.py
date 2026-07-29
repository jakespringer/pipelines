"""The run server: a resource-aware scheduler that *runs jobs* and *serves an API*.

One process does both. The scheduler keeps a ready-set and launches each artifact whose
dependencies are satisfied and whose resource request fits the free pool, pinning GPUs via
``CUDA_VISIBLE_DEVICES`` in a worker subprocess. Concurrently a TCP server (port 16400+)
accepts clients that subscribe to the live event stream and send control commands
(``cancel``/``hold``/``release``); worker subprocesses connect to the same port to report a
``yield`` (resources freed while the job finishes saving/uploading).

Everything is event-driven through one internal queue, so all job-state mutation happens on
the scheduler thread — other threads only enqueue. State reads (snapshots) take a short lock.
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import socket
import subprocess
import threading
import time
import uuid
from pathlib import Path

from ..identity import slug
from ..reporting import Reporter, short_hostname
from . import protocol, registry
from .resources import Assignment, ResourcePool, ResourceRequest

log = logging.getLogger("pipelines")

# Terminal job states and the "a dependent of this can never run" states.
_TERMINAL = {"completed", "failed", "cancelled", "blocked"}
_BAD_DEP = {"failed", "cancelled", "blocked"}


class Job:
    """Mutable per-artifact scheduling state. The artifact object stays in-process."""

    def __init__(self, artifact, deps: list[str], log_path: Path):
        self.artifact = artifact
        self.relpath: str = artifact.relpath
        self.deps = deps                      # dep relpaths that are part of this run's graph
        self.req = ResourceRequest.from_annotations(getattr(artifact, "annotations", {}) or {})
        self.env = dict(getattr(artifact.__pipelines__, "env", None) or {})
        self.log_path = log_path

        self.state = "queued"
        self.held = False
        self.assignment: Assignment | None = None
        self.released = False
        self.proc: subprocess.Popen | None = None
        self.logf = None
        self.pid: int | None = None
        self.token: str | None = None
        self.started_at: float | None = None
        self.ended_at: float | None = None
        self.exit_code: int | None = None
        self.reason: str = ""

    def snapshot(self) -> dict:
        return {
            "relpath": self.relpath,
            "cls": type(self.artifact).__name__,
            "state": self.state,
            "gpus": list(self.assignment.gpus) if self.assignment else [],
            "pid": self.pid,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "exit_code": self.exit_code,
            "log_path": str(self.log_path),
            "req": {"gpus": self.req.gpus, "cpus": self.req.cpus, "memory_mb": self.req.memory_mb},
            "deps": list(self.deps),
            "held": self.held,
            "reason": self.reason,
        }


class _Client:
    """A connected subscriber socket with its own write lock."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.lock = threading.Lock()
        self.alive = True

    def send(self, obj: dict) -> None:
        if not self.alive:
            return
        try:
            protocol.send_msg(self.sock, obj, self.lock)
        except OSError:
            self.alive = False


class RunServer:
    """Schedules and runs a set of jobs while serving a monitor/control API."""

    def __init__(self, *, specs: list, done: set[str], pool: ResourcePool,
                 worker_argv_prefix: list[str], store: str, base_path: str,
                 runs_root: Path | None = None, project: str | None = None,
                 manifest: list[dict] | None = None):
        # Bind first: the port names this run's log dir and the registry entry.
        self._sock, self.port = _bind_server_socket()
        root = Path(runs_root) if runs_root is not None else registry.registry_dir()
        self.logdir = root / str(self.port)
        self.logdir.mkdir(parents=True, exist_ok=True)
        (self.logdir / "jobs").mkdir(exist_ok=True)
        self.started_at = time.time()
        # The local logdir stays port-keyed (so `pipelines attach` is unchanged); the dashboard,
        # which a remote job ships to, needs an id unique across every host reporting to it.
        self.remote_run_id = f"{short_hostname()}-{int(self.started_at)}-{os.getpid()}"
        self.reporter = Reporter.open(
            events_path=self.logdir / "events.log", run_id=self.remote_run_id,
            meta={"project": project, "store": store, "base_path": base_path,
                  "started_at": self.started_at},
            on_record=self._broadcast)

        # specs: list of (artifact, dep_relpaths). Build Job objects now that logdir exists.
        self.order = [Job(a, deps, job_log_path(self.logdir, a.relpath)) for a, deps in specs]
        self.jobs = {j.relpath: j for j in self.order}
        self._level = self._compute_levels()              # BFS depth per job (homogeneous launch order)
        self.done = set(done)                              # satisfied (pre-committed) relpaths
        self.pool = pool
        self.worker_argv_prefix = worker_argv_prefix
        self.store = store
        self.base_path = base_path
        self.project = project
        self.manifest = manifest or []        # full plan (incl. cached) for monitors; display-only

        self.events: queue.Queue = queue.Queue()
        self._lock = threading.RLock()                     # guards job-state reads/writes
        self._clients: list[_Client] = []
        self._tokens: dict[str, str] = {}                  # yield-token -> relpath
        self._stop = threading.Event()

    # ------------------------------------------------------------------ #
    # Public entry
    # ------------------------------------------------------------------ #
    def run(self) -> int:
        """Run to completion (foreground). Returns 0 if every job succeeded, else 1."""
        registry.write({
            "port": self.port, "pid": os.getpid(), "logdir": str(self.logdir),
            "base_path": self.base_path, "store": self.store,
            "project": self.project, "started_at": self.started_at,
        })
        acceptor = threading.Thread(target=self._accept_loop, daemon=True)
        acceptor.start()

        self.emit("server_start", port=self.port, logdir=str(self.logdir),
                  pool=self.pool.snapshot(), n_jobs=len(self.order),
                  jobs=[j.relpath for j in self.order],
                  project=self.project, store=self.store, base_path=self.base_path,
                  plan=self.manifest)
        print(f"pipelines: parallel run serving on 127.0.0.1:{self.port}")
        print(f"pipelines: events -> {self.logdir/'events.log'}")
        print(f"pipelines: attach with `pipelines attach {self.port}`")

        try:
            with self._lock:
                self._try_launch()
            while not self._terminal():
                try:
                    item = self.events.get(timeout=1.0)
                except queue.Empty:
                    continue
                with self._lock:
                    self._handle(item)
                    self._try_launch()
        except KeyboardInterrupt:
            print("\npipelines: interrupted; terminating running jobs ...")
            with self._lock:
                self._terminate_all("interrupted")
        finally:
            ok = self._finish()
        return 0 if ok else 1

    # ------------------------------------------------------------------ #
    # Event handling (scheduler thread only)
    # ------------------------------------------------------------------ #
    def _handle(self, item) -> None:
        kind = item[0]
        if kind == "done":
            self._on_done(item[1], item[2])
        elif kind == "yield":
            self._on_yield(item[1])
        elif kind == "cmd":
            self._on_cmd(item[1])

    def _on_done(self, relpath: str, rc: int) -> None:
        job = self.jobs.get(relpath)
        if job is None:
            return
        self._release(job)
        job.ended_at = time.time()
        job.exit_code = rc
        if job.state == "cancelled":
            pass                                           # we asked for this
        elif rc == 0:
            job.state = "completed"
            self.done.add(relpath)
        else:
            job.state = "failed"
            job.reason = f"exit {rc}"
        self._emit_job(job)

    def _on_yield(self, relpath: str) -> None:
        job = self.jobs.get(relpath)
        if job is None or job.state != "running":
            return
        self._release(job)
        job.state = "yielding"
        self._emit_job(job)

    def _on_cmd(self, payload: dict) -> None:
        cmd = payload.get("cmd")
        relpath = payload.get("relpath")
        if cmd in ("cancel", "hold", "release") and relpath not in self.jobs:
            return
        if cmd == "cancel":
            self._cancel(self.jobs[relpath])
        elif cmd == "hold":
            job = self.jobs[relpath]
            if job.state in ("queued", "held"):
                job.held = True
                job.state = "held"
                self._emit_job(job)
        elif cmd == "release":
            job = self.jobs[relpath]
            if job.state == "held":
                job.held = False
                job.state = "queued"
                self._emit_job(job)
        elif cmd in ("cancel_all", "shutdown"):
            self._terminate_all("cancelled by client")

    def _cancel(self, job: Job) -> None:
        if job.state in ("running", "yielding"):
            job.state = "cancelled"
            job.reason = "cancelled"
            self._emit_job(job)
            if job.proc is not None:
                try:
                    job.proc.terminate()
                except OSError:
                    pass
            self._release(job)                             # free promptly; done event also no-ops
        elif job.state in ("queued", "held"):
            job.state = "cancelled"
            job.reason = "cancelled"
            self._emit_job(job)

    # ------------------------------------------------------------------ #
    # Launching
    # ------------------------------------------------------------------ #
    def _try_launch(self) -> None:
        # Resolve dependency-driven states for queued jobs, and collect those ready to run.
        ready = []
        for job in self.order:
            if job.state != "queued":
                continue
            dep_states = [self._dep_state(d) for d in job.deps]
            if any(s in _BAD_DEP for s in dep_states):
                job.state = "blocked"
                job.reason = "dependency did not complete"
                self._emit_job(job)
                continue
            if not all(self._is_done(d) for d in job.deps):
                continue                                   # still waiting on a dependency
            if not self.pool.can_ever_fit(job.req):
                job.state = "failed"
                job.reason = f"unschedulable: needs {job.req.describe()}, host too small"
                self._emit_job(job)
                continue
            ready.append(job)                              # deps satisfied; host can fit it eventually

        # Launch ready jobs that fit right now, one at a time, re-evaluating after each. Tie-break so
        # the set of concurrently-running jobs stays as homogeneous as possible: prefer a job whose
        # type already has a sibling running; otherwise walk the dependency graph breadth-first
        # (shallower stages first), then declaration order. A freshly started type becomes "running",
        # so its siblings are picked next.
        while True:
            fits = [j for j in ready if j.state == "queued" and self.pool.can_fit_now(j.req)]
            if not fits:
                break
            active = self._running_types()
            fits.sort(key=lambda j: (0 if self._type_of(j) in active else 1,
                                     self._level.get(j.relpath, 0)))
            self._launch(fits[0])

    @staticmethod
    def _type_of(job: Job) -> str:
        return type(job.artifact).__name__

    def _running_types(self) -> set:
        """Artifact types with a job currently running (or yielding)."""
        return {self._type_of(j) for j in self.order if j.state in ("running", "yielding")}

    def _compute_levels(self) -> dict:
        """Breadth-first depth per job over this run's dependency graph: 0 for a job with no in-run
        dependency, else one past its deepest in-run dependency. Jobs at the same depth are usually
        the same pipeline step, so depth order keeps concurrently-running work homogeneous."""
        level: dict[str, int] = {}

        def depth(relpath: str) -> int:
            if relpath in level:
                return level[relpath]
            level[relpath] = 0                             # tentative (the graph is a DAG)
            job = self.jobs.get(relpath)
            deps = [d for d in (job.deps if job else []) if d in self.jobs]
            level[relpath] = 1 + max((depth(d) for d in deps), default=-1)
            return level[relpath]

        for relpath in self.jobs:
            depth(relpath)
        return level

    def _launch(self, job: Job) -> None:
        job.assignment = self.pool.acquire(job.req)
        job.token = uuid.uuid4().hex
        self._tokens[job.token] = job.relpath

        env = dict(os.environ)
        env.update(job.env)
        gpu_csv = ",".join(job.assignment.gpus)
        env["CUDA_VISIBLE_DEVICES"] = gpu_csv              # "" hides GPUs from cpu-only jobs
        env["PIPELINES_GPU_IDS"] = gpu_csv
        env["PIPELINES_SERVER_PORT"] = str(self.port)
        env["PIPELINES_JOB_TOKEN"] = job.token
        env["PIPELINES_RELPATH"] = job.relpath

        argv = self.worker_argv_prefix + ["--only", job.relpath]
        job.logf = open(job.log_path, "wb")
        try:
            job.proc = subprocess.Popen(argv, stdout=job.logf,
                                        stderr=subprocess.STDOUT, env=env)
        except OSError as e:
            job.logf.close()
            self.pool.release(job.assignment)
            job.released = True
            job.state = "failed"
            job.reason = f"failed to launch: {e}"
            self._emit_job(job)
            return
        job.pid = job.proc.pid
        job.started_at = time.time()
        job.state = "running"
        self._emit_job(job)
        self.emit("pool", **self.pool.snapshot())
        threading.Thread(target=self._wait_job, args=(job,), daemon=True).start()

    def _wait_job(self, job: Job) -> None:
        rc = job.proc.wait()
        try:
            job.logf.close()
        except OSError:
            pass
        self.events.put(("done", job.relpath, rc))

    def _release(self, job: Job) -> None:
        if job.assignment is not None and not job.released:
            self.pool.release(job.assignment)
            job.released = True
            self.emit("pool", **self.pool.snapshot())

    def _terminate_all(self, reason: str) -> None:
        for job in self.order:
            if job.state in ("running", "yielding"):
                job.state = "cancelled"
                job.reason = reason
                if job.proc is not None:
                    try:
                        job.proc.terminate()
                    except OSError:
                        pass
                self._release(job)
                self._emit_job(job)
            elif job.state in ("queued", "held"):
                job.state = "cancelled"
                job.reason = reason
                self._emit_job(job)
        self._stop.set()

    # ------------------------------------------------------------------ #
    # Readiness helpers
    # ------------------------------------------------------------------ #
    def _dep_state(self, relpath: str) -> str:
        job = self.jobs.get(relpath)
        return job.state if job is not None else "completed"   # not-a-job ⇒ pre-satisfied

    def _is_done(self, relpath: str) -> bool:
        return relpath in self.done or self._dep_state(relpath) == "completed"

    def _terminal(self) -> bool:
        with self._lock:
            return all(j.state in _TERMINAL for j in self.order)

    # ------------------------------------------------------------------ #
    # Eventing / broadcast
    # ------------------------------------------------------------------ #
    def emit(self, type: str, **fields) -> None:
        self.reporter.emit(type, **fields)             # local events.log + broadcast (via on_record)

    def _emit_job(self, job: Job) -> None:
        self.emit("job_state", **job.snapshot())

    def _broadcast(self, record: dict) -> None:
        dead = []
        for c in list(self._clients):
            c.send(record)
            if not c.alive:
                dead.append(c)
        for c in dead:
            self._clients.remove(c)

    # ------------------------------------------------------------------ #
    # Client/worker connections
    # ------------------------------------------------------------------ #
    def _accept_loop(self) -> None:
        self._sock.settimeout(0.5)
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._serve_conn, args=(conn,), daemon=True).start()

    def _serve_conn(self, conn: socket.socket) -> None:
        conn.settimeout(10.0)
        fh = protocol.reader(conn)
        first = protocol.next_msg(fh)
        if first is None:
            conn.close()
            return
        cmd = first.get("cmd")

        if cmd == "yield":
            relpath = self._tokens.get(first.get("token", ""))
            if relpath:
                self.events.put(("yield", relpath))
            conn.close()
            return

        if cmd == "hello":
            conn.settimeout(None)
            self._serve_subscriber(conn, fh)
            return

        # One-shot control or query.
        if cmd == "status":
            protocol.send_msg(conn, self._status_message())
        elif cmd in ("cancel", "hold", "release", "cancel_all", "shutdown"):
            self.events.put(("cmd", first))
            protocol.send_msg(conn, {"type": "ack", "cmd": cmd})
        conn.close()

    def _serve_subscriber(self, conn: socket.socket, fh) -> None:
        client = _Client(conn)
        with self._lock:
            self._clients.append(client)
            client.send(self._status_message())            # initial snapshot
        try:
            for msg in protocol.iter_msgs(fh):             # further commands on same socket
                if msg.get("cmd") in ("cancel", "hold", "release", "cancel_all", "shutdown"):
                    self.events.put(("cmd", msg))
        except OSError:
            pass
        finally:
            client.alive = False
            with self._lock:
                if client in self._clients:
                    self._clients.remove(client)
            conn.close()

    def _status_message(self) -> dict:
        with self._lock:
            return {
                "type": "snapshot",
                "port": self.port,
                "logdir": str(self.logdir),
                "started_at": self.started_at,
                "pool": self.pool.snapshot(),
                "jobs": [j.snapshot() for j in self.order],
                "counts": self._counts(),
            }

    def _counts(self) -> dict:
        counts: dict[str, int] = {}
        for j in self.order:
            counts[j.state] = counts.get(j.state, 0) + 1
        return counts

    # ------------------------------------------------------------------ #
    # Shutdown
    # ------------------------------------------------------------------ #
    def _finish(self) -> bool:
        counts = self._counts()
        ok = all(j.state == "completed" for j in self.order)
        self.emit("server_done", ok=ok, counts=counts)
        elapsed = time.time() - self.started_at
        print(f"pipelines: done in {elapsed:.0f}s — {counts}")
        self._stop.set()
        for c in list(self._clients):
            try:
                c.sock.close()
            except OSError:
                pass
        try:
            self._sock.close()
        except OSError:
            pass
        self.reporter.close(ok=ok, counts=counts)      # final flush + remote `done` if shipping
        registry.remove(self.port)                     # local attach hint; remote entry persists
        return ok


def _bind_server_socket(start: int = 16400, tries: int = 200) -> tuple[socket.socket, int]:
    """Bind a listening socket on the first free port at/after ``start``."""
    for port in range(start, start + tries):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            s.listen(64)
            return s, port
        except OSError:
            s.close()
            continue
    raise RuntimeError(f"no free port in [{start}, {start + tries})")


def job_log_path(logdir: Path, relpath: str) -> Path:
    """Per-job combined-output log path; ``relpath`` is slugged into one filename.

    Deeply-chained relpaths can slug past the filesystem's 255-byte filename
    limit (NAME_MAX), and the resulting ``open()`` OSError would kill the whole
    scheduler — cap the slug and keep a short digest of the full relpath so
    truncated names stay unique.
    """
    name = slug(relpath)
    if len(name) > 200:
        digest = hashlib.sha1(relpath.encode()).hexdigest()[:10]
        name = f"{name[:200].rstrip('_')}-{digest}"
    return Path(logdir) / "jobs" / f"{name}.log"
