"""System-metrics sampling for the dashboard's System page — a *producer* and a *reader*.

Producer — :class:`SystemSampler` runs on a **job machine** (driven by the reporting Shipper when
``[config.dashboard].metrics`` is set, one per host). A background thread samples that host's
GPU/CPU/memory at a fixed cadence into a rolling, time-bounded ring, and persists the ring to a small
``<node>.jsonl`` file. The Shipper ``rsync``s that file to the dashboard machine's metrics dir.

Reader — :class:`MetricsStore` runs on the **dashboard machine**. It is a pure read-only view over
the metrics dir: it lists every ``<node>.jsonl`` (the local host plus every remote node rsync'd in)
and serves the node-first API (``nodes()`` and a per-node ``series()``) the System page consumes. The
dashboard starts no sampler of its own — it is a viewer.

Sampling is best-effort and never fatal: any probe that fails contributes a gap, not a crash. CPU
utilization is the standard ``/proc/stat`` busy-fraction over the sampling interval; memory is
``MemTotal - MemAvailable``; per-GPU compute/memory come from ``nvidia-smi`` (absent ⇒ no GPUs).
Disk and network throughput are **rates** computed the same way as CPU — successive readings of the
kernel's monotonic byte counters, differenced over the interval: per block device from
``/proc/diskstats`` (labeled by its mountpoint via ``/proc/mounts``, so only filesystems actually in
use appear), and per interface from ``/proc/net/dev`` (``lo`` and never-used interfaces dropped).
Each sample also carries its ``node`` id and ``ncpu`` so the reader can label and scale a remote
node it can't introspect.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import subprocess
import threading
import time
from collections import deque
from pathlib import Path

from ..identity import slug as _slug
from ..scheduler import registry

log = logging.getLogger("pipelines")

SAMPLE_INTERVAL = 3.0          # seconds between samples
RETAIN_SECONDS = 6 * 3600      # history kept in the ring (and on disk) — caps the longest window
FLUSH_INTERVAL = 60.0          # seconds between disk flushes of the ring
GPU_TIMEOUT = 4.0              # nvidia-smi call timeout (seconds)


def metrics_dir() -> Path:
    """``<cache>/pipelines/metrics/`` — sibling of the run registry, created on demand."""
    d = registry.registry_dir().parent / "metrics"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _to_int(token: str):
    """Parse one ``nvidia-smi`` CSV cell to int; ``None`` for ``[N/A]`` and friends."""
    try:
        return int(str(token).strip())
    except (TypeError, ValueError):
        return None


def _avg(values) -> float | None:
    nums = [v for v in values if v is not None]
    return round(sum(nums) / len(nums), 1) if nums else None


class SystemSampler:
    """Samples this host's GPU/CPU/memory/disk/network into a time-bounded ring on a daemon thread.

    Runs on a job machine and writes ``<node>.jsonl``; the reporting Shipper mirrors that file to the
    dashboard. Thread-safety: a single lock guards the ring; the sampler thread appends/trims under
    it. Sampling and disk I/O happen outside the lock.
    """

    def __init__(self, *, node_id: str | None = None, interval: float = SAMPLE_INTERVAL,
                 retain: float = RETAIN_SECONDS, flush_every: float = FLUSH_INTERVAL,
                 store_dir: Path | None = None):
        self.node_id = node_id or socket.gethostname() or "localhost"
        self.interval = float(interval)
        self.retain = float(retain)
        self.flush_every = float(flush_every)
        self._dir = Path(store_dir) if store_dir is not None else metrics_dir()
        self._path = self._dir / f"{_slug(self.node_id)}.jsonl"
        self.path = self._path                            # the file the Shipper mirrors
        self._lock = threading.Lock()
        self._samples: deque[dict] = deque()
        self._prev_cpu: tuple[int, int] | None = None     # (idle, total) from last /proc/stat read
        self._prev_disk: dict[str, tuple[int, int]] = {}  # dev -> (sectors_read, sectors_written)
        self._prev_net: dict[str, tuple[int, int]] = {}   # iface -> (rx_bytes, tx_bytes)
        self._prev_t: float | None = None                 # timestamp of the last sample (for rates)
        self._mounts: dict[str, str] | None = None        # cached dev -> mountpoint map
        self._mounts_at = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._dirty = False

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    def start(self) -> "SystemSampler":
        self._load()
        self._prev_cpu = self._read_cpu_times()           # seed so the first sample has a delta
        self._prev_disk = self._read_diskstats()
        self._prev_net = self._read_netdev()
        self._prev_t = time.time()
        self._thread = threading.Thread(target=self._loop, name="system-sampler", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._flush()

    def _loop(self) -> None:
        last_flush = time.time()
        while not self._stop.wait(self.interval):
            try:
                sample = self._sample()
            except Exception as exc:                       # never let a probe kill the thread
                log.debug("system-sampler: sample failed: %s", exc)
                continue
            now = sample["t"]
            with self._lock:
                self._samples.append(sample)
                self._trim_locked(now)
                self._dirty = True
            if now - last_flush >= self.flush_every:
                self._flush()
                last_flush = now

    # ------------------------------------------------------------------ #
    # Sampling
    # ------------------------------------------------------------------ #
    def _sample(self) -> dict:
        now = time.time()
        dt = (now - self._prev_t) if self._prev_t else 0.0
        mem_used, mem_total = self._sample_mem()
        sample = {
            "t": now,
            "node": self.node_id,                 # so the reader can label a node it can't introspect
            "ncpu": os.cpu_count() or 1,
            "cpu": self._sample_cpu(),
            "mem_used": mem_used,
            "mem_total": mem_total,
            "gpus": self._sample_gpus(),
            "disk": self._sample_disk(dt, now),
            "net": self._sample_net(dt),
        }
        self._prev_t = now
        return sample

    def _sample_cpu(self) -> float | None:
        cur = self._read_cpu_times()
        prev, self._prev_cpu = self._prev_cpu, cur
        if cur is None or prev is None:
            return None
        d_idle, d_total = cur[0] - prev[0], cur[1] - prev[1]
        if d_total <= 0:
            return None
        return round(100.0 * (1.0 - d_idle / d_total), 1)

    @staticmethod
    def _read_cpu_times() -> tuple[int, int] | None:
        """``(idle, total)`` jiffies from the aggregate ``cpu`` line of ``/proc/stat``."""
        try:
            with open("/proc/stat") as fh:
                line = fh.readline()
        except OSError:
            return None
        if not line.startswith("cpu "):
            return None
        try:
            nums = [int(x) for x in line.split()[1:]]
        except ValueError:
            return None
        if len(nums) < 4:
            return None
        idle = nums[3] + (nums[4] if len(nums) > 4 else 0)     # idle + iowait
        return idle, sum(nums)

    @staticmethod
    def _sample_mem() -> tuple[int | None, int | None]:
        """``(used_mb, total_mb)`` from ``/proc/meminfo`` (used = MemTotal − MemAvailable)."""
        total = avail = None
        try:
            with open("/proc/meminfo") as fh:
                for line in fh:
                    if line.startswith("MemTotal:"):
                        total = int(line.split()[1]) // 1024
                    elif line.startswith("MemAvailable:"):
                        avail = int(line.split()[1]) // 1024
                    if total is not None and avail is not None:
                        break
        except (OSError, ValueError, IndexError):
            return None, None
        if total is None:
            return None, None
        return (total - avail if avail is not None else None), total

    def _sample_gpus(self) -> list[dict]:
        nvsmi = shutil.which("nvidia-smi")
        if not nvsmi:
            return []
        try:
            out = subprocess.run(
                [nvsmi, "--query-gpu=index,utilization.gpu,memory.used,memory.total",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=GPU_TIMEOUT, check=True).stdout
        except (subprocess.SubprocessError, OSError) as exc:
            log.debug("system-sampler: nvidia-smi failed: %s", exc)
            return []
        gpus = []
        for line in out.splitlines():
            cols = [c.strip() for c in line.split(",")]
            if len(cols) < 4 or _to_int(cols[0]) is None:
                continue
            gpus.append({"i": _to_int(cols[0]), "util": _to_int(cols[1]),
                         "mem_used": _to_int(cols[2]), "mem_total": _to_int(cols[3])})
        return gpus

    # ------------------------------------------------------------------ #
    # Disk + network throughput — counter deltas over the sample interval
    # ------------------------------------------------------------------ #
    def _sample_disk(self, dt: float, now: float) -> list[dict]:
        """Per-mounted-device read/write rates (bytes/s). Devices not backing a mount are skipped."""
        cur = self._read_diskstats()
        prev, self._prev_disk = self._prev_disk, cur
        if not prev or dt <= 0:
            return []
        mounts = self._mount_map(now)
        out = []
        for dev, (rsec, wsec) in cur.items():
            mount = mounts.get(dev)
            if mount is None or dev not in prev:           # only filesystems actually in use
                continue
            d_read, d_write = rsec - prev[dev][0], wsec - prev[dev][1]
            if d_read < 0 or d_write < 0:                  # counter reset / device re-added
                continue
            out.append({"dev": dev, "mount": mount,
                        "read_bps": round(d_read * 512 / dt),     # diskstats sectors are 512 bytes
                        "write_bps": round(d_write * 512 / dt)})
        out.sort(key=lambda d: d["mount"])
        return out

    def _sample_net(self, dt: float) -> list[dict]:
        """Per-interface rx/tx rates (bytes/s). ``lo`` and never-used interfaces are dropped."""
        cur = self._read_netdev()
        prev, self._prev_net = self._prev_net, cur
        if not prev or dt <= 0:
            return []
        out = []
        for iface, (rx, tx) in cur.items():
            if iface == "lo" or (rx == 0 and tx == 0) or iface not in prev:
                continue
            d_rx, d_tx = rx - prev[iface][0], tx - prev[iface][1]
            if d_rx < 0 or d_tx < 0:
                continue
            out.append({"iface": iface, "rx_bps": round(d_rx / dt), "tx_bps": round(d_tx / dt)})
        out.sort(key=lambda n: n["iface"])
        return out

    @staticmethod
    def _read_diskstats() -> dict[str, tuple[int, int]]:
        """``{device: (sectors_read, sectors_written)}`` from ``/proc/diskstats``."""
        out: dict[str, tuple[int, int]] = {}
        try:
            with open("/proc/diskstats") as fh:
                for line in fh:
                    parts = line.split()
                    if len(parts) < 10:
                        continue
                    try:
                        out[parts[2]] = (int(parts[5]), int(parts[9]))
                    except ValueError:
                        continue
        except OSError:
            return {}
        return out

    @staticmethod
    def _read_netdev() -> dict[str, tuple[int, int]]:
        """``{iface: (rx_bytes, tx_bytes)}`` from ``/proc/net/dev``."""
        out: dict[str, tuple[int, int]] = {}
        try:
            with open("/proc/net/dev") as fh:
                for line in fh:
                    name, sep, rest = line.partition(":")
                    if not sep:
                        continue
                    cols = rest.split()
                    if len(cols) < 9:
                        continue
                    try:
                        out[name.strip()] = (int(cols[0]), int(cols[8]))
                    except ValueError:
                        continue
        except OSError:
            return {}
        return out

    def _mount_map(self, now: float) -> dict[str, str]:
        """``{device_basename: mountpoint}`` from ``/proc/mounts``, cached for 30s.

        Real block devices (``/dev/…``) only; ``realpath`` resolves ``/dev/mapper/*`` and
        ``by-uuid`` symlinks to their ``dm-N``/``sdaN`` basename so it lines up with diskstats.
        When a device has several mounts, the shortest mountpoint wins (the "main" one).
        """
        if self._mounts is not None and (now - self._mounts_at) < 30.0:
            return self._mounts
        mapping: dict[str, str] = {}
        try:
            with open("/proc/mounts") as fh:
                for line in fh:
                    parts = line.split()
                    if len(parts) < 2 or not parts[0].startswith("/dev/"):
                        continue
                    mount = parts[1].replace("\\040", " ")
                    try:
                        base = os.path.basename(os.path.realpath(parts[0]))
                    except OSError:
                        base = os.path.basename(parts[0])
                    if base and (base not in mapping or len(mount) < len(mapping[base])):
                        mapping[base] = mount
        except OSError:
            mapping = {}
        self._mounts, self._mounts_at = mapping, now
        return mapping

    # ------------------------------------------------------------------ #
    # Ring maintenance + persistence
    # ------------------------------------------------------------------ #
    def _trim_locked(self, now: float) -> None:
        cutoff = now - self.retain
        while self._samples and self._samples[0].get("t", 0) < cutoff:
            self._samples.popleft()

    def _flush(self) -> None:
        with self._lock:
            if not self._dirty:
                return
            rows = list(self._samples)
            self._dirty = False
        tmp = self._path.with_name(self._path.name + ".tmp")
        try:
            with tmp.open("w") as fh:
                for s in rows:
                    fh.write(json.dumps(s, separators=(",", ":")) + "\n")
            os.replace(tmp, self._path)
        except OSError as exc:
            log.debug("system-sampler: flush failed: %s", exc)

    def _load(self) -> None:
        try:
            text = self._path.read_text()
        except OSError:
            return
        cutoff = time.time() - self.retain
        loaded = []
        for raw in text.splitlines():
            raw = raw.strip()
            if not raw:
                continue
            try:
                s = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(s, dict) and isinstance(s.get("t"), (int, float)) and s["t"] >= cutoff:
                loaded.append(s)
        loaded.sort(key=lambda s: s["t"])
        with self._lock:
            self._samples = deque(loaded)


class MetricsStore:
    """Read-only view over every ``<node>.jsonl`` in the metrics dir (local + rsync'd remote nodes).

    The dashboard machine is a viewer: it does not sample. It re-reads the small JSONL files on demand
    — all of them for :meth:`nodes`, one for :meth:`series` — keying each node by the ``node`` id its
    samples carry (falling back to the file's slugged stem). Partial last lines arriving mid-rsync are
    skipped, exactly like the run-event replay.
    """

    def __init__(self, store_dir: Path | None = None, *, retain: float = RETAIN_SECONDS):
        self._dir = Path(store_dir) if store_dir is not None else metrics_dir()
        self.retain = float(retain)

    def stop(self) -> None:
        """No-op (no thread); kept so the server teardown can call it like the old sampler."""

    # -- file discovery / reading -------------------------------------- #
    def _node_files(self) -> dict[str, Path]:
        out: dict[str, Path] = {}
        try:
            for f in self._dir.glob("*.jsonl"):
                out[f.stem] = f
        except OSError:
            pass
        return out

    def _read(self, path: Path) -> list[dict]:
        try:
            text = path.read_text()
        except OSError:
            return []
        cutoff = time.time() - self.retain
        rows = []
        for raw in text.splitlines():
            raw = raw.strip()
            if not raw:
                continue
            try:
                s = json.loads(raw)
            except json.JSONDecodeError:
                continue                                  # torn final line mid-rsync; skip it
            if isinstance(s, dict) and isinstance(s.get("t"), (int, float)) and s["t"] >= cutoff:
                rows.append(s)
        rows.sort(key=lambda s: s["t"])
        return rows

    @staticmethod
    def _label(stem: str, rows: list[dict]) -> str:
        return (rows[-1].get("node") if rows else None) or stem

    # -- read API (called from HTTP handler threads) ------------------- #
    def nodes(self, *, live_runs_by_node: dict[str, int] | None = None) -> dict:
        """Summary of every node we have a metrics file for, for the node selector."""
        live = live_runs_by_node or {}
        now = time.time()
        nodes = []
        for stem, path in sorted(self._node_files().items()):
            rows = self._read(path)
            if not rows:
                continue
            latest, first = rows[-1], rows[0]
            label = self._label(stem, rows)
            gpus = latest.get("gpus") or []
            nodes.append({
                "id": label,
                "label": label,
                "ncpu": latest.get("ncpu") or 1,
                "ngpu": len(gpus),
                "ndisk": len(latest.get("disk") or []),
                "nnet": len(latest.get("net") or []),
                "mem_total_mb": latest.get("mem_total"),
                "cpu": latest.get("cpu"),
                "mem_used_mb": latest.get("mem_used"),
                "gpu_util": _avg([g.get("util") for g in gpus]),
                "last": latest.get("t"),
                "since": first.get("t"),
                "n_samples": len(rows),
                "live_runs": live.get(label, 0),
            })
        nodes.sort(key=lambda n: n["id"])
        # No "self" node on a viewer; the UI falls back to the first node.
        return {"now": now, "current": None, "nodes": nodes}

    def series(self, node: str, *, window: float = 3600.0,
               since_ts: float | None = None) -> dict | None:
        """Time-series for ``node`` over the trailing ``window`` seconds.

        ``since_ts`` (optional) returns only samples newer than it — the client passes the last
        timestamp it holds so steady-state polls transfer just the few new points, not the window.
        Returns ``None`` for an unknown node (→ 404).
        """
        files = self._node_files()
        path = files.get(_slug(node)) or files.get(node)
        if path is None:                                  # fall back: match by the stored label
            for stem, p in files.items():
                if self._label(stem, self._read(p)) == node:
                    path = p
                    break
        if path is None:
            return None
        rows = self._read(path)
        if not rows:
            return None
        now = time.time()
        floor = now - max(1.0, float(window))
        sel = [s for s in rows
               if s.get("t", 0) >= floor and (since_ts is None or s.get("t", 0) > since_ts)]
        latest = rows[-1]
        return {
            "node": node,
            "now": now,
            "window": window,
            "ncpu": latest.get("ncpu") or 1,
            "ngpu": len(latest.get("gpus") or []),
            "mem_total_mb": latest.get("mem_total"),
            "samples": sel,
        }
