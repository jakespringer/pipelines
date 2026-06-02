"""Resource requests, host detection, and a concrete resource pool.

The parallel scheduler reads an artifact's portable ``annotations`` (``gpus``/``cpus``/
``memory`` — see ``design/08 §7``) into a :class:`ResourceRequest`, detects what the host
actually has, and hands out **concrete GPU ids** so each job can be pinned via
``CUDA_VISIBLE_DEVICES``. All pure-ish helpers; the only I/O is reading host facts.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import re
import shutil
import subprocess

log = logging.getLogger("pipelines")

_MEM_UNITS = {"": 1, "B": 1 / (1024 ** 2), "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 ** 2}


def parse_memory_mb(value) -> int:
    """Parse a memory annotation (``"96G"``, ``"512M"``, ``8`` → MB) into integer MB.

    A bare number is read as GB (matching how ``gpu_annotations`` writes ``"96G"``-style
    strings). Returns 0 for ``None``/unparseable, meaning "no memory constraint".
    """
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value * 1024)                      # bare number == GB
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?)B?\s*", str(value), re.IGNORECASE)
    if not m:
        log.info("resources: could not parse memory %r; treating as unconstrained", value)
        return 0
    return int(float(m.group(1)) * _MEM_UNITS[m.group(2).upper()])


@dataclasses.dataclass(frozen=True)
class ResourceRequest:
    """What one artifact needs to run, distilled from its ``annotations``."""
    gpus: int = 0
    cpus: int = 1
    memory_mb: int = 0

    @classmethod
    def from_annotations(cls, ann: dict | None) -> "ResourceRequest":
        ann = ann or {}
        gpus = int(ann.get("gpus", 0) or 0)
        cpus = int(ann.get("cpus", 0) or 0) or 1            # always need at least one core
        return cls(gpus=gpus, cpus=cpus, memory_mb=parse_memory_mb(ann.get("memory")))

    def describe(self) -> str:
        bits = [f"{self.gpus}gpu", f"{self.cpus}cpu"]
        if self.memory_mb:
            bits.append(f"{self.memory_mb // 1024}G" if self.memory_mb >= 1024
                        else f"{self.memory_mb}M")
        return "/".join(bits)


@dataclasses.dataclass
class Assignment:
    """A concrete grant handed back by :meth:`ResourcePool.acquire`."""
    gpus: list[str]
    cpus: int
    memory_mb: int


# --------------------------------------------------------------------------- #
# Host detection
# --------------------------------------------------------------------------- #

def detect_gpu_ids() -> list[str]:
    """Concrete GPU ids visible on this host.

    Honors a pre-set ``CUDA_VISIBLE_DEVICES`` (so the scheduler only ever hands out a
    subset of an already-restricted set); otherwise asks ``nvidia-smi``. Returns ids as
    strings (they may be integer indices or UUIDs). Empty list ⇒ no GPUs.
    """
    env = os.environ.get("CUDA_VISIBLE_DEVICES")
    if env is not None and env.strip() != "":
        ids = [tok.strip() for tok in env.split(",") if tok.strip()]
        log.info("resources: GPUs from CUDA_VISIBLE_DEVICES = %s", ids)
        return ids
    nvsmi = shutil.which("nvidia-smi")
    if not nvsmi:
        log.info("resources: no nvidia-smi on PATH; assuming 0 GPUs")
        return []
    try:
        out = subprocess.run([nvsmi, "--query-gpu=index", "--format=csv,noheader"],
                             capture_output=True, text=True, check=True, timeout=15).stdout
    except (subprocess.SubprocessError, OSError) as e:
        log.info("resources: nvidia-smi failed (%s); assuming 0 GPUs", e)
        return []
    ids = [line.strip() for line in out.splitlines() if line.strip()]
    log.info("resources: detected %d GPU(s) via nvidia-smi: %s", len(ids), ids)
    return ids


def detect_cpus() -> int:
    return os.cpu_count() or 1


def detect_memory_mb() -> int:
    """Total system memory in MB (Linux ``/proc/meminfo``); 0 if unknown (unconstrained)."""
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) // 1024     # kB -> MB
    except OSError:
        pass
    return 0


# --------------------------------------------------------------------------- #
# The pool
# --------------------------------------------------------------------------- #

class ResourcePool:
    """Tracks free GPUs/CPUs/memory and grants concrete GPU ids.

    ``memory_mb == 0`` (host memory unknown) disables the memory constraint entirely, as
    does a request with ``memory_mb == 0``.
    """

    def __init__(self, gpu_ids: list[str], cpus: int, memory_mb: int):
        self.all_gpus = list(gpu_ids)
        self.free_gpus = list(gpu_ids)
        self.total_cpus = cpus
        self.free_cpus = cpus
        self.total_memory_mb = memory_mb
        self.free_memory_mb = memory_mb

    # --- queries ----------------------------------------------------------- #
    def _mem_ok(self, need: int, available: int) -> bool:
        return self.total_memory_mb == 0 or need == 0 or need <= available

    def can_ever_fit(self, req: ResourceRequest) -> bool:
        """Whether this host could *ever* satisfy ``req`` (used to fail fast)."""
        return (req.gpus <= len(self.all_gpus)
                and req.cpus <= self.total_cpus
                and self._mem_ok(req.memory_mb, self.total_memory_mb))

    def can_fit_now(self, req: ResourceRequest) -> bool:
        return (req.gpus <= len(self.free_gpus)
                and req.cpus <= self.free_cpus
                and self._mem_ok(req.memory_mb, self.free_memory_mb))

    # --- mutation ---------------------------------------------------------- #
    def acquire(self, req: ResourceRequest) -> Assignment:
        if not self.can_fit_now(req):
            raise RuntimeError(f"cannot acquire {req.describe()}: insufficient free resources")
        gpus = [self.free_gpus.pop(0) for _ in range(req.gpus)]
        self.free_cpus -= req.cpus
        if self.total_memory_mb:
            self.free_memory_mb -= req.memory_mb
        return Assignment(gpus=gpus, cpus=req.cpus, memory_mb=req.memory_mb)

    def release(self, assignment: Assignment) -> None:
        self.free_gpus.extend(assignment.gpus)
        # Keep a stable order so the lowest ids are reused first (nicer logs).
        self.free_gpus.sort(key=lambda x: (len(x), x))
        self.free_cpus += assignment.cpus
        if self.total_memory_mb:
            self.free_memory_mb += assignment.memory_mb

    def snapshot(self) -> dict:
        return {
            "gpus": {"free": len(self.free_gpus), "total": len(self.all_gpus),
                     "free_ids": list(self.free_gpus)},
            "cpus": {"free": self.free_cpus, "total": self.total_cpus},
            "memory_mb": {"free": self.free_memory_mb, "total": self.total_memory_mb},
        }


def detect_pool(gpus: int | None = None, cpus: int | None = None,
                memory_mb: int | None = None) -> ResourcePool:
    """Build a pool from host detection, with optional explicit overrides.

    ``gpus`` override caps the GPU id list to the first N detected (or fabricates
    ``"0".."N-1"`` if none were detected but N was requested — useful for dry CPU-only hosts
    that still want to pretend-schedule).
    """
    gpu_ids = detect_gpu_ids()
    if gpus is not None:
        if gpus <= len(gpu_ids):
            gpu_ids = gpu_ids[:gpus]
        else:
            gpu_ids = gpu_ids + [str(i) for i in range(len(gpu_ids), gpus)]
    return ResourcePool(
        gpu_ids=gpu_ids,
        cpus=cpus if cpus is not None else detect_cpus(),
        memory_mb=memory_mb if memory_mb is not None else detect_memory_mb(),
    )
