"""What a fermenter's VMs use, sampled all the time, kept for later.

Sizing a fermenter for a cloud VM wants the peaks (memory above all) of
real runs, read after the fact. So the server samples every VM it has
(itself, and the workload VM through its agent) every INTERVAL_S and keeps
the samples in its own store, outside the snapshots: a restore doesn't take
them back, and they're stamped with the server's clock, which is real time
on a split fermenter (the workload VM's is the fake one, so it isn't used).

A sample, per VM: CPU busy (%), load, memory used and total (MiB), swap
used, pressure (PSI `some avg10`, cpu/memory/io), disk used (GiB) for the
paths asked, and memory per process: each workload process's whole tree
(its PSS summed: a JVM and what it forks, shared pages counted once) when the VM runs a workload,
else the largest processes by name.

Marks name stretches of time (an experiment, a stage being made, a segment
of a run) so a summary can be asked for one of them.

Linux only (/proc); elsewhere `collect` returns what it can.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import statistics
import threading
from collections import defaultdict
from collections.abc import Callable, Iterable
from pathlib import Path

INTERVAL_S = 15.0
TOP = 8  # processes kept by name when there's no workload to group by


def _read(path: str) -> str:
    try:
        return Path(path).read_text()
    except OSError:
        return ""


class CpuMeter:
    """CPU busy % between calls (from /proc/stat's first line)."""

    def __init__(self) -> None:
        self.prev: tuple[int, int] | None = None

    def busy_pct(self) -> float | None:
        line = _read("/proc/stat").split("\n", 1)[0].split()
        if len(line) < 8:
            return None
        v = [int(x) for x in line[1:9]]
        idle = v[3] + v[4]  # idle + iowait
        total = sum(v)
        prev, self.prev = self.prev, (total - idle, total)
        if prev is None or total == prev[1]:
            return None
        return round(100 * (total - idle - prev[0]) / (total - prev[1]), 1)


def _meminfo() -> dict[str, int]:
    out = {}
    for line in _read("/proc/meminfo").splitlines():
        k, _, v = line.partition(":")
        if v.strip():
            out[k] = int(v.split()[0])  # kB
    return out


def _psi(kind: str) -> float | None:
    for line in _read(f"/proc/pressure/{kind}").splitlines():
        if line.startswith("some"):
            for part in line.split():
                if part.startswith("avg10="):
                    return float(part[6:])
    return None


def _procs() -> dict[int, tuple[int, str, int]]:
    """pid -> (ppid, comm, rss_kib) for every process."""
    out = {}
    for d in os.listdir("/proc") if os.path.isdir("/proc") else []:
        if not d.isdigit():
            continue
        stat = _read(f"/proc/{d}/stat")
        if not stat:
            continue
        comm = stat[stat.find("(") + 1 : stat.rfind(")")]
        fields = stat[stat.rfind(")") + 2 :].split()
        out[int(d)] = (int(fields[1]), comm, _pss_kib(d))
    return out


def _pss_kib(pid: str) -> int:
    """A process's proportional set size: its share of the pages it shares with
    others (forked workers share most of theirs), so a tree's sum is what it
    really takes. RSS where smaps_rollup can't be read (another user's process)."""
    for line in _read(f"/proc/{pid}/smaps_rollup").splitlines():
        if line.startswith("Pss:"):
            return int(line.split()[1])
    for line in _read(f"/proc/{pid}/status").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    return 0


def _tree_mib(procs: dict[int, tuple[int, str, int]], roots: dict[str, int]) -> dict[str, float]:
    """Each named root pid's RSS with all its descendants', in MiB."""
    children: dict[int, list[int]] = defaultdict(list)
    for pid, (ppid, _, _) in procs.items():
        children[ppid].append(pid)
    out = {}
    for name, root in roots.items():
        total, stack, seen = 0, [root], set()
        while stack:
            p = stack.pop()
            if p in seen or p not in procs:
                continue
            seen.add(p)
            total += procs[p][2]
            stack += children.get(p, [])
        out[name] = round(total / 1024, 1)
    return out


def _top_by_name(procs: dict[int, tuple[int, str, int]], n: int = TOP) -> dict[str, float]:
    by: dict[str, int] = defaultdict(int)
    for _, comm, rss in procs.values():
        by[comm] += rss
    return {k: round(v / 1024, 1) for k, v in sorted(by.items(), key=lambda kv: -kv[1])[:n]}


def collect(cpu: CpuMeter, disks: dict[str, Path] | None = None, workload: Iterable[dict] | None = None) -> dict:
    """One sample of this machine. `disks`: label -> a path on each filesystem
    to report. `workload`: the supervisor's processes ({name, pid}); their trees'
    memory, else the largest processes by name."""
    mem = _meminfo()
    procs = _procs()
    roots = {p["name"]: int(p["pid"]) for p in workload or [] if p.get("pid")}
    sample: dict = {
        "cpu_busy_pct": cpu.busy_pct(),
        "ncpu": os.cpu_count(),
        "load1": float(_read("/proc/loadavg").split()[0]) if _read("/proc/loadavg") else None,
        "mem_total_mib": round(mem.get("MemTotal", 0) / 1024),
        "mem_used_mib": round((mem.get("MemTotal", 0) - mem.get("MemAvailable", 0)) / 1024),
        "swap_used_mib": round((mem.get("SwapTotal", 0) - mem.get("SwapFree", 0)) / 1024),
        "psi_cpu": _psi("cpu"),
        "psi_mem": _psi("memory"),
        "psi_io": _psi("io"),
        "disk_used_gib": {},
        "processes_mib": _tree_mib(procs, roots) if roots else _top_by_name(procs),
    }
    for label, path in (disks or {}).items():
        with contextlib.suppress(OSError):
            u = shutil.disk_usage(path)
            sample["disk_used_gib"][label] = round(u.used / 2**30, 2)
    return sample


class Sampler:
    """Every `interval` s, a sample from each source (name -> a function that
    returns one, or raises if it can't now), handed to `store(vm, sample)`.
    A source that fails is skipped for that round (an agent restarting)."""

    def __init__(
        self,
        sources: dict[str, Callable[[], dict]],
        store: Callable[[str, dict], None],
        interval: float = INTERVAL_S,
    ):
        self.sources, self.store, self.interval = sources, store, interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def tick(self) -> None:
        for vm, source in self.sources.items():
            try:
                sample = source()
            except Exception:  # noqa: BLE001 - a VM that can't answer now is a gap, not a crash
                continue
            self.store(vm, sample)

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="measure", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            self.tick()


def summarize(samples: list[dict]) -> dict:
    """Per VM: for each number in its samples (nested ones by dotted name), the
    peak, p95 and mean, and how many samples there were."""
    by_vm: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for s in samples:
        for k, v in _flat(s["sample"]):
            if isinstance(v, int | float) and not isinstance(v, bool):
                by_vm[s["vm"]][k].append(float(v))
    out = {}
    for vm, metrics in by_vm.items():
        out[vm] = {
            k: {
                "peak": max(vs),
                "p95": statistics.quantiles(vs, n=20, method="inclusive")[-1] if len(vs) >= 2 else vs[0],
                "mean": round(statistics.fmean(vs), 2),
                "n": len(vs),
            }
            for k, vs in sorted(metrics.items())
        }
    return out


def _flat(d: dict, prefix: str = "") -> Iterable[tuple[str, object]]:
    for k, v in d.items():
        if isinstance(v, dict):
            yield from _flat(v, f"{prefix}{k}.")
        else:
            yield f"{prefix}{k}", v
