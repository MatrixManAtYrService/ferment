"""The server's side of a monitoring stack (Prometheus, Loki) beside it.

`/metrics` re-exports what the server already knows, in Prometheus' text
format: the latest sample of each VM (ferment.measure) and the guest
clock's offset, so Grafana and `fmnt measure` read the same numbers.

`LogShipper` pushes the workload's process logs (read through the agent, by
byte offset) and any local files to Loki, each line stamped with **real**
time as it's shipped (the workload VM's clock is the fake one, and nothing
that needs real time runs there). A line that starts with a timestamp keeps
it as `fake_time` structured metadata; one that doesn't gets the fake time
it was shipped at. Offsets are kept in a state file, so a restart neither
repeats nor skips; on the first start, existing logs are skipped (shipping
gigabytes of backlog stamped "now" would only mislead).
"""

from __future__ import annotations

import datetime as dt
import json
import re
import threading
import time
from collections.abc import Callable
from pathlib import Path

import httpx

# -- /metrics ---------------------------------------------------------------------


def _esc(v: object) -> str:
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _line(name: str, labels: dict[str, object], value: object) -> str | None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    inner = ",".join(f'{k}="{_esc(v)}"' for k, v in labels.items())
    return f"{name}{{{inner}}} {value}" if inner else f"{name} {value}"


# sample key -> (metric, help)
_GAUGES = {
    "cpu_busy_pct": ("ferment_vm_cpu_busy_percent", "CPU busy over the last interval, %"),
    "ncpu": ("ferment_vm_cpus", "CPUs"),
    "load1": ("ferment_vm_load1", "1-minute load average"),
    "mem_total_mib": ("ferment_vm_memory_total_mib", "Memory, MiB"),
    "mem_used_mib": ("ferment_vm_memory_used_mib", "Memory used (total - available), MiB"),
    "swap_used_mib": ("ferment_vm_swap_used_mib", "Swap used, MiB"),
}
_PSI = {"psi_cpu": "cpu", "psi_mem": "memory", "psi_io": "io"}


def render_metrics(latest: dict[str, tuple[float, dict]], offset_seconds: float, fake_now: dt.datetime) -> str:
    """`latest`: VM -> (unix real time, sample)."""
    out: list[str] = []

    def family(name: str, help_: str, lines: list[str | None]) -> None:
        kept = [x for x in lines if x is not None]
        if kept:
            out.extend([f"# HELP {name} {help_}", f"# TYPE {name} gauge", *kept])

    for key, (name, help_) in _GAUGES.items():
        family(name, help_, [_line(name, {"vm": vm}, s.get(key)) for vm, (_, s) in sorted(latest.items())])
    family(
        "ferment_vm_pressure_avg10",
        "Pressure stall (PSI some avg10, %): above ~10, work was held back",
        [_line("ferment_vm_pressure_avg10", {"vm": vm, "resource": r}, s.get(k))
         for vm, (_, s) in sorted(latest.items()) for k, r in _PSI.items()],
    )  # fmt: skip
    family(
        "ferment_vm_disk_used_gib",
        "Disk used, GiB",
        [_line("ferment_vm_disk_used_gib", {"vm": vm, "disk": d}, v)
         for vm, (_, s) in sorted(latest.items()) for d, v in sorted((s.get("disk_used_gib") or {}).items())],
    )  # fmt: skip
    family(
        "ferment_process_memory_mib",
        "Memory per workload process tree (PSS), or the largest processes by name, MiB",
        [_line("ferment_process_memory_mib", {"vm": vm, "process": p}, v)
         for vm, (_, s) in sorted(latest.items()) for p, v in sorted((s.get("processes_mib") or {}).items())],
    )  # fmt: skip
    family(
        "ferment_vm_sample_time_seconds",
        "When the VM's latest sample was taken (unix, real time)",
        [_line("ferment_vm_sample_time_seconds", {"vm": vm}, round(t, 3)) for vm, (t, _) in sorted(latest.items())],
    )
    family("ferment_clock_offset_seconds", "The guest clock minus real time, s", [
        _line("ferment_clock_offset_seconds", {}, offset_seconds)])  # fmt: skip
    family("ferment_clock_fake_time_seconds", "What the guest clock reads (unix)", [
        _line("ferment_clock_fake_time_seconds", {}, round(fake_now.timestamp(), 3))])  # fmt: skip
    return "\n".join(out) + "\n"


# -- logs -------------------------------------------------------------------------

# A timestamp at the start of a line (after a bracket, maybe): Spring's
# "2026-11-01T03:10:00.123Z", Python's "2026-11-01 03:10:00,123".
_STAMP = re.compile(r"^\W{0,2}(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:[.,](\d{1,9}))?(Z|[+-]\d{2}:?\d{2})?")
CHUNK = 1_000_000


def fake_stamp(line: str) -> str | None:
    """The line's own timestamp (ISO, UTC if it says no zone), if it starts with one."""
    m = _STAMP.match(line)
    if not m:
        return None
    day, clock, frac, zone = m.groups()
    zone = "+00:00" if zone in (None, "Z") else (zone if ":" in zone else f"{zone[:3]}:{zone[3:]}")
    return f"{day}T{clock}{'.' + frac[:6] if frac else ''}{zone}"


Reader = Callable[[int], tuple[str, int]]  # offset -> (text from there, the next offset)


def file_reader(path: Path) -> Reader:
    def read(offset: int) -> tuple[str, int]:
        if not path.exists():
            return "", offset
        offset = min(offset, path.stat().st_size)
        with path.open("rb") as f:
            f.seek(offset)
            data = f.read(CHUNK)
        return data.decode(errors="replace"), offset + len(data)

    return read


class LogShipper:
    """Every `interval` s, what each source wrote since last time, to Loki.

    `sources()` -> {stream name: (labels, reader)}; the name keys the offset.
    `fake_now()` is the guest clock's reading now (for lines without a stamp).
    """

    def __init__(
        self,
        loki_url: str,
        sources: Callable[[], dict[str, tuple[dict[str, str], Reader]]],
        state_file: Path,
        fake_now: Callable[[], dt.datetime],
        real_now: Callable[[], dt.datetime],
        interval: float = 5.0,
        http: httpx.Client | None = None,
    ):
        self.url = loki_url.rstrip("/") + "/loki/api/v1/push"
        self.sources, self.state_file, self.fake_now, self.real_now = sources, state_file, fake_now, real_now
        self.interval = interval
        self.http = http or httpx.Client(timeout=30)
        self.first_start = not state_file.exists()
        self.offsets: dict[str, int] = json.loads(state_file.read_text()) if state_file.exists() else {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._ns = 0  # the last timestamp pushed: each line's is later, so Loki keeps their order

    def tick(self) -> int:
        """Ship what's new; the number of lines shipped."""
        shipped = 0
        for name, (labels, read) in self.sources().items():
            if name not in self.offsets:
                # Skip what was there before shipping ever started; ship a new log from its start.
                self.offsets[name] = 2**62 if self.first_start else 0
            try:
                shipped += self._ship(name, labels, read)
            except (httpx.HTTPError, OSError):  # Loki or the agent not up yet: try again next time
                continue
        self.first_start = False
        self._save()
        return shipped

    def _ship(self, name: str, labels: dict[str, str], read: Reader) -> int:
        n = 0
        while True:
            start = self.offsets[name]
            text, nxt = read(start)
            if not text:
                self.offsets[name] = nxt  # where the reader clamped to (the end, on a first start)
                return n
            # Whole lines only: a partial last line waits for the rest, unless
            # the chunk is all one line.
            cut = text.rfind("\n") + 1
            if cut == 0 and nxt - start < CHUNK:
                return n
            body, new = (text, nxt) if cut in (0, len(text)) else (text[:cut], nxt - len(text[cut:].encode()))
            fake, real = self.fake_now().isoformat(), self.real_now()
            values = []
            for line in body.splitlines():
                if line.strip():
                    self._ns = max(self._ns + 1, int(real.timestamp() * 1e9))
                    values.append([str(self._ns), line, {"fake_time": fake_stamp(line) or fake}])
            if values:
                self.http.post(self.url, json={"streams": [{"stream": labels, "values": values}]}).raise_for_status()
                n += len(values)
            self.offsets[name] = new
            if new != nxt or nxt - start < CHUNK:  # a partial line waits, or caught up
                return n

    def _save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.offsets))
        tmp.replace(self.state_file)

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="logship", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.tick()
            except Exception as e:  # noqa: BLE001 - a bad round is a gap, not a dead shipper
                print(f"logship: {e}", flush=True)
                time.sleep(self.interval)
