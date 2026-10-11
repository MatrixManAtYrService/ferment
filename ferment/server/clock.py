"""The workload's clock is the VM's system clock.

A fermenter is a VM of its own, so instead of faking time per process (an
LD_PRELOAD shim, which every runtime sees differently and which made idle
JVMs burn two cores), the whole guest runs at the fake date: the kernel, file
timestamps, MySQL, the JVM, Go programs. They all agree by construction, at
no cost.

The provider keeps anything from syncing the guest's clock back to real time
(see ferment.vm.lima). This module remembers the offset between the guest's
clock and real time, so the server can still say what time it really is
(event logs, checkpoints), and so it can put the fake date back after a
reboot resets the clock to real time.
"""

from __future__ import annotations

import datetime as dt
import json
import subprocess
from collections.abc import Callable
from pathlib import Path


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def sudo_date(when: dt.datetime) -> None:
    """Set the guest's system clock (the server runs as a sudo-capable user)."""
    subprocess.run(["sudo", "date", "-u", "-s", f"@{when.timestamp():.6f}"], check=True, capture_output=True)


def boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return ""


class GuestClock:
    """offset = guest clock - real time. 0: the guest is on real time."""

    def __init__(
        self,
        state_file: Path,
        set_system_time: Callable[[dt.datetime], None] = sudo_date,
        system_now: Callable[[], dt.datetime] = _utcnow,
        current_boot: Callable[[], str] = boot_id,
    ):
        self.state_file = state_file
        self._set = set_system_time
        self._now = system_now
        self._boot = current_boot
        self.offset = dt.timedelta(0)
        self.set_to: str | None = None  # the fake date last requested
        self.set_at: str | None = None  # real time it was requested
        self.boot: str | None = None  # boot the offset was applied in
        if state_file.exists():
            s = json.loads(state_file.read_text())
            self.offset = dt.timedelta(seconds=s["offset_seconds"])
            self.set_to, self.set_at, self.boot = s.get("set_to"), s.get("set_at"), s.get("boot")

    def now(self) -> dt.datetime:
        """The workload's time: what the guest's clock says."""
        return self._now()

    def real_now(self) -> dt.datetime:
        return self._now() - self.offset

    @property
    def is_set(self) -> bool:
        return self.set_to is not None

    def set(self, target: dt.datetime) -> None:
        if target.tzinfo is None:
            target = target.replace(tzinfo=dt.UTC)
        real = self.real_now()
        self._set(target)
        self.offset = target - real
        self.set_to, self.set_at, self.boot = target.isoformat(), real.isoformat(), self._boot()
        self._save()

    def ensure(self) -> bool:
        """After a reboot the guest is back on real time: put the fake date
        back (it kept advancing meanwhile, like real time). True if it did."""
        if self.offset == dt.timedelta(0) or self.boot == self._boot():
            return False
        self._set(self._now() + self.offset)  # the clock reads real time right now
        self.boot = self._boot()
        self._save()
        return True

    def _save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps({
            "offset_seconds": self.offset.total_seconds(),
            "set_to": self.set_to, "set_at": self.set_at, "boot": self.boot,
        }, indent=2))  # fmt: skip


# The fermenter's real time, for records that should be in real time (event
# log, checkpoint real_time). Set by the server once its clock exists.
_clock: GuestClock | None = None


def use(clock: GuestClock) -> None:
    global _clock
    _clock = clock


def real_now() -> dt.datetime:
    return _clock.real_now() if _clock is not None else _utcnow()
