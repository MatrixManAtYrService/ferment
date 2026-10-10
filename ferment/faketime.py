"""Faking the clock for a process tree with libfaketime.

libfaketime is LD_PRELOADed and intercepts libc's time calls (time,
gettimeofday, clock_gettime, ...). Anything that reads the clock through libc
is fooled: the JVM, MariaDB, Python, coreutils. Anything that doesn't is not:
Go binaries (process-compose, grafana, loki, prometheus) read the clock
through the vDSO directly. That split is used on purpose: the workload is
faked, the supervisor and observability stack stay on real time.

Consistency across processes is the whole point, so we use libfaketime's
*offset* mode (FAKETIME="+N" seconds), never its start-at mode
(FAKETIME="@2026-10-03 00:00:00"). Start-at is relative to each process's
own start, so a service that boots 40s after the database would run 40s
behind it. An offset is the same for every process however late it starts
(including children spawned hours later, like airflow task runs).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class FakeClock:
    """A fixed offset from real time, chosen so that 'now' reads `target`
    at the moment `anchored_at` (real time)."""

    offset_seconds: int

    @classmethod
    def starting_at(cls, target: dt.datetime, now: dt.datetime | None = None) -> FakeClock:
        if target.tzinfo is None:
            raise ValueError("target must be timezone-aware")
        now = now or dt.datetime.now(dt.UTC)
        return cls(offset_seconds=round((target - now).total_seconds()))

    def now(self) -> dt.datetime:
        return dt.datetime.now(dt.UTC) + dt.timedelta(seconds=self.offset_seconds)

    def env(self, libfaketime: str) -> dict[str, str]:
        """Environment for one faked process."""
        sign = "+" if self.offset_seconds >= 0 else "-"
        return {
            "LD_PRELOAD": libfaketime,
            "FAKETIME": f"{sign}{abs(self.offset_seconds)}",
            # Monotonic clocks measure intervals (timeouts, sleeps, JVM
            # timers); shifting them buys nothing and upsets runtimes.
            "FAKETIME_DONT_FAKE_MONOTONIC": "1",
            # Wall-clock strings must not depend on the guest's zone.
            "TZ": "UTC",
        }
